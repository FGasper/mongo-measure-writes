package main

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/FGasper/mongo-measure-writes/agg"
	"github.com/FGasper/mongo-measure-writes/cursor"
	mmmath "github.com/FGasper/mongo-measure-writes/math"
	"github.com/FGasper/mongo-measure-writes/resumetoken"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"github.com/urfave/cli/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	getURI := func(c *cli.Command) (string, error) {
		uri := c.Args().First()
		if uri == "" {
			return "", fmt.Errorf("connection string required")
		}

		return uri, nil
	}

	durationFlag := cli.DurationFlag{
		Name:    "duration",
		Aliases: sliceOf("d"),
		Usage:   "interval over which to compile metrics",
		Value:   time.Minute,
	}

	cmd := cli.Command{
		Name:  os.Args[0],
		Usage: "Measure MongoDB document writes per second",
		Commands: []*cli.Command{
			{
				Name:    "oplog",
				Aliases: sliceOf("o"),
				Usage:   "measure via oplog (once)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runOplogMode(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "tailoplog",
				Aliases: sliceOf("to"),
				Usage:   "measure by tailing the oplog",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runTailOplogMode(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "changestream",
				Aliases: sliceOf("cs"),
				Usage:   "measure via change stream",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runChangeStream(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "changestreamloop",
				Aliases: sliceOf("csl"),
				Usage:   "measure via change stream (continually)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runChangeStreamLoop(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "serverstatusloop",
				Aliases: sliceOf("ssl"),
				Usage:   "measure via serverStatus (continually)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runServerStatusLoop(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}

func _runServerStatusLoop(ctx context.Context, connstr string, window time.Duration) error {
	interval := window / 10

	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	type serverStatus struct {
		Opcounters struct {
			Insert int
			Update int
			Delete int
		}
		Metrics struct {
			Document struct {
				Inserted int
				Updated  int
				Deleted  int
			}
		}
	}

	type ssSample struct {
		time   time.Time
		status serverStatus
	}

	var samples []ssSample

	for {
		ss := client.Database("admin").RunCommand(
			ctx,
			bson.D{{"serverStatus", 1}},
		)
		if ss.Err() != nil {
			return fmt.Errorf("fetching server status: %w", ss.Err())
		}

		var newSS serverStatus

		if err := ss.Decode(&newSS); err != nil {
			return fmt.Errorf("parsing server status: %w", err)
		}

		now := time.Now()

		minSampleTime := now.Add(-window)

		samples = append(samples, ssSample{now, newSS})
		for samples[0].time.Before(minSampleTime) {
			samples = samples[1:]
		}

		if len(samples) == 1 {
			fmt.Printf("Compiling stats every %s …\n", interval)
		} else {
			newestSample := lo.LastOrEmpty(samples)
			timeElapsed := newestSample.time.Sub(samples[0].time).Round(
				10 * time.Millisecond,
			)

			docsInserted := newestSample.status.Metrics.Document.Inserted - samples[0].status.Metrics.Document.Inserted
			docsUpdated := newestSample.status.Metrics.Document.Updated - samples[0].status.Metrics.Document.Updated
			docsDeleted := newestSample.status.Metrics.Document.Deleted - samples[0].status.Metrics.Document.Deleted

			totalDocsChanges := docsInserted + docsUpdated + docsDeleted

			fmt.Print("\n" + strings.Repeat("-", 70) + "\n")
			fmt.Printf("Write statistics from the past %s:\n\n", timeElapsed.String())

			fmt.Printf(
				"Metrics (%s total, %s/sec):\n",
				FmtReal(totalDocsChanges),
				FmtReal(mmmath.DivideToF64(
					totalDocsChanges,
					timeElapsed.Seconds(),
				)),
			)

			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{
				"Event Type",
				"Count",
				"% of total count",
			})

			lo.Must0(table.Append(
				"delete",
				FmtReal(docsDeleted),
				FmtReal(mmmath.DivideToF64(100*docsDeleted, totalDocsChanges)),
			))

			lo.Must0(table.Append(
				"insert",
				FmtReal(docsInserted),
				FmtReal(mmmath.DivideToF64(100*docsInserted, totalDocsChanges)),
			))

			lo.Must0(table.Append(
				"update",
				FmtReal(docsUpdated),
				FmtReal(mmmath.DivideToF64(100*docsUpdated, totalDocsChanges)),
			))
			lo.Must0(table.Render())

			insertOps := newestSample.status.Opcounters.Insert - samples[0].status.Opcounters.Insert
			updateOps := newestSample.status.Opcounters.Update - samples[0].status.Opcounters.Update
			deleteOps := newestSample.status.Opcounters.Delete - samples[0].status.Opcounters.Delete

			totalOps := insertOps + updateOps + deleteOps

			fmt.Printf(
				"\nOpcounter (%s total, %s/sec):\n",
				FmtReal(totalOps),
				FmtReal(mmmath.DivideToF64(
					totalOps,
					timeElapsed.Seconds(),
				)),
			)
			table = tablewriter.NewWriter(os.Stdout)
			table.Header([]string{
				"Event Type",
				"Count",
				"% of total count",
			})

			lo.Must0(table.Append(
				"delete",
				FmtReal(deleteOps),
				FmtReal(mmmath.DivideToF64(100*deleteOps, totalOps)),
			))

			lo.Must0(table.Append(
				"insert",
				FmtReal(insertOps),
				FmtReal(mmmath.DivideToF64(100*insertOps, totalOps)),
			))

			lo.Must0(table.Append(
				"update",
				FmtReal(updateOps),
				FmtReal(mmmath.DivideToF64(100*updateOps, totalOps)),
			))
			lo.Must0(table.Render())
		}

		time.Sleep(interval)
	}
}

func _runTailOplogMode(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	isSharded, err := isSharded(ctx, client)
	if err != nil {
		return fmt.Errorf("determining whether cluster is sharded: %w", err)
	}

	if isSharded {
		return fmt.Errorf("cannot tail oplog on a sharded cluster")
	}

	startUnixTime := time.Now().Unix()

	db := client.Database("local")

	resp := db.RunCommand(
		ctx,
		bson.D{
			{"find", "oplog.rs"},
			{"filter", agg.And{
				bson.D{
					{"ts", bson.D{
						{"$gte", bson.Timestamp{T: uint32(startUnixTime)}},
					}},
				},
				bson.D{{"$expr", oplogQueryExpr}},
			}},
			{"tailable", true},
			{"awaitData", true},
			{"projection", bson.D{
				{"ts", 1},
				/*
					{"ns", 1},
					{"o", 1},
					{"o2", 1},
				*/

				{"op", makeSuffixedOpFieldExpr("$$ROOT")},

				{"size", agg.Cond{
					If:   agg.Eq("$op", "c"),
					Then: "$$REMOVE",
					Else: agg.BSONSize("$$ROOT"),
				}},

				{"ops", agg.Cond{
					If: agg.Eq("$op", "c"),
					Then: agg.Map{
						Input: "$o.applyOps",
						As:    "opEntry",
						In: bson.D{
							{"op", makeSuffixedOpFieldExpr("$$opEntry")},
							{"size", agg.BSONSize("$$opEntry")},
						},
					},
				}},
			}},
		},
	)

	cursor, err := cursor.New(db, resp)

	if err != nil {
		return fmt.Errorf("opening oplog cursor: %w", err)
	}

	fmt.Printf("Listening for oplog events. Stats showing every %s …\n", interval)

	eventsMutex := sync.Mutex{}
	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}
	var lag time.Duration

	go func() {
		ticker := time.NewTicker(interval)

		start := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				eventsMutex.Lock()
				counts := maps.Clone(eventCountsByType)
				sizes := maps.Clone(eventSizesByType)
				eventsMutex.Unlock()

				displayTable(counts, sizes, time.Since(start))

				fmt.Printf("Lag: %s\n\n", lag)
			}
		}
	}()

	for !cursor.IsFinished() {
		batch := cursor.GetCurrentBatch()

		eventsMutex.Lock()

		for i, op := range batch {
			/*
				if mustExtract[string](op, "ns") != "admin.$cmd" {
					fmt.Printf("====== op: %v\n\n", op)
				}
			*/

			opType := mustExtract[string](op, "op")

			switch opType {
			case "c":
				ops := mustExtract[[]bson.Raw](op, "ops")

				for _, subOp := range ops {
					opType := "applyOps." + mustExtract[string](subOp, "op")
					eventCountsByType[opType]++
					eventSizesByType[opType] += mustExtract[int](subOp, "size")
				}
			default:
				eventCountsByType[opType]++
				eventSizesByType[opType] += mustExtract[int](op, "size")
			}

			if i == len(batch)-1 {
				ts := mustExtract[bson.Timestamp](op, "ts")

				ct := mustExtract[bson.Timestamp](
					cursor.GetExtra()["$clusterTime"].Document(),
					"clusterTime",
				)

				lag = time.Second * time.Duration(int(ct.T)-int(ts.T))
			}
		}

		eventsMutex.Unlock()

		if err := cursor.GetNext(ctx); err != nil {
			return fmt.Errorf("reading oplog: %w", err)
		}
	}

	panic("cursor should never finish!")
}

func _runOplogMode(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	isSharded, err := isSharded(ctx, client)
	if err != nil {
		return fmt.Errorf("determining whether cluster is sharded: %w", err)
	}

	if isSharded {
		fmt.Println("Connection string refers to a mongos. Will try to connect to individual shards …")

		shardInfos, err := getShards(ctx, client)
		if err != nil {
			return fmt.Errorf("fetching shards’ connection strings: %w", err)
		}

		slices.SortFunc(
			shardInfos,
			func(a, b ShardInfo) int {
				return cmp.Compare(a.Name, b.Name)
			},
		)

		for _, shard := range shardInfos {
			fmt.Printf("Querying shard %#q’s oplog for write stats over the last %s (%s) …\n", shard.Name, interval, shard.ConnStr)

			shardClient, err := getClient(shard.ConnStr)

			if err != nil {
				return fmt.Errorf("creating connection to shard %#q (%#q): %w", shard.Name, shard.ConnStr, err)
			}

			err = printOplogStats(ctx, shardClient, interval)
			if err != nil {
				return fmt.Errorf("getting shard %s’s oplog stats: %w", shard.Name, err)
			}
		}

		return nil
	}

	fmt.Printf("Querying the oplog for write stats over the last %s …\n", interval)

	return printOplogStats(ctx, client, interval)
}

func printOplogStats(ctx context.Context, client *mongo.Client, interval time.Duration) error {
	coll := client.Database("local").Collection("oplog.rs")

	createPipeline := func() mongo.Pipeline {

		// NB: This query seems to run much faster when querying on a full
		// timestamp rather than just ts.t.
		unixTimeStart := uint32(time.Now().Add(-interval).Unix())

		return lo.Flatten([]mongo.Pipeline{
			{
				// Match timestamp
				{{"$match", bson.D{
					{"ts", bson.D{{"$gte", bson.Timestamp{T: unixTimeStart}}}},
				}}},
			},
			oplogUnrollFormatStages,
			{
				// Group by op type
				{{"$group", bson.D{
					{"_id", "$ops.op"},
					{"count", bson.D{{"$sum", 1}}},
					{"totalSize", bson.D{{"$sum", "$size"}}},
					{"minTs", bson.D{{"$min", "$ts"}}},
					{"maxTs", bson.D{{"$max", "$ts"}}},
				}}},

				// Final projection
				{{"$project", bson.D{
					{"op", "$_id"},
					{"count", 1},
					{"totalSize", 1},
					{"minTs", 1},
					{"maxTs", 1},
					{"_id", 0},
				}}},
			},
		})
	}

	cursor, err := coll.Aggregate(ctx, createPipeline())
	if err != nil {
		return fmt.Errorf("querying oplog: %w", err)
	}

	defer cursor.Close(ctx)

	var minTS, maxTS bson.Timestamp
	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}

	for cursor.Next(ctx) {
		decoded := struct {
			Op        string
			Count     int
			TotalSize int
			MinTS     bson.Timestamp
			MaxTS     bson.Timestamp
		}{}

		if err := cursor.Decode(&decoded); err != nil {
			return fmt.Errorf("decoding oplog aggregation: %w", err)
		}

		eventSizesByType[decoded.Op] = decoded.TotalSize
		eventCountsByType[decoded.Op] = decoded.Count

		if minTS.IsZero() {
			minTS = decoded.MinTS
		} else {
			minTS = lo.MinBy(
				sliceOf(minTS, decoded.MinTS),
				bson.Timestamp.Before,
			)
		}

		maxTS = lo.MaxBy(
			sliceOf(maxTS, decoded.MaxTS),
			bson.Timestamp.After,
		)
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("reading oplog aggregation: %w", err)
	}

	delta := time.Duration(1+maxTS.T-minTS.T) * time.Second

	displayTable(
		eventCountsByType,
		eventSizesByType,
		delta,
	)

	return nil
}

func getClient(connstr string) (*mongo.Client, error) {
	client, err := mongo.Connect(
		options.Client().ApplyURI(connstr),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to %#q: %w", connstr, err)
	}

	return client, nil
}

func _runChangeStream(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	unixTimeStart := uint32(time.Now().Add(-interval).Unix())
	startTS := bson.Timestamp{T: unixTimeStart}

	db := client.Database("admin")

	fmt.Printf("Gathering change events from the past %s …\n", interval)

	startTime := time.Now()

	resp := db.RunCommand(
		sctx,
		bson.D{
			{"aggregate", 1},
			{"cursor", bson.D{}},
			{"pipeline", mongo.Pipeline{
				{{"$changeStream", bson.D{
					{"allChangesForCluster", true},
					{"showSystemEvents", true},
					{"showExpandedEvents", true},
					{"startAtOperationTime", startTS},
				}}},
				{{"$match", bson.D{
					{"clusterTime", bson.D{
						{"$lte", bson.Timestamp{T: uint32(time.Now().Unix())}},
					}},
				}}},
				{{"$addFields", bson.D{
					{"operationType", "$$REMOVE"},
					{"op", bson.D{{"$cond", bson.D{
						{"if", bson.D{{"$in", [2]any{
							"$operationType",
							eventsToTruncate,
						}}}},
						{"then", bson.D{{"$substr",
							[3]any{"$operationType", 0, 1},
						}}},
						{"else", "$operationType"},
					}}}},
					{"size", bson.D{{"$bsonSize", "$$ROOT"}}},
				}}},
				{{"$project", bson.D{
					{"_id", 1},
					{"op", 1},
					{"size", 1},
					{"clusterTime", 1},
				}}},
			}},
		},
	)

	cursor, err := cursor.New(db, resp)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}

	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	var minUnixSecs, maxUnixSecs uint32

cursorLoop:
	for {
		if cursor.IsFinished() {
			return fmt.Errorf("unexpected end of change stream")
		}

		for _, event := range cursor.GetCurrentBatch() {
			t, _ := event.Lookup("clusterTime").Timestamp()

			if time.Unix(int64(t), 0).After(startTime) {
				break cursorLoop
			}

			if minUnixSecs == 0 {
				minUnixSecs = t
			}

			maxUnixSecs = t

			op := event.Lookup("op").StringValue()

			if fullOp, isShortened := fullEventName[op]; isShortened {
				op = fullOp
			}

			eventCountsByType[op]++
			eventSizesByType[op] += int(event.Lookup("size").AsInt64())
		}

		rt, hasToken := cursor.GetCursorExtra()["postBatchResumeToken"]
		if !hasToken {
			return fmt.Errorf("change stream lacks resume token??")
		}

		tokenTS, err := resumetoken.New(rt.Document()).Timestamp()
		if err != nil {
			return fmt.Errorf("parsing timestamp from change stream resume token")
		}

		if time.Unix(int64(tokenTS.T), 0).After(startTime) {
			break cursorLoop
		}

		if err := cursor.GetNext(sctx); err != nil {
			return fmt.Errorf("iterating change stream: %w", err)
		}
	}

	delta := time.Duration(1+maxUnixSecs-minUnixSecs) * time.Second

	displayTable(eventCountsByType, eventSizesByType, delta)

	return nil
}

func displayTable(
	eventCountsByType map[string]int,
	eventSizesByType map[string]int,
	delta time.Duration,
) {
	if delta == 0 {
		panic("nonzero delta is nonsensical!")
	}

	if len(eventCountsByType) == 0 {
		fmt.Printf("\tNo writes seen.\n")
		return
	}

	allEventsCount := lo.Sum(slices.Collect(maps.Values(eventCountsByType)))
	totalSize := lo.Sum(slices.Collect(maps.Values(eventSizesByType)))

	fmt.Printf(
		"\n%s ops/sec (%s/sec; avg: %s)\n",
		FmtReal((mmmath.DivideToF64(allEventsCount, delta.Seconds()))),
		FmtBytes(mmmath.DivideToF64(totalSize, delta.Seconds())),
		FmtBytes(mmmath.DivideToF64(totalSize, allEventsCount)),
	)

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Event",
		"Count",
		"Total Size",
		"Avg Size",
	})

	eventTypes := slices.Sorted(maps.Keys(eventCountsByType))

	for _, eventType := range eventTypes {
		countFraction := mmmath.DivideToF64(eventCountsByType[eventType], allEventsCount)
		sizeFraction := mmmath.DivideToF64(eventSizesByType[eventType], totalSize)

		lo.Must0(table.Append([]string{
			eventType,
			fmt.Sprintf(
				"%s (%s%%)",
				FmtReal(eventCountsByType[eventType]),
				FmtReal(100*countFraction),
			),
			fmt.Sprintf(
				"%s (%s%%)",
				FmtBytes(eventSizesByType[eventType]),
				FmtReal(100*sizeFraction),
			),
			FmtBytes(mmmath.DivideToF64(eventSizesByType[eventType], eventCountsByType[eventType])),
		}))
	}

	lo.Must0(table.Render())
}

func evacuateMap[K comparable, V any, M ~map[K]V](theMap M) {
	for k := range theMap {
		delete(theMap, k)
	}
}

func sliceOf[T any](els ...T) []T {
	return slices.Clone(els)
}

func _runChangeStreamLoop(ctx context.Context, connstr string, interval time.Duration) error {
	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("opening session: %w", err)
	}

	sctx := mongo.NewSessionContext(ctx, sess)

	cs, err := client.Watch(
		sctx,
		mongo.Pipeline{
			{{"$addFields", bson.D{
				{"operationType", "$$REMOVE"},
				{"op", agg.Cond{
					If:   agg.In("$operationType", eventsToTruncate...),
					Then: agg.SubstrBytes{"$operationType", 0, 1},
					Else: "$operationType",
				}},
				{"size", agg.BSONSize("$$ROOT")},
			}}},
			{{"$project", bson.D{
				{"_id", 1},
				{"op", 1},
				{"size", 1},
				{"clusterTime", 1},
			}}},
		},
		options.ChangeStream().
			SetCustomPipeline(bson.M{
				"showSystemEvents":   true,
				"showExpandedEvents": true,
			}),
	)
	if err != nil {
		return fmt.Errorf("opening change stream: %w", err)
	}
	defer cs.Close(sctx)

	fmt.Printf("Listening for change events. Stats showing every %s …\n", interval)

	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}
	eventCountsMutex := sync.Mutex{}
	var changeStreamLag uint32

	go func() {
		startTime := time.Now()

		for {
			time.Sleep(interval)

			eventCountsMutex.Lock()

			now := time.Now()
			delta := now.Sub(startTime)

			displayTable(eventCountsByType, eventSizesByType, delta)

			fmt.Printf("Change stream lag: %s\n", time.Duration(changeStreamLag)*time.Second)

			evacuateMap(eventCountsByType)
			evacuateMap(eventSizesByType)

			startTime = time.Now()

			eventCountsMutex.Unlock()
		}
	}()

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
	}

	for cs.Next(sctx) {
		op := cs.Current.Lookup("op").StringValue()

		if fullOp, isShortened := fullEventName[op]; isShortened {
			op = fullOp
		}

		eventCountsMutex.Lock()
		eventCountsByType[op]++
		eventSizesByType[op] += int(cs.Current.Lookup("size").AsInt64())

		sessTS, err := GetClusterTimeFromSession(sess)
		if err != nil {

		} else {
			eventT, _ := cs.Current.Lookup("clusterTime").Timestamp()

			changeStreamLag = sessTS.T - eventT
		}

		eventCountsMutex.Unlock()
	}
	if cs.Err() != nil {
		return fmt.Errorf("reading change stream: %w", cs.Err())
	}

	return fmt.Errorf("unexpected end of change stream")
}
