package main

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

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
				Name:    "tailchangestream",
				Aliases: sliceOf("tcs"),
				Usage:   "measure via change stream (continually)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runTailChangeStream(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}

var eventsToTruncate = sliceOf(
	"insert",
	"update",
	"replace",
	"delete",
)

var oplogOps = sliceOf("i", "u", "d")

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

		return mongo.Pipeline{
			// Stage 1: Match timestamp >= now - 5 sec
			{{"$match", bson.D{
				{"ts", bson.D{{"$gte", bson.Timestamp{T: unixTimeStart}}}},
			}}},
			// Stage 2: Match op in ["i", "u", "d"] or applyOps array
			{{"$match", bson.D{
				{"$or", bson.A{
					bson.D{
						{"op", bson.D{{"$in", oplogOps}}},
					},
					bson.D{
						{"op", "c"},
						{"ns", "admin.$cmd"},
						{"o.applyOps", bson.D{{"$type", "array"}}},
					},
				}},
			}}},
			// Stage 3: Normalize to ops array
			{{"$addFields", bson.D{
				{"ops", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{{"$eq", bson.A{"$op", "c"}}}},
						//{"then", "$o.applyOps"},
						{"then", bson.D{
							{"$map", bson.D{
								{"input", "$o.applyOps"},
								{"as", "opEntry"},
								{"in", bson.D{
									{"$mergeObjects", bson.A{
										"$$opEntry",
										bson.D{
											{"op", bson.D{
												{"$concat", bson.A{
													"applyOps.",
													"$$opEntry.op",
												}},
											}},
										},
									}},
								}},
							}},
						}},
						{"else", bson.A{"$$ROOT"}},
					}},
				}},
			}}},
			// Stage 4: Unwind ops
			{{"$unwind", "$ops"}},
			// Stage 5: Match sub-ops of interest
			{{"$match", bson.D{
				{"ops.op", bson.D{{"$in", lo.Flatten(
					sliceOf(
						oplogOps,
						lo.Map(
							oplogOps,
							func(op string, _ int) string { return "applyOps." + op },
						),
					),
				),
				}}},
			}}},
			// Stage 6: Add BSON size per op
			{{"$addFields", bson.D{
				{"size", bson.D{{"$bsonSize", "$ops"}}},
			}}},
			// Stage 7: Group by op type
			{{"$group", bson.D{
				{"_id", "$ops.op"},
				{"count", bson.D{{"$sum", 1}}},
				{"totalSize", bson.D{{"$sum", "$size"}}},
				{"minTs", bson.D{{"$min", "$ts"}}},
				{"maxTs", bson.D{{"$max", "$ts"}}},
			}}},
			// Stage 8: Final projection
			{{"$project", bson.D{
				{"op", "$_id"},
				{"count", 1},
				{"totalSize", 1},
				{"minTs", 1},
				{"maxTs", 1},
				{"_id", 0},
			}}},
		}
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
		"Event Type",
		"Count",
		"Size",
		"% of total count",
		"% of total size",
	})

	eventTypes := slices.Sorted(maps.Keys(eventCountsByType))

	for _, eventType := range eventTypes {
		countFraction := mmmath.DivideToF64(eventCountsByType[eventType], allEventsCount)
		sizeFraction := mmmath.DivideToF64(eventSizesByType[eventType], totalSize)

		table.Append([]string{
			eventType,
			FmtReal(eventCountsByType[eventType]),
			FmtBytes(eventSizesByType[eventType]),
			FmtReal(100*countFraction) + "%",
			FmtReal(100*sizeFraction) + "%",
		})
	}

	table.Render()
}

func evacuateMap[K comparable, V any, M ~map[K]V](theMap M) {
	for k := range theMap {
		delete(theMap, k)
	}
}

func sliceOf[T any](els ...T) []T {
	return slices.Clone(els)
}

func _runTailChangeStream(ctx context.Context, connstr string, interval time.Duration) error {
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
