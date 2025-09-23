package main

import (
	"context"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	mmmath "github.com/FGasper/mongo-measure-change-stream/math"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const statsInterval = 15 * time.Second

func main() {
	ctx := context.Background()

	if err := _run(ctx); err != nil {
		fmt.Fprint(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}

var eventsToTruncate = []string{
	"insert",
	"update",
	"replace",
	"delete",
}

func _runOplogMode(ctx context.Context, client *mongo.Client) error {
	coll := client.Database("local").Collection("oplog.rs")

	createPipeline := func() mongo.Pipeline {
		secondsAgo := uint32(time.Now().Add(-statsInterval).Unix())
		timestamp := bson.Timestamp{T: secondsAgo}

		return mongo.Pipeline{
			// Stage 1: Match timestamp >= now - 5 sec
			{{"$match", bson.D{
				{"ts", bson.D{{"$gte", timestamp}}},
			}}},
			// Stage 2: Match op in ["i", "u", "d"] or applyOps array
			{{"$match", bson.D{
				{"$or", bson.A{
					bson.D{
						{"op", bson.D{{"$in", bson.A{"i", "u", "d"}}}},
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
						{"then", "$o.applyOps"},
						{"else", bson.A{"$$ROOT"}},
					}},
				}},
			}}},
			// Stage 4: Unwind ops
			{{"$unwind", "$ops"}},
			// Stage 5: Match sub-ops of interest
			{{"$match", bson.D{
				{"ops.op", bson.D{{"$in", bson.A{"i", "u", "d"}}}},
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

	for {
		cursor, err := coll.Aggregate(ctx, createPipeline())
		if err != nil {
			return fmt.Errorf("querying oplog: %w", err)
		}

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
				_ = cursor.Close(ctx)
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
			_ = cursor.Close(ctx)
			return fmt.Errorf("reading oplog aggregation: %w", err)
		}

		delta := time.Duration(maxTS.T-minTS.T) * time.Second

		displayTable(
			eventCountsByType,
			eventSizesByType,
			delta,
		)

		_ = cursor.Close(ctx)

		time.Sleep(statsInterval)
	}
}

func _run(ctx context.Context) error {
	if len(os.Args) < 2 {
		return fmt.Errorf("give connection string first")
	}

	client, err := mongo.Connect(
		options.Client().ApplyURI(os.Args[1]),
	)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}

	if slices.Contains(os.Args, "--oplog") {
		return _runOplogMode(ctx, client)
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

	fmt.Printf("Listening for change events. Stats showing every %s …\n", statsInterval)

	eventSizesByType := map[string]int{}
	eventCountsByType := map[string]int{}
	eventCountsMutex := sync.Mutex{}
	var changeStreamLag uint32

	go func() {
		startTime := time.Now()

		for {
			time.Sleep(statsInterval)

			eventCountsMutex.Lock()

			now := time.Now()
			delta := now.Sub(startTime)

			if len(eventCountsByType) > 0 {
				displayTable(eventCountsByType, eventSizesByType, delta)

				fmt.Printf("Change stream lag: %s\n", time.Duration(changeStreamLag)*time.Second)

				evacuateMap(eventCountsByType)
				evacuateMap(eventSizesByType)
			} else {
				fmt.Printf("\t(No recent events seen …)\n")
			}

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

func displayTable(
	eventCountsByType map[string]int,
	eventSizesByType map[string]int,
	delta time.Duration,
) {
	allEventsCount := lo.Sum(slices.Collect(maps.Values(eventCountsByType)))
	totalSize := lo.Sum(slices.Collect(maps.Values(eventSizesByType)))

	fmt.Printf(
		"\n%s ops/sec (%s/sec; avg: %s)\n",
		FmtReal(math.Round((mmmath.DivideToF64(allEventsCount, delta.Seconds())))),
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
			FmtReal(math.Round(100*countFraction)) + "%",
			FmtReal(math.Round(100*sizeFraction)) + "%",
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
