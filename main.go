package main

import (
	"context"
	"fmt"
	"log"
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

	// Build aggregation pipeline
	pipeline := mongo.Pipeline{
		// Stage 1: Filter relevant entries in last 5 min
		{{"$match", bson.D{
			{"$expr", bson.D{
				{"$and", bson.A{
					bson.D{
						{"$gte", bson.A{
							"$ts",
							bson.Timestamp{
								T: uint32(time.Now().Add(-10 * time.Second).Unix()),
							},
						}},
					},
					bson.D{
						{"$or", bson.A{
							bson.D{{"$in", bson.A{"$op", bson.A{"i", "u", "d"}}}},
							bson.D{
								{"$and", bson.A{
									bson.D{{"$eq", bson.A{"$op", "c"}}},
									bson.D{{"$eq", bson.A{"$ns", "admin.$cmd"}}},
									bson.D{{"$isArray", "$o.applyOps"}},
								}},
							},
						}},
					},
				}},
			}},
		}}},
		// Stage 2: Normalize to "ops" array
		{{"$addFields", bson.D{
			{"ops", bson.D{
				{"$cond", bson.D{
					{"if", bson.D{{"$eq", bson.A{"$op", "c"}}}},
					{"then", "$o.applyOps"},
					{"else", bson.A{"$$ROOT"}},
				}},
			}},
		}}},
		// Stage 3: Unwind ops
		{{"$unwind", "$ops"}},
		// Stage 4: Filter to relevant sub-ops
		{{"$match", bson.D{
			{"ops.op", bson.D{{"$in", bson.A{"i", "u", "d"}}}},
		}}},
		// Stage 5: Compute size of each op
		{{"$addFields", bson.D{
			{"size", bson.D{{"$bsonSize", "$ops"}}},
		}}},
		// Stage 6: Group by op type
		{{"$group", bson.D{
			{"_id", "$ops.op"},
			{"count", bson.D{{"$sum", 1}}},
			{"totalSize", bson.D{{"$sum", "$size"}}},
		}}},
		// Stage 7: Project clean output
		{{"$project", bson.D{
			{"op", "$_id"},
			{"count", 1},
			{"totalSize", 1},
			{"_id", 0},
		}}},
	}

	for {
		func() {

			fmt.Printf("starting agg\n")
			cursor, err := coll.Aggregate(ctx, pipeline)
			if err != nil {
				log.Fatalf("Aggregation failed: %v", err)
			}
			defer cursor.Close(ctx)

			// Print results
			fmt.Printf("reading agg\n")
			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					log.Fatal(err)
				}
				fmt.Printf("%+v\n", doc)
			}
			if err := cursor.Err(); err != nil {
				log.Fatal(err)
			}
		}()

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

	fullEventName := map[string]string{}
	for _, eventName := range eventsToTruncate {
		fullEventName[eventName[:1]] = eventName
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

	eventSizesByType := map[string]int64{}
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

			allEventsCount := lo.Sum(slices.Collect(maps.Values(eventCountsByType)))
			totalSize := lo.Sum(slices.Collect(maps.Values(eventSizesByType)))

			if allEventsCount > 0 {
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

					var fullEventType string

					if full, shortened := fullEventName[eventType]; shortened {
						fullEventType = full
					} else {
						fullEventType = eventType
					}

					table.Append([]string{
						fullEventType,
						FmtReal(eventCountsByType[eventType]),
						FmtBytes(eventSizesByType[eventType]),
						FmtReal(math.Round(100*countFraction)) + "%",
						FmtReal(math.Round(100*sizeFraction)) + "%",
					})
				}

				table.Render()

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

	for cs.Next(sctx) {
		op := cs.Current.Lookup("op").StringValue()

		eventCountsMutex.Lock()
		eventCountsByType[op]++
		eventSizesByType[op] += cs.Current.Lookup("size").AsInt64()

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

func evacuateMap[K comparable, V any, M ~map[K]V](theMap M) {
	for k := range theMap {
		delete(theMap, k)
	}
}
