package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/FGasper/mongo-measure-writes/agg"
	"github.com/FGasper/mongo-measure-writes/cursor"
	"github.com/FGasper/mongo-measure-writes/resumetoken"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
