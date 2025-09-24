package main

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ShardInfo struct {
	Name    string
	ConnStr string
}

func isSharded(ctx context.Context, client *mongo.Client) (bool, error) {
	var result bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result)
	if err != nil {
		return false, err
	}

	if msg, ok := result["msg"]; ok && msg == "isdbgrid" {
		return true, nil
	}

	return false, nil
}

func getShards(ctx context.Context, client *mongo.Client) ([]ShardInfo, error) {
	shardsResp := client.Database("admin").
		RunCommand(ctx, bson.D{{"listShards", 1}})

	var shards struct {
		Shards []struct {
			Name string `bson:"_id"`
		}
	}
	if err := shardsResp.Decode(&shards); err != nil {
		return nil, fmt.Errorf("listing shards: %w")
	}

	var shardNames []string

	for _, shard := range shards.Shards {
		shardNames = append(shardNames, shard.Name)
	}

	shardMapResp := client.Database("admin").
		RunCommand(ctx, bson.D{{"getShardMap", 1}})

	var parsedShardMap struct {
		ConnStrings map[string]string
	}
	if err := shardMapResp.Decode(&parsedShardMap); err != nil {
		return nil, fmt.Errorf("getting shard map: %w", err)
	}

	shardMap := map[string]ShardInfo{}

	for connstr, shardName := range parsedShardMap.ConnStrings {
		if !slices.Contains(shardNames, shardName) {
			continue
		}

		info, exists := shardMap[shardName]
		if exists {
			if len(connstr) > len(info.ConnStr) {
				info.ConnStr = connstr
				shardMap[shardName] = info
			}
		} else {
			shardMap[shardName] = ShardInfo{
				Name:    shardName,
				ConnStr: connstr,
			}
		}
	}

	return slices.Collect(maps.Values(shardMap)), nil
}
