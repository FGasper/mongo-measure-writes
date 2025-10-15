package main

import (
	"fmt"
	"slices"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func getClient(connstr string) (*mongo.Client, error) {
	client, err := mongo.Connect(
		options.Client().ApplyURI(connstr),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to %#q: %w", connstr, err)
	}

	return client, nil
}

func evacuateMap[K comparable, V any, M ~map[K]V](theMap M) {
	for k := range theMap {
		delete(theMap, k)
	}
}

func sliceOf[T any](els ...T) []T {
	return slices.Clone(els)
}
