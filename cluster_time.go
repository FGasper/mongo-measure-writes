package main

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func GetClusterTimeFromSession(sess *mongo.Session) (bson.Timestamp, error) {
	ctStruct := struct {
		ClusterTime struct {
			ClusterTime bson.Timestamp `bson:"clusterTime"`
		} `bson:"$clusterTime"`
	}{}

	clusterTimeRaw := sess.ClusterTime()
	err := bson.Unmarshal(sess.ClusterTime(), &ctStruct)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("finding clusterTime in session cluster time document (%v): %w", clusterTimeRaw, err)
	}

	return ctStruct.ClusterTime.ClusterTime, nil
}
