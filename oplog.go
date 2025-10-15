package main

import (
	"github.com/FGasper/mongo-measure-writes/agg"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var eventsToTruncate = sliceOf(
	"insert",
	"update",
	"replace",
	"delete",
)

var oplogOps = sliceOf("i", "u", "d")

var oplogQuery = bson.D{
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
}

func makeOpFieldExpr(opRef string) bson.D {
	return agg.Cond{
		If: agg.In(opRef+".op", "u", "applyOps.u"),
		Then: agg.Concat(
			opRef+".op",
			"/",
			agg.Cond{
				If:   agg.Eq("missing", agg.Type(opRef+".o._id")),
				Then: "u",
				Else: "r",
			},
		),
		Else: opRef + ".op",
	}.D()
}

var oplogUnrollFormatStages = mongo.Pipeline{
	// Match op in ["i", "u", "d"] or applyOps array
	{{"$match", oplogQuery}},

	// Normalize to ops array
	{{"$addFields", bson.D{
		{"ops", bson.D{
			{"$cond", bson.D{
				{"if", bson.D{{"$eq", bson.A{"$op", "c"}}}},
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

	// Unwind ops
	{{"$unwind", "$ops"}},

	// Match sub-ops of interest
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

	// Add BSON size per op, and append either /u or /r to op:u.
	{{"$addFields", bson.D{
		{"size", bson.D{{"$bsonSize", "$ops"}}},
		{"ops.op", makeOpFieldExpr("$ops")},
	}}},
}
