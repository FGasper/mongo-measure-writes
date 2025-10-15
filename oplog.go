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

var oplogOpsWithApplyOpsPrefix = lo.Flatten(
	sliceOf(
		oplogOps,
		lo.Map(
			oplogOps,
			func(op string, _ int) string { return "applyOps." + op },
		),
	),
)

func makeOplogSingleOpFilterExpr(opRef string, opsToAccept []string) any {
	return agg.And{
		agg.In(opRef+".op", opsToAccept...),
		agg.Not{agg.Eq(
			agg.SubstrBytes{opRef + ".ns", 0, 7},
			"config.",
		)},
	}
}

var oplogQueryExpr = agg.Or{
	makeOplogSingleOpFilterExpr("$$ROOT", oplogOps),
	agg.And{
		agg.Eq("$op", "c"),
		agg.Eq("$ns", "admin.$cmd"),
		agg.Eq(agg.Type("$o.applyOps"), "array"),
	},
}

// This appends /u or /r to “u” oplog events.
func makeSuffixedOpFieldExpr(opRef string) bson.D {
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
	{{"$match", bson.D{{"$expr", oplogQueryExpr}}}},

	// Normalize to ops array
	{{"$addFields", bson.D{
		{"ops", agg.Cond{
			If: agg.Eq("$op", "c"),
			Then: agg.Map{
				Input: "$o.applyOps",
				As:    "opEntry",
				In: agg.MergeObjects{
					"$$opEntry",
					bson.D{
						{"op", agg.Concat(
							"applyOps.",
							"$$opEntry.op",
						)},
					},
				},
			},
			Else: bson.A{"$$ROOT"},
		}},
	}}},

	// Unwind ops
	{{"$unwind", "$ops"}},

	// Match sub-ops of interest
	{{"$match", bson.D{
		{"$expr", makeOplogSingleOpFilterExpr("$ops", oplogOpsWithApplyOpsPrefix)},
	}}},

	// Add BSON size per op, and append either /u or /r to op:u.
	{{"$addFields", bson.D{
		{"size", bson.D{{"$bsonSize", "$ops"}}},
		{"ops.op", makeSuffixedOpFieldExpr("$ops")},
	}}},
}
