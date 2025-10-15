/*
console.log(db.getSiblingDB("local").getCollection("oplog.rs").aggregate([
  {
    $match: { ts: {$gte: Timestamp( Math.floor((new Date().getTime() / 1000) - 5), 0)}},
  },
  { $match: {
    $or: [
        { op: { $in: ["i", "u", "d"] }},
        {
            op: "c",
            ns: "admin.$cmd",
            "o.applyOps": {$type: "array"},
        },
    ],
  } },
  {
    $addFields: {
      ops: {
        $cond: {
          if: { $eq: ["$op", "c"] },
          then: "$o.applyOps",
          else: ["$$ROOT"]
        }
      }
    }
  },
  { $unwind: "$ops" },
  {
    $match: {
      "ops.op": { $in: ["i", "u", "d"] }
    }
  },
  {
    $addFields: {
      size: { $bsonSize: "$ops" }
    }
  },
  {
    $group: {
      _id: "$ops.op",
      count: { $sum: 1 },
      totalSize: { $sum: "$size" }
    }
  },
  {
    $project: {
      op: "$_id",
      count: 1,
      totalSize: 1,
      _id: 0
    }
  }
]).toArray())
*/

function opAgg(opRef) {
    return {
        $cond: {
            if: {$eq: [opRef + ".op", "u"]},
            then: {
                $concat: [
                    opRef + ".op",
                    "/",
                    {$cond: {
                        if: {$eq: ["missing", {$type: opRef + ".o._id"}]},
                        then: "u",
                        else: "r",
                    }},
                ],
            },
            else: opRef + ".op",
        },
    }
}

let cursor = db.getSiblingDB("local").getCollection("oplog.rs").find(
  {
    $or: [
        {
            op: { $in: ["i", "u", "d"] },
        },
        {
            op: "c",
            ns: "admin.$cmd",
            "o.applyOps": {$type: "array"},
        },
    ],
  },
  {
    op: opAgg("$$ROOT"),
    size: {
        $cond: {
            if: { $eq: ["$op", "c"] },
            then: "$$REMOVE",
            else: { $bsonSize: "$$ROOT" },
        },
    },
    ops: {
        $cond: {
            if: { $eq: ["$op", "c"] },
            then: {
                $map: {
                    input: "$o.applyOps",
                    as: "opEntry",
                    in: {
                        size: {$bsonSize: "$$opEntry"},
                        op: opAgg("$$opEntry"),
                    },
                },
            },
            else: "$$REMOVE",
        },
    },
    ts: 1,
  },
);

cursor.tailable({awaitData: true});

while (true) {
    console.log(cursor.next());
}
