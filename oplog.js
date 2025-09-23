db.getSiblingDB("local").getCollection("oplog.rs").aggregate([
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
])
