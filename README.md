# mongo-measure-writes

This tool reports various statistics around a MongoDB cluster’s write load.

## Build Instructions

Just run `go build`. 🙂

## Oplog Mode

In its simplest form, this tool queries the oplog then prints a report, thus:
```
> ./mongo-measure-writes oplog 'mongodb://localhost:27017'

Querying the oplog for write stats over the last 1m0s …

23,517.04 ops/sec (15.06 MiB/sec; avg: 671.34 bytes)
┌────────────┬─────────┬───────────┬───────────────────┬──────────────────┐
│ EVENT TYPE │  COUNT  │   SIZE    │ %  OF TOTAL COUNT │ %  OF TOTAL SIZE │
├────────────┼─────────┼───────────┼───────────────────┼──────────────────┤
│ applyOps.d │ 222,911 │ 18.92 MiB │ 21.06%            │ 2.79%            │
│ d          │ 2       │ 256 bytes │ 0%                │ 0%               │
│ i          │ 472,554 │ 596.2 MiB │ 44.65%            │ 87.99%           │
│ u          │ 362,800 │ 62.42 MiB │ 34.28%            │ 9.21%            │
└────────────┴─────────┴───────────┴───────────────────┴──────────────────┘
```
`d`, `i`, and `u` refer to oplog delete, insert, and update/replace entries,
respectively. `applyOps.d` indicates delete entries nested inside `applyOps`
entries.

### Sharded Clusters

To read from a sharded cluster, this tool will try to connect to each
shard individually and create a report like the above.

This won’t work if the tool can’t connect to the shards via the connection
strings that the mongos reports. In this case, read from the change stream
instead.

## Change Stream Mode

This tool can also compile statistics by reading a change stream. This
tends to underperform direct oplog reads but works seamlessly with sharded
clusters.

Like oplog mode, this will compile statistics once then exit.

## Tail Change Stream Mode

You can also tail the change stream and report metrics as they arrive.
In this mode, the tool runs until stopped (e.g., via CTRL-C).

Note that, if the change stream lags the source, the reported write speed
will be lower than reality. For this reason, in this mode the tool also
reports change stream lag.
