# Event schema

A single JSON event models a retail purchase/cart event. It is intentionally
small (~500 bytes serialized) so that 100k records/s ≈ 50 MB/s, matching the
target ingest rate for the benchmark.

## v1 (baseline)

| field           | type            | notes                                             |
|-----------------|-----------------|---------------------------------------------------|
| `event_id`      | string (uuid)   | unique per event                                  |
| `user_id`       | long            | key used for hash distribution / upsert identity  |
| `event_type`    | string          | `purchase` \| `cart` \| `view`                    |
| `product_id`    | long            |                                                   |
| `country`       | string          | ISO code, lowercased at source                    |
| `amount`        | double          | > 0 for purchase/cart                             |
| `currency`      | string          |                                                   |
| `quantity`      | int             |                                                   |
| `event_ts`      | long (epoch ms) | logical event time                                |
| `ingest_ts`     | long (epoch ms) | **stamped by producer** — used for E2E latency    |
| `payload`       | string          | filler to reach target payload size               |

## v2 (schema evolution variant)

Adds two columns to exercise connector schema-evolution handling:

| field            | type    | notes                          |
|------------------|---------|--------------------------------|
| `loyalty_tier`   | string  | **added** column               |
| `session_id`     | string  | **added** column               |

The producer can be told to emit a mix of v1 and v2 records mid-run
(`--evolve-after N`) so we can observe how each engine reacts to a new column
appearing in the stream (auto-add vs. drop vs. fail).

## Iceberg target table

```sql
CREATE TABLE demo.streaming.events (
  event_id     string,
  user_id      bigint,
  event_type   string,
  product_id   bigint,
  country      string,
  amount       double,
  currency     string,
  quantity     int,
  event_ts     bigint,
  ingest_ts    bigint,
  payload      string
) USING iceberg
PARTITIONED BY (bucket(16, user_id))
TBLPROPERTIES (
  'format-version' = '2',
  'write.distribution-mode' = 'hash'
);
```

## Latency definition

End-to-end latency = `iceberg_commit_wallclock - ingest_ts`, measured by a
reader that scans committed snapshots and compares the max `ingest_ts` in each
new snapshot against the snapshot's commit timestamp. Because Iceberg is
commit-oriented, visible latency is bounded below by the commit/checkpoint
interval — this is the central methodological point of the benchmark.
