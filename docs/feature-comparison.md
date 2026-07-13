# Iceberg connector feature comparison: Spark vs Flink

This is the qualitative half of the benchmark — what each engine's Iceberg
connector can actually *do*, independent of how fast it does it. Versions:
**Spark 4.1.2 + iceberg-spark-runtime 1.11.0**, **Flink 2.2.0 +
iceberg-flink-runtime 1.11.0**, Iceberg table format **v2/v3**.

## At a glance

| Capability | Spark (Structured Streaming) | Flink (DataStream / SQL) |
|---|---|---|
| **Write distribution modes** | `none`, `hash`, `range` via `write.distribution-mode` table prop; `fanout-enabled` to skip the pre-sort | `none`, `hash`, `range` via table prop **or** `.distributionMode()` on `IcebergSink`; hash is the default for partitioned tables |
| **Streaming append** | `writeStream.format("iceberg").toTable()` (DSv2 writer) | `IcebergSink` (SinkV2) / `INSERT INTO` |
| **Upsert / CDC** | `MERGE INTO` in `foreachBatch` (copy-on-write or merge-on-read) | Native `.upsert(true)` + `equalityFieldColumns`, or SQL `write.upsert.enabled` |
| **Equality vs positional deletes** | Positional (CoW) or MoR; equality deletes not produced by streaming append | Emits **equality deletes** for upserts; **deletion vectors** on v3 (Iceberg 1.11) |
| **Dynamic / multi-table sink** | No first-class API — demux with N `writeStream`s or route inside `foreachBatch` | `DynamicIcebergSink` (routes + creates + evolves tables at runtime); SQL `StatementSet` for fixed fan-out |
| **Schema evolution (in-stream)** | New columns require table alter; `mergeSchema` on write handles adds | Auto add/evolve with `DynamicIcebergSink` (`immediateTableUpdate`); SQL needs explicit DDL |
| **Commit trigger** | Micro-batch `Trigger.ProcessingTime` (commit = batch boundary) | Checkpoint (commit = checkpoint completion) |
| **Exactly-once** | Yes — offsets in Spark checkpoint, atomic Iceberg commit | Yes — two-phase commit tied to Flink checkpoints |
| **In-job maintenance** | External (`rewrite_data_files`, `expire_snapshots` via Spark procedures) | Optional in-job compaction / `RewriteDataFiles`, or external |
| **Backpressure** | Micro-batch pull (`maxOffsetsPerTrigger`) | Continuous credit-based backpressure |

## Write distribution modes — what actually changes

The `write.distribution-mode` controls how records are shuffled across writer
tasks *before* files are written. It is the single biggest lever on the
small-files problem:

- **`none`** — every writer task writes every partition it sees. Lowest shuffle
  cost, highest file count (tasks × partitions per commit). Good for few
  partitions, terrible for high-cardinality partitioning.
- **`hash`** — records are hash-partitioned on the partition key so each
  partition is written by one task. Far fewer files; a shuffle cost. Default for
  partitioned tables in both engines.
- **`range`** — range-partitioned via sampling; best clustering and file sizes,
  highest cost. Useful when you also sort within partitions.

Both engines honor the table property. Flink additionally lets you set it
imperatively on the `IcebergSink` builder; Spark exposes `fanout-enabled` to
avoid the local sort that `none`/`hash` would otherwise require, trading memory
for skipping a sort. **The benchmark measures file count / avg-rows-per-file per
mode so the small-files tradeoff is quantified, not just described.**

## Dynamic sinks — the clearest divergence

Flink's `DynamicIcebergSink` accepts a stream of records that each declare their
own target table + schema at runtime. One operator can fan a stream into an
open-ended set of tables, creating and schema-evolving them on the fly. This is
genuinely hard to replicate in Spark Structured Streaming: you either

1. start one `writeStream` per known table (fixed set, N checkpoints), or
2. route inside `foreachBatch` and issue per-target writes imperatively (you own
   idempotency and schema management).

For a fixed, known set of targets, Flink SQL `StatementSet` and Spark's
per-table streams are comparable. For an *unbounded/evolving* set, Flink has a
real capability Spark lacks out of the box. The repo demonstrates both
(`DynamicSinkJob`, `multi_table_routing.sql`).

## Schema evolution behavior (v1 → v2 events mid-stream)

The producer can start injecting v2 events (two extra columns) partway through a
run (`--evolve-after`). Observed handling:

- **Flink `DynamicIcebergSink`** (`immediateTableUpdate=true`): detects the new
  columns and evolves the target table without a restart.
- **Flink SQL / static `IcebergSink`**: schema is fixed at job start; extra JSON
  fields are ignored (`json.ignore-parse-errors`) unless DDL is altered.
- **Spark append with `mergeSchema`**: new columns are added on write; without
  it, extra fields are dropped by the `from_json` schema.

## Upsert / CDC and deletes

- **Flink** produces equality deletes for upserts natively and, on **format v3**
  with Iceberg 1.11, writes **deletion vectors** (compact positional deletes),
  which materially reduces read amplification versus v2 equality deletes.
- **Spark** streaming upserts go through `MERGE INTO` in `foreachBatch`
  (copy-on-write by default; merge-on-read configurable). Simpler mental model,
  heavier per-batch rewrite under CoW.

The benchmark runs both engines in `--upsert` mode against v2 (equality deletes)
and v3 (deletion vectors) tables to compare write amplification and the
resulting read cost.
