# Flink vs Spark — identical envelope (fairness contract)

Following the spark-rtm-vs-flink discipline: *"every engine gets byte-for-byte the
same envelope."* A Spark-vs-Flink comparison is only valid if the two engines are
given identical compute, memory, parallelism, partitions, GC, and commit cadence.
This is the contract both `jobs/flink.yaml` and `jobs/spark.yaml` are held to.

| Dimension | Flink | Spark | Match |
|---|---|---|---|
| **Work compute** | 2 TaskManagers × **2 cores** (6 slots each — heavily oversubscribed) | 2 executors × **2 cores** | ✅ 4 cores total (SMALL profile; was 7c/16GB originally, see below) |
| **Work memory** | 2 TaskManagers × **4 GB** process mem | 2 executors × **4 GB** (3 GB heap + 1 GB overhead) | ✅ 8 GB total |
| **GC** | **G1** (`env.java.opts.taskmanager`) same pause/occupancy targets | **G1** (`spark.executor.extraJavaOptions`) same targets | ✅ |
| **Coordinator** | JobManager 1 core / 2 GB | driver 1 core / 2 GB | ✅ |
| **Parallelism** | 12 | 12 (`default.parallelism`) | ✅ |
| **Kafka partitions** | 12 (one per parallel task) | 12 | ✅ |
| **Kafka consumer fetch** | explicit tuned profile (`.setProperty`) | identical profile (`kafka.*` options) | ✅ |
| **Commit interval** | checkpoint 60 s | `processingTime` trigger 60 s | ✅ |
| **Iceberg table** | same Glue table, `hash` dist, `bucket(N=BUCKETS, user_id)` | same | ✅ |
| **Kafka source** | same Strimzi topic `events` | same | ✅ |
| **Producer load** | same `sfi-producer` fleet | same | ✅ |
| **Region/warehouse** | us-east-1 Glue + S3 | us-east-1 Glue + S3 | ✅ |
| **Engine / language** | Flink 2.2.0 DataStream (Java) | Spark 4.1.2 (PySpark) | (engine) |

### Kafka consumer fetch profile (identical on both)

Neither engine tunes the Kafka *fetch* path by default — both inherit stock Kafka
client defaults (`fetch.min.bytes=1`, `max.partition.fetch.bytes=1 MiB`,
`fetch.max.bytes=50 MiB`, `receive.buffer.bytes=64 KiB`, `max.poll.records=500`).
(Verified against the flink-connector-kafka source: `KafkaSourceBuilder` /
`KafkaPartitionSplitReader` set none of these.) We set the same tuned values on
both, env-overridable, so fetch size is not a hidden variable:

| key | value |
|---|---|
| `fetch.min.bytes` | 1 MiB |
| `fetch.max.wait.ms` | 500 |
| `max.partition.fetch.bytes` | 8 MiB |
| `fetch.max.bytes` | 64 MiB |
| `receive.buffer.bytes` | 2 MiB |
| `max.poll.records` | 5000 |

The **producer** (`common/producer.py`, `linger.ms`/`batch.size`/lz4/acks) is a
single shared fleet feeding one topic, so it is identical for both by construction.

## Small profile (2 cores / 4 GB per worker container)

A deliberately tight envelope, well past the original 7c/16GB sizing above, to see
where each engine's ceiling sits under real pressure: **2 cores / 4 GB per worker
container** (2 workers each → 4 cores / 8 GB total per engine). Kafka partitions
(12) and job parallelism (12) were **not** reduced to match, so both engines are
heavily oversubscribed relative to real cores — that's a deliberate stress
condition, not a bug. Flink's `taskmanager.numberOfTaskSlots` stays at 6 per TM (12
total) because Flink requires total slots >= job parallelism to schedule at all;
this means 6 slots now share 2 cores (3x oversubscribed at the slot level, on top
of the Kafka-partition oversubscription). G1's
`MaxGCPauseMillis`/`InitiatingHeapOccupancyPercent` flags are unchanged since
they're target/percentage-based, not absolute heap sizes, so they remain correct
at the smaller heap.

Prior data points for reference (backed up under `results/eks/*.2c4gb.*`,
`*.3c4gb.*` and `*.4c5gb.*`): at these same 2 cores/TM, an earlier pass already
showed Flink genuinely falling behind — input landed dropped to ~71k/s against
100k rps producers, Writer busy jumped to ~45%, and the post-commit lag floor grew
steadily (1.4M -> 13.9M rows over ~200s) instead of holding flat. At 3 cores/TM
both engines kept up; at 4 cores/TM (and the original 7 cores) both coasted with
lag flat.

### Correction: trigger interval vs. batch execution time, and Flink's "commit"

Two framing errors from an earlier pass, corrected here:

1. **Spark's trigger interval is a genuine 60s** (`--trigger 60` in the manifest,
   confirmed by ~6,000,000 rows/batch at ~100k rows/s sustained input =
   6,000,000/100,000 = 60s of accumulated data per batch). `triggerExecution_ms`
   in `batches.csv` is NOT the interval between triggers — it's how long that one
   micro-batch took to *execute*. With `Trigger.ProcessingTime` there is no
   overlap, so the real cycle time IS `triggerExecution_ms`, and Spark starts the
   next batch immediately after, it does not sit idle for the remainder of a fixed
   60s slot. So "addBatch is ~93% busy of the 60s trigger" is the wrong framing —
   the correct reading is that Spark processes 60s of accumulated input in
   ~triggerExecution_ms of wall-clock work (i.e., it runs faster than real-time by
   that ratio), which is a *headroom* signal, not a *saturation* signal.
2. **Flink's checkpoint duration is not the Iceberg flush/commit time.** Flink
   writes continuously and only pauses to align a checkpoint barrier + snapshot
   its (here, tiny) keyed state; the Iceberg commit itself happens in
   `notifyCheckpointComplete` and is not captured by the checkpoint
   `end_to_end_duration` pulled from the JobManager REST API. Treating that
   ~1.1-1.9s figure as "Flink's per-cycle write cost" understates it — it is
   the barrier/state-snapshot cost only, not the flush-to-Iceberg cost. Getting a
   true per-commit Iceberg flush time for Flink requires reading the sink
   operator's own commit metrics or diffing snapshot timestamps, not the generic
   checkpoint history.

## Deliberate, documented asymmetries (inherent to the engines, not the setup)

- **Checkpoint mechanism**: Flink async barrier vs Spark synchronous offset commit —
  this *is* the thing under test. Not a fairness bug.
- **Adaptive Query Execution (AQE)**: Spark 4.1 applies AQE (coalesce shuffle
  partitions to a 128 MB advisory size) to *stateless* streaming micro-batches; the
  append is stateless, so AQE shapes Spark's write shuffle. Flink has no direct
  equivalent — its keyed hash distribution to the sink is always on. AQE is a Spark
  capability we deliberately enable and exercise, not an unfair head start; both
  engines are given the same 128 MB target file size.
- **Async progress tracking**: evaluated for the Spark side and **rejected** — it
  drops end-to-end exactly-once (offset ranges can change on failure), which would
  double-append to Iceberg on recovery and corrupt the commit-honest row counts.
  Left disabled so both engines keep exactly-once.
- **Coordinator role**: Flink JM doesn't process data; Spark driver in cluster mode
  also mostly coordinates. Both sized identically (1c/2GB) and excluded from the
  "work" envelope.
- **Upsert path**: Flink native equality-deletes/DVs vs Spark `MERGE INTO`
  (MoR or CoW) — again the thing under test, run on the same v3 table.

## How the numbers are measured (identical for both)

- **Keep-up / lag** = Kafka topic end-offsets (produced) − Iceberg current-snapshot
  `total-records` (landed), sampled over the window. Flat/shrinking = keeps up;
  growing = falling behind. Read from the shared table metadata → engine-agnostic.
- **Memory**: with the **binding 16 GB total cap** (both engines), `kubectl top`
  working-set now reflects *demand under a fixed equal budget* rather than greedy
  fill of an oversized envelope. The honest comparison at that budget is **keep-up
  (lag slope) + GC behaviour**, not raw RSS — both run identical G1 settings.
- **Throughput / files / commit timing** = from the shared Iceberg snapshots.
