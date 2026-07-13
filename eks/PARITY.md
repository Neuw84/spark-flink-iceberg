# Flink vs Spark — identical envelope (fairness contract)

Following the spark-rtm-vs-flink discipline: *"every engine gets byte-for-byte the
same envelope."* A Spark-vs-Flink comparison is only valid if the two engines are
given identical compute, memory, parallelism, partitions, and commit cadence. This
is the contract both `jobs/flink.yaml` and `jobs/spark.yaml` are held to.

| Dimension | Flink | Spark | Match |
|---|---|---|---|
| **Work compute** | TaskManager 6 slots / **7 cores** | 1 executor × **7 cores** | ✅ |
| **Work memory** | TaskManager **14 GB** | executor **14 GB** | ✅ |
| **Coordinator** | JobManager 1 core / 2 GB | driver 1 core / 2 GB | ✅ |
| **Parallelism** | 6 | 6 (`default.parallelism` + `shuffle.partitions`) | ✅ |
| **Kafka partitions** | 6 (one per parallel task) | 6 | ✅ |
| **Node fit** | 7 cores ≤ 7.9 allocatable on m5.2xlarge | same | ✅ |
| **Commit interval** | checkpoint 60 s | `processingTime` trigger 60 s | ✅ |
| **Iceberg table** | same Glue table, `hash` dist, `bucket(16,user_id)` | same | ✅ |
| **Kafka source** | same Strimzi topic `events` | same | ✅ |
| **Producer load** | same `sfi-producer` fleet (10 pods × 15k = 150k rps) | same | ✅ |
| **Region/warehouse** | us-east-1 Glue + S3 | us-east-1 Glue + S3 | ✅ |
| **Image base** | flink 2.2.0 | spark 4.1.2 | (engine) |

## Deliberate, documented asymmetries (inherent to the engines, not the setup)

- **Checkpoint mechanism**: Flink async barrier vs Spark synchronous offset commit —
  this *is* the thing under test (RTM found Spark's synchronous commit stalls the
  data path → higher p99). Not a fairness bug.
- **Coordinator role**: Flink JM doesn't process data; Spark driver in cluster mode
  also mostly coordinates. Both sized identically (1c/2GB) and excluded from the
  "work" envelope.
- **Upsert path**: Flink native equality-deletes/DVs vs Spark `MERGE INTO`
  copy-on-write — again the thing under test, run on the same v3 table.

## How the numbers are measured (identical for both)

- **Source lag** = Flink `pendingRecords` metric / Spark read-position — the
  commit-independent "does it keep up" signal (NOT produced−committed).
- **CPU** = median over *active* samples (idle between micro-batches excluded).
- **Throughput / files / latency** = from the shared Iceberg snapshots.
