# spark-flink-iceberg

**A reproducible, open-source benchmark of Apache Spark 4.1 vs Apache Flink 2.2
streaming into Apache Iceberg** — who keeps up under a fixed resource budget, what
it costs in CPU and memory, how healthy the files are, and how the connectors'
upsert paths behave. Runs on a laptop with Docker, then identically on Amazon EKS.

Companion to the Medium post:
**[`blog/spark-vs-flink-iceberg.md`](blog/spark-vs-flink-iceberg.md)**
(self-contained HTML: [`blog/spark-vs-flink-iceberg.html`](blog/spark-vs-flink-iceberg.html)).

## The core idea

Most Spark-vs-Flink benchmarks measure Kafka→Kafka per-record latency. Real teams
are building **Kafka→Iceberg lakehouse ingestion**, which behaves differently: an
Iceberg sink only makes data visible at **commit boundaries**, so the commit
interval — not the engine's record path — dominates latency. The interesting
question becomes: at a fixed, *equal* resource envelope and a fixed input rate,
**does the engine keep up or fall behind?** And what does keeping up cost?

Measurement is commit-honest and engine-agnostic: backlog = `kafka_produced −
iceberg_snapshot_total_records`; file sizes come from the Iceberg snapshot summary.
No consumer-offset or engine-internal metric that would flatter one side.

## Headline result (30-min, 100k rows/s, 14 cores / 28 GB per engine)

| Cell | Committed rows/s | Verdict | Peak mem |
|---|---|---|---|
| Flink append | ~99k | **keeps up** | ~17 GB |
| Spark append | ~99k | **keeps up** | ~20 GB |
| Flink upsert (deletion vectors) | ~98k | **keeps up** | ~17 GB |
| Spark upsert (merge-on-read) | ~37k | **falls behind** | ~31 GB |
| Spark upsert (copy-on-write) | ~3k | **falls behind** | ~30 GB |

Append is a near-tie (Spark costs more memory); **upsert is where they diverge** —
Flink's deletion-vector upsert keeps up, both Spark merge modes fall behind. Full
analysis, charts, and the local (50k / 7-core) run that mirrors this are in the post.

## Engines & APIs

| Engine | Job benchmarked |
|---|---|
| **Flink 2.2** | DataStream `IcebergSink` (Java) — also SQL, PyFlink, `DynamicIcebergSink` demos |
| **Spark 4.1** | Structured Streaming (Scala), micro-batch — append + `MERGE INTO` upsert (MoR & CoW) |

Iceberg **1.11.0** (v2 equality deletes, v3 deletion vectors), REST catalog locally
/ AWS Glue on EKS.

> **Why no Spark RTM?** Real-Time Mode is Databricks-only (not in OSS Spark 4.1),
> and for a *table* sink the latency floor is the commit interval regardless of
> trigger — micro-batch is the honest apples-to-apples path.

## Quick start — local (laptop, ~10 min, free)

```bash
docker compose up -d                       # Kafka, MinIO, Iceberg REST, Flink, Spark
python -m venv .venv && .venv/bin/pip install -r bench/requirements.txt

RUN_SECONDS=300 bash bench/run_local_bench.sh flink append   # one cell
bash bench/run_local_matrix.sh                               # full 5-cell matrix
python bench/analyze_matrix.py results/local local_ "LOCAL"  # table
python bench/make_charts.py                                  # charts → docs/charts/
```

Flink UI → http://localhost:8081, Spark UI → http://localhost:4040.

## EKS at scale (from an empty account)

The `eks/` directory is a standalone numbered runbook — it provisions a **fresh
VPC** and cluster; nothing assumes pre-existing infrastructure. This path is tested
end-to-end from a clean account.

```bash
cd eks
# set ACCOUNT_EXPECTED in 00_profile.env to your AWS account id, then:
source 00_profile.env
bash 01_create_cluster.sh      # eksctl: VPC + 6× m5.2xlarge, KMS, OIDC, IMDSv2
bash 02_install.sh             # S3 + Glue, EBS CSI/gp3, Strimzi Kafka, Flink+Spark operators
bash 02b_irsa.sh               # least-privilege IRSA (S3 + Glue + KMS)
bash 03_build_push_images.sh   # build & push images to ECR
bash 05_run_matrix_30min.sh    # the full 5-cell matrix (or 04_run_benchmark.sh for one)
bash 99_teardown.sh            # deletes EVERYTHING (cluster ~$2.40/hr — don't skip)
```

Fairness contract (identical cores, memory, parallelism, partitions, buckets,
commit interval) is enforced in the job manifests and documented in
[`eks/PARITY.md`](eks/PARITY.md).

## Repo layout

```
.env                  versions + workload knobs (sourced everywhere)
docker-compose.yml    local stack: Kafka, MinIO, Iceberg REST, Flink, Spark
common/producer.py    latency-stamped Kafka event producer
flink/datastream/     Java IcebergSink job (+ SQL, PyFlink, DynamicSink demos)
spark/scala/          Structured Streaming Iceberg job (append + MERGE upsert)
bench/                run scripts (local), commit-honest analysis, chart generation
eks/                  numbered EKS runbook (01→05, 99 teardown) + job manifests
docs/                 feature-comparison.md + published charts
blog/                 the Medium post (md + standalone html)
results/              run outputs — summaries committed, raw CSVs gitignored
```

## Feature comparison

See [`docs/feature-comparison.md`](docs/feature-comparison.md). Headline: **Flink**
leads on connector surface (native upsert, deletion vectors, dynamic multi-table
sink); **Spark** is simpler (`MERGE INTO`, `writeStream.toTable()`) but its upsert
paths don't keep up at high rate.

## Versions

Pinned in [`.env`](.env): Flink 2.2.0, Spark 4.1.2, Iceberg 1.11.0, Java 17,
Scala 2.13.

## License

Apache-2.0 — all dependencies are open source.
