# Part 2 — Spark vs Flink → Iceberg at scale on Amazon EKS

Reproducible, numbered runbook (same shape as the `spark-rtm-vs-flink` EKS setup).
The identical engine jobs from Part 1 run here; only the **bindings** change:
MinIO → **S3**, Iceberg REST catalog → **AWS Glue**, single-node Kafka → **Strimzi**,
`local[N]` / single TM → a real **6-node cluster**. Commits stay at **1 minute**.

> 💸 **This provisions billable AWS infrastructure** (EKS + 6× m5.2xlarge + S3 +
> Glue). Nothing runs until *you* run the numbered scripts. `99_teardown.sh` stops
> all costs.

## Safety: dedicated profile + kubeconfig

You have another EKS cluster in context. **Every script sources
[`00_profile.env`](00_profile.env)**, which pins a dedicated `AWS_PROFILE`
(`sfi-iceberg-bench`) and a dedicated `KUBECONFIG` (`eks/kubeconfig`) — so nothing
here ever touches your shared `~/.kube/config` or default AWS profile.
`01_create_cluster.sh` prints the resolved account and asks you to confirm it's the
right one before doing anything.

## Runbook

```bash
cd eks
# 0. set your profile name in 00_profile.env (default: sfi-iceberg-bench)
#    and fill the <YOUR_*> placeholders in cluster.yaml (KMS key, VPC subnets).

./01_create_cluster.sh        # eksctl create cluster  (~15-20 min)
./02_install.sh               # Strimzi + Flink/Spark operators + S3 bucket + Glue db
./02b_irsa.sh                 # least-privilege IRSA → S3 warehouse + Glue
TAG=v1 ../eks/03_build_push_images.sh   # build+push flink/spark/load images (amd64)

# Run the matrix (each: deploy engine → fan out producers → measure lag/commits → clean up)
WRITE=append PART=bucket ./04_run_benchmark.sh flink 8 10 15000
WRITE=append PART=bucket ./04_run_benchmark.sh spark 8 10 15000
WRITE=append PART=time   ./04_run_benchmark.sh flink 8 10 15000
WRITE=append PART=time   ./04_run_benchmark.sh spark 8 10 15000
WRITE=upsert PART=bucket ./04_run_benchmark.sh flink 8 10 15000
WRITE=upsert PART=bucket ./04_run_benchmark.sh spark 8 10 15000

./99_teardown.sh              # delete cluster + IAM (+ optionally S3/Glue). STOPS COSTS.
```

## Why the workload is sized the way it is

Part 1 found that on a laptop, a *stateless append* is **sink-bound** — the
Iceberg-commit + object-store write is the bottleneck, so both engines hit the
same ~385k rows/s ceiling and neither falls behind at 100k. At cluster scale the
sink ceiling is much higher (parallel writers, real S3), so the **engine's ability
to keep its Kafka consumer lag flat under sustained pressure** becomes the real
differentiator. The producer fleet here is sized so **aggregate input exceeds what
a single host could push** (10 pods × 15k = 150k rps), keeping the sources hot and
forcing each engine to prove it keeps up — data still lands each minute, but the
question is whether lag stays flat or grows.

## What Part 2 measures (beyond Part 1)

- **Sustained lag** at 150k rps: does the Kafka consumer group stay flat (keeps up)
  or grow (falling behind)? — the honest "faster" metric.
- **Per-pod CPU/mem** from `kubectl top` / Container Insights (real per-second
  accounting, not the idle-averaged local `docker stats`).
- **Commit cost against real S3** — the durable-checkpoint tail that MinIO hides.
- **$ / billion records** each engine × mode, from instance-hours.
- **Recovery** after killing a TaskManager / executor pod.
- **Upsert read cost**: v3 deletion vectors (Flink) vs copy-on-write MERGE (Spark).

## Files

```
00_profile.env          dedicated AWS_PROFILE + KUBECONFIG (safety)
cluster.yaml            eksctl manifest (KMS/OIDC/IMDSv2, node group)
01_create_cluster.sh    create the cluster
02_install.sh           Strimzi + operators + S3 bucket + Glue db
02b_irsa.sh             least-privilege IRSA
03_build_push_images.sh build+push flink/spark/load images to ECR
04_run_benchmark.sh     run one engine × write × partition, measure, clean up
99_teardown.sh          delete everything (stops costs)
images/                 flink/spark/load Dockerfiles (amd64)
jobs/                   flink.yaml, spark.yaml, producer-fleet.yaml (templated)
manifests/strimzi/      Kafka cluster (KRaft, 3 brokers, short retention)
```
