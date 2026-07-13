#!/usr/bin/env bash
# Build + push the engine and load-generator images to ECR (amd64 for m5 nodes).
#   sfi-bench/flink   flink 2.2 + Iceberg + shaded-hadoop + the DataStream jar
#   sfi-bench/spark   spark 4.1 + Iceberg + Glue + Kafka jars + the Scala jar
#   sfi-bench/load    python + confluent-kafka + producer.py (distributed load)
set -euo pipefail
cd "$(dirname "$0")/.."
source eks/00_profile.env
_sfi_guard || exit 1
ACCOUNT=$(_sfi_whoami)
REGISTRY="${ACCOUNT}.dkr.ecr.${AWS_REGION}.amazonaws.com"
TAG="${TAG:-v1}"
PLATFORM=linux/amd64

echo "==> ECR login ($REGISTRY)"
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY"
ensure_repo() { aws ecr describe-repositories --repository-names "$1" >/dev/null 2>&1 || aws ecr create-repository --repository-name "$1" >/dev/null; }
for r in sfi-bench/flink sfi-bench/spark sfi-bench/load; do ensure_repo "$r"; done

echo "==> [1/3] flink jar + image"
( cd flink/datastream && mvn -q -B -DskipTests package )
test -f flink/datastream/target/flink-iceberg-bench.jar || { echo "flink jar missing"; exit 1; }
docker buildx build --platform $PLATFORM -t "$REGISTRY/sfi-bench/flink:$TAG" \
  -f eks/images/flink.Dockerfile . --push

echo "==> [2/3] spark jars + image"
( cd spark/scala && sbt -batch package )
bash spark/fetch-jars.sh
docker buildx build --platform $PLATFORM -t "$REGISTRY/sfi-bench/spark:$TAG" \
  -f eks/images/spark.Dockerfile . --push

echo "==> [3/3] load-generator image"
docker buildx build --platform $PLATFORM -t "$REGISTRY/sfi-bench/load:$TAG" \
  -f eks/images/load.Dockerfile . --push

echo "REGISTRY=$REGISTRY" >  eks/.registry.env
echo "TAG=$TAG"           >> eks/.registry.env
echo "==> images pushed. Wrote eks/.registry.env"
