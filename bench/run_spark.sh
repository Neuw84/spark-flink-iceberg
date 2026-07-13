#!/usr/bin/env bash
# Spark Structured Streaming → Iceberg benchmark run.
#   Usage: bench/run_spark.sh [distribution_mode] [run_name] [--pyspark]
set -euo pipefail
cd "$(dirname "$0")/.."
source bench/lib.sh
load_env

MODE="${1:-${WRITE_DISTRIBUTION_MODE}}"
RUN="${2:-spark_${MODE}}"
VARIANT="${3:-}"
echo "[bench] === Spark Structured Streaming, distribution=${MODE}, run=${RUN} ${VARIANT} ==="

docker compose up -d kafka minio minio-init iceberg-rest spark
ensure_topic

TRIGGER=$(( CHECKPOINT_INTERVAL_MS / 1000 ))
if [ "${VARIANT}" == "--pyspark" ]; then
  docker cp spark/pyspark/ingest.py spark:/opt/spark/work-dir/ingest.py
  docker exec -d spark /opt/spark/bin/spark-submit \
    --master 'local[4]' --conf spark.sql.shuffle.partitions=16 \
    /opt/spark/work-dir/ingest.py --distribution-mode "${MODE}" --trigger "${TRIGGER}"
  TABLE="demo.${ICEBERG_DB}.events_pyspark"
else
  (cd spark/scala && sbt -batch package)
  docker cp spark/scala/target/scala-2.13/spark-iceberg-bench_2.13-1.0.0.jar spark:/opt/spark/work-dir/app.jar
  docker exec -d spark /opt/spark/bin/spark-submit \
    --master 'local[4]' --class com.benchmark.IcebergIngestJob \
    --conf spark.sql.shuffle.partitions=16 \
    /opt/spark/work-dir/app.jar --distribution-mode "${MODE}" --trigger "${TRIGGER}"
  TABLE="demo.${ICEBERG_DB}.events_spark"
fi
sleep 20  # Spark session + first trigger warm-up

start_stats "${RUN}"
python3 bench/measure_latency.py --table "${TABLE}" --run "${RUN}" --seconds "${RUN_SECONDS}" &
MEAS_PID=$!
start_producers "${RUN}" 4
wait_producers
kill ${MEAS_PID} 2>/dev/null || true
stop_stats "${RUN}"
echo "[bench] done -> results/${RUN}.*"
