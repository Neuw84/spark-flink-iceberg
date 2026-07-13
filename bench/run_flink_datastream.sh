#!/usr/bin/env bash
# Flink DataStream → Iceberg benchmark run.
#   Usage: bench/run_flink_datastream.sh [distribution_mode] [run_name]
set -euo pipefail
cd "$(dirname "$0")/.."
source bench/lib.sh
load_env

MODE="${1:-${WRITE_DISTRIBUTION_MODE}}"
RUN="${2:-flink_ds_${MODE}}"
echo "[bench] === Flink DataStream, distribution=${MODE}, run=${RUN} ==="

# Build the fat jar (once; skip if present).
if [ ! -f flink/datastream/target/flink-iceberg-bench.jar ]; then
  (cd flink/datastream && mvn -q -DskipTests package)
fi

docker compose up -d kafka minio minio-init iceberg-rest flink-jobmanager flink-taskmanager
ensure_topic

# Submit the job.
docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar
JOB_ID=$(docker exec flink-jobmanager flink run -d -c com.benchmark.IcebergIngestJob \
  /opt/flink/app.jar --distribution-mode "${MODE}" --parallelism 4 \
  | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}')
echo "[bench] submitted Flink job ${JOB_ID}"
sleep 15  # let the job reach RUNNING before load starts

start_stats "${RUN}"
python3 bench/measure_latency.py --table "demo.${ICEBERG_DB}.events" --run "${RUN}" --seconds "${RUN_SECONDS}" &
MEAS_PID=$!
start_producers "${RUN}" 4
wait_producers
kill ${MEAS_PID} 2>/dev/null || true
stop_stats "${RUN}"

docker exec flink-jobmanager flink cancel "${JOB_ID}" 2>/dev/null || true
echo "[bench] done -> results/${RUN}.*"
