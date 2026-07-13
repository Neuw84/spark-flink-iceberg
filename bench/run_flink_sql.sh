#!/usr/bin/env bash
# Flink SQL → Iceberg benchmark run (pipes flink/sql/ingest.sql to sql-client).
#   Usage: bench/run_flink_sql.sh [run_name]
set -euo pipefail
cd "$(dirname "$0")/.."
source bench/lib.sh
load_env

RUN="${1:-flink_sql}"
echo "[bench] === Flink SQL, run=${RUN} ==="

docker compose up -d kafka minio minio-init iceberg-rest flink-jobmanager flink-taskmanager
ensure_topic

docker cp flink/sql/ingest.sql flink-jobmanager:/opt/flink/ingest.sql
# -d detaches the INSERT as a continuous job.
docker exec flink-jobmanager bash -lc \
  "/opt/flink/bin/sql-client.sh -f /opt/flink/ingest.sql" &
sleep 20

start_stats "${RUN}"
python3 bench/measure_latency.py --table "demo.${ICEBERG_DB}.events_sql" --run "${RUN}" --seconds "${RUN_SECONDS}" &
MEAS_PID=$!
start_producers "${RUN}" 4
wait_producers
kill ${MEAS_PID} 2>/dev/null || true
stop_stats "${RUN}"
echo "[bench] done -> results/${RUN}.*"
