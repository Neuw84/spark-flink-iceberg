#!/usr/bin/env bash
# OVERLOAD test: the honest "who keeps up / who's faster" comparison.
#
# We PREFILL the topic with a large backlog, THEN start the engine and drive
# MORE than it can commit. With an unbounded backlog the engine is never idle,
# so:
#   * committed-rows/s = the TRUE commit ceiling (no idle-averaging artifact)
#   * pipeline_lag slope = how fast it falls behind at this input rate
# The engine that sustains higher committed-rps and a flatter lag wins.
#
# Usage: bench/overload.sh <engine: flink|spark> <cores> <prefill_millions> <sustain_rate> <seconds>
set -euo pipefail
cd "$(dirname "$0")/.."
source bench/lib.sh
load_env
PY=.venv/bin/python

ENGINE="${1:-flink}"
CORES="${2:-2}"
PREFILL_M="${3:-3}"
RATE="${4:-100000}"
SECS="${5:-180}"
RUN="overload_${ENGINE}_${CORES}c"
MEM_GB="${BUDGET_MEM_GB:-6}"

echo "[overload] engine=${ENGINE} cores=${CORES} prefill=${PREFILL_M}M sustain=${RATE}rps ${SECS}s"

# --- clean slate ---
for j in $(curl -s http://localhost:8081/jobs 2>/dev/null | $PY -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin)['jobs'] if x['status'] in ('RUNNING','RESTARTING')]" 2>/dev/null); do
  curl -s -XPATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null 2>&1; done
docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "${SOURCE_TOPIC}" 2>/dev/null || true
sleep 3
# BIG retention so the prefilled backlog isn't purged mid-test
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic "${SOURCE_TOPIC}" --partitions "${KAFKA_PARTITIONS}" --replication-factor 1 \
  --config retention.ms=1800000 --config retention.bytes=5368709120 --config segment.ms=60000 2>/dev/null || true

TABLE="events"; GROUP="flink-iceberg-bench"
[ "$ENGINE" = "spark" ] && TABLE="events_spark"
$PY - <<PY 2>/dev/null || true
from pyiceberg.catalog import load_catalog
c=load_catalog('demo',uri='http://localhost:8181',warehouse='s3://warehouse/',**{'type':'rest','s3.endpoint':'http://localhost:9000','s3.access-key-id':'admin','s3.secret-access-key':'password','s3.path-style-access':'true'})
try: c.drop_table(('streaming','${TABLE}'))
except Exception: pass
PY

# --- PREFILL the backlog (engine NOT running yet) ---
echo "[overload] prefilling ${PREFILL_M}M records..."
PF_TOTAL=$(( PREFILL_M * 1000000 ))
PF_EACH=$(( PF_TOTAL / 6 ))
for i in 1 2 3 4 5 6; do
  $PY common/producer.py --bootstrap "${KAFKA_BOOTSTRAP_HOST}" --topic "${SOURCE_TOPIC}" \
    --rate 40000 --seconds $(( PF_EACH / 40000 + 1 )) --payload-bytes "${PAYLOAD_BYTES}" \
    > "results/${RUN}.prefill.${i}.log" 2>&1 &
done
wait
echo "[overload] prefill done: $(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic ${SOURCE_TOPIC} --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s}') msgs in topic"

# --- start the engine reading from EARLIEST (so it must chew the whole backlog) ---
if [ "$ENGINE" = "flink" ]; then
  docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar
  docker exec flink-jobmanager flink run -d -c com.benchmark.IcebergIngestJob /opt/flink/app.jar \
    --distribution-mode hash --partitioning bucket --parallelism "${CORES}" \
    --starting-offsets earliest >/dev/null 2>&1
else
  docker exec spark bash -c "rm -rf /tmp/spark-ckpt/events_spark" 2>/dev/null || true
  docker cp spark/scala/target/scala-2.13/spark-iceberg-bench_2.13-1.0.0.jar spark:/opt/spark/work-dir/app.jar
  docker exec -d -e KAFKA_BOOTSTRAP=kafka:9092 -e CATALOG_URI=http://iceberg-rest:8181 \
    -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e AWS_REGION=us-east-1 \
    -e WAREHOUSE_BUCKET=warehouse -e S3_ENDPOINT=http://minio:9000 -e STARTING_OFFSETS=earliest \
    spark bash -c "nohup /opt/spark/bin/spark-submit --master 'local[${CORES}]' --driver-memory ${MEM_GB}g \
      --class com.benchmark.IcebergIngestJob --conf spark.sql.shuffle.partitions=16 \
      /opt/spark/work-dir/app.jar --distribution-mode hash --partitioning bucket \
      --starting-offsets earliest --trigger 30 > /tmp/spark_${RUN}.log 2>&1 &"
fi

# --- sample lag while ALSO sustaining input above capacity ---
$PY bench/sample_lag.py --run "$RUN" --table "demo.${ICEBERG_DB}.${TABLE}" --group "$GROUP" --seconds "$((SECS+30))" > "results/${RUN}.lag.log" 2>&1 &
LPID=$!
$PY bench/measure.py --table "demo.${ICEBERG_DB}.${TABLE}" --run "$RUN" --seconds "$((SECS+30))" > "results/${RUN}.measure.log" 2>&1 &
MPID=$!
# keep the pressure on
PF_PIDS=()
for i in 1 2 3; do
  $PY common/producer.py --bootstrap "${KAFKA_BOOTSTRAP_HOST}" --topic "${SOURCE_TOPIC}" \
    --rate $(( RATE / 3 )) --seconds "${SECS}" --payload-bytes "${PAYLOAD_BYTES}" > "results/${RUN}.sustain.${i}.log" 2>&1 &
  PF_PIDS+=($!)
done
wait "${PF_PIDS[@]}" 2>/dev/null || true
wait "$MPID" "$LPID" 2>/dev/null || true
echo "[overload] done → results/${RUN}.*"
