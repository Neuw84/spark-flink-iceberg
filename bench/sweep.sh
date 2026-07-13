#!/usr/bin/env bash
# Fair head-to-head sweep: for each engine, at a FIXED resource budget and a
# FIXED input rate, measure whether it keeps up (lag), how fast it makes data
# visible (latency), how it sizes files (small-files), and what it costs
# (CPU/mem). This is the data behind "which is better/faster/cheaper for
# streaming into Iceberg".
#
# Fairness: both engines get the same slot/core budget (BUDGET_CORES) and the
# producer drives the same RATE. Flink = JM+TM(BUDGET_CORES slots); Spark =
# local[BUDGET_CORES]. Same host, same Kafka, same Iceberg REST catalog.
#
# Usage: bench/sweep.sh [rate] [seconds]
set -euo pipefail
cd "$(dirname "$0")/.."
source bench/lib.sh
load_env

RATE="${1:-${TARGET_RATE}}"
SECONDS_RUN="${2:-${RUN_SECONDS}}"
BUDGET_CORES="${BUDGET_CORES:-4}"
BUDGET_MEM_GB="${BUDGET_MEM_GB:-6}"   # matched heap for both engines (fairness)
PRODUCERS="${PRODUCERS:-4}"
PY=.venv/bin/python

mkdir -p results
echo "[sweep] rate=${RATE} rps  duration=${SECONDS_RUN}s  budget=${BUDGET_CORES} cores / ${BUDGET_MEM_GB}GB  producers=${PRODUCERS}"

# --- helpers -------------------------------------------------------------
cancel_flink_jobs() {
  # Cancel ALL non-terminal jobs (a RESTARTING job still holds slots and would
  # deadlock the next submit — cancelling only RUNNING is not enough).
  for j in $(curl -s http://localhost:8081/jobs 2>/dev/null \
            | $PY -c "import sys,json;[print(x['id']) for x in json.load(sys.stdin)['jobs'] if x['status'] in ('RUNNING','RESTARTING','CREATED','FAILING','CANCELLING')]" 2>/dev/null); do
    curl -s -XPATCH "http://localhost:8081/jobs/$j?mode=cancel" >/dev/null 2>&1 || true
  done
  # wait until all 4 slots are free before returning
  for _ in $(seq 1 12); do
    local avail
    avail=$(curl -s http://localhost:8081/overview 2>/dev/null | $PY -c "import sys,json;print(json.load(sys.stdin)['slots-available'])" 2>/dev/null || echo 0)
    [ "${avail:-0}" -ge "${BUDGET_CORES}" ] && break
    sleep 3
  done
}

fresh_topic() {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic "${SOURCE_TOPIC}" 2>/dev/null || true
  sleep 3
  # Aggressive retention so a 50 MB/s stream can't fill local disk: 2-min time
  # cap, 512 MB/partition size cap, 30s segment roll (retention only purges
  # closed segments, so short segments matter). Bounds Kafka to a few GB.
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "${SOURCE_TOPIC}" --partitions "${KAFKA_PARTITIONS}" --replication-factor 1 \
    --config retention.ms=120000 --config retention.bytes=536870912 \
    --config segment.ms=30000 --config segment.bytes=268435456 2>/dev/null || true
  # reset any stale consumer-group offsets so flink_group_lag starts clean (not negative)
  for g in flink-iceberg-bench flink-sql-iceberg-bench pyflink-iceberg-bench flink-dynamic-sink-bench; do
    docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
      --delete --group "$g" 2>/dev/null || true
  done
}

wait_flink_running() { # wait until at least one job is RUNNING (max ~45s)
  for _ in $(seq 1 15); do
    local n
    n=$(curl -s http://localhost:8081/jobs 2>/dev/null \
        | $PY -c "import sys,json;print(sum(1 for x in json.load(sys.stdin)['jobs'] if x['status']=='RUNNING'))" 2>/dev/null || echo 0)
    [ "${n:-0}" -ge 1 ] && return 0
    sleep 3
  done
  echo "[sweep] WARNING: no RUNNING Flink job after wait"
}

drop_table() {  # $1 = table short name
  $PY - "$1" <<'PY' 2>/dev/null || true
import sys
from pyiceberg.catalog import load_catalog
import os
c=load_catalog('demo',**{'type':'rest','uri':'http://localhost:8181','warehouse':'s3://warehouse/','s3.endpoint':'http://localhost:9000','s3.access-key-id':'admin','s3.secret-access-key':'password','s3.path-style-access':'true'})
try: c.drop_table(('streaming', sys.argv[1]))
except Exception: pass
PY
  purge_minio "$1"
}

# CRITICAL for disk safety: Iceberg drop_table only removes catalog metadata —
# the Parquet data files stay in MinIO forever and a 50 MB/s stream fills the
# disk in minutes across a sweep. Physically delete the table's object prefix.
purge_minio() {  # $1 = table short name
  docker run --rm --network sfi-bench --entrypoint sh minio/mc:RELEASE.2024-11-05T11-29-45Z -c "
    mc alias set m http://minio:9000 ${AWS_ACCESS_KEY_ID:-admin} ${AWS_SECRET_ACCESS_KEY:-password} >/dev/null 2>&1;
    mc rm -r --force m/${WAREHOUSE_BUCKET:-warehouse}/streaming/$1 >/dev/null 2>&1;
    mc rm -r --force m/${WAREHOUSE_BUCKET:-warehouse}/streaming.db/$1 >/dev/null 2>&1;
  " 2>/dev/null || true
}

# samplers: docker stats (cpu/mem) for a set of containers, into results/<run>.stats.csv
start_stats() { # $1=run  $2=container-grep
  local run="$1" pat="$2"
  ( echo "ts,container,cpu_pct,mem_mb" > "results/${run}.stats.csv"
    while true; do
      ts=$(date +%s)
      docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
        | grep -E "$pat" \
        | while IFS=, read -r name cpu mem; do
            echo "${ts},${name},${cpu/\%/},$(echo "$mem" | awk -F/ '{print $1}' | sed 's/[^0-9.]//g')"
          done >> "results/${run}.stats.csv"
      sleep 2
    done ) &
  echo $! > "results/${run}.stats.pid"
}
stop_stats() { [ -f "results/$1.stats.pid" ] && kill "$(cat results/$1.stats.pid)" 2>/dev/null || true; rm -f "results/$1.stats.pid"; }

PRODUCER_PIDS=()
run_producers() {
  local write="${1:-append}"
  local per=$(( RATE / PRODUCERS ))
  # For upsert, reuse keys (50%) so real updates/deletes are exercised.
  local dup=""; [ "$write" = "upsert" ] && dup="--dup-rate 0.5"
  PRODUCER_PIDS=()
  for i in $(seq 1 "${PRODUCERS}"); do
    $PY common/producer.py --bootstrap "${KAFKA_BOOTSTRAP_HOST}" --topic "${SOURCE_TOPIC}" \
      --rate "${per}" --seconds "${SECONDS_RUN}" --payload-bytes "${PAYLOAD_BYTES}" ${dup} \
      > "results/producer.${i}.log" 2>&1 &
    PRODUCER_PIDS+=($!)
  done
}

measure_and_load() { # $1=run $2=table $3=engine $4=write $5=lag-source
  # $5 = flink job id (engine=flink) OR spark log path (engine=spark)
  local run="$1" table="$2" engine="$3" write="${4:-append}" lagsrc="${5:-}"
  local expected=0
  [ "$write" = "append" ] && expected=$(( RATE * SECONDS_RUN ))
  start_stats "$run" 'flink|spark'
  $PY bench/measure.py --table "$table" --run "$run" --seconds "$((SECONDS_RUN + 40))" --expected-rows "$expected" \
     > "results/${run}.measure.log" 2>&1 &
  local mpid=$!
  # True source lag (commit-independent): Flink pendingRecords via REST, Spark
  # read-position from the driver log — NOT produced-minus-committed.
  local lagargs="--engine $engine"
  if [ "$engine" = "flink" ]; then lagargs="$lagargs --flink-job $lagsrc"; else lagargs="$lagargs --spark-log $lagsrc"; fi
  $PY bench/sample_lag.py --run "$run" --table "$table" $lagargs --seconds "$((SECONDS_RUN + 40))" \
     > "results/${run}.lag.log" 2>&1 &
  local lpid=$!
  sleep 3
  run_producers "$write"
  # Wait ONLY on the producer PIDs — a bare `wait` would block forever on the
  # infinite start_stats docker-stats loop (that bug hung the whole sweep).
  wait "${PRODUCER_PIDS[@]}" 2>/dev/null || true
  echo "[sweep] producers done; draining for final commits"
  wait "$mpid" "$lpid" 2>/dev/null || true
  stop_stats "$run"
}

# --- engine runners: (mode, write[append|upsert], partitioning[bucket|time], fmt[2|3]) ---
run_flink_ds() {
  local mode="$1" write="$2" part="$3" fmt="$4"
  local run="flink_${mode}_${write}_${part}_v${fmt}"
  local ups="false"; [ "$write" = "upsert" ] && ups="true"
  echo "[sweep] === Flink ${mode}/${write}/${part}/v${fmt} ==="
  cancel_flink_jobs; fresh_topic; drop_table events
  docker cp flink/datastream/target/flink-iceberg-bench.jar flink-jobmanager:/opt/flink/app.jar
  local jid
  jid=$(docker exec flink-jobmanager flink run -d -c com.benchmark.IcebergIngestJob /opt/flink/app.jar \
    --distribution-mode "${mode}" --upsert "${ups}" --partitioning "${part}" \
    --format-version "${fmt}" --parallelism "${BUDGET_CORES}" 2>&1 | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}')
  echo "[sweep]   flink job=$jid"
  wait_flink_running
  measure_and_load "$run" "demo.${ICEBERG_DB}.events" "flink" "$write" "$jid"
  cancel_flink_jobs
}

run_spark() {
  local mode="$1" write="$2" part="$3" fmt="$4"
  local run="spark_${mode}_${write}_${part}_v${fmt}"
  local ups="false"; [ "$write" = "upsert" ] && ups="true"
  echo "[sweep] === Spark ${mode}/${write}/${part}/v${fmt} ==="
  cancel_flink_jobs   # ensure no Flink job is also consuming the topic
  fresh_topic         # same clean topic every run, so 'produced' is comparable
  drop_table events_spark
  # clear the Spark streaming checkpoint so it doesn't resume from offsets the
  # aggressive Kafka retention has already purged (would stall the query).
  docker exec spark bash -c "rm -rf /tmp/spark-ckpt/events_spark" 2>/dev/null || true
  docker cp spark/scala/target/scala-2.13/spark-iceberg-bench_2.13-1.0.0.jar spark:/opt/spark/work-dir/app.jar
  docker exec -d -e KAFKA_BOOTSTRAP=kafka:9092 -e CATALOG_URI=http://iceberg-rest:8181 \
    -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password -e AWS_REGION=us-east-1 \
    -e WAREHOUSE_BUCKET=warehouse -e S3_ENDPOINT=http://minio:9000 \
    -e PARTITIONING="${part}" -e TABLE_FORMAT_VERSION="${fmt}" \
    spark bash -c "nohup /opt/spark/bin/spark-submit --master 'local[${BUDGET_CORES}]' \
      --driver-memory ${BUDGET_MEM_GB}g \
      --class com.benchmark.IcebergIngestJob --conf spark.sql.shuffle.partitions=16 \
      --conf spark.sql.files.maxRecordsPerFile=2000000 \
      /opt/spark/work-dir/app.jar --distribution-mode '${mode}' --upsert '${ups}' \
      --partitioning '${part}' --format-version '${fmt}' --trigger '$(( CHECKPOINT_INTERVAL_MS / 1000 ))' \
      > /tmp/spark_${run}.log 2>&1 &"
  sleep 25
  # copy the driver log out so the sampler can parse read-position (endOffset)
  local slog="results/${run}.spark.log"
  ( while :; do docker cp "spark:/tmp/spark_${run}.log" "$slog" 2>/dev/null; sleep 5; done ) &
  local tailer=$!
  measure_and_load "$run" "demo.${ICEBERG_DB}.events_spark" "spark" "$write" "$slog"
  kill "$tailer" 2>/dev/null || true
  # kill the Spark job and WAIT for it to actually exit, else the next run hits
  # CONCURRENT_STREAM_LOG_UPDATE from an overlapping streaming query.
  docker exec spark bash -c "pkill -9 -f spark-submit" 2>/dev/null || true
  for _ in $(seq 1 10); do
    n=$(docker exec spark bash -c "ps aux | grep -c '[S]parkSubmit'" 2>/dev/null || echo 0)
    [ "${n:-0}" -eq 0 ] && break
    sleep 2
  done
}

# --- the cross-product sweep --------------------------------------------
# axes: dist-mode × write × partitioning. Upsert forces format-version 3
# (deletion vectors); append uses v2. Upsert + time partitioning is skipped
# (upsert identity key is user_id, so cardinality/bucket is the meaningful pair).
# hash is the only distribution mode that keeps up at high rate for BOTH engines.
# At 100k rps, Flink's 'none' and 'range' modes committed ZERO (file explosion /
# global sort can't keep pace); Spark tolerates them but they add no signal over
# hash. So the default sweep is hash-only. Override with MODES_OVERRIDE to explore.
MODES="${MODES_OVERRIDE:-hash}"
WRITES="${WRITES_OVERRIDE:-append upsert}"
PARTS="${PARTS_OVERRIDE:-bucket time}"

for mode in ${MODES}; do
  for write in ${WRITES}; do
    for part in ${PARTS}; do
      [ "$write" = "upsert" ] && [ "$part" = "time" ] && continue  # not meaningful
      fmt=2; [ "$write" = "upsert" ] && fmt=3
      run_flink_ds "$mode" "$write" "$part" "$fmt"
      run_spark    "$mode" "$write" "$part" "$fmt"
    done
  done
done

$PY bench/analyze.py
echo "[sweep] done → results/comparison.md, results/*.summary.json, docs/charts/"
