#!/usr/bin/env bash
# Shared helpers for bench run scripts. Source this after loading .env.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${ROOT}/results"
mkdir -p "${RESULTS_DIR}"

# Load .env into the environment.
load_env() {
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
}

# Sample per-container CPU%/mem every second into a CSV until stop_stats is called.
# Usage: start_stats <run_name>
start_stats() {
  local run="$1"
  local out="${RESULTS_DIR}/${run}.stats.csv"
  echo "ts,container,cpu_pct,mem_mb" > "${out}"
  (
    while true; do
      local ts; ts=$(date +%s)
      docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
        | while IFS=, read -r name cpu mem; do
            local cpun; cpun=${cpu/\%/}
            local memmb; memmb=$(echo "${mem}" | awk -F/ '{print $1}' | sed 's/[^0-9.]//g')
            echo "${ts},${name},${cpun},${memmb}" >> "${out}"
          done
      sleep 1
    done
  ) &
  echo $! > "${RESULTS_DIR}/${run}.stats.pid"
}

stop_stats() {
  local run="$1"
  local pidfile="${RESULTS_DIR}/${run}.stats.pid"
  [ -f "${pidfile}" ] && kill "$(cat "${pidfile}")" 2>/dev/null || true
  rm -f "${pidfile}"
}

# Launch the producer fleet (N parallel processes summing to TARGET_RATE).
# Usage: start_producers <run_name> [n_procs]
start_producers() {
  local run="$1"; local n="${2:-4}"
  local per=$(( TARGET_RATE / n ))
  echo "[bench] launching ${n} producers @ ${per} rps each (${TARGET_RATE} total) for ${RUN_SECONDS}s"
  for i in $(seq 1 "${n}"); do
    python3 "${ROOT}/common/producer.py" \
      --bootstrap "${KAFKA_BOOTSTRAP_HOST}" --topic "${SOURCE_TOPIC}" \
      --rate "${per}" --seconds "${RUN_SECONDS}" --payload-bytes "${PAYLOAD_BYTES}" \
      > "${RESULTS_DIR}/${run}.producer.${i}.log" 2>&1 &
  done
}

wait_producers() { wait; }

# Ensure the Kafka topic exists with the configured partition count.
ensure_topic() {
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "${SOURCE_TOPIC}" \
    --partitions "${KAFKA_PARTITIONS}" --replication-factor 1 2>/dev/null || true
}
