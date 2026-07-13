#!/usr/bin/env bash
# Local 5-min matrix — same 5 cells as EKS, proportional envelope (7c/14GB, 50k,
# 24 buckets, 60s commits). Sequential so each engine owns the host.
set -uo pipefail
cd "$(dirname "$0")/.."
RUN_SECONDS="${RUN_SECONDS:-300}"
WARMUP="${WARMUP:-60}"
log(){ echo "[$(date '+%H:%M:%S')] $*"; }

run(){ # engine write [merge_mode]
  log "=== CELL: $* ==="
  RUN_SECONDS="$RUN_SECONDS" WARMUP="$WARMUP" bash bench/run_local_bench.sh "$@"
  log "=== DONE: $* ==="
  sleep 10
}

log "LOCAL MATRIX START — 5 cells × ${RUN_SECONDS}s"
run flink append
run spark append
run flink upsert
run spark upsert merge-on-read
run spark upsert copy-on-write
log "LOCAL MATRIX COMPLETE — results/local/*"
