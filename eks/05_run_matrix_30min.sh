#!/usr/bin/env bash
# Drive the full 30-minute steady-state matrix sequentially. Each cell is a full
# deploy → 30-min measurement → snapshot-metric capture → teardown, using
# 04_run_benchmark.sh (commit-honest Iceberg-snapshot metrics). Runs serially so
# every run gets the whole 7c/14GB envelope to itself (fair, no cross-run noise).
#
# Matrix (per the blog scope — Flink DataStream vs Spark Scala):
#   1. flink  append  bucket
#   2. spark  append  bucket
#   3. flink  upsert  bucket            (Flink deletion-vector upsert)
#   4. spark  upsert  bucket  MoR       (merge-on-read / deletion vectors)
#   5. spark  upsert  bucket  CoW       (copy-on-write MERGE)
#
# Usage: eks/05_run_matrix_30min.sh            # all 5 cells
#        RUN_SECONDS=1800 WARMUP=120 ...       # overridable
set -uo pipefail
cd "$(dirname "$0")"

RUN_SECONDS="${RUN_SECONDS:-1800}"   # 30 min steady-state window
WARMUP="${WARMUP:-120}"              # 2-min warmup before measuring
PARALLELISM="${PARALLELISM:-12}"     # 14c/28GB envelope, par 12 = 12 Kafka partitions
PARTITIONS="${PARTITIONS:-12}"
BUCKETS="${BUCKETS:-24}"             # 24 concurrent write streams: Flink holds 100k, files ~10MB
PRODPODS="${PRODPODS:-10}"
RATE="${RATE:-10000}"                # 10 pods × 10k = 100k rows/s aggregate (keep-up test)

log() { echo "[$(date '+%H:%M:%S')] $*"; }

run() {  # engine write part [merge_mode]
  local engine="$1" write="$2" part="$3" mm="${4:-merge-on-read}"
  log "=== CELL: $engine $write $part ${mm} (${RUN_SECONDS}s) ==="
  WRITE="$write" PART="$part" MERGE_MODE="$mm" PARTITIONS="$PARTITIONS" BUCKETS="$BUCKETS" \
    RUN_SECONDS="$RUN_SECONDS" WARMUP="$WARMUP" \
    bash ./04_run_benchmark.sh "$engine" "$PARALLELISM" "$PRODPODS" "$RATE"
  log "=== CELL DONE: $engine $write $part ${mm} ==="
  sleep 20   # let teardown settle before next deploy
}

log "MATRIX START — 5 cells × ${RUN_SECONDS}s (~$(( (RUN_SECONDS+400)*5/60 )) min total)"

run flink append bucket
run spark append bucket
run flink upsert bucket
run spark upsert bucket merge-on-read
run spark upsert bucket copy-on-write

log "MATRIX COMPLETE — results in results/eks/*.lag.csv / *.files.txt / *.snapshots.txt"
