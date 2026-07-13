#!/usr/bin/env bash
# Full local benchmark sweep: every engine × every write distribution mode,
# then aggregate. Runs sequentially so containers don't contend for the host.
#   Usage: bench/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MODES=(none hash range)

for m in "${MODES[@]}"; do
  bench/run_flink_datastream.sh "$m" "flink_ds_${m}"
  bench/run_spark.sh            "$m" "spark_${m}"
done

# Single-config variants (mode is a table prop / fixed here).
bench/run_flink_sql.sh "flink_sql"
bench/run_spark.sh hash "pyspark_hash" --pyspark

python3 bench/analyze.py
echo "[bench] sweep complete — see results/comparison.md and docs/charts/"
