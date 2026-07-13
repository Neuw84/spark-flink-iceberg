#!/usr/bin/env python3
"""EKS-scale aggregation: same snapshot-based latency/throughput as analyze.py,
but pulls CPU/mem from a Prometheus range query instead of docker stats, and
adds a cost estimate from instance-hours.

Usage:
  python3 analyze_eks.py --prometheus http://localhost:9090 \
      --run flink_ds_hash --node-hourly-usd 0.384 --nodes 6
"""
import argparse
import json
import os

import pandas as pd

try:
    import requests
except ImportError:
    requests = None

ROOT = os.path.join(os.path.dirname(__file__), "..")
RESULTS = os.path.join(ROOT, "results")


def prom_range(prom, query, start, end, step="15s"):
    if requests is None:
        raise SystemExit("pip install requests to query Prometheus")
    r = requests.get(f"{prom}/api/v1/query_range",
                     params={"query": query, "start": start, "end": end, "step": step},
                     timeout=30)
    r.raise_for_status()
    return r.json()["data"]["result"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--namespace", default="bench")
    ap.add_argument("--start", type=float, required=True, help="unix start")
    ap.add_argument("--end", type=float, required=True, help="unix end")
    ap.add_argument("--node-hourly-usd", type=float, default=0.384)
    ap.add_argument("--nodes", type=int, default=6)
    args = ap.parse_args()

    # CPU cores used and memory bytes for the benchmark namespace.
    cpu_q = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{args.namespace}"}}[1m]))'
    mem_q = f'sum(container_memory_working_set_bytes{{namespace="{args.namespace}"}})'
    cpu = prom_range(args.prometheus, cpu_q, args.start, args.end)
    mem = prom_range(args.prometheus, mem_q, args.start, args.end)

    def avg(series):
        vals = [float(v) for r in series for _, v in r["values"]]
        return sum(vals) / len(vals) if vals else float("nan")

    avg_cores = round(avg(cpu), 2)
    avg_mem_gb = round(avg(mem) / (1024 ** 3), 2)

    # Throughput from the snapshot summary written by measure_latency.py.
    summ_path = os.path.join(RESULTS, f"{args.run}.summary.json")
    summ = json.load(open(summ_path)) if os.path.exists(summ_path) else {}
    rps = summ.get("throughput_rps", float("nan"))
    rows = summ.get("rows_total", 0)

    hours = (args.end - args.start) / 3600.0
    cluster_cost = args.node_hourly_usd * args.nodes * hours
    cost_per_billion = round(cluster_cost / rows * 1e9, 2) if rows else float("nan")

    out = {
        "run": args.run,
        "throughput_rps": rps,
        "avg_cores": avg_cores,
        "avg_mem_gb": avg_mem_gb,
        "core_seconds_per_1M": round(avg_cores * (args.end - args.start) / (rows / 1e6), 1) if rows else None,
        "cluster_cost_usd": round(cluster_cost, 2),
        "usd_per_billion_records": cost_per_billion,
    }
    with open(os.path.join(RESULTS, f"{args.run}.eks.json"), "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
