#!/usr/bin/env python3
"""Poll an Iceberg table during a run and record per-snapshot metrics.

For every NEW snapshot that appears we capture the levers that decide whether an
engine is *faster*, *better* (file sizing / correctness), and *cheaper*:

  visibility_latency_ms = commit_time - max(ingest_ts visible at that commit)
                          → how stale the freshest record was when queryable
  added_rows            → throughput signal
  added_files           → small-files signal
  added_bytes           → data volume
  avg_file_bytes        → THE file-sizing metric (bigger = fewer small files)

At the end we write a summary (throughput, latency percentiles, file sizing,
correctness vs an expected row count) to results/<run>.summary.json and the raw
per-snapshot series to results/<run>.snapshots.csv.

Usage:
  measure.py --table demo.streaming.events_spark --run spark_hash \
             --seconds 240 [--expected-rows 4800000]
"""
import argparse
import csv
import json
import os
import time

import pyarrow.compute as pc
import requests
from pyiceberg.catalog import load_catalog

RESULTS = os.path.join(os.path.dirname(__file__), "..", "results")


def _env(name, default):
    v = os.getenv(name)  # empty string (from `set -a; source .env`) → unset
    return v if v else default


REST_URI = _env("CATALOG_URI_HOST", "http://localhost:8181")


def catalog():
    return load_catalog("demo", **{
        "type": "rest",
        "uri": REST_URI,
        "warehouse": f"s3://{_env('WAREHOUSE_BUCKET', 'warehouse')}/",
        "s3.endpoint": _env("S3_ENDPOINT_HOST", "http://localhost:9000"),
        "s3.access-key-id": _env("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": _env("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": "true",
    })


def rest_snapshots(ns, tbl):
    """Per-snapshot metrics via the REST catalog HTTP API (no pyiceberg 404 cache)."""
    r = requests.get(f"{REST_URI}/v1/namespaces/{ns}/tables/{tbl}", timeout=15)
    if r.status_code != 200:
        return []
    return r.json().get("metadata", {}).get("snapshots", [])


def freshest_ingest_ts(ident, snapshot_id):
    """max(ingest_ts) visible as of this snapshot — column-projected scan.

    Only called once the table is known to exist, so pyiceberg's 404 cache
    (which broke the poll loop) is not a factor here.
    """
    try:
        # strip the catalog name if present: ('demo','streaming','events') -> ('streaming','events')
        load_ident = ident[-2:]
        tbl = catalog().load_table(load_ident)
        arr = tbl.scan(snapshot_id=snapshot_id, selected_fields=("ingest_ts",)).to_arrow()
        if arr.num_rows == 0:
            return None
        return pc.max(arr.column("ingest_ts")).as_py()
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "240")))
    ap.add_argument("--poll", type=float, default=3.0)
    ap.add_argument("--expected-rows", type=int, default=0,
                    help="rows the producer sent; used for the correctness check")
    args = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)

    ns, tbl_name = args.table.split(".")[-2], args.table.split(".")[-1]
    ident = tuple(args.table.split("."))
    seen = set()
    rows_csv = os.path.join(RESULTS, f"{args.run}.snapshots.csv")
    f = open(rows_csv, "w", newline="")
    w = csv.writer(f)
    w.writerow(["commit_ms", "snapshot_id", "added_rows", "added_files",
                "added_bytes", "avg_file_bytes", "visibility_latency_ms"])

    start = time.time()
    while time.time() - start < args.seconds:
        try:
            # Snapshots via the REST HTTP API — pyiceberg caches a 404 for a
            # table that doesn't exist yet (dropped then recreated by the job),
            # so it would never see it appear. Raw HTTP has no such cache.
            for s in rest_snapshots(ns, tbl_name):
                sid = s.get("snapshot-id")
                if sid in seen:
                    continue
                seen.add(sid)
                summ = s.get("summary", {})
                added_rows = int(summ.get("added-records", 0) or 0)
                added_files = int(summ.get("added-data-files", 0) or 0)
                added_bytes = int(summ.get("added-files-size", 0) or 0)
                avg_file = round(added_bytes / added_files) if added_files else 0
                commit_ms = s.get("timestamp-ms")
                mx = freshest_ingest_ts(ident, sid) if added_rows else None
                lat = (commit_ms - mx) if mx else ""
                w.writerow([commit_ms, sid, added_rows, added_files,
                            added_bytes, avg_file, lat])
                f.flush()
        except Exception as e:
            print(f"[measure] waiting for {args.table}… ({e})")
        time.sleep(args.poll)
    f.close()

    # ---- summarize ----
    import statistics
    commit_rows, files, byts, lats = [], [], [], []
    with open(rows_csv) as rf:
        for r in csv.DictReader(rf):
            ar = int(r["added_rows"])
            if ar <= 0:  # skip empty / delete-only snapshots
                continue
            commit_rows.append(ar)
            files.append(int(r["added_files"]))
            byts.append(int(r["added_bytes"]))
            if r["visibility_latency_ms"] != "":
                lats.append(int(r["visibility_latency_ms"]))

    total_rows = sum(commit_rows)
    total_files = sum(files)
    total_bytes = sum(byts)
    elapsed = time.time() - start

    def pct(v, q):
        if not v:
            return None
        v = sorted(v)
        return int(v[min(len(v) - 1, int(q * len(v)))])

    summary = {
        "run": args.run,
        "table": args.table,
        "elapsed_s": round(elapsed, 1),
        "commits": len(commit_rows),
        "rows_committed": total_rows,
        "throughput_rps": round(total_rows / elapsed, 1) if elapsed else 0,
        "files_total": total_files,
        "avg_rows_per_file": round(total_rows / total_files, 1) if total_files else 0,
        "avg_file_bytes": round(total_bytes / total_files) if total_files else 0,
        "avg_file_mb": round(total_bytes / total_files / 1e6, 2) if total_files else 0,
        "visibility_latency_ms": {
            "p50": pct(lats, 0.50), "p95": pct(lats, 0.95),
            "p99": pct(lats, 0.99), "max": max(lats) if lats else None,
        },
    }
    if args.expected_rows:
        summary["correctness"] = {
            "expected_rows": args.expected_rows,
            "committed_rows": total_rows,
            "delta": total_rows - args.expected_rows,
            "exactly_once_ok": total_rows == args.expected_rows,
        }
    with open(os.path.join(RESULTS, f"{args.run}.summary.json"), "w") as sf:
        json.dump(summary, sf, indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
