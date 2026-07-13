#!/usr/bin/env python3
"""Iceberg snapshot-based latency & throughput measurement.

Because an Iceberg sink makes data visible only at commit boundaries, we don't
measure per-record latency. Instead we poll the table's snapshots and, for each
NEW snapshot, compute:

  * visibility_latency = snapshot.commit_ms - max(ingest_ts in that snapshot)
    i.e. how stale the freshest record was at the moment it became queryable.
  * rows_committed, files_added   -> throughput and small-file behavior.

This is the honest way to compare Spark and Flink Iceberg sinks: it captures the
commit-interval floor plus the engine's per-commit overhead, and it exposes the
small-files tradeoff of each write distribution mode.

Usage:
  python3 measure_latency.py --table demo.streaming.events_spark \
      --run spark_hash --seconds 600
"""
import argparse
import json
import os
import time

from pyiceberg.catalog import load_catalog


def load_demo_catalog():
    return load_catalog("demo", **{
        "type": "rest",
        "uri": os.getenv("CATALOG_URI_HOST", os.getenv("CATALOG_URI", "http://localhost:8181")),
        "warehouse": f"s3://{os.getenv('WAREHOUSE_BUCKET', 'warehouse')}/",
        "s3.endpoint": os.getenv("S3_ENDPOINT_HOST", "http://localhost:9000"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "s3.path-style-access": "true",
    })


def snapshot_metrics(tbl, snap):
    """Extract rows + files + max ingest_ts for a snapshot from its summary/manifests."""
    summary = snap.summary or {}
    rows = int(summary.get("added-records", 0))
    files = int(summary.get("added-data-files", 0))
    return rows, files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "600")))
    ap.add_argument("--poll", type=float, default=2.0)
    args = ap.parse_args()

    results_dir = os.path.join(os.path.dirname(__file__), "..", "results")
    os.makedirs(results_dir, exist_ok=True)
    out_csv = os.path.join(results_dir, f"{args.run}.snapshots.csv")

    catalog = load_demo_catalog()
    seen = set()
    start = time.time()
    rows_total = files_total = 0

    with open(out_csv, "w") as f:
        f.write("commit_ms,snapshot_id,rows,files,visibility_latency_ms\n")
        while time.time() - start < args.seconds:
            try:
                tbl = catalog.load_table(tuple(args.table.split(".")))
                for snap in tbl.snapshots():
                    if snap.snapshot_id in seen:
                        continue
                    seen.add(snap.snapshot_id)
                    rows, files = snapshot_metrics(tbl, snap)
                    # visibility latency = commit time - freshest record in this snapshot.
                    latency = _visibility_latency(tbl, snap)
                    rows_total += rows
                    files_total += files
                    f.write(f"{snap.timestamp_ms},{snap.snapshot_id},{rows},{files},{latency}\n")
                    f.flush()
            except Exception as e:  # table may not exist yet at run start
                print(f"[measure] waiting for table… ({e})")
            time.sleep(args.poll)

    elapsed = time.time() - start
    summary = {
        "run": args.run,
        "table": args.table,
        "elapsed_s": round(elapsed, 1),
        "snapshots": len(seen),
        "rows_total": rows_total,
        "files_total": files_total,
        "avg_rows_per_file": round(rows_total / files_total, 1) if files_total else 0,
        "throughput_rps": round(rows_total / elapsed, 1) if elapsed else 0,
    }
    with open(os.path.join(results_dir, f"{args.run}.summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))


def _visibility_latency(tbl, snap):
    """commit_ms - max(ingest_ts) among the data files this snapshot added.

    Uses per-file column upper bounds from the snapshot's manifests, so it costs
    a manifest read rather than a full data scan. Returns None if the bound for
    ingest_ts can't be resolved (e.g. an overwrite/delete-only snapshot).
    """
    import struct

    commit_ms = snap.timestamp_ms
    if commit_ms is None:
        return None

    # Resolve the field id of ingest_ts in the table schema.
    try:
        ingest_fid = tbl.schema().find_field("ingest_ts").field_id
    except Exception:
        return None

    max_ingest = None
    io = tbl.io
    for manifest in snap.manifests(io):
        # Only data-file manifests carry column bounds for appended rows.
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            bounds = getattr(entry.data_file, "upper_bounds", None) or {}
            raw = bounds.get(ingest_fid)
            if raw is None:
                continue
            # ingest_ts is a bigint -> little-endian 8-byte lower/upper bound.
            val = struct.unpack("<q", raw[:8].ljust(8, b"\x00"))[0]
            max_ingest = val if max_ingest is None else max(max_ingest, val)

    if max_ingest is None:
        return None
    return max(0, commit_ms - max_ingest)


if __name__ == "__main__":
    main()
