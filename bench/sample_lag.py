#!/usr/bin/env python3
"""Sample TRUE Kafka consumer lag while a job runs — the honest "does it keep up".

The naive metric `produced - committed_to_iceberg` is WRONG for this comparison:
it conflates two different things — how far behind the engine is in *reading*
Kafka (true lag) versus how often it *commits* to Iceberg (every 60s here). Flink
reads continuously but only advances committed offsets on checkpoint, so a
commit-based lag makes Flink look behind when it isn't. We therefore read each
engine's own source read-position, which is commit-frequency independent:

  Flink:  sum of the `pendingRecords` source metric across subtasks, from the
          Flink REST API (records fetched-from-Kafka-but-not-yet-emitted = lag).
  Spark:  Kafka log-end-offset − Spark's latest batch `endOffset` (its read
          position), parsed from the driver log's StreamingQueryProgress JSON.

Both are "records sitting in Kafka the engine hasn't consumed yet". Flat = keeps
up; rising = falling behind. We still record committed rows for throughput, but
`source_lag` is the headline.

Usage: sample_lag.py --run <run> --engine flink|spark [--flink-job <jid>] \
        [--spark-log /tmp/spark_<run>.log] --table demo.streaming.events --seconds 240
"""
import argparse
import csv
import json
import os
import re
import subprocess
import time

import requests

RESULTS = os.path.join(os.path.dirname(__file__), "..", "results")
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "kafka")
TOPIC = os.getenv("SOURCE_TOPIC", "events")


def _env(name, default):
    v = os.getenv(name)  # empty string (from `set -a; source .env`) → unset
    return v if v else default


REST_URI = _env("CATALOG_URI_HOST", "http://localhost:8181")
FLINK_URI = _env("FLINK_URI_HOST", "http://localhost:8081")


def kafka_end_offsets():
    """dict partition->end offset, and the total, for the source topic."""
    out = subprocess.run(
        ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-get-offsets.sh",
         "--bootstrap-server", "localhost:9092", "--topic", TOPIC, "--time", "-1"],
        capture_output=True, text=True, timeout=30).stdout
    parts = {}
    for line in out.strip().splitlines():
        f = line.split(":")
        if len(f) == 3:
            parts[int(f[1])] = int(f[2])
    return parts


# ---------- Flink: pendingRecords (commit-independent source lag) ----------
_flink_src_vertex = {"jid": None, "vid": None, "metric_ids": None}


def flink_source_lag(jid):
    """Sum pendingRecords across source subtasks via the Flink REST metrics API."""
    if not jid:
        return None
    try:
        # Re-discover until the metrics actually appear: pendingRecords registers
        # lazily ~15s after job start (once records flow), so we must NOT cache an
        # empty list — retry the discovery every poll until it's populated.
        if _flink_src_vertex["jid"] != jid or not _flink_src_vertex["metric_ids"]:
            plan = requests.get(f"{FLINK_URI}/jobs/{jid}", timeout=10).json()
            vid = next(v["id"] for v in plan["vertices"] if "source" in v["name"].lower())
            base = f"{FLINK_URI}/jobs/{jid}/vertices/{vid}/metrics"
            ids = [m["id"] for m in requests.get(base, timeout=10).json()
                   if m["id"].endswith("pendingRecords")]
            _flink_src_vertex.update(jid=jid, vid=vid, metric_ids=ids)
        vid = _flink_src_vertex["vid"]
        ids = _flink_src_vertex["metric_ids"]
        if not ids:
            return None  # not registered yet; try again next poll
        base = f"{FLINK_URI}/jobs/{jid}/vertices/{vid}/metrics"
        q = "?" + "&".join("get=" + i for i in ids)
        vals = requests.get(base + q, timeout=10).json()
        return int(sum(float(v["value"]) for v in vals if v.get("value") not in (None, "")))
    except Exception as e:
        print(f"[lag] flink source-lag error: {e}")
        return None


# ---------- Spark: kafka end-offset − latest batch endOffset (read position) ----------
def spark_source_lag(spark_log, end_offsets):
    """Kafka end offsets minus Spark's latest committed read position per partition."""
    if not spark_log or not os.path.exists(spark_log):
        return None
    try:
        txt = open(spark_log, errors="ignore").read()
        # find the LAST endOffset block for the events topic
        # progress JSON: "endOffset" : { "events" : { "0": N, "1": N, ... } }
        matches = list(re.finditer(r'"endOffset"\s*:\s*\{\s*"' + re.escape(TOPIC) + r'"\s*:\s*\{([^}]*)\}', txt))
        if not matches:
            return None
        body = matches[-1].group(1)
        pos = {int(p): int(o) for p, o in re.findall(r'"(\d+)"\s*:\s*(\d+)', body)}
        # lag = sum over partitions of (kafka_end - spark_read_pos)
        lag = 0
        for p, end in end_offsets.items():
            lag += max(0, end - pos.get(p, 0))
        return lag
    except Exception as e:
        print(f"[lag] spark source-lag error: {e}")
        return None


def committed_rows(ident):
    """Rows committed to Iceberg so far (for throughput; NOT used for lag)."""
    ns, tbl = ident[-2], ident[-1]
    try:
        r = requests.get(f"{REST_URI}/v1/namespaces/{ns}/tables/{tbl}", timeout=15)
        if r.status_code != 200:
            return 0
        snaps = r.json().get("metadata", {}).get("snapshots", [])
        return sum(int(s.get("summary", {}).get("added-records", 0) or 0) for s in snaps)
    except Exception:
        return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--engine", choices=["flink", "spark"], required=True)
    ap.add_argument("--flink-job", default="")
    ap.add_argument("--spark-log", default="")
    ap.add_argument("--seconds", type=int, default=int(os.getenv("RUN_SECONDS", "240")))
    ap.add_argument("--poll", type=float, default=5.0)
    args = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)
    ident = tuple(args.table.split("."))

    path = os.path.join(RESULTS, f"{args.run}.lag.csv")
    start = time.time()
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_s", "produced", "source_lag", "committed"])
        while time.time() - start < args.seconds:
            t = round(time.time() - start, 1)
            try:
                ends = kafka_end_offsets()
                produced = sum(ends.values())
                if args.engine == "flink":
                    lag = flink_source_lag(args.flink_job)
                else:
                    lag = spark_source_lag(args.spark_log, ends)
                comm = committed_rows(ident)
                w.writerow([t, produced, lag if lag is not None else "", comm])
                f.flush()
                print(f"[lag] t={t}s produced={produced} source_lag={lag} committed={comm}")
            except Exception as e:
                print(f"[lag] sample error: {e}")
            time.sleep(args.poll)
    print(f"[lag] wrote {path}")


if __name__ == "__main__":
    main()
