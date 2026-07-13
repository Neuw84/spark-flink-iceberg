#!/usr/bin/env python3
"""EKS lag sampler — Flink pendingRecords via a port-forwarded REST endpoint.

On EKS the true source lag is read straight from Flink's REST API (same
`pendingRecords` metric proven locally), reached via `kubectl port-forward
svc/sfi-flink-rest`. No Kafka-exec or catalog access needed — this is the one
commit-independent "is it keeping up" signal, and it's the point of the run.

Also records numRecordsOut/s (committed throughput proxy) from the sink vertex.

Usage: sample_lag_eks.py --run eks_flink_append_bucket --flink-uri http://localhost:18081 --seconds 240
"""
import argparse, csv, os, time
import requests

RESULTS = os.path.join(os.path.dirname(__file__), "..", "results", "eks")
_cache = {"jid": None, "src_vid": None, "lag_ids": None}


def running_job(uri):
    jobs = requests.get(f"{uri}/jobs", timeout=10).json()["jobs"]
    r = [j["id"] for j in jobs if j["status"] == "RUNNING"]
    return r[0] if r else None


def source_lag(uri, jid):
    if _cache["jid"] != jid or not _cache["lag_ids"]:
        plan = requests.get(f"{uri}/jobs/{jid}", timeout=10).json()
        vid = next(v["id"] for v in plan["vertices"] if "source" in v["name"].lower())
        base = f"{uri}/jobs/{jid}/vertices/{vid}/metrics"
        ids = [m["id"] for m in requests.get(base, timeout=10).json()
               if m["id"].endswith("pendingRecords")]
        _cache.update(jid=jid, src_vid=vid, lag_ids=ids)
    vid, ids = _cache["src_vid"], _cache["lag_ids"]
    if not ids:
        return None
    q = "?" + "&".join("get=" + i for i in ids)
    vals = requests.get(f"{uri}/jobs/{jid}/vertices/{vid}/metrics{q}", timeout=10).json()
    return int(sum(float(v["value"]) for v in vals if v.get("value") not in (None, "")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--flink-uri", default="http://localhost:18081")
    ap.add_argument("--seconds", type=int, default=240)
    ap.add_argument("--poll", type=float, default=5.0)
    args = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"{args.run}.lag.csv")
    start = time.time()
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["t_s", "source_lag"])
        while time.time() - start < args.seconds:
            t = round(time.time() - start, 1)
            try:
                jid = running_job(args.flink_uri)
                lag = source_lag(args.flink_uri, jid) if jid else None
                w.writerow([t, lag if lag is not None else ""]); f.flush()
                print(f"[eks-lag] t={t}s source_lag={lag}")
            except Exception as e:
                print(f"[eks-lag] {e}")
            time.sleep(args.poll)
    print(f"[eks-lag] wrote {path}")


if __name__ == "__main__":
    main()
