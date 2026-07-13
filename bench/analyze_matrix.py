#!/usr/bin/env python3
"""Analyze a benchmark matrix (local or EKS) into a comparison table + chart CSVs.

Keep-up verdict uses the POST-COMMIT LAG FLOOR TREND: the lag sampled right after
each new Iceberg snapshot is the low point of that commit cycle; the linear trend
of those floors tells us whether the engine's steady-state backlog is growing
(falling behind) or flat/shrinking (keeping up). Fitting ALL samples is wrong —
the warmup ramp and the intra-cycle sawtooth pollute the slope.

Usage: analyze_matrix.py <results_dir> <prefix> <label>
  e.g. analyze_matrix.py results/local            local_   "LOCAL 5-min 50k"
       analyze_matrix.py results/eks/matrix_100k_30min "" "EKS 30-min 100k"
"""
import csv, os, sys

CELLS = [
    ("Flink append",      "flink_append_bucket"),
    ("Spark append",      "spark_append_bucket"),
    ("Flink upsert (DV)", "flink_upsert_bucket"),
    ("Spark upsert (MoR)","spark_upsert_bucket_mor"),
    ("Spark upsert (CoW)","spark_upsert_bucket_cow"),
]


def analyze(path):
    r = list(csv.DictReader(open(path)))
    lr = [(int(x["t_s"]), int(x["landed"])) for x in r if int(x["landed"]) > 0]
    if len(lr) < 2:
        return None
    commit_rate = (lr[-1][1] - lr[0][1]) / (lr[-1][0] - lr[0][0])
    # post-commit floors: first row at each new snapshot count
    floors, prev = [], None
    for x in r:
        s = int(x["snapshots"])
        if s > 0 and s != prev:
            floors.append((int(x["t_s"]), int(x["lag"])))
            prev = s
    slope = None
    if len(floors) >= 3:
        ts = [f[0] for f in floors]; lg = [f[1] for f in floors]; n = len(ts)
        sx, sy = sum(ts), sum(lg)
        sxx = sum(t * t for t in ts); sxy = sum(ts[i] * lg[i] for i in range(n))
        slope = (n * sxy - sx * sy) / (n * sxx - sx * sx)
    cpus = sorted(float(x["cpu_cores"]) for x in r if float(x["cpu_cores"]) > 0.5)
    cpu = cpus[len(cpus) // 2] if cpus else 0
    mem = max((int(x["mem_mb"]) for x in r if int(x["mem_mb"]) > 0), default=0)
    snaps = max(int(x["snapshots"]) for x in r)
    return dict(commit_rate=commit_rate, slope=slope, cpu=cpu, mem=mem, snaps=snaps,
                floor_first=floors[0][1] if floors else 0,
                floor_last=floors[-1][1] if floors else 0)


def files_of(d, base):
    p = f"{d}/{base}.files.txt"
    if not os.path.exists(p):
        return {}
    return dict(t.split("=", 1) for t in open(p).read().split() if "=" in t)


def main():
    d, prefix, label = sys.argv[1], sys.argv[2], sys.argv[3]
    print(f"=== {label} — keep-up by post-commit lag-floor trend ===")
    hdr = f"{'CELL':<20}{'commit/s':>9}{'floor_slope':>12}{'verdict':>14}{'files':>6}{'avgMB':>7}{'CPU':>5}{'mem_MB':>8}"
    print(hdr); print("-" * len(hdr))
    rows = []
    for name, base in CELLS:
        p = f"{d}/{prefix}{base}.lag.csv"
        if not os.path.exists(p):
            print(f"{name:<20} (missing)"); continue
        a = analyze(p)
        if not a:
            print(f"{name:<20} (no data)"); continue
        fd = files_of(d, prefix + base)
        sl = a["slope"]
        verdict = "?" if sl is None else ("KEEPS UP" if sl < 300 else "FALLS BEHIND")
        sls = "n/a" if sl is None else f"{sl:+,.0f}"
        print(f"{name:<20}{a['commit_rate']:>9,.0f}{sls:>12}{verdict:>14}"
              f"{fd.get('files','?'):>6}{fd.get('avg_mb','?'):>7}{a['cpu']:>5.1f}{a['mem']:>8,}")
        rows.append((name, base, a, fd, verdict))
    return rows


if __name__ == "__main__":
    main()
