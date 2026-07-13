#!/usr/bin/env python3
"""Aggregate a sweep into the better/faster/cheaper comparison + charts.

Consumes, per run:
  results/<run>.summary.json   throughput, latency pctiles, file sizing, correctness
  results/<run>.lag.csv        pipeline lag time series (does it keep up?)
  results/<run>.stats.csv      per-container CPU% / mem (cost)

Emits:
  results/comparison.md        the table for the blog
  docs/charts/*.png            lag, latency, small-files, resources, throughput
"""
import glob
import json
import os

import pandas as pd

ROOT = os.path.join(os.path.dirname(__file__), "..")
RESULTS = os.path.join(ROOT, "results")
CHARTS = os.path.join(ROOT, "docs", "charts")
os.makedirs(CHARTS, exist_ok=True)


def load_runs():
    runs = {}
    for sp in glob.glob(os.path.join(RESULTS, "*.summary.json")):
        run = os.path.basename(sp)[: -len(".summary.json")]
        d = {"summary": json.load(open(sp))}
        for kind in ("lag", "stats", "snapshots"):
            p = os.path.join(RESULTS, f"{run}.{kind}.csv")
            if os.path.exists(p) and os.path.getsize(p) > 0:
                try:
                    d[kind] = pd.read_csv(p)
                except Exception:
                    pass
        runs[run] = d
    return runs


def lag_verdict(lag_df):
    """Does the engine keep up UNDER SUSTAINED LOAD?

    The honest test is the slope of the backlog *while producers are still
    running*: if pipeline_lag grows steadily, the engine can't keep pace at this
    rate (it only ever "catches up" because the producer stops — which we must
    ignore). We fit a line to the lag over the load window and report:
      lag_slope_rps  — records/s the backlog grows (>0 and large => falling behind)
      peak_lag       — worst backlog seen
    """
    if lag_df is None or "source_lag" not in lag_df or len(lag_df) < 6:
        return None, None, None
    # empty cells (warmup, before source metrics register) read as "" OR NaN
    lag_df = lag_df.copy()
    lag_df["source_lag"] = pd.to_numeric(lag_df["source_lag"], errors="coerce")
    lag_df = lag_df.dropna(subset=["source_lag"])
    if len(lag_df) < 6:
        return None, None, None
    # Load window = while `produced` is still increasing (producers active).
    prod = lag_df["produced"].astype(float).values
    t = lag_df["t_s"].astype(float).values
    lag = lag_df["source_lag"].astype(float).values
    # last index where produced still rising
    load_end = 0
    for i in range(1, len(prod)):
        if prod[i] > prod[i - 1]:
            load_end = i
    if load_end < 4:
        load_end = len(prod) - 1
    tw, lw = t[:load_end + 1], lag[:load_end + 1]
    # linear slope (records/s) of the backlog during the load window
    try:
        slope = float(((tw - tw.mean()) * (lw - lw.mean())).sum() / ((tw - tw.mean()) ** 2).sum())
    except Exception:
        slope = 0.0
    peak = int(lag.max())
    # keeps up if the backlog isn't growing meaningfully during load
    keeps_up = slope < 2000  # <2k rec/s drift ≈ noise
    return peak, int(slope), keeps_up


def resource_cost(stats_df, engine_pat):
    """Return (active_cpu_pct, peak_cpu_pct, mem_mb).

    active_cpu = median CPU over samples where the engine is actually working
    (>=20% of one core). Time-averaging INCLUDING idle gaps between micro-batches
    unfairly rewards a bursty engine for sitting idle — that isn't 'cheaper', the
    work just hasn't happened yet. Peak reflects the cores you must PROVISION.
    """
    if stats_df is None or "container" not in stats_df:
        return None, None, None
    eng = stats_df[stats_df["container"].str.contains(engine_pat, case=False, na=False)]
    # exclude the spark<->flink name overlap handled by caller's pattern choice
    if not len(eng):
        return None, None, None
    per_ts = eng.groupby("ts")["cpu_pct"].sum()
    active = per_ts[per_ts >= 20.0]
    active_cpu = round(active.median(), 1) if len(active) else round(per_ts.median(), 1)
    peak_cpu = round(per_ts.quantile(0.95), 1)
    mem = round(eng.groupby("ts")["mem_mb"].sum().max(), 1)  # peak provisioned mem
    return active_cpu, peak_cpu, mem


def build_table(runs):
    rows = []
    for run, d in sorted(runs.items()):
        s = d["summary"]
        lat = s.get("visibility_latency_ms", {})
        peak_lag, lag_slope, keeps_up = lag_verdict(d.get("lag"))
        is_spark = run.startswith("spark") or "pyspark" in run
        # NB: the flink taskmanager container name literally contains BOTH "spark"
        # and "flink" (spark-flink-iceberg-flink-taskmanager-1). Match on the
        # unambiguous token so we don't attribute Flink CPU to the Spark run.
        engine_pat = r"\bspark\b(?!-flink)" if is_spark else "flink"
        # Simpler + robust: for spark, the container is exactly named "spark".
        engine_pat = "^spark$" if is_spark else "flink"
        active_cpu, peak_cpu, mem = resource_cost(d.get("stats"), engine_pat)
        corr = s.get("correctness", {})
        rps = s.get("throughput_rps") or 0
        # cost = ACTIVE CPU-core-seconds per million rows (idle gaps excluded).
        cps_per_m = round((active_cpu / 100.0) / (rps / 1e6), 1) if (active_cpu and rps) else None
        rows.append({
            "run": run,
            "keeps_up": ("yes" if keeps_up else "NO") if keeps_up is not None else "?",
            "lag_slope_rps": lag_slope,   # backlog growth under load (>0 big = behind)
            "peak_lag": peak_lag,
            "commit_rps": rps,            # sustained committed throughput
            "lat_p50": lat.get("p50"), "lat_p95": lat.get("p95"),
            "avg_file_mb": s.get("avg_file_mb"),
            "files": s.get("files_total"),
            "active_cpu%": active_cpu, "peak_cpu%": peak_cpu, "peak_mem_mb": mem,
            "active_core_s/1M": cps_per_m,
            "exactly_once": corr.get("exactly_once_ok") if corr else "?",
        })
    return pd.DataFrame(rows)


def write_md(df):
    out = os.path.join(RESULTS, "comparison.md")
    with open(out, "w") as f:
        f.write("# Streaming into Iceberg — Spark vs Flink (local sweep)\n\n")
        f.write("**keeps_up** = pipeline lag stayed flat (yes) or grew (NO). ")
        f.write("**lat_*** = Iceberg visibility latency ms. ")
        f.write("**core_s/1M** = CPU-core-seconds per million rows (cheaper = lower).\n\n")
        f.write(df.to_markdown(index=False))
        f.write("\n")
    print(f"[analyze] wrote {out}\n")
    print(df.to_string(index=False))


def make_charts(df, runs):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not len(df):
        print("[analyze] no runs")
        return

    # 1) Lag over time — THE headline chart (does it keep up?)
    fig, ax = plt.subplots(figsize=(11, 6))
    for run, d in sorted(runs.items()):
        lg = d.get("lag")
        if lg is not None and "source_lag" in lg:
            lg2 = lg[lg["source_lag"] != ""].copy()
            lg2["source_lag"] = lg2["source_lag"].astype(float)
            ax.plot(lg2["t_s"], lg2["source_lag"] / 1000.0, marker=".", label=run)
    ax.set_xlabel("time (s)"); ax.set_ylabel("pipeline lag (thousands of records)")
    ax.set_title("Kafka→Iceberg lag over time — flat = keeps up, rising = falling behind")
    ax.legend(fontsize=8); ax.grid(alpha=0.3); fig.tight_layout()
    fig.savefig(os.path.join(CHARTS, "lag.png"), dpi=140)

    x = range(len(df))
    # 2) latency percentiles
    fig, ax = plt.subplots(figsize=(11, 5))
    w = 0.35
    for i, p in enumerate(["lat_p50", "lat_p95"]):
        ax.bar([xx + (i - 0.5) * w for xx in x], df[p].fillna(0), w, label=p)
    ax.set_xticks(range(len(df))); ax.set_xticklabels(df["run"], rotation=30, ha="right")
    ax.set_ylabel("visibility latency (ms)"); ax.set_title("Iceberg visibility latency")
    ax.legend(); fig.tight_layout(); fig.savefig(os.path.join(CHARTS, "latency.png"), dpi=140)

    # 3) file sizing
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(df["run"], df["avg_file_mb"].fillna(0), color="tab:purple")
    ax.set_ylabel("avg data-file size (MB)"); ax.set_title("File sizing (bigger = fewer small files)")
    ax.set_xticks(range(len(df))); ax.set_xticklabels(df["run"], rotation=30, ha="right"); fig.tight_layout()
    fig.savefig(os.path.join(CHARTS, "small_files.png"), dpi=140)

    # 4) cost: active vs peak CPU
    fig, ax1 = plt.subplots(figsize=(11, 5))
    w = 0.35
    ax1.bar([xx - w / 2 for xx in x], df["active_cpu%"].fillna(0), w, color="tab:blue", alpha=0.8, label="active CPU%")
    ax1.bar([xx + w / 2 for xx in x], df["peak_cpu%"].fillna(0), w, color="tab:orange", alpha=0.8, label="peak CPU%")
    ax1.set_ylabel("CPU % (sum of cores)"); ax1.set_title("Resource cost — active vs peak CPU")
    ax1.legend(); ax1.set_xticks(range(len(df))); ax1.set_xticklabels(df["run"], rotation=30, ha="right"); fig.tight_layout()
    fig.savefig(os.path.join(CHARTS, "resources.png"), dpi=140)

    # 5) throughput
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(df["run"], df["commit_rps"].fillna(0), color="tab:green")
    ax.set_ylabel("committed rows/s"); ax.set_title("Sustained committed throughput")
    ax.set_xticks(range(len(df))); ax.set_xticklabels(df["run"], rotation=30, ha="right"); fig.tight_layout()
    fig.savefig(os.path.join(CHARTS, "throughput.png"), dpi=140)
    print(f"[analyze] wrote charts → {CHARTS}/")


if __name__ == "__main__":
    runs = load_runs()
    df = build_table(runs)
    write_md(df)
    make_charts(df, runs)
