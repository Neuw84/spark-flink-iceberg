#!/usr/bin/env python3
"""Generate the blog charts from the EKS 30-min matrix (headline) + local matrix.

Design-system palette (validated colorblind-safe): Flink = blue #2a78d6,
Spark = orange #eb6834, held CONSTANT across every chart so color = engine identity.
Upsert modes are distinguished by marker/linestyle, not new hues. Direct labels on
every series (secondary encoding, required by the contrast WARN).

Outputs → docs/charts/*.png (light background, Medium-friendly).
"""
import csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

FLINK = "#2a78d6"
SPARK = "#eb6834"
INK = "#383835"; MUTED = "#8a8a86"; GRID = "#e6e5e1"; SURFACE = "#fcfcfb"
plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK, "text.color": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.8, "font.size": 11,
    "axes.spines.top": False, "axes.spines.right": False,
})
EKS = "results/eks/matrix_100k_30min"
LOC = "results/local"


def load(path):
    return list(csv.DictReader(open(path)))


def millions(x, _):
    return f"{x/1e6:.0f}M" if x >= 1e6 else f"{x/1e3:.0f}k"


def save(fig, name):
    os.makedirs("docs/charts", exist_ok=True)
    fig.tight_layout()
    fig.savefig(f"docs/charts/{name}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  docs/charts/{name}.png")


# 1. LAG OVER TIME — append: Flink vs Spark both keep up (produced vs landed).
def chart_lag_append():
    fig, ax = plt.subplots(figsize=(9, 5))
    for base, color, lab in [("flink_append_bucket", FLINK, "Flink"),
                             ("spark_append_bucket", SPARK, "Spark")]:
        r = load(f"{EKS}/{base}.lag.csv")
        t = [int(x["t_s"]) / 60 for x in r]
        lag = [int(x["lag"]) / 1e6 for x in r]
        ax.plot(t, lag, color=color, lw=2, label=lab)
        ax.annotate(lab, (t[-1], lag[-1]), color=color, fontsize=11, fontweight="bold",
                    xytext=(6, 0), textcoords="offset points", va="center")
    ax.set_title("Append: Kafka backlog over 30 min (100k rows/s, 14c/28GB)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_xlabel("minutes"); ax.set_ylabel("backlog (M rows)")
    ax.set_xlim(0, 32)
    fig.text(0.01, -0.02, "Both engines hold a flat sawtooth (one 60s commit ≈ 6M rows) — neither falls behind on plain append.",
             fontsize=9, color=MUTED)
    save(fig, "lag_append")


# 2. LAG OVER TIME — upsert: Flink DV flat, Spark MoR/CoW diverge.
def chart_lag_upsert():
    fig, ax = plt.subplots(figsize=(9, 5))
    series = [("flink_upsert_bucket", FLINK, "-", "Flink DV"),
              ("spark_upsert_bucket_mor", SPARK, "--", "Spark MoR"),
              ("spark_upsert_bucket_cow", SPARK, ":", "Spark CoW")]
    for base, color, ls, lab in series:
        r = load(f"{EKS}/{base}.lag.csv")
        t = [int(x["t_s"]) / 60 for x in r]
        lag = [int(x["lag"]) / 1e6 for x in r]
        ax.plot(t, lag, color=color, lw=2, ls=ls, label=lab)
        ax.annotate(lab, (t[-1], lag[-1]), color=color, fontsize=10, fontweight="bold",
                    xytext=(6, 0), textcoords="offset points", va="center")
    ax.set_title("Upsert: Kafka backlog over 30 min (100k rows/s, 14c/28GB)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_xlabel("minutes"); ax.set_ylabel("backlog (M rows)")
    ax.set_xlim(0, 34)
    fig.text(0.01, -0.02, "Flink's deletion-vector upsert stays flat; Spark merge-on-read and copy-on-write both fall behind (backlog grows without bound).",
             fontsize=9, color=MUTED)
    save(fig, "lag_upsert")


# 3. THROUGHPUT — committed rows/s bar chart, all 5 cells.
def chart_throughput():
    cells = [("Flink\nappend", "flink_append_bucket", FLINK),
             ("Spark\nappend", "spark_append_bucket", SPARK),
             ("Flink\nupsert (DV)", "flink_upsert_bucket", FLINK),
             ("Spark\nupsert (MoR)", "spark_upsert_bucket_mor", SPARK),
             ("Spark\nupsert (CoW)", "spark_upsert_bucket_cow", SPARK)]
    rates = []
    for _, base, _ in cells:
        r = load(f"{EKS}/{base}.lag.csv")
        lr = [(int(x["t_s"]), int(x["landed"])) for x in r if int(x["landed"]) > 0]
        rates.append((lr[-1][1] - lr[0][1]) / (lr[-1][0] - lr[0][0]) / 1e3)
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = range(len(cells))
    bars = ax.bar(xs, rates, color=[c for _, _, c in cells], width=0.62, zorder=3)
    ax.axhline(100, color=MUTED, lw=1, ls="--", zorder=2)
    ax.annotate("100k input", (4.4, 100), color=MUTED, fontsize=9, va="bottom", ha="right")
    for i, v in enumerate(rates):
        ax.annotate(f"{v:.0f}k", (i, v), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontweight="bold", color=INK, fontsize=11)
    ax.set_xticks(list(xs)); ax.set_xticklabels([c[0] for c in cells])
    ax.set_ylabel("committed rows/s (thousands)")
    ax.set_title("Sustained throughput into Iceberg (30 min, equal 14c/28GB)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_ylim(0, 115)
    fig.text(0.01, -0.02, "Append ties near the 100k input rate; Flink's DV upsert holds it, while Spark's upsert modes collapse.",
             fontsize=9, color=MUTED)
    save(fig, "throughput")


# 4. MEMORY over time — Spark buffers, Flink flat.
def chart_memory():
    fig, ax = plt.subplots(figsize=(9, 5))
    for base, color, ls, lab in [("flink_append_bucket", FLINK, "-", "Flink append"),
                                 ("spark_append_bucket", SPARK, "-", "Spark append"),
                                 ("spark_upsert_bucket_mor", SPARK, "--", "Spark upsert (MoR)")]:
        r = load(f"{EKS}/{base}.lag.csv")
        t = [int(x["t_s"]) / 60 for x in r]
        mem = [int(x["mem_mb"]) / 1024 for x in r]
        ax.plot(t, mem, color=color, lw=2, ls=ls, label=lab)
        ax.annotate(lab, (t[-1], mem[-1]), color=color, fontsize=9.5, fontweight="bold",
                    xytext=(6, 0), textcoords="offset points", va="center")
    ax.set_title("Worker memory over 30 min (14c/28GB envelope)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_xlabel("minutes"); ax.set_ylabel("worker memory (GB)")
    ax.set_xlim(0, 40)
    fig.text(0.01, -0.02, "Flink holds ~17 GB steady; Spark runs hotter (~20 GB append) and pins ~31 GB on upsert as it buffers the growing backlog.",
             fontsize=9, color=MUTED)
    save(fig, "memory")


# 5. COMMIT INTERVAL — Flink flat 60s vs Spark CoW stretching.
def chart_commit_interval():
    fig, ax = plt.subplots(figsize=(9, 5))
    for base, color, ls, lab in [("flink_append_bucket", FLINK, "-", "Flink append"),
                                 ("spark_append_bucket", SPARK, "-", "Spark append"),
                                 ("spark_upsert_bucket_cow", SPARK, ":", "Spark upsert (CoW)")]:
        p = f"{EKS}/{base}.snapshots.csv"
        if not os.path.exists(p):
            continue
        r = [x for x in load(p) if float(x["interval_s"]) > 0]
        idx = [int(x["idx"]) for x in r]
        iv = [float(x["interval_s"]) for x in r]
        ax.plot(idx, iv, color=color, lw=2, ls=ls, marker="o", ms=4, label=lab)
    ax.axhline(60, color=MUTED, lw=1, ls="--")
    ax.annotate("60s trigger", (1, 60), color=MUTED, fontsize=9, va="bottom")
    ax.legend(frameon=False, loc="upper left")
    ax.set_title("Time between Iceberg commits", fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_xlabel("commit #"); ax.set_ylabel("interval (s)")
    fig.text(0.01, -0.02, "Flink commits on a metronome-flat 60s; Spark's copy-on-write MERGE can't finish in the window, so commits stretch past 60s.",
             fontsize=9, color=MUTED)
    save(fig, "commit_interval")


# 6. FILE SIZE — avg MB per cell.
def chart_file_size():
    cells = [("Flink append", "flink_append_bucket", FLINK),
             ("Spark append", "spark_append_bucket", SPARK),
             ("Flink upsert", "flink_upsert_bucket", FLINK),
             ("Spark MoR", "spark_upsert_bucket_mor", SPARK),
             ("Spark CoW", "spark_upsert_bucket_cow", SPARK)]
    vals = []
    for _, base, _ in cells:
        fd = dict(t.split("=", 1) for t in open(f"{EKS}/{base}.files.txt").read().split() if "=" in t)
        vals.append(float(fd.get("avg_mb", 0)))
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = range(len(cells))
    ax.bar(xs, vals, color=[c for _, _, c in cells], width=0.6, zorder=3)
    for i, v in enumerate(vals):
        ax.annotate(f"{v:.0f} MB", (i, v), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontweight="bold", color=INK, fontsize=11)
    ax.set_xticks(list(xs)); ax.set_xticklabels([c[0] for c in cells], fontsize=9)
    ax.set_ylabel("avg data file size (MB)")
    ax.set_title("Average Iceberg data-file size (24 buckets, 60s commits)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    fig.text(0.01, -0.02, "Similar file sizes where engines keep up; Spark MoR's small files come from writing delete files it can't keep pace with.",
             fontsize=9, color=MUTED)
    save(fig, "file_size")


# 7. SPARK BATCH DURATION breakdown — where each micro-batch's time goes.
def chart_batch_breakdown():
    p = f"{EKS}/spark_append_bucket.batches.csv"
    if not os.path.exists(p):
        return
    r = [x for x in load(p) if x["addBatch_ms"] not in ("", "None")][1:]  # skip batch 0
    if not r:
        return
    # average the components
    def avg(k):
        vals = [float(x[k]) for x in r if x[k] not in ("", "None")]
        return sum(vals) / len(vals) / 1000 if vals else 0
    parts = [("addBatch\n(Iceberg write+flush)", avg("addBatch_ms"), SPARK),
             ("walCommit", avg("walCommit_ms"), "#c9a15f"),
             ("getBatch", avg("getBatch_ms"), "#9a9a96"),
             ("queryPlanning", avg("queryPlanning_ms"), "#c4c4bf")]
    total = sum(v for _, v, _ in parts)
    fig, ax = plt.subplots(figsize=(9, 3.6))
    left = 0
    handles = []
    for lab, v, c in parts:
        b = ax.barh(0, v, left=left, color=c, height=0.5, zorder=3,
                    edgecolor=SURFACE, linewidth=2)
        pct = v / total * 100
        # Only label INSIDE a segment wide enough to hold text; else rely on legend.
        if v > total * 0.15:
            ax.annotate(f"{lab.split(chr(10))[0]}\n{v:.1f}s ({pct:.0f}%)", (left + v / 2, 0),
                        ha="center", va="center", fontsize=10, color="white", fontweight="bold")
        handles.append((b, f"{lab.split(chr(10))[0]} — {v:.1f}s ({pct:.0f}%)"))
        left += v
    ax.legend([h for h, _ in handles], [t for _, t in handles],
              frameon=False, loc="lower center", bbox_to_anchor=(0.5, -0.55),
              ncol=2, fontsize=9)
    ax.set_yticks([]); ax.set_xlabel("seconds per micro-batch")
    ax.set_title("Where a Spark micro-batch spends its time (append)",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    ax.set_xlim(0, total * 1.02)
    fig.text(0.01, -0.02, "The Iceberg write (addBatch) dominates — the commit path, not compute, is the bottleneck.",
             fontsize=9, color=MUTED)
    save(fig, "batch_breakdown")


# 8. LOCAL vs EKS — throughput ratio holds at half scale.
def chart_local_vs_eks():
    cells = [("Flink\nappend", "flink_append_bucket"),
             ("Spark\nappend", "spark_append_bucket"),
             ("Flink\nupsert", "flink_upsert_bucket"),
             ("Spark\nMoR", "spark_upsert_bucket_mor"),
             ("Spark\nCoW", "spark_upsert_bucket_cow")]

    def rate(path):
        r = load(path); lr = [(int(x["t_s"]), int(x["landed"])) for x in r if int(x["landed"]) > 0]
        return (lr[-1][1] - lr[0][1]) / (lr[-1][0] - lr[0][0]) / 1e3

    loc = [rate(f"{LOC}/local_{b}.lag.csv") for _, b in cells]
    eks = [rate(f"{EKS}/{b}.lag.csv") for _, b in cells]
    fig, ax = plt.subplots(figsize=(9, 5))
    xs = range(len(cells)); w = 0.38
    ax.bar([x - w/2 for x in xs], loc, width=w, color="#9a9a96", zorder=3, label="Local (7c/14GB, 50k)")
    ax.bar([x + w/2 for x in xs], eks, width=w, color=FLINK, zorder=3, label="EKS (14c/28GB, 100k)")
    for i in xs:
        ax.annotate(f"{loc[i]:.0f}k", (i - w/2, loc[i]), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=8.5, color=INK)
        ax.annotate(f"{eks[i]:.0f}k", (i + w/2, eks[i]), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=8.5, color=INK)
    ax.set_xticks(list(xs)); ax.set_xticklabels([c[0] for c in cells])
    ax.set_ylabel("committed rows/s (thousands)")
    ax.legend(frameon=False, loc="upper right")
    ax.set_title("Same story at half scale: local mirrors EKS",
                 fontsize=13, fontweight="bold", color=INK, loc="left")
    fig.text(0.01, -0.02, "Local (50k on 7c) reproduces every EKS (100k on 14c) verdict; the keep-up cells hold the ~2:1 rate ratio (CoW's absolute rate is noisy at short window, but both fall behind).",
             fontsize=8.5, color=MUTED)
    save(fig, "local_vs_eks")


if __name__ == "__main__":
    print("generating charts →")
    chart_lag_append(); chart_lag_upsert(); chart_throughput(); chart_memory()
    chart_commit_interval(); chart_file_size(); chart_batch_breakdown(); chart_local_vs_eks()
    print("done.")
