"""Shared loading, aggregation and styling for the experiment plots.

Each experiment directory written by `scripts/run_experiments.py` holds
`results.csv` (one record per trial) and, for right_cash trials,
`breakdown.csv` (per-phase propose/commit timings). Every configuration
runs with three seeds shared across targets; the plots show the median
over seeds with min-max whiskers, and each script prints the same numbers
as a table and writes it next to the figures.
"""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESULTS = ROOT / "benchmark" / "results" / "2026-11-07-interleaved"

# Reference categorical palette, fixed slot order (light mode).
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"

# The system's name in figure text.
NAME = "right_cash"

# One color per entity, the same in every figure.
TARGETS = {
    "firewood": ("firewood (bare)", SLOTS[0]),
    "right_cash": ("right_cash", SLOTS[1]),
    "right_cash_spec": ("right_cash + precompute", SLOTS[2]),
}

# Workload names as figure text, in panel order (left to right).
WORKLOADS = {"tenkrandom": "tenkrandom", "zipf": "zipf"}

# What every figure plots: propose latency (the critical path), mean and
# p99. Prefetch runs off the critical path and is left to the tables.
PROPOSE = ("Propose latency", ["propose_mean_ms", "propose_p99_ms"])

# `--blog`: figures for the blog post (qusuyan.github.io). Dark surface and
# text matching the site (#222831, #eee), the reference palette's dark steps
# (each >= 3.8:1 on that surface), larger type for a ~1000 px column, the
# post's names for targets and workloads, and no setup subtitle (the post's
# caption carries the setup). PNG only, no summary CSV.
BLOG = False
BLOG_THEME = {
    "SLOTS": ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"],
    "INK": "#eeeeee",
    "INK_2": "#c3c2b7",
    "MUTED": "#9a9a9a",
    "GRID": "#393e46",
    "AXIS": "#5b616b",
    "SURFACE": "#222831",
}
BLOG_NAME = "RightCash"
BLOG_LABELS = {"firewood": "Firewood (bare)", "right_cash": f"{BLOG_NAME} w/o Precomputation",
               "right_cash_spec": f"{BLOG_NAME} w/ Precomputation"}
BLOG_WORKLOADS = {"tenkrandom": "Mixed", "zipf": "Zipf"}


def args(description, default_subdir):
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--results", type=Path, default=DEFAULT_RESULTS,
                    help="directory written by scripts/run_experiments.py")
    ap.add_argument("--out", type=Path, default=None, help="output directory (default: <results>/plots)")
    ap.add_argument("--blog", action="store_true", help="dark, larger, captionless figures for the blog post")
    a = ap.parse_args()
    a.dir = a.results / default_subdir
    a.out = a.out or a.results / "plots"
    a.out.mkdir(parents=True, exist_ok=True)
    if a.blog:
        use_blog_theme()
    return a


def use_blog_theme():
    global BLOG, NAME, TARGETS, WORKLOADS
    BLOG = True
    NAME = BLOG_NAME
    globals().update(BLOG_THEME)
    TARGETS = {k: (BLOG_LABELS[k], SLOTS[i]) for i, k in enumerate(BLOG_LABELS)}
    WORKLOADS = dict(BLOG_WORKLOADS)


def subtitle(fig, text, **kw):
    """The setup line above a figure; omitted for the blog."""
    if not BLOG:
        fig.suptitle(text, color=INK_2, **kw)


def load(exp_dir):
    df = pd.read_csv(exp_dir / "results.csv")
    # Results recorded before the 2026-11-09 rename call the system
    # auth_wal, in trial ids and the target column.
    df["target"] = df["target"].str.replace("auth_wal", "right_cash", regex=False)
    # Trial ids end in -<seed>; the configuration is everything before,
    # less the workload prefix of experiments that run on several workloads
    # (`zipf-lru-write-101`; the workload is its own column).
    df["config"] = df["trial"].str.replace("auth_wal", "right_cash", regex=False).str.rsplit("-", n=1).str[0]
    df["config"] = [c.removeprefix(f"{w}-") for c, w in zip(df["config"], df["workload"])]
    ms = [c for c in df.columns if c.endswith("_us")]
    for c in ms:
        df[c.replace("_us", "_ms")] = df[c] / 1000.0
    # Prefetch time per measured block, ms.
    df["hint_per_block_ms"] = df["hint_time_s"] * 1000.0 / df["commits"]
    return df


def summarize(df, by, metrics):
    """Median, min and max over seeds of each metric, per configuration."""
    g = df.groupby(by)[metrics]
    out = pd.concat({"median": g.median(), "min": g.min(), "max": g.max(), "n": g.count()}, axis=1)
    return out


def style():
    big = BLOG
    plt.rcParams.update({
        "font.family": ["Roboto", "DejaVu Sans"] if big else "sans-serif",
        "font.size": 12 if big else 9,
        "axes.facecolor": SURFACE,
        "figure.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "axes.edgecolor": AXIS,
        "axes.labelcolor": INK_2,
        "axes.titlecolor": INK,
        "axes.titlesize": 13 if big else 10,
        "axes.titleweight": "bold",
        "axes.titlepad": 10 if big else 6,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.grid.axis": "y",
        "axes.axisbelow": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelcolor": INK_2,
        "ytick.labelcolor": INK_2,
        "xtick.labelsize": 11 if big else 9,
        "ytick.labelsize": 11 if big else 9,
        "legend.frameon": False,
        "legend.labelcolor": INK_2,
        "legend.fontsize": 12 if big else 9,
        "lines.linewidth": 2,
        "lines.markersize": 6 if big else 5,
    })


def bars(ax, categories, series, values, lo, hi, colors, labels, width=0.8):
    """Grouped bars: one group per category, one bar per series, whiskers
    from lo to hi. values/lo/hi are indexed [series][category]. Bars too
    small to see are labelled "≈0"."""
    n = len(series)
    w = min(width / n, 0.28)
    placed = []
    for i, s in enumerate(series):
        xs = [j + (i - (n - 1) / 2) * w for j in range(len(categories))]
        v = values[s]
        err = [[v[j] - lo[s][j] for j in range(len(v))], [hi[s][j] - v[j] for j in range(len(v))]]
        ax.bar(xs, v, w * 0.92, color=colors[s], label=labels[s], edgecolor=SURFACE, linewidth=1)
        ax.errorbar(xs, v, yerr=err, fmt="none", ecolor=INK_2, elinewidth=1, capsize=2)
        placed += list(zip(xs, v))
    ax.set_xticks(range(len(categories)), categories)
    ax.tick_params(axis="x", length=0)
    ax.set_ylim(bottom=0)
    top = ax.get_ylim()[1]
    for x, y in placed:
        if y < 0.004 * top:
            ax.annotate("≈0", (x, 0), textcoords="offset points", xytext=(0, 2), ha="center",
                        color=INK_2, fontsize=7)


def save(fig, out, name):
    # Blog: PNG at 2x for a ~1000 px column (figures are 10 in wide).
    exts, dpi = (("png",), 200) if BLOG else (("png", "pdf"), 160)
    for ext in exts:
        fig.savefig(out / f"{name}.{ext}", dpi=dpi, bbox_inches="tight", pad_inches=0.15 if BLOG else 0.1)
    plt.close(fig)
    print(f"wrote {out / name}: " + ", ".join(exts))


def table(df, out, name):
    """Print the summary and write it as CSV (the figure's table view)."""
    with pd.option_context("display.width", 200, "display.max_columns", 50, "display.float_format", "{:.3f}".format):
        print(df)
    if not BLOG:
        df.to_csv(out / f"{name}.csv")


def workload_rows(df, by, metrics, order):
    """(workload name, summary indexed by `order`) per workload, in
    WORKLOADS order, for `latency_figure`. The research figures show the
    measured workloads; the blog keeps a panel for every workload, with
    summary None while it is unmeasured."""
    rows = []
    for w, name in WORKLOADS.items():
        sub = df[df["workload"] == w]
        if len(sub):
            rows.append((name, summarize(sub, by, metrics).reindex(order)))
        elif BLOG:
            rows.append((name, None))
    return rows


def latency_figure(rows, order, labels, colors, title):
    """Propose latency, mean and p99: one panel per (workload name,
    summary) in `rows`, side by side, where summary is `summarize` output
    indexed by the configurations in `order`; a summary of None leaves an
    empty panel marked unmeasured. Bars are medians over seeds with
    min-max whiskers."""
    panel, cols = PROPOSE
    size = (10, 4.4) if BLOG else (4.5 * len(rows), 4)
    fig, axes = plt.subplots(1, len(rows), figsize=size, squeeze=False)
    handles = None
    for ax, (name, s) in zip(axes[0], rows):
        ax.set_title(f"{name}: {panel}", loc="left")
        ax.set_ylabel("ms")
        if s is None:
            ax.set_xlim(-0.5, 1.5)
            ax.set_xticks([0, 1], ["mean", "p99"])
            ax.tick_params(axis="x", length=0)
            ax.set_yticks([])
            ax.text(0.5, 0.5, "not yet measured", transform=ax.transAxes, ha="center", va="center",
                    color=MUTED)
            continue
        pick = lambda stat: {o: [s.loc[o, (stat, m)] for m in cols] for o in order}
        bars(ax, ["mean", "p99"], order, pick("median"), pick("min"), pick("max"), colors, labels)
        handles = handles or ax.get_legend_handles_labels()
    if BLOG:
        fig.tight_layout()
        fig.legend(*handles, loc="lower center", ncol=len(order), bbox_to_anchor=(0.5, 1.0))
        return fig
    # Legend and subtitle a fixed distance above the panels.
    fig.legend(*handles, loc="upper center", ncol=len(order), bbox_to_anchor=(0.5, 1 + 0.25 / size[1]))
    subtitle(fig, title, y=1 + 0.5 / size[1], fontsize=9)
    fig.tight_layout()
    return fig
