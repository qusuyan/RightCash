#!/usr/bin/env python3
"""Experiment 5: capacity, trigger (high watermark) and low watermark.

tenkrandom on a 1M-row firewood with precomputation on (LRU-write,
rehash) around the baseline (capacity 10000, trigger 7900, low 3701).
The trigger and the low watermark are varied one at a time with the other
two fixed (`watermarks/`); capacity is varied with the trigger and the low
watermark shifted along, so capacity - trigger and trigger - low stay
fixed and only the set size changes (`watermarks_capacity/`, with its own
baseline and firewood runs). One figure per knob, propose
mean and p99 over all measured blocks side by side, bare firewood as a
reference line. Whiskers = min-max over the three seeds.

The mean bar splits propose into its measured parts, averaged over all
blocks (`--blocks-csv`): waiting for the precomputation worker to finish
(`finish`), storage reads and writes applied inside propose (`reads`,
`storage`: victims the precomputation did not cover plus predictions the
evicting block withdrew), and everything else (set work, commitment hash). The stack shown is the seed with the median mean.

The summary table also carries the eviction model for comparison: a
tenkrandom block adds W = 2100 new entries, eviction runs when occupancy
exceeds capacity and drains to the low watermark, and the precomputation
predicts exactly capacity - low victims, so a cycle is k = (C - L) // W + 1
blocks and k*W - (C - L) victims are left over.
"""

import matplotlib.pyplot as plt
import pandas as pd

import common as c

W = 2100
BASE = {"C": 10000, "T": 7900, "L": 3701}
COLUMNS = {"C": "capacity", "T": "high_watermark", "L": "low_watermark"}
KNOBS = [("capacity", "C", "Capacity"),
         ("low_watermark", "L", "Low watermark"),
         ("trigger", "T", "Trigger (high watermark)")]
PARTS = [("other_ms", "set work, commitment hash, rest", c.SLOTS[0]),
         ("storage_ms", "storage reads + writes inside propose", c.SLOTS[1]),
         ("wait_ms", "waiting for the precomputation", c.SLOTS[3])]
# Shorter legend and axis text for the blog.
BLOG_PARTS = {"other_ms": "set work, commitment, rest", "storage_ms": "storage reads + writes",
              "wait_ms": "waiting for precomputation"}
BLOG_KNOBS = {"T": "High watermark (trigger)"}
METRICS = ["propose_mean_ms", "propose_p99_ms", "other_ms", "storage_ms", "wait_ms", "storage_ops",
           "commit_mean_ms", "steps_per_commit", "in_place_update_ratio"]


def model(C, T, L):
    k = (C - L) // W + 1
    return k, k * W - (C - L)


def decompose(exp_dir, trial):
    """Mean per block of the propose parts, ms, from the per-block rows."""
    b = pd.read_csv(exp_dir / "blocks" / f"{trial}.csv")
    wait = b["finish_us"].mean() / 1000
    storage = (b["reads_us"] + b["storage_us"]).mean() / 1000
    total = b["propose_us"].mean() / 1000
    return {"wait_ms": wait, "storage_ms": storage, "other_ms": total - wait - storage,
            "storage_ops": b["storage_ops"].mean()}


def median_seed(g):
    """The trial whose propose mean is the median of its configuration."""
    return g.loc[(g["propose_mean_ms"] - g["propose_mean_ms"].median()).abs().idxmin()]


def load(a, exp):
    """Firewood reference and right_cash trials (with propose parts) of one
    experiment directory."""
    d = a.results / exp
    df = c.load(d)
    fw = df[df["config"] == "firewood"]
    ref = {"propose_mean_ms": fw["propose_mean_ms"].median(), "propose_p99_ms": fw["propose_p99_ms"].median()}
    cache = df[df["config"] != "firewood"].copy()
    cache = cache.join(pd.DataFrame([decompose(d, t) for t in cache["trial"]], index=cache.index))
    return ref, cache


def main():
    a = c.args(__doc__, "watermarks")
    c.style()
    tables = []
    for name, knob, label in KNOBS:
        column = COLUMNS[knob]
        if knob == "C":
            # Trigger and low watermark shift with capacity (fixed gaps).
            ref, sel = load(a, "watermarks_capacity")
            gaps = {k: BASE["C"] - BASE[k] for k in ("T", "L")}
            point = lambda v: {"C": v, **{k: v - g for k, g in gaps.items()}}
            fixed = (f"trigger = capacity − {gaps['T']}, low watermark = capacity − {gaps['L']}")
        else:
            ref, cache = load(a, "watermarks")
            others = {k: v for k, v in BASE.items() if k != knob}
            sel = cache
            for k, v in others.items():
                sel = sel[sel[COLUMNS[k]] == v]
            point = lambda v, knob=knob: {**BASE, knob: v}
            fixed = ", ".join(f"{COLUMNS[k].replace('_', ' ')} {v}" for k, v in others.items())
        s = sel.groupby(column)[METRICS].agg(["median", "min", "max"])
        stack = pd.DataFrame({v: median_seed(g) for v, g in sel.groupby(column)}).T[[p for p, _, _ in PARTS]].astype(float)
        values = s.index.to_numpy()
        models = [model(**point(int(v))) for v in values]
        tables.append(s.xs("median", axis=1, level=1).assign(
            k_model=[m[0] for m in models], leftover_model=[m[1] for m in models], sweep=knob))

        x = range(len(values))
        ticks = [f"{v}\n(baseline)" if v == BASE[knob] else f"{v}" for v in values]
        if c.BLOG:
            # Stacked, sharing the x axis: every bar gets the column's width.
            fig, axes = plt.subplots(2, 1, figsize=(10, 7.5), sharex=True)
            label = BLOG_KNOBS.get(knob, label)
        else:
            fig, axes = plt.subplots(1, 2, figsize=(max(12, 1.15 * len(values) + 1), 4.2))

        ax = axes[0]
        bottom = [0.0] * len(values)
        # The parts in the theme's palette (the module-level PARTS took the default one).
        parts = [(p, l, col) for (p, l, _), col in zip(PARTS, (c.SLOTS[0], c.SLOTS[1], c.SLOTS[3]))]
        for part, plabel, color in parts:
            ax.bar(x, stack[part], 0.6, bottom=bottom, color=color, label=BLOG_PARTS[part] if c.BLOG else plabel,
                   edgecolor=c.SURFACE, linewidth=0.6)
            bottom = [b + v for b, v in zip(bottom, stack[part])]
        med = s[("propose_mean_ms", "median")]
        ax.errorbar(x, med, yerr=[med - s[("propose_mean_ms", "min")], s[("propose_mean_ms", "max")] - med],
                    fmt="none", ecolor=c.INK_2, elinewidth=1, capsize=2)
        ax.set_title("Propose, mean per block", loc="left")

        ax = axes[1]
        med = s[("propose_p99_ms", "median")]
        ax.bar(x, med, 0.6, color=c.TARGETS["right_cash_spec"][1], label=f"{c.TARGETS['right_cash_spec'][0]} (p99)",
               edgecolor=c.SURFACE, linewidth=0.6)
        ax.errorbar(x, med, yerr=[med - s[("propose_p99_ms", "min")], s[("propose_p99_ms", "max")] - med],
                    fmt="none", ecolor=c.INK_2, elinewidth=1, capsize=2)
        ax.set_title("Propose, p99", loc="left")

        for ax, m in zip(axes, ["propose_mean_ms", "propose_p99_ms"]):
            ref_label = (c.TARGETS["firewood"][0] if c.BLOG
                         else f"bare firewood ({ref[m]:.1f} ms mean, {ref['propose_p99_ms']:.1f} ms p99)")
            ax.axhline(ref[m], color=c.MUTED, linewidth=1.2, linestyle="--",
                       label=ref_label if m == "propose_mean_ms" else None)
            ax.set_ylim(0, max(ax.get_ylim()[1], ref[m] * 1.15))
            ax.set_xticks(list(x), ticks)
            ax.tick_params(axis="x", length=0)
            if not c.BLOG or ax is axes[-1]:
                ax.set_xlabel(label)
            ax.set_ylabel("ms")

        handles, lbls = axes[0].get_legend_handles_labels()
        fig.tight_layout()
        if c.BLOG:
            fig.legend(handles, lbls, loc="lower center", ncol=2, bbox_to_anchor=(0.5, 1.0))
        else:
            fig.legend(handles, lbls, loc="upper center", ncol=4, bbox_to_anchor=(0.5, 1.03), fontsize=8)
        c.subtitle(fig, f"tenkrandom, 1M rows, precompute on (LRU-write, rehash); {fixed}; all 500 measured blocks; "
                     "whiskers = min–max over 3 seeds; mean stack = median seed",
                   y=1.09, fontsize=8.5)
        if not c.BLOG:
            fig.tight_layout()
        c.save(fig, a.out, f"watermarks_{name}")
    c.table(pd.concat(tables), a.out, "watermarks_summary")


if __name__ == "__main__":
    main()
