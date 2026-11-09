#!/usr/bin/env python3
"""Experiment 1: auth_wal across Zipf exponents.

zipf on a 1M-row firewood, bare firewood vs plain auth_wal vs auth_wal with
precomputation, one line per target over the exponent. Panels: propose
latency, mean and p99 (the bar figures' panel, over the exponent).
Median over seeds, band = min-max. The table also gives prefetch
latency, the set's hit rate, storage ops and evictions per block.
"""

import matplotlib.pyplot as plt

import common as c


def main():
    a = c.args(__doc__, "zipf_sweep")
    c.style()
    df = c.load(a.dir)
    df["target_key"] = df["config"].str.split("-", n=1).str[1]
    metrics = ["propose_mean_ms", "propose_p99_ms", "hint_mean_ms", "hint_p99_ms", "hint_per_block_ms",
               "in_place_update_ratio", "storage_ops_per_commit", "evictions_per_commit",
               "puts_per_commit", "commit_mean_ms"]
    s = c.summarize(df, ["target_key", "zipf_exponent"], metrics)

    # Propose latency, mean left and p99 right.
    name, cols = c.PROPOSE
    panels = [(metric, f"{name}, {stat}") for metric, stat in zip(cols, ["mean", "p99"])]
    size = (10, 4.4) if c.BLOG else (9, 4)
    fig, axes = plt.subplots(1, 2, figsize=size, squeeze=False)
    for ax, (metric, title) in zip(axes.flat, panels):
        for key, (label, color) in c.TARGETS.items():
            if key not in s.index.get_level_values(0):
                continue
            t = s.loc[key]
            x = t.index.to_numpy()
            ax.plot(x, t[("median", metric)], marker="o", markersize=None if c.BLOG else 5, color=color, label=label)
            ax.fill_between(x, t[("min", metric)], t[("max", metric)], color=color, alpha=0.25 if c.BLOG else 0.15,
                            linewidth=0)
        ax.set_title(title, loc="left")
        if not c.BLOG or ax in axes[-1]:
            ax.set_xlabel("Zipf exponent (0 = uniform)")
        ax.set_ylabel("ms")
        ax.set_ylim(bottom=0)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    if c.BLOG:
        fig.tight_layout()
        fig.legend(handles, labels, loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.0))
    else:
        # Legend and subtitle as far above the panels as in the bar figures.
        fig.legend(handles, labels, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1 + 0.25 / size[1]))
    wm = df[df["target_key"] != "firewood"][["capacity", "high_watermark", "low_watermark"]].drop_duplicates()
    wm = (f"; {c.NAME} capacity {wm.iloc[0]['capacity']}, trigger {wm.iloc[0]['high_watermark']}, "
          f"low watermark {wm.iloc[0]['low_watermark']}") if len(wm) == 1 else ""
    c.subtitle(fig, f"zipf over 1M rows, 2400-op hinted blocks{wm}: median of 3 seeds, band = min–max",
               y=1 + 0.5 / size[1], fontsize=9)
    if not c.BLOG:
        fig.tight_layout()
    c.save(fig, a.out, "zipf_sweep")
    c.table(s.xs("median", axis=1, level=0), a.out, "zipf_sweep_summary")


if __name__ == "__main__":
    main()
