#!/usr/bin/env python3
"""Experiment 4: bare firewood vs right_cash vs right_cash with precomputation.

zipf (exponent 1.2) and tenkrandom on a 1M-row firewood, LRU-write and
rehash for right_cash. One panel per workload: propose latency, mean and
p99. Median over seeds, whiskers = min-max. The table also gives
prefetch latency and the time per block in each API call (prefetch,
propose, commit and their sum, excluding the emulated execution gaps).
"""

import common as c


def main():
    a = c.args(__doc__, "baseline")
    c.style()
    df = c.load(a.dir)
    df["target_key"] = df["config"]
    df["api_per_block_ms"] = df["hint_per_block_ms"] + df["propose_mean_ms"] + df["commit_mean_ms"]
    metrics = ["propose_mean_ms", "propose_p99_ms", "hint_mean_ms", "hint_p99_ms", "hint_per_block_ms",
               "commit_mean_ms", "api_per_block_ms", "storage_ops_per_commit", "evictions_per_commit",
               "in_place_update_ratio", "peak_rss_bytes"]
    s = c.summarize(df, ["workload", "target_key"], metrics)

    order = [k for k in c.TARGETS if k in set(df["target_key"])]
    labels = {k: v[0] for k, v in c.TARGETS.items()}
    colors = {k: v[1] for k, v in c.TARGETS.items()}
    rows = c.workload_rows(df, "target_key", metrics, order)
    fig = c.latency_figure(rows, order, labels, colors,
                           "1M rows, 2400-op blocks hinted as 30 × 80 keys: median of 3 seeds, whiskers = min–max")
    c.save(fig, a.out, "baseline")
    c.table(s.xs("median", axis=1, level=0), a.out, "baseline_summary")


if __name__ == "__main__":
    main()
