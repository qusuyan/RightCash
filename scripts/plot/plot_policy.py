#!/usr/bin/env python3
"""Experiment 2: eviction policy, LRU-write vs FIFO.

tenkrandom and zipf on a 1M-row firewood with precomputation
(speculation) on, rehash commitment, with bare firewood as the baseline.
One panel per workload: propose latency, mean and p99. Median over seeds,
whiskers = min-max. The table also gives prefetch latency and the
precomputation's outcome per block.
"""

import common as c

METRICS = ["propose_mean_ms", "propose_p99_ms", "hint_mean_ms", "hint_p99_ms", "hint_per_block_ms",
           "commit_mean_ms", "evictions_per_commit", "reused_ops_per_commit", "reverted_ops_per_commit",
           "storage_ops_per_commit", "steps_per_commit", "worker_time_s", "peak_rss_bytes", "step_failures",
           "fallbacks"]


def main():
    a = c.args(__doc__, "policy")
    c.style()
    df = c.load(a.dir)
    order = [o for o in ["firewood", "lru-write", "fifo"] if o in set(df["config"])]
    labels = {"firewood": c.TARGETS["firewood"][0], "lru-write": f"{c.NAME} LRU-write", "fifo": f"{c.NAME} FIFO"}
    colors = {"firewood": c.TARGETS["firewood"][1], "lru-write": c.SLOTS[1], "fifo": c.SLOTS[2]}
    s = c.summarize(df, ["workload", "config"], METRICS)
    fig = c.latency_figure(c.workload_rows(df, "config", METRICS, order), order, labels, colors,
                           "1M rows, precomputation on, rehash: median of 3 seeds, whiskers = min–max")
    c.save(fig, a.out, "policy")
    c.table(s.xs("median", axis=1, level=0), a.out, "policy_summary")


if __name__ == "__main__":
    main()
