#!/usr/bin/env python3
"""Experiment 3: commitment scheme, rehash vs merkle-blake3.

tenkrandom and zipf on a 1M-row firewood with precomputation on,
LRU-write. One panel per workload: propose latency, mean and p99. Median
over seeds, whiskers = min-max. On tenkrandom the experiment also runs
lthash and the sparse Merkle reference scheme; they are left out of the
figure but kept in the table.
"""

import common as c

from plot_policy import METRICS


def main():
    a = c.args(__doc__, "vc")
    c.style()
    df = c.load(a.dir)
    order = [o for o in ["rehash", "merkle-blake3"] if o in set(df["config"])]
    labels = ({"rehash": "flat rehash", "merkle-blake3": "Merkle tree (16-ary)"} if c.BLOG
              else {"rehash": "rehash", "merkle-blake3": "merkle-blake3 (b = 16)"})
    colors = {"rehash": c.SLOTS[1], "merkle-blake3": c.SLOTS[2]}
    s = c.summarize(df, ["workload", "config"], METRICS)
    shown = df[df["config"].isin(order)]
    setup = "; ".join(f"{rev} revisions, {blocks} blocks"
                      for (rev, blocks), _ in shown.groupby(["revisions", "commits"], sort=False))
    fig = c.latency_figure(c.workload_rows(df, "config", METRICS, order), order, labels, colors,
                           f"1M rows, precomputation on, LRU-write, {setup}: median of 3 seeds, whiskers = min–max")
    c.save(fig, a.out, "vc")
    c.table(s.xs("median", axis=1, level=0), a.out, "vc_summary")


if __name__ == "__main__":
    main()
