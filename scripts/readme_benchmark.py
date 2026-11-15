#!/usr/bin/env python3
"""Render the README's benchmark table from `run_experiments.py --only readme`.

Reads the `readme` experiment's results.csv: bare firewood and right_cash
(LRU-write policy, merkle-blake3 commitment, precomputation on) on `zipf`
and `tenkrandom`. Reports the propose, prefetch (one hint call) and commit
latencies, mean and p99, and the seed (the median if the file holds
several). Fails if a configuration lacks a seed the others ran or if the
trials disagree on a parameter, so a failed trial cannot pass as a complete
table.

The table goes to stdout and, optionally, replaces the block between the
markers in the README and is appended to a GitHub job summary.

Usage:
  scripts/readme_benchmark.py RESULTS_CSV [--readme README.md] [--summary FILE]
                              [--commit SHA] [--host TEXT] [--date YYYY-MM-DD]
"""

import argparse
import csv
import datetime
import re
import statistics
import sys
from pathlib import Path

START = "<!-- BENCHMARK-TABLE:START -->"
END = "<!-- BENCHMARK-TABLE:END -->"

WORKLOADS = ["zipf", "tenkrandom"]
# (row label, which trials) in row order within a workload. Results
# recorded before the 2026-11-09 rename say auth_wal.
TARGETS = [
    ("Firewood", lambda r: r["target"] == "firewood" and r["baseline"] == "true"),
    ("RightCash", lambda r: r["target"] in ("right_cash", "auth_wal")
     and r["policy"] == "lru-write" and r["vc"] == "merkle-blake3" and r["speculate"] == "true"),
]
# (column title, CSV column) in table order.
METRICS = [
    ("Propose mean", "propose_mean_us"),
    ("Propose p99", "propose_p99_us"),
    ("Prefetch mean", "hint_mean_us"),
    ("Prefetch p99", "hint_p99_us"),
    ("Commit mean", "commit_mean_us"),
    ("Commit p99", "commit_p99_us"),
]
# Columns the trials must share: the setup the table describes. Every trial
# shares the protocol; the right_cash trials also share the set's settings.
PROTOCOL = [
    "engine", "engine_id", "batch_size", "rows", "warmup_batches", "commits", "revisions", "cache_memory_limit",
    "hints", "hint_chunks", "hint_gap_min_us", "hint_gap_max_us", "hint_noise", "prefill",
]
RIGHT_CASH = [
    "merkle_branching", "capacity", "low_watermark", "high_watermark", "speculation_rounds",
    "speculation_min_step", "speculation_margin",
]


def latency(us):
    """A latency with a unit that keeps three significant digits or so. The
    unit is chosen after rounding, so 999.6 µs reads 1.00 ms, not 1000 µs."""
    if round(us) < 1_000:
        return f"{us:.0f} µs"
    ms = us / 1_000
    if round(ms, 2) < 10:
        return f"{ms:.2f} ms"
    if round(ms, 1) < 100:
        return f"{ms:.1f} ms"
    return f"{ms:.0f} ms"


def select(path):
    """The trials the table reports, by (workload, target label), and their
    seeds; exits on a gap or a disagreement."""
    if not path.is_file():
        sys.exit(f"{path}: no results (did every trial fail? see the trial logs)")
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        sys.exit(f"{path}: no trials recorded")
    missing = {"workload", "target", "baseline", "seed"} - set(rows[0])
    if missing:
        sys.exit(f"{path}: not a run_experiments.py results file (missing {sorted(missing)})")
    cells = {(w, label): [r for r in rows if r["workload"] == w and pick(r)]
             for w in WORKLOADS for label, pick in TARGETS}
    # Every configuration must have run each seed exactly once.
    seeds = sorted({int(r["seed"]) for trials in cells.values() for r in trials})
    problems = [] if seeds else ["no matching trials"]
    for (w, label), trials in cells.items():
        found = sorted(int(r["seed"]) for r in trials)
        if found != seeds:
            problems.append(f"{w} / {label}: expected seeds {seeds}, found {found}")
    chosen = [r for trials in cells.values() for r in trials]
    checks = [(c, chosen) for c in PROTOCOL]
    checks.append(("zipf_exponent", [r for r in chosen if r["workload"] == "zipf"]))
    right_cash = [r for (_, label), trials in cells.items() if label == "RightCash" for r in trials]
    checks += [(c, right_cash) for c in RIGHT_CASH]
    for col, trials in checks:
        values = {r[col] for r in trials}
        if len(values) > 1:
            problems.append(f"trials disagree on {col}: {sorted(values)}")
    if problems:
        sys.exit(f"{path}: incomplete or inconsistent results (see the trial logs):\n  " + "\n  ".join(problems))
    return cells, seeds


def render(cells, seeds, commit, host, date):
    lines = [
        "| Workload | Target | " + " | ".join(title for title, _ in METRICS) + " |",
        "|---|---|" + "--:|" * len(METRICS),
    ]
    for w in WORKLOADS:
        for label, _ in TARGETS:
            trials = cells[(w, label)]
            medians = [statistics.median(float(r[col]) for r in trials) for _, col in METRICS]
            lines.append(f"| `{w}` | {label} | " + " | ".join(latency(m) for m in medians) + " |")
    one = cells[("zipf", "Firewood")][0]
    rc = cells[("zipf", "RightCash")][0]
    gap_min, gap_max = int(one["hint_gap_min_us"]) / 1000, int(one["hint_gap_max_us"]) / 1000
    rerun = f"`scripts/run_experiments.py --only readme --seed {seeds[0]}`"
    if len(seeds) == 1:
        over = f"Seed {seeds[0]}, the same for every row (rerun with {rerun})."
    else:
        over = f"Median over seeds {', '.join(map(str, seeds))}."
    setup = (
        f"{over} {int(one['rows']):,} prefilled rows in {one['engine']} (`{one['engine_id']}`); "
        f"{int(one['batch_size']):,}-op blocks, each hinted as {one['hint_chunks']} prefetch calls "
        f"{gap_min:g}–{gap_max:g} ms apart; {one['warmup_batches']} warm-up and {one['commits']} measured blocks; "
        f"{one['revisions']} committed revisions kept; Zipf exponent {one['zipf_exponent']}. "
        f"RightCash: capacity {int(rc['capacity']):,}, high watermark {int(rc['high_watermark']):,}, "
        f"low watermark {int(rc['low_watermark']):,}; merkle branching {rc['merkle_branching']}; "
        f"{rc['speculation_rounds']} speculation rounds, so its Firewood keeps "
        f"{int(rc['revisions']) * int(rc['speculation_rounds']):,} revisions for the speculative steps."
    )
    where = f"Measured on {date}"
    if commit:
        where += f" at commit `{commit[:7]}`"
    if host:
        where += f" on {host}"
    return "\n".join(lines) + "\n\n" + setup + "\n" + where + "."


def replace_block(readme, table):
    text = readme.read_text()
    if text.count(START) != 1 or text.count(END) != 1 or text.index(START) > text.index(END):
        sys.exit(f"{readme}: expected one {START} ... {END} block")
    block = f"{START}\n{table}\n{END}"
    readme.write_text(re.sub(re.escape(START) + ".*?" + re.escape(END), lambda _: block, text, flags=re.DOTALL))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path, help="results.csv of the readme experiment")
    ap.add_argument("--readme", type=Path, help="replace the table block in this file")
    ap.add_argument("--summary", type=Path, help="append the table to this file (GITHUB_STEP_SUMMARY)")
    ap.add_argument("--commit", default="", help="commit the binary was built from")
    ap.add_argument("--host", default="", help="the machine, as table text")
    ap.add_argument("--date", default=datetime.datetime.now(datetime.timezone.utc).date().isoformat())
    args = ap.parse_args()

    table = render(*select(args.results), args.commit, args.host, args.date)
    print(table)
    if args.readme:
        replace_block(args.readme, table)
    if args.summary:
        with args.summary.open("a") as f:
            f.write(f"## Firewood vs RightCash (LRU-write, merkle-blake3)\n\n{table}\n")


if __name__ == "__main__":
    main()
