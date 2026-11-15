#!/usr/bin/env python3
"""Render the README's benchmark table from `run_experiments.py --only vc`.

Reads the `vc` experiment's results.csv, keeps the right_cash trials with
the LRU-write policy and the merkle-blake3 commitment on `zipf` and
`tenkrandom`, and reports the median over seeds of the propose, prefetch
(one hint call) and commit latencies, mean and p99. Fails if a workload is
missing a seed or if the trials disagree on a parameter, so a failed trial
cannot pass as a complete table.

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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_experiments import SEEDS  # noqa: E402

START = "<!-- BENCHMARK-TABLE:START -->"
END = "<!-- BENCHMARK-TABLE:END -->"

POLICY, VC = "lru-write", "merkle-blake3"
WORKLOADS = ["zipf", "tenkrandom"]
# (column title, CSV column) in table order.
METRICS = [
    ("Propose mean", "propose_mean_us"),
    ("Propose p99", "propose_p99_us"),
    ("Prefetch mean", "hint_mean_us"),
    ("Prefetch p99", "hint_p99_us"),
    ("Commit mean", "commit_mean_us"),
    ("Commit p99", "commit_p99_us"),
]
# Columns every selected trial must share: the setup the table describes.
SETUP = [
    "engine", "engine_id", "merkle_branching", "capacity", "low_watermark", "high_watermark",
    "batch_size", "rows", "zipf_exponent", "warmup_batches", "commits", "revisions", "speculate",
    "speculation_rounds", "hints", "hint_chunks", "hint_gap_min_us", "hint_gap_max_us", "prefill",
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
    """The trials the table reports, by workload; exits on a gap."""
    with open(path, newline="") as f:
        # Results recorded before the 2026-11-09 rename say auth_wal.
        rows = [r for r in csv.DictReader(f)
                if r["target"] in ("right_cash", "auth_wal") and r["policy"] == POLICY and r["vc"] == VC
                and r["workload"] in WORKLOADS]
    by_workload = {w: [r for r in rows if r["workload"] == w] for w in WORKLOADS}
    problems = []
    for w, trials in by_workload.items():
        seeds = sorted(int(r["seed"]) for r in trials)
        if seeds != sorted(SEEDS):
            problems.append(f"{w}: expected seeds {sorted(SEEDS)}, found {seeds}")
    for col in SETUP:
        if col == "zipf_exponent":
            values = {r[col] for r in by_workload["zipf"]}
        else:
            values = {r[col] for r in rows}
        if len(values) > 1:
            problems.append(f"trials disagree on {col}: {sorted(values)}")
    if problems:
        sys.exit(f"{path}: incomplete or inconsistent results (see the trial logs):\n  " + "\n  ".join(problems))
    return by_workload


def render(by_workload, commit, host, date):
    one = by_workload["zipf"][0]
    lines = [
        "| Workload | " + " | ".join(title for title, _ in METRICS) + " |",
        "|---|" + "--:|" * len(METRICS),
    ]
    for w in WORKLOADS:
        medians = [statistics.median(float(r[col]) for r in by_workload[w]) for _, col in METRICS]
        lines.append(f"| `{w}` | " + " | ".join(latency(m) for m in medians) + " |")
    gap_min, gap_max = int(one["hint_gap_min_us"]) / 1000, int(one["hint_gap_max_us"]) / 1000
    setup = (
        f"Median over seeds {', '.join(map(str, SEEDS))}. "
        f"{int(one['rows']):,} prefilled rows in {one['engine']} (`{one['engine_id']}`); "
        f"{int(one['batch_size']):,}-op blocks, each hinted as {one['hint_chunks']} prefetch calls "
        f"{gap_min:g}–{gap_max:g} ms apart; {one['warmup_batches']} warm-up and {one['commits']} measured blocks; "
        f"capacity {int(one['capacity']):,}, high watermark {int(one['high_watermark']):,}, "
        f"low watermark {int(one['low_watermark']):,}; merkle branching {one['merkle_branching']}; "
        f"{one['speculation_rounds']} speculation rounds; {one['revisions']} revisions; "
        f"Zipf exponent {one['zipf_exponent']}."
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
    ap.add_argument("results", type=Path, help="results.csv of the vc experiment")
    ap.add_argument("--readme", type=Path, help="replace the table block in this file")
    ap.add_argument("--summary", type=Path, help="append the table to this file (GITHUB_STEP_SUMMARY)")
    ap.add_argument("--commit", default="", help="commit the binary was built from")
    ap.add_argument("--host", default="", help="the machine, as table text")
    ap.add_argument("--date", default=datetime.datetime.now(datetime.timezone.utc).date().isoformat())
    args = ap.parse_args()

    table = render(select(args.results), args.commit, args.host, args.date)
    print(table)
    if args.readme:
        replace_block(args.readme, table)
    if args.summary:
        with args.summary.open("a") as f:
            f.write(f"## RightCash (LRU-write, merkle-blake3)\n\n{table}\n")


if __name__ == "__main__":
    main()
