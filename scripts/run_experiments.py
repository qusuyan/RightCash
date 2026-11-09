#!/usr/bin/env python3
"""Run the benchmark experiments and collect their CSV records.

Experiments (all on firewood, prefilled, hinted blocks; see PROTOCOL):

  zipf_sweep   zipf with several exponents: bare firewood, plain auth_wal,
               speculative auth_wal (precomputation).
  policy       tenkrandom and zipf, precomputation on: LRU-write vs FIFO,
               with bare firewood as the baseline.
  vc           tenkrandom and zipf, precomputation on: rehash vs
               merkle-blake3 (b = 16); tenkrandom also runs lthash and the
               sparse merkle.
  baseline     zipf and tenkrandom: bare firewood vs plain auth_wal vs
               speculative auth_wal, propose and prefetch latency.
  watermarks   tenkrandom, precomputation on: trigger (high watermark) and
               low watermark varied one at a time around a baseline;
               per-block rows in <experiment>/blocks/.
  watermarks_capacity
               tenkrandom, precomputation on: capacity varied with trigger
               and low watermark shifted along, so both gaps stay fixed.

Each trial runs alone (latencies are the measurement), writes its full
stdout/stderr to `<out>/<experiment>/logs/<trial>.log`, and appends its CSV
record to `<out>/<experiment>/results.csv` plus its per-phase breakdown to
`<out>/<experiment>/breakdown.csv`. Trials already present in results.csv
are skipped, so an interrupted run resumes where it stopped, and a
configuration added later (a new workload) runs into the same directory
beside the existing trials.

Usage:
  scripts/run_experiments.py [--out DIR] [--only EXP ...] [--match REGEX] [--dry-run]
"""

import argparse
import csv
import io
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target" / "release" / "auth_wal-benchmark"

SEEDS = [101, 202, 303]

# The protocol shared by every trial (the 2026-11-03 setup, design §14):
# 1M prefilled rows, 2400-op blocks hinted as 30 chunks of 80 keys with
# 1-5 ms emulated execution gaps, 50 warm-up and 500 measured blocks. A
# configuration may override a value (clap rejects repeated flags).
PROTOCOL = {
    "--engine": "firewood",
    "-b": "2400", "-n": "417",
    "--prefill": None,
    "--warmup-batches": "50",
    "--iterations": "550",
    "--hints": None, "--hint-chunks": "30",
    "--hint-gap-min-us": "1000", "--hint-gap-max-us": "5000",
    "--speculation-rounds": "30",
    "--progress-every": "0",
}


def protocol(overrides):
    merged = {**PROTOCOL, **overrides}
    return [x for k, v in merged.items() for x in ([k] if v is None else [k, v])]


FIREWOOD = ["--baseline"]
PLAIN = ["--policy", "lru-write", "--vc", "rehash", "--breakdown"]
SPEC = PLAIN + ["--speculate"]
# The watermark experiment's baseline (capacity - trigger = W, trigger - low
# = 2W - 1 for tenkrandom's W = 2100 new entries per block), used by every
# auth_wal configuration of experiments 1-4. Experiments 5 and 6 set their
# own.
WATERMARKS = ["--capacity", "10000", "--high-watermark", "7900", "--low-watermark", "3701"]


def configs():
    """Every configuration: (experiment, config id, flags, protocol
    overrides, workload)."""
    out = []

    # 1. Zipf exponent sweep; exponent 0 is uniform over the rows.
    for s in ["0.0", "0.3", "0.6", "0.8", "1.0", "1.2", "1.4", "1.6", "2.0"]:
        for name, flags in [("firewood", FIREWOOD), ("auth_wal", PLAIN + WATERMARKS),
                            ("auth_wal_spec", SPEC + WATERMARKS)]:
            out.append(("zipf_sweep", f"s{s}-{name}", flags + ["--zipf-exponent", s], {}, "zipf"))

    # Experiments 2-4 run on several workloads into one directory: a config
    # id starts with its workload, so a workload's trials sit beside the
    # others' and one figure shows them all (zipf at the default exponent,
    # 1.2).

    # 2. Eviction policy with precomputation.
    for workload in ["tenkrandom", "zipf"]:
        for name, flags in [
            ("firewood", FIREWOOD),
            ("lru-write", ["--policy", "lru-write", "--vc", "rehash", "--speculate", "--breakdown"] + WATERMARKS),
            ("fifo", ["--policy", "fifo", "--vc", "rehash", "--speculate", "--breakdown"] + WATERMARKS),
        ]:
            out.append(("policy", f"{workload}-{name}", flags, {}, workload))

    # 3. Commitment scheme with precomputation. The sparse Merkle reference
    # scheme rewrites ~1.1M tree nodes per block and needs far more than
    # 15 GB with 128 retained revisions, so every scheme retains 8; merkle
    # proposes in 1.5-2.5 s, so it measures 100 blocks. zipf runs only the
    # two schemes the figure compares.
    for workload, schemes in [("tenkrandom", ["rehash", "lthash", "merkle", "merkle-blake3"]),
                              ("zipf", ["rehash", "merkle-blake3"])]:
        for vc in schemes:
            overrides = {"--revisions": "8"}
            if vc == "merkle":
                overrides["--iterations"] = "150"
            out.append(("vc", f"{workload}-{vc}",
                        ["--policy", "lru-write", "--vc", vc, "--speculate", "--breakdown"] + WATERMARKS,
                        overrides, workload))

    # 4. The original comparison, propose and prefetch.
    for workload in ["zipf", "tenkrandom"]:
        for name, flags in [("firewood", FIREWOOD), ("auth_wal", PLAIN + WATERMARKS),
                            ("auth_wal_spec", SPEC + WATERMARKS)]:
            out.append(("baseline", f"{workload}-{name}", flags, {}, workload))

    # 5. Trigger (high watermark) and low watermark on tenkrandom with
    # precomputation, one knob at a time around the baseline. A block
    # adds W = 2100 new entries (2400 ops, 300 rewrites of the previous
    # block's keys). Baseline: capacity - trigger = W, trigger - low =
    # 2W - 1, capacity - low = 3W - 1 (eviction is strictly above capacity,
    # so 3W - 1 makes every third block evict 3W with all but one victim
    # predicted). Per-block rows go to <experiment>/blocks/<trial>.csv.
    base = {"C": 10000, "T": 7900, "L": 3701}
    sweeps = {
        "T": [3701, 4751, 5801, 6851, 8500, 9200, 10000],
        "L": [1000, 1601, 2500, 4000, 5000, 5801, 7000, 7900],
    }
    points = [("base", base)] + [(f"{k}-{v}", {**base, k: v}) for k, vals in sweeps.items() for v in vals]
    blocks = ["--blocks-csv", "{blocks}"]
    out.append(("watermarks", "firewood", FIREWOOD + blocks, {}, "tenkrandom"))
    for name, p in points:
        out.append(("watermarks", name,
                    SPEC + ["--capacity", str(p["C"]), "--high-watermark", str(p["T"]),
                            "--low-watermark", str(p["L"])] + blocks, {}, "tenkrandom"))

    # 6. Capacity with the trigger and low watermark shifted along (capacity
    # - trigger = W, trigger - low = 2W - 1), so every point evicts every
    # third block with one unpredicted victim and only the set size changes.
    # Below capacity 8700 the low watermark (capacity - 6299) is under one
    # block's 2400 keys, so eviction takes keys the block just wrote; 6300
    # (low watermark 1) is the smallest capacity these gaps allow. Its own experiment so that its baseline and firewood reference
    # run in the same stretch as its points.
    out.append(("watermarks_capacity", "firewood", FIREWOOD + blocks, {}, "tenkrandom"))
    for C in [6300, 6700, 7100, 7500, 8000, 8400, 8700, 9300, 10000, 12000, 14000, 17000, 20000]:
        T, L = C - (base["C"] - base["T"]), C - (base["C"] - base["L"])
        out.append(("watermarks_capacity", "base" if C == base["C"] else f"C-{C}",
                    SPEC + ["--capacity", str(C), "--high-watermark", str(T),
                            "--low-watermark", str(L)] + blocks, {}, "tenkrandom"))
    return out


def trials(experiments=None):
    """Every trial: (experiment, trial id, command line tail, workload).
    Configurations are interleaved by seed within an experiment (seed 1 of
    every configuration, then seed 2, ...), so a slow drift of the machine
    spreads over all configurations instead of landing on one."""
    order = experiments or ["policy", "baseline", "zipf_sweep", "vc", "watermarks", "watermarks_capacity"]
    out = []
    for exp in order:
        for seed in SEEDS:
            for e, cfg, flags, overrides, workload in configs():
                if e == exp:
                    out.append((exp, f"{cfg}-{seed}", protocol(overrides) + flags + ["--seed", str(seed)], workload))
    return out


PHASE_HEADER = re.compile(r"^== (propose|commit) breakdown")
PHASE_ROW = re.compile(r"^(\S+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)%$")


def parse(stdout):
    """The CSV record (last two lines) and the breakdown rows, if printed."""
    lines = stdout.strip().splitlines()
    record = next(csv.DictReader(io.StringIO("\n".join(lines[-2:]))))
    phases, call = [], None
    for line in lines[:-2]:
        m = PHASE_HEADER.match(line)
        if m:
            call = m.group(1)
            continue
        m = PHASE_ROW.match(line.strip())
        if call and m:
            phases.append({"call": call, "phase": m.group(1),
                           "mean_us": m.group(2), "p99_us": m.group(3)})
    return record, phases


def append(path, rows, fields):
    """Append rows; if the file's header lacks some of `fields` (the
    benchmark added a column since it was written), rewrite the file with
    the union of the columns first, so no row shifts under the header."""
    if path.exists():
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            old_fields, old_rows = reader.fieldnames or [], list(reader)
        if set(fields) - set(old_fields):
            fields = old_fields + [c for c in fields if c not in old_fields]
            # Write beside and rename over, so an interruption never leaves
            # the finished trials' rows half written.
            tmp = path.with_name(path.name + ".tmp")
            with tmp.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields, restval="")
                w.writeheader()
                w.writerows(old_rows)
            os.replace(tmp, path)
        else:
            fields = old_fields
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, restval="")
        if new:
            w.writeheader()
        w.writerows(rows)


def done(path):
    if not path.exists():
        return set()
    with path.open() as f:
        return {row["trial"] for row in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=ROOT / "benchmark" / "results" / "2026-11-07-interleaved")
    ap.add_argument("--db", type=Path, default=None, help="firewood database directory (default: <out>/db)")
    ap.add_argument("--only", nargs="*", help="experiments to run")
    ap.add_argument("--match", default=None, help="only trials whose id matches this regex (e.g. '^(firewood|base)-')")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    db = args.db or args.out / "db"

    todo = trials(args.only)
    if args.match:
        todo = [t for t in todo if re.search(args.match, t[1])]
    for i, (exp, trial, flags, workload) in enumerate(todo, 1):
        exp_dir = args.out / exp
        results = exp_dir / "results.csv"
        if trial in done(results):
            continue
        blocks = exp_dir / "blocks" / f"{trial}.csv"
        cmd = [str(BINARY), *[str(blocks) if f == "{blocks}" else f for f in flags], "--dbname", str(db), workload]
        print(f"[{i}/{len(todo)}] {exp}/{trial}: {' '.join(cmd)}", flush=True)
        if args.dry_run:
            continue
        (exp_dir / "logs").mkdir(parents=True, exist_ok=True)
        blocks.parent.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(db, ignore_errors=True)
        started = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        wall = time.time() - started
        (exp_dir / "logs" / f"{trial}.log").write_text(
            f"$ {' '.join(cmd)}\n== stdout ==\n{proc.stdout}\n== stderr ==\n{proc.stderr}")
        shutil.rmtree(db, ignore_errors=True)
        if proc.returncode != 0:
            print(f"  FAILED ({proc.returncode}); see logs/{trial}.log\n{proc.stderr[-2000:]}", flush=True)
            continue
        record, phases = parse(proc.stdout)
        record = {"experiment": exp, "trial": trial, "wall_s": f"{wall:.1f}", **record}
        append(results, [record], list(record))
        if phases:
            append(exp_dir / "breakdown.csv", [{"trial": trial, **p} for p in phases],
                   ["trial", "call", "phase", "mean_us", "p99_us"])
        print(f"  ok in {wall:.0f}s: propose {record['propose_mean_us']} us, "
              f"hint {record['hint_mean_us']} us", flush=True)


if __name__ == "__main__":
    sys.exit(main())
