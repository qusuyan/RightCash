//! Shared pieces of the four workloads: firewood's key derivation and batch
//! generators, the stop rule, hint emulation, the latency recorder and the
//! update workloads' loop.

use std::collections::BTreeMap;
use std::error::Error;
use std::time::{Duration, Instant};

use auth_wal::Stats;
use auth_wal::timings::{CommitTimings, ProposeTimings};
use auth_wal::types::BatchOp;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Geometric};
use sha2::{Digest, Sha256};

use crate::report::{Breakdown, Measurements, mean, p99};
use crate::target::{BatchTiming, Target};
use crate::{Args, BenchKey, BenchValue};

/// `sha256(row_id)` in native endian, as firewood does.
pub fn row_key(row: u64) -> BenchKey {
    Sha256::digest(row.to_ne_bytes()).into()
}

/// Inserts of rows `start..start+count` with value = key (firewood `create`).
pub fn inserts(start: u64, count: u64) -> impl Iterator<Item = BatchOp<BenchKey, BenchValue>> {
    (start..start + count).map(|row| {
        let key = row_key(row);
        BatchOp::Put {
            key,
            value: key.to_vec(),
        }
    })
}

pub fn deletes(start: u64, count: u64) -> impl Iterator<Item = BatchOp<BenchKey, BenchValue>> {
    (start..start + count).map(|row| BatchOp::Delete { key: row_key(row) })
}

pub fn updates(start: u64, count: u64, value: BenchValue) -> impl Iterator<Item = BatchOp<BenchKey, BenchValue>> {
    (start..start + count).map(move |row| BatchOp::Put {
        key: row_key(row),
        value: value.clone(),
    })
}

/// The update workloads' loop (`tenkrandom`, `zipf`, `single`): the prefill,
/// then one batch from `next` (given the batch number) per turn until the
/// stop rule (firewood's: `--iterations` batches when set, else the
/// duration), the recorder reset after `--warmup-batches`, and the
/// measurements. Auth_wal must hold at least the low watermark before the
/// first measured block; insufficient prefill/warm-up is an error.
pub fn run_loop(
    target: &mut dyn Target,
    args: &Args,
    mut next: impl FnMut(u64) -> Vec<BatchOp<BenchKey, BenchValue>>,
) -> Result<Measurements, Box<dyn Error>> {
    let g = &args.global_opts;
    let mut rec = Recorder::new(args);
    prefill(target, args, &mut rec)?;
    let start = Instant::now();
    let deadline = Duration::from_secs(g.duration_minutes * 60);
    let mut batch = 0u64;
    let mut measuring = false;
    loop {
        if batch == g.warmup_batches {
            let stats = target.stats();
            if !g.baseline && stats.occupancy < g.watermark() {
                return Err(format!(
                    "measurement requires occupancy >= low watermark: {} < {}; use --prefill with enough rows or increase --warmup-batches",
                    stats.occupancy, g.watermark()
                ).into());
            }
            eprintln!(
                "[measurement] start_occupancy={} low_watermark={} warmup_batches={}",
                stats.occupancy,
                g.watermark(),
                batch
            );
            rec.reset(stats);
            measuring = true;
        }
        apply(target, args, &mut rec, next(batch))?;
        batch += 1;
        if (g.iterations > 0 && batch >= g.iterations) || start.elapsed() >= deadline {
            break;
        }
    }
    if !measuring {
        return Err("run ended before measurement could start; increase --iterations or --duration-minutes, or reduce --warmup-batches".into());
    }
    Ok(rec.finish(target.stats()))
}

/// The gap between two hints (design §11): `min` plus a geometric number of
/// 100 µs steps with mean `(max - min) / 2`, truncated to `max`; a constant
/// `min` when `max <= min`; nothing when both are zero.
struct Gaps {
    rng: StdRng,
    step: Option<Geometric>,
    min: Duration,
    max: Duration,
}

impl Gaps {
    const STEP_US: f64 = 100.0;

    fn new(min_us: u64, max_us: u64, seed: u64) -> Self {
        let step = (max_us > min_us).then(|| {
            let mean_steps = (max_us - min_us) as f64 / 2.0 / Self::STEP_US;
            Geometric::new(1.0 / (1.0 + mean_steps)).expect("probability in (0, 1]")
        });
        Self {
            rng: StdRng::seed_from_u64(seed ^ 0x5DEE_CE66_D1CE_4A1D),
            step,
            min: Duration::from_micros(min_us),
            max: Duration::from_micros(max_us.max(min_us)),
        }
    }

    fn sample(&mut self) -> Duration {
        match &self.step {
            Some(step) => {
                let steps = step.sample(&mut self.rng);
                (self.min + Duration::from_micros((steps as f64 * Self::STEP_US) as u64)).min(self.max)
            }
            None => self.min,
        }
    }
}

/// Collects per-batch timings and turns them into [`Measurements`].
pub struct Recorder {
    proposes: Vec<Duration>,
    commits: Vec<Duration>,
    /// The write set's per-phase timings of every block (design §9.5),
    /// empty for a bare engine.
    propose_details: Vec<ProposeTimings>,
    commit_details: Vec<CommitTimings>,
    /// Latency of every `Prefetch` call (design §11: the time the execution
    /// thread spends sending hints).
    hints: Vec<Duration>,
    gaps: Gaps,
    /// The emulated reads of `--hint-noise` (R35): keys drawn uniformly
    /// from the workload's rows.
    noise: StdRng,
    rows: u64,
    started: Instant,
    progress_every: u64,
    label: &'static str,
    /// Target counters at the end of warm-up, subtracted in `finish` so
    /// per-commit ratios cover the measured batches only.
    baseline_stats: Stats,
}

impl Recorder {
    pub fn new(args: &Args) -> Self {
        let g = &args.global_opts;
        Self {
            proposes: Vec::new(),
            commits: Vec::new(),
            propose_details: Vec::new(),
            commit_details: Vec::new(),
            hints: Vec::new(),
            gaps: Gaps::new(g.hint_gap_min_us, g.hint_gap_max_us, g.seed),
            noise: StdRng::seed_from_u64(g.seed ^ 0x9E37_79B9_7F4A_7C15),
            rows: g.rows().max(1),
            started: Instant::now(),
            progress_every: g.progress_every,
            label: args.test_name.name(),
            baseline_stats: Stats::default(),
        }
    }

    /// Forget everything measured so far (end of warm-up); `stats` are the
    /// target's counters at this point.
    pub fn reset(&mut self, stats: Stats) {
        self.proposes.clear();
        self.commits.clear();
        self.propose_details.clear();
        self.commit_details.clear();
        self.hints.clear();
        self.started = Instant::now();
        self.baseline_stats = stats;
    }

    pub fn record(&mut self, t: BatchTiming) {
        self.proposes.push(t.propose);
        self.commits.push(t.commit);
        if let Some(detail) = t.propose_detail {
            self.propose_details.push(detail);
        }
        if let Some(detail) = t.commit_detail {
            self.commit_details.push(detail);
        }
        let n = self.proposes.len() as u64;
        if self.progress_every > 0 && n.is_multiple_of(self.progress_every) {
            let recent = &self.proposes[self.proposes.len() - self.progress_every as usize..];
            let recent_c = &self.commits[self.commits.len() - self.progress_every as usize..];
            eprintln!(
                "[{}] {} batches, last {}: propose mean {:?}, commit mean {:?}, elapsed {:?}",
                self.label,
                n,
                self.progress_every,
                mean(recent),
                mean(recent_c),
                self.started.elapsed()
            );
        }
    }

    /// `n` keys drawn uniformly from the workload's rows: the emulated
    /// reads of `--hint-noise`, hinted but not written.
    fn noise_keys(&mut self, n: usize) -> Vec<BenchKey> {
        (0..n).map(|_| row_key(self.noise.random_range(0..self.rows))).collect()
    }

    /// Sleeps one sampled gap: the emulated execution between two hints.
    fn gap(&mut self) {
        let gap = self.gaps.sample();
        if !gap.is_zero() {
            std::thread::sleep(gap);
        }
    }

    pub fn finish(&self, stats: Stats) -> Measurements {
        let commits = self.commits.len() as u64;
        let per_commit = |x: u64| if commits == 0 { 0.0 } else { x as f64 / commits as f64 };
        let stats = stats.since(&self.baseline_stats);
        let spec = stats.speculation;
        Measurements {
            commits,
            elapsed: self.started.elapsed(),
            propose_mean: mean(&self.proposes),
            propose_p99: p99(&self.proposes),
            commit_mean: mean(&self.commits),
            commit_p99: p99(&self.commits),
            hint_time: self.hints.iter().sum(),
            hint_mean: mean(&self.hints),
            hint_p99: p99(&self.hints),
            start_occupancy: self.baseline_stats.occupancy,
            final_occupancy: stats.occupancy,
            evictions_per_commit: per_commit(stats.total_evictions),
            storage_ops_per_commit: per_commit(stats.total_storage_ops),
            puts_per_commit: per_commit(stats.puts),
            in_place_update_ratio: if stats.puts == 0 {
                0.0
            } else {
                stats.in_place_updates as f64 / stats.puts as f64
            },
            reused_ops_per_commit: per_commit(spec.reused_ops),
            reverted_ops_per_commit: per_commit(spec.reverted_ops),
            steps_per_commit: per_commit(spec.steps),
            peak_rss_bytes: peak_rss_bytes(),
            worker: spec,
            breakdown: (!self.propose_details.is_empty())
                .then(|| Breakdown::new(&self.propose_details, &self.commit_details)),
        }
    }
}

/// `VmHWM` from `/proc/self/status` (Linux), else 0.
fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmHWM:")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|kb| kb.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
        })
        .unwrap_or(0)
}

/// `--prefill`: inserts rows `0..rows()` in `batch_size` batches through the
/// target (through the write set when there is one, so firewood holds all
/// but the set's last occupants), then forgets the timings. Lets the
/// update workloads run against a trie of `rows()` keys, which is where
/// firewood's hashing cost lives (design §11).
fn prefill(target: &mut dyn Target, args: &Args, rec: &mut Recorder) -> Result<(), Box<dyn Error>> {
    let g = &args.global_opts;
    if !g.prefill {
        return Ok(());
    }
    let rows = g.rows();
    let mut start = 0;
    while start < rows {
        let count = g.batch_size.min(rows - start);
        apply(target, args, rec, inserts(start, count).collect())?;
        start += count;
    }
    eprintln!("[prefill] {rows} rows in {:?}", rec.started.elapsed());
    rec.reset(target.stats());
    Ok(())
}

/// Applies one batch to the target. With `--hints` the block is emulated
/// first (design §9.4, §11): the batch is cut into `hint_chunks` chunks, one
/// per emulated transaction; each chunk's keys are hinted (`Prefetch`, its
/// latency recorded) after the sampled execution gap, with `--hint-noise`
/// extra keys per chunk the block does not write (emulated reads), and the
/// block is then proposed as the union of the chunks, the last write of a
/// key winning, in key order.
pub fn apply(
    target: &mut dyn Target,
    args: &Args,
    rec: &mut Recorder,
    batch: Vec<BatchOp<BenchKey, BenchValue>>,
) -> Result<(), Box<dyn Error>> {
    let batch = if args.global_opts.hints {
        let chunks = args.global_opts.hint_chunks.max(1) as usize;
        let per = batch.len().div_ceil(chunks).max(1);
        for (i, chunk) in batch.chunks(per).enumerate() {
            if i > 0 {
                rec.gap();
            }
            let mut keys: Vec<BenchKey> = chunk.iter().filter_map(|op| op.key().copied()).collect();
            let extra = (keys.len() as f64 * args.global_opts.hint_noise).round() as usize;
            keys.extend(rec.noise_keys(extra));
            let d = target.prefetch(&keys);
            rec.hints.push(d);
        }
        let mut union: BTreeMap<BenchKey, BatchOp<BenchKey, BenchValue>> = BTreeMap::new();
        for op in batch {
            if let Some(key) = op.key().copied() {
                union.insert(key, op);
            }
        }
        union.into_values().collect()
    } else {
        batch
    };
    let timing = target.apply(batch)?;
    rec.record(timing);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn run(extra: &[&str]) -> Result<Measurements, Box<dyn Error>> {
        let mut argv = vec![
            "benchmark",
            "--engine",
            "memory",
            "--capacity",
            "10",
            "-b",
            "4",
            "--progress-every",
            "0",
        ];
        argv.extend_from_slice(extra);
        let args = Args::try_parse_from(argv)?;
        let mut target = crate::build_target(&args)?;
        let measured = args.test_name.run(&mut *target, &args);
        target.finish()?;
        measured
    }

    #[test]
    fn measurement_requires_low_watermark_after_warmup() {
        let empty = run(&["--iterations", "1", "zipf"]).unwrap_err();
        assert!(empty.to_string().contains("0 < 7"));
        let short_prefill = run(&["-n", "1", "--prefill", "--iterations", "1", "zipf"]).unwrap_err();
        assert!(short_prefill.to_string().contains("4 < 7"));

        let filled = run(&[
            "-n",
            "3",
            "--prefill",
            "--iterations",
            "3",
            "--warmup-batches",
            "1",
            "zipf",
        ])
        .unwrap();
        assert_eq!(filled.commits, 2);
        assert!(filled.start_occupancy >= 7);

        let warmed = run(&["--iterations", "4", "--warmup-batches", "2", "tenkrandom"]).unwrap();
        assert_eq!(warmed.commits, 2);
        assert!(warmed.start_occupancy >= 7);

        // Rewriting a small fixed key set cannot make warm-up sufficient.
        let too_small = run(&["--iterations", "4", "--warmup-batches", "2", "single"]).unwrap_err();
        assert!(too_small.to_string().contains("4 < 7"));
        let no_measurement = run(&[
            "-n",
            "3",
            "--prefill",
            "--iterations",
            "2",
            "--warmup-batches",
            "2",
            "zipf",
        ])
        .unwrap_err();
        assert!(no_measurement.to_string().contains("before measurement could start"));

        let bare = run(&["--baseline", "--iterations", "1", "zipf"]).unwrap();
        assert_eq!((bare.start_occupancy, bare.commits), (0, 1));
    }
}
