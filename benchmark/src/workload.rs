//! Shared pieces of the four workloads: firewood's key derivation and batch
//! generators, the stop rule, hint emulation, and the latency recorder.

use std::error::Error;
use std::time::{Duration, Instant};

use auth_wal::types::BatchOp;
use sha2::{Digest, Sha256};

use crate::report::Measurements;
use crate::target::{BatchTiming, Target, TargetStats};
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

/// Stop after `iterations` batches when set, else when the duration elapses
/// (firewood's rule).
pub struct Clock {
    start: Instant,
    deadline: Duration,
    iterations: u64,
    done: u64,
}

impl Clock {
    pub fn new(args: &Args) -> Self {
        Self {
            start: Instant::now(),
            deadline: Duration::from_secs(args.global_opts.duration_minutes * 60),
            iterations: args.global_opts.iterations,
            done: 0,
        }
    }

    pub fn tick(&mut self) -> bool {
        self.done += 1;
        (self.iterations > 0 && self.done >= self.iterations) || self.start.elapsed() >= self.deadline
    }
}

/// Collects per-batch timings and turns them into [`Measurements`].
pub struct Recorder {
    proposes: Vec<Duration>,
    commits: Vec<Duration>,
    hint: Duration,
    started: Instant,
    progress_every: u64,
    label: &'static str,
    /// Target counters at the end of warm-up, subtracted in `finish` so
    /// per-commit ratios cover the measured batches only.
    baseline_stats: TargetStats,
}

impl Recorder {
    pub fn new(label: &'static str, args: &Args) -> Self {
        Self {
            proposes: Vec::new(),
            commits: Vec::new(),
            hint: Duration::ZERO,
            started: Instant::now(),
            progress_every: args.global_opts.progress_every,
            label,
            baseline_stats: TargetStats::default(),
        }
    }

    /// Forget everything measured so far (end of warm-up); `stats` are the
    /// target's counters at this point.
    pub fn reset(&mut self, stats: TargetStats) {
        self.proposes.clear();
        self.commits.clear();
        self.hint = Duration::ZERO;
        self.started = Instant::now();
        self.baseline_stats = stats;
    }

    pub fn record(&mut self, t: BatchTiming) {
        self.proposes.push(t.propose);
        self.commits.push(t.commit);
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

    pub fn add_hint_time(&mut self, d: Duration) {
        self.hint += d;
    }

    pub fn finish(&self, stats: TargetStats) -> Measurements {
        let commits = self.commits.len() as u64;
        let per_commit = |x: u64| if commits == 0 { 0.0 } else { x as f64 / commits as f64 };
        let base = &self.baseline_stats;
        let stats = TargetStats {
            occupancy: stats.occupancy,
            evictions: stats.evictions - base.evictions,
            storage_ops: stats.storage_ops - base.storage_ops,
            puts: stats.puts - base.puts,
            in_place_updates: stats.in_place_updates - base.in_place_updates,
            speculation: stats.speculation,
        };
        let spec = stats.speculation.unwrap_or_default();
        Measurements {
            commits,
            elapsed: self.started.elapsed(),
            propose_mean: mean(&self.proposes),
            propose_p99: p99(&self.proposes),
            commit_mean: mean(&self.commits),
            commit_p99: p99(&self.commits),
            hint_time: self.hint,
            final_occupancy: stats.occupancy,
            evictions_per_commit: per_commit(stats.evictions),
            storage_ops_per_commit: per_commit(stats.storage_ops),
            in_place_update_ratio: if stats.puts == 0 {
                0.0
            } else {
                stats.in_place_updates as f64 / stats.puts as f64
            },
            reused_ops_per_commit: per_commit(spec.reused_ops),
            reverted_ops_per_commit: per_commit(spec.reverted_ops),
            steps_per_commit: per_commit(spec.steps),
            peak_rss_bytes: peak_rss_bytes(),
        }
    }
}

fn mean(xs: &[Duration]) -> Duration {
    if xs.is_empty() {
        return Duration::ZERO;
    }
    xs.iter().sum::<Duration>() / xs.len() as u32
}

fn p99(xs: &[Duration]) -> Duration {
    if xs.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
    sorted[idx.clamp(1, sorted.len()) - 1]
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

/// Applies one batch to the target, first emulating libevm's per-transaction
/// prefetch hints when `--hints` is on: the batch's keys are split into
/// `hint_chunks` chunks and each chunk is hinted before `Propose` (design §9.4).
pub fn apply(
    target: &mut dyn Target,
    args: &Args,
    rec: &mut Recorder,
    batch: Vec<BatchOp<BenchKey, BenchValue>>,
) -> Result<(), Box<dyn Error>> {
    if args.global_opts.hints {
        let keys: Vec<BenchKey> = batch
            .iter()
            .filter_map(|op| match op {
                BatchOp::Put { key, .. } | BatchOp::Delete { key } => Some(*key),
                BatchOp::PrefixDelete { .. } => None,
            })
            .collect();
        let chunks = args.global_opts.hint_chunks.max(1) as usize;
        let per = keys.len().div_ceil(chunks).max(1);
        for chunk in keys.chunks(per) {
            let d = target.prefetch(chunk);
            rec.add_hint_time(d);
        }
    }
    let timing = target.apply(batch)?;
    rec.record(timing);
    Ok(())
}
