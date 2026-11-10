//! Per-run metrics, emitted as one CSV record (design §11), and the
//! per-phase latency breakdown printed with `--breakdown` (design §9.5). A
//! workload returns [`Measurements`]; the driver adds the configuration
//! columns.

use std::time::Duration;

use clap::ValueEnum;
use right_cash::speculate::SpeculationStats;
use right_cash::storage::{MemoryStorage, StorageEngine};
use right_cash::timings::{CommitTimings, ProposeTimings};

use crate::{Args, BenchKey, BenchValue, EngineArg, VcArg};

#[derive(Debug)]
pub struct Measurements {
    pub commits: u64,
    pub elapsed: Duration,
    pub propose_mean: Duration,
    pub propose_p99: Duration,
    pub commit_mean: Duration,
    pub commit_p99: Duration,
    /// Total time spent in `Prefetch` calls (the hint path, design §9.4).
    pub hint_time: Duration,
    /// Per-call `Prefetch` latency (design §11).
    pub hint_mean: Duration,
    pub hint_p99: Duration,
    /// Occupancy immediately before the first measured block; zero for
    /// the bare engine and the intentionally empty-start `create` workload.
    pub start_occupancy: usize,
    pub final_occupancy: usize,
    pub evictions_per_commit: f64,
    pub storage_ops_per_commit: f64,
    /// Puts applied per commit: the block's size after the hint-mode union.
    pub puts_per_commit: f64,
    /// `in_place_updates / puts`: the share of writes absorbed by the set
    /// (the `zipf` workload's hit rate). Zero for a bare engine.
    pub in_place_update_ratio: f64,
    /// Design §9.5: pairs omitted from the final proposal because a step
    /// already carried them, per commit.
    pub reused_ops_per_commit: f64,
    /// Design §9.5: reverts for mispredicted keys, per commit.
    pub reverted_ops_per_commit: f64,
    pub steps_per_commit: f64,
    pub peak_rss_bytes: u64,
    /// The worker's side over the measured blocks (design §9.5): the steps,
    /// their time off every caller's path, and what went wrong; zeros for a
    /// bare engine.
    pub worker: SpeculationStats,
    /// The per-phase breakdown over the measured blocks; `None` for a bare
    /// engine (one engine call per block, nothing to split).
    pub breakdown: Option<Breakdown>,
}

/// Microseconds of a duration, for the tables and the CSV.
fn micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

pub fn mean(xs: &[Duration]) -> Duration {
    if xs.is_empty() {
        return Duration::ZERO;
    }
    xs.iter().sum::<Duration>() / xs.len() as u32
}

pub fn p99(xs: &[Duration]) -> Duration {
    if xs.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64) * 0.99).ceil() as usize;
    sorted[idx.clamp(1, sorted.len()) - 1]
}

/// One phase of a breakdown: its mean and p99 over the measured blocks and
/// the mean's share of the mean total.
#[derive(Debug)]
pub struct Phase {
    pub name: &'static str,
    pub mean: Duration,
    pub p99: Duration,
    pub share: f64,
}

/// Where propose and commit spent their time over the measured blocks
/// (design §9.5, §11), phase by phase, with the block shape it was measured
/// on.
#[derive(Debug)]
pub struct Breakdown {
    pub blocks: usize,
    pub batch_ops: f64,
    pub evicted: f64,
    pub storage_ops: f64,
    pub steps: f64,
    /// Share of the blocks that took the speculative path.
    pub speculative: f64,
    pub propose: Vec<Phase>,
    pub commit: Vec<Phase>,
}

impl Breakdown {
    pub fn new(propose: &[ProposeTimings], commit: &[CommitTimings]) -> Self {
        let blocks = propose.len();
        let per_block = |x: f64| if blocks == 0 { 0.0 } else { x / blocks as f64 };
        let phases = |total: Duration, columns: Vec<(&'static str, Vec<Duration>)>| -> Vec<Phase> {
            columns
                .into_iter()
                .map(|(name, xs)| {
                    let m = mean(&xs);
                    Phase {
                        name,
                        mean: m,
                        p99: p99(&xs),
                        share: if total.is_zero() {
                            0.0
                        } else {
                            m.as_secs_f64() / total.as_secs_f64()
                        },
                    }
                })
                .collect()
        };
        let column = |f: fn(&ProposeTimings) -> Duration| propose.iter().map(f).collect::<Vec<_>>();
        let propose_total = mean(&column(|t| t.total));
        let propose_phases = phases(
            propose_total,
            vec![
                ("copy", column(|t| t.copy)),
                ("ops", column(|t| t.ops)),
                ("writes", column(|t| t.writes)),
                ("merge", column(|t| t.merge)),
                ("digest", column(|t| t.digest)),
                ("vc", column(|t| t.commitment)),
                ("finish", column(|t| t.finish)),
                ("reads", column(|t| t.reads)),
                ("storage", column(|t| t.storage)),
                ("rest", column(|t| t.rest)),
                ("total", column(|t| t.total)),
            ],
        );
        let column = |f: fn(&CommitTimings) -> Duration| commit.iter().map(f).collect::<Vec<_>>();
        let commit_total = mean(&column(|t| t.total));
        let commit_phases = phases(
            commit_total,
            vec![
                ("engine", column(|t| t.engine)),
                ("advance", column(|t| t.advance)),
                ("retarget", column(|t| t.retarget)),
                ("total", column(|t| t.total)),
            ],
        );
        Self {
            blocks,
            batch_ops: per_block(propose.iter().map(|t| t.batch_ops as f64).sum()),
            evicted: per_block(propose.iter().map(|t| t.evicted as f64).sum()),
            storage_ops: per_block(propose.iter().map(|t| t.storage_ops as f64).sum()),
            steps: per_block(propose.iter().map(|t| f64::from(t.steps)).sum()),
            speculative: per_block(propose.iter().filter(|t| t.speculative).count() as f64),
            propose: propose_phases,
            commit: commit_phases,
        }
    }

    /// The breakdown as text: one table per call, then the worker's side.
    pub fn render(&self, w: &SpeculationStats) -> String {
        let mut out = format!(
            "== propose breakdown: {} blocks | per block {:.1} batch ops, {:.1} evicted, {:.1} storage ops, {:.2} steps | {:.1} % speculative ==\n",
            self.blocks,
            self.batch_ops,
            self.evicted,
            self.storage_ops,
            self.steps,
            self.speculative * 100.0,
        );
        table(&mut out, &self.propose);
        out.push_str("== commit breakdown ==\n");
        table(&mut out, &self.commit);
        let per = |ns: u64, n: u64| if n == 0 { 0.0 } else { ns as f64 / 1e3 / n as f64 };
        let ops = if w.steps_built == 0 {
            0.0
        } else {
            w.step_ops as f64 / w.steps_built as f64
        };
        out.push_str(&format!(
            "== worker: {} steps, {:.1} us and {:.1} ops per step ({} retractions); {} tail steps, {:.1} us per tail step; {} blocks kept the chain; {} steps and {} ops dropped; {} step failures, {} fallbacks, {} panics ==",
            w.steps_built,
            per(w.worker_time_ns, w.steps_built),
            ops,
            w.retractions,
            w.tail_steps,
            per(w.tail_time_ns, w.tail_steps),
            w.chains_kept,
            w.steps_dropped,
            w.ops_dropped,
            w.step_failures,
            w.fallbacks,
            w.worker_panics,
        ));
        out
    }
}

/// One phase table: a header line, then a line per phase.
fn table(out: &mut String, phases: &[Phase]) {
    out.push_str(&format!(
        "{:<10}{:>12}{:>12}{:>9}\n",
        "phase", "mean_us", "p99_us", "share"
    ));
    for p in phases {
        out.push_str(&format!(
            "{:<10}{:>12.1}{:>12.1}{:>8.1}%\n",
            p.name,
            micros(p.mean),
            micros(p.p99),
            p.share * 100.0
        ));
    }
}

/// One run's record: the arguments it ran with and what it measured.
pub struct Report<'a> {
    pub args: &'a Args,
    pub m: Measurements,
}

impl Report<'_> {
    /// The CSV columns, name and value, in order: one list serves the
    /// header and the record, so the two cannot drift apart. `target` names
    /// the bare engine of `--baseline`, else `right_cash`.
    fn columns(&self) -> Vec<(&'static str, String)> {
        let g = &self.args.global_opts;
        let m = &self.m;
        let target = if g.baseline {
            value_name(g.engine)
        } else {
            "right_cash".to_string()
        };
        let us = |d: Duration| format!("{:.1}", micros(d));
        let elapsed = m.elapsed.as_secs_f64();
        let rate = if elapsed > 0.0 { m.commits as f64 / elapsed } else { 0.0 };
        // Use the backend's actual identifier: dependency feature unification
        // can enable ethhash even without this binary's forwarding feature.
        let engine_id = match g.engine {
            EngineArg::Memory => {
                <MemoryStorage<BenchKey, BenchValue> as StorageEngine<BenchKey, BenchValue>>::ENGINE_ID
            }
            #[cfg(feature = "firewood")]
            EngineArg::Firewood => {
                <right_cash::storage::FirewoodStorage as StorageEngine<BenchKey, BenchValue>>::ENGINE_ID
            }
            #[cfg(not(feature = "firewood"))]
            EngineArg::Firewood => unreachable!("build_target rejects an unavailable engine"),
        };
        vec![
            ("workload", self.args.test_name.name().to_string()),
            ("target", target),
            ("engine", value_name(g.engine)),
            ("engine_id", engine_id.to_string()),
            ("policy", value_name(g.policy)),
            ("vc", value_name(g.vc)),
            (
                "merkle_branching",
                match g.vc {
                    VcArg::MerkleBlake3 => g.merkle_branching.to_string(),
                    _ => String::new(),
                },
            ),
            ("capacity", g.capacity.to_string()),
            ("low_watermark", g.watermark().to_string()),
            ("high_watermark", g.trigger().to_string()),
            ("batch_size", g.batch_size.to_string()),
            ("number_of_batches", g.number_of_batches.to_string()),
            ("rows", g.rows().to_string()),
            ("zipf_exponent", g.zipf_exponent.to_string()),
            ("cache_memory_limit", g.cache_memory_limit.to_string()),
            ("firewood_commit_permits", g.commit_permits().to_string()),
            ("iterations", g.iterations.to_string()),
            ("duration_minutes", g.duration_minutes.to_string()),
            ("warmup_batches", g.warmup_batches.to_string()),
            ("revisions", g.revisions.to_string()),
            ("speculate", g.speculate.to_string()),
            ("speculation_rounds", g.speculation_rounds.to_string()),
            ("speculation_min_step", g.speculation_min_step.to_string()),
            ("speculation_margin", g.speculation_margin.to_string()),
            ("hints", g.hints.to_string()),
            ("hint_chunks", g.hint_chunks.to_string()),
            ("hint_gap_min_us", g.hint_gap_min_us.to_string()),
            ("hint_gap_max_us", g.hint_gap_max_us.to_string()),
            ("hint_noise", format!("{:.2}", g.hint_noise)),
            ("storage_latency_us", g.storage_latency_us.to_string()),
            ("seed", g.seed.to_string()),
            ("baseline", g.baseline.to_string()),
            ("prefill", g.prefill.to_string()),
            ("commits", m.commits.to_string()),
            ("elapsed_s", format!("{elapsed:.3}")),
            ("commits_per_s", format!("{rate:.2}")),
            ("propose_mean_us", us(m.propose_mean)),
            ("propose_p99_us", us(m.propose_p99)),
            ("commit_mean_us", us(m.commit_mean)),
            ("commit_p99_us", us(m.commit_p99)),
            ("hint_time_s", format!("{:.3}", m.hint_time.as_secs_f64())),
            ("hint_mean_us", us(m.hint_mean)),
            ("hint_p99_us", us(m.hint_p99)),
            ("worker_time_s", format!("{:.3}", m.worker.worker_time_ns as f64 / 1e9)),
            ("start_occupancy", m.start_occupancy.to_string()),
            ("final_occupancy", m.final_occupancy.to_string()),
            ("evictions_per_commit", format!("{:.2}", m.evictions_per_commit)),
            ("storage_ops_per_commit", format!("{:.2}", m.storage_ops_per_commit)),
            ("puts_per_commit", format!("{:.2}", m.puts_per_commit)),
            ("in_place_update_ratio", format!("{:.4}", m.in_place_update_ratio)),
            ("reused_ops_per_commit", format!("{:.2}", m.reused_ops_per_commit)),
            ("reverted_ops_per_commit", format!("{:.2}", m.reverted_ops_per_commit)),
            ("steps_per_commit", format!("{:.3}", m.steps_per_commit)),
            ("peak_rss_bytes", m.peak_rss_bytes.to_string()),
            ("step_failures", m.worker.step_failures.to_string()),
            ("fallbacks", m.worker.fallbacks.to_string()),
        ]
    }

    /// The CSV header and the record, two lines.
    pub fn csv(&self) -> String {
        let columns = self.columns();
        let header: Vec<&str> = columns.iter().map(|(name, _)| *name).collect();
        let values: Vec<&str> = columns.iter().map(|(_, value)| value.as_str()).collect();
        format!("{}\n{}", header.join(","), values.join(","))
    }
}

/// A flag value's name as clap prints it (`fifo`, `lru-write`, `rehash`).
fn value_name<E: ValueEnum>(value: E) -> String {
    value
        .to_possible_value()
        .expect("a named variant")
        .get_name()
        .to_string()
}
