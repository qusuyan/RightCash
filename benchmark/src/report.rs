//! Per-run metrics, emitted as one CSV record (design §11). A workload returns
//! [`Measurements`]; the driver adds the configuration columns.

use std::time::Duration;

use crate::Args;

#[derive(Clone, Debug, Default)]
pub struct Measurements {
    pub commits: u64,
    pub elapsed: Duration,
    pub propose_mean: Duration,
    pub propose_p99: Duration,
    pub commit_mean: Duration,
    pub commit_p99: Duration,
    /// Total time spent in `Prefetch` calls (the hint path, design §9.4).
    pub hint_time: Duration,
    pub final_occupancy: usize,
    pub evictions_per_commit: f64,
    pub storage_ops_per_commit: f64,
    /// `in_place_updates / puts`: the share of writes absorbed by the set
    /// (the `zipf` workload's hit rate). Zero for a bare engine.
    pub in_place_update_ratio: f64,
    /// Design §9.5: pairs omitted from the final proposal because a link
    /// already carried them, per commit.
    pub reused_ops_per_commit: f64,
    /// Design §9.5: reverts for mispredicted keys, per commit.
    pub reverted_ops_per_commit: f64,
    pub steps_per_commit: f64,
    pub peak_rss_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct Report {
    pub workload: &'static str,
    pub target: &'static str,
    pub policy: &'static str,
    pub vc: &'static str,
    pub capacity: usize,
    pub low_watermark: usize,
    pub batch_size: u64,
    pub revisions: usize,
    pub speculate: bool,
    pub speculation_rounds: u32,
    pub speculation_min_step: usize,
    pub speculation_margin: usize,
    pub hints: bool,
    pub hint_chunks: u32,
    pub storage_latency_us: u64,
    pub seed: u64,
    pub baseline: bool,
    pub m: Measurements,
}

impl Report {
    pub const CSV_HEADER: &'static str = "workload,target,policy,vc,capacity,low_watermark,batch_size,revisions,speculate,\
        speculation_rounds,speculation_min_step,speculation_margin,hints,hint_chunks,\
        storage_latency_us,seed,baseline,commits,elapsed_s,commits_per_s,propose_mean_us,propose_p99_us,\
        commit_mean_us,commit_p99_us,hint_time_s,final_occupancy,evictions_per_commit,storage_ops_per_commit,\
        in_place_update_ratio,reused_ops_per_commit,reverted_ops_per_commit,steps_per_commit,peak_rss_bytes";

    pub fn new(workload: &'static str, target: &'static str, args: &Args, m: Measurements) -> Self {
        let g = &args.global_opts;
        Self {
            workload,
            target,
            policy: match g.policy {
                crate::PolicyArg::Fifo => "fifo",
                crate::PolicyArg::LruWrite => "lru-write",
            },
            vc: match g.vc {
                crate::VcArg::Lthash => "lthash",
                crate::VcArg::Merkle => "merkle",
            },
            capacity: g.capacity,
            low_watermark: g
                .low_watermark
                // The R16 convention for the v0 policies, half the set (design §6.2);
                // the watermark is each policy's own parameter, not the trait's.
                .unwrap_or(g.capacity / 2),
            batch_size: g.batch_size,
            revisions: g.revisions,
            speculate: g.speculate,
            speculation_rounds: g.speculation_rounds,
            speculation_min_step: g.speculation_min_step,
            speculation_margin: g.speculation_margin,
            hints: g.hints,
            hint_chunks: g.hint_chunks,
            storage_latency_us: g.storage_latency_us,
            seed: g.seed,
            baseline: g.baseline,
            m,
        }
    }

    pub fn to_csv(&self) -> String {
        let m = &self.m;
        let us = |d: Duration| format!("{:.1}", d.as_secs_f64() * 1e6);
        let elapsed = m.elapsed.as_secs_f64();
        let rate = if elapsed > 0.0 { m.commits as f64 / elapsed } else { 0.0 };
        [
            self.workload.to_string(),
            self.target.to_string(),
            self.policy.to_string(),
            self.vc.to_string(),
            self.capacity.to_string(),
            self.low_watermark.to_string(),
            self.batch_size.to_string(),
            self.revisions.to_string(),
            self.speculate.to_string(),
            self.speculation_rounds.to_string(),
            self.speculation_min_step.to_string(),
            self.speculation_margin.to_string(),
            self.hints.to_string(),
            self.hint_chunks.to_string(),
            self.storage_latency_us.to_string(),
            self.seed.to_string(),
            self.baseline.to_string(),
            m.commits.to_string(),
            format!("{elapsed:.3}"),
            format!("{rate:.2}"),
            us(m.propose_mean),
            us(m.propose_p99),
            us(m.commit_mean),
            us(m.commit_p99),
            format!("{:.3}", m.hint_time.as_secs_f64()),
            m.final_occupancy.to_string(),
            format!("{:.2}", m.evictions_per_commit),
            format!("{:.2}", m.storage_ops_per_commit),
            format!("{:.4}", m.in_place_update_ratio),
            format!("{:.2}", m.reused_ops_per_commit),
            format!("{:.2}", m.reverted_ops_per_commit),
            format!("{:.3}", m.steps_per_commit),
            m.peak_rss_bytes.to_string(),
        ]
        .join(",")
    }
}
