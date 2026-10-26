//! Per-run metrics, emitted as one CSV record (design §11). A workload returns
//! [`Measurements`]; the driver adds the configuration columns.

use std::time::Duration;

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
    /// (the `zipf` workload's hit rate).
    pub in_place_update_ratio: f64,
    /// Design §9.5: pairs omitted from the final proposal because a link
    /// already carried them, per commit.
    pub reused_ops_per_commit: f64,
    /// Design §9.5: reverts for mispredicted keys, per commit.
    pub reverted_ops_per_commit: f64,
    pub links_per_commit: f64,
    pub peak_rss_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct Report {
    pub workload: &'static str,
    pub policy: &'static str,
    pub vc: &'static str,
    pub capacity: usize,
    pub low_watermark: usize,
    pub batch_size: u64,
    pub revisions: usize,
    pub speculate: bool,
    pub speculation_rounds: u32,
    pub speculation_min_link: usize,
    pub speculation_max_links: usize,
    pub speculation_margin: usize,
    pub hints: bool,
    pub hint_chunks: u32,
    pub storage_latency_us: u64,
    pub seed: u64,
    pub baseline: bool,
    pub m: Measurements,
}

impl Report {
    pub const CSV_HEADER: &'static str = "workload,policy,vc,capacity,low_watermark,batch_size,revisions,speculate,\
        speculation_rounds,speculation_min_link,speculation_max_links,speculation_margin,hints,hint_chunks,\
        storage_latency_us,seed,baseline,\
        commits,elapsed_s,\
        propose_mean_us,propose_p99_us,\
        commit_mean_us,commit_p99_us,hint_time_s,final_occupancy,evictions_per_commit,storage_ops_per_commit,\
        in_place_update_ratio,reused_ops_per_commit,reverted_ops_per_commit,links_per_commit,peak_rss_bytes";

    pub fn to_csv(&self) -> String {
        todo!()
    }
}
