//! What a workload drives: a storage engine on its own (the baseline, with
//! vanilla firewood as the comparison point) or auth_wal in front of one
//! (milestone 1). Both expose the same three operations so the workloads and
//! the report do not care which is running.

use std::error::Error;
use std::time::{Duration, Instant};

use auth_wal::speculate::SpeculationStats;
use auth_wal::storage::{StorageEngine, StorageProposal};
use auth_wal::types::BatchOp;

use crate::{BenchKey, BenchValue};

/// Timings of one block: propose (root known) and commit (accepted).
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchTiming {
    pub propose: Duration,
    pub commit: Duration,
}

/// Counters the report derives its ratios from. Zero where the target has no
/// such notion (a bare engine has no set).
#[derive(Clone, Copy, Debug, Default)]
pub struct TargetStats {
    pub occupancy: usize,
    pub evictions: u64,
    pub storage_ops: u64,
    pub puts: u64,
    pub in_place_updates: u64,
    pub speculation: Option<SpeculationStats>,
}

pub trait Target {
    fn name(&self) -> &'static str;

    /// Propose `batch` on the committed state and commit it, like one block.
    fn apply(&mut self, batch: Vec<BatchOp<BenchKey, BenchValue>>) -> Result<BatchTiming, Box<dyn Error>>;

    /// Prefetch hint (design §9.4) relative to the committed state. Returns
    /// the time spent in the call.
    fn prefetch(&mut self, keys: &[BenchKey]) -> Duration;

    fn stats(&self) -> TargetStats;

    /// Close the target (persist, release handles).
    fn finish(&mut self) -> Result<(), Box<dyn Error>>;
}

/// A bare storage engine: every batch is one proposal and one commit.
pub struct Baseline<S: StorageEngine<BenchKey, BenchValue>> {
    engine: S,
    name: &'static str,
    storage_ops: u64,
    puts: u64,
}

impl<S: StorageEngine<BenchKey, BenchValue>> Baseline<S> {
    pub fn new(engine: S, name: &'static str) -> Self {
        Self {
            engine,
            name,
            storage_ops: 0,
            puts: 0,
        }
    }
}

impl<S: StorageEngine<BenchKey, BenchValue>> Target for Baseline<S> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn apply(&mut self, batch: Vec<BatchOp<BenchKey, BenchValue>>) -> Result<BatchTiming, Box<dyn Error>> {
        self.storage_ops += batch.len() as u64;
        self.puts += batch.iter().filter(|op| matches!(op, BatchOp::Put { .. })).count() as u64;
        let t0 = Instant::now();
        let proposal = self.engine.head()?.propose(&batch, None)?;
        let t1 = Instant::now();
        proposal.commit()?;
        let t2 = Instant::now();
        Ok(BatchTiming {
            propose: t1 - t0,
            commit: t2 - t1,
        })
    }

    /// Warms the engine's paths for `keys` through the committed handle; on
    /// firewood this is what the node cache sees under `CacheReadStrategy::All`.
    fn prefetch(&mut self, keys: &[BenchKey]) -> Duration {
        let t0 = Instant::now();
        if let Ok(head) = self.engine.head() {
            let _ = head.prefetch(keys);
        }
        t0.elapsed()
    }

    fn stats(&self) -> TargetStats {
        TargetStats {
            storage_ops: self.storage_ops,
            puts: self.puts,
            ..TargetStats::default()
        }
    }

    fn finish(&mut self) -> Result<(), Box<dyn Error>> {
        self.engine.close()?;
        Ok(())
    }
}
