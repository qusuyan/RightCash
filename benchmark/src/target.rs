//! What a workload drives: a storage engine on its own (the baseline, with
//! vanilla firewood as the comparison point) or auth_wal in front of one
//! (`WriteSet`). Both expose the same operations so the workloads and the
//! report do not care which is running.

use std::error::Error;
use std::time::{Duration, Instant};

use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::{StorageEngine, StorageProposal};
use auth_wal::timings::{CommitTimings, ProposeTimings};
use auth_wal::types::BatchOp;
use auth_wal::vc::VectorCommitment;
use auth_wal::{AuthWal, Reader, Stats};

use crate::{BenchKey, BenchValue};

/// Timings of one block: propose (root known) and commit (accepted), and
/// the write set's per-phase breakdown of each (design §9.5), `None` for a
/// bare engine.
#[derive(Clone, Copy, Debug)]
pub struct BatchTiming {
    pub propose: Duration,
    pub commit: Duration,
    pub propose_detail: Option<ProposeTimings>,
    pub commit_detail: Option<CommitTimings>,
}

pub trait Target {
    /// Propose `batch` on the committed state and commit it, like one block.
    fn apply(&mut self, batch: Vec<BatchOp<BenchKey, BenchValue>>) -> Result<BatchTiming, Box<dyn Error>>;

    /// Prefetch hint (design §9.4) relative to the committed state. Returns
    /// the time spent in the call.
    fn prefetch(&mut self, keys: &[BenchKey]) -> Duration;

    /// The counters the report derives its ratios from: auth_wal's own
    /// `Stats`. A bare engine fills `total_storage_ops` and `puts` and
    /// leaves the rest zero (it has no set).
    fn stats(&self) -> Stats;

    /// Close the target (persist, release handles).
    fn finish(&mut self) -> Result<(), Box<dyn Error>>;
}

/// A bare storage engine: every batch is one proposal and one commit.
pub struct Baseline<S: StorageEngine<BenchKey, BenchValue>> {
    engine: S,
    storage_ops: u64,
    puts: u64,
}

impl<S: StorageEngine<BenchKey, BenchValue>> Baseline<S> {
    pub fn new(engine: S) -> Self {
        Self {
            engine,
            storage_ops: 0,
            puts: 0,
        }
    }
}

impl<S: StorageEngine<BenchKey, BenchValue>> Target for Baseline<S> {
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
            propose_detail: None,
            commit_detail: None,
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

    fn stats(&self) -> Stats {
        Stats {
            total_storage_ops: self.storage_ops,
            puts: self.puts,
            ..Stats::default()
        }
    }

    fn finish(&mut self) -> Result<(), Box<dyn Error>> {
        self.engine.close()?;
        Ok(())
    }
}

/// auth_wal in front of a storage engine: every batch is one `propose` on
/// the committed state and one `commit`, like one block; hints go to
/// `AuthWal::prefetch` on the committed state.
pub struct WriteSet<C, P, S>
where
    C: VectorCommitment<BenchKey>,
    P: EvictionPolicy<BenchKey>,
    S: StorageEngine<BenchKey, BenchValue>,
{
    wal: AuthWal<BenchKey, BenchValue, C, P, S>,
    reader: Reader<BenchKey, BenchValue, C, P, S>,
}

impl<C, P, S> WriteSet<C, P, S>
where
    C: VectorCommitment<BenchKey>,
    P: EvictionPolicy<BenchKey>,
    S: StorageEngine<BenchKey, BenchValue>,
{
    pub fn new(wal: AuthWal<BenchKey, BenchValue, C, P, S>) -> Self {
        let reader = wal.reader();
        Self { wal, reader }
    }
}

impl<C, P, S> Target for WriteSet<C, P, S>
where
    C: VectorCommitment<BenchKey>,
    P: EvictionPolicy<BenchKey>,
    S: StorageEngine<BenchKey, BenchValue>,
{
    fn apply(&mut self, batch: Vec<BatchOp<BenchKey, BenchValue>>) -> Result<BatchTiming, Box<dyn Error>> {
        let t0 = Instant::now();
        let proposal = self.wal.propose(None, batch, None)?;
        let t1 = Instant::now();
        self.wal.commit(proposal)?;
        let t2 = Instant::now();
        Ok(BatchTiming {
            propose: t1 - t0,
            commit: t2 - t1,
            propose_detail: Some(self.wal.last_propose_timings()),
            commit_detail: Some(self.wal.last_commit_timings()),
        })
    }

    fn prefetch(&mut self, keys: &[BenchKey]) -> Duration {
        let t0 = Instant::now();
        self.wal.prefetch(None, keys);
        t0.elapsed()
    }

    fn stats(&self) -> Stats {
        self.reader.stats()
    }

    fn finish(&mut self) -> Result<(), Box<dyn Error>> {
        self.wal.close()?;
        Ok(())
    }
}
