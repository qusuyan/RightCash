//! `zipf` workload (design §11). Zipf-distributed updates across the row space (seeded; `--warmup-batches` before measuring). The target workload: hot keys should stay resident; reports hit rate vs capacity.

use std::error::Error;

use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::MemoryStorage;
use auth_wal::vc::VectorCommitment;

use crate::report::Measurements;
use crate::{Args, BenchDb, BenchKey, BenchValue, TestRunner};

#[derive(Clone, Default)]
pub struct Zipf;

impl TestRunner for Zipf {
    const NAME: &'static str = "zipf";

    fn run<C, P>(&self, db: &BenchDb<C, P>, args: &Args) -> Result<Measurements, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey, BenchValue>,
        P: EvictionPolicy<BenchKey>,
    {
        todo!("design §11 zipf")
    }

    fn run_baseline(
        &self,
        db: &MemoryStorage<BenchKey, BenchValue>,
        args: &Args,
    ) -> Result<Measurements, Box<dyn Error>> {
        todo!("design §11 zipf baseline")
    }
}
