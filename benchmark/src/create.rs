//! `create` workload (design §11). Inserts rows from an empty set, `batch_size` per commit. Every batch overflows the set, so this measures pure eviction throughput.

use std::error::Error;

use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::MemoryStorage;
use auth_wal::vc::VectorCommitment;

use crate::report::Measurements;
use crate::{Args, BenchDb, BenchKey, BenchValue, TestRunner};

#[derive(Clone, Default)]
pub struct Create;

impl TestRunner for Create {
    const NAME: &'static str = "create";

    fn run<C, P>(&self, db: &BenchDb<C, P>, args: &Args) -> Result<Measurements, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey, BenchValue>,
        P: EvictionPolicy<BenchKey>,
    {
        todo!("design §11 create")
    }

    fn run_baseline(
        &self,
        db: &MemoryStorage<BenchKey, BenchValue>,
        args: &Args,
    ) -> Result<Measurements, Box<dyn Error>> {
        todo!("design §11 create baseline")
    }
}
