//! `tenkrandom` workload (design §11). Per batch: delete 2,500 rows from `low`, insert 2,500 from `high`, update 5,000 from the midpoint; then advance both by 2,500. Exercises Put/Delete mixes and tombstones.

use std::error::Error;

use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::MemoryStorage;
use auth_wal::vc::VectorCommitment;

use crate::report::Measurements;
use crate::{Args, BenchDb, BenchKey, BenchValue, TestRunner};

#[derive(Clone, Default)]
pub struct TenKRandom;

impl TestRunner for TenKRandom {
    const NAME: &'static str = "tenkrandom";

    fn run<C, P>(&self, db: &BenchDb<C, P>, args: &Args) -> Result<Measurements, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey, BenchValue>,
        P: EvictionPolicy<BenchKey>,
    {
        todo!("design §11 tenkrandom")
    }

    fn run_baseline(
        &self,
        db: &MemoryStorage<BenchKey, BenchValue>,
        args: &Args,
    ) -> Result<Measurements, Box<dyn Error>> {
        todo!("design §11 tenkrandom baseline")
    }
}
