//! `single` workload (design §11). Tiny batch on the same keys every commit. Storage should see zero writes.

use std::error::Error;

use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::MemoryStorage;
use auth_wal::vc::VectorCommitment;

use crate::report::Measurements;
use crate::{Args, BenchDb, BenchKey, BenchValue, TestRunner};

#[derive(Clone, Default)]
pub struct Single;

impl TestRunner for Single {
    const NAME: &'static str = "single";

    fn run<C, P>(&self, db: &BenchDb<C, P>, args: &Args) -> Result<Measurements, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey, BenchValue>,
        P: EvictionPolicy<BenchKey>,
    {
        todo!("design §11 single")
    }

    fn run_baseline(
        &self,
        db: &MemoryStorage<BenchKey, BenchValue>,
        args: &Args,
    ) -> Result<Measurements, Box<dyn Error>> {
        todo!("design §11 single baseline")
    }
}
