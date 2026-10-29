//! `zipf` workload (design §11), firewood's: `batch_size` updates per batch
//! with rows drawn from a Zipf distribution over `rows()` (exponent
//! `--zipf-exponent`, default 1.2), value `sha256(batch_id)`. Seeded, with
//! `--warmup-batches` before measuring. The target workload for the write
//! set: hot keys should stay resident, and the report's in-place update ratio
//! is its hit rate.

use std::error::Error;

use auth_wal::types::BatchOp;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Zipf as ZipfDist};
use sha2::{Digest, Sha256};

use crate::report::Measurements;
use crate::target::Target;
use crate::workload::{Clock, Recorder, apply, row_key};
use crate::{Args, BenchKey, BenchValue, TestRunner};

#[derive(Clone, Default)]
pub struct Zipf;

impl TestRunner for Zipf {
    const NAME: &'static str = "zipf";

    fn run(&self, target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
        let g = &args.global_opts;
        let dist = ZipfDist::new(g.rows() as f64, g.zipf_exponent)?;
        let mut rng = StdRng::seed_from_u64(g.seed);
        let mut rec = Recorder::new(Self::NAME, args);
        let mut clock = Clock::new(args);
        let mut batch_id: u32 = 0;
        loop {
            let value = Sha256::digest(batch_id.to_ne_bytes()).to_vec();
            let ops: Vec<BatchOp<BenchKey, BenchValue>> = (0..g.batch_size)
                .map(|_| {
                    let row = dist.sample(&mut rng) as u64;
                    BatchOp::Put {
                        key: row_key(row),
                        value: value.clone(),
                    }
                })
                .collect();
            apply(target, args, &mut rec, ops)?;
            batch_id += 1;
            if u64::from(batch_id) == g.warmup_batches {
                rec.reset(target.stats());
            }
            if clock.tick() {
                break;
            }
        }
        Ok(rec.finish(target.stats()))
    }
}
