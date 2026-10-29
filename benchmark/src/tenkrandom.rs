//! `tenkrandom` workload (design §11), firewood's: per batch, insert
//! `batch_size/4` new rows from `high`, delete `batch_size/4` rows from `low`,
//! update `batch_size/2` rows from the midpoint with `sha256(low)`; then
//! advance both by `batch_size/4`. Exercises Put/Delete mixes and tombstones.

use std::error::Error;

use crate::report::Measurements;
use crate::target::Target;
use crate::workload::{Clock, Recorder, apply, deletes, inserts, row_key, updates};
use crate::{Args, TestRunner};

#[derive(Clone, Default)]
pub struct TenKRandom;

impl TestRunner for TenKRandom {
    const NAME: &'static str = "tenkrandom";

    fn run(&self, target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
        let g = &args.global_opts;
        let quarter = g.batch_size / 4;
        let mut low = 0u64;
        let mut high = g.rows();
        let mut rec = Recorder::new(Self::NAME, args);
        let mut clock = Clock::new(args);
        loop {
            let ops = inserts(high, quarter)
                .chain(deletes(low, quarter))
                .chain(updates(low + high / 2, quarter * 2, row_key(low).to_vec()))
                .collect();
            apply(target, args, &mut rec, ops)?;
            low += quarter;
            high += quarter;
            if clock.tick() {
                break;
            }
        }
        Ok(rec.finish(target.stats()))
    }
}
