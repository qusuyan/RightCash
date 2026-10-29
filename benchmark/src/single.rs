//! `single` workload (design §11), firewood's: the same `batch_size` keys
//! (`sha256(0..batch_size)`) rewritten every batch with the value
//! `[batch_id as u8]`. Pure in-set update path for the write set; storage
//! should see zero writes once the keys are resident.

use std::error::Error;

use auth_wal::types::BatchOp;

use crate::report::Measurements;
use crate::target::Target;
use crate::workload::{Clock, Recorder, apply, row_key};
use crate::{Args, BenchKey, TestRunner};

#[derive(Clone, Default)]
pub struct Single;

impl TestRunner for Single {
    const NAME: &'static str = "single";

    fn run(&self, target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
        let g = &args.global_opts;
        let keys: Vec<BenchKey> = (0..g.batch_size).map(row_key).collect();
        let mut rec = Recorder::new(Self::NAME, args);
        let mut clock = Clock::new(args);
        let mut batch_id: u64 = 0;
        loop {
            let ops = keys
                .iter()
                .map(|k| BatchOp::Put {
                    key: *k,
                    value: vec![batch_id as u8],
                })
                .collect();
            apply(target, args, &mut rec, ops)?;
            batch_id += 1;
            if clock.tick() {
                break;
            }
        }
        Ok(rec.finish(target.stats()))
    }
}
