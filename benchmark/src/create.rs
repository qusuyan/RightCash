//! `create` workload (design §11): inserts `number_of_batches` batches of
//! `batch_size` rows from an empty database, keys and values `sha256(row)`.
//! Every batch overflows a write set, so it measures pure eviction throughput;
//! on a bare engine it is firewood's own `create`.

use std::error::Error;

use crate::report::Measurements;
use crate::target::Target;
use crate::workload::{Recorder, apply, inserts};
use crate::{Args, TestRunner};

#[derive(Clone, Default)]
pub struct Create;

impl TestRunner for Create {
    const NAME: &'static str = "create";

    fn run(&self, target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
        let g = &args.global_opts;
        let mut rec = Recorder::new(Self::NAME, args);
        for batch in 0..g.number_of_batches {
            let ops = inserts(batch * g.batch_size, g.batch_size).collect();
            apply(target, args, &mut rec, ops)?;
        }
        Ok(rec.finish(target.stats()))
    }
}
