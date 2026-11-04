//! The four workloads (design §11), firewood's: `create`, `tenkrandom`,
//! `zipf` and `single`, each a batch generator over [`run_loop`] (or
//! `create`'s fixed count of batches).

use std::error::Error;

use auth_wal::types::BatchOp;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Zipf};
use sha2::{Digest, Sha256};

use crate::report::Measurements;
use crate::target::Target;
use crate::workload::{Recorder, apply, deletes, inserts, row_key, run_loop, updates};
use crate::{Args, BenchKey, BenchValue};

/// Inserts `number_of_batches` batches of `batch_size` rows from an empty
/// database, keys and values `sha256(row)`. Every batch overflows a write
/// set, so it measures pure eviction throughput; on a bare engine it is
/// firewood's own `create`.
pub fn create(target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
    let g = &args.global_opts;
    let mut rec = Recorder::new(args);
    for batch in 0..g.number_of_batches {
        apply(
            target,
            args,
            &mut rec,
            inserts(batch * g.batch_size, g.batch_size).collect(),
        )?;
    }
    Ok(rec.finish(target.stats()))
}

/// Per batch, insert `batch_size/4` new rows from `high`, delete
/// `batch_size/4` rows from `low`, update `batch_size/2` rows from the
/// midpoint with `sha256(low)`; then advance both by `batch_size/4`.
/// Exercises Put/Delete mixes and tombstones.
pub fn tenkrandom(target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
    let g = &args.global_opts;
    let quarter = g.batch_size / 4;
    let mut low = 0u64;
    let mut high = g.rows();
    run_loop(target, args, |_| {
        let ops = inserts(high, quarter)
            .chain(deletes(low, quarter))
            .chain(updates(low + high / 2, quarter * 2, row_key(low).to_vec()))
            .collect();
        low += quarter;
        high += quarter;
        ops
    })
}

/// `batch_size` updates per batch with rows drawn from a Zipf distribution
/// over `rows()` (exponent `--zipf-exponent`), value `sha256(batch_id)`.
/// Seeded. The target workload for the write set: hot keys should stay
/// resident, and the report's in-place update ratio is its hit rate.
pub fn zipf(target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
    let g = &args.global_opts;
    let dist = Zipf::new(g.rows() as f64, g.zipf_exponent)?;
    let mut rng = StdRng::seed_from_u64(g.seed);
    run_loop(target, args, |batch| {
        let value: BenchValue = Sha256::digest((batch as u32).to_ne_bytes()).to_vec();
        (0..g.batch_size)
            .map(|_| BatchOp::Put {
                key: row_key(dist.sample(&mut rng) as u64),
                value: value.clone(),
            })
            .collect()
    })
}

/// The same `batch_size` keys (`sha256(0..batch_size)`) rewritten every
/// batch with the value `[batch_id as u8]`. Pure in-set update path for the
/// write set; storage should see zero writes once the keys are resident.
pub fn single(target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
    let keys: Vec<BenchKey> = (0..args.global_opts.batch_size).map(row_key).collect();
    run_loop(target, args, |batch| {
        keys.iter()
            .map(|key| BatchOp::Put {
                key: *key,
                value: vec![batch as u8],
            })
            .collect()
    })
}
