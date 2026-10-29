//! Benchmark driver modelled on `firewood/benchmark` (design §11).
//!
//! Same four workloads and key derivation (`sha256(row_id)`), the same
//! inherited flags, plus auth_wal knobs. Every run prints a CSV header and one
//! record to stdout (progress goes to stderr), so sweeps over capacity,
//! policy, speculation and hints can be plotted directly.
//!
//! Targets: `--baseline` runs the workload straight into a storage engine
//! (`--engine memory` or `--engine firewood`, the vanilla comparison point);
//! without it the workload runs through auth_wal, which lands with
//! milestone 1.

use std::error::Error;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use auth_wal::storage::{MemoryConfig, MemoryStorage};
use clap::{Parser, Subcommand, ValueEnum};

mod create;
mod report;
mod single;
mod target;
mod tenkrandom;
mod workload;
mod zipf;

use target::{Baseline, Target};

/// Key and value types used by every workload: 32-byte SHA-256 digests, as in
/// firewood's benchmark.
pub type BenchKey = [u8; 32];
pub type BenchValue = Vec<u8>;

#[derive(Parser, Debug)]
pub struct Args {
    #[clap(flatten)]
    pub global_opts: GlobalOpts,
    #[clap(subcommand)]
    pub test_name: TestName,
}

#[derive(clap::Args, Debug)]
pub struct GlobalOpts {
    // ---- inherited from firewood/benchmark ----
    #[arg(short, long, default_value_t = 10_000)]
    pub batch_size: u64,
    #[arg(
        short,
        long,
        default_value_t = 1_000,
        help = "Batches for `create`; also N rows for tenkrandom/zipf"
    )]
    pub number_of_batches: u64,
    #[arg(short, long, default_value_t = 128, help = "Revision window K")]
    pub revisions: usize,
    #[arg(short = 't', long, default_value_t = 65, help = "Terminate after this many minutes")]
    pub duration_minutes: u64,
    #[arg(short, long, default_value_t = NonZeroUsize::new(192_000_000).expect("non-zero"), help = "Firewood node cache bytes")]
    pub cache_memory_limit: NonZeroUsize,
    #[arg(
        short,
        long,
        default_value = "auth_wal_benchmark_db",
        help = "Firewood database directory"
    )]
    pub dbname: PathBuf,

    // ---- run control ----
    #[arg(
        long,
        default_value_t = 0,
        help = "Batches to run for tenkrandom/zipf/single (0 = until the duration elapses)"
    )]
    pub iterations: u64,
    #[arg(
        long,
        default_value_t = 100,
        help = "Progress line to stderr every this many batches (0 = never)"
    )]
    pub progress_every: u64,
    #[arg(long, value_enum, default_value_t = EngineArg::Memory)]
    pub engine: EngineArg,
    #[arg(
        long,
        default_value_t = false,
        help = "Run the workload straight into the storage engine, no write set"
    )]
    pub baseline: bool,

    // ---- auth_wal knobs (design §11) ----
    #[arg(
        long,
        default_value_t = 10_000,
        help = "Write-set capacity in entries (R16; sweep 1k..1M)"
    )]
    pub capacity: usize,
    #[arg(long, help = "Eviction low watermark; default capacity / 2 (design §6.2)")]
    pub low_watermark: Option<usize>,
    #[arg(long, value_enum, default_value_t = PolicyArg::LruWrite)]
    pub policy: PolicyArg,
    #[arg(long, value_enum, default_value_t = VcArg::Lthash)]
    pub vc: VcArg,
    #[arg(
        long,
        default_value_t = false,
        help = "Speculative eviction: send predicted victims early (design §9)"
    )]
    pub speculate: bool,
    #[arg(
        long,
        default_value_t = 3,
        help = "Maximum speculative steps per precomputation (design §9.2)"
    )]
    pub speculation_rounds: u32,
    #[arg(
        long,
        default_value_t = 1_000,
        help = "Minimum pending ops before the worker builds a step ahead of the flush (design §9.2)"
    )]
    pub speculation_min_step: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "Extra predicted victims once over capacity (pessimistic prediction, design §9.2)"
    )]
    pub speculation_margin: usize,
    #[arg(
        long,
        default_value_t = false,
        help = "Emulate libevm prefetch hints: call Prefetch with the batch's keys in chunks before Propose (design §9.4)"
    )]
    pub hints: bool,
    #[arg(
        long,
        default_value_t = 16,
        help = "Hint chunks per batch (one per emulated transaction)"
    )]
    pub hint_chunks: u32,
    #[arg(
        long,
        default_value_t = 0,
        help = "Synthetic per-op latency of the in-memory storage engine, microseconds"
    )]
    pub storage_latency_us: u64,
    #[arg(long, default_value_t = 1.2, help = "Zipf exponent (firewood default)")]
    pub zipf_exponent: f64,
    #[arg(long, default_value_t = 0, help = "RNG seed for zipf")]
    pub seed: u64,
    #[arg(long, default_value_t = 0, help = "Batches to run before measuring (zipf)")]
    pub warmup_batches: u64,
}

impl GlobalOpts {
    pub fn storage_latency(&self) -> Duration {
        Duration::from_micros(self.storage_latency_us)
    }

    /// Rows in the database for tenkrandom/zipf, as in firewood.
    pub fn rows(&self) -> u64 {
        self.number_of_batches * self.batch_size
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EngineArg {
    /// In-memory model of firewood (design §10).
    Memory,
    /// Vanilla firewood (design §10, R13).
    Firewood,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PolicyArg {
    Fifo,
    LruWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VcArg {
    Lthash,
    Merkle,
}

#[derive(Debug, Subcommand, Clone, Copy, PartialEq, Eq)]
pub enum TestName {
    /// Insert N rows, `batch_size` per commit: pure eviction throughput.
    Create,
    /// 2.5k deletes, 2.5k inserts, 5k updates per batch over a moving window.
    TenKRandom,
    /// Zipf-distributed updates: hot keys should stay resident.
    Zipf,
    /// Tiny batch on the same keys every commit: pure in-set update path.
    Single,
}

/// A workload. A runner returns measurements only; `run_test` fills the
/// configuration columns of the report.
pub trait TestRunner {
    const NAME: &'static str;

    fn run(&self, target: &mut dyn Target, args: &Args) -> Result<report::Measurements, Box<dyn Error>>;
}

fn build_target(args: &Args) -> Result<Box<dyn Target>, Box<dyn Error>> {
    let g = &args.global_opts;
    if !g.baseline {
        return Err("the auth_wal target lands with milestone 1 (design §13); run with --baseline for now".into());
    }
    match g.engine {
        EngineArg::Memory => {
            let engine = MemoryStorage::<BenchKey, BenchValue>::new(MemoryConfig {
                revisions: g.revisions,
                op_latency: g.storage_latency(),
            });
            Ok(Box::new(Baseline::new(engine, "memory")))
        }
        #[cfg(feature = "firewood")]
        EngineArg::Firewood => {
            use auth_wal::storage::{FirewoodConfig, FirewoodStorage};
            let mut cfg = FirewoodConfig::new(g.dbname.clone());
            // `create` starts from scratch; the other workloads run on the
            // database `create` left behind, as in firewood's benchmark.
            cfg.truncate = args.test_name == TestName::Create;
            cfg.revisions = g.revisions;
            cfg.node_cache_bytes = g.cache_memory_limit;
            cfg.free_list_cache_entries =
                NonZeroUsize::new(4 * g.batch_size as usize).unwrap_or(NonZeroUsize::new(1).expect("non-zero"));
            let engine = FirewoodStorage::new(cfg)?;
            Ok(Box::new(Baseline::new(engine, "firewood")))
        }
        #[cfg(not(feature = "firewood"))]
        EngineArg::Firewood => Err("built without the `firewood` feature".into()),
    }
}

fn run_test<R: TestRunner>(runner: &R, args: &Args) -> Result<report::Report, Box<dyn Error>> {
    let mut target = build_target(args)?;
    let measurements = runner.run(&mut *target, args)?;
    target.finish()?;
    Ok(report::Report::new(R::NAME, target.name(), args, measurements))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    if args.test_name == TestName::Single && args.global_opts.batch_size > 1000 {
        return Err("single is not designed for batch sizes > 1000 (as in firewood)".into());
    }
    let report = match args.test_name {
        TestName::Create => run_test(&create::Create, &args)?,
        TestName::TenKRandom => run_test(&tenkrandom::TenKRandom, &args)?,
        TestName::Zipf => run_test(&zipf::Zipf, &args)?,
        TestName::Single => run_test(&single::Single, &args)?,
    };
    println!("{}\n{}", report::Report::CSV_HEADER, report.to_csv());
    Ok(())
}
