//! Benchmark driver modelled on `firewood/benchmark` (design §11).
//!
//! Same four workloads and key derivation (`sha256(row_id)`), plus auth_wal
//! knobs. Prints a CSV header and one record per run so sweeps can be plotted
//! directly.

// Skeleton phase: bodies are `todo!()`, so parameters are unused for now.
#![allow(dead_code, unused_variables)]

use std::error::Error;
use std::time::Duration;

use auth_wal::AuthWal;
use auth_wal::policy::EvictionPolicy;
use auth_wal::storage::MemoryStorage;
use auth_wal::vc::VectorCommitment;
use clap::{Parser, Subcommand, ValueEnum};

mod create;
mod report;
mod single;
mod tenkrandom;
mod zipf;

/// Key and value types used by every workload: 32-byte SHA-256 digests, as in
/// firewood's benchmark.
pub type BenchKey = [u8; 32];
pub type BenchValue = Vec<u8>;

/// The instantiation every workload runs against; the engine is fixed, the
/// scheme and policy are chosen from the CLI.
pub type BenchDb<C, P> = AuthWal<BenchKey, BenchValue, C, P, MemoryStorage<BenchKey, BenchValue>>;

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
    #[arg(short, long, default_value_t = 1_000)]
    pub number_of_batches: u64,
    #[arg(short, long, default_value_t = 128)]
    pub revisions: usize,
    #[arg(short = 't', long, default_value_t = 65, help = "Terminate after this many minutes")]
    pub duration_minutes: u64,

    // ---- auth_wal knobs (design §11) ----
    #[arg(
        long,
        default_value_t = 1_000_000,
        help = "Write-set capacity in entries (100 firewood batches)"
    )]
    pub capacity: usize,
    #[arg(long, help = "Eviction low watermark; default capacity - capacity/8 (design §6.2)")]
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
        help = "Maximum speculative links per parent (design §9.2)"
    )]
    pub speculation_rounds: u32,
    #[arg(
        long,
        default_value_t = 1_000,
        help = "Minimum new predicted entries before a link is sent (design §9.2)"
    )]
    pub speculation_min_link: usize,
    #[arg(
        long,
        default_value_t = 16,
        help = "Instance-wide budget of live speculative links (design §9.2)"
    )]
    pub speculation_max_links: usize,
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
        help = "Synthetic per-op latency of the in-memory storage engine"
    )]
    pub storage_latency_us: u64,
    #[arg(long, default_value_t = 1.2, help = "Zipf exponent (firewood default)")]
    pub zipf_exponent: f64,
    #[arg(long, default_value_t = 0, help = "RNG seed for zipf and tenkrandom")]
    pub seed: u64,
    #[arg(long, default_value_t = 0, help = "Batches to run before measuring (zipf)")]
    pub warmup_batches: u64,
    #[arg(
        long,
        default_value_t = false,
        help = "Run the workload straight into the storage engine, no write set"
    )]
    pub baseline: bool,
}

impl GlobalOpts {
    pub fn storage_latency(&self) -> Duration {
        Duration::from_micros(self.storage_latency_us)
    }
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

#[derive(Debug, Subcommand)]
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

/// A workload, generic over the scheme and policy chosen from the CLI. A
/// runner returns measurements only; `run_test` fills the configuration
/// columns of the report. With `--hints`, a runner splits each batch into
/// `hint_chunks` chunks and calls `Prefetch` with each chunk's keys before
/// `Propose`, timing the hint path separately.
pub trait TestRunner {
    const NAME: &'static str;

    fn run<C, P>(&self, db: &BenchDb<C, P>, args: &Args) -> Result<report::Measurements, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey, BenchValue>,
        P: EvictionPolicy<BenchKey>;

    /// `--baseline`: the same workload against the bare engine.
    fn run_baseline(
        &self,
        db: &MemoryStorage<BenchKey, BenchValue>,
        args: &Args,
    ) -> Result<report::Measurements, Box<dyn Error>>;
}

/// `sha256(row_id)` in native endian, as firewood does.
pub fn row_key(row: u64) -> BenchKey {
    todo!("design §11 key derivation")
}

/// Static dispatch over the CLI's (vc, policy) choice: builds the engine and
/// `BenchDb<C, P>` for each of the 2x2 combinations and runs `runner` (or
/// the baseline against the bare engine), then fills the configuration
/// columns. `TestRunner::run` is generic, so the trait is not
/// dyn-compatible; `main` matches on `TestName` instead, as firewood's does.
fn run_test<R: TestRunner>(runner: &R, args: &Args) -> Result<report::Report, Box<dyn Error>> {
    todo!(
        "match (args.global_opts.vc, args.global_opts.policy); --baseline -> run_baseline; Report from measurements + config"
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let report = match args.test_name {
        TestName::Create => run_test(&create::Create, &args)?,
        TestName::TenKRandom => run_test(&tenkrandom::TenKRandom, &args)?,
        TestName::Zipf => run_test(&zipf::Zipf, &args)?,
        TestName::Single => run_test(&single::Single, &args)?,
    };
    println!("{}\n{}", report::Report::CSV_HEADER, report.to_csv());
    Ok(())
}
