//! Benchmark driver modelled on `firewood/benchmark` (design §11).
//!
//! Same four workloads and key derivation (`sha256(row_id)`), the same
//! inherited flags, plus auth_wal knobs. Every run prints a CSV header and one
//! record to stdout (progress goes to stderr), so sweeps over capacity,
//! policy, speculation and hints can be plotted directly.
//!
//! Targets: `--baseline` runs the workload straight into a storage engine
//! (`--engine memory` or `--engine firewood`, the vanilla comparison point);
//! without it the workload runs through auth_wal in front of that engine
//! (`WriteSet`), with the policy, scheme and knobs given.

use std::error::Error;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use auth_wal::policy::{EvictionPolicy, Fifo, LruWrite};
use auth_wal::storage::{MemoryConfig, MemoryStorage, StorageEngine};
use auth_wal::vc::VectorCommitment;
use auth_wal::vc::lthash::LtHash;
use auth_wal::vc::merkle::Merkle;
use auth_wal::vc::rehash::Rehash;
use auth_wal::{AuthWal, Config, Consensus, Local};
use clap::{Parser, Subcommand, ValueEnum};

mod report;
mod target;
mod workload;
mod workloads;

use report::{Measurements, Report};
use target::{Baseline, Target, WriteSet};

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
        help = "Batches for `create`; `-n × -b` is the row count of tenkrandom/zipf, `--prefill` and `--hint-noise`"
    )]
    pub number_of_batches: u64,
    #[arg(short, long, default_value_t = 128, help = "Revision window K")]
    pub revisions: usize,
    #[arg(short = 't', long, default_value_t = 65, help = "Terminate after this many minutes")]
    pub duration_minutes: u64,
    #[arg(short, long, default_value_t = NonZeroUsize::new(192_000_000).expect("non-zero"), help = "Firewood node cache bytes")]
    pub cache_memory_limit: NonZeroUsize,
    #[arg(
        long,
        help = "Firewood revisions allowed to await persistence; defaults to speculation-rounds for speculative auth_wal runs, otherwise 1. Must be less than the engine revision window"
    )]
    pub firewood_commit_permits: Option<NonZeroU64>,
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
    #[arg(
        long,
        help = "Eviction low watermark; default 70% of capacity, rounded down (design §6.2)"
    )]
    pub low_watermark: Option<usize>,
    #[arg(
        long,
        help = "Local speculation trigger: start precomputing capacity minus low-watermark victims when projected occupancy reaches this; default 90% of capacity, rounded down. Actual eviction only occurs above capacity"
    )]
    pub high_watermark: Option<usize>,
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
        help = "Maximum storage proposals per speculative chain, including final refinement (design §9.2)"
    )]
    pub speculation_rounds: u32,
    #[arg(
        long,
        default_value_t = 256,
        help = "Minimum distinct pending keys before the worker builds a step ahead of the flush (design §9.2)"
    )]
    pub speculation_min_step: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "Extra predicted victims once the high watermark is reached (pessimistic prediction, design §9.2)"
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
        help = "Hint chunks per batch (one per emulated transaction); with --hints the batch is proposed as the union of the chunks, the last write of a key winning (design §11)"
    )]
    pub hint_chunks: u32,
    #[arg(
        long,
        default_value_t = 0,
        help = "Minimum gap between two hints, microseconds: the emulated execution time of a transaction (0 = no gap)"
    )]
    pub hint_gap_min_us: u64,
    #[arg(
        long,
        default_value_t = 0,
        help = "Maximum gap between two hints, microseconds: the gap is the minimum plus a geometric number of 100 µs steps with mean (max - min) / 2, truncated here"
    )]
    pub hint_gap_max_us: u64,
    #[arg(
        long,
        default_value_t = 0.0,
        help = "Extra keys per hint chunk as a fraction of its size, drawn uniformly from the workload's rows and not written by the block: emulated reads that make the hints over-predict (R35)"
    )]
    pub hint_noise: f64,
    #[arg(
        long,
        default_value_t = 0,
        help = "Synthetic per-op latency of the in-memory storage engine, microseconds"
    )]
    pub storage_latency_us: u64,
    #[arg(long, default_value_t = 1.2, help = "Zipf exponent (firewood default)")]
    pub zipf_exponent: f64,
    #[arg(
        long,
        default_value_t = rand::random::<u64>(),
        hide_default_value = true,
        help = "RNG seed for Zipf draws, hint gaps and hint noise; random by default, recorded in the report"
    )]
    pub seed: u64,
    #[arg(
        long,
        default_value_t = 0,
        help = "Batches to run before measuring (tenkrandom/zipf/single); auth_wal occupancy must then be at least the low watermark"
    )]
    pub warmup_batches: u64,
    #[arg(
        long,
        default_value_t = false,
        help = "Insert all `rows()` keys before the workload, outside the measurement (firewood's `create`, in-process), so tenkrandom/zipf/single update a populated trie; with the write set the rows pass through it"
    )]
    pub prefill: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "Print where propose and commit spent their time, phase by phase over the measured blocks, before the CSV record (write-set targets; design §9.5)"
    )]
    pub breakdown: bool,
}

impl GlobalOpts {
    /// Rows in the database for tenkrandom/zipf, as in firewood.
    pub fn rows(&self) -> u64 {
        self.number_of_batches * self.batch_size
    }

    /// Explicit low watermark, otherwise 70% of capacity rounded down.
    /// Consensus parameters remain explicit in the library; these are the
    /// benchmark's configuration defaults.
    pub fn watermark(&self) -> usize {
        self.low_watermark
            .unwrap_or(((self.capacity as u128) * 7 / 10) as usize)
    }

    /// Explicit precomputation threshold, otherwise 90% rounded down.
    pub fn trigger(&self) -> usize {
        self.high_watermark
            .unwrap_or(((self.capacity as u128) * 9 / 10) as usize)
    }

    /// Resolve the explicit permit limit or reserve room for one speculative
    /// chain. A zero round count remains invalid and is rejected before opening.
    pub fn commit_permits(&self) -> u64 {
        self.firewood_commit_permits.map(NonZeroU64::get).unwrap_or_else(|| {
            if !self.baseline && self.speculate {
                u64::from(self.speculation_rounds)
            } else {
                1
            }
        })
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum EngineArg {
    /// In-memory model of firewood (design §10).
    Memory,
    /// Vanilla firewood (design §10, R13).
    Firewood,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PolicyArg {
    /// Evict in order of first insertion (design §6.1).
    Fifo,
    /// Evict the key whose last write is oldest (design §6.1).
    LruWrite,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum VcArg {
    /// Lattice hash, incremental (design §7.2).
    Lthash,
    /// Sparse Merkle tree, the reference scheme (design §7.2).
    Merkle,
    /// Whole-set rehash (design §7.2).
    Rehash,
}

#[derive(Debug, Subcommand, Clone, Copy, PartialEq, Eq)]
pub enum TestName {
    /// Insert `-n` batches of `-b` rows: pure eviction throughput.
    Create,
    /// A quarter deletes, a quarter inserts, half updates per batch over a moving window.
    #[command(name = "tenkrandom", alias = "ten-k-random")]
    TenKRandom,
    /// Zipf-distributed updates: hot keys should stay resident.
    Zipf,
    /// Tiny batch on the same keys every commit: pure in-set update path.
    Single,
}

impl TestName {
    pub fn name(self) -> &'static str {
        match self {
            TestName::Create => "create",
            TestName::TenKRandom => "tenkrandom",
            TestName::Zipf => "zipf",
            TestName::Single => "single",
        }
    }

    pub fn run(self, target: &mut dyn Target, args: &Args) -> Result<Measurements, Box<dyn Error>> {
        match self {
            TestName::Create => workloads::create(target, args),
            TestName::TenKRandom => workloads::tenkrandom(target, args),
            TestName::Zipf => workloads::zipf(target, args),
            TestName::Single => workloads::single(target, args),
        }
    }
}

/// The auth_wal configuration from the flags (design §11): the capacity is
/// consensus, the rest local; the engine window is `engine_window` (design
/// §8).
fn wal_config(g: &GlobalOpts) -> Config {
    Config {
        consensus: Consensus { capacity: g.capacity },
        local: Local {
            snapshot_path: None,
            revisions: g.revisions,
            speculate: g.speculate,
            speculation_rounds: g.speculation_rounds,
            speculation_min_step: g.speculation_min_step,
            speculation_margin: g.speculation_margin,
        },
    }
}

/// The write set over `engine` with the flags' policy and scheme: one of the
/// six monomorphizations (design §3).
fn write_set<S>(config: Config, g: &GlobalOpts, engine: S) -> Result<Box<dyn Target>, Box<dyn Error>>
where
    S: StorageEngine<BenchKey, BenchValue>,
{
    fn open<C, P, S>(config: Config, engine: S, policy: P, vc: C) -> Result<Box<dyn Target>, Box<dyn Error>>
    where
        C: VectorCommitment<BenchKey>,
        P: EvictionPolicy<BenchKey>,
        S: StorageEngine<BenchKey, BenchValue>,
    {
        let set = auth_wal::set::Snapshot::empty(vc, policy, config.consensus.capacity)?;
        Ok(Box::new(WriteSet::new(AuthWal::new(config, engine, set)?)))
    }
    // `--high-watermark` only schedules precomputation (design §6.2).
    let low = g.watermark();
    let high = g.trigger();
    let fifo = || Fifo::new(low).with_trigger(high);
    let lru = || LruWrite::new(low).with_trigger(high);
    match (g.policy, g.vc) {
        (PolicyArg::Fifo, VcArg::Lthash) => open(config, engine, fifo(), LtHash::new()),
        (PolicyArg::Fifo, VcArg::Merkle) => open(config, engine, fifo(), Merkle::new()),
        (PolicyArg::Fifo, VcArg::Rehash) => open(config, engine, fifo(), Rehash::new()),
        (PolicyArg::LruWrite, VcArg::Lthash) => open(config, engine, lru(), LtHash::new()),
        (PolicyArg::LruWrite, VcArg::Merkle) => open(config, engine, lru(), Merkle::new()),
        (PolicyArg::LruWrite, VcArg::Rehash) => open(config, engine, lru(), Rehash::new()),
    }
}

/// The engine window a run needs (design §8): `K` for a bare engine, else
/// what auth_wal's settings need on an engine of type `S`.
fn window<S: StorageEngine<BenchKey, BenchValue>>(config: &Config, baseline: bool) -> usize {
    if baseline {
        config.local.revisions
    } else {
        config.local.engine_window(S::COMMITS_CHAIN_LINKS_SEPARATELY)
    }
}

fn build_target(args: &Args) -> Result<Box<dyn Target>, Box<dyn Error>> {
    let g = &args.global_opts;
    let config = wal_config(g);
    match g.engine {
        EngineArg::Memory => {
            let engine = MemoryStorage::<BenchKey, BenchValue>::new(MemoryConfig {
                revisions: window::<MemoryStorage<BenchKey, BenchValue>>(&config, g.baseline),
                op_latency: Duration::from_micros(g.storage_latency_us),
            });
            if g.baseline {
                Ok(Box::new(Baseline::new(engine)))
            } else {
                write_set(config, g, engine)
            }
        }
        #[cfg(feature = "firewood")]
        EngineArg::Firewood => {
            use auth_wal::storage::{FirewoodConfig, FirewoodStorage};
            let mut cfg = FirewoodConfig::new(g.dbname.clone());
            // `create` starts from scratch; the other baseline workloads run
            // on the database `create` left behind, as in firewood's
            // benchmark, unless `--prefill` builds the rows in-process.
            // auth_wal requires a fresh engine (design §1, §12.6), so the
            // write-set target always starts from scratch and its workloads
            // build their rows themselves (`--prefill`, `--warmup-batches`).
            cfg.truncate = args.test_name == TestName::Create || !g.baseline || g.prefill;
            cfg.revisions = window::<FirewoodStorage>(&config, g.baseline);
            cfg.commit_permits = NonZeroU64::new(g.commit_permits())
                .ok_or("Firewood commit permits require a positive speculation-rounds value")?;
            cfg.node_cache_bytes = g.cache_memory_limit;
            cfg.free_list_cache_entries = NonZeroUsize::new(4 * g.batch_size as usize).unwrap_or(NonZeroUsize::MIN);
            let engine = FirewoodStorage::new(cfg)?;
            if g.baseline {
                Ok(Box::new(Baseline::new(engine)))
            } else {
                write_set(config, g, engine)
            }
        }
        #[cfg(not(feature = "firewood"))]
        EngineArg::Firewood => Err("built without the `firewood` feature".into()),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    eprintln!("[run] seed={}", args.global_opts.seed);
    if args.test_name == TestName::Single && args.global_opts.batch_size > 1000 {
        return Err("single is not designed for batch sizes > 1000 (as in firewood)".into());
    }
    let mut target = build_target(&args)?;
    let measurement = args.test_name.run(&mut *target, &args);
    // Close even when occupancy validation or the workload fails.
    let close_started = Instant::now();
    let closed = target.finish();
    eprintln!("[close] elapsed_s={:.6}", close_started.elapsed().as_secs_f64());
    let m = measurement?;
    closed?;
    let report = Report { args: &args, m };
    if args.global_opts.breakdown
        && let Some(breakdown) = &report.m.breakdown
    {
        println!("{}", breakdown.render(&report.m.worker));
    }
    println!("{}", report.csv());
    Ok(())
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    #[test]
    fn firewood_commit_permits_default_to_one_speculative_chain() {
        for (options, expected) in [
            (vec![], 1),
            (vec!["--speculate"], 3),
            (vec!["--speculate", "--speculation-rounds", "30"], 30),
            (vec!["--baseline", "--speculate", "--speculation-rounds", "30"], 1),
            (vec!["--speculate", "--firewood-commit-permits", "32"], 32),
        ] {
            let mut argv = vec!["benchmark"];
            argv.extend(options);
            argv.push("zipf");
            assert_eq!(
                Args::try_parse_from(argv).unwrap().global_opts.commit_permits(),
                expected
            );
        }
        assert!(Args::try_parse_from(["benchmark", "--firewood-commit-permits", "0", "zipf"]).is_err());
    }
}
