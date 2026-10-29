//! Micro-benchmarks for the hot paths behind every write (design §11): the
//! lattice accumulator (one item add/remove per op, the cost of `lthash` and
//! of `MemoryStorage`'s root), the per-proposal copy of the set (design §4,
//! R20), the synchronisation primitives of design §10 and the memory engine's
//! propose. The benches of the decided alternatives (persistent maps, the
//! in-place map with an undo record, the sorted array, the blake3-XOF and
//! AES expansions) were removed with R20; their figures are recorded in the
//! design (§4, §7.2, §12.13).

use std::time::Duration;

use auth_wal::lattice::{LANES, LatticeAccumulator};
use auth_wal::storage::{MemoryConfig, MemoryStorage, StorageEngine, StorageProposal};
use auth_wal::types::BatchOp;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use sha2::{Digest, Sha256};

type K = [u8; 32];
type V = Vec<u8>;

fn key(i: u64) -> K {
    Sha256::digest(i.to_ne_bytes()).into()
}

fn config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1))
}

fn lattice(c: &mut Criterion) {
    let items: Vec<Vec<u8>> = (0..10_000u64)
        .map(|i| LatticeAccumulator::<LANES>::encode_item(&[&key(i), &key(i)]))
        .collect();
    let mut g = c.benchmark_group("lattice");
    g.throughput(Throughput::Elements(items.len() as u64));
    g.bench_function("add 10k encoded items", |b| {
        b.iter_batched(
            || LatticeAccumulator::<LANES>::new("bench"),
            |mut acc| {
                for it in &items {
                    acc.add(it);
                }
                acc
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("encode + add 10k (key,value)", |b| {
        b.iter_batched(
            || LatticeAccumulator::<LANES>::new("bench"),
            |mut acc| {
                for i in 0..10_000u64 {
                    let k = key(i);
                    acc.add(&LatticeAccumulator::<LANES>::encode_item(&[&k, &k]));
                }
                acc
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("add_fields 10k (key,value), no alloc", |b| {
        b.iter_batched(
            || LatticeAccumulator::<LANES>::new("bench"),
            |mut acc| {
                for i in 0..10_000u64 {
                    let k = key(i);
                    acc.add_fields(&[&k, &k]);
                }
                acc
            },
            BatchSize::SmallInput,
        )
    });
    g.bench_function("digest", |b| {
        let mut acc = LatticeAccumulator::<LANES>::new("bench");
        acc.add(&items[0]);
        b.iter(|| acc.digest())
    });
    g.finish();
}

/// The per-item lane expansion the accumulator uses (a 32-byte blake3 hash
/// followed by a ChaCha8 keystream, design §7.2) and a chunked parallel
/// accumulation merged by lane-wise addition.
fn accumulate(c: &mut Criterion) {
    use chacha20::cipher::{KeyIvInit, StreamCipher};
    let items: Vec<Vec<u8>> = (0..10_000u64)
        .map(|i| LatticeAccumulator::<LANES>::encode_item(&[&key(i), &key(i)]))
        .collect();
    let mut g = c.benchmark_group("accumulate");
    g.throughput(Throughput::Elements(items.len() as u64));
    g.bench_function("blake3 32 B + chacha8 2 KiB (current construction)", |b| {
        b.iter(|| {
            let mut sum = 0u16;
            let mut buf = [0u8; 2048];
            for it in &items {
                let h = blake3::hash(it);
                let mut c = chacha20::ChaCha8::new(h.as_bytes().into(), &[0u8; 12].into());
                buf.fill(0);
                c.apply_keystream(&mut buf);
                sum = sum.wrapping_add(buf[0] as u16);
            }
            sum
        })
    });
    g.bench_function("rayon: accumulator over 10k items in 16 chunks, merged", |b| {
        use rayon::prelude::*;
        b.iter(|| {
            items
                .par_chunks(256)
                .map(|chunk| {
                    let mut acc = LatticeAccumulator::<LANES>::new("bench");
                    for it in chunk {
                        acc.add(it);
                    }
                    acc
                })
                .reduce(
                    || LatticeAccumulator::<LANES>::new("bench"),
                    |mut a, b| {
                        a.merge(&b);
                        a
                    },
                )
        })
    });
    g.finish();
}

fn cow_base(c: &mut Criterion) {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const SET: u64 = 10_000;
    const WRITES: u64 = 2_000;
    let base_keys: Vec<K> = (0..SET).map(key).collect();
    let base_vals: Vec<V> = (0..SET).map(|i| key(i).to_vec()).collect();
    // 1k updates of existing keys (every tenth key) and 1k inserts of new keys.
    let writes: Vec<(K, V)> = (0..WRITES / 2)
        .map(|i| (base_keys[(i * 10) as usize], key(SET + i).to_vec()))
        .chain((0..WRITES / 2).map(|i| (key(SET + WRITES + i), key(SET + i).to_vec())))
        .collect();
    let arc_map: BTreeMap<K, Arc<[u8]>> = base_keys
        .iter()
        .copied()
        .zip(base_vals.iter().map(|v| Arc::from(v.as_slice())))
        .collect();
    let arc_writes: Vec<(K, Arc<[u8]>)> = writes.iter().map(|(k, v)| (*k, Arc::from(v.as_slice()))).collect();

    let mut g = c.benchmark_group("cow_base");
    g.throughput(Throughput::Elements(WRITES));
    // R20 (design §4): a proposal is a full copy of its parent's set with the
    // batch applied directly, policy state included. Measured here for the
    // two v0 policies on the 10k set with 2k writes (1k updates, 1k inserts),
    // and with the bulk eviction of half the set that follows when the set
    // overflows (design §6.2).
    let order: std::collections::BTreeSet<(u64, K)> =
        base_keys.iter().enumerate().map(|(i, k)| (i as u64, *k)).collect();
    let index: BTreeMap<K, u64> = base_keys.iter().enumerate().map(|(i, k)| (*k, i as u64)).collect();
    let height = SET + 1;
    g.bench_function(
        "full copy per proposal: clone 10k entries + FIFO state, apply 2k writes",
        |b| {
            b.iter(|| {
                let mut m = arc_map.clone();
                let mut order = order.clone();
                let mut index = index.clone();
                for (k, v) in &arc_writes {
                    m.insert(*k, v.clone());
                    if !index.contains_key(k) {
                        index.insert(*k, height);
                        order.insert((height, *k));
                    }
                }
                (m, order, index)
            })
        },
    );
    g.bench_function(
        "full copy per proposal: clone 10k entries + LRU-write state, apply 2k writes",
        |b| {
            b.iter(|| {
                let mut m = arc_map.clone();
                let mut order = order.clone();
                let mut index = index.clone();
                for (k, v) in &arc_writes {
                    m.insert(*k, v.clone());
                    if let Some(old) = index.insert(*k, height) {
                        order.remove(&(old, *k));
                    }
                    order.insert((height, *k));
                }
                (m, order, index)
            })
        },
    );
    g.bench_function("full copy per proposal: the above (LRU-write) + evict 5k oldest", |b| {
        b.iter(|| {
            let mut m = arc_map.clone();
            let mut order = order.clone();
            let mut index = index.clone();
            for (k, v) in &arc_writes {
                m.insert(*k, v.clone());
                if let Some(old) = index.insert(*k, height) {
                    order.remove(&(old, *k));
                }
                order.insert((height, *k));
            }
            let victims: Vec<(u64, K)> = order.iter().take(5_000).copied().collect();
            let mut evicted = Vec::with_capacity(victims.len());
            for (h, k) in victims {
                order.remove(&(h, k));
                index.remove(&k);
                if let Some(v) = m.remove(&k) {
                    evicted.push((k, v));
                }
            }
            (m, order, index, evicted)
        })
    });
    g.finish();
}

/// The synchronisation primitives design §10 (R19) chooses between, each
/// uncontended on one thread, so the lock inventory has numbers: a
/// parking_lot mutex hold (the registry lock, the removed commit and chain
/// locks), a read-write lock read guard (the firewood handle lock), a
/// `OnceLock::get` (a link slot's result, a node's committed snapshot before
/// R19), an atomic load (a cancel flag) and an `Arc` clone (a `NodeRef`).
fn sync_primitives(c: &mut Criterion) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, OnceLock};

    let mutex = parking_lot::Mutex::new(0u64);
    let rwlock = parking_lot::RwLock::new(0u64);
    let once: OnceLock<Arc<u64>> = OnceLock::new();
    once.set(Arc::new(7)).unwrap();
    let flag = AtomicBool::new(false);
    let arc = Arc::new([0u8; 64]);

    let mut g = c.benchmark_group("sync_primitives");
    g.bench_function("parking_lot Mutex lock+unlock", |b| {
        b.iter(|| {
            let mut guard = mutex.lock();
            *guard += 1;
            *guard
        })
    });
    g.bench_function("parking_lot RwLock read guard", |b| b.iter(|| *rwlock.read()));
    g.bench_function("OnceLock::get (set)", |b| b.iter(|| once.get().map(|v| **v)));
    g.bench_function("AtomicBool load (Acquire)", |b| b.iter(|| flag.load(Ordering::Acquire)));
    g.bench_function("Arc clone + drop", |b| b.iter(|| Arc::clone(&arc)));
    g.finish();

    // The registry's K-ring is one deque in commit order (design §8): a lookup
    // by root scans it from the newest entry. Worst case, a miss over K = 128
    // entries, against the map lookup it replaced.
    let roots: Vec<Arc<[u8; 32]>> = (0..128u64)
        .map(|i| Arc::new(*blake3::hash(&i.to_le_bytes()).as_bytes()))
        .collect();
    let deque: std::collections::VecDeque<Arc<[u8; 32]>> = roots.iter().cloned().collect();
    let map: std::collections::HashMap<[u8; 32], Arc<[u8; 32]>> = roots.iter().map(|r| (**r, r.clone())).collect();
    let miss = *blake3::hash(b"absent").as_bytes();
    let mut g = c.benchmark_group("registry_ring");
    g.bench_function("deque scan, miss over 128 roots", |b| {
        b.iter(|| deque.iter().rev().find(|r| ***r == miss).cloned())
    });
    g.bench_function("hashmap get, miss over 128 roots", |b| {
        b.iter(|| map.get(&miss).cloned())
    });
    g.finish();
}

fn memory_engine(c: &mut Criterion) {
    let engine = MemoryStorage::<K, V>::new(MemoryConfig::default());
    let mut next = 0u64;
    // 500k rows in place first.
    for _ in 0..50 {
        let batch: Vec<BatchOp<K, V>> = (next..next + 10_000)
            .map(|i| BatchOp::Put {
                key: key(i),
                value: key(i).to_vec(),
            })
            .collect();
        next += 10_000;
        engine.head().unwrap().propose(&batch, None).unwrap().commit().unwrap();
    }
    let batch: Vec<BatchOp<K, V>> = (next..next + 10_000)
        .map(|i| BatchOp::Put {
            key: key(i),
            value: key(i).to_vec(),
        })
        .collect();
    let mut g = c.benchmark_group("memory_engine");
    g.throughput(Throughput::Elements(10_000));
    g.bench_function("propose 10k new keys on 500k", |b| {
        b.iter(|| engine.head().unwrap().propose(&batch, None).unwrap())
    });
    g.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = lattice, accumulate, cow_base, sync_primitives, memory_engine
}
criterion_main!(benches);
