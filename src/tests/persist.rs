//! Clean-shutdown persistence and recovery through the AuthWal API.

use super::common::{del, k, put};
use crate::policy::EvictionPolicy;
use crate::policy::{Fifo, LruWrite};
use crate::set::{Entry, Snapshot};
use crate::storage::{MemoryConfig, MemoryStorage, StorageEngine, StorageProposal};
use crate::types::BatchOp;
use crate::types::{EMPTY_ROOT, Error, Hash, Result};
use crate::vc::VectorCommitment;
use crate::vc::{lthash::LtHash, merkle::Merkle, merkle_blake3::MerkleBlake3, rehash::Rehash};
use crate::{AuthWal, Config, Consensus, Local};
use std::fs;
use std::path::Path;

type MemWal<C = Rehash, P = LruWrite<Vec<u8>>> = AuthWal<Vec<u8>, Vec<u8>, C, P, MemoryStorage<Vec<u8>, Vec<u8>>>;

fn config(path: &Path, capacity: usize, speculate: bool) -> Config {
    Config {
        consensus: Consensus { capacity },
        local: Local {
            snapshot_path: Some(path.to_path_buf()),
            revisions: 4,
            speculate,
            speculation_rounds: 3,
            speculation_min_step: 1,
            ..Local::default()
        },
    }
}

fn memory() -> MemoryStorage<Vec<u8>, Vec<u8>> {
    MemoryStorage::new(MemoryConfig {
        revisions: 12,
        ..MemoryConfig::default()
    })
}

fn roundtrip<C: VectorCommitment<Vec<u8>>, P: EvictionPolicy<Vec<u8>>>(vc: C, policy: P) {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(&dir.path().join("snapshot"), 6, true);
    let make_set = || Snapshot::empty(vc.clone(), policy.clone(), 6).unwrap();
    let mut wal = MemWal::new(cfg.clone(), memory(), make_set()).unwrap();
    let mut plain_cfg = cfg.clone();
    plain_cfg.local.snapshot_path = None;
    plain_cfg.local.speculate = false;
    let mut control = MemWal::new(plain_cfg, memory(), make_set()).unwrap();
    let batches = [
        vec![put("a", "A"), put("b", "B"), put("c", "C")],
        vec![put("a", "new A"), put("d", "D"), del("b")],
        vec![put("e", "E"), put("f", "")],
    ];
    let mut first_root = EMPTY_ROOT;
    for batch in batches {
        wal.prefetch(None, &[k("false hint")]);
        let root = wal.update(batch.clone(), Some(b"block metadata")).unwrap();
        assert_eq!(root, control.update(batch, Some(b"block metadata")).unwrap());
        if first_root == EMPTY_ROOT {
            first_root = root;
        }
    }
    let saved_root = wal.reader().root();
    let discarded = wal.propose(None, vec![put("a", "uncommitted")], None).unwrap();
    wal.close().unwrap();
    let original_file = fs::read(cfg.local.snapshot_path.as_ref().unwrap()).unwrap();
    wal.close().unwrap();
    assert_eq!(
        fs::read(cfg.local.snapshot_path.as_ref().unwrap()).unwrap(),
        original_file
    );
    assert_eq!(discarded.get(&k("a")).unwrap(), Some(k("uncommitted")));
    drop(discarded);
    drop(wal);

    // Local settings can change across restarts; consensus state cannot.
    cfg.local.speculation_min_step = 256;
    cfg.local.revisions = 3;
    let mut recovered = MemWal::recover(cfg.clone(), memory(), vc.clone(), policy.clone()).unwrap();
    assert_eq!(recovered.reader().root(), saved_root);
    assert_eq!(recovered.reader().stats().height, 3);
    assert_eq!(recovered.reader().stats().puts, 0);
    assert!(recovered.reader().revision(first_root).is_err());
    assert!(
        recovered
            .reader()
            .latest_revision()
            .node()
            .snapshot()
            .predicted()
            .is_empty()
    );
    for name in ["a", "b", "c", "d", "e", "f", "missing"] {
        assert_eq!(
            recovered.reader().get(&k(name)).unwrap(),
            control.reader().get(&k(name)).unwrap()
        );
    }
    // These blocks overflow the set: restored ages must select exactly the
    // same victims as uninterrupted execution, for FIFO as well as LRU.
    for batch in [
        vec![put("g", "G"), put("a", "again")],
        vec![del("c"), put("h", "H"), put("i", "I")],
    ] {
        let hints: Vec<_> = batch.iter().filter_map(|op| op.key().cloned()).collect();
        recovered.prefetch(None, &hints);
        assert_eq!(
            recovered.update(batch.clone(), None).unwrap(),
            control.update(batch, None).unwrap()
        );
    }
    assert!(recovered.reader().stats().total_evictions > 0);
    recovered.close().unwrap();
    control.close().unwrap();
}

#[test]
fn checkpoint_recovery_preserves_future_roots_for_all_schemes_and_policies() {
    roundtrip(Rehash, Fifo::new(3).with_trigger(4));
    roundtrip(Rehash, LruWrite::new(3).with_trigger(4));
    roundtrip(Merkle::new(), Fifo::new(3).with_trigger(4));
    roundtrip(Merkle::new(), LruWrite::new(3).with_trigger(4));
    roundtrip(LtHash::new(), Fifo::new(3).with_trigger(4));
    roundtrip(LtHash::new(), LruWrite::new(3).with_trigger(4));
    roundtrip(MerkleBlake3::new(2), Fifo::new(3).with_trigger(4));
    roundtrip(MerkleBlake3::new(3), LruWrite::new(3).with_trigger(4));
}

#[test]
fn checkpoint_genesis_and_fixed_size_bytes_recover() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&dir.path().join("snapshot"), 3, false);
    type Wal = AuthWal<[u8; 2], [u8; 3], Rehash, Fifo<[u8; 2]>, MemoryStorage<[u8; 2], [u8; 3]>>;
    let mut wal = Wal::new(
        cfg.clone(),
        MemoryStorage::new(MemoryConfig::default()),
        Snapshot::empty(Rehash, Fifo::new(2), 3).unwrap(),
    )
    .unwrap();
    wal.close().unwrap();
    drop(wal);
    let mut wal = Wal::recover(
        cfg.clone(),
        MemoryStorage::new(MemoryConfig::default()),
        Rehash,
        Fifo::new(2),
    )
    .unwrap();
    assert_eq!(wal.reader().root(), EMPTY_ROOT);
    let root = wal
        .update(
            vec![BatchOp::Put {
                key: [1, 2],
                value: [3, 4, 5],
            }],
            Some(&[]),
        )
        .unwrap();
    wal.close().unwrap();
    drop(wal);
    let mut wal = Wal::recover(
        cfg.clone(),
        MemoryStorage::new(MemoryConfig::default()),
        Rehash,
        Fifo::new(2),
    )
    .unwrap();
    assert_eq!(wal.reader().root(), root);
    assert_eq!(wal.reader().get(&[1, 2]).unwrap(), Some([3, 4, 5]));
    wal.close().unwrap();
    type WrongKey = AuthWal<[u8; 3], [u8; 3], Rehash, Fifo<[u8; 3]>, MemoryStorage<[u8; 3], [u8; 3]>>;
    assert!(matches!(
        WrongKey::recover(
            cfg.clone(),
            MemoryStorage::new(MemoryConfig::default()),
            Rehash,
            Fifo::new(2)
        ),
        Err(Error::InvalidSnapshot(_))
    ));
    type WrongValue = AuthWal<[u8; 2], [u8; 4], Rehash, Fifo<[u8; 2]>, MemoryStorage<[u8; 2], [u8; 4]>>;
    assert!(matches!(
        WrongValue::recover(cfg, MemoryStorage::new(MemoryConfig::default()), Rehash, Fifo::new(2)),
        Err(Error::InvalidSnapshot(_))
    ));
}

fn saved_file(path: &Path) -> Config {
    let cfg = config(path, 3, false);
    let set = Snapshot::from_entries(
        Rehash,
        LruWrite::new(2),
        3,
        [(k("a"), Entry::Value(k("A"))), (k("b"), Entry::Tombstone)],
    )
    .unwrap();
    let mut wal = MemWal::new(cfg.clone(), memory(), set).unwrap();
    wal.close().unwrap();
    cfg
}

#[test]
fn checkpoint_rejects_missing_files_and_mismatched_configuration_or_storage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let cfg = config(&path, 3, false);
    assert!(matches!(
        MemWal::recover(cfg, memory(), Rehash, LruWrite::new(2)),
        Err(Error::SnapshotIo(_))
    ));
    let cfg = saved_file(&path);
    assert!(
        MemWal::new(
            cfg.clone(),
            memory(),
            Snapshot::empty(Rehash, LruWrite::new(2), 3).unwrap()
        )
        .is_err()
    );
    let mut changed = cfg.clone();
    changed.consensus.capacity = 4;
    assert!(matches!(
        MemWal::recover(changed, memory(), Rehash, LruWrite::new(2)),
        Err(Error::InvalidSnapshot(_))
    ));
    assert!(matches!(
        MemWal::recover(cfg.clone(), memory(), Rehash, LruWrite::new(1)),
        Err(Error::InvalidSnapshot(_))
    ));
    assert!(matches!(
        MemWal::<Rehash, Fifo<Vec<u8>>>::recover(cfg.clone(), memory(), Rehash, Fifo::new(2)),
        Err(Error::InvalidSnapshot(_))
    ));
    assert!(matches!(
        MemWal::<Merkle>::recover(cfg.clone(), memory(), Merkle::new(), LruWrite::new(2)),
        Err(Error::InvalidSnapshot(_))
    ));
    let engine = memory();
    engine
        .head()
        .unwrap()
        .propose(&[put("stored", "different")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert!(matches!(
        MemWal::recover(cfg.clone(), engine, Rehash, LruWrite::new(2)),
        Err(Error::InvalidSnapshot(_))
    ));
    // The trigger is local and can be changed freely.
    let mut wal = MemWal::recover(cfg, memory(), Rehash, LruWrite::new(2).with_trigger(2)).unwrap();
    wal.close().unwrap();
}

fn rechecksum(bytes: &mut [u8]) {
    let len = bytes.len() - 32;
    let checksum = *blake3::hash(&bytes[..len]).as_bytes();
    bytes[len..].copy_from_slice(&checksum);
}

#[test]
fn checkpoint_rejects_corruption_and_malformed_checked_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let cfg = saved_file(&path);
    let bytes = fs::read(&path).unwrap();
    let mut bad_files = vec![Vec::new(), bytes[..bytes.len() - 1].to_vec()];
    let mut bad = bytes.clone();
    bad[0] ^= 1;
    bad_files.push(bad);
    let mut bad = bytes.clone();
    bad[8..12].copy_from_slice(&2u32.to_le_bytes());
    rechecksum(&mut bad);
    bad_files.push(bad);
    let mut bad = bytes.clone();
    bad[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    rechecksum(&mut bad);
    bad_files.push(bad);
    // Locate the stored combined root after IDs, parameters, capacity, height.
    let mut root_offset = 12; // Magic and format version.
    for _ in 0..3 {
        let len = u32::from_le_bytes(bytes[root_offset..root_offset + 4].try_into().unwrap()) as usize;
        root_offset += 4 + len;
    }
    root_offset += 64 + 16; // Parameter digests, capacity, and height.
    let mut bad = bytes.clone();
    bad[root_offset] ^= 1;
    rechecksum(&mut bad);
    bad_files.push(bad);
    // Last policy height would be in the future, despite a valid checksum.
    let mut bad = bytes.clone();
    let len = bad.len();
    bad[len - 40..len - 32].copy_from_slice(&u64::MAX.to_le_bytes());
    rechecksum(&mut bad);
    bad_files.push(bad);
    for bad in bad_files {
        fs::write(&path, bad).unwrap();
        assert!(matches!(
            MemWal::recover(cfg.clone(), memory(), Rehash, LruWrite::new(2)),
            Err(Error::InvalidSnapshot(_))
        ));
    }
}

#[test]
fn checkpoint_failed_engine_close_cannot_publish_on_retry() {
    // Model an engine that consumes its database on a failed flush, so the
    // next close returns Ok without doing any more persistence work.
    struct FailingClose {
        inner: MemoryStorage<Vec<u8>, Vec<u8>>,
        attempted: bool,
    }
    impl StorageEngine<Vec<u8>, Vec<u8>> for FailingClose {
        type Proposal = <MemoryStorage<Vec<u8>, Vec<u8>> as StorageEngine<Vec<u8>, Vec<u8>>>::Proposal;
        const ENGINE_ID: &'static str = "failing-close";
        const ROOT_IS_STATE_FUNCTION: bool = true;
        const COMMITS_CHAIN_LINKS_SEPARATELY: bool = true;
        fn revisions(&self) -> usize {
            self.inner.revisions()
        }
        fn root(&self) -> Hash {
            self.inner.root()
        }
        fn head(&self) -> Result<Self::Proposal> {
            self.inner.head()
        }
        fn close(&mut self) -> Result<()> {
            self.inner.close()?;
            if self.attempted {
                return Ok(());
            }
            self.attempted = true;
            Err(Error::Storage("flush failed after consuming database".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let cfg = config(&path, 3, false);
    let mut wal = AuthWal::new(
        cfg,
        FailingClose {
            inner: memory(),
            attempted: false,
        },
        Snapshot::empty(Rehash, LruWrite::new(2), 3).unwrap(),
    )
    .unwrap();
    fs::write(&path, b"previous checkpoint").unwrap();
    assert!(matches!(wal.close(), Err(Error::Storage(_))));
    assert!(matches!(wal.close(), Err(Error::Poisoned(_))));
    assert_eq!(fs::read(&path).unwrap(), b"previous checkpoint");
}

#[test]
fn checkpoint_close_retries_io_and_active_handle_failures() {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("parent");
    let path = parent.join("snapshot");
    let cfg = config(&path, 3, false);
    let mut wal = MemWal::new(cfg, memory(), Snapshot::empty(Rehash, LruWrite::new(1), 3).unwrap()).unwrap();
    wal.update(vec![put("a", "A"), put("b", "B"), put("c", "C")], None)
        .unwrap();
    let outstanding = wal.propose(None, vec![put("d", "D")], None).unwrap();
    fs::write(&parent, b"not a directory").unwrap();
    assert!(matches!(wal.close(), Err(Error::SnapshotIo(_))));
    fs::remove_file(&parent).unwrap();
    assert!(matches!(wal.close(), Err(Error::ActiveHandles)));
    assert!(!path.exists());
    drop(outstanding);
    wal.close().unwrap();
    assert!(path.is_file());
    assert!(matches!(wal.update(vec![], None), Err(Error::Closed)));
}

#[cfg(feature = "firewood")]
fn firewood_roundtrip<P: EvictionPolicy<Vec<u8>>>(policy: P) {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(&dir.path().join("snapshot"), 3, true);
    let mut engine_config = FirewoodConfig::new(dir.path().join("firewood"));
    engine_config.revisions = 12;
    let open_engine = || FirewoodStorage::new(engine_config.clone()).unwrap();
    let mut wal = AuthWal::<Vec<u8>, Vec<u8>, _, _, _>::new(
        cfg.clone(),
        open_engine(),
        Snapshot::empty(Rehash, policy.clone(), 3).unwrap(),
    )
    .unwrap();
    for batch in [
        vec![put("a", "A"), put("b", "B"), put("c", "C"), put("d", "D")],
        vec![del("a"), put("b", "new B")],
        vec![put("d", "new D")],
    ] {
        let keys: Vec<_> = batch.iter().filter_map(|op| op.key().cloned()).collect();
        wal.prefetch(None, &keys);
        wal.update(batch, Some(b"metadata survives restart")).unwrap();
    }
    assert!(wal.reader().stats().total_evictions > 0);
    let root = wal.reader().root();
    wal.close().unwrap();
    drop(wal); // Release pinned Firewood views before reopening its file.
    let mut wal =
        AuthWal::<Vec<u8>, Vec<u8>, _, _, _>::recover(cfg.clone(), open_engine(), Rehash, policy.clone()).unwrap();
    assert_eq!(wal.reader().root(), root);
    assert_eq!(wal.reader().get(&k("a")).unwrap(), None); // Tombstone masks stored A.
    assert_eq!(wal.reader().get(&k("b")).unwrap(), Some(k("new B")));
    assert_eq!(wal.reader().get(&k("c")).unwrap(), Some(k("C"))); // Evicted, read from disk.
    assert_eq!(wal.reader().get(&k("d")).unwrap(), Some(k("new D")));
    assert_eq!(wal.reader().stats().height, 3);
    wal.prefetch(None, &[k("e"), k("f")]);
    let next = wal.update(vec![put("e", "E"), put("f", "F")], None).unwrap();
    wal.close().unwrap();
    drop(wal);
    let mut wal = AuthWal::<Vec<u8>, Vec<u8>, _, _, _>::recover(cfg, open_engine(), Rehash, policy).unwrap();
    assert_eq!(wal.reader().root(), next);
    assert_eq!(wal.reader().stats().height, 4);
    assert_eq!(wal.reader().get(&k("a")).unwrap(), None);
    assert_eq!(wal.reader().get(&k("e")).unwrap(), Some(k("E")));
    assert_eq!(wal.reader().get(&k("f")).unwrap(), Some(k("F")));
    wal.close().unwrap();
}

#[cfg(feature = "firewood")]
#[test]
fn checkpoint_firewood_recovers_evicted_values_and_keeps_tombstones() {
    firewood_roundtrip(Fifo::new(1).with_trigger(2));
    firewood_roundtrip(LruWrite::new(1).with_trigger(2));
}
