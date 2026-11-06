//! Initialization, reads, roots, lineage, revisions, and the plain write path.

use super::common::*;
use crate::policy::{EvictionPolicy, Fifo, LruWrite};
use crate::set::Entry;
use crate::storage::{MemoryConfig, MemoryStorage, StorageProposal};
use crate::types::{BatchOp, EMPTY_ROOT, Error, Hash};
use crate::vc::{VectorCommitment, lthash::LtHash, merkle::Merkle, rehash::Rehash};
use crate::{AuthWal, Config};
use std::collections::BTreeMap;

fn suite<C, P>(policy: impl Fn() -> P, vc: impl Fn() -> C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    fresh_instance(policy(), vc());
    reads_and_roots(policy(), vc());
    evictions_reach_storage(policy(), vc());
    revision_window(policy(), vc());
    lineage_rules(policy(), vc());
    metadata_and_equal_roots(policy(), vc());
    chained_equals_sequential(policy(), vc());
    close_and_errors(policy(), vc());
    differential(policy(), vc());
}

fn fresh_instance<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    assert_eq!(reader.root(), EMPTY_ROOT);
    assert_eq!(reader.get(&k("a")).unwrap(), None);
    // Genesis resolves while it is in the ring (design §3, §8).
    assert_eq!(reader.revision(EMPTY_ROOT).unwrap().root(), EMPTY_ROOT);
    assert_eq!(reader.latest_revision().root(), EMPTY_ROOT);
    let stats = reader.stats();
    assert_eq!((stats.height, stats.occupancy, stats.puts), (0, 0, 0));
}

fn reads_and_roots<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    let p1 = wal.propose(None, vec![put("a", "1"), put("b", "2")], None).unwrap();
    let r1 = p1.root();
    assert_ne!(r1, EMPTY_ROOT);
    // A live proposal reads its own set and resolves by root (design §5, §8).
    assert_eq!(p1.get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(reader.get(&k("a")).unwrap(), None);
    assert_eq!(reader.revision(r1).unwrap().get(&k("b")).unwrap(), Some(k("2")));
    assert_eq!(wal.commit(p1).unwrap(), r1);
    assert_eq!(reader.root(), r1);
    assert_eq!(get(&wal, "a"), Some(k("1")));
    // A rewrite and a delete: in-place update, tombstone reads as absent, a
    // put over the tombstone is a write to a present key.
    let r2 = wal.update(vec![put("a", "3"), del("b"), del("c")], None).unwrap();
    assert_ne!(r2, r1);
    assert_eq!(get(&wal, "a"), Some(k("3")));
    assert_eq!(get(&wal, "b"), None);
    assert_eq!(get(&wal, "c"), None);
    let stats = reader.stats();
    assert_eq!(stats.height, 2);
    assert_eq!(stats.occupancy, 3);
    assert_eq!(stats.puts, 3);
    assert_eq!(stats.in_place_updates, 1);
    assert_eq!(stats.total_evictions, 0);
    assert_eq!(stats.storage_proposals, 0);
    wal.update(vec![put("c", "4")], None).unwrap();
    assert_eq!(reader.stats().in_place_updates, 2);
    assert_eq!(get(&wal, "c"), Some(k("4")));
    // An empty block still gets a new root (height is in the preimage).
    let r3 = reader.root();
    let r4 = wal.update(vec![], None).unwrap();
    assert_ne!(r3, r4);
}

fn evictions_reach_storage<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    // Capacity 4, watermark 2: the fifth entry evicts two.
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    wal.update(vec![put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")], None)
        .unwrap();
    assert_eq!(reader.stats().occupancy, 4);
    let p = wal.propose(None, vec![put("e", "5"), del("f")], None).unwrap();
    // Six entries: the policy evicts down to two; the node owns a storage
    // proposal and reads through it for the evicted keys.
    let expected = [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")];
    for (key, value) in expected {
        assert_eq!(p.get(&k(key)).unwrap(), Some(k(value)));
    }
    assert_eq!(p.get(&k("f")).unwrap(), None);
    wal.commit(p).unwrap();
    let stats = reader.stats();
    assert_eq!(stats.occupancy, 2);
    assert_eq!(stats.total_evictions, 4);
    assert_eq!(stats.storage_proposals, 1);
    assert_eq!(stats.total_storage_ops, 4);
    for (key, value) in expected {
        assert_eq!(get(&wal, key), Some(k(value)));
    }
    assert_eq!(get(&wal, "f"), None);
    // The committed state stays readable through a retained revision after
    // further commits, exactly as it was.
    let r = reader.root();
    wal.update(vec![put("a", "9")], None).unwrap();
    assert_eq!(reader.revision(r).unwrap().get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(get(&wal, "a"), Some(k("9")));
}

fn revision_window<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    let mut roots = vec![EMPTY_ROOT];
    for i in 0..5 {
        roots.push(wal.update(vec![put("k", &i.to_string())], None).unwrap());
    }
    // K = 3: the last three committed roots resolve, the older ones do not.
    for root in &roots[..3] {
        assert!(matches!(reader.revision(*root), Err(Error::RevisionNotFound)));
    }
    for (i, root) in roots[3..].iter().enumerate() {
        let handle = reader.revision(*root).unwrap();
        assert_eq!(handle.get(&k("k")).unwrap(), Some(k(&(i + 2).to_string())));
    }
    // A live proposal resolves while its handle is held, not after.
    let p = wal.propose(None, vec![put("x", "1")], None).unwrap();
    let live = p.root();
    assert_eq!(reader.revision(live).unwrap().get(&k("x")).unwrap(), Some(k("1")));
    drop(p);
    assert!(matches!(reader.revision(live), Err(Error::RevisionNotFound)));
}

fn lineage_rules<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    // A deeper pipelined node: commit the parent first.
    let p1 = wal.propose(None, vec![put("a", "1")], None).unwrap();
    let p2 = wal.propose(Some(&p1), vec![put("b", "2")], None).unwrap();
    assert_eq!(p2.get(&k("a")).unwrap(), Some(k("1")));
    assert!(matches!(wal.commit(p2), Err(Error::ParentUncommitted)));
    let p2 = wal.propose(Some(&p1), vec![put("b", "2")], None).unwrap();
    wal.commit(p1).unwrap();
    wal.commit(p2).unwrap();
    assert_eq!(get(&wal, "b"), Some(k("2")));
    // An orphan: the parent dropped uncommitted.
    let p3 = wal.propose(None, vec![put("c", "3")], None).unwrap();
    let p4 = wal.propose(Some(&p3), vec![put("d", "4")], None).unwrap();
    drop(p3);
    assert!(matches!(wal.commit(p4), Err(Error::ProposalOrphaned)));
    // A twin of the head: that state is committed already.
    let p5 = wal.propose(None, vec![put("e", "5")], None).unwrap();
    let p5b = wal.propose(None, vec![put("e", "5")], None).unwrap();
    assert_eq!(p5.root(), p5b.root());
    wal.commit(p5).unwrap();
    assert!(matches!(wal.commit(p5b), Err(Error::ProposalConsumed)));
    // Proposing on a handle whose root is the head's builds on the head.
    let head = wal.reader().latest_revision();
    let p6 = wal.propose(Some(&head), vec![put("f", "6")], None).unwrap();
    wal.commit(p6).unwrap();
    assert_eq!(get(&wal, "f"), Some(k("6")));
}

fn metadata_and_equal_roots<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let batch = || vec![put("a", "1")];
    let none = wal.propose(None, batch(), None).unwrap().root();
    let empty = wal.propose(None, batch(), Some(b"")).unwrap().root();
    let some = wal.propose(None, batch(), Some(b"m")).unwrap().root();
    let same = wal.propose(None, batch(), Some(b"m")).unwrap().root();
    assert_ne!(none, empty);
    assert_ne!(empty, some);
    assert_eq!(some, same);
    // The equal-root property (design §7.3): a child proposed on the
    // committed node and on a live twin of it gets one root.
    let p1 = wal.propose(None, batch(), None).unwrap();
    let twin = wal.propose(None, batch(), None).unwrap();
    wal.commit(p1).unwrap();
    let on_head = wal.propose(None, vec![put("b", "2")], None).unwrap();
    let on_twin = wal.propose(Some(&twin), vec![put("b", "2")], None).unwrap();
    assert_eq!(on_head.root(), on_twin.root());
}

fn chained_equals_sequential<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let blocks = |i: u64| vec![put(&format!("k{}", i % 5), &i.to_string()), put(&format!("n{i}"), "x")];
    // Sequential: propose, commit, propose, commit.
    let mut a = open(4, 8, policy.clone(), vc.clone());
    let mut sequential = Vec::new();
    for i in 0..6 {
        sequential.push(a.update(blocks(i), None).unwrap());
    }
    // Chained: every block proposed on the previous live one, committed in order.
    let mut b = open(4, 8, policy, vc);
    let mut handles = Vec::new();
    let mut parent = None;
    for i in 0..6 {
        let h = b.propose(parent.as_ref(), blocks(i), None).unwrap();
        handles.push(h.root());
        parent = Some(b.reader().revision(h.root()).unwrap());
        drop(h);
    }
    drop(parent);
    assert_eq!(handles, sequential);
}

fn close_and_errors<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(4, 3, policy, vc);
    let reader = wal.reader();
    wal.update(vec![put("a", "1")], None).unwrap();
    assert!(matches!(
        wal.propose(
            None,
            vec![BatchOp::PrefixDelete {
                prefix: Box::new([0u8])
            }],
            None
        ),
        Err(Error::PrefixDeleteUnsupported)
    ));
    wal.close().unwrap();
    wal.close().unwrap();
    assert!(matches!(wal.propose(None, vec![], None), Err(Error::Closed)));
    // Reads keep working after close (design §3).
    assert_eq!(reader.get(&k("a")).unwrap(), Some(k("1")));
    assert_ne!(reader.root(), EMPTY_ROOT);
}

/// Random-ish blocks of puts and deletes against a reference map: every read
/// through the committed state, and through each live proposal, matches.
fn differential<C, P>(policy: P, vc: C)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let mut wal = open(16, 4, policy, vc);
    let reader = wal.reader();
    let mut reference: BTreeMap<K, V> = BTreeMap::new();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut next = || rng.next();
    for _ in 0..60 {
        let mut batch = Vec::new();
        let mut pending: BTreeMap<K, Option<V>> = BTreeMap::new();
        for _ in 0..(next() % 12) {
            let key = k(&format!("k{}", next() % 40));
            if next() % 4 == 0 {
                batch.push(BatchOp::Delete { key: key.clone() });
                pending.insert(key, None);
            } else {
                let value = k(&format!("v{}", next() % 1000));
                batch.push(BatchOp::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
                pending.insert(key, Some(value));
            }
        }
        let proposal = wal.propose(None, batch, None).unwrap();
        for (key, value) in pending {
            match value {
                Some(value) => {
                    reference.insert(key, value);
                }
                None => {
                    reference.remove(&key);
                }
            }
        }
        for i in 0..40 {
            let key = k(&format!("k{i}"));
            assert_eq!(proposal.get(&key).unwrap(), reference.get(&key).cloned());
        }
        wal.commit(proposal).unwrap();
        for i in 0..40 {
            let key = k(&format!("k{i}"));
            assert_eq!(reader.get(&key).unwrap(), reference.get(&key).cloned());
        }
        assert!(reader.stats().occupancy <= 16);
    }
    assert!(reader.stats().total_evictions > 0);
}

#[test]
fn plain_path_lthash_fifo() {
    suite(|| Fifo::new(2), LtHash::new);
}

#[test]
fn plain_path_lthash_lru_write() {
    suite(|| LruWrite::new(2), LtHash::new);
}

#[test]
fn plain_path_merkle_fifo() {
    suite(|| Fifo::new(2), Merkle::new);
}

#[test]
fn plain_path_merkle_lru_write() {
    suite(|| LruWrite::new(2), Merkle::new);
}

#[test]
fn plain_path_rehash_fifo() {
    suite(|| Fifo::new(2), Rehash::new);
}

#[test]
fn plain_path_rehash_lru_write() {
    suite(|| LruWrite::new(2), Rehash::new);
}

#[test]
fn lru_write_and_fifo_differ_on_a_same_value_rewrite() {
    // Under lru-write a same-value rewrite moves the key, so the root differs
    // from an empty block's; under fifo the two blocks share a root (design
    // §6.1 tests, §7.3).
    fn roots<P: EvictionPolicy<K>>(policy: impl Fn() -> P) -> (Hash, Hash) {
        let mut wal = open(4, 3, policy(), LtHash::new());
        wal.update(vec![put("a", "1")], None).unwrap();
        let rewrite = wal.propose(None, vec![put("a", "1")], None).unwrap().root();
        let empty = wal.propose(None, vec![], None).unwrap().root();
        (rewrite, empty)
    }
    let (rewrite, empty) = roots(|| LruWrite::new(2));
    assert_ne!(rewrite, empty);
    let (rewrite, empty) = roots(|| Fifo::new(2));
    assert_eq!(rewrite, empty);
}

#[test]
fn schemes_and_policies_give_distinct_roots() {
    let block = vec![put("a", "1")];
    let r1 = open(4, 3, Fifo::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r2 = open(4, 3, Fifo::new(2), Merkle::new())
        .update(block.clone(), None)
        .unwrap();
    let r3 = open(4, 3, LruWrite::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r4 = open(4, 3, Fifo::new(1), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r5 = open(5, 3, Fifo::new(2), LtHash::new())
        .update(block.clone(), None)
        .unwrap();
    let r6 = open(4, 3, Fifo::new(2), Rehash::new()).update(block, None).unwrap();
    let roots = [r1, r2, r3, r4, r5, r6];
    for (i, a) in roots.iter().enumerate() {
        for b in &roots[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

fn new_with_populated_set<C, P>(vc: C, policy: P)
where
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    use crate::set::Snapshot;
    let initial = vec![
        (k("a"), Entry::Value(k("old"))),
        (k("b"), Entry::Tombstone),
        (k("c"), Entry::Value(k("3"))),
        (k("a"), Entry::Value(k("1"))),
    ];
    let set = Snapshot::from_entries(vc.clone(), policy.clone(), 4, initial).unwrap();
    assert_eq!((set.len(), set.height()), (3, 1));
    let mut reference = open(4, 4, policy, vc);
    let root = reference
        .update(vec![put("a", "old"), del("b"), put("c", "3"), put("a", "1")], None)
        .unwrap();
    let engine = MemoryStorage::new(MemoryConfig::default());
    let mut wal = AuthWal::new(speculating(4, 4, 4, 1, 0), engine, set).unwrap();
    let reader = wal.reader();
    assert_eq!(reader.root(), root);
    assert_ne!(root, EMPTY_ROOT);
    assert_eq!(reader.revision(root).unwrap().root(), root);
    assert_eq!(reader.latest_revision().node().storage_view().root(), EMPTY_ROOT);
    assert_eq!((reader.stats().height, reader.stats().occupancy), (1, 3));
    assert_eq!(reader.stats().puts, 0, "initialization is not measured as a write");
    assert_eq!(reader.get(&k("a")).unwrap(), Some(k("1")));
    assert_eq!(reader.get(&k("b")).unwrap(), None);
    for batch in [
        vec![put("d", "4")],
        vec![put("e", "5"), put("a", "new")],
        vec![del("c"), put("f", "6")],
    ] {
        let hints: Vec<_> = batch.iter().filter_map(|op| op.key().cloned()).collect();
        wal.prefetch(None, &hints);
        agree(&mut reference, &mut wal, batch);
        for key in ["a", "b", "c", "d", "e", "f"] {
            assert_eq!(reader.get(&k(key)).unwrap(), reference.reader().get(&k(key)).unwrap());
        }
    }
    assert!(reader.stats().total_evictions > 0);
    assert_no_failures(&reader.stats().speculation);
    wal.close().unwrap();
    reference.close().unwrap();
}

#[test]
fn new_accepts_a_populated_set_with_matching_policy_and_root() {
    new_with_populated_set(LtHash::new(), Fifo::new(2).with_trigger(3));
    new_with_populated_set(Merkle::new(), LruWrite::new(2).with_trigger(3));
    new_with_populated_set(Rehash::new(), LruWrite::new(2).with_trigger(3));
}

#[test]
fn new_validates_supplied_set_capacity_and_empty_genesis() {
    use crate::set::Snapshot;
    let set = Snapshot::empty(LtHash::new(), Fifo::<K>::new(2), 4).unwrap();
    let mut wal = AuthWal::new(config(4, 3), MemoryStorage::<K, V>::new(MemoryConfig::default()), set).unwrap();
    assert_eq!(wal.reader().root(), EMPTY_ROOT);
    assert_eq!(wal.reader().stats().occupancy, 0);
    wal.close().unwrap();

    let set = Snapshot::empty(LtHash::new(), Fifo::<K>::new(2), 4).unwrap();
    let result = AuthWal::new(config(5, 3), MemoryStorage::<K, V>::new(MemoryConfig::default()), set);
    assert!(matches!(result, Err(Error::InvalidConfig(_))));
    assert!(Snapshot::<K, V, _, _>::empty(LtHash::new(), Fifo::new(0), 0).is_err());
    let too_large = Snapshot::from_entries(
        LtHash::new(),
        Fifo::new(1),
        2,
        keys(&["a", "b", "c"])
            .into_iter()
            .map(|key| (key, Entry::Value(k("v")))),
    );
    assert!(matches!(too_large, Err(Error::InvalidConfig(_))));
}

#[test]
fn new_rejects_bad_configuration_and_a_used_engine() {
    let engine = || MemoryStorage::<K, V>::new(MemoryConfig::default());
    let bad = |config: Config| new_empty(config, engine(), Fifo::new(2), LtHash::new()).err();
    assert!(matches!(bad(config(0, 3)), Some(Error::InvalidConfig(_))));
    assert!(matches!(bad(config(4, 1)), Some(Error::InvalidConfig(_))));
    let mut speculating = config(4, 3);
    speculating.local.speculate = true;
    speculating.local.speculation_rounds = 0;
    assert!(matches!(bad(speculating.clone()), Some(Error::InvalidConfig(_))));
    speculating.local.speculation_rounds = 3;
    speculating.local.speculation_min_step = 0;
    assert!(matches!(bad(speculating.clone()), Some(Error::InvalidConfig(_))));
    speculating.local.speculation_min_step = 1;
    assert!(bad(speculating).is_none());
    // The policy's own parameters against the capacity (design §6.2).
    assert!(matches!(
        new_empty(config(4, 3), engine(), Fifo::new(10), LtHash::new()).err(),
        Some(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        new_empty(config(8, 3), engine(), LruWrite::new(6).with_trigger(4), LtHash::new()).err(),
        Some(Error::InvalidConfig(_))
    ));
    // The engine keeps fewer revisions than the window needs.
    assert!(matches!(bad(config(4, 1_000)), Some(Error::InvalidConfig(_))));
    // An engine with committed state.
    let used = engine();
    {
        use crate::storage::{StorageEngine, StorageProposal};
        used.head()
            .unwrap()
            .propose(&[put("a", "1")], None)
            .unwrap()
            .commit()
            .unwrap();
    }
    assert!(matches!(
        new_empty(config(4, 3), used, Fifo::new(2), LtHash::new()).err(),
        Some(Error::EngineNotFresh)
    ));
}

#[cfg(feature = "firewood")]
#[test]
fn firewood_hashing_algorithm_is_committed_before_eviction() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = TempDir::new("hashing-id");
    let mut wal = new_empty(
        config(4, 3),
        FirewoodStorage::new(FirewoodConfig::new(dir.db())).unwrap(),
        Fifo::new(2),
        Rehash,
    )
    .unwrap();
    let node = wal.propose(None, vec![put("a", "1")], None).unwrap();
    let snapshot = node.node().snapshot();
    assert_eq!(StorageProposal::<K, V>::root(&**node.node().storage_view()), EMPTY_ROOT);
    let mut terms = wal.terms.clone();
    let expected_id = if cfg!(feature = "ethhash") {
        "firewood/ethereum"
    } else {
        "firewood/merkledb"
    };
    assert_eq!(terms.engine_id, expected_id);
    let root_for = |terms: &crate::root::Terms| {
        terms.combine(
            snapshot.capacity(),
            snapshot.height(),
            snapshot.commitment(),
            snapshot.policy_digest(),
            EMPTY_ROOT,
            crate::root::metadata_hash(None),
        )
    };
    assert_eq!(node.root(), root_for(&terms));
    terms.engine_id = if cfg!(feature = "ethhash") {
        "firewood/merkledb"
    } else {
        "firewood/ethereum"
    };
    assert_ne!(node.root(), root_for(&terms));
    wal.close().unwrap();
}

#[cfg(feature = "firewood")]
#[test]
fn plain_path_on_firewood() {
    use crate::storage::{FirewoodConfig, FirewoodStorage};
    let dir = TempDir::new("m1");
    let mut cfg = FirewoodConfig::new(dir.db());
    cfg.truncate = true;
    cfg.revisions = 8;
    let engine = FirewoodStorage::new(cfg).unwrap();
    let mut wal: AuthWal<K, V, LtHash, LruWrite<K>, FirewoodStorage> =
        new_empty(config(4, 3), engine, LruWrite::new(2), LtHash::new()).unwrap();
    let reader = wal.reader();
    for i in 0..10u32 {
        wal.update(
            vec![put(&format!("k{i}"), &format!("v{i}")), put("hot", &i.to_string())],
            None,
        )
        .unwrap();
    }
    for i in 0..10u32 {
        assert_eq!(reader.get(&k(&format!("k{i}"))).unwrap(), Some(k(&format!("v{i}"))));
    }
    assert_eq!(reader.get(&k("hot")).unwrap(), Some(k("9")));
    let stats = reader.stats();
    assert!(stats.total_evictions > 0 && stats.occupancy <= 4);
    wal.close().unwrap();
}
