//! Engine contract tests (design §10 items 1–7 and the state-function
//! property), run against every backend: `MemoryStorage` always, firewood
//! behind its feature in a temporary directory.

use super::*;
use crate::tests::{del, k, put};
use crate::types::{BatchOp, EMPTY_ROOT, Error};

type K = Vec<u8>;
type V = Vec<u8>;

/// Reads the committed state through the engine's `head()` handle (contract
/// item 7): the only read path auth_wal uses.
fn get<S: StorageEngine<K, V>>(s: &S, key: &str) -> Option<V> {
    s.head().unwrap().get(&k(key)).unwrap()
}

fn contract<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    // Fresh engine: EMPTY_ROOT, empty.
    let s = make();
    assert_eq!(s.root(), EMPTY_ROOT);
    assert_eq!(s.head().unwrap().root(), EMPTY_ROOT);
    assert_eq!(get(&s, "a"), None);

    // Propose + commit; the handle stays readable after commit (item 2); a
    // second commit errors (item 1).
    let p1 = s
        .head()
        .unwrap()
        .propose(&[put("a", "1"), put("b", "2")], None)
        .unwrap();
    assert_ne!(p1.root(), EMPTY_ROOT);
    assert_eq!(p1.get(&k("a")).unwrap(), Some(k("1")));
    let r1 = p1.commit().unwrap();
    assert_eq!(r1, p1.root());
    assert_eq!(s.root(), r1);
    assert_eq!(p1.get(&k("b")).unwrap(), Some(k("2")));
    assert!(matches!(p1.commit(), Err(Error::ProposalConsumed)));
    assert_eq!(get(&s, "a"), Some(k("1")));
    // Prefetch is a method of the view (design §9.4); absent keys are fine.
    p1.prefetch(&[k("a"), k("nope")]).unwrap();

    // Item 4: propose on a committed handle that is still the head proposes
    // on the committed state; the child commits normally.
    let child = p1.propose(&[put("z", "9")], None).unwrap();
    assert_eq!(child.get(&k("z")).unwrap(), Some(k("9")));
    assert_eq!(child.get(&k("a")).unwrap(), Some(k("1")));
    let rz = child.commit().unwrap();
    assert_eq!(s.root(), rz);
    assert_eq!(get(&s, "z"), Some(k("9")));
    // p1 is superseded now: `ProposalOrphaned`, never a panic (item 4); it
    // still reads (item 2).
    assert!(matches!(
        p1.propose(&[put("z", "8")], None),
        Err(Error::ProposalOrphaned)
    ));
    assert_eq!(p1.get(&k("a")).unwrap(), Some(k("1")));

    // Chain: child of an uncommitted parent; committing the child commits the
    // parent first (adapter walks the chain oldest-first; firewood would
    // otherwise reject the child with ParentNotLatest).
    let p2 = s.head().unwrap().propose(&[put("c", "3")], None).unwrap();
    let p3 = p2.propose(&[put("d", "4"), del("a")], None).unwrap();
    assert_eq!(p3.get(&k("c")).unwrap(), Some(k("3")));
    assert_eq!(p3.get(&k("a")).unwrap(), None);
    let r3 = p3.commit().unwrap();
    assert_eq!(s.root(), r3);
    assert_eq!(get(&s, "d"), Some(k("4")));
    assert_eq!(get(&s, "a"), None);

    // Item 3: a child proposed on an uncommitted parent still commits after
    // the parent was committed by its own call (the adapter's pipelined
    // path; firewood re-parents onto the new head).
    let a = s.head().unwrap().propose(&[put("e", "5")], None).unwrap();
    let b = a.propose(&[put("f", "6")], None).unwrap();
    a.commit().unwrap();
    let rb = b.commit().unwrap();
    assert!(matches!(a.commit(), Err(Error::ProposalConsumed)));
    assert_eq!(s.root(), rb);
    assert_eq!(get(&s, "f"), Some(k("6")));

    // Sibling that lost: committing it after the head moved is an error.
    let loser = s.head().unwrap().propose(&[put("x", "1")], None).unwrap();
    let winner = s.head().unwrap().propose(&[put("y", "1")], None).unwrap();
    winner.commit().unwrap();
    assert!(loser.commit().is_err());
    // A retried failed commit reports the failure again, never
    // `ProposalConsumed` (design §9.2).
    assert!(matches!(loser.commit(), Err(Error::Storage(_))));

    // PrefixDelete removes exactly the prefixed keys.
    let p = s
        .head()
        .unwrap()
        .propose(&[put("p1", "1"), put("p2", "2"), put("q", "3")], None)
        .unwrap();
    p.commit().unwrap();
    let p = s
        .head()
        .unwrap()
        .propose(
            &[BatchOp::PrefixDelete {
                prefix: b"p".to_vec().into_boxed_slice(),
            }],
            None,
        )
        .unwrap();
    p.commit().unwrap();
    assert_eq!(get(&s, "p1"), None);
    assert_eq!(get(&s, "p2"), None);
    assert_eq!(get(&s, "q"), Some(k("3")));
}

/// Root is a function of state alone: a speculative chain whose final
/// proposal reverts the mispredictions reaches the same root as the plain
/// path, and no-op puts/deletes do not change it (design §9.1, §9.3).
fn state_function<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let base = [put("k0", "v0"), put("k9", "v9")];

    let plain = make();
    plain.head().unwrap().propose(&base, None).unwrap().commit().unwrap();
    let plain_root = plain
        .head()
        .unwrap()
        .propose(&[put("k1", "v1"), put("k2", "v2")], None)
        .unwrap()
        .commit()
        .unwrap();

    let spec = make();
    spec.head().unwrap().propose(&base, None).unwrap().commit().unwrap();
    // The step predicts k1 (right), k3 (wrong, absent before) and k9 (wrong, present before).
    let step = spec
        .head()
        .unwrap()
        .propose(&[put("k1", "v1"), put("k3", "v3"), put("k9", "changed")], None)
        .unwrap();
    // F: reverts first (k3 -> absent, k9 -> old value), then the batch
    // omitting the pair identical in the step (k1).
    let f = step
        .propose(&[del("k3"), put("k9", "v9"), put("k2", "v2")], None)
        .unwrap();
    let spec_root = f.commit().unwrap();
    assert_eq!(
        spec_root, plain_root,
        "speculative path must reach the plain path's root"
    );
    assert_eq!(get(&spec, "k3"), None);
    assert_eq!(get(&spec, "k9"), Some(k("v9")));

    // No-ops: identical put, delete of absent key.
    let noop = spec
        .head()
        .unwrap()
        .propose(&[put("k1", "v1"), del("absent")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(noop, plain_root);
}

/// Contract item 5: a proposal that is built and dropped (alone or as a
/// chain) leaves no trace on the committed state or on any later root.
fn propose_is_side_effect_free<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let reference = make();
    reference
        .head()
        .unwrap()
        .propose(&[put("a", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    let expected = reference
        .head()
        .unwrap()
        .propose(&[put("b", "2")], None)
        .unwrap()
        .commit()
        .unwrap();

    let s = make();
    s.head()
        .unwrap()
        .propose(&[put("a", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    let dropped = s.head().unwrap().propose(&[put("zz", "9"), del("a")], None).unwrap();
    let dropped_child = dropped.propose(&[put("zy", "8")], None).unwrap();
    drop((dropped, dropped_child));
    let root = s
        .head()
        .unwrap()
        .propose(&[put("b", "2")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(root, expected);
    assert_eq!(get(&s, "zz"), None);
    assert_eq!(get(&s, "a"), Some(b"1".to_vec()));
}

/// A commit that empties the trie leaves a readable handle (every key absent)
/// with the empty root.
fn emptying_commit_is_readable<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    s.head()
        .unwrap()
        .propose(&[put("a", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    let p = s.head().unwrap().propose(&[del("a")], None).unwrap();
    let root = p.commit().unwrap();
    assert_eq!(root, EMPTY_ROOT);
    assert_eq!(p.get(&k("a")).unwrap(), None);
    assert_eq!(get(&s, "a"), None);
}

/// Design §9.2 (R17) / §10 (e): one handle admits concurrent proposes and
/// reads (a link being hashed on the worker and `F` on the same handle never
/// wait for each other); only `commit` is exclusive. The compiler enforces
/// `S::Proposal: Sync` through `StorageProposal`; this exercises the adapter's lock.
fn concurrent_proposes_on_one_handle<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    let base = s.head().unwrap().propose(&[put("a", "1")], None).unwrap();
    let (c1, c2) = std::thread::scope(|scope| {
        let t1 = scope.spawn(|| base.propose(&[put("b", "2")], None).unwrap());
        let t2 = scope.spawn(|| base.propose(&[put("c", "3")], None).unwrap());
        assert_eq!(base.get(&k("a")).unwrap(), Some(b"1".to_vec()));
        (t1.join().unwrap(), t2.join().unwrap())
    });
    assert_eq!(c1.get(&k("b")).unwrap(), Some(b"2".to_vec()));
    assert_eq!(c1.get(&k("c")).unwrap(), None);
    assert_eq!(c2.get(&k("c")).unwrap(), Some(b"3".to_vec()));
    c1.commit().unwrap();
    assert_eq!(get(&s, "b"), Some(b"2".to_vec()));
    assert!(c2.commit().is_err());
}

/// Contract item 6: an uncommitted proposal pins the engine (`ActiveHandles`)
/// until it is committed or dropped; committed proposal handles, `head`
/// handles and revision views do not, so `close` succeeds while they are
/// alive, and is idempotent. Afterwards a committed handle can no longer
/// propose (`Closed`), never panics.
fn close_requires_no_live_handles<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let mut s = make();
    let p = s.head().unwrap().propose(&[put("a", "1")], None).unwrap();
    assert!(matches!(s.close(), Err(Error::ActiveHandles)));
    let root = p.commit().unwrap();
    let dropped = s.head().unwrap().propose(&[put("zz", "9")], None).unwrap();
    assert!(matches!(s.close(), Err(Error::ActiveHandles)));
    drop(dropped);
    let h = s.head().unwrap();
    assert_eq!(h.root(), root);
    s.close().unwrap();
    s.close().unwrap();
    assert!(matches!(h.propose(&[put("b", "2")], None), Err(Error::Closed)));
    assert_eq!(p.get(&k("a")).unwrap(), Some(k("1")));
    drop((p, h));
}

/// Contract item 1, failure half (design §9.2): exactly the handle whose own
/// engine commit failed is consumed; it reports that failure on every later
/// operation and no longer pins the engine. A live child's `commit` reports
/// the ancestor's failure, but the child stays live, readable and pinning
/// until dropped (firewood commits oldest first and stops at the consumed
/// parent).
fn failed_commit_consumes_the_handle<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let mut s = make();
    let loser = s.head().unwrap().propose(&[put("x", "1")], None).unwrap();
    let child = loser.propose(&[put("y", "1")], None).unwrap();
    let winner = s.head().unwrap().propose(&[put("w", "1")], None).unwrap();
    let root = winner.commit().unwrap();
    assert!(matches!(loser.commit(), Err(Error::Storage(_))));
    assert!(matches!(loser.commit(), Err(Error::Storage(_))));
    assert!(matches!(loser.get(&k("x")), Err(Error::Storage(_))));
    assert!(matches!(loser.propose(&[put("z", "1")], None), Err(Error::Storage(_))));
    assert!(matches!(child.commit(), Err(Error::Storage(_))));
    assert_eq!(child.get(&k("y")).unwrap(), Some(k("1")));
    assert_eq!(s.root(), root);
    // The consumed handle does not pin the engine; the live child does.
    assert!(matches!(s.close(), Err(Error::ActiveHandles)));
    drop(child);
    s.close().unwrap();
    drop(loser);
}

/// Contract item 7: `head` is a committed handle at the committed root,
/// readable and proposable (item 4) exactly like a committed proposal.
fn head_handle<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    let h0 = s.head().unwrap();
    assert_eq!(h0.root(), EMPTY_ROOT);
    assert_eq!(h0.get(&k("a")).unwrap(), None);
    assert!(matches!(h0.commit(), Err(Error::ProposalConsumed)));
    // The genesis node's pinned view: the first proposal of an instance goes
    // through it (design §10 item 7).
    let p = h0.propose(&[put("a", "1")], None).unwrap();
    let r = p.commit().unwrap();
    assert_eq!(s.root(), r);
    assert_eq!(get(&s, "a"), Some(k("1")));
    // Superseded: the before-and-after root check rejects it.
    assert!(matches!(
        h0.propose(&[put("b", "2")], None),
        Err(Error::ProposalOrphaned)
    ));
    assert_eq!(h0.get(&k("a")).unwrap(), None, "the empty state keeps reading as empty");
    let h1 = s.head().unwrap();
    assert_eq!(h1.root(), r);
    assert_eq!(h1.get(&k("a")).unwrap(), Some(k("1")));
    // A chain built on the head handle commits like one built by the engine.
    let c1 = h1.propose(&[put("b", "2")], None).unwrap();
    let c2 = c1.propose(&[put("c", "3")], None).unwrap();
    let r2 = c2.commit().unwrap();
    assert_eq!(s.root(), r2);
    assert_eq!(get(&s, "c"), Some(k("3")));
    assert!(matches!(c1.commit(), Err(Error::ProposalConsumed)));
}

/// Empty values: stored, read back as `Some([])` through every handle kind,
/// distinct from absence in the root, deletable.
fn empty_values<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    let p = s.head().unwrap().propose(&[put("e", ""), put("f", "1")], None).unwrap();
    assert_eq!(p.get(&k("e")).unwrap(), Some(Vec::new()), "live proposal");
    let r = p.commit().unwrap();
    assert_eq!(p.get(&k("e")).unwrap(), Some(Vec::new()), "committed handle");
    assert_eq!(get(&s, "e"), Some(Vec::new()), "head");
    let other = make();
    let r2 = other
        .head()
        .unwrap()
        .propose(&[put("f", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_ne!(r, r2, "an empty value and an absent key differ in the root");
    s.head()
        .unwrap()
        .propose(&[put("f", "")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(get(&s, "f"), Some(Vec::new()), "an empty value over a value");
    s.head().unwrap().propose(&[del("e")], None).unwrap().commit().unwrap();
    assert_eq!(get(&s, "e"), None);
}

/// Contract item 2, second half: an implicitly committed ancestor may error
/// but never answers wrong data, and is consumed; deletes of absent keys
/// through a chain read back as absent.
fn implicitly_committed_ancestor<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    let a = s.head().unwrap().propose(&[put("a", "1")], None).unwrap();
    let b = a.propose(&[put("b", "2")], None).unwrap();
    let rb = b.commit().unwrap();
    assert_eq!(s.root(), rb);
    if let Ok(v) = a.get(&k("b")) {
        assert_eq!(v, None, "the ancestor's view must not show the child's write");
    }
    if let Ok(v) = a.get(&k("a")) {
        assert_eq!(v, Some(k("1")));
    }
    assert!(matches!(a.commit(), Err(Error::ProposalConsumed)));
    assert!(matches!(
        a.propose(&[put("c", "3")], None),
        Err(Error::ProposalOrphaned)
    ));
    let c = s.head().unwrap().propose(&[del("nope")], None).unwrap();
    let d = c.propose(&[del("nope"), put("x", "1")], None).unwrap();
    d.commit().unwrap();
    assert_eq!(d.get(&k("nope")).unwrap(), None);
    assert_eq!(get(&s, "nope"), None);
    assert_eq!(get(&s, "x"), Some(k("1")));
}

/// A chain whose base was followed by a *same-root* commit (a no-op batch)
/// is refused (contract items 3 and 4 compare revisions by identity, not
/// by root): the oldest link fails and is consumed, its child stays live,
/// the superseded committed handle no longer proposes, and a fresh `head`
/// does. The Firewood adapter preserves this logical revision rule even
/// when the backend absorbs a same-root commit without creating a revision.
fn commit_after_same_root_commit<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    s.head()
        .unwrap()
        .propose(&[put("a", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    let r1 = s.root();
    let base = s.head().unwrap();
    let step = base.propose(&[put("b", "2")], None).unwrap();
    let child = step.propose(&[put("c", "3")], None).unwrap();
    let r2 = s
        .head()
        .unwrap()
        .propose(&[put("a", "1")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(r1, r2, "a no-op commit keeps the root");
    assert!(
        matches!(child.commit(), Err(Error::Storage(_))),
        "the chain's base was superseded"
    );
    assert!(
        matches!(step.commit(), Err(Error::Storage(_))),
        "the oldest link is consumed"
    );
    assert_eq!(child.get(&k("c")).unwrap(), Some(k("3")), "the child stays live");
    assert!(matches!(
        base.propose(&[put("d", "4")], None),
        Err(Error::ProposalOrphaned)
    ));
    s.head()
        .unwrap()
        .propose(&[put("e", "5")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(get(&s, "b"), None);
    assert_eq!(get(&s, "e"), Some(k("5")));
}

/// A live proposal built on a committed handle keeps reading its base
/// exactly, however many commits follow and whether or not the base left
/// the engine's window: a proposal names its base by hash and reads nodes
/// by address, so the adapter pins the base through the handle (design
/// §10 (c)). `make` opens the engine with a window of two revisions and,
/// on firewood, no node cache, so the reads go to the file.
fn live_proposal_on_a_reaped_base<S: StorageEngine<K, V>>(make: impl Fn() -> S) {
    let s = make();
    let n = 200usize;
    let key = |i: usize| format!("key-{i:04}");
    let base: Vec<BatchOp<K, V>> = (0..n).map(|i| put(&key(i), &format!("base-{i:04}"))).collect();
    s.head().unwrap().propose(&base, None).unwrap().commit().unwrap();
    let head = s.head().unwrap();
    let live = head.propose(&[put("zz", "1")], None).unwrap();
    drop(head);
    for round in 0..20 {
        let batch: Vec<BatchOp<K, V>> = (0..n)
            .map(|i| put(&key(i), &format!("round-{round:02}-{i:04}")))
            .collect();
        s.head().unwrap().propose(&batch, None).unwrap().commit().unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(200));
    for i in 0..n {
        assert_eq!(
            live.get(&k(&key(i))).unwrap(),
            Some(k(&format!("base-{i:04}"))),
            "key {i}"
        );
    }
    assert_eq!(live.get(&k("zz")).unwrap(), Some(k("1")));
}

#[test]
fn memory_contract() {
    contract(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    empty_values(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    implicitly_committed_ancestor(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    commit_after_same_root_commit(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    live_proposal_on_a_reaped_base(|| {
        MemoryStorage::<K, V>::new(MemoryConfig {
            revisions: 2,
            ..MemoryConfig::default()
        })
    });
    state_function(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    close_requires_no_live_handles(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    failed_commit_consumes_the_handle(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    propose_is_side_effect_free(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    emptying_commit_is_readable(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    concurrent_proposes_on_one_handle(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
    head_handle(|| MemoryStorage::<K, V>::new(MemoryConfig::default()));
}

/// Contract item 4, the *after* half (design §9.2 "The first link against a
/// commit"): a committed handle's `propose` that passed its first root check
/// and then saw the head move while it was building must report
/// `ProposalOrphaned`, and the built proposal leaves no trace (item 5). The
/// memory model's synthetic latency opens the window: the propose sleeps
/// `op_latency` per op between its two checks (40 ops here, about a second)
/// while the main thread, after a wait of one `op_latency`, commits a
/// sibling. A main thread descheduled for most of that second would let the
/// propose succeed; a slow spawned thread only moves the failure to the
/// first check, which the assertion also accepts. (Firewood's window is
/// hashing time: not tested there.)
#[test]
fn memory_committed_handle_propose_overtaken_by_commit() {
    let latency = std::time::Duration::from_millis(25);
    let s = MemoryStorage::<K, V>::new(MemoryConfig {
        revisions: 8,
        op_latency: latency,
    });
    let h = s.head().unwrap();
    let sibling = s.head().unwrap().propose(&[put("x", "1")], None).unwrap();
    let batch: Vec<BatchOp<K, V>> = (0..40).map(|i| put(&format!("a{i}"), "1")).collect();
    let (overtaken, root) = std::thread::scope(|scope| {
        let t = scope.spawn(|| h.propose(&batch, None));
        // Let the propose pass its first check and start its latency sleep.
        std::thread::sleep(latency);
        let root = sibling.commit().unwrap();
        (t.join().unwrap(), root)
    });
    assert!(matches!(overtaken, Err(Error::ProposalOrphaned)));
    assert_eq!(s.root(), root);
    assert_eq!(get(&s, "a0"), None);
    assert_eq!(get(&s, "x"), Some(k("1")));
}

#[test]
fn memory_revision_window_goes_stale() {
    let s = MemoryStorage::<K, V>::new(MemoryConfig {
        revisions: 2,
        ..MemoryConfig::default()
    });
    let p1 = s.head().unwrap().propose(&[put("a", "1")], None).unwrap();
    p1.commit().unwrap();
    s.head()
        .unwrap()
        .propose(&[put("b", "2")], None)
        .unwrap()
        .commit()
        .unwrap();
    s.head()
        .unwrap()
        .propose(&[put("c", "3")], None)
        .unwrap()
        .commit()
        .unwrap();
    assert!(matches!(p1.get(&k("a")), Err(Error::RevisionStale)));
}

#[cfg(feature = "firewood")]
mod firewood_backend {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::storage::firewood::{FirewoodConfig, FirewoodStorage};
    use crate::tests::TempDir;

    thread_local! {
        /// Databases this test thread created, removed by `Cleanup`.
        static DIRS: std::cell::RefCell<Vec<TempDir>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    fn make_with(revisions: usize, node_cache_bytes: usize) -> FirewoodStorage {
        let dir = TempDir::new("fw");
        let mut cfg = FirewoodConfig::new(dir.db());
        cfg.truncate = true;
        cfg.revisions = revisions;
        cfg.node_cache_bytes = NonZeroUsize::new(node_cache_bytes).expect("non-zero");
        DIRS.with(|d| d.borrow_mut().push(dir));
        FirewoodStorage::new(cfg).expect("firewood opens")
    }

    fn make() -> FirewoodStorage {
        make_with(8, 192_000_000)
    }

    /// Removes this thread's databases when dropped, on success and on panic.
    /// Declare it first in a test so it drops after every engine the test built.
    struct Cleanup;

    impl Drop for Cleanup {
        fn drop(&mut self) {
            DIRS.with(|d| d.borrow_mut().clear());
        }
    }

    #[test]
    fn firewood_contract() {
        let _cleanup = Cleanup;
        contract(make);
        empty_values(make);
        implicitly_committed_ancestor(make);
        commit_after_same_root_commit(make);
        live_proposal_on_a_reaped_base(|| make_with(2, 1));
        state_function(make);
        close_requires_no_live_handles(make);
        failed_commit_consumes_the_handle(make);
        propose_is_side_effect_free(make);
        emptying_commit_is_readable(make);
        concurrent_proposes_on_one_handle(make);
        head_handle(make);
    }
}
