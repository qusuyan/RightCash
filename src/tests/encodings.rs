//! Known answers for consensus encodings.

use super::common::*;
use crate::set::Entry;
use crate::storage::{MemoryConfig, MemoryStorage, StorageProposal};
use crate::types::Hash;
use crate::vc::{VectorCommitment, lthash::LtHash, merkle::Merkle, rehash::Rehash};

/// Known answers for the consensus encodings (design §7.2, §7.3): a change
/// to an item's bytes or domain, to the Merkle leaf or inner hash, or to
/// the preimage's order changes every root, and the other tests, which
/// compare two in-tree computations, would not notice.
#[test]
fn known_answers_for_the_encodings() {
    use crate::root::{Terms, metadata_hash};
    use crate::set::EntryRef;
    fn hex(d: &[u8]) -> String {
        d.iter().map(|b| format!("{b:02x}")).collect()
    }
    let key = k("k");
    let lthash = LtHash::new();
    let mut state = VectorCommitment::<K>::empty(&lthash);
    let no_entries = std::iter::empty::<(&K, EntryRef<'_>)>;
    VectorCommitment::<K>::apply(&lthash, &mut state, &key, None, Some(EntryRef::Value(b"vv")));
    let lattice_value = hex(&VectorCommitment::<K>::commitment(&lthash, &state, no_entries()));
    VectorCommitment::<K>::apply(
        &lthash,
        &mut state,
        &key,
        Some(EntryRef::Value(b"vv")),
        Some(EntryRef::Tombstone),
    );
    let lattice_tombstone = hex(&VectorCommitment::<K>::commitment(&lthash, &state, no_entries()));
    let memory_root = {
        use crate::storage::StorageEngine;
        let memory = MemoryStorage::<K, V>::new(MemoryConfig::default());
        let proposal = memory
            .head()
            .unwrap()
            .propose(&[put("a", "1"), del("b")], None)
            .unwrap();
        hex(&proposal.commit().unwrap().0)
    };
    let entries = [(k("a"), Entry::Value(k("1"))), (k("b"), Entry::Tombstone)];
    let items = || entries.iter().map(|(key, entry)| (key, entry.as_ref()));
    let rehash = hex(&VectorCommitment::<K>::commitment(&Rehash, &(), items()));
    let merkle = Merkle::new();
    let mut state = VectorCommitment::<K>::empty(&merkle);
    for (key, entry) in &entries {
        VectorCommitment::<K>::apply(&merkle, &mut state, key, None, Some(entry.as_ref()));
    }
    let merkle = hex(&VectorCommitment::<K>::commitment(&merkle, &state, items()));
    let terms = Terms {
        scheme_id: "lthash",
        policy_name: "fifo",
        engine_id: "memory",
        scheme_params: [1; 32],
        policy_params: [2; 32],
    };
    let root = hex(&terms
        .combine(10, 3, [3; 32], [4; 32], Hash([5; 32]), metadata_hash(Some(b"m")))
        .0);
    let actual = [
        ("lattice value", lattice_value.as_str()),
        ("lattice tombstone", lattice_tombstone.as_str()),
        ("rehash", rehash.as_str()),
        ("merkle", merkle.as_str()),
        ("root", root.as_str()),
        ("memory root", memory_root.as_str()),
    ];
    let expected = [
        (
            "lattice value",
            "a6ee9510061e24bf1ce9102373303a3b57a34b3a334391f4f40997dd9beb2a66",
        ),
        (
            "lattice tombstone",
            "1b786b4d7483232acaac8768c4bdd65e7c1f949e52c27d0eef810d077c56ac1a",
        ),
        (
            "rehash",
            "c0f53ba41d38411087747723b70c9d3891cf1bc9df93839bc8b4d3a0dafeccd0",
        ),
        (
            "merkle",
            "dbddcbc409901b07370f96e94c3d582031f62b769d1b5f6946337a921eb62d2b",
        ),
        (
            "root",
            "2700bf8ef876785482b3cd8d5f450bc5e5f59c2873cf49130322b558b8e56c13",
        ),
        (
            "memory root",
            "171488d0abe82d0401050bbab94c5c5ea9448d058718f9f4c520fddc87535706",
        ),
    ];
    assert_eq!(actual, expected);
}
