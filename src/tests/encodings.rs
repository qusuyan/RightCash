//! Known answers for consensus encodings.

use super::common::*;
use crate::set::Entry;
use crate::storage::{MemoryConfig, MemoryStorage, StorageProposal};
use crate::types::Hash;
use crate::vc::{VectorCommitment, lthash::LtHash, merkle::Merkle, merkle_blake3::MerkleBlake3, rehash::Rehash};

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
    let mut lthash = LtHash::new();
    let no_entries = std::iter::empty::<(&K, EntryRef<'_>)>;
    VectorCommitment::<K>::apply(&mut lthash, &key, None, Some(EntryRef::Value(b"vv")));
    let lattice_value = hex(&VectorCommitment::<K>::commitment(&mut lthash, no_entries()));
    VectorCommitment::<K>::apply(
        &mut lthash,
        &key,
        Some(EntryRef::Value(b"vv")),
        Some(EntryRef::Tombstone),
    );
    let lattice_tombstone = hex(&VectorCommitment::<K>::commitment(&mut lthash, no_entries()));
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
    let rehash = hex(&VectorCommitment::<K>::commitment(&mut Rehash, items()));
    let mut merkle = Merkle::new();
    for (key, entry) in &entries {
        VectorCommitment::<K>::apply(&mut merkle, key, None, Some(entry.as_ref()));
    }
    let merkle = hex(&VectorCommitment::<K>::commitment(&mut merkle, items()));
    // Three entries under branching 2: two levels above the leaves, the
    // last leaf wrapped by a single-child node.
    let mut three = entries.to_vec();
    three.push((k("c"), Entry::Value(k("3"))));
    let merkle_blake3 =
        hex(&MerkleBlake3::<K>::new(2).commitment(three.iter().map(|(key, entry)| (key, entry.as_ref()))));
    let merkle_blake3_params = hex(&MerkleBlake3::<K>::new(16).params_digest());
    let merkle_blake3_empty = hex(&MerkleBlake3::<K>::new(16).commitment(no_entries()));
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
        ("merkle-blake3", merkle_blake3.as_str()),
        ("merkle-blake3 params", merkle_blake3_params.as_str()),
        ("merkle-blake3 empty", merkle_blake3_empty.as_str()),
        ("root", root.as_str()),
        ("memory root", memory_root.as_str()),
    ];
    let expected = [
        (
            "lattice value",
            "4cca5e242b9c1f87e8ca4da85a3603d7149c842a5b646a900188181809840088",
        ),
        (
            "lattice tombstone",
            "a1138820fcb718b4e9bbeafb1887bb5b33b0927fd6afe29b4731f1ad216fb685",
        ),
        (
            "rehash",
            "ab49bef49aed8c0744cff2961df943190f6ef635e276cd2352668b2609b30981",
        ),
        (
            "merkle",
            "dbddcbc409901b07370f96e94c3d582031f62b769d1b5f6946337a921eb62d2b",
        ),
        (
            "merkle-blake3",
            "a40b0b5e19c017c1df5f391de8cee01f78db6512053e380010387b56b98612ff",
        ),
        (
            "merkle-blake3 params",
            "771f08a6c27f3530b0ebfd40dd09e756a9559b017d91e4a15c9fb0bbc8ea9f4c",
        ),
        (
            "merkle-blake3 empty",
            "73cc7e467ab6b16fb29f4be8fbfa74b4cfbbf308cced68b74f82ef3d4e719732",
        ),
        (
            "root",
            "3ce7c1f4f180ebdadbfe764677d332e2e87cd2bfdd3564c7d1a79a3f530d3969",
        ),
        (
            "memory root",
            "47575d88c4a7fb674c8054ebb5428d731eb0027cd9d59b1a3805c09e345cc2a6",
        ),
    ];
    assert_eq!(actual, expected);
}
