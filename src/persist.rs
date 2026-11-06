//! Versioned clean-shutdown checkpoints. Only committed logical state is saved;
//! the engine owns persistence of evicted entries. See design §12.6.

use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::policy::EvictionPolicy;
use crate::set::{Entry, EntryRef, Snapshot};
use crate::types::{Digest, EMPTY_ROOT, Error, Hash, Key, Result, Value};
use crate::vc::VectorCommitment;

const MAGIC: &[u8; 8] = b"AWALSNAP";
const VERSION: u32 = 1;

fn invalid(message: &str) -> Error {
    Error::InvalidSnapshot(message.into())
}

fn field(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("snapshot field exceeds 4 GiB"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn encode<K, V, C, P>(
    snapshot: &Snapshot<K, V, C, P>,
    engine_id: &'static str,
    root: Hash,
    storage_root: Hash,
    metadata_hash: Digest,
) -> Result<Vec<u8>>
where
    K: Key,
    V: Value,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let terms = snapshot.terms(engine_id);
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    for id in [terms.scheme_id, terms.policy_name, terms.engine_id] {
        field(&mut out, id.as_bytes())?;
    }
    out.extend_from_slice(&terms.scheme_params);
    out.extend_from_slice(&terms.policy_params);
    out.extend_from_slice(&(snapshot.capacity() as u64).to_le_bytes());
    out.extend_from_slice(&snapshot.height().to_le_bytes());
    out.extend_from_slice(&root.0);
    out.extend_from_slice(&storage_root.0);
    out.extend_from_slice(&metadata_hash);
    out.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
    for (key, entry) in snapshot.entries() {
        field(&mut out, key.as_ref())?;
        match entry {
            EntryRef::Tombstone => out.push(0),
            EntryRef::Value(value) => {
                out.push(1);
                field(&mut out, value)?;
            }
        }
    }
    field(&mut out, &snapshot.encode_policy()?)?;
    out.extend_from_slice(blake3::hash(&out).as_bytes());
    Ok(out)
}

/// Bounds every read against the verified payload, including length prefixes.
struct Decoder<'a>(&'a [u8]);

impl<'a> Decoder<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if len > self.0.len() {
            return Err(invalid("truncated snapshot"));
        }
        let (bytes, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("checked length"))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn field(&mut self) -> Result<&'a [u8]> {
        let len = u32::from_le_bytes(self.array()?) as usize;
        self.take(len)
    }
}

type Recovered<K, V, C, P> = (Snapshot<K, V, C, P>, Digest);

pub(crate) fn load<K, V, C, P>(
    path: &Path,
    vc: C,
    policy: P,
    capacity: usize,
    engine_id: &'static str,
    storage_root: Hash,
) -> Result<Recovered<K, V, C, P>>
where
    K: Key + TryFrom<Vec<u8>>,
    V: Value + TryFrom<Vec<u8>>,
    C: VectorCommitment<K>,
    P: EvictionPolicy<K>,
{
    let bytes = fs::read(path)?;
    let payload_len = bytes
        .len()
        .checked_sub(32)
        .ok_or_else(|| invalid("truncated snapshot"))?;
    let (payload, checksum) = bytes.split_at(payload_len);
    if blake3::hash(payload).as_bytes() != checksum {
        return Err(invalid("snapshot checksum mismatch"));
    }
    let mut input = Decoder(payload);
    if input.take(8)? != MAGIC || u32::from_le_bytes(input.array()?) != VERSION {
        return Err(invalid("unsupported snapshot format or version"));
    }
    for expected in [vc.scheme_id(), policy.name(), engine_id] {
        if input.field()? != expected.as_bytes() {
            return Err(invalid("scheme, policy, or storage engine differs from the snapshot"));
        }
    }
    if input.array::<32>()? != vc.params_digest() || input.array::<32>()? != policy.params_digest() {
        return Err(invalid("consensus parameters differ from the snapshot"));
    }
    if input.u64()? != capacity as u64 {
        return Err(invalid("capacity differs from the snapshot"));
    }
    let height = input.u64()?;
    let saved_root = Hash(input.array()?);
    if Hash(input.array()?) != storage_root {
        return Err(invalid(
            "storage root differs from the snapshot; a matching clean shutdown is required",
        ));
    }
    let metadata_hash = input.array()?;
    if height == 0 && (storage_root != EMPTY_ROOT || metadata_hash != [0; 32]) {
        return Err(invalid("nonempty genesis storage or metadata"));
    }
    let count = usize::try_from(input.u64()?).map_err(|_| invalid("entry count overflows usize"))?;
    if count > capacity || count > input.0.len() / 5 {
        return Err(invalid("invalid snapshot entry count"));
    }
    let mut entries = Vec::new();
    for _ in 0..count {
        let key =
            K::try_from(input.field()?.to_vec()).map_err(|_| invalid("key cannot be decoded into this key type"))?;
        let entry = match input.take(1)?[0] {
            0 => Entry::Tombstone,
            1 => Entry::Value(
                V::try_from(input.field()?.to_vec())
                    .map_err(|_| invalid("value cannot be decoded into this value type"))?,
            ),
            _ => return Err(invalid("unknown entry tag")),
        };
        entries.push((key, entry));
    }
    let policy_state = input.field()?;
    if !input.0.is_empty() {
        return Err(invalid("trailing snapshot data"));
    }
    let snapshot = Snapshot::restore(vc, policy, capacity, height, entries, policy_state)?;
    let root = snapshot.terms(engine_id).combine(
        capacity,
        height,
        snapshot.commitment(),
        snapshot.policy_digest(),
        storage_root,
        metadata_hash,
    );
    if root != saved_root {
        return Err(invalid("reconstructed root differs from the snapshot"));
    }
    Ok((snapshot, metadata_hash))
}

/// Prepare and sync the new file, flush the engine, then atomically replace
/// the checkpoint. A failed write or engine close leaves the old file intact.
/// A failed rename can be retried after the engine has closed. This orders a
/// clean shutdown; it is not an atomic transaction with the engine across crashes.
pub(crate) fn write(path: &Path, bytes: &[u8], flush_engine: impl FnOnce() -> Result<()>) -> Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    flush_engine()?;
    temporary
        .persist(path)
        .map_err(|error| Error::SnapshotIo(error.error))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_failed_flush_does_not_replace_previous_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot");
        fs::write(&path, b"previous").unwrap();
        assert!(matches!(
            write(&path, b"replacement", || Err(Error::Storage("flush failed".into()))),
            Err(Error::Storage(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), b"previous");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
