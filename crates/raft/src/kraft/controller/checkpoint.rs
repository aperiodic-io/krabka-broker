//! The KIP-630 `.checkpoint` artifacts under the controller's metadata
//! directory: their filename encoding, atomic write, id ordering, and the
//! two-snapshot retention the engine keeps the directory at.

use crate::{error::RaftError, kraft::snapshot_fetch::SnapshotId};

/// Kafka's `Snapshots.BOOTSTRAP_SNAPSHOT_ID`: the id of the bootstrap
/// checkpoint that a dynamic format writes. No replica replicates it, and no
/// replica applies its metadata records to its image. The leader writes those
/// records to the log instead.
pub(crate) const BOOTSTRAP_SNAPSHOT_ID: SnapshotId = (0, 0);

/// Write a KIP-630 `.checkpoint` artifact (bytes only) atomically and
/// durably: the temporary file is synced before the rename and the directory
/// after it, as Kafka's `FileRawSnapshotWriter.freeze` does.
///
/// # Errors
///
/// Returns an error when the directory cannot be created, or the temporary
/// file cannot be written, synced or renamed into place.
pub fn write_checkpoint(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
    bytes: &[u8],
) -> Result<(), RaftError> {
    std::fs::create_dir_all(dir).map_err(krabka_log::LogError::Io)?;
    let name = checkpoint_name(end_offset, epoch);
    let path = dir.join(name);
    krabka_log::write_file_atomic(&path.with_extension("tmp"), &path, bytes)
        .map_err(krabka_log::LogError::Io)?;
    Ok(())
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts, pick the highest
/// `(end_offset, epoch)`, and return its raw bytes. Returns `None` when the
/// directory is absent or holds no checkpoint. Unlike `snapshot::load_latest`,
/// this reads only the `.checkpoint` (the `.meta` sidecar is gone in
/// this engine — the durable epoch lives in the quorum-state file).
///
/// # Errors
///
/// Returns an error when the selected checkpoint file cannot be read.
pub fn load_latest_checkpoint(dir: &std::path::Path) -> Result<Option<Vec<u8>>, RaftError> {
    let Some((end_offset, epoch)) = latest_checkpoint_id(dir) else {
        return Ok(None);
    };
    let bytes = std::fs::read(dir.join(checkpoint_name(end_offset, epoch)))
        .map_err(krabka_log::LogError::Io)?;
    Ok(Some(bytes))
}

pub(crate) fn checkpoint_name(end_offset: i64, epoch: i32) -> String {
    format!("{end_offset:020}-{epoch:010}.checkpoint")
}

pub(crate) fn parse_checkpoint_name(name: &str) -> Option<SnapshotId> {
    let stem = name.strip_suffix(".checkpoint")?;
    let (off, ep) = stem.split_once('-')?;
    let id = (off.parse().ok()?, ep.parse().ok()?);
    if id.0 < 0 || id.1 < 0 {
        return None;
    }
    (checkpoint_name(id.0, id.1) == name).then_some(id)
}

/// Scan `dir` for `<end_offset>-<epoch>.checkpoint` artifacts and return the
/// highest `(end_offset, epoch)` id, or `None` when the directory is absent or
/// holds no checkpoint.
#[must_use]
pub fn latest_checkpoint_id(dir: &std::path::Path) -> Option<SnapshotId> {
    let ids = checkpoint_ids(dir);
    krabka_verified::latest_checkpoint_index(&ids).map(|index| ids[index])
}

/// Every parseable `(end_offset, epoch)` id in `dir`, in directory order. An
/// absent directory and one holding no checkpoint are both empty.
pub(crate) fn checkpoint_ids(dir: &std::path::Path) -> Vec<SnapshotId> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| parse_checkpoint_name(entry.file_name().to_str()?))
        .collect()
}

/// The two newest checkpoint ids in a directory.
struct RecentCheckpointIds {
    latest: SnapshotId,
    previous: SnapshotId,
}

/// The two newest ids in `dir`: the latest and the one before it. A directory
/// holding a single checkpoint returns that id twice, which is what
/// [`krabka_verified::checkpoint_id_retained`] expects for "there is no
/// runner-up".
fn recent_checkpoint_ids(dir: &std::path::Path) -> Option<RecentCheckpointIds> {
    let mut ids = checkpoint_ids(dir);
    let latest = ids.swap_remove(krabka_verified::latest_checkpoint_index(&ids)?);
    let previous =
        krabka_verified::latest_checkpoint_index(&ids).map_or(latest, |index| ids[index]);
    Some(RecentCheckpointIds { latest, previous })
}

#[must_use]
pub fn checkpoint_id_is_newer(candidate: SnapshotId, current: SnapshotId) -> bool {
    krabka_verified::checkpoint_id_newer(candidate.0, candidate.1, current.0, current.1)
}

/// Delete every `.checkpoint` in `dir` except the two newest
/// `(end_offset, epoch)` ids, after the controller's snapshot+prune or an
/// install. Best-effort: read/remove errors are ignored.
///
/// The previous checkpoint stays because a `FetchSnapshot` reader is chunking
/// a snapshot id byte range by byte range: deleting the id it started on the
/// moment a newer one lands answers its next chunk with `SNAPSHOT_NOT_FOUND`
/// and sends it back to position 0 against the newer id, which a follower
/// slower than one snapshot interval never escapes. Kafka keeps the previous
/// snapshot the same way, through retention alone rather than by tracking
/// in-flight readers.
pub fn retain_recent_checkpoints(dir: &std::path::Path) {
    let Some(RecentCheckpointIds { latest, previous }) = recent_checkpoint_ids(dir) else {
        return;
    };
    retain_checkpoints(dir, latest, previous);
}

/// Delete every `.checkpoint` in `dir` except the latest `(end_offset, epoch)`.
///
/// This is the rule for a directory no peer fetches from — the broker
/// observer's own resume store, which only its own restart reads — where the
/// second copy [`retain_recent_checkpoints`] keeps for an in-flight
/// `FetchSnapshot` reader would buy nothing.
pub fn retain_latest_checkpoint(dir: &std::path::Path) {
    let Some(latest) = latest_checkpoint_id(dir) else {
        return;
    };
    retain_checkpoints(dir, latest, latest);
}

/// Delete every `.checkpoint` in `dir` outside the two retained ids. Passing
/// one id as both keeps exactly that one.
fn retain_checkpoints(dir: &std::path::Path, latest: SnapshotId, previous: SnapshotId) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some((off, ep)) = parse_checkpoint_name(name) else {
            continue;
        };
        if !krabka_verified::checkpoint_id_retained(
            off, ep, latest.0, latest.1, previous.0, previous.1,
        ) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Read a specific checkpoint `<end_offset>-<epoch>.checkpoint` by id, or `None`
/// if it is absent (the leader's `FetchSnapshot` serve path).
#[must_use]
pub fn load_checkpoint_by_id(
    dir: &std::path::Path,
    end_offset: i64,
    epoch: i32,
) -> Option<Vec<u8>> {
    std::fs::read(dir.join(checkpoint_name(end_offset, epoch))).ok()
}
