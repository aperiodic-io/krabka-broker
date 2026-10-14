//! Reconciliation of the bucket scan against an RLMM snapshot, which is what
//! `--rlmm-snapshot` turns on. It is its own file because it is the only part
//! of discovery that has a second source of truth to compare against, and
//! because the rules for which disagreement is routine lag and which is a real
//! inconsistency are stated once, here.

use {
    krabka_remote_storage::{
        PartitionDump, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    },
    krabka_remote_storage_topic::Snapshot,
    krabka_verified::{RestoreReconcileDecision, RestoreSnapshotState},
    std::collections::{HashMap, HashSet},
    uuid::Uuid,
};

#[cfg(test)]
mod tests;

use {
    super::{PartitionInventory, SegmentInventory},
    crate::{args::RestoreArgs, error::RestoreError},
};

/// Load the `--rlmm-snapshot` file, mapping an absent or corrupt file onto
/// [`RestoreError::Io`]: the restore crate defines no dedicated snapshot-error
/// variant, and an operator who passed the flag expects the file to be there
/// and to be readable.
pub(crate) fn load_snapshot(path: &std::path::Path) -> Result<Snapshot, RestoreError> {
    match Snapshot::load(path) {
        Ok(Some(snapshot)) => Ok(snapshot),
        Ok(None) => Err(RestoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("--rlmm-snapshot {} does not exist", path.display()),
        ))),
        Err(error) => Err(RestoreError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("--rlmm-snapshot {}: {error}", path.display()),
        ))),
    }
}

/// Reconcile the bucket scan in `partitions` against the RLMM snapshot at
/// `path`, dropping segments the snapshot has marked deleted and failing on a
/// genuine disagreement between the two sources.
///
/// A segment the snapshot names as [`RemoteLogSegmentState::DeleteSegmentStarted`]
/// is dropped silently: deletion is in flight, and the remote tier has not
/// necessarily caught up, so leftover bytes are expected. A segment the
/// snapshot names as [`RemoteLogSegmentState::DeleteSegmentFinished`] is the
/// opposite case: the metadata says the bytes are gone, so bytes still being
/// in the archive is a genuine disagreement worth stopping for, not a routine
/// lag. The same holds for a segment the scan found that the snapshot does
/// not mention at all, and for a segment the snapshot names as live that the
/// scan did not find.
///
/// # Errors
///
/// Returns [`RestoreError::Io`] when the snapshot file is absent or corrupt,
/// and [`RestoreError::MetadataDisagreement`] when a partition's scanned
/// segments and its snapshot entry disagree about what is live.
pub(super) fn reconcile_with_snapshot(
    partitions: &mut Vec<PartitionInventory>,
    args: &RestoreArgs,
    path: &std::path::Path,
) -> Result<(), RestoreError> {
    reconcile_with_snapshot_policy(partitions, args, &load_snapshot(path)?, false)
}

/// Reconcile a WORM archive with a snapshot whose digest was authenticated by
/// the signed capture boundary. Immutable objects for completed deletions are
/// expected and are excluded from restore.
pub(crate) fn reconcile_authenticated_snapshot(
    partitions: &mut Vec<PartitionInventory>,
    args: &RestoreArgs,
    snapshot: &Snapshot,
) -> Result<(), RestoreError> {
    reconcile_with_snapshot_policy(partitions, args, snapshot, true)
}

fn reconcile_with_snapshot_policy(
    partitions: &mut Vec<PartitionInventory>,
    args: &RestoreArgs,
    snapshot: &Snapshot,
    retain_deleted_objects: bool,
) -> Result<(), RestoreError> {
    let mut dumps: HashMap<&TopicIdPartition, &PartitionDump> = HashMap::new();
    for dump in &snapshot.dump.partitions {
        if args.selects_topic(&dump.topic_id_partition.topic)
            && dumps.insert(&dump.topic_id_partition, dump).is_some()
        {
            return Err(RestoreError::MetadataDisagreement {
                topic: dump.topic_id_partition.topic.clone(),
                partition: dump.topic_id_partition.partition,
                scanned: "duplicate partition entries in RLMM snapshot".to_owned(),
                snapshot: summarize_dump(Some(dump)),
            });
        }
    }

    let mut scanned_keys: HashSet<TopicIdPartition> = HashSet::new();
    for partition in partitions.iter_mut() {
        let key = partition.partition.clone();
        reconcile_partition(partition, dumps.get(&key).copied(), retain_deleted_objects)?;
        scanned_keys.insert(key);
    }

    // A partition the snapshot names with live segments, but the scan found
    // nothing for at all, is also a disagreement: the scan loop above never
    // visits it, because it never became a `PartitionInventory`.
    for (key, dump) in &dumps {
        if scanned_keys.contains(*key) {
            continue;
        }
        if dump.segments.iter().any(|segment| {
            reconcile_decision(false, Some(segment.state()), retain_deleted_objects)
                == RestoreReconcileDecision::Disagree
        }) {
            return Err(RestoreError::MetadataDisagreement {
                topic: dump.topic_id_partition.topic.clone(),
                partition: dump.topic_id_partition.partition,
                scanned: "0 segments, bases []".to_owned(),
                snapshot: summarize_dump(Some(dump)),
            });
        }
    }

    partitions.retain(|partition| !partition.segments.is_empty());
    Ok(())
}

fn snapshot_state(state: Option<RemoteLogSegmentState>) -> RestoreSnapshotState {
    match state {
        None => RestoreSnapshotState::Missing,
        Some(
            RemoteLogSegmentState::CopySegmentStarted | RemoteLogSegmentState::CopySegmentFinished,
        ) => RestoreSnapshotState::Live,
        Some(RemoteLogSegmentState::DeleteSegmentStarted) => RestoreSnapshotState::DeleteStarted,
        Some(RemoteLogSegmentState::DeleteSegmentFinished) => RestoreSnapshotState::DeleteFinished,
    }
}

fn reconcile_decision(
    scanned: bool,
    state: Option<RemoteLogSegmentState>,
    retain_deleted_objects: bool,
) -> RestoreReconcileDecision {
    if retain_deleted_objects
        && scanned
        && state == Some(RemoteLogSegmentState::DeleteSegmentFinished)
    {
        return RestoreReconcileDecision::Exclude;
    }
    krabka_verified::restore_archive_reconcile(scanned, snapshot_state(state))
}

/// A segment as the RLMM snapshot and the scan both identify it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SegmentKey {
    segment_id: Uuid,
    base_offset: i64,
}

/// Reconcile one partition's scanned segments against its snapshot entry, if
/// any, then drop the segments the snapshot says are mid-deletion.
///
/// # Errors
///
/// Returns [`RestoreError::MetadataDisagreement`] under the conditions
/// [`reconcile_with_snapshot`] documents.
fn reconcile_partition(
    partition: &mut PartitionInventory,
    dump: Option<&PartitionDump>,
    retain_deleted_objects: bool,
) -> Result<(), RestoreError> {
    let mut by_key: HashMap<SegmentKey, RemoteLogSegmentState> = HashMap::new();
    let mut duplicate_snapshot_key = false;
    for segment in dump.into_iter().flat_map(|dump| &dump.segments) {
        let key = SegmentKey {
            segment_id: segment.remote_log_segment_id().id,
            base_offset: segment.start_offset(),
        };
        duplicate_snapshot_key |= by_key.insert(key, segment.state()).is_some();
    }

    let scanned_decision = |segment: &SegmentInventory| {
        reconcile_decision(
            true,
            by_key
                .get(&SegmentKey {
                    segment_id: segment.segment_id,
                    base_offset: segment.base_offset.get(),
                })
                .copied(),
            retain_deleted_objects,
        )
    };
    let scan_disagrees = partition
        .segments
        .iter()
        .any(|segment| scanned_decision(segment) == RestoreReconcileDecision::Disagree);
    let snapshot_disagrees =
        dump.into_iter()
            .flat_map(|dump| &dump.segments)
            .any(|snapshot_segment| {
                let scanned = partition.segments.iter().any(|scanned| {
                    scanned.segment_id == snapshot_segment.remote_log_segment_id().id
                        && scanned.base_offset.get() == snapshot_segment.start_offset()
                });
                reconcile_decision(
                    scanned,
                    Some(snapshot_segment.state()),
                    retain_deleted_objects,
                ) == RestoreReconcileDecision::Disagree
            });

    if duplicate_snapshot_key || scan_disagrees || snapshot_disagrees {
        return Err(RestoreError::MetadataDisagreement {
            topic: partition.partition.topic.clone(),
            partition: partition.partition.partition,
            scanned: summarize_scan(&partition.segments),
            snapshot: summarize_dump(dump),
        });
    }

    partition
        .segments
        .retain(|segment| scanned_decision(segment) == RestoreReconcileDecision::Keep);
    Ok(())
}

/// One-line summary of what the bucket scan found for a partition, for a
/// [`RestoreError::MetadataDisagreement`] message.
fn summarize_scan(segments: &[SegmentInventory]) -> String {
    let bases: Vec<i64> = segments.iter().map(|s| s.base_offset.get()).collect();
    summarize(bases.len(), &bases)
}

/// One-line summary of what the RLMM snapshot states for a partition, for a
/// [`RestoreError::MetadataDisagreement`] message.
fn summarize_dump(dump: Option<&PartitionDump>) -> String {
    let mut bases: Vec<i64> = dump.map_or_default(|dump| {
        dump.segments
            .iter()
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect()
    });
    bases.sort_unstable();
    summarize(bases.len(), &bases)
}

/// Render `"N segment(s), bases [...]"`, the shared shape of both summary
/// halves of a [`RestoreError::MetadataDisagreement`] message.
fn summarize(count: usize, bases: &[i64]) -> String {
    let plural = if count == 1 { "" } else { "s" };
    format!("{count} segment{plural}, bases {bases:?}")
}
