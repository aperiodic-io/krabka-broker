//! Integrity of one archived segment, and the facts it yields.
//!
//! This module owns the decision that a segment is safe to rehydrate. It
//! fetches the segment's artifacts, walks the `.log` batch by batch with
//! `krabka_protocol::records::validate_one_v2_batch`, and checks framing and
//! CRC without decoding records. A batch whose declared length overruns the
//! object is a truncated segment; a batch whose CRC disagrees with its body is
//! a checksum mismatch, reported with the object key and the byte position. It
//! also checks that the copy is not torn: a segment that carries a log but no
//! time index was archived only in part. From the batch headers it derives the
//! two facts the rest of the pipeline needs and the archive metadata cannot be
//! trusted for, the segment's true `end_offset` and `max_timestamp_ms`, and it
//! returns the verified log bytes so the segment is fetched exactly once.
//!
//! This file holds the orchestration and the size caps every fetch is held to.
//! The per-artifact checks live beside it: [`self::log_walk`] for the `.log`
//! pass, [`self::index`] for the sparse sidecar indexes, [`self::snapshot`] for
//! the producer-state snapshot, and [`self::leader_epoch`] for the
//! leader-epoch checkpoint.

use {
    bytes::Bytes,
    krabka_ids::{LeaderEpoch, Offset},
    krabka_object_store::{ObjectOps, ObjectStoreError},
    krabka_remote_storage::{ObjectEntry, Sha256Digest, TopicIdPartition},
    object_store::path::Path,
    std::collections::BTreeMap,
    uuid::Uuid,
};

mod index;
mod leader_epoch;
mod log_walk;
mod snapshot;

#[cfg(test)]
mod tests;

use {
    self::{
        index::{validate_offset_index, validate_time_index, validate_txn_index},
        leader_epoch::parse_leader_epoch_checkpoint,
        log_walk::walk_log,
        snapshot::validate_producer_snapshot,
    },
    crate::{
        backend::ArchiveStore,
        discover::{ArchiveObject, SegmentInventory},
        error::RestoreError,
    },
};

/// What verification established about one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFacts {
    /// The per-copy segment id.
    pub segment_id: Uuid,
    /// First offset the segment holds.
    pub base_offset: Offset,
    /// Last offset the segment holds, derived from the batch headers.
    pub end_offset: Offset,
    /// Highest record timestamp in the segment, derived from the batch
    /// headers. It is `-1` for a segment with no timestamped record.
    pub max_timestamp_ms: i64,
    /// Batches the segment holds.
    pub batches: u64,
    /// Records the batch headers account for.
    pub records: u64,
    /// Size of the verified `.log`, in bytes.
    pub log_bytes: u64,
    /// The offset each leader epoch starts at, from the leader-epoch
    /// checkpoint. The target partition needs it to answer `OffsetForLeaderEpoch`.
    pub leader_epochs: Vec<LeaderEpochStart>,
}

/// The offset a leader epoch starts at, from a leader-epoch checkpoint row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderEpochStart {
    /// The leader epoch.
    pub epoch: LeaderEpoch,
    /// The first offset written under `epoch`.
    pub start_offset: Offset,
}

/// A segment that passed verification, with the bytes that passed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSegment {
    /// What the verification established.
    pub facts: SegmentFacts,
    /// The verified `.log` bytes.
    pub log: Bytes,
}

/// Guard against an unbounded read of a corrupt or hostile `.log` object.
/// Kafka's default `segment.bytes` is 1 GiB, and an operator can raise it, so
/// this cap is a generous multiple of that default rather than the exact
/// configured value, which this offline tool never sees.
pub(super) const MAX_LOG_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Guard for the sparse `.index` and `.timeindex` sidecars. An entry lands
/// only every `index.interval.bytes` (4 KiB by default), so even a segment at
/// the [`MAX_LOG_BYTES`] cap produces a sidecar many orders of magnitude
/// smaller than this.
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;

/// Guard for the `.txnindex` sidecar: one entry per aborted transaction.
const MAX_TXN_INDEX_BYTES: u64 = MAX_INDEX_BYTES;

/// Guard for the `.snapshot` producer-state sidecar: 46 bytes per producer.
const MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;

/// Guard for the `.leader_epoch_checkpoint` text sidecar: one short line per
/// leader change over the segment's lifetime.
const MAX_LEADER_EPOCH_BYTES: u64 = 16 * 1024 * 1024;

/// Fetch and verify one segment.
///
/// # Errors
///
/// Returns [`RestoreError::TornCopy`] when a required artifact is absent,
/// [`RestoreError::TruncatedSegment`] when the log ends inside a batch,
/// [`RestoreError::ChecksumMismatch`] when a batch CRC disagrees with its
/// body, and [`RestoreError::ObjectStore`] when a fetch fails.
pub async fn verify_segment(
    store: &ArchiveStore,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<VerifiedSegment, RestoreError> {
    verify_segment_inner(store, partition, segment, None).await
}

/// Fetch and verify one segment against signed WORM object claims.
///
/// # Errors
///
/// Returns [`RestoreError::Authenticity`] when an artifact is absent from the
/// authenticated manifest set or the bytes fetched now differ from its signed
/// size or digest. Other failures are the same as [`verify_segment`].
pub async fn verify_segment_authenticated(
    store: &ArchiveStore,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
    objects: &BTreeMap<String, ObjectEntry>,
) -> Result<VerifiedSegment, RestoreError> {
    verify_segment_inner(store, partition, segment, Some(objects)).await
}

async fn verify_segment_inner(
    store: &ArchiveStore,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
    objects: Option<&BTreeMap<String, ObjectEntry>>,
) -> Result<VerifiedSegment, RestoreError> {
    // Checked in the order the broker's copy path writes the artifacts, so a
    // torn copy is reported by the first one actually missing.
    let log = require_artifact(segment.log.as_ref(), ".log", partition, segment)?;
    let offset_index =
        require_artifact(segment.offset_index.as_ref(), ".index", partition, segment)?;
    let time_index = require_artifact(
        segment.time_index.as_ref(),
        ".timeindex",
        partition,
        segment,
    )?;
    let producer_snapshot = require_artifact(
        segment.producer_snapshot.as_ref(),
        ".snapshot",
        partition,
        segment,
    )?;
    let leader_epoch = require_artifact(
        segment.leader_epoch.as_ref(),
        ".leader_epoch_checkpoint",
        partition,
        segment,
    )?;

    let ops = store.ops();
    // The `.log` is fetched exactly once, here; every later stage reads it
    // from the `Bytes` this function returns, not from the archive again.
    let log_bytes = fetch_capped(ops, &log.key, MAX_LOG_BYTES, objects).await?;
    let offset_index_bytes = fetch_capped(ops, &offset_index.key, MAX_INDEX_BYTES, objects).await?;
    let time_index_bytes = fetch_capped(ops, &time_index.key, MAX_INDEX_BYTES, objects).await?;
    let producer_snapshot_bytes =
        fetch_capped(ops, &producer_snapshot.key, MAX_SNAPSHOT_BYTES, objects).await?;
    let leader_epoch_bytes =
        fetch_capped(ops, &leader_epoch.key, MAX_LEADER_EPOCH_BYTES, objects).await?;
    let transaction_index = match &segment.transaction_index {
        Some(artifact) => Some((
            artifact.key.clone(),
            fetch_capped(ops, &artifact.key, MAX_TXN_INDEX_BYTES, objects).await?,
        )),
        None => None,
    };

    let walked = walk_log(&log.key, segment, &log_bytes)?;
    let log_bytes_len = u64::try_from(log_bytes.len()).unwrap_or(u64::MAX);

    validate_offset_index(
        &offset_index.key,
        &offset_index_bytes,
        segment.base_offset,
        walked.end_offset,
        log_bytes_len,
    )?;
    validate_time_index(
        &time_index.key,
        &time_index_bytes,
        segment.base_offset,
        walked.end_offset,
    )?;
    if let Some((key, bytes)) = &transaction_index {
        validate_txn_index(key, bytes, segment.base_offset, walked.end_offset)?;
    }
    let snapshot_offset =
        walked
            .end_offset
            .0
            .checked_add(1)
            .ok_or_else(|| RestoreError::TruncatedSegment {
                key: producer_snapshot.key.to_string(),
                position: 0,
                declared: u64::MAX,
                available: u64::MAX,
            })?;
    validate_producer_snapshot(
        &producer_snapshot.key,
        &producer_snapshot_bytes,
        snapshot_offset,
    )?;
    let leader_epochs = parse_leader_epoch_checkpoint(
        &leader_epoch.key,
        &leader_epoch_bytes,
        segment.base_offset,
        walked.end_offset,
    )?;

    Ok(VerifiedSegment {
        facts: SegmentFacts {
            segment_id: segment.segment_id,
            base_offset: segment.base_offset,
            end_offset: walked.end_offset,
            max_timestamp_ms: walked.max_timestamp_ms,
            batches: walked.batches,
            records: walked.records,
            log_bytes: log_bytes_len,
            leader_epochs,
        },
        log: log_bytes,
    })
}

/// Look up one mandatory artifact, or report the torn copy it reveals.
fn require_artifact<'a>(
    artifact: Option<&'a ArchiveObject>,
    name: &str,
    partition: &TopicIdPartition,
    segment: &SegmentInventory,
) -> Result<&'a ArchiveObject, RestoreError> {
    artifact.ok_or_else(|| RestoreError::TornCopy {
        topic: partition.topic.clone(),
        partition: partition.partition,
        segment_id: segment.segment_id,
        artifact: name.to_owned(),
    })
}

/// Fetch a whole object, refusing it before buffering any bytes if it exceeds
/// `max_bytes`. [`ObjectOps::get_capped`] binds its read to backend metadata
/// when available and enforces the limit while streaming the response.
pub(super) async fn fetch_capped(
    ops: &dyn ObjectOps,
    key: &Path,
    max_bytes: u64,
    authenticated: Option<&BTreeMap<String, ObjectEntry>>,
) -> Result<Bytes, RestoreError> {
    let expected = authenticated.and_then(|objects| objects.get(&key.to_string()));
    if authenticated.is_some() && expected.is_none() {
        return Err(RestoreError::Authenticity {
            reason: format!("object `{key}` is not named by an authenticated manifest"),
        });
    }
    let meta = ops.head(key).await?;
    if let Some(expected) = expected
        && meta.size != expected.size_bytes
    {
        return Err(RestoreError::Authenticity {
            reason: format!(
                "object `{key}` changed after manifest verification: expected {} bytes, found {} bytes",
                expected.size_bytes, meta.size
            ),
        });
    }
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        }
        .into());
    }
    let bytes = ops.get_capped(key, max_bytes).await?;
    if let Some(expected) = expected {
        let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let actual_digest = Sha256Digest::of(&bytes);
        if expected.size_bytes != actual_size || expected.sha256 != actual_digest {
            return Err(RestoreError::Authenticity {
                reason: format!(
                    "object `{key}` changed after manifest verification: expected {} bytes with SHA-256 {}, fetched {actual_size} bytes with SHA-256 {actual_digest}",
                    expected.size_bytes, expected.sha256
                ),
            });
        }
    }
    Ok(bytes)
}

/// Best-effort `i64` → `u64` for a diagnostic [`RestoreError`] field. A Kafka
/// offset or timestamp is never negative in a well-formed segment; this only
/// has to render something sensible for corrupt input, never panic, and never
/// allocate.
fn offset_as_u64(value: i64) -> u64 { u64::try_from(value).unwrap_or(0) }
