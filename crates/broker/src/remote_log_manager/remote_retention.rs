//! Remote-retention eviction: which segments the remote tier has held past
//! the topic's total retention window, and the delete lifecycle that removes
//! them.
//!
//! A write-once archive evicts nothing, so the pass ends before it lists.

use {
    super::{NO_BYTES, archive::ArchiveMode, delete::delete_one_segment},
    crate::metrics::RemoteTierPath,
    krabka_ids::LeaderEpoch,
    krabka_log::{DeliveryPolicy, LogConfig, Offset, SegmentExport},
    krabka_remote_storage::{RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition},
    krabka_units::{
        ByteSize, Time,
        convert::{ByteSizeExt as _, TimeExt as _},
    },
    krabka_verified::retention::{
        RemoteRetentionSegment, remote_retention_floor_step, remote_retention_prefix,
    },
    tracing::warn,
};

/// The archive's delete policy and the first segment whose activation scan
/// did not prove it due. Its identity bounds every retention axis.
#[derive(Clone, Copy)]
pub(crate) struct RetentionProtection {
    archive: ArchiveMode,
    first_blocked: Option<uuid::Uuid>,
}

impl From<ArchiveMode> for RetentionProtection {
    fn from(archive: ArchiveMode) -> Self {
        Self {
            archive,
            first_blocked: None,
        }
    }
}

/// KIP-405: compute the set of finished remote segments the topic no longer
/// keeps, in oldest-first order. The walk **stops at the first segment it
/// keeps**, so the remaining remote prefix stays contiguous.
///
/// The rule is Kafka's `RemoteLogManager.cleanupExpiredRemoteLogSegments`,
/// proved in [`krabka_verified::retention::remote_retention_prefix`]. Each
/// segment in turn goes when any of these holds, checked in this order:
/// - `md.end_offset() < deleted_below`, the log-start breach. It leaves the
///   size debt alone.
/// - `md.max_timestamp_ms < now_ms - retention`, Kafka's
///   `isSegmentBreachedByRetentionTime` against `cleanupUntilMs`; Kafka runs
///   no time axis while `now_ms - retention` is negative. It lowers the size
///   debt by the segment's size, but not below zero.
/// - The size debt, `total - retention_size`, is positive and still covers
///   the whole segment (`isSegmentBreachedByRetentionSize`). It lowers the
///   debt by the segment's size.
///
/// `total` is Kafka's `buildRetentionSizeData` total: the finished remote
/// segments' bytes plus `only_local_size`, the local bytes the remote tier
/// does not hold yet (see [`LocalLogFootprint::only_local_size`]). So
/// `retention.bytes` bounds the partition's whole footprint, and local data
/// waiting to be copied pushes the oldest remote segments out.
///
/// A `None` setting disables its axis; the log-start breach has no retention
/// setting to disable and evicts whatever falls below the floor even when both
/// retention settings are `None`. That is what makes a `DeleteRecords` on a
/// tiered topic free the remote bytes it deleted, rather than leaving them
/// listed, fetchable and billed until time or size retention happens to reach
/// them. Kafka's
/// `RemoteLogRetentionHandler.deleteLogStartOffsetBreachedSegments` is the
/// same rule.
///
/// `deleted_below` is the floor **someone deleted up to**, not whatever the
/// partition's `log_start_offset` currently reads: a floor merely inferred
/// from the segments left on disk at `Log::open` sits above the whole archive
/// on a partition whose local segments were evicted, and breaching against it
/// would delete the archive on every restart. A `None` disables the axis.
///
/// The caller must already have filtered to `CopySegmentFinished` and sorted
/// by `start_offset`.
/// `protection.first_blocked` identifies the first scheduled candidate whose
/// stored headers did not prove every batch active. All axes stop before it.
///
/// [`ArchiveMode::WriteOnce`] evicts nothing, whatever the topic's retention
/// settings and log start say: remote retention is a delete, and a write-once
/// archive has none to give.
pub(crate) fn remote_retention_eviction_set(
    protection: RetentionProtection,
    finished: &[RemoteLogSegmentMetadata],
    retention: Option<Time>,
    retention_size: Option<ByteSize>,
    deleted_below: Option<Offset>,
    now_ms: i64,
    only_local_size: ByteSize,
) -> Vec<RemoteLogSegmentMetadata> {
    let total: ByteSize = finished
        .iter()
        .map(segment_size)
        .fold(only_local_size, |acc, size| acc + size);
    // Kafka's `remainingBreachedSize`: zero when `retention.bytes` is unset
    // or not exceeded.
    let size_debt = retention_size.map_or(NO_BYTES, |budget| (total - budget).max(NO_BYTES));
    // Kafka's `RetentionTimeData` exists only while `cleanupUntilMs = now -
    // retention.ms` is non-negative, and a segment breaches it when
    // `maxTimestampMs < cleanupUntilMs`, which is `now - maxTimestampMs >
    // retention.ms`. Both sides are compared as `Time`, so a window at the top
    // of the range converts the same way on either side.
    let window = retention.filter(|window| Time::from_millis(now_ms) >= *window);
    let facts: Vec<RemoteRetentionSegment> = finished
        .iter()
        .map(|md| RemoteRetentionSegment {
            blocked: protection.first_blocked == Some(md.remote_log_segment_id().id),
            log_start_breached: matches!(
                deleted_below,
                Some(floor) if md.end_offset() < floor.0
            ),
            time_expired: window.is_some_and(|window| {
                Time::from_millis(now_ms.saturating_sub(md.max_timestamp_ms())) > window
            }),
            size: segment_size(md).bytes_u64(),
        })
        .collect();
    let len = remote_retention_prefix(
        protection.archive != ArchiveMode::WriteOnce,
        &facts,
        size_debt.bytes_u64(),
    );
    finished.iter().take(len).cloned().collect()
}

/// Kafka's `deleteLogSegmentsDueToLeaderEpochCacheTruncation`: the segments
/// whose leader epochs all lie below `earliest_epoch`, in the order given.
///
/// Such a segment lies outside the leader's epoch lineage, so no offset the
/// log serves is in it. Kafka's comment names the case that leaves them
/// behind, an unclean leader election that puts a leader with a later earliest
/// epoch in charge of a tier an earlier leader filled.
///
/// `earliest_epoch` is [`krabka_log::Log::log_start_epoch`]: `None`, which
/// disables the rule, unless the epoch cache was cut from an established log
/// start.
pub(crate) fn epoch_cache_eviction_set(
    segments: &[RemoteLogSegmentMetadata],
    earliest_epoch: Option<LeaderEpoch>,
) -> Vec<RemoteLogSegmentMetadata> {
    let Some(earliest_epoch) = earliest_epoch else {
        return Vec::new();
    };
    segments
        .iter()
        .filter(|md| {
            md.segment_leader_epochs()
                .keys()
                .all(|epoch| *epoch < earliest_epoch)
        })
        .cloned()
        .collect()
}

/// The remote metadata's `segment_size_in_bytes` (a wire `int32`) as a
/// quantity. Negative sizes are impossible but cheap to clamp.
fn segment_size(md: &RemoteLogSegmentMetadata) -> ByteSize {
    ByteSize::from_bytes_i64(i64::from(md.segment_size_in_bytes().max(0)))
}

/// The local log as Kafka's `UnifiedLog.onlyLocalLogSegmentsSize()` reads
/// it: the sealed local segments, and the whole local size with the active
/// segment in it. The tick reads both under one hold of the log lock.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalLogFootprint<'a> {
    /// Every sealed local segment, oldest first, including any below the
    /// global log start.
    pub sealed: &'a [SegmentExport],
    /// The whole local log's size, active segment included.
    pub size: ByteSize,
}

impl LocalLogFootprint<'_> {
    /// A partition with no local bytes.
    #[cfg(test)]
    pub(crate) const EMPTY: LocalLogFootprint<'static> = LocalLogFootprint {
        sealed: &[],
        size: NO_BYTES,
    };

    /// Kafka's `onlyLocalLogSegmentsSize()`: the bytes of every local segment
    /// whose base offset is above `highest_offset_in_remote_storage` (`None`
    /// is Kafka's `-1`, a tier that holds nothing). The active segment is
    /// never copied and always counts, so this is the whole local size less
    /// the sealed segments that start at or below the highest remote offset.
    pub(crate) fn only_local_size(
        &self,
        highest_offset_in_remote_storage: Option<i64>,
    ) -> ByteSize {
        let copied = self
            .sealed
            .iter()
            .filter(|export| {
                highest_offset_in_remote_storage
                    .is_some_and(|highest| export.base_offset.0 <= highest)
            })
            .fold(NO_BYTES, |total, export| total + export.size);
        (self.size - copied).max(NO_BYTES)
    }
}

/// The partition facts one [`remote_retention_pass`] measures its segments
/// against: the topic's total-retention settings, whether the archive accepts
/// a delete at all, the two readings of the partition's global log start, and
/// the clock reading segments are aged against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteRetentionBounds<'a> {
    pub log_config: &'a LogConfig,
    /// The partition's current `log_start_offset`, whatever established it.
    /// The pass may only report a floor above this one, and only across
    /// offsets it removed itself.
    pub log_start_offset: Offset,
    /// The floor a `DeleteRecords` or an earlier remote deletion moved, if
    /// any: the log-start breach axis measures against this and nothing else.
    /// See [`remote_retention_eviction_set`].
    pub deleted_below: Option<Offset>,
    /// The epoch the leader-epoch cache was cut to by an established log
    /// start, which the epoch-cache-truncation axis measures against. See
    /// [`epoch_cache_eviction_set`].
    pub earliest_epoch: Option<LeaderEpoch>,
    pub now_ms: i64,
    /// The local log whose not-yet-copied bytes count toward
    /// `retention.bytes`.
    pub local: LocalLogFootprint<'a>,
}

/// What one [`remote_retention_pass`] did to a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RemoteRetentionOutcome {
    /// Segments that reached `DeleteSegmentFinished`.
    pub deleted: usize,
    /// The floor the caller must raise the partition's `log_start_offset` to,
    /// or `None` when the pass deleted nothing. It is the last deleted
    /// segment's `end_offset + 1`: those records are now in no tier, so
    /// `ListOffsets(earliest)` and the fetch path must both follow. Kafka's
    /// `cleanupExpiredRemoteLogSegments` hands the same value to
    /// `handleLogStartOffsetUpdate`.
    pub log_start: Option<Offset>,
}

/// KIP-405: evict remote segments the topic no longer keeps -- past its total
/// retention window (`retention.ms` and `retention.bytes`), or wholly below
/// its `log_start_offset`. For each deletable segment, it runs the lifecycle
/// `CopySegmentFinished` → `DeleteSegmentStarted` → RSM delete →
/// `DeleteSegmentFinished`. A failure logs at WARN and ends the
/// partition's pass early. Leftover `DeleteSegmentStarted` metadata is
/// invisible to the read path's finished-only filter, and the next tick
/// retries it.
///
/// The pass runs even when neither retention setting is set, because the
/// log-start breach is an axis of its own: a `DeleteRecords` that moved the
/// floor has to free the remote bytes below it.
///
/// Under [`ArchiveMode::WriteOnce`] the pass returns before it lists
/// anything, so a partition on a write-once archive costs a 30-second tick
/// nothing at all.
pub(crate) async fn remote_retention_pass(
    tp: &TopicIdPartition,
    broker_id: i32,
    bounds: RemoteRetentionBounds<'_>,
    tier: &super::RemoteTier<'_>,
) -> RemoteRetentionOutcome {
    let (rsm, rlmm, index_cache) = (tier.rsm, tier.rlmm, tier.index_cache);
    let RemoteRetentionBounds {
        log_config,
        log_start_offset,
        deleted_below,
        earliest_epoch,
        now_ms,
        local,
    } = bounds;
    // The archive mode comes from the tier and not from the bounds: a tier
    // that refuses deletes and a bounds struct that says it accepts them
    // could not both be right, and there is no reason to let a caller write
    // that pair down.
    let archive = tier.archive;
    if archive == ArchiveMode::WriteOnce {
        return RemoteRetentionOutcome::default();
    }
    let retention = log_config.retention;
    let retention_size = log_config.retention_size;

    let mut finished: Vec<RemoteLogSegmentMetadata> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for retention");
            // A listing this pass cannot read is a delete attempt it cannot
            // make: Kafka counts the same failure under
            // `RemoteDeleteErrorsPerSec`, and without it a tier whose metadata
            // store is unreachable shows no errors at all.
            tier.metrics
                .record_remote_request(RemoteTierPath::Delete, &tp.topic);
            tier.metrics
                .record_remote_error(RemoteTierPath::Delete, &tp.topic);
            return RemoteRetentionOutcome::default();
        }
    };
    finished.sort_by_key(RemoteLogSegmentMetadata::start_offset);
    // Kafka's `highestOffsetInRemoteStorage`: the last offset the finished
    // copies reach.
    let highest_offset_in_remote_storage = finished
        .iter()
        .map(RemoteLogSegmentMetadata::end_offset)
        .max();

    let mut protection = RetentionProtection::from(archive);
    let candidate = remote_retention_eviction_set(
        protection,
        &finished,
        retention,
        retention_size,
        deleted_below,
        now_ms,
        local.only_local_size(highest_offset_in_remote_storage),
    );
    if log_config.delivery_policy == DeliveryPolicy::Scheduled {
        protection.first_blocked = super::delivery::first_pending_remote_segment(
            &candidate,
            rsm,
            log_config.delivery_clock_uncertainty,
            now_ms,
        )
        .await;
    }
    let evict = remote_retention_eviction_set(
        protection,
        &finished,
        retention,
        retention_size,
        deleted_below,
        now_ms,
        local.only_local_size(highest_offset_in_remote_storage),
    );
    // Kafka runs the epoch-cache-truncation cleanup after the retention
    // deletes, over what they leave (`cleanupExpiredRemoteLogSegments`).
    let scheduled = evict.len();
    let outside_lineage = epoch_cache_eviction_set(&finished[scheduled..], earliest_epoch);
    // KIP-405's `RemoteDeleteLagSegments` / `RemoteDeleteLagBytes`, recorded
    // before the round the way `RLMExpirationTask` does: the remote segments
    // this pass has decided to remove and has not removed yet. A tier whose
    // deletes are failing shows a lag that climbs.
    tier.metrics.set_remote_delete_lag(
        &tp.topic,
        u64::try_from(scheduled + outside_lineage.len()).unwrap_or(u64::MAX),
        evict
            .iter()
            .chain(&outside_lineage)
            .map(|md| u64::try_from(md.segment_size_in_bytes().max(0)).unwrap_or(0))
            .sum(),
    );
    let mut outcome = RemoteRetentionOutcome::default();
    // The floor may only cross offsets this pass actually removed, in one
    // unbroken run up from where it stands. The finished list is not always a
    // contiguous offset prefix: `copy_eligible` skips a segment whose copy
    // failed and carries on with the next one, so a gap can sit between two
    // finished segments. Publishing the last delete's end over such a gap
    // would put the floor above a segment that is still on local disk and
    // still readable, and make it unreadable.
    let mut floor = log_start_offset;
    let mut contiguous = true;
    for md in evict {
        tier.metrics
            .record_remote_request(RemoteTierPath::Delete, &tp.topic);
        if !delete_one_segment(tp, broker_id, &md, archive, rsm, rlmm, index_cache).await {
            tier.metrics
                .record_remote_error(RemoteTierPath::Delete, &tp.topic);
            // Stop at the first failure to preserve the contiguous-prefix
            // invariant — the next tick re-tries from the same base.
            break;
        }
        outcome.deleted += 1;
        (floor.0, contiguous) =
            remote_retention_floor_step(floor.0, contiguous, md.start_offset(), md.end_offset());
    }
    if floor > log_start_offset {
        outcome.log_start = Some(floor);
    }
    // A failed retention delete ends the pass, as it always did. These
    // segments hold offsets in no lineage the log serves, so Kafka does not
    // move the log start for them either.
    if outcome.deleted == scheduled {
        outcome.deleted += delete_outside_lineage(tp, broker_id, &outside_lineage, tier).await;
    }
    outcome
}

/// Deletes `segments`, oldest first, until one fails, and returns how many
/// went. The lifecycle and the metrics are the retention loop's.
async fn delete_outside_lineage(
    tp: &TopicIdPartition,
    broker_id: i32,
    segments: &[RemoteLogSegmentMetadata],
    tier: &super::RemoteTier<'_>,
) -> usize {
    let mut deleted = 0;
    for md in segments {
        tier.metrics
            .record_remote_request(RemoteTierPath::Delete, &tp.topic);
        if !delete_one_segment(
            tp,
            broker_id,
            md,
            tier.archive,
            tier.rsm,
            tier.rlmm,
            tier.index_cache,
        )
        .await
        {
            tier.metrics
                .record_remote_error(RemoteTierPath::Delete, &tp.topic);
            break;
        }
        deleted += 1;
    }
    deleted
}

#[cfg(test)]
mod epoch_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod floor_tests;
