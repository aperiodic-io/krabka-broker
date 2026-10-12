//! Local-retention eviction: which copied sealed segments a replica may drop
//! from its own disk once the remote tier holds them.
//!
//! Leader and follower alike run this pass. What makes a sealed segment
//! droppable is the RLMM saying the leader finished copying the offsets it
//! holds, and the RLMM is shared, so a follower reaches the same answer over
//! its own disk that the leader reaches over the leader's.
//!
//! The question is asked in offsets, never in segment boundaries. A replica
//! rolls its own segments, so a follower's segment need not line up with the
//! leader's; [`remote_covered_through`] turns the RLMM listing into the one
//! offset the tier holds an unbroken copy through, and nothing past it is
//! droppable on any replica.
//!
//! The walk ends at the active segment, which the remote tier never holds.
//! When it gets there and the active segment breaches the window too, the
//! pass rolls it, as Kafka's `deletableSegments` does, so that the next copy
//! uploads its records and the next pass drops them. Without the roll a
//! partition that stops taking writes keeps its newest records on local disk
//! until `segment.ms` or `segment.bytes` rolls the segment.
//!
//! The pure walk that picks the deletion target and the roll sits beside the
//! pass that applies them, because they share one contiguous-prefix rule.

use std::sync::Arc;

use krabka_log::{ActiveSegmentExport, DeliveryPolicy, LogConfig, Offset, SegmentExport};
use krabka_remote_storage::{RemoteLogMetadataManager, RemoteLogSegmentState, TopicIdPartition};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use krabka_verified::retention::{
    LocalRetentionSegment, local_retention_prefix, retention_delete_target,
};
use tracing::{debug, info, warn};

use crate::{api_catalog::UnstableApiVersions, partition::Partition};

/// The offset through which the remote tier holds an unbroken copy of this
/// partition, given the `(start, end)` range of every `CopySegmentFinished`
/// segment and the base offset of the replica's oldest local segment.
/// Returns `None` when the remote tier does not reach `local_start` at all.
///
/// This is Kafka's `UnifiedLog.highestOffsetInRemoteStorage()`, the bound
/// `RLMFollowerTask` keeps current on a follower by reading the RLMM. Local
/// retention needs it because **a replica's segment boundaries are its own**:
/// a follower rolls on its own `segment.bytes` as it appends what it fetched,
/// so a leader segment copied as 0..=99 says nothing about a follower segment
/// spanning 0..=199. Matching a local segment's base offset against a remote
/// start offset would call that follower segment copied and delete 100..=199
/// with no remote copy anywhere; a failover in that window would lose
/// acknowledged records. An offset-range bound holds whatever the boundaries.
///
/// The walk stops at the first gap rather than taking the maximum end, because
/// a copy that failed between two that succeeded leaves a hole, and the
/// segments past it cover none of it.
pub(crate) fn remote_covered_through(finished: &[(i64, i64)], local_start: i64) -> Option<i64> {
    let mut ranges: Vec<(i64, i64)> = finished.to_vec();
    ranges.sort_unstable();
    krabka_verified::retention::remote_covered_through(&ranges, local_start)
}

/// The local log as one local-retention walk reads it: the sealed segments,
/// the active segment the walk ends on, and the size of the whole.
pub(crate) struct LocalSegments<'a> {
    /// Sealed segments, oldest first.
    pub sealed: &'a [SegmentExport],
    /// The active segment, or `None` when the walk must stop short of it.
    /// Kafka's `deletableSegments` reaches the active segment only once the
    /// high watermark has passed the log end, and only after every sealed
    /// segment before it.
    pub active: Option<ActiveSegmentExport>,
    /// Bytes of the whole local log, the active segment included.
    pub size: ByteSize,
    /// The actual batch activation frontier, recomputed under the log lock.
    /// `None` on immediate topics.
    pub delivery_watermark: Option<Offset>,
}

/// What one local-retention pass does to a tiered log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalRetentionDecision {
    /// The target to pass to
    /// [`krabka_log::Log::delete_local_segments_through`], or `None` when no
    /// sealed segment goes.
    pub delete_through: Option<i64>,
    /// Roll the active segment: the walk reached it and it breaches local
    /// retention. This is the `shouldRoll` of Kafka's `deletableSegments`.
    pub roll_active: bool,
}

/// Decide what local retention does to a tiered log, given its local
/// segments, the offset the remote tier covers it through, and the per-topic
/// local-retention settings.
///
/// The rule is Kafka's `UnifiedLog.deleteOldSegments` on a tiered log,
/// proved in [`krabka_verified::retention::local_retention_prefix`]. A
/// segment can go only when the remote tier covers it whole, that is, its
/// `last_offset` is at or below `covered_through` (see
/// [`remote_covered_through`]); the walk stops at the first segment the tier
/// does not cover, so the local prefix stays contiguous. Kafka runs
/// `local.retention.bytes` first: it deletes an oldest segment only while
/// the local log's bytes over the budget still cover the whole segment, and
/// only when the local log is at least its budget. Then
/// `local.retention.ms` deletes the prefix with
/// `now_ms - anchor > effective_local`, where the anchor is the segment's
/// `largestTimestamp()` ([`SegmentExport::max_timestamp`]). A segment whose
/// records claim a timestamp in the future has a negative age and is never
/// time-expired, which is what Kafka 4.3.1's
/// `UnifiedLog.deleteRetentionMsBreachedSegments` does (it only logs that the
/// segment is "ineligible to be deleted"). Kafka trunk (KAFKA-20609) ages such
/// a segment of a tiered topic by its file's `lastModified()` instead, so
/// producer clock skew cannot pin local disk forever; that anchor applies only
/// under `unstable.api.versions.enable`, which is what `unstable` carries.
/// Scheduled delivery additionally stops both axes at the actual batch
/// activation frontier, including clock uncertainty. A retention timestamp
/// or a copied remote segment cannot authorize evicting a pending batch.
///
/// Kafka's walk ends at the active segment. The remote tier never holds it,
/// so it is never eligible for deletion (`isSegmentEligibleForDeletion`). When
/// the walk reaches it and the time or size predicate holds for it, Kafka
/// rolls it instead ("Rolling the active segment to make it eligible for
/// deletion"): the next copy uploads the sealed records, and the next pass
/// deletes them. [`LocalRetentionDecision::roll_active`] is that roll. The
/// walk never takes an empty active segment, so the roll always seals records.
pub(crate) fn local_retention_decision(
    unstable: UnstableApiVersions,
    local: &LocalSegments<'_>,
    covered_through: Option<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    now_ms: i64,
) -> LocalRetentionDecision {
    let size_debt = effective_local_size
        .and_then(|budget| local.size.bytes_u64().checked_sub(budget.bytes_u64()));
    let expired = |max_timestamp: i64, last_modified_ms: i64| {
        let anchor = match unstable {
            UnstableApiVersions::Enabled if now_ms < max_timestamp => last_modified_ms,
            _ => max_timestamp,
        };
        let age = Time::from_millis(now_ms.saturating_sub(anchor));
        matches!(effective_local, Some(retention) if age > retention)
    };
    let mut facts: Vec<LocalRetentionSegment> = local
        .sealed
        .iter()
        .map(|ex| LocalRetentionSegment {
            blocked: !matches!(covered_through, Some(through) if ex.last_offset.0 <= through)
                || local
                    .delivery_watermark
                    .is_some_and(|watermark| ex.last_offset >= watermark),
            expired: expired(ex.max_timestamp, ex.last_modified_ms),
            size: ex.size.bytes_u64(),
        })
        .collect();
    // The remote tier is never what holds the active segment back: only the
    // high watermark is, and it already decided whether `local.active` is
    // `Some`.
    let active = if let Some(active) = local.active {
        LocalRetentionSegment {
            blocked: false,
            expired: expired(active.max_timestamp, active.last_modified_ms),
            size: active.size.bytes_u64(),
        }
    } else {
        let sealed_size = local
            .sealed
            .iter()
            .fold(ByteSize::from_bytes(0), |total, ex| total + ex.size);
        LocalRetentionSegment {
            blocked: true,
            expired: false,
            size: local
                .size
                .bytes_u64()
                .saturating_sub(sealed_size.bytes_u64()),
        }
    };
    facts.push(active);
    let len = local_retention_prefix(&facts, size_debt);
    // `delete_local_segments_through` never removes the active segment, so
    // the deletion ends at the last sealed segment whatever the walk took.
    let deleted = len.min(local.sealed.len());
    LocalRetentionDecision {
        delete_through: retention_delete_target(
            deleted
                .checked_sub(1)
                .map(|index| local.sealed[index].last_offset.0),
        ),
        roll_active: len == facts.len(),
    }
}

/// The deletion target [`local_retention_decision`] picks under Kafka 4.3.1's
/// behavior, with the walk stopped short of the active segment. The size of
/// the local log is `local_log_size`, so the active segment's bytes still
/// count toward `local.retention.bytes`.
#[cfg(test)]
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    covered_through: Option<i64>,
    effective_local: Option<Time>,
    effective_local_size: Option<ByteSize>,
    local_log_size: ByteSize,
    now_ms: i64,
) -> Option<i64> {
    local_retention_decision(
        UnstableApiVersions::Disabled,
        &LocalSegments {
            sealed: exports,
            active: None,
            size: local_log_size,
            delivery_watermark: None,
        },
        covered_through,
        effective_local,
        effective_local_size,
        now_ms,
    )
    .delete_through
}

/// Where a local-retention pass measures from: the wall clock, and the
/// partition's high watermark.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalRetentionBounds {
    /// Wall-clock time in epoch milliseconds.
    pub now_ms: i64,
    /// Kafka's walk stops at the first segment the high watermark has not
    /// passed, and for the active segment that bound is the log end. The
    /// sweep reads the watermark before it takes the log lock. The watermark
    /// only rises, so a stale reading can hold a roll back but never let one
    /// through early.
    pub high_watermark: Offset,
}

/// Whether `active` is the segment a walk over `sealed` reaches next.
///
/// The sweep read `sealed` before the copy pass, under an earlier hold of the
/// log lock. An append that rolled the log since then sealed a segment that
/// `sealed` does not describe. Kafka's walk stops at that segment, because the
/// remote tier does not hold it yet, so it never reaches the new active
/// segment, and neither may this one.
fn walk_reaches(
    sealed: &[SegmentExport],
    active: &ActiveSegmentExport,
    local_log_start: Offset,
) -> bool {
    match sealed.last() {
        Some(last) => last.last_offset + 1 == active.base_offset,
        // No sealed segment is left to walk once none below the active one
        // holds a record at or above the log start.
        None => local_log_start >= active.base_offset,
    }
}

/// After the copy pass, drop local sealed segments whose
/// remote copy is `CopySegmentFinished` and that fall outside the
/// per-topic local-retention window, and roll the active segment when
/// it breaches that window too. Returns the count of segments
/// that this pass physically removed from disk.
///
/// This runs on every replica of a tiered partition. On a follower the copy
/// pass belongs to another broker, so `rlmm` is the only thing that says a
/// segment is safe to drop -- which is exactly what it says on the leader too.
/// It says it in offsets: see [`remote_covered_through`] for why a follower
/// cannot read a remote segment's boundaries as its own. Kafka runs
/// `deleteOldSegments`, and so the roll, on every replica as well.
pub(crate) fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    log_config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    bounds: LocalRetentionBounds,
    unstable: UnstableApiVersions,
) -> usize {
    // Trunk ages a future-timestamped segment by its file only while the tier
    // still takes copies (`remoteLogEnabledAndRemoteCopyEnabled`).
    let unstable = if log_config.remote_tier.copy_disable {
        UnstableApiVersions::Disabled
    } else {
        unstable
    };
    let effective_local = log_config.local_retention.or(log_config.retention);
    let effective_local_size = log_config
        .local_retention_size
        .or(log_config.retention_size);

    // Only a sealed segment needs the remote tier's cover. With none, the
    // walk starts at the active segment, which the tier never holds.
    let covered_through = match exports.first() {
        None => None,
        Some(first) => {
            let finished: Vec<(i64, i64)> = match rlmm.list_remote_log_segments(tp) {
                Ok(list) => finished_segment_ranges(&list),
                Err(e) => {
                    warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                          "remote-log-manager: failed to list remote segments for local retention");
                    return 0;
                }
            };
            remote_covered_through(&finished, first.base_offset.0)
        }
    };

    let (decision, deleted, rolled) = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        let active = log.active_segment_export().filter(|active| {
            bounds.high_watermark >= log.log_end_offset()
                && walk_reaches(exports, active, log.local_log_start_offset())
        });
        let delivery_watermark = (log_config.delivery_policy == DeliveryPolicy::Scheduled)
            .then(|| log.advance_delivery_watermark(bounds.now_ms).watermark);
        let decision = local_retention_decision(
            unstable,
            &LocalSegments {
                sealed: exports,
                active,
                size: log.size(),
                delivery_watermark,
            },
            covered_through,
            effective_local,
            effective_local_size,
            bounds.now_ms,
        );
        let deleted = decision
            .delete_through
            .map(|target| log.delete_local_segments_through(Offset(target)));
        let rolled = decision.roll_active.then(|| log.roll());
        (decision, deleted, rolled)
    };
    let removed = match deleted {
        None => 0,
        Some(Ok(n)) => {
            debug!(topic = %tp.topic, partition = tp.partition, target = decision.delete_through,
                   removed = n, "remote-log-manager: local-retention deletion pass completed");
            n
        }
        Some(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, target = decision.delete_through,
                  error = %e, "remote-log-manager: failed to delete local segments");
            0
        }
    };
    match rolled {
        Some(Ok(true)) => {
            info!(topic = %tp.topic, partition = tp.partition,
                  "remote-log-manager: rolled the active segment to make it eligible for deletion");
        }
        Some(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to roll the active segment");
        }
        Some(Ok(false)) | None => {}
    }
    removed
}

#[cfg(test)]
mod tests;

/// Offset ranges whose remote metadata has reached the copy-finished state.
pub(crate) fn finished_segment_ranges(
    segments: &[krabka_remote_storage::RemoteLogSegmentMetadata],
) -> Vec<(i64, i64)> {
    segments
        .iter()
        .filter(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished)
        .map(|metadata| (metadata.start_offset(), metadata.end_offset()))
        .collect()
}
