//! The per-partition copy pass: which sealed segments the remote tier still
//! lacks, and how one tick's copies thread a write-once archive's chain.

use krabka_log::SegmentExport;
use krabka_remote_storage::{RemoteLogSegmentState, TopicIdPartition};
use krabka_units::convert::{ByteSizeExt, TimeExt};
use tracing::{debug, warn};

use super::{
    RemoteTier,
    archive::ChainPosition,
    copy_segment::{CopyOutcome, copy_one},
    local_retention::remote_covered_through,
};

/// KIP-1241's delayed copy: how long a sealed segment may wait before this
/// pass copies it.
///
/// This is Kafka trunk's `RLMCopyTask.isEligibleForUpload`, which
/// `candidateLogSegments` asks of each sealed segment in order and stops at the
/// first that says no. A segment is eligible when the topic's effective
/// `remote.copy.lag.ms` or `remote.copy.lag.bytes` is `0`, or when it is at
/// least that old by its newest record's timestamp, or when at least that many
/// bytes of newer local data sit after it. A limit of `-1` is derived from the
/// effective local retention (`LogConfig.remoteCopyLagMs()`), and one that
/// stays negative, because that retention is unlimited, does not apply.
///
/// The defaults are `0` and `-1`, and `0` wins, so a topic that sets neither
/// copies a segment as soon as it is sealed. Only a broker serving trunk's keys
/// resolves anything else; see [`CopyDelay::resolve`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CopyDelay {
    /// The effective `remote.copy.lag.ms`: `0` copies at once, a positive value
    /// waits, and a negative one is no time check.
    lag_ms: i64,
    /// The effective `remote.copy.lag.bytes`, read the same way.
    lag_bytes: i64,
    /// The clock the age of a segment is measured against.
    now_ms: i64,
    /// The size of the active segment, which is newer than every sealed one and
    /// so counts toward the bytes sitting after each of them.
    active_bytes: u64,
}

impl CopyDelay {
    /// No delay: every sealed segment is eligible.
    pub(crate) const IMMEDIATE: Self = Self {
        lag_ms: 0,
        lag_bytes: 0,
        now_ms: 0,
        active_bytes: 0,
    };

    /// The delay `lag` and `log_config` give a partition at `now_ms`, whose
    /// active segment is `active_bytes` long. Kafka's `LogConfig.remoteCopyLagMs()`
    /// and `remoteCopyLagBytes()` turn a `-1` into the effective
    /// `local.retention.ms` / `local.retention.bytes`, which is `retention.*`
    /// unless the topic sets its own, and `-1` again when that is unlimited.
    pub(crate) fn resolve(
        lag: crate::config_keys::RemoteCopyLag,
        log_config: &krabka_log::LogConfig,
        now_ms: i64,
        active_bytes: u64,
    ) -> Self {
        let derive = |configured: i64, effective_local: Option<i64>| {
            if configured == -1 {
                effective_local.unwrap_or(-1)
            } else {
                configured
            }
        };
        let local_ms = log_config
            .local_retention
            .or(log_config.retention)
            .map(TimeExt::millis_i64);
        let local_bytes = log_config
            .local_retention_size
            .or(log_config.retention_size)
            .map(ByteSizeExt::bytes_i64);
        Self {
            lag_ms: derive(lag.ms, local_ms),
            lag_bytes: derive(lag.bytes, local_bytes),
            now_ms,
            active_bytes,
        }
    }

    /// Whether a sealed segment whose newest record is stamped
    /// `max_timestamp_ms`, with `sealed_after` bytes of sealed segments after
    /// it, may be copied.
    fn allows(self, max_timestamp_ms: i64, sealed_after: u64) -> bool {
        if self.lag_ms == 0 || self.lag_bytes == 0 {
            return true;
        }
        // A segment stamped in the future has a negative age, which Kafka
        // reads as eligible.
        if self.lag_ms > 0 {
            let age_ms = self.now_ms.saturating_sub(max_timestamp_ms);
            if age_ms < 0 || age_ms >= self.lag_ms {
                return true;
            }
        }
        u64::try_from(self.lag_bytes)
            .is_ok_and(|lag| lag > 0 && self.active_bytes.saturating_add(sealed_after) >= lag)
    }
}

/// The offset this copy pass resumes at: one past the offset through which the
/// remote tier holds an unbroken copy, or the partition's oldest local offset
/// when the tier does not reach it at all.
///
/// This is Kafka's `RLMCopyTask.copyLogSegmentsToRemote`, which resumes from
/// `max(findHighestRemoteOffset() + 1, logStartOffset)` rather than from a set
/// of base offsets. A replica rolls its own segments, so **a base offset is
/// not an identity**: after a leader election the new leader's boundaries
/// almost never line up with what the previous leader copied. Keying the skip
/// on base offsets re-uploads every sealed segment whose base does not happen
/// to match one already in the tier, and -- worse -- treats a segment whose
/// base does match, but that runs further than the copied one, as copied,
/// which leaves the tier a hole no later tick ever fills.
///
/// The bound is [`remote_covered_through`], the same walk local retention
/// asks, so the offset the copy pass resumes at and the offset eviction
/// trusts are one number. It stops at the first gap rather than taking the
/// highest end offset, which is what makes a failed copy in the middle get
/// re-copied instead of stranded behind later successes.
fn copy_start_offset(finished: &[(i64, i64)], local_start: i64) -> i64 {
    remote_covered_through(finished, local_start)
        .map_or(local_start, |through| through.saturating_add(1))
}

/// The finished ranges merged into disjoint ascending intervals, so a
/// containment question is one scan. Abutting ranges join: `(0, 99)` and
/// `(100, 199)` are one copy of `0..=199`, which is what makes the merged
/// interval, and not the segment that happens to sit at an offset, the thing
/// the copy pass asks about.
fn merged_coverage(finished: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut ranges: Vec<(i64, i64)> = finished.to_vec();
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some(last) if start <= last.1.saturating_add(1) => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// Whether the remote tier already holds every offset in `base..=last`.
///
/// Past a gap the resume point alone does not answer this: the tier can hold
/// `0..=99` and `200..=299` with `100..=199` missing, and resuming at 100
/// would otherwise re-copy `200..=299` on the way to filling the hole. One
/// merged interval has to contain the whole segment: the intervals are
/// maximal, so a segment that spans two of them spans the hole between them,
/// and the tier holds no copy of the offsets in it.
fn tier_holds_whole(merged: &[(i64, i64)], base: i64, last: i64) -> bool {
    merged
        .iter()
        .any(|&(start, end)| start <= base && last <= end)
}

/// Copy every sealed segment in `exports` that the remote tier does not
/// already hold in full. Returns the number of segments newly copied to
/// `CopySegmentFinished`. This is a separate function from
/// [`tick_all`](super::tick_all) so
/// that tests can drive it directly against a real `Log` and a reference
/// RSM/RLMM.
///
/// A segment is copied when any of its offsets sit at or after
/// [`copy_start_offset`]. A segment that straddles that offset is copied
/// whole, the way Kafka's `candidateLogSegments` includes the segment that
/// contains the resume point: the alternative is to skip it and leave the tier
/// a permanent hole between the resume point and the next segment's base.
#[cfg(test)]
pub(crate) async fn copy_eligible(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    exports: Vec<SegmentExport>,
) -> usize {
    copy_eligible_below(
        tier,
        tp,
        broker_id,
        leader_epoch,
        exports,
        krabka_log::Offset(i64::MAX),
    )
    .await
}

/// [`copy_eligible`], for a partition whose last stable offset is
/// `last_stable_offset`: a segment is copied only when it ends below it, which
/// is Kafka's `RLMCopyTask.candidateLogSegments` rule (`nextSegmentBaseOffset <=
/// lastStableOffset`). The remote tier must hold only committed, acknowledged
/// records, so nothing above the high watermark, and nothing inside an open
/// transaction, is uploaded. After a leader change the tier would otherwise
/// hold records the new leader never had, and unresolved transactions Kafka
/// would not tier yet.
///
/// The copy lag still counts the segments the last stable offset holds back:
/// they are sealed local segments the tier does not hold, as Kafka's
/// `recordLagStats` counts them.
#[cfg(test)]
pub(crate) async fn copy_eligible_below(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: krabka_ids::LeaderEpoch,
    exports: Vec<SegmentExport>,
    last_stable_offset: krabka_log::Offset,
) -> usize {
    copy_eligible_delayed(
        tier,
        tp,
        (broker_id, leader_epoch),
        exports,
        (last_stable_offset, CopyDelay::IMMEDIATE),
    )
    .await
}

/// The copy pass for a partition whose last stable offset is
/// `last_stable_offset` and whose topic's copy lag is `delay`: a segment is
/// copied only when it ends below the last stable offset, which is Kafka's
/// `RLMCopyTask.candidateLogSegments` rule (`nextSegmentBaseOffset <=
/// lastStableOffset`), and only when [`CopyDelay`] says it is eligible. The
/// pass copies the sealed segments in order and stops at the first that is
/// held back, as `candidateLogSegments` does. A held segment is still local and
/// uncopied, so the copy lag counts it.
pub(crate) async fn copy_eligible_delayed(
    tier: &RemoteTier<'_>,
    tp: &TopicIdPartition,
    (broker_id, leader_epoch): (i32, krabka_ids::LeaderEpoch),
    exports: Vec<SegmentExport>,
    (last_stable_offset, delay): (krabka_log::Offset, CopyDelay),
) -> usize {
    let listed = match tier.rlmm.list_remote_log_segments(tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments");
            return 0;
        }
    };

    // Only a *finished* copy covers its offsets.
    //
    // The coverage used to be keyed on every state. A segment left in
    // `CopySegmentStarted` by a failed copy therefore covered its offsets
    // forever and was never retried. On a mutable tier `rollback` erased that
    // metadata, so the bug stayed hidden; a write-once archive keeps it, and
    // tiering for those offsets would stop silently and permanently. A
    // `Delete*` segment does not cover its offsets either: its bytes are on
    // the way out, so a still-local segment over the same range is copyable
    // again.
    let mut finished: Vec<(i64, i64)> = Vec::new();
    for md in &listed {
        match md.state() {
            RemoteLogSegmentState::CopySegmentFinished => {
                finished.push((md.start_offset(), md.end_offset()));
            }
            // This listing is taken once per tick, so anything already in
            // `CopySegmentStarted` was left there by an earlier tick.
            RemoteLogSegmentState::CopySegmentStarted => {
                warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                      segment = %md.remote_log_segment_id().id,
                      "remote-log-manager: segment still in CopySegmentStarted after an \
                       earlier tick; re-copying it under a fresh segment id");
            }
            RemoteLogSegmentState::DeleteSegmentStarted
            | RemoteLogSegmentState::DeleteSegmentFinished => {}
        }
    }

    let Some(local_start) = exports.first().map(|ex| ex.base_offset.0) else {
        tier.metrics.set_remote_copy_lag(&tp.topic, 0, 0);
        return 0;
    };
    let merged = merged_coverage(&finished);
    let copy_start = copy_start_offset(&finished, local_start);
    // The segments this round will copy: everything from the resume point on,
    // less anything the tier already holds whole past a gap.
    let wanted = |ex: &SegmentExport| {
        ex.last_offset.0 >= copy_start
            && !tier_holds_whole(&merged, ex.base_offset.0, ex.last_offset.0)
    };

    // A hole inside this leader's own copies. `highest_offset_for_epoch` is
    // the maximum end offset the tier holds for this epoch whatever sits
    // below it, so it running past the resume point means an earlier copy in
    // this epoch failed and a later one succeeded over it. The resume point
    // is deliberately the near side of that hole -- coverage, not maximum --
    // and this is the line that says the tier is being re-copied over ground
    // it already has.
    match tier.rlmm.highest_offset_for_epoch(tp, leader_epoch) {
        Ok(Some(highest)) if highest >= copy_start => {
            warn!(topic = %tp.topic, partition = tp.partition, copy_start, highest,
                  epoch = leader_epoch.0,
                  "remote-log-manager: the remote tier holds offsets past the resume point in \
                   this leader epoch; an earlier copy left a gap and is being re-copied");
        }
        Ok(_) => {}
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to read the tier's highest offset for this epoch");
        }
    }

    // KIP-405's `RemoteCopyLagSegments` / `RemoteCopyLagBytes`, recorded
    // before the round the way `RLMCopyTask.copyLogSegmentsToRemote` does:
    // the sealed local segments this partition has not finished copying, and
    // their total size. A tier that has stopped keeping up shows a lag that
    // climbs, where a rate would merely stop. The lag counts what this round
    // will copy, so a segment the tier holds only in part is lag until the
    // tier holds all of it.
    let pending: Vec<&SegmentExport> = exports.iter().filter(|ex| wanted(ex)).collect();
    tier.metrics.set_remote_copy_lag(
        &tp.topic,
        u64::try_from(pending.len()).unwrap_or(u64::MAX),
        pending.iter().map(|ex| ex.size.bytes_u64()).sum(),
    );

    let mut chain = ChainPosition::seed(tier.archive, &listed);
    let mut copied = 0;
    // The bytes of sealed segments after the one in hand, which with the active
    // segment are the local data newer than it.
    let mut sealed_after: u64 = exports.iter().map(|ex| ex.size.bytes_u64()).sum();
    for ex in exports {
        sealed_after = sealed_after.saturating_sub(ex.size.bytes_u64());
        // Everything this segment holds is already in the tier.
        if !wanted(&ex) {
            continue;
        }
        // The segment ends at or above the last stable offset, so part of it
        // is not committed yet. The segments after it end above it as well.
        if ex.last_offset >= last_stable_offset {
            break;
        }
        // Kafka trunk's `remote.copy.lag.*`: this segment is too young or has
        // too little after it, and so is every segment after it.
        if !delay.allows(ex.max_timestamp, sealed_after) {
            debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                   "remote-log-manager: segment is within its remote copy lag; not copying yet");
            break;
        }
        if chain == ChainPosition::Exhausted {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  "remote-log-manager: WORM chain sequence exhausted; refusing to copy a \
                   segment without a distinct chain position");
            break;
        }
        // Each success hands back the next chain position, so a run of
        // consecutive segments chains inside one tick with no further listing.
        if let CopyOutcome::Copied { next } =
            copy_one(tier, tp, broker_id, leader_epoch, &ex, chain).await
        {
            copied += 1;
            chain = next;
        }
    }
    copied
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_ids::{LeaderEpoch, Offset};
    use krabka_remote_storage::{
        ChainHead, ChainStamp, CustomMetadata, EpochId, IndexType,
        InmemoryRemoteLogMetadataManager, LogSegmentData, ManifestSeq, RemoteLogMetadataManager,
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
        RemoteLogSegmentMetadataUpdate, RemoteStorageError, RemoteStorageManager, WormChainRecord,
    };
    use krabka_units::bytes;
    use uuid::Uuid;

    use super::*;
    use crate::{
        metrics::BrokerMetrics,
        remote_log_manager::{
            ArchiveMode, test_support as fixtures,
            test_support::{
                FakeWormArchive, archived_backends, copy_exports, local_backends,
                missing_remote_reads, stuck_started_segment, synth_export, three_exports, tier, tp,
            },
        },
        test_support::UnixMillis,
    };

    /// KIP-405's `RemoteCopyRequestsPerSec`, `RemoteCopyBytesPerSec` and the
    /// two copy-lag gauges. An operator whose object store starts refusing
    /// writes learns about it from these; before them, a stalled tier was
    /// visible only as consumer lag and a filling disk.
    macro_rules! accepting_copy_fixture {
        ($rsm:ident, $rlmm:ident, $metrics:ident, $tier:ident, $unstable:expr) => {
            let ($rsm, $rlmm) = accepting_backends(None);
            let $metrics = BrokerMetrics::new();
            let $tier = fixtures::configured_tier(
                &$rsm,
                &$rlmm,
                fixtures::TierSetup {
                    metrics: &$metrics,
                    unstable_api_versions: $unstable,
                    ..Default::default()
                },
            );
        };
    }

    #[tokio::test]
    async fn a_copy_round_records_its_requests_bytes_and_lag() {
        accepting_copy_fixture!(
            rsm,
            rlmm,
            metrics,
            tier,
            crate::api_catalog::UnstableApiVersions::Disabled
        );
        let exports = fixtures::two_exports();

        let copied = copy_exports(&tier, exports).await;

        check!(copied == 2);
        let topic = fixtures::orders_label();
        check!(
            metrics
                .remote_copy_requests_total
                .get_or_create(&topic)
                .get()
                == 2
        );
        check!(metrics.remote_copy_errors_total.get_or_create(&topic).get() == 0);
        check!(metrics.remote_copy_bytes_total.get_or_create(&topic).get() == 128);
        // The lag is what the round found waiting, recorded before it ran.
        check!(metrics.remote_copy_lag_segments.get_or_create(&topic).get() == 2);
        check!(metrics.remote_copy_lag_bytes.get_or_create(&topic).get() == 128);
    }

    /// Kafka's `RLMCopyTask.candidateLogSegments`: a segment is a copy
    /// candidate only when the next segment's base offset is at or below the
    /// last stable offset, that is when the segment ends below it. The tier
    /// must hold only committed, acknowledged records. The copy lag still
    /// counts the segments the bound holds back, which are local and
    /// uncopied.
    #[tokio::test]
    async fn a_segment_is_copied_only_when_it_ends_below_the_last_stable_offset() {
        // `(last stable offset, segments copied)` over the sealed segments
        // 0..=9, 10..=19 and 20..=29.
        for (last_stable_offset, want) in [
            (0, 0),
            (9, 0),
            (10, 1),
            (19, 1),
            (20, 2),
            (29, 2),
            (30, 3),
            (i64::MAX, 3),
        ] {
            accepting_copy_fixture!(
                rsm,
                rlmm,
                metrics,
                tier,
                crate::api_catalog::UnstableApiVersions::Disabled
            );
            let exports = three_exports();

            let copied = copy_eligible_below(
                &tier,
                &tp(),
                1,
                LeaderEpoch(0),
                exports,
                krabka_log::Offset(last_stable_offset),
            )
            .await;

            check!(copied == want, "last stable offset {last_stable_offset}");
            check!(
                rlmm.list_remote_log_segments(&tp()).unwrap().len() == want,
                "last stable offset {last_stable_offset}: the tier's listing"
            );
            let topic = fixtures::orders_label();
            check!(
                metrics.remote_copy_lag_segments.get_or_create(&topic).get() == 3,
                "last stable offset {last_stable_offset}: the lag counts every uncopied segment"
            );
        }
    }

    /// Kafka trunk's `remote.copy.lag.ms` and `remote.copy.lag.bytes`
    /// (KIP-1241) hold a sealed segment back until it is old enough by its
    /// newest record, or enough newer local data sits after it, and a held
    /// segment holds every one after it too. Three sealed segments stamped 100,
    /// 200 and 300 with 64 bytes each and a 64-byte active segment, at a clock of
    /// 1000: the segments are 900, 800 and 700 ms old with 192, 128 and 64 bytes
    /// after them.
    #[tokio::test]
    async fn a_sealed_segment_waits_out_its_copy_lag() {
        use crate::config_keys::RemoteCopyLag;

        let unlimited = krabka_log::LogConfig {
            retention: None,
            retention_size: None,
            ..Default::default()
        };
        // `(lag.ms, lag.bytes, segments copied)`.
        for (label, ms, bytes, want) in [
            ("no lag copies every sealed segment", 0, -1, 3),
            (
                "a zero size lag copies at once whatever the time lag",
                10_000,
                0,
                3,
            ),
            (
                "both derived from unlimited retention never apply",
                -1,
                -1,
                0,
            ),
            ("a time lag nothing has reached", 10_000, -1, 0),
            ("a time lag only the oldest has reached", 900, -1, 1),
            ("a time lag all have reached", 700, -1, 3),
            ("a size lag two segments have reached", 10_000, 100, 2),
            ("a size lag the newest sealed segment lacks", 10_000, 192, 1),
            ("a size lag nothing has reached", 10_000, 193, 0),
            ("either check is enough", 700, 10_000, 3),
        ] {
            accepting_copy_fixture!(
                rsm,
                rlmm,
                metrics,
                tier,
                crate::api_catalog::UnstableApiVersions::Enabled
            );
            let exports = three_exports();
            let delay = CopyDelay::resolve(RemoteCopyLag { ms, bytes }, &unlimited, 1000, 64);

            let copied = copy_eligible_delayed(
                &tier,
                &tp(),
                (1, LeaderEpoch(0)),
                exports,
                (krabka_log::Offset(i64::MAX), delay),
            )
            .await;

            check!(copied == want, "{label}");
            let topic = fixtures::orders_label();
            check!(
                metrics.remote_copy_lag_segments.get_or_create(&topic).get() == 3,
                "{label}: a held segment is still lag"
            );
        }
    }

    /// A `-1` lag is the effective local retention (`LogConfig.remoteCopyLagMs()`
    /// and `remoteCopyLagBytes()`): `retention.*` until the topic sets a
    /// `local.retention.*` of its own, and no check at all when that is
    /// unlimited.
    #[test]
    fn a_derived_copy_lag_is_the_effective_local_retention() {
        use krabka_units::{bytes, millis};

        use crate::config_keys::RemoteCopyLag;

        let derived = RemoteCopyLag { ms: -1, bytes: -1 };
        let config = |retention, local: Option<u32>| krabka_log::LogConfig {
            retention,
            retention_size: retention.map(|_| bytes(4096)),
            local_retention: local.map(millis),
            local_retention_size: local.map(|_| bytes(1024)),
            ..Default::default()
        };
        // A segment stamped 0 at a clock of 5000, with 100 bytes after it.
        let eligible = |delay: CopyDelay| delay.allows(0, 100 - 64);
        for (label, config, want) in [
            (
                "inherits a 4 s retention, and 5 s is over it",
                config(Some(millis(4000)), None),
                true,
            ),
            (
                "inherits a 6 s retention, and 5 s is under it",
                config(Some(millis(6000)), None),
                false,
            ),
            (
                "a 1 s local retention wins over a 6 s retention",
                config(Some(millis(6000)), Some(1000)),
                true,
            ),
            (
                "unlimited retention applies no time check",
                config(None, None),
                false,
            ),
        ] {
            // Only the time lag is in play: the size lag derives a 4096 or 1024
            // byte lag that 100 bytes never reach, or none at all.
            check!(
                eligible(CopyDelay::resolve(derived, &config, 5000, 64)) == want,
                "{label}"
            );
        }
    }

    /// A copy the backend refuses counts as an error and moves no bytes, so
    /// the ratio of the two counters is a failure rate an alert can read.
    #[tokio::test]
    async fn a_refused_copy_counts_an_error_and_no_bytes() {
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(RefusingRsm);
        let rlmm = fixtures::in_memory_metadata();
        let metrics = BrokerMetrics::new();
        let tier = fixtures::configured_tier(
            &rsm,
            &rlmm,
            fixtures::TierSetup {
                metrics: &metrics,
                ..Default::default()
            },
        );

        let copied = copy_exports(
            &tier,
            vec![synth_export(fixtures::SegmentExportSetup::default())],
        )
        .await;

        check!(copied == 0);
        let topic = fixtures::orders_label();
        check!(
            metrics
                .remote_copy_requests_total
                .get_or_create(&topic)
                .get()
                == 1
        );
        check!(metrics.remote_copy_errors_total.get_or_create(&topic).get() == 1);
        check!(metrics.remote_copy_bytes_total.get_or_create(&topic).get() == 0);
    }

    /// An RSM that refuses every copy, so the error path is the only one a
    /// round over it can take.
    struct RefusingRsm;

    impl RemoteStorageManager for RefusingRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::Io(std::io::Error::other(
                "the backend refused the copy",
            )))
        }
        missing_remote_reads!(delete_ok);
    }

    /// An RSM whose copy always succeeds, handing back `receipt` verbatim,
    /// and whose delete always succeeds. It touches no files, so tests can
    /// drive it with synthetic exports.
    struct AcceptingRsm {
        receipt: Option<CustomMetadata>,
    }

    impl RemoteStorageManager for AcceptingRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Ok(self.receipt.clone())
        }
        missing_remote_reads!(delete_ok);
    }

    /// Fresh metadata and a backend that returns the chosen successful-copy receipt.
    fn accepting_backends(
        receipt: Option<CustomMetadata>,
    ) -> (
        Arc<dyn RemoteStorageManager>,
        Arc<dyn RemoteLogMetadataManager>,
    ) {
        (
            Arc::new(AcceptingRsm { receipt }),
            Arc::new(InmemoryRemoteLogMetadataManager::new()),
        )
    }

    /// Every WORM receipt the metadata manager holds for `tp()`, oldest
    /// segment first.
    fn chain_records(rlmm: &Arc<dyn RemoteLogMetadataManager>) -> Vec<WormChainRecord> {
        let mut listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        listed
            .iter()
            .map(|md| {
                WormChainRecord::from_custom_metadata(
                    md.custom_metadata()
                        .expect("an archived segment carries a chain receipt"),
                )
                .expect("the chain receipt decodes")
            })
            .collect()
    }

    #[tokio::test]
    async fn copies_all_sealed_segments_and_records_finished() {
        fixtures::rolled_log_fixture!(log_dir, remote_dir, log, exports);
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let (rsm, rlmm) = archived_backends(remote_dir.path(), &exports).await;

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == exports.len());
        for md in &listed {
            // The data and the indexes are fetchable from the remote store.
            check!(md.state() == RemoteLogSegmentState::CopySegmentFinished);
            check!(!rsm.fetch_log_segment(md, 0, None).unwrap().is_empty());
            // A segment of one batch has an empty offset index, as in Kafka:
            // `LogSegment.append` indexes a batch only once more than
            // `index.interval.bytes` were written before it.
            check!(rsm.fetch_index(md, IndexType::Offset).is_ok());
            check!(
                !rsm.fetch_index(md, IndexType::ProducerSnapshot)
                    .unwrap()
                    .is_empty()
            );
            check!(
                !rsm.fetch_index(md, IndexType::LeaderEpoch)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn re_running_is_idempotent() {
        fixtures::rolled_log_fixture!(log_dir, remote_dir, log, exports);

        let (rsm, rlmm) = local_backends(remote_dir.path());

        let first = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports.clone()).await;
        assert!(first == exports.len());
        // Second pass: everything is already known → nothing re-copied.
        let second = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports.clone()).await;
        assert!(second == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn empty_exports_copies_nothing() {
        let remote_dir = tempfile::tempdir().unwrap();
        let (rsm, rlmm) = local_backends(remote_dir.path());
        let copied = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), Vec::new()).await;
        assert!(copied == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_eligible_records_the_rsm_receipt_on_copy_segment_finished() {
        let receipt = CustomMetadata(b"backend-receipt-42".to_vec());
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AcceptingRsm {
            receipt: Some(receipt.clone()),
        });
        let rlmm = fixtures::in_memory_metadata();

        let copied = copy_exports(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            vec![synth_export(fixtures::SegmentExportSetup::default())],
        )
        .await;

        check!(copied == 1);
        let listed =
            fixtures::check_one_segment_state(&rlmm, RemoteLogSegmentState::CopySegmentFinished);
        check!(listed[0].custom_metadata() == Some(&receipt));
    }

    #[tokio::test]
    async fn copy_eligible_chains_consecutive_segments() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm = fixtures::in_memory_metadata();
        let exports = three_exports();

        let copied = copy_exports(&tier(ArchiveMode::WriteOnce, &rsm, &rlmm), exports).await;

        check!(copied == 3);
        check!(archive.archived_segments() == 3);
        let records = chain_records(&rlmm);
        check!(records.len() == 3);
        check!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>()
                == vec![ManifestSeq(0), ManifestSeq(1), ManifestSeq(2)]
        );
        // One chain run, and each manifest hashes onto the one before it.
        check!(records.iter().all(|r| r.epoch_id == records[0].epoch_id));
        check!(records[0].prev_head == ChainHead::GENESIS);
        check!(records[1].prev_head == records[0].head.unwrap());
        check!(records[2].prev_head == records[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_finishes_the_last_sequence_then_stops() {
        let rlmm = fixtures::in_memory_metadata();
        let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(0xdead));
        let started = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            0,
            9,
            100,
            1,
            100,
            RemoteLogSegmentDetails::new(
                64,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(started).unwrap();
        let receipt = WormChainRecord::request(ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(7)),
            seq: ManifestSeq(u64::MAX - 1),
            prev_head: ChainHead([0xaa; 32]),
        })
        .with_head(ChainHead([0xbb; 32]))
        .to_custom_metadata();
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: segment_id,
            event_timestamp_ms: 101,
            custom_metadata: Some(receipt),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();

        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let copied =
            copy_exports(&tier(ArchiveMode::WriteOnce, &rsm, &rlmm), three_exports()).await;

        check!(copied == 1);
        check!(archive.archived_segments() == 1);
        let records = chain_records(&rlmm);
        check!(records.len() == 2);
        check!(records[1].seq == ManifestSeq(u64::MAX));
    }

    #[tokio::test]
    async fn copy_eligible_resumes_the_chain_from_the_rlmm_after_a_restart() {
        let rlmm = fixtures::in_memory_metadata();
        let first_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_exports(
            &tier(ArchiveMode::WriteOnce, &first_rsm, &rlmm),
            fixtures::two_exports(),
        )
        .await;
        check!(copied == 2);
        let before = chain_records(&rlmm);

        // A restart: a brand-new backend and a brand-new copy pass, sharing
        // only the metadata manager. The chain continues from the receipts.
        let second_rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
        let copied = copy_exports(
            &tier(ArchiveMode::WriteOnce, &second_rsm, &rlmm),
            three_exports(),
        )
        .await;

        check!(copied == 1, "only the segment the archive lacks is copied");
        let after = chain_records(&rlmm);
        check!(after.len() == 3);
        check!(after[..2] == before[..]);
        check!(after[2].epoch_id == before[0].epoch_id);
        check!(after[2].seq == ManifestSeq(2));
        check!(after[2].prev_head == before[1].head.unwrap());
    }

    #[tokio::test]
    async fn copy_eligible_starts_a_new_epoch_when_the_rlmm_is_empty() {
        let mut genesis = Vec::new();
        for _ in 0..2 {
            let (rsm, rlmm) = fixtures::write_once_backends();
            let copied = copy_exports(
                &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
                vec![synth_export(fixtures::SegmentExportSetup::default())],
            )
            .await;
            check!(copied == 1);
            genesis.push(chain_records(&rlmm).remove(0));
        }

        // A metadata manager holding no receipt cannot continue the old
        // chain, so each run says so with a fresh epoch instead of restarting
        // the old one at sequence zero and looking like a rewrite.
        check!(genesis[0].epoch_id != genesis[1].epoch_id);
        for record in &genesis {
            check!((record.seq, record.prev_head) == (ManifestSeq(0), ChainHead::GENESIS));
        }
    }

    /// Put one `CopySegmentFinished` segment covering `base..=last` into the
    /// metadata manager, the way a previous leader's copy pass left it. The
    /// boundaries are the caller's, which is the point: the next leader's are
    /// its own.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct FinishedSegmentSetup {
        #[default(Uuid::from_u128(1))]
        id: Uuid,
        #[default(krabka_log::Offset(0))]
        base: krabka_log::Offset,
        #[default(krabka_log::Offset(99))]
        last: krabka_log::Offset,
        #[default(LeaderEpoch(0))]
        epoch: LeaderEpoch,
    }

    fn finished_segment(rlmm: &Arc<dyn RemoteLogMetadataManager>, setup: FinishedSegmentSetup) {
        let FinishedSegmentSetup {
            id,
            base,
            last,
            epoch,
        } = setup;
        let segment_id = RemoteLogSegmentId::new(tp(), id);
        let started = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            base.0,
            last.0,
            100,
            1,
            100,
            RemoteLogSegmentDetails::new(
                64,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {epoch => base.0},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(started).unwrap();
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: segment_id,
            event_timestamp_ms: 101,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
    }

    /// Every finished range the metadata manager holds for `tp()`, ascending.
    fn finished_ranges(rlmm: &Arc<dyn RemoteLogMetadataManager>) -> Vec<(i64, i64)> {
        let mut ranges: Vec<(i64, i64)> =
            crate::remote_log_manager::local_retention::finished_segment_ranges(
                &rlmm.list_remote_log_segments(&tp()).unwrap(),
            );
        ranges.sort_unstable();
        ranges
    }

    /// One coverage case: what the tier holds, where the replica's local log
    /// starts, and the resume point and containment answers that follow.
    struct CoverageCase {
        label: &'static str,
        finished: &'static [(i64, i64)],
        local_start: i64,
        merged: &'static [(i64, i64)],
        copy_start: i64,
    }

    #[test]
    fn coverage_merges_ranges_and_places_the_resume_point() {
        let cases = [
            CoverageCase {
                label: "obsolete ranges cannot move copy resumption below the local start",
                finished: &[(0, 9)],
                local_start: 20,
                merged: &[(0, 9)],
                copy_start: 20,
            },
            CoverageCase {
                label: "obsolete disconnected prefix cannot suppress later anchored coverage",
                finished: &[(0, 9), (20, 29)],
                local_start: 20,
                merged: &[(0, 9), (20, 29)],
                copy_start: 30,
            },
            CoverageCase {
                label: "an empty tier resumes at the oldest local offset",
                finished: &[],
                local_start: 40,
                merged: &[],
                copy_start: 40,
            },
            CoverageCase {
                label: "abutting copies are one interval, in any listed order",
                finished: &[(100, 199), (0, 99)],
                local_start: 0,
                merged: &[(0, 199)],
                copy_start: 200,
            },
            CoverageCase {
                label: "overlapping copies merge rather than double-count",
                finished: &[(0, 99), (50, 149)],
                local_start: 0,
                merged: &[(0, 149)],
                copy_start: 150,
            },
            CoverageCase {
                label: "a hole puts the resume point on its near side",
                finished: &[(0, 99), (200, 299)],
                local_start: 0,
                merged: &[(0, 99), (200, 299)],
                copy_start: 100,
            },
            CoverageCase {
                label: "a tier that does not reach the local log resumes at the local start",
                finished: &[(200, 299)],
                local_start: 0,
                merged: &[(200, 299)],
                copy_start: 0,
            },
        ];
        for case in cases {
            check!(
                merged_coverage(case.finished) == case.merged,
                "{}",
                case.label
            );
            check!(
                copy_start_offset(case.finished, case.local_start) == case.copy_start,
                "{}",
                case.label
            );
        }
    }

    #[test]
    fn tier_holds_whole_needs_one_interval_around_the_segment() {
        let merged = merged_coverage(&[(0, 99), (200, 299)]);
        check!(tier_holds_whole(&merged, 0, 99));
        check!(tier_holds_whole(&merged, 20, 40));
        check!(tier_holds_whole(&merged, 200, 299));
        check!(
            !tier_holds_whole(&merged, 50, 149),
            "the segment runs past the copy"
        );
        check!(!tier_holds_whole(&merged, 100, 199), "the hole itself");
        check!(
            !tier_holds_whole(&merged, 90, 210),
            "two intervals do not join around it"
        );
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct MisalignedExportSetup {
        #[default([bytes(64); 3])]
        sizes: [krabka_units::ByteSize; 3],
    }

    /// The new leader's offset ranges straddle the old leader's segment boundary.
    fn misaligned_exports(setup: MisalignedExportSetup) -> Vec<SegmentExport> {
        [
            (Offset(0)..=Offset(49), UnixMillis(100)),
            (Offset(50)..=Offset(149), UnixMillis(200)),
            (Offset(150)..=Offset(249), UnixMillis(300)),
        ]
        .into_iter()
        .zip(setup.sizes)
        .map(|((bounds, timestamp), size)| {
            synth_export(fixtures::SegmentExportSetup {
                bounds,
                timestamp,
                size,
            })
        })
        .collect()
    }

    /// The failover case the whole change is for. The previous leader copied
    /// its own `0..=99` and `100..=199`; the new leader rolled `0..=49`,
    /// `50..=149` and `150..=249` over the same records. Under the old
    /// base-offset skip set the new leader re-uploaded `50..=149` and
    /// `150..=249` -- two duplicate objects -- and under a base offset that
    /// happened to line up it would have skipped a segment outright and left
    /// the tier a hole.
    #[tokio::test]
    async fn a_new_leaders_misaligned_segments_below_the_watermark_are_not_re_copied() {
        let (rsm, rlmm) = accepting_backends(None);
        finished_segment(
            &rlmm,
            FinishedSegmentSetup {
                id: Uuid::from_u128(0xa1),
                ..Default::default()
            },
        );
        finished_segment(
            &rlmm,
            FinishedSegmentSetup {
                id: Uuid::from_u128(0xa2),
                base: krabka_log::Offset(100),
                last: krabka_log::Offset(199),
                ..Default::default()
            },
        );

        let copied = copy_eligible(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            &tp(),
            2,
            LeaderEpoch(1),
            misaligned_exports(MisalignedExportSetup::default()),
        )
        .await;

        // Only the segment holding offsets the tier lacks is copied, and it is
        // copied whole: skipping it because it starts under the watermark
        // would leave 200..=249 in no remote segment at all.
        check_copy_coverage(copied, &rlmm, &[(0, 99), (100, 199), (150, 249)], 249);
    }

    /// Check the completed copy against the caller's independent range and coverage oracle.
    fn check_copy_coverage(
        copied: usize,
        rlmm: &Arc<dyn RemoteLogMetadataManager>,
        expected_ranges: &[(i64, i64)],
        expected_end: i64,
    ) {
        check!(copied == 1);
        check!(finished_ranges(rlmm) == expected_ranges);
        check!(remote_covered_through(&finished_ranges(rlmm), 0) == Some(expected_end));
    }

    /// A tick that finds a hole fills the hole and leaves what sits above it
    /// alone. Resuming at the near side of the hole is what makes the copy
    /// happen at all; the containment check is what keeps the segments past it
    /// from being uploaded a second time on the way.
    #[tokio::test]
    async fn a_hole_is_filled_without_re_copying_the_segments_above_it() {
        let (rsm, rlmm) = accepting_backends(None);
        finished_segment(
            &rlmm,
            FinishedSegmentSetup {
                id: Uuid::from_u128(0xb1),
                ..Default::default()
            },
        );
        finished_segment(
            &rlmm,
            FinishedSegmentSetup {
                id: Uuid::from_u128(0xb2),
                base: krabka_log::Offset(200),
                last: krabka_log::Offset(299),
                ..Default::default()
            },
        );

        let copied = copy_exports(
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
            vec![
                synth_export(fixtures::SegmentExportSetup {
                    bounds: Offset(0)..=Offset(99),
                    ..Default::default()
                }),
                synth_export(fixtures::SegmentExportSetup {
                    bounds: Offset(100)..=Offset(199),
                    timestamp: UnixMillis(200),
                    ..Default::default()
                }),
                synth_export(fixtures::SegmentExportSetup {
                    bounds: Offset(200)..=Offset(299),
                    timestamp: UnixMillis(300),
                    ..Default::default()
                }),
            ],
        )
        .await;

        check_copy_coverage(copied, &rlmm, &[(0, 99), (100, 199), (200, 299)], 299);
    }

    /// The copy-lag gauges read off the same coverage. Before this, a segment
    /// the tier held only in part was in the skip set, so the lag read zero
    /// while the tier had a hole and nothing was ever copied for it.
    #[tokio::test]
    async fn copy_lag_counts_the_segments_the_tier_does_not_hold_whole() {
        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(RefusingRsm);
        let rlmm = fixtures::in_memory_metadata();
        finished_segment(
            &rlmm,
            FinishedSegmentSetup {
                id: Uuid::from_u128(0xc1),
                ..Default::default()
            },
        );
        let metrics = BrokerMetrics::new();
        let tier = fixtures::configured_tier(
            &rsm,
            &rlmm,
            fixtures::TierSetup {
                metrics: &metrics,
                ..Default::default()
            },
        );

        // 0..=49 the tier holds; 50..=149 it holds only through 99, and
        // 150..=249 not at all. The store refuses both, so the lag is what the
        // round found waiting rather than what it managed to move.
        let copied = copy_eligible(
            &tier,
            &tp(),
            2,
            LeaderEpoch(1),
            misaligned_exports(MisalignedExportSetup {
                sizes: [bytes(64), bytes(32), bytes(16)],
            }),
        )
        .await;

        check!(copied == 0);
        let topic = fixtures::orders_label();
        check!(metrics.remote_copy_lag_segments.get_or_create(&topic).get() == 2);
        check!(metrics.remote_copy_lag_bytes.get_or_create(&topic).get() == 48);
        check!(metrics.remote_copy_errors_total.get_or_create(&topic).get() == 2);
    }

    #[tokio::test]
    async fn copy_eligible_retries_a_segment_stuck_in_copy_segment_started() {
        let cases: [(&str, ArchiveMode, Arc<dyn RemoteStorageManager>); 2] = [
            (
                "mutable tier",
                ArchiveMode::Mutable,
                Arc::new(AcceptingRsm { receipt: None }),
            ),
            (
                "write-once archive",
                ArchiveMode::WriteOnce,
                Arc::new(FakeWormArchive::new()),
            ),
        ];
        for (name, archive, rsm) in cases {
            let rlmm = fixtures::in_memory_metadata();
            let abandoned = stuck_started_segment(&rlmm, 0x57c, 0);

            let copied = copy_exports(
                &tier(archive, &rsm, &rlmm),
                vec![synth_export(fixtures::SegmentExportSetup::default())],
            )
            .await;

            check!(copied == 1, "case {name}");
            let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
            let finished: Vec<&RemoteLogSegmentMetadata> = listed
                .iter()
                .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
                .collect();
            check!(finished.len() == 1, "case {name}");
            check!(finished[0].start_offset() == 0, "case {name}");
            check!(
                finished[0].remote_log_segment_id().id != abandoned,
                "case {name}: the retry runs under a fresh segment id"
            );
        }
    }
}
