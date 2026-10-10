//! Unit tests for the remote-retention eviction set and its delete pass.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    CustomMetadata, LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId,
    RemoteLogSegmentMetadataUpdate, RemoteStorageError, RemoteStorageManager,
};
use krabka_units::{bytes, hours, millis};
use uuid::Uuid;

use super::*;
use crate::{
    remote_log_manager::{
        test_support as fixtures,
        test_support::{
            FakeWormArchive, archived_backends, copy_exports, local_backends, missing_remote_reads,
            seed_finished_segments, synth_export, tier, tp,
        },
    },
    test_support::UnixMillis,
    time_util::now_ms,
};

/// A partition whose `DeleteRecords` floor has never moved: offset 0, so no
/// segment breaches it and the case under test is the only axis in play.
const NO_FLOOR: Offset = Offset(0);

/// Bounds for a fixture with no local bytes or leader-epoch restriction.
fn empty_local_bounds(
    config: &LogConfig,
    log_start_offset: Offset,
    deleted_below: Option<Offset>,
    now_ms: i64,
) -> RemoteRetentionBounds<'_> {
    RemoteRetentionBounds {
        log_config: config,
        log_start_offset,
        deleted_below,
        now_ms,
        local: LocalLogFootprint::EMPTY,
        earliest_epoch: None,
    }
}

/// An RSM that refuses every delete the way a WORM backend does, and
/// counts how many times it was asked. Modelled on [`AlwaysFailRsm`],
/// with the failure moved from the copy to the delete.
#[derive(Default)]
struct RefusesDeleteRsm {
    deletes_attempted: std::sync::atomic::AtomicUsize,
}

impl RemoteStorageManager for RefusesDeleteRsm {
    fn copy_log_segment_data(
        &self,
        _metadata: &RemoteLogSegmentMetadata,
        _data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        Ok(None)
    }
    missing_remote_reads!();
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.deletes_attempted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(RemoteStorageError::Worm(
            krabka_remote_storage::WormError::DeleteRefused {
                key: format!("{}.log", metadata.remote_log_segment_id().id),
            },
        ))
    }
}

fn synth_remote_md(
    id: u128,
    start: i64,
    end: i64,
    max_ts: i64,
    size: i32,
) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        start,
        end,
        max_ts,
        1,
        max_ts,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => start},
        ),
    )
    .unwrap()
    .with_update(&RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
        event_timestamp_ms: max_ts,
        custom_metadata: None,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 1,
    })
    .unwrap()
}

#[test]
fn remote_retention_eviction_set_returns_empty_when_no_segments() {
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &[],
        Some(millis(1)),
        Some(bytes(1)),
        None,
        10_000,
        NO_BYTES,
    );
    assert!(out.is_empty());
}

/// Kafka's `isSegmentBreachedByRetentionTime`: a segment goes when its
/// `maxTimestampMs` is strictly below `cleanupUntilMs = now - retention.ms`,
/// whatever that timestamp is, and there is no time axis at all while
/// `cleanupUntilMs` is negative. `now` is 10 000 ms throughout.
#[test]
fn remote_time_retention_follows_kafkas_cleanup_until() {
    for (name, max_ts, retention_ms, deleted) in [
        ("an unknown timestamp is older than any cutoff", -1, 1, true),
        ("one below the cutoff goes", 8_999, 1_000, true),
        ("one on the cutoff stays", 9_000, 1_000, false),
        (
            "a window longer than the clock reading deletes nothing",
            -1,
            10_001,
            false,
        ),
        (
            "a window the clock reading just covers still applies",
            -1,
            10_000,
            true,
        ),
    ] {
        let segments = vec![synth_remote_md(10, 0, 9, max_ts, 100)];
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            Some(millis(retention_ms)),
            None,
            None,
            10_000,
            NO_BYTES,
        );
        check!(out.len() == usize::from(deleted), "{name}");
    }
}

/// Kafka's `buildRetentionSizeData` counts the local bytes the remote tier
/// does not hold yet (`onlyLocalLogSegmentsSize`) toward `retention.bytes`,
/// so they push the oldest remote segment out. Two 100-byte remote segments
/// under a 200-byte budget.
#[test]
fn local_bytes_not_yet_copied_count_toward_the_remote_size_budget() {
    let segments = vec![
        synth_remote_md(10, 0, 9, 0, 100),
        synth_remote_md(11, 10, 19, 0, 100),
    ];
    for (name, only_local, deleted) in [
        ("nothing local keeps both", 0, 0),
        ("less than a segment of local bytes keeps both", 99, 0),
        (
            "a segment's worth of local bytes pushes the oldest out",
            100,
            1,
        ),
    ] {
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            None,
            Some(bytes(200)),
            None,
            10_000,
            bytes(only_local),
        );
        check!(out.len() == deleted, "{name}");
    }
}

/// Kafka's `onlyLocalLogSegmentsSize`: a local segment counts when its base
/// offset is above `highestOffsetInRemoteStorage`, and the active segment,
/// never copied, always counts. Sealed segments at 0 and 10 of 30 bytes each,
/// and a 40-byte active segment, so a 100-byte local log.
#[test]
fn only_local_size_counts_the_segments_above_the_highest_remote_offset() {
    let sealed = [
        synth_export(fixtures::SegmentExportSetup {
            timestamp: UnixMillis(0),
            size: bytes(30),
            ..Default::default()
        }),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(10)..=Offset(19),
            timestamp: UnixMillis(0),
            size: bytes(30),
        }),
    ];
    let local = LocalLogFootprint {
        sealed: &sealed,
        size: bytes(100),
    };
    for (name, highest, expected) in [
        ("an empty tier leaves everything local", None, 100),
        ("a copy through 9 covers the first segment", Some(9), 70),
        (
            "a copy that ends inside a segment covers the one it starts in",
            Some(12),
            40,
        ),
        ("a copy through 19 leaves the active segment", Some(19), 40),
        ("a copy below the first base covers nothing", Some(-1), 100),
    ] {
        check!(local.only_local_size(highest) == bytes(expected), "{name}");
    }
}

#[test]
fn maximum_retention_window_keeps_the_host_time_comparison() {
    let segments = vec![synth_remote_md(10, 0, 9, 0, 100)];

    check!(
        remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segments,
            Some(Time::from_millis(i64::MAX)),
            None,
            None,
            i64::MAX,
            NO_BYTES,
        )
        .is_empty()
    );
}

/// The canonical 500ms window at time 10,000, with an optional explicit breach.
fn time_window_evictions(
    segments: &[RemoteLogSegmentMetadata],
    deleted_below: Option<Offset>,
) -> Vec<RemoteLogSegmentMetadata> {
    remote_retention_eviction_set(
        ArchiveMode::Mutable,
        segments,
        Some(millis(500)),
        None,
        deleted_below,
        10_000,
        NO_BYTES,
    )
}

#[test]
fn remote_retention_eviction_set_time_based_picks_oldest_until_first_in_window() {
    let segs = retention_segments([100, 200, 9_500]);
    // now=10_000, retention=500ms → seg with max_ts < 9_500 is deletable.
    // seg0 (100) + seg1 (200) qualify; seg2 (9_500) stops the walk.
    let out = time_window_evictions(&segs, None);
    assert!(out.len() == 2);
    check!(out[0].start_offset() == 0);
    check!(out[1].start_offset() == 10);
}

/// Kafka's `isSegmentBreachedByRetentionSize` deletes a segment only while
/// the bytes over `retention.bytes` still cover the whole segment. Three
/// 100-byte segments make 300 bytes in total.
#[test]
fn remote_retention_eviction_set_size_based_deletes_only_what_the_debt_covers() {
    let segs = retention_segments([100, 200, 300]);
    let cases = [
        ("150 over deletes one, not two", Some(bytes(150)), 1),
        ("200 over deletes two", Some(bytes(100)), 2),
        ("250 over deletes two, not three", Some(bytes(50)), 2),
        ("50 over deletes nothing", Some(bytes(250)), 0),
        ("300 over deletes all three", Some(bytes(0)), 3),
        ("under budget deletes nothing", Some(bytes(10_000)), 0),
    ];
    for (name, budget, expected_len) in cases {
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            None,
            budget,
            None,
            1_000,
            NO_BYTES,
        );
        check!(out.len() == expected_len, "{name}");
    }
}

#[test]
fn remote_retention_eviction_set_equal_size_budget_keeps_all_segments() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        None,
        Some(bytes(100)),
        None,
        1_000,
        NO_BYTES,
    );
    assert!(out.is_empty());
}

#[test]
fn remote_retention_eviction_set_time_and_size_take_union_of_either() {
    let segs = retention_segments([100, 200, 5_000]);
    // Time-window: seg0+seg1 qualify (max_ts<500). Budget very generous
    // so size-based evicts nothing. Result is the time-window prefix.
    let out = remote_retention_eviction_set(
        ArchiveMode::Mutable,
        &segs,
        Some(millis(500)),
        Some(bytes(10_000)),
        None,
        1_000,
        NO_BYTES,
    );
    assert!(out.len() == 2);
}

/// Kafka's `isSegmentBreachedByRetentionTime` charges a time-expired segment
/// against the size debt, so the size axis sees the bytes the time axis
/// already reclaimed. The oldest segment is past a 500 ms window and the two
/// after it are inside it. Three 100-byte segments make 300 bytes in total.
#[test]
fn a_time_deletion_is_charged_against_the_size_debt() {
    let segs = retention_segments([100, 9_900, 9_900]);
    let cases = [
        ("150 over: time takes 100, 50 cannot cover the next", 150, 1),
        ("200 over: time takes 100, size takes the next", 100, 2),
        ("50 over: time alone, the debt floors at zero", 250, 1),
    ];
    for (name, budget, expected_len) in cases {
        let out = remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            Some(millis(500)),
            Some(bytes(budget)),
            None,
            10_000,
            NO_BYTES,
        );
        check!(out.len() == expected_len, "{name}");
    }
}

#[test]
fn remote_retention_eviction_set_none_settings_disable_axis() {
    let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
    // No time or size → no eviction.
    assert!(
        remote_retention_eviction_set(
            ArchiveMode::Mutable,
            &segs,
            None,
            None,
            None,
            10_000,
            NO_BYTES
        )
        .is_empty()
    );
}

#[test]
fn remote_retention_eviction_set_walk_stops_at_first_non_deletable() {
    let segs = vec![
        synth_remote_md(10, 0, 9, 100, 100),     // deletable by time
        synth_remote_md(11, 10, 19, 9_500, 100), // in window → stops walk
        synth_remote_md(12, 20, 29, 200, 100),   // also deletable by time, but
                                                 // walk stopped at seg1 already.
    ];
    let out = time_window_evictions(&segs, None);
    assert!(out.len() == 1);
    assert!(out[0].start_offset() == 0);
}

#[tokio::test]
async fn remote_retention_pass_evicts_old_segments_through_lifecycle() {
    fixtures::rolled_log_fixture!(log_dir, remote_dir, log, exports);
    let (rsm, rlmm) = archived_backends(remote_dir.path(), &exports).await;
    let pre = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert!(!pre.is_empty());

    // far-future `now_ms` → every finished segment is past the window.
    let outcome = expire_remote_segments(ArchiveMode::Mutable, &rsm, &rlmm).await;
    assert!(outcome.deleted == exports.len());
    // Every remote copy is gone, so the global floor moves past the last of
    // them and `ListOffsets(earliest)` follows it.
    assert!(
        outcome.log_start
            == Some(Offset(
                pre.iter()
                    .map(RemoteLogSegmentMetadata::end_offset)
                    .max()
                    .unwrap()
                    + 1
            ))
    );

    // DeleteSegmentFinished drops the entries entirely from the cache.
    let post = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert!(
        post.is_empty(),
        "every segment should be gone, got {} left",
        post.len()
    );
    // RSM data is gone too.
    for md in &pre {
        assert!(rsm.fetch_log_segment(md, 0, None).is_err());
    }
}

#[tokio::test]
async fn remote_retention_pass_noop_when_nothing_qualifies() {
    fixtures::rolled_log_fixture!(log_dir, remote_dir, log, exports);
    let (rsm, rlmm) = local_backends(remote_dir.path());
    copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports.clone()).await;

    let cfg = LogConfig {
        // Long retention; nothing is past the window.
        retention: Some(hours(8_760)),
        retention_size: None,
        ..LogConfig::default()
    };
    // Use a `now_ms` close to the segments' max_timestamp so the test
    // is independent of wall-clock. `rolled_log` builds batches with
    // default base_timestamp=0, so picking now=1 keeps every segment
    // inside the year-long retention window.
    let outcome =
        empty_retention_pass(&cfg, (NO_FLOOR, None), 1, ArchiveMode::Mutable, &rsm, &rlmm).await;
    assert!(outcome == RemoteRetentionOutcome::default());
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
}

/// With no retention settings and an unmoved floor there is nothing to
/// evict, but the pass no longer returns before it lists: the log-start
/// breach is an axis of its own and a `DeleteRecords` has to be able to free
/// remote bytes on a topic that keeps its records forever.
#[tokio::test]
async fn remote_retention_pass_no_settings_and_an_unmoved_floor_evict_nothing() {
    let remote_dir = tempfile::tempdir().unwrap();
    let (rsm, rlmm) = local_backends(remote_dir.path());
    seed_finished_segments(&rlmm, 3);
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let outcome = empty_retention_pass(
        &cfg,
        (NO_FLOOR, None),
        now_ms(),
        ArchiveMode::Mutable,
        &rsm,
        &rlmm,
    )
    .await;
    assert!(outcome == RemoteRetentionOutcome::default());
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == 3);
}

#[test]
fn remote_retention_eviction_set_is_empty_for_a_write_once_archive() {
    let segs = retention_segments([100, 200, 300]);
    // The `mutable_len` column keeps the fixture honest: an empty result
    // under `WriteOnce` only means something if the very same inputs do
    // evict on a mutable tier.
    let cases = [
        ("time window past for all", Some(millis(1)), None, 10_000, 3),
        (
            "time window past for a prefix",
            Some(millis(9_750)),
            None,
            10_000,
            2,
        ),
        (
            "size budget below one segment",
            None,
            Some(bytes(50)),
            1_000,
            2,
        ),
        (
            "size budget of half the total",
            None,
            Some(bytes(150)),
            1_000,
            1,
        ),
        (
            "time and size together",
            Some(millis(1)),
            Some(bytes(150)),
            10_000,
            3,
        ),
    ];
    for (name, retention, retention_size, now, mutable_len) in cases {
        check!(
            remote_retention_eviction_set(
                ArchiveMode::Mutable,
                &segs,
                retention,
                retention_size,
                None,
                now,
                NO_BYTES,
            )
            .len()
                == mutable_len,
            "case {name}"
        );
        check!(
            remote_retention_eviction_set(
                ArchiveMode::WriteOnce,
                &segs,
                retention,
                retention_size,
                None,
                now,
                NO_BYTES,
            )
            .is_empty(),
            "case {name}"
        );
    }
}

#[tokio::test]
async fn remote_retention_pass_never_reaches_the_rsm_for_a_write_once_archive() {
    let rlmm = fixtures::in_memory_metadata();
    seed_finished_segments(&rlmm, 3);
    // `FakeWormArchive::delete_log_segment_data` panics.
    let rsm: Arc<dyn RemoteStorageManager> = Arc::new(FakeWormArchive::new());
    let cfg = LogConfig {
        retention: Some(millis(1)),
        retention_size: Some(bytes(1)),
        ..LogConfig::default()
    };

    let outcome = empty_retention_pass(
        &cfg,
        (NO_FLOOR, None),
        now_ms() + 1_000_000,
        ArchiveMode::WriteOnce,
        &rsm,
        &rlmm,
    )
    .await;

    check!(outcome == RemoteRetentionOutcome::default());
    // Not even the metadata lifecycle moved: the pass returns before it
    // lists, so a 30-second tick over a WORM partition costs nothing.
    let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(listed.len() == 3);
    check!(
        listed
            .iter()
            .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
    );
}

#[tokio::test]
async fn remote_retention_pass_reaches_a_refusing_rsm_only_on_a_mutable_tier() {
    let cases = [
        ("mutable tier asks and is refused", ArchiveMode::Mutable, 1),
        ("write-once archive never asks", ArchiveMode::WriteOnce, 0),
    ];
    for (name, archive, expected_attempts) in cases {
        let (rlmm, rsm_impl, rsm) = refusing_backends();

        let outcome = expire_remote_segments(archive, &rsm, &rlmm).await;

        check!(outcome == RemoteRetentionOutcome::default(), "case {name}");
        check!(
            rsm_impl
                .deletes_attempted
                .load(std::sync::atomic::Ordering::Relaxed)
                == expected_attempts,
            "case {name}"
        );
    }
}

/// The log-start breach is its own eviction axis (Kafka's
/// `deleteLogStartOffsetBreachedSegments`): a finished segment whose whole
/// offset range fell below the partition's `log_start_offset` goes, whatever
/// `retention.ms` and `retention.bytes` say -- including when both are unset,
/// which is exactly the topic where a `DeleteRecords` used to leave the
/// remote copies listed, fetchable and billed forever.
///
/// The floor is compared against `end_offset`, so a segment the floor lands
/// inside keeps its remaining records and stops the walk.
#[test]
fn a_segment_below_the_log_start_is_evicted_whatever_retention_says() {
    // Three ten-record segments: [0, 9], [10, 19], [20, 29].
    let segs = retention_segments([9_500, 9_500, 9_500]);
    // now=10_000 against max_ts=9_500 keeps every segment inside a 500ms
    // window, so the time axis never fires and a non-empty result is the
    // breach axis alone.
    let cases = [
        ("no floor at all, no settings", None, None, None, 0),
        (
            "no floor at all, time window open",
            None,
            Some(millis(500)),
            None,
            0,
        ),
        (
            "a floor at zero breaches nothing",
            Some(NO_FLOOR),
            None,
            None,
            0,
        ),
        (
            "breach only, no settings at all",
            Some(Offset(20)),
            None,
            None,
            2,
        ),
        (
            "breach only, generous settings",
            Some(Offset(20)),
            Some(millis(500)),
            Some(bytes(10_000)),
            2,
        ),
        (
            "the floor inside a segment keeps it",
            Some(Offset(15)),
            None,
            None,
            1,
        ),
        (
            "the floor at a segment's end offset keeps it",
            Some(Offset(9)),
            None,
            None,
            0,
        ),
        (
            "a floor past every segment takes them all",
            Some(Offset(30)),
            None,
            None,
            3,
        ),
    ];
    for (name, floor, retention, retention_size, expected) in cases {
        check!(
            remote_retention_eviction_set(
                ArchiveMode::Mutable,
                &segs,
                retention,
                retention_size,
                floor,
                10_000,
                NO_BYTES,
            )
            .len()
                == expected,
            "case {name}"
        );
        check!(
            remote_retention_eviction_set(
                ArchiveMode::WriteOnce,
                &segs,
                retention,
                retention_size,
                floor,
                10_000,
                NO_BYTES,
            )
            .is_empty(),
            "case {name}: a write-once archive has no delete to give"
        );
    }
}

/// Breach and time window together take the union, and the union is still a
/// contiguous prefix: a segment the time axis would have skipped over is
/// covered by the breach, so the walk does not stop short of it.
#[test]
fn the_breach_and_the_time_window_take_the_union_of_either() {
    let segs = retention_segments([
        9_500, // in the time window; breached
        100,   // past the time window
        9_500, // in the window, not breached
    ]);
    let out = time_window_evictions(&segs, Some(Offset(10)));
    assert!(out.len() == 2);
    check!(out[0].start_offset() == 0, "breached");
    check!(out[1].start_offset() == 10, "past the time window");
}

/// Archive a real rolled prefix while keeping the source log and directories alive.
macro_rules! archived_prefix_fixture {
    ($local:ident, $remote:ident, $log:ident, $exports:ident, $rsm:ident, $rlmm:ident) => {
        fixtures::rolled_log_fixture!($local, $remote, $log, $exports);
        assert!($exports.len() >= 2, "the test needs a prefix to evict");
        let ($rsm, $rlmm) = archived_backends($remote.path(), &$exports).await;
    };
}

/// A breach eviction frees the archive and leaves the floor where the
/// operator put it.
///
/// The reported floor is an *advance*, and a breach can never produce one: it
/// removes only what is already below the floor. A time or size eviction is
/// the case that moves it, which
/// [`remote_retention_pass_evicts_old_segments_through_lifecycle`] covers.
#[tokio::test]
async fn a_breach_eviction_frees_the_archive_without_moving_the_floor() {
    archived_prefix_fixture!(log_dir, remote_dir, log, exports, rsm, rlmm);

    // A `DeleteRecords` floor one past the oldest copied segment, and a topic
    // that keeps its records forever: the breach is the only axis that can
    // evict anything here.
    let floor = exports[0].last_offset + 1;
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };

    let outcome = empty_retention_pass(
        &cfg,
        (floor, Some(floor)),
        1,
        ArchiveMode::Mutable,
        &rsm,
        &rlmm,
    )
    .await;

    check!(outcome.deleted == 1);
    check!(
        outcome.log_start == None,
        "everything it removed was already under the floor"
    );
    let left = rlmm.list_remote_log_segments(&tp()).unwrap();
    check!(left.len() == exports.len() - 1);
    check!(
        left.iter().all(|md| md.end_offset() >= floor.0),
        "only the breached prefix goes"
    );
}

/// The floor may only cross a run of segments the pass removed without a gap.
///
/// `copy_eligible` skips a segment whose copy failed and carries on with the
/// next one, so the finished list is not always a contiguous offset prefix.
/// Publishing the last delete's end offset over such a gap would put the floor
/// above a segment that is still on local disk and still readable.
#[tokio::test]
async fn the_reported_floor_stops_at_a_gap_in_the_finished_segments() {
    fixtures::rolled_log_fixture!(log_dir, remote_dir, log, exports);
    assert!(exports.len() >= 3, "the test needs a segment to skip over");
    let (rsm, rlmm) = local_backends(remote_dir.path());
    // Copy the first and the third segment and not the second, which is what
    // a failed copy in the middle of a tick leaves behind.
    let gapped = vec![exports[0].clone(), exports[2].clone()];
    let copied = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), gapped).await;
    assert!(copied == 2);

    // Time retention past every segment, so both finished copies are
    // deletable and the walk reaches the far side of the gap.
    let outcome = expire_remote_segments(ArchiveMode::Mutable, &rsm, &rlmm).await;

    check!(outcome.deleted == 2, "both copies are past the window");
    check!(
        outcome.log_start == Some(exports[0].last_offset + 1),
        "the floor stops below the segment that was never copied"
    );
}

/// A partition reopened after its local segments were evicted keeps its
/// archive.
///
/// `Log::open` infers a `log_start_offset` from the segments that survived on
/// disk, and on a tiered partition that inference sits above the whole
/// archive. If the breach axis measured against it, the first tick after every
/// restart would delete every remote segment the partition has. The axis
/// measures against the floor someone actually deleted up to, which a reopened
/// log reports as `None`.
#[tokio::test]
async fn a_floor_nobody_moved_leaves_the_archive_alone() {
    archived_prefix_fixture!(log_dir, remote_dir, log, exports, rsm, rlmm);

    // What a restart leaves behind: a `log_start_offset` past every copied
    // segment, and nothing saying anyone deleted up to it.
    let inferred = exports[exports.len() - 1].last_offset + 1;
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let outcome =
        empty_retention_pass(&cfg, (inferred, None), 1, ArchiveMode::Mutable, &rsm, &rlmm).await;

    check!(outcome.deleted == 0, "an inferred floor deletes nothing");
    check!(outcome.log_start == None);
    check!(
        rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len(),
        "the archive is intact"
    );
}

/// KIP-405's `RemoteDeleteRequestsPerSec`, `RemoteDeleteErrorsPerSec` and the
/// two delete-lag gauges. Without them an archive whose deletes are failing
/// shows nothing at all: the pass stops at the first failure and retries on
/// the next tick, and object-store spend is the only other symptom.
#[tokio::test]
async fn a_retention_pass_records_its_delete_requests_errors_and_lag() {
    let (rlmm, _rsm_impl, rsm) = refusing_backends();
    let metrics = crate::metrics::BrokerMetrics::new();
    let index_cache = Arc::new(krabka_remote_storage::RemoteIndexCache::disabled());
    let tier = fixtures::configured_tier(
        &rsm,
        &rlmm,
        fixtures::TierSetup {
            metrics: &metrics,
            index_cache: &index_cache,
            ..Default::default()
        },
    );
    let cfg = LogConfig {
        retention: Some(millis(1)),
        ..LogConfig::default()
    };

    let outcome = remote_retention_pass(
        &tp(),
        1,
        empty_local_bounds(&cfg, NO_FLOOR, None, now_ms() + 1_000_000),
        &tier,
    )
    .await;

    check!(outcome.deleted == 0);
    let topic = fixtures::orders_label();
    // The lag is what the pass decided to remove, recorded before it tried.
    check!(
        metrics
            .remote_delete_lag_segments
            .get_or_create(&topic)
            .get()
            == 3
    );
    // The pass stops at the first failure to keep the contiguous-prefix
    // invariant, so exactly one attempt and one error are recorded.
    check!(
        metrics
            .remote_delete_requests_total
            .get_or_create(&topic)
            .get()
            == 1
    );
    check!(
        metrics
            .remote_delete_errors_total
            .get_or_create(&topic)
            .get()
            == 1
    );
}

async fn empty_retention_pass(
    config: &LogConfig,
    floors: (Offset, Option<Offset>),
    now_ms: i64,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> RemoteRetentionOutcome {
    remote_retention_pass(
        &tp(),
        1,
        empty_local_bounds(config, floors.0, floors.1, now_ms),
        &tier(archive, rsm, rlmm),
    )
    .await
}

fn refusing_backends() -> (
    Arc<dyn RemoteLogMetadataManager>,
    Arc<RefusesDeleteRsm>,
    Arc<dyn RemoteStorageManager>,
) {
    let rlmm = fixtures::in_memory_metadata();
    seed_finished_segments(&rlmm, 3);
    let rsm_impl = Arc::new(RefusesDeleteRsm::default());
    let rsm: Arc<dyn RemoteStorageManager> = rsm_impl.clone();
    (rlmm, rsm_impl, rsm)
}

fn retention_segments(timestamps: [i64; 3]) -> Vec<RemoteLogSegmentMetadata> {
    vec![
        synth_remote_md(10, 0, 9, timestamps[0], 100),
        synth_remote_md(11, 10, 19, timestamps[1], 100),
        synth_remote_md(12, 20, 29, timestamps[2], 100),
    ]
}

async fn expire_remote_segments(
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> RemoteRetentionOutcome {
    let config = LogConfig {
        retention: Some(millis(1)),
        ..LogConfig::default()
    };
    empty_retention_pass(
        &config,
        (NO_FLOOR, None),
        now_ms() + 1_000_000,
        archive,
        rsm,
        rlmm,
    )
    .await
}
