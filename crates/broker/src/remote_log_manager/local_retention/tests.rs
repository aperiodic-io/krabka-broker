//! Unit tests for local-retention eviction: the deletion walk, the roll of a
//! breached active segment, and the pass that applies both to a real log.

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_log::Log;
use krabka_units::{bytes, millis};

use super::*;
use crate::{
    remote_log_manager::{
        ArchiveMode, test_support as fixtures,
        test_support::{
            archived_backends, batch, copy_exports, leading_partition_over, local_backends,
            partition_log_fixture, partition_snapshot, rolled_tiered_partition_with_config,
            synth_export, three_exports, tier, tp,
        },
    },
    test_support::UnixMillis,
    time_util::now_ms,
};

// The sealed segments of `log` as `(base, last)` offset ranges.
/// The sealed segments a copy would see. A rolled segment is tierable once
/// its rollover flush publishes the boundary snapshot, so this waits for the
/// flush first.
fn sealed_ranges(log: &mut Log) -> Vec<(Offset, Offset)> {
    log.sync().expect("flush rolled segments");
    log.tierable_segments()
        .iter()
        .map(|export| (export.base_offset, export.last_offset))
        .collect()
}

/// A rolled, tiered partition whose sealed segments expire after one millisecond.
fn short_retention_partition(
    log_dir: &std::path::Path,
) -> std::sync::Arc<crate::partition::Partition> {
    rolled_tiered_partition_with_config(
        log_dir,
        LogConfig {
            segment_size: bytes(256),
            remote_storage_enable: true,
            local_retention: Some(millis(1)),
            ..LogConfig::default()
        },
    )
}

// One `local_retention_decision` case over a log whose active segment
// starts at offset 20.
struct RollCase {
    label: &'static str,
    sealed: Vec<SegmentExport>,
    covered_through: Option<i64>,
    // The active segment's newest timestamp and size, or `None` when the
    // high watermark has not passed the log end.
    active: Option<(i64, u32)>,
    retention: Option<Time>,
    retention_size: Option<ByteSize>,
    expected: LocalRetentionDecision,
}

// Kafka's `UnifiedLog.deletableSegments` on a tiered log walks the active
// segment last. It never deletes it, because the remote tier never holds
// it, but when the walk reaches it and the time or size predicate holds
// for it, Kafka rolls it so the next copy can upload it. `now` is
// 10 000 ms.
#[test]
fn the_walk_rolls_a_breached_active_segment_only_where_kafka_would() {
    let two_sealed = || fixtures::two_exports_with_size(bytes(100));
    let cases = [
        RollCase {
            label: "a lone active segment past the window rolls",
            sealed: vec![],
            covered_through: None,
            active: Some((100, 50)),
            retention: Some(millis(1_000)),
            retention_size: None,
            expected: LocalRetentionDecision {
                delete_through: None,
                roll_active: true,
            },
        },
        RollCase {
            label: "a lone active segment inside the window stays",
            sealed: vec![],
            covered_through: None,
            active: Some((9_500, 50)),
            retention: Some(millis(1_000)),
            retention_size: None,
            expected: LocalRetentionDecision {
                delete_through: None,
                roll_active: false,
            },
        },
        RollCase {
            label: "an empty active segment never rolls",
            sealed: vec![],
            covered_through: None,
            active: Some((100, 0)),
            retention: Some(millis(1_000)),
            retention_size: None,
            expected: LocalRetentionDecision {
                delete_through: None,
                roll_active: false,
            },
        },
        RollCase {
            label: "an active segment above the high watermark never rolls",
            sealed: vec![],
            covered_through: None,
            active: None,
            retention: Some(millis(1_000)),
            retention_size: None,
            expected: LocalRetentionDecision {
                delete_through: None,
                roll_active: false,
            },
        },
        timed_active_case(
            "copied, breached sealed segments go and the active one rolls",
            two_sealed(),
            Some(19),
            LocalRetentionDecision {
                delete_through: Some(20),
                roll_active: true,
            },
        ),
        timed_active_case(
            "a sealed segment the tier does not hold stops the walk",
            two_sealed(),
            Some(9),
            LocalRetentionDecision {
                delete_through: Some(10),
                roll_active: false,
            },
        ),
        timed_active_case(
            "a sealed segment inside the window stops the walk",
            vec![
                synth_export(fixtures::SegmentExportSetup {
                    size: bytes(100),
                    ..Default::default()
                }),
                synth_export(fixtures::SegmentExportSetup {
                    bounds: Offset(10)..=Offset(19),
                    timestamp: UnixMillis(9_500),
                    size: bytes(100),
                }),
            ],
            Some(19),
            LocalRetentionDecision {
                delete_through: Some(10),
                roll_active: false,
            },
        ),
        RollCase {
            label: "a size debt that covers the active segment rolls it",
            sealed: two_sealed(),
            covered_through: Some(19),
            active: Some((9_500, 50)),
            retention: None,
            retention_size: Some(bytes(0)),
            expected: LocalRetentionDecision {
                delete_through: Some(20),
                roll_active: true,
            },
        },
        RollCase {
            label: "a size debt short of the active segment keeps it",
            sealed: two_sealed(),
            covered_through: Some(19),
            active: Some((9_500, 50)),
            retention: None,
            retention_size: Some(bytes(50)),
            expected: LocalRetentionDecision {
                delete_through: Some(20),
                roll_active: false,
            },
        },
    ];
    for case in cases {
        let active_size = case.active.map_or(0, |(_, size)| size);
        let sealed_size: u32 = case
            .sealed
            .iter()
            .map(|ex| u32::try_from(ex.size.bytes_u64()).unwrap())
            .sum();
        let local = LocalSegments {
            sealed: &case.sealed,
            active: case
                .active
                .map(|(max_timestamp, size)| ActiveSegmentExport {
                    base_offset: Offset(20),
                    max_timestamp,
                    last_modified_ms: max_timestamp,
                    size: bytes(size),
                }),
            size: bytes(sealed_size + active_size),
        };
        check!(
            local_retention_decision(
                UnstableApiVersions::Disabled,
                &local,
                case.covered_through,
                case.retention,
                case.retention_size,
                10_000,
            ) == case.expected,
            "{}",
            case.label
        );
    }
}

#[test]
fn local_retention_target_returns_none_when_no_finished_segments() {
    let exports = fixtures::two_exports();
    // Big enough time-pressure to delete everything, but the remote tier
    // covers nothing.
    assert!(
        local_retention_target(&exports, None, Some(millis(1)), None, bytes(128), 10_000) == None
    );
}

/// Kafka 4.3.1's `UnifiedLog.deleteRetentionMsBreachedSegments` ages a
/// segment by its `largestTimestamp()` alone: one whose records claim a
/// timestamp in the future is never time-expired, and the log only says it
/// is "ineligible to be deleted". Kafka trunk (KAFKA-20609) ages such a
/// segment of a tiered topic by its file's `lastModified()` instead, and
/// only under `unstable.api.versions.enable` does krabka follow it. `now`
/// is 10 000 ms and the window 1 ms; the sealed segment is fully copied,
/// and the active segment after it carries the same timestamps, so the
/// roll follows the same anchor as the deletion.
///
/// Each case is `(name, newest timestamp, file last modified, whether the
/// segments expire in the default mode, whether they expire under the
/// unstable flag)`.
#[test]
fn a_future_timestamp_is_aged_by_the_file_only_under_the_unstable_flag() {
    for (name, max_timestamp, last_modified_ms, default_expires, unstable_expires) in [
        (
            "a past timestamp ages the segment either way",
            100,
            100,
            true,
            true,
        ),
        (
            "a future timestamp with an old file",
            20_000,
            100,
            false,
            true,
        ),
        (
            "a future timestamp with a young file",
            20_000,
            9_999,
            false,
            false,
        ),
    ] {
        let exports = vec![SegmentExport {
            last_modified_ms,
            ..synth_export(fixtures::SegmentExportSetup {
                timestamp: UnixMillis(max_timestamp),
                size: bytes(100),
                ..Default::default()
            })
        }];
        let local = LocalSegments {
            sealed: &exports,
            active: Some(ActiveSegmentExport {
                base_offset: Offset(10),
                max_timestamp,
                last_modified_ms,
                size: bytes(50),
            }),
            size: bytes(150),
        };
        for (unstable, expires) in [
            (UnstableApiVersions::Disabled, default_expires),
            (UnstableApiVersions::Enabled, unstable_expires),
        ] {
            check!(
                local_retention_decision(unstable, &local, Some(9), Some(millis(1)), None, 10_000)
                    == LocalRetentionDecision {
                        delete_through: expires.then_some(10),
                        roll_active: expires,
                    },
                "{name}: {unstable:?}"
            );
        }
    }
}

#[test]
fn maximum_retention_window_keeps_the_host_time_comparison() {
    let exports = vec![synth_export(fixtures::SegmentExportSetup {
        timestamp: UnixMillis(0),
        size: bytes(100),
        ..Default::default()
    })];

    check!(
        local_retention_target(
            &exports,
            Some(9),
            Some(Time::from_millis(i64::MAX)),
            None,
            bytes(100),
            i64::MAX,
        ) == None
    );
}

#[test]
fn local_retention_target_time_based_eviction() {
    let exports = vec![
        synth_export(fixtures::SegmentExportSetup::default()),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(10)..=Offset(19),
            timestamp: UnixMillis(200),
            ..Default::default()
        }),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(20)..=Offset(29),
            timestamp: UnixMillis(5_000),
            ..Default::default()
        }),
    ];
    // now=1000, retention=500ms → segs with max_ts<500 are deletable.
    // Only seg0 (max_ts=100) and seg1 (max_ts=200) qualify; seg2 stops it.
    let target = local_retention_target(
        &exports,
        Some(29),
        Some(millis(500)),
        None,
        bytes(192),
        1_000,
    );
    assert!(target == Some(20));
}

/// Kafka's `deleteRetentionSizeBreachedSegments` on a tiered log: an oldest
/// segment goes only while the local log's bytes over
/// `local.retention.bytes` still cover the whole segment, and the active
/// segment counts toward the local log's size. Three 100-byte sealed
/// segments and a 50-byte active segment make a 350-byte local log.
#[test]
fn local_retention_target_size_based_eviction() {
    let exports = vec![
        synth_export(fixtures::SegmentExportSetup {
            size: bytes(100),
            ..Default::default()
        }),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(10)..=Offset(19),
            timestamp: UnixMillis(200),
            size: bytes(100),
        }),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(20)..=Offset(29),
            timestamp: UnixMillis(300),
            size: bytes(100),
        }),
    ];
    let cases = [
        ("200 over deletes two", bytes(150), Some(20)),
        ("150 over deletes one, not two", bytes(200), Some(10)),
        ("the active segment's bytes count", bytes(250), Some(10)),
        ("50 over deletes nothing", bytes(300), None),
        ("exactly at budget deletes nothing", bytes(350), None),
        ("300 over deletes all three", bytes(50), Some(30)),
        ("under budget deletes nothing", bytes(10_000), None),
    ];
    for (name, budget, expected) in cases {
        let target =
            local_retention_target(&exports, Some(29), None, Some(budget), bytes(350), 1_000);
        check!(target == expected, "{name}");
    }
}

#[test]
fn local_retention_target_equal_size_budget_keeps_all_segments() {
    let exports = fixtures::two_exports_with_size(bytes(100));
    let target = local_retention_target(
        &exports,
        Some(19),
        None,
        Some(bytes(200)),
        bytes(200),
        1_000,
    );
    assert!(target == None);
}

#[test]
fn local_retention_target_skips_unfinished_segments_and_stops() {
    let exports = three_exports();
    // The tier holds 0..=9 and 20..=29 but not 10..=19, so its unbroken
    // cover ends at 9 and the walk stops at seg1.
    let covered = remote_covered_through(&[(0, 9), (20, 29)], 0);
    assert!(covered == Some(9));
    let target =
        local_retention_target(&exports, covered, Some(millis(1)), None, bytes(192), 10_000);
    assert!(
        target == Some(10),
        "only seg0 deletable; walk stops at seg1"
    );
}

/// One [`remote_covered_through`] case: an RLMM listing, the base offset
/// of the replica's oldest local segment, and the bound they imply.
struct CoverCase {
    label: &'static str,
    finished: &'static [(i64, i64)],
    local_start: i64,
    expected: Option<i64>,
}

#[test]
fn remote_covered_through_walks_an_unbroken_prefix() {
    let cases = [
        CoverCase {
            label: "obsolete disconnected prefix cannot suppress the local anchor",
            finished: &[(0, 9), (20, 29)],
            local_start: 20,
            expected: Some(29),
        },
        CoverCase {
            label: "obsolete ranges alone do not cover the local anchor",
            finished: &[(0, 9)],
            local_start: 20,
            expected: None,
        },
        CoverCase {
            label: "nothing copied",
            finished: &[],
            local_start: 0,
            expected: None,
        },
        CoverCase {
            label: "one segment from the local start",
            finished: &[(0, 9)],
            local_start: 0,
            expected: Some(9),
        },
        CoverCase {
            label: "abutting segments join, in any listed order",
            finished: &[(20, 29), (0, 9), (10, 19)],
            local_start: 0,
            expected: Some(29),
        },
        CoverCase {
            label: "a gap stops the walk, and a later segment cannot bridge it",
            finished: &[(0, 9), (20, 29)],
            local_start: 0,
            expected: Some(9),
        },
        CoverCase {
            label: "the tier starts past the oldest local segment",
            finished: &[(10, 19)],
            local_start: 0,
            expected: None,
        },
        CoverCase {
            label: "remote retention dropped the head, but the local log moved too",
            finished: &[(10, 19), (20, 29)],
            local_start: 10,
            expected: Some(29),
        },
    ];
    for case in cases {
        check!(
            remote_covered_through(case.finished, case.local_start) == case.expected,
            "{}",
            case.label
        );
    }
}

#[test]
fn a_local_segment_the_tier_covers_only_in_part_is_not_droppable() {
    // The follower rolled one 0..=199 segment where the leader rolled two,
    // and the leader has copied only the first of them. Reading the remote
    // start offset 0 as "this local segment is copied" would delete
    // 100..=199, which no remote segment holds; a failover in that window
    // would lose acknowledged records.
    let exports = vec![
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(0)..=Offset(199),
            ..Default::default()
        }),
        synth_export(fixtures::SegmentExportSetup {
            bounds: Offset(200)..=Offset(399),
            timestamp: UnixMillis(200),
            ..Default::default()
        }),
    ];
    let covered = remote_covered_through(&[(0, 99)], 0);
    check!(covered == Some(99));
    check!(
        local_retention_target(&exports, covered, Some(millis(1)), None, bytes(128), 10_000)
            == None
    );

    // Once the leader copies 100..=199 too, the follower's first segment is
    // covered whole and goes.
    let covered = remote_covered_through(&[(0, 99), (100, 199)], 0);
    check!(covered == Some(199));
    check!(
        local_retention_target(&exports, covered, Some(millis(1)), None, bytes(128), 10_000)
            == Some(200)
    );
}

#[test]
fn local_retention_target_rejects_exhausted_offset() {
    let exports = vec![synth_export(fixtures::SegmentExportSetup {
        bounds: Offset(0)..=Offset(i64::MAX),
        ..Default::default()
    })];

    assert!(
        local_retention_target(
            &exports,
            Some(i64::MAX),
            Some(millis(1)),
            None,
            bytes(64),
            10_000
        ) == None
    );
}

#[test]
fn local_retention_target_uses_already_resolved_effective_ms() {
    // The pure helper takes already-resolved effective_* args. This test
    // pins that contract: when caller passes effective_local_ms equal to
    // the topic's `retention` (the fallback), the helper deletes the
    // same set as if `local_retention` had been set directly.
    let exports = fixtures::two_exports();
    // Caller resolved effective_local = retention = 250ms; now=1000.
    let target = local_retention_target(
        &exports,
        Some(19),
        Some(millis(250)),
        None,
        bytes(128),
        1_000,
    );
    assert!(target == Some(20));
}

/// Test-only drive helper. It mirrors the body of `local_retention_pass`
/// without the `Partition` wrapper, so the test can exercise the
/// integration against a real `Log` and no broker fixtures.
fn local_retention_drive(
    log: &mut Log,
    finished: &[(i64, i64)],
    log_config: &LogConfig,
    now_ms: i64,
) -> usize {
    let effective_local = log_config.local_retention.or(log_config.retention);
    let effective_local_size = log_config
        .local_retention_size
        .or(log_config.retention_size);
    let exports = log.tierable_segments();
    let Some(local_start) = exports.first().map(|ex| ex.base_offset.0) else {
        return 0;
    };
    let Some(target) = local_retention_target(
        &exports,
        remote_covered_through(finished, local_start),
        effective_local,
        effective_local_size,
        log.size(),
        now_ms,
    ) else {
        return 0;
    };
    log.delete_local_segments_through(Offset(target)).unwrap()
}

#[tokio::test]
async fn local_retention_drive_deletes_copied_segments() {
    let (log_dir, remote_dir) = fixtures::temporary_dirs();
    let mut log = Log::open(
        log_dir.path(),
        LogConfig {
            segment_size: bytes(256),
            remote_storage_enable: true,
            local_retention: Some(millis(1)),
            ..LogConfig::default()
        },
    )
    .unwrap();
    fixtures::append_fixture_batches(&mut log, 12);
    log.sync().expect("flush sealed segments before archiving");
    let exports = log.tierable_segments();
    assert!(exports.len() >= 2, "test needs multiple sealed segments");
    let log_config = log.config_snapshot();

    let (_rsm, rlmm) = archived_backends(remote_dir.path(), &exports).await;

    // Gather finished ranges the same way `local_retention_pass` would.
    let finished: Vec<(i64, i64)> =
        crate::remote_log_manager::local_retention::finished_segment_ranges(
            &rlmm.list_remote_log_segments(&tp()).unwrap(),
        );
    assert!(finished.len() == exports.len());

    // Drive retention with `now_ms` far in the future so every sealed
    // segment satisfies the 1ms time-based eviction.
    let future = now_ms() + 1_000_000;
    let removed = local_retention_drive(&mut log, &finished, &log_config, future);
    assert!(removed == exports.len());

    // local_log_start_offset advanced; sealed log files are gone.
    let last = exports.last().unwrap().last_offset;
    assert!(log.local_log_start_offset() == last + 1);
    for ex in &exports {
        assert!(
            !ex.log_path.exists(),
            "sealed segment {:?} should be deleted",
            ex.log_path
        );
    }
    // Re-running is a no-op.
    let removed_again = local_retention_drive(&mut log, &finished, &log_config, future);
    assert!(removed_again == 0);
}

// Keep the three eviction/roll oracles together while preserving each test's
// original aborting `assert!` or accumulating `check!` behavior.
macro_rules! evicted_local_segments {
    ($assertion:ident, $removed:expr, $exports:expr, $partition:expr) => {{
        $assertion!($removed == $exports.len());
        let mut log = fixtures::partition_log_guard($partition);
        let active_base = $exports.last().unwrap().last_offset + 1;
        $assertion!(log.local_log_start_offset() == active_base);
        let sealed = sealed_ranges(&mut log);
        $assertion!(sealed == vec![(active_base, log.log_end_offset() - 1)]);
    }};
}

#[tokio::test]
async fn local_retention_pass_deletes_finished_segments_and_returns_count() {
    let (log_dir, remote_dir) = fixtures::temporary_dirs();
    let partition = short_retention_partition(log_dir.path());
    let (exports, _rsm, _rlmm, removed) = fixtures::archived_local_retention(
        &partition,
        remote_dir.path(),
        crate::api_catalog::UnstableApiVersions::Disabled,
    )
    .await;

    // The active segment breached the window as well, so the pass rolled
    // it for the next copy.
    evicted_local_segments!(assert, removed, exports, &partition);
}

#[tokio::test]
async fn local_retention_still_evicts_under_a_write_once_archive() {
    let log_dir = tempfile::tempdir().unwrap();
    let partition = short_retention_partition(log_dir.path());
    let (exports, log_config) = fixtures::multiple_segment_snapshot(&partition);

    let (rsm, rlmm) = fixtures::write_once_backends();
    let copied = copy_exports(&tier(ArchiveMode::WriteOnce, &rsm, &rlmm), exports.clone()).await;
    check!(copied == exports.len());

    // Archiving a segment is exactly what makes its local copy droppable.
    // A write-once remote tier does not change that: local retention
    // deletes local files and never touches the archive.
    let removed = fixtures::local_retention_at(
        &partition,
        &exports,
        &log_config,
        &rlmm,
        (
            crate::api_catalog::UnstableApiVersions::Disabled,
            now_ms() + 1_000_000,
        ),
    )
    .await;

    evicted_local_segments!(check, removed, exports, &partition);
}

/// A tiered topic whose producers stamp records far in the future keeps
/// its copied segments on local disk under Kafka 4.3.1, which never
/// time-expires them. Kafka trunk (KAFKA-20609) ages them by their file's
/// `lastModified()`, and only under `unstable.api.versions.enable`, and
/// only while the tier still takes copies (`remoteLogEnabledAndRemoteCopyEnabled`).
///
/// Each case is `(name, unstable flag, remote.log.copy.disable, whether the
/// copied segments are evicted)`.
#[tokio::test]
async fn future_stamped_segments_leave_the_disk_only_under_trunks_rule() {
    for (name, unstable, copy_disable, evicted) in [
        (
            "4.3.1 keeps them",
            UnstableApiVersions::Disabled,
            false,
            false,
        ),
        (
            "trunk ages them by their file",
            UnstableApiVersions::Enabled,
            false,
            true,
        ),
        (
            "trunk keeps them when the tier takes no copies",
            UnstableApiVersions::Enabled,
            true,
            false,
        ),
    ] {
        partition_log_fixture!(
            log_dir,
            remote_dir,
            log,
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                remote_tier: krabka_log::RemoteTierFlags {
                    copy_disable,
                    ..krabka_log::RemoteTierFlags::DEFAULT
                },
                ..LogConfig::default()
            }
        );
        // Every record claims a timestamp a million seconds ahead.
        let future = now_ms() + 1_000_000_000;
        for _ in 0..12 {
            let mut future_batch = batch(2);
            future_batch.base_timestamp = future;
            future_batch.max_timestamp = future;
            log.append(&mut future_batch).unwrap();
        }
        log.sync().unwrap();
        let partition = leading_partition_over(PartitionIndex(0), log_dir.path(), log);
        let (exports, _rsm, _rlmm, removed) =
            fixtures::archived_local_retention(&partition, remote_dir.path(), unstable).await;

        check!(
            removed == if evicted { exports.len() } else { 0 },
            "{name}: removed {removed} of {}",
            exports.len()
        );
    }
}

// Where one local-retention pass left a log: what it removed, its
// sealed segments, its active segment's base, and its local log start.
#[derive(Debug, PartialEq, Eq)]
struct PassOutcome {
    removed: usize,
    sealed: Vec<(Offset, Offset)>,
    active_base: Option<Offset>,
    local_log_start: Offset,
}

// Run one pass over `partition` with the sealed segments it holds now,
// at the partition's own high watermark, and describe the log after it.
async fn pass_over(
    partition: &Partition,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> PassOutcome {
    let (exports, log_config) = partition_snapshot(partition);
    let removed = local_retention_pass(
        &tp(),
        partition,
        &exports,
        &log_config,
        rlmm,
        LocalRetentionBounds {
            now_ms,
            high_watermark: partition.high_watermark().await,
        },
        UnstableApiVersions::Disabled,
    );
    let mut log = fixtures::partition_log_guard(partition);
    PassOutcome {
        removed,
        sealed: sealed_ranges(&mut log),
        active_base: log.active_segment_export().map(|active| active.base_offset),
        local_log_start: log.local_log_start_offset(),
    }
}

// A tiered partition that stops taking writes still leaves local disk.
//
// Every record here sits in the active segment, which `segment.bytes`
// and `segment.ms` are far from rolling, and which the remote tier never
// holds. Kafka's `deletableSegments` rolls it once it breaches
// `local.retention.ms`, the next copy uploads it, and the next pass drops
// it, so `ListOffsets(EARLIEST_LOCAL)` reaches the log end. Without the
// roll the local log start stays at 0 for as long as the partition is
// idle.
#[tokio::test]
async fn a_breached_active_segment_rolls_then_tiers_then_leaves_the_disk() {
    partition_log_fixture!(
        log_dir,
        remote_dir,
        log,
        LogConfig {
            remote_storage_enable: true,
            local_retention: Some(millis(1)),
            retention: None,
            ..LogConfig::default()
        }
    );
    for _ in 0..3 {
        log.append(&mut batch(2)).unwrap();
    }
    let partition = leading_partition_over(PartitionIndex(0), log_dir.path(), log);
    let (rsm, rlmm) = local_backends(remote_dir.path());

    check!(
        pass_over(&partition, &rlmm, now_ms()).await
            == PassOutcome {
                removed: 0,
                sealed: vec![(Offset(0), Offset(5))],
                active_base: Some(Offset(6)),
                local_log_start: Offset(0),
            },
        "the first pass rolls the active segment and deletes nothing"
    );

    let exports = {
        let mut log = fixtures::partition_log_guard(&partition);
        log.sync().expect("flush the rolled segment");
        log.tierable_segments()
    };
    let copied = copy_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), exports).await;
    check!(copied == 1, "the next copy uploads the rolled segment");

    check!(
        pass_over(&partition, &rlmm, now_ms()).await
            == PassOutcome {
                removed: 1,
                sealed: vec![],
                active_base: Some(Offset(6)),
                local_log_start: Offset(6),
            },
        "the next pass drops it, and the empty active segment stays"
    );
}

// Kafka's walk reaches the active segment only once the high watermark
// has passed the log end, and only through every sealed segment before
// it. The sweep read its sealed segments before the copy pass, so an
// append that rolled the log since then sealed a segment the walk does
// not know about; Kafka would stop at that segment, which the tier does
// not hold yet, and the pass must not reach past it either. Every sealed
// segment the sweep read is copied and breached in each case.
//
// Each case is `(name, high watermark one short of the log end, a roll
// since the sweep read the log, whether the pass rolls)`.
#[tokio::test]
async fn the_roll_waits_for_the_high_watermark_and_for_the_walk_to_reach_it() {
    for (name, unreplicated, rolled_since, rolls) in [
        ("replicated and next in the walk", false, false, true),
        ("a record above the high watermark", true, false, false),
        (
            "a segment sealed since the sweep read the log",
            false,
            true,
            false,
        ),
    ] {
        let (log_dir, remote_dir) = fixtures::temporary_dirs();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_size: bytes(256),
                remote_storage_enable: true,
                local_retention: Some(millis(1)),
                retention: None,
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = partition_snapshot(&partition);
        let (_rsm, rlmm) = archived_backends(remote_dir.path(), &exports).await;
        let active_before = {
            let mut log = fixtures::partition_log_guard(&partition);
            if rolled_since {
                assert!(log.roll().unwrap());
                log.append(&mut batch(2)).unwrap();
            }
            log.active_segment_export().map(|active| active.base_offset)
        };
        let log_end = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .log_end_offset();
        partition.replica_state.lock().await.hw = if unreplicated { log_end - 1 } else { log_end };

        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            LocalRetentionBounds {
                now_ms: now_ms(),
                high_watermark: partition.high_watermark().await,
            },
            UnstableApiVersions::Disabled,
        );

        let active_after = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .active_segment_export()
            .map(|active| active.base_offset);
        check!(removed == exports.len(), "{name}: the copied segments go");
        check!(
            (active_after != active_before) == rolls,
            "{name}: active segment {active_before:?} -> {active_after:?}"
        );
    }
}

fn timed_active_case(
    label: &'static str,
    sealed: Vec<SegmentExport>,
    covered_through: Option<i64>,
    expected: LocalRetentionDecision,
) -> RollCase {
    RollCase {
        label,
        sealed,
        covered_through,
        active: Some((300, 50)),
        retention: Some(millis(1_000)),
        retention_size: None,
        expected,
    }
}
