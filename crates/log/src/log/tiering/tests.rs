//! Unit tests for the tiered-storage surface: the sealed-segment export,
//! the leader-epoch ranges it carries, and the local deletion that
//! follows a successful offload.

use assert2::check;
use krabka_units::prelude::{bytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{
        append_samples, check_contiguous_exports, check_stamps, rolled_log, sample_batch,
        sample_batch_with_epoch, sample_log, stamped_rolling_log, test_log,
    },
    test_support::{log_file_count, segmented_log},
};

/// An epoch covers `[start_offset, next.start_offset)`, so one ending
/// exactly where the requested range begins does not overlap it.
///
/// The half-open end is what keeps a fetch from being told the epoch of a
/// record it did not ask for -- the divergence check a follower runs on
/// the answer would then compare against the wrong epoch.
#[test]
fn an_epoch_ending_where_the_range_begins_does_not_overlap_it() {
    use crate::leader_epoch_checkpoint::EpochEntry;

    let sorted = vec![
        EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(0),
        },
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(10),
        },
    ];

    // Epoch 1 covers [0, 10) and epoch 2 covers [10, MAX).
    let from_ten = epochs_for_range(&sorted, Offset(10), Offset(20));
    check!(
        from_ten == vec![(LeaderEpoch(2), Offset(10))],
        "a range starting at 10 is epoch 2 alone, got {from_ten:?}"
    );

    // One offset earlier and both epochs are in range.
    let from_nine = epochs_for_range(&sorted, Offset(9), Offset(20));
    check!(
        from_nine == vec![(LeaderEpoch(1), Offset(9)), (LeaderEpoch(2), Offset(10))],
        "got {from_nine:?}"
    );

    // A range wholly inside the first epoch sees only it.
    let early = epochs_for_range(&sorted, Offset(0), Offset(5));
    check!(early == vec![(LeaderEpoch(1), Offset(0))], "got {early:?}");
}

#[test]
fn tiered_local_delete_removes_only_deleted_segment_stamp_indexes() {
    let (_dir, mut log) = stamped_rolling_log(10, 1, 3);

    check!(log.sealed_txn_indexes.contains_key(&Offset(0)));
    check!(log.delete_local_segments_through(Offset(1)).unwrap() == 1);
    check!(!log.sealed_txn_indexes.contains_key(&Offset(0)));
    check!(log.sealed_txn_indexes.contains_key(&Offset(1)));

    check_stamps(&log, &[(0, None), (1, Some(11)), (2, Some(12))]);
}

#[test]
fn tierable_segments_excludes_active_and_reports_paths() {
    // small so we roll fast
    let (dir, mut log) = sample_log(
        LogConfig {
            segment_size: bytes(200),
            ..LogConfig::default()
        },
        10,
        2,
    );
    log.sync().unwrap();
    let sealed_count = log_file_count(dir.path()) - 1; // minus the active segment's .log

    let exports = log.tierable_segments();
    assert2::assert!(exports.len() == sealed_count);

    let active_base = log.log_end_offset(); // not literally, but exports must be below it
    let mut prev_last = Offset(-1);
    for ex in &exports {
        check!(ex.log_path.exists(), "log file present: {:?}", ex.log_path);
        check!(ex.offset_index_path.exists());
        check!(ex.time_index_path.exists());
        check!(ex.last_offset >= ex.base_offset);
        check!(ex.max_timestamp == 0);
        check!(ex.base_offset > prev_last, "segments are offset-ordered");
        prev_last = ex.last_offset;
        assert2::assert!(ex.last_offset < active_base);
    }
}

#[test]
fn tierable_segments_empty_for_single_active_segment() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch(3);
    log.append(&mut b).unwrap();
    // No roll happened: the only segment is active and never tierable.
    assert2::assert!(log.tierable_segments().is_empty());
}

#[test]
fn tierable_segments_last_offset_matches_next_base() {
    let (_dir, mut log) = sample_log(
        LogConfig {
            segment_size: bytes(200),
            ..LogConfig::default()
        },
        8,
        2,
    );
    log.sync().unwrap();
    let exports = log.tierable_segments();
    assert2::assert!(!exports.is_empty());
    // Each sealed segment's last_offset is exactly one below the next
    // segment's base — contiguous coverage with no gaps.
    check_contiguous_exports(&exports);
}

#[test]
fn tierable_segments_carry_leader_epochs() {
    let dir = tempdir().unwrap();
    let mut log = segmented_log(dir.path(), bytes(200));
    // epoch 0 for the first few, then epoch 1.
    for _ in 0..4 {
        let mut b = sample_batch_with_epoch(2, 0);
        log.append(&mut b).unwrap();
    }
    for _ in 0..4 {
        let mut b = sample_batch_with_epoch(2, 1);
        log.append(&mut b).unwrap();
    }
    log.sync().unwrap();
    let exports = log.tierable_segments();
    assert2::assert!(!exports.is_empty());
    // Every export carries at least one epoch, and each recorded start
    // offset is clamped to >= the segment base.
    for ex in &exports {
        assert2::assert!(!ex.leader_epochs.is_empty());
        for (_epoch, start) in &ex.leader_epochs {
            assert2::assert!(*start >= ex.base_offset);
            assert2::assert!(*start <= ex.last_offset);
        }
    }
}

#[test]
fn epochs_for_range_clamps_and_filters() {
    use crate::leader_epoch_checkpoint::EpochEntry;
    let entries = vec![
        EpochEntry {
            epoch: LeaderEpoch(0),
            start_offset: Offset(0),
        },
        EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(50),
        },
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(100),
        },
    ];
    for (name, start, end, want) in [
        // Segment [60, 90] sits entirely in epoch 1.
        (
            "within one epoch",
            Offset(60),
            Offset(90),
            vec![(LeaderEpoch(1), Offset(60))],
        ),
        // Segment [40, 60] straddles epoch 0 (->clamped to 40) and epoch 1.
        (
            "straddles epochs",
            Offset(40),
            Offset(60),
            vec![(LeaderEpoch(0), Offset(40)), (LeaderEpoch(1), Offset(50))],
        ),
        // Segment [0, 200] covers all three.
        (
            "covers all epochs",
            Offset(0),
            Offset(200),
            vec![
                (LeaderEpoch(0), Offset(0)),
                (LeaderEpoch(1), Offset(50)),
                (LeaderEpoch(2), Offset(100)),
            ],
        ),
    ] {
        check!(
            epochs_for_range(&entries, start, end) == want,
            "case {name}: range [{}, {}]",
            start.0,
            end.0
        );
    }
    // No entries -> empty.
    assert2::assert!(epochs_for_range(&[], Offset(0), Offset(100)).is_empty());
}

#[test]
fn local_log_start_offset_matches_log_start_offset() {
    let (_dir, mut log) = test_log();
    append_samples(&mut log, 3, 2);
    assert2::assert!(log.local_log_start_offset() == log.log_start_offset());
}

#[test]
fn delete_local_segments_through_drops_sealed_below_target() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    assert2::assert!(exports.len() >= 3);

    // Pick a target strictly between two sealed-segment boundaries:
    // one past the second sealed segment's last_offset. Every sealed
    // segment whose last_offset < target should be deleted.
    let target = exports[1].last_offset + 1;
    let expected_deleted: Vec<Offset> = exports
        .iter()
        .filter(|e| e.last_offset < target)
        .map(|e| e.base_offset)
        .collect();
    let active_base_before = log.log_end_offset();

    let removed = log.delete_local_segments_through(target).unwrap();
    assert2::assert!(removed == expected_deleted.len());

    // (a) sealed segments below target are gone from the in-memory list.
    let remaining_bases: Vec<Offset> = log
        .tierable_segments()
        .iter()
        .map(|e| e.base_offset)
        .collect();
    for base in &expected_deleted {
        assert2::assert!(!remaining_bases.contains(base));
    }

    // (b) on-disk files for deleted segments are gone.
    for base in &expected_deleted {
        check!(!name::log_path(dir.path(), base.0).exists());
        check!(!name::index_path(dir.path(), base.0).exists());
        check!(!name::timeindex_path(dir.path(), base.0).exists());
    }

    // (c) the active segment is untouched.
    assert2::assert!(log.log_end_offset() == active_base_before);
}

/// Kafka's `deleteSegments` → `deleteProducerSnapshots`: a local eviction
/// removes the producer snapshot at each evicted segment's base and keeps
/// every other one, the snapshot at the first surviving base included.
#[test]
fn delete_local_segments_through_removes_the_evicted_bases_snapshots() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    log.sync().unwrap();
    let exports = log.tierable_segments();
    let snapshots = |dir: &std::path::Path| -> Vec<Offset> {
        producer_snapshot::list(dir)
            .unwrap()
            .into_iter()
            .map(|(offset, _)| offset)
            .collect()
    };
    let before = snapshots(dir.path());
    let target = exports[1].last_offset + 1;
    check!(
        before.contains(&exports[1].base_offset),
        "fixture: {before:?}"
    );
    check!(before.contains(&target), "fixture: {before:?}");

    log.delete_local_segments_through(target).unwrap();

    let expected: Vec<Offset> = before
        .into_iter()
        .filter(|offset| *offset != exports[0].base_offset && *offset != exports[1].base_offset)
        .collect();
    check!(snapshots(dir.path()) == expected);
}

#[test]
fn delete_local_segments_through_keeps_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let leo_before = log.log_end_offset();
    let active_log = dir.path().join(format!(
        "{:020}.log",
        log.tierable_segments().last().unwrap().last_offset + 1
    ));
    // The active segment's .log file should exist before and after.
    assert2::assert!(active_log.exists());

    // First: target far beyond every sealed segment but well past
    // active.base_offset. The active segment must not be removed.
    let huge_target = leo_before + 1_000_000;
    let _ = log.delete_local_segments_through(huge_target).unwrap();
    check!(active_log.exists(), "active segment must survive");
    check!(
        log.log_end_offset() == leo_before,
        "active segment untouched (LEO unchanged)"
    );
    // Sealed-segment pointer should have advanced past everything.
    check!(log.tierable_segments().is_empty());
}

/// KIP-405 gives a tiered partition two floors, and a local eviction moves
/// only one of them: the records are still in the remote tier, so the global
/// `log_start_offset` a fetch is measured against must stay where it was.
/// When the two moved together, an offset whose copy the archive still held
/// answered `OFFSET_OUT_OF_RANGE` from the local floor and `ListOffsets`
/// reported an earliest offset that skipped everything tiered.
#[test]
fn delete_local_segments_through_moves_only_the_local_start_pointer() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    let start_before = log.log_start_offset();
    let target = exports[1].last_offset + 1;

    log.delete_local_segments_through(target).unwrap();

    assert2::assert!(log.local_log_start_offset() == target);
    assert2::assert!(log.log_start_offset() == start_before);
}

/// The band between the two floors is the remote tier's to serve, so a local
/// read of it fails rather than answering with the first batch a surviving
/// segment happens to begin with. Below the global floor there is nothing
/// anywhere, and the error says so.
#[test]
fn a_read_between_the_two_floors_is_the_remote_tier_s_to_serve() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let exports = log.tierable_segments();
    let target = exports[1].last_offset + 1;
    log.delete_local_segments_through(target).unwrap();
    log.set_log_start_offset(exports[0].base_offset + 1)
        .unwrap();
    let global = log.log_start_offset();

    check!(
        matches!(
            log.read(global - 1, mebibytes(1)),
            Err(LogError::OffsetTooLow { .. })
        ),
        "below the global floor no tier answers"
    );
    for offset in [global, target - 1] {
        check!(
            matches!(
                log.read(offset, mebibytes(1)),
                Err(LogError::OffsetBelowLocalStart { .. })
            ),
            "offset {offset} is tiered"
        );
        check!(
            matches!(
                log.read_raw(offset, log.log_end_offset(), mebibytes(1)),
                Err(LogError::OffsetBelowLocalStart { .. })
            ),
            "offset {offset} is tiered, raw read"
        );
    }
    check!(
        log.read(target, mebibytes(1)).is_ok(),
        "the local floor itself still reads"
    );
}

#[test]
fn delete_local_segments_through_is_noop_at_or_below_current_start() {
    let dir = tempdir().unwrap();
    let mut log = rolled_log(dir.path(), &LogConfig::default());
    let start_before = log.log_start_offset();
    let sealed_before = log.tierable_segments().len();

    let removed = log.delete_local_segments_through(start_before).unwrap();
    assert2::assert!(removed == 0);
    let removed_below = log
        .delete_local_segments_through((start_before - 1).max(Offset(0)))
        .unwrap();
    assert2::assert!(removed_below == 0);
    assert2::assert!(log.log_start_offset() == start_before);
    assert2::assert!(log.tierable_segments().len() == sealed_before);
}

#[test]
fn delete_local_segments_through_rejects_negative_target() {
    let (_dir, mut log) = test_log();
    let err = log.delete_local_segments_through(Offset(-1)).unwrap_err();
    assert2::assert!(matches!(err, LogError::InvalidArgument(_)));
}

/// Evicting a copied segment leaves the reopened log with no floor anything
/// may be refused or deleted against.
///
/// A local eviction deletes no records -- the archive still answers for them
/// -- so it writes no checkpoint, and [`Log::open`] can then only read a start
/// off the segments that survived. On a tiered partition those begin above
/// everything the archive holds. A remote read measured against that
/// derivation would refuse offsets the tier can still serve, and the log-start
/// breach in remote retention would delete the archive, so
/// [`Log::established_log_start`] answers `None`.
#[test]
fn a_local_eviction_leaves_a_reopened_log_with_no_floor_to_refuse_against() {
    let dir = tempdir().unwrap();
    let config = LogConfig::default();
    let mut log = rolled_log(dir.path(), &config);
    let exports = log.tierable_segments();
    let evicted_through = exports[1].last_offset + 1;
    log.delete_local_segments_through(evicted_through).unwrap();
    drop(log);

    let mut reopened = Log::open(dir.path(), config).unwrap();
    check!(
        reopened.log_start_offset() == evicted_through,
        "the derivation is still the first surviving segment"
    );
    check!(
        reopened.established_log_start() == None,
        "but nobody deleted up to it, so nothing may be refused against it"
    );

    reopened.set_log_start_offset(evicted_through + 1).unwrap();
    check!(
        reopened.established_log_start() == Some(evicted_through + 1),
        "a floor this process moved is one a reader may be refused against"
    );
}

/// A `DeleteRecords` on a tiered partition survives the reopen, and stays
/// below the segments that survived it.
///
/// This is the restart half of the issue: the checkpoint carries the *global*
/// floor, so a reopened partition still refuses what an operator deleted while
/// still serving the band above it from the archive. Raising the checkpoint to
/// the first surviving segment -- which is where a single-floor log would put
/// it -- would hide every offset the tier holds.
#[test]
fn a_delete_records_floor_under_the_local_segments_survives_a_reopen() {
    let dir = tempdir().unwrap();
    let config = LogConfig::default();
    let mut log = rolled_log(dir.path(), &config);
    let exports = log.tierable_segments();
    let deleted_through = exports[0].base_offset + 1;
    let evicted_through = exports[1].last_offset + 1;
    log.set_log_start_offset(deleted_through).unwrap();
    log.delete_local_segments_through(evicted_through).unwrap();
    log.sync().unwrap();
    drop(log);

    let reopened = Log::open(dir.path(), config).unwrap();

    check!(
        reopened.log_start_offset() == deleted_through,
        "the operator's floor, not where the surviving files begin"
    );
    check!(reopened.established_log_start() == Some(deleted_through));
    check!(
        reopened.local_log_start_offset() == evicted_through,
        "the band between the two is the archive's to serve"
    );
}

// What a log holds after `Log::roll`: whether it rolled, the sealed
// segments as `(base, last)` offset ranges, the active segment's base and
// size, and the log end.
#[derive(Debug, PartialEq)]
struct AfterRoll {
    rolled: bool,
    sealed: Vec<(Offset, Offset)>,
    active: Option<(Offset, ByteSize)>,
    log_end: Offset,
}

fn roll_and_describe(log: &mut Log) -> AfterRoll {
    let rolled = log.roll().unwrap();
    // A rolled segment is tierable once its rollover flush publishes the
    // boundary snapshot. `sync` waits for that flush.
    log.sync().unwrap();
    AfterRoll {
        rolled,
        sealed: log
            .tierable_segments()
            .iter()
            .map(|export| (export.base_offset, export.last_offset))
            .collect(),
        active: log
            .active_segment_export()
            .map(|active| (active.base_offset, active.size)),
        log_end: log.log_end_offset(),
    }
}

// Kafka's `UnifiedLog.roll()` as tiered local retention calls it: a roll
// seals the active segment's records and opens an empty segment at the log
// end, and a second roll over that empty segment does nothing, because there
// is nothing in it to copy and the new segment would open at the same base.
#[test]
fn roll_seals_the_active_records_and_never_rolls_an_empty_segment() {
    let (_dir, mut log) = test_log();

    check!(
        roll_and_describe(&mut log)
            == AfterRoll {
                rolled: false,
                sealed: vec![],
                active: Some((Offset(0), ByteSize::ZERO)),
                log_end: Offset(0),
            },
        "an empty log has nothing to roll"
    );

    log.append(&mut sample_batch(3)).unwrap();
    check!(
        roll_and_describe(&mut log)
            == AfterRoll {
                rolled: true,
                sealed: vec![(Offset(0), Offset(2))],
                active: Some((Offset(3), ByteSize::ZERO)),
                log_end: Offset(3),
            },
        "the three records move into a sealed segment"
    );

    check!(
        roll_and_describe(&mut log)
            == AfterRoll {
                rolled: false,
                sealed: vec![(Offset(0), Offset(2))],
                active: Some((Offset(3), ByteSize::ZERO)),
                log_end: Offset(3),
            },
        "the fresh segment is empty, so a second roll does nothing"
    );
}

// The active segment's export carries Kafka's `largestTimestamp()`: the
// newest record timestamp, or the `.log` file's modification time when no
// record carries one.
#[test]
fn active_segment_export_ages_the_segment_the_way_retention_does() {
    for (name, max_timestamp, ages_by_file) in [
        ("a stamped record", 5_000, false),
        ("no record timestamp", -1, true),
    ] {
        let (dir, mut log) = test_log();
        let mut batch = sample_batch(2);
        batch.max_timestamp = max_timestamp;
        log.append(&mut batch).unwrap();
        let size = log.size();
        let last_modified_ms = last_modified_ms(&name::log_path(dir.path(), 0));

        check!(
            log.active_segment_export()
                == Some(ActiveSegmentExport {
                    base_offset: Offset(0),
                    max_timestamp: if ages_by_file {
                        last_modified_ms
                    } else {
                        max_timestamp
                    },
                    last_modified_ms,
                    size,
                }),
            "{name}"
        );
    }
}
