//! Unit tests for opening a log directory and for the producer,
//! transaction, and snapshot state that recovery rebuilds from a
//! partially written tail.

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use tempfile::tempdir;

use super::*;
use crate::{
    io::FileIo,
    log::test_support::{
        NO_LIMIT, append_transaction, commit_marker, rolling_test_log, sample_batch,
        sample_batch_with_epoch, test_log, tiny_segments, transaction_fields, transactional_batch,
    },
};

#[test]
fn open_empty_dir_creates_first_segment() {
    let (_dir, log) = test_log();
    assert2::assert!(log.log_start_offset() == Offset(0));
    assert2::assert!(log.log_end_offset() == Offset(0));
    log.close();
}

#[test]
fn open_creates_log_file() {
    let (dir, log) = test_log();
    drop(log);
    let log_path = dir.path().join("00000000000000000000.log");
    assert2::assert!(log_path.exists());
}

#[test]
fn open_rejects_a_negative_segment_base() {
    let dir = tempdir().unwrap();
    std::fs::File::create(name::log_path(dir.path(), -1)).unwrap();

    assert2::assert!(matches!(
        Log::open(dir.path(), LogConfig::default()),
        Err(LogError::Corrupt(message)) if message.contains("segment bases")
    ));
}

#[test]
fn open_recovers_partial_trailing_batch() {
    let dir = tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
    }
    // Append 10 bytes of garbage to the .log file.
    let log_path = dir.path().join("00000000000000000000.log");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .unwrap();
    std::io::Write::write_all(&mut f, &[0xAB; 10]).unwrap();
    f.sync_data().unwrap();
    drop(f);
    let log = crate::test_support::open_log(dir.path());
    assert2::assert!(log.log_end_offset() == 5);
}

#[test]
fn open_truncates_epoch_checkpoint_to_recovered_leo() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("00000000000000000000.log");
    let first_batch_len = {
        let mut log = crate::test_support::open_log(dir.path());
        let mut first = sample_batch_with_epoch(1, 1);
        log.append(&mut first).unwrap();
        let first_batch_len = log.read_raw(Offset(0), Offset(1), NO_LIMIT).unwrap().total;
        let mut torn = sample_batch_with_epoch(1, 7);
        log.append(&mut torn).unwrap();
        assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(7)));
        first_batch_len
    };

    std::fs::OpenOptions::new()
        .write(true)
        .open(&log_path)
        .unwrap()
        .set_len(u64::try_from(first_batch_len + 5).unwrap())
        .unwrap();

    let cfg = LogConfig {
        validate_on_open: true,
        ..LogConfig::default()
    };
    let reopened = Log::open(dir.path(), cfg).unwrap();

    assert!(reopened.log_end_offset() == Offset(1));
    assert!(
        reopened
            .epoch_checkpoint()
            .entries()
            .iter()
            .all(|entry| entry.start_offset < reopened.log_end_offset())
    );
    assert!(reopened.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
}

/// On a tiered partition (KIP-405) the log start stays below the local
/// segments, so the snapshot at the first local base is above it and Kafka's
/// `truncateAndReload` keeps and loads it.
#[test]
fn producer_snapshot_survives_local_segment_deletion_and_restart() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        remote_storage_enable: true,
        ..tiny_segments()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let mut producer = sample_batch(2);
    producer.producer_id = 42;
    producer.producer_epoch = 3;
    producer.base_sequence = 7;
    producer.max_timestamp = 1_234;
    log.append(&mut producer).unwrap();

    // The second append rolls the producer batch into a sealed segment and
    // writes the snapshot at the new segment's base offset.
    log.append(&mut sample_batch(1)).unwrap();
    log.sync().unwrap();
    let export = log.tierable_segments().into_iter().next().unwrap();
    check!(export.producer_snapshot_path.exists());
    let local_start = export.last_offset + 1;
    check!(log.delete_local_segments_through(local_start).unwrap() == 1);
    drop(log);

    let reopened = Log::open(dir.path(), config).unwrap();
    let entry = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 42)
        .unwrap();
    check!(entry.producer_epoch == 3);
    check!(entry.last_sequence == 8);
    check!(entry.last_offset == 1);
    check!(entry.offset_delta == 1);
    check!(entry.timestamp == 1_234);
}

#[test]
fn snapshot_recovery_preserves_open_and_completed_transaction_fields() {
    let dir = tempdir().unwrap();
    let config = tiny_segments();
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let mut data = transactional_batch(77, 4, &["a", "b"]);
    data.base_sequence = 10;
    log.append(&mut data).unwrap();
    log.append(&mut sample_batch(1)).unwrap();

    let open = log
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 77)
        .unwrap();
    check!(open.current_txn_first_offset == Some(Offset(0)));

    log.append(&mut commit_marker(77, 4)).unwrap();
    // Roll once more so the newest snapshot, rather than the replay tail,
    // is the only durable source of the completed coordinator epoch.
    log.append(&mut sample_batch(1)).unwrap();
    drop(log);
    let reopened = Log::open(dir.path(), config).unwrap();
    let completed = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 77)
        .unwrap();
    check!(completed.last_sequence == 11);
    check!(completed.current_txn_first_offset == None);
    check!(completed.coordinator_epoch == 17);
    check!(transaction_fields(&reopened, ProducerId(77)) == (17, None));
}

#[test]
fn recovery_recreates_missing_producer_snapshot_at_segment_boundary() {
    let dir = tempdir().unwrap();
    let config = tiny_segments();
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for (base_sequence, timestamp) in [(0, 10), (2, 20), (4, 30)] {
            let mut batch = sample_batch(2);
            batch.producer_id = 42;
            batch.producer_epoch = 3;
            batch.base_sequence = base_sequence;
            batch.max_timestamp = timestamp;
            log.append(&mut batch).unwrap();
        }
    }

    let missing = producer_snapshot::path(dir.path(), Offset(4));
    check!(missing.exists());
    std::fs::remove_file(&missing).unwrap();

    let reopened = Log::open(dir.path(), config).unwrap();
    check!(missing.exists());
    let range = krabka_verified::ProducerReloadRange {
        log_start: 0,
        local_start: 0,
        log_end: 4,
    };
    let (_, boundary_state) = producer_snapshot::reload(dir.path(), range)
        .unwrap()
        .unwrap();
    let boundary = boundary_state.get(&ProducerId(42)).unwrap();
    check!(boundary.last_sequence == 3);
    check!(boundary.last_offset == Offset(3));
    let recovered = reopened
        .producer_state_snapshot()
        .into_iter()
        .find(|entry| entry.producer_id == 42)
        .unwrap();
    check!(recovered.last_sequence == 5);
    check!(recovered.last_offset == Offset(5));
}

#[test]
fn producer_tail_accepts_zero_and_rejects_negative_identity_fields() {
    assert2::assert!(
        Log::data_producer_tail(ProducerId(0), 0, 1, Offset(10)).unwrap() == Some((1, Offset(11)))
    );
    assert2::assert!(
        Log::data_producer_tail(ProducerId(1), 5, 1, Offset(10)).unwrap() == Some((6, Offset(11)))
    );
    assert2::assert!(Log::data_producer_tail(ProducerId(-2), 0, 0, Offset(10)).unwrap() == None);
    assert2::assert!(Log::data_producer_tail(ProducerId(1), -2, 0, Offset(10)).unwrap() == None);
}

#[test]
fn producer_tail_wraps_sequence_at_signed_maximum() {
    assert2::assert!(
        Log::data_producer_tail(ProducerId(1), i32::MAX - 1, 2, Offset(10)).unwrap()
            == Some((0, Offset(12)))
    );
}

#[test]
fn recovered_batch_offsets_require_checked_progress() {
    let mut batch = sample_batch(3);
    batch.base_offset = 10;
    assert2::assert!(
        Log::recovered_batch_offsets(Offset(10), Offset(13), &batch).unwrap()
            == (Offset(12), Offset(13))
    );

    batch.last_offset_delta = -1;
    assert2::assert!(matches!(
        Log::recovered_batch_offsets(Offset(10), Offset(20), &batch),
        Err(LogError::Corrupt(message)) if message.contains("rejected batch")
    ));

    batch.base_offset = i64::MAX;
    batch.last_offset_delta = 1;
    assert2::assert!(matches!(
        Log::recovered_batch_offsets(Offset(i64::MAX - 2), Offset(i64::MAX), &batch),
        Err(LogError::Corrupt(message)) if message.contains("rejected batch")
    ));
}

#[test]
fn producer_sequence_rollover_survives_reopen() {
    let check_sequence_rollover = |log: &Log| {
        let entry = log.producer_state_snapshot().into_iter().next().unwrap();
        assert2::assert!(entry.last_sequence == 0);
        assert2::assert!(entry.last_offset == Offset(2));
        assert2::assert!(entry.offset_delta == 2);
    };
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        let mut batch = sample_batch(3);
        batch.producer_id = 1;
        batch.producer_epoch = 0;
        batch.base_sequence = i32::MAX - 1;
        log.append(&mut batch).unwrap();

        check_sequence_rollover(&log);
    }

    let reopened = crate::test_support::open_log(dir.path());
    check_sequence_rollover(&reopened);
}

#[test]
fn higher_epoch_control_marker_clears_data_batch_metadata() {
    let check_cleared_metadata = |log: &Log| {
        let entry = log
            .producer_state_snapshot()
            .into_iter()
            .find(|entry| entry.producer_id == 88)
            .unwrap();
        check!(entry.producer_epoch == 5);
        check!(entry.last_sequence == -1);
        check!(entry.last_offset == Offset(-1));
        check!(entry.offset_delta == 0);
        check!(entry.current_txn_first_offset == None);
        check!(entry.coordinator_epoch == 17);
    };
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        let mut data = transactional_batch(88, 4, &["a", "b"]);
        data.base_sequence = 10;
        log.append(&mut data).unwrap();
        log.append(&mut commit_marker(88, 5)).unwrap();

        check_cleared_metadata(&log);
    }

    let reopened = crate::test_support::open_log(dir.path());
    check_cleared_metadata(&reopened);
}

#[test]
fn zero_producer_id_and_only_transactional_ranges_survive_recovery() {
    let check_zero_producer = |log: &Log| {
        let state = log.producer_state_snapshot();
        let zero = state.iter().find(|entry| entry.producer_id == 0).unwrap();
        assert2::assert!(zero.last_sequence == 6);
        assert2::assert!(zero.current_txn_first_offset == Some(Offset(0)));
    };
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());

        let mut zero_pid = transactional_batch(0, 3, &["a", "b"]);
        zero_pid.base_sequence = 5;
        log.append(&mut zero_pid).unwrap();

        let mut ordinary = sample_batch(1);
        ordinary.producer_id = 1;
        ordinary.producer_epoch = 0;
        ordinary.base_sequence = 7;
        log.append(&mut ordinary).unwrap();

        let mut negative_pid = transactional_batch(-2, 0, &["ignored"]);
        negative_pid.base_sequence = 0;
        log.append(&mut negative_pid).unwrap();

        check_zero_producer(&log);
    }

    let reopened = crate::test_support::open_log(dir.path());
    check_zero_producer(&reopened);
    assert2::assert!(reopened.lso() == Offset(0));
    assert2::assert!(reopened.pending_stamp_ranges.len() == 1);
    assert2::assert!(
        reopened.pending_stamp_ranges.get(&ProducerId(0)) == Some(&vec![(Offset(0), Offset(1))])
    );
}

#[test]
fn reopen_rebuilds_pending_transactions_and_lso() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        append_transaction(&mut log, (1000, 4), &["a", "b"]);
        assert2::assert!(log.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
        assert2::assert!(log.lso() == Offset(0));
    }

    let mut reopened = crate::test_support::open_log(dir.path());
    assert2::assert!(reopened.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
    assert2::assert!(reopened.lso() == Offset(0));
    crate::log::test_support::install_stamps(&mut reopened, 30, 1);

    reopened.append(&mut commit_marker(1000, 4)).unwrap();
    check!(reopened.stamp_for_offset(Offset(0)) == Some(30));
    check!(reopened.stamp_for_offset(Offset(1)) == Some(30));
    check!(reopened.stamp_for_offset(Offset(2)) == None);
    assert2::assert!(transaction_fields(&reopened, ProducerId(1000)) == (17, None));
    assert2::assert!(
        reopened
            .pending_transaction_start(ProducerId(1000))
            .is_none()
    );
    let log_end = reopened.log_end_offset();
    assert2::assert!(reopened.last_stable_offset(log_end) == log_end);

    append_transaction(&mut reopened, (1000, 4), &["next"]);
    assert2::assert!(transaction_fields(&reopened, ProducerId(1000)) == (17, Some(Offset(3))));
    drop(reopened);

    let recovered_again = crate::test_support::open_log(dir.path());
    assert2::assert!(
        transaction_fields(&recovered_again, ProducerId(1000)) == (17, Some(Offset(3)))
    );
}

#[test]
fn reopen_does_not_treat_non_transactional_producer_data_as_pending() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        let mut batch = sample_batch(1);
        batch.producer_id = 42;
        log.append(&mut batch).unwrap();
    }

    let reopened = crate::test_support::open_log(dir.path());
    check!(reopened.pending_transaction_start(ProducerId(42)) == None);
    check!(reopened.lso() == reopened.log_end_offset());
}

/// On reopen, each recovered sealed segment's `last_offset` is set to
/// `next_base - 1` (line: `seg.seal_at(Offset(base_offsets[i + 1] - 1))`).
/// Multi-record segments give non-consecutive bases so the `- 1` is
/// observable: for consecutive exports `last_offset + 1 == next_base`.
/// Mutating `- 1`→`+ 1` sets `last_offset = next_base + 1` (so
/// `last_offset + 1 == next_base + 2`); mutating `- 1`→`/ 1` sets
/// `last_offset = next_base` (so `last_offset + 1 == next_base + 1`).
#[test]
fn reopen_seals_recovered_segments_at_next_base_minus_one() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let cfg = tiny_segments();
    {
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        // Multi-record batches → segment bases are 0, 2, 4, ... (each
        // sealed segment spans two offsets), so next_base - base == 2.
        crate::log::test_support::append_samples(&mut log, 4, 2);
        assert2::assert!(log.segments.len() >= 2);
    }
    // Reopen: sealed segments recovered via no-scan open + seal_at(next-1).
    let reopened = Log::open(dir.path(), cfg).unwrap();
    let exports = reopened.tierable_segments();
    assert2::assert!(exports.len() >= 2);
    crate::log::test_support::check_contiguous_exports(&exports);
}

fn trimmed_active_log(root: &std::path::Path) -> Log {
    let mut log = crate::test_support::open_log(root);
    log.append(&mut sample_batch(5)).unwrap();
    // A single segment cannot witness the interior trim in its filename.
    log.set_log_start_offset(Offset(3)).unwrap();
    log
}

#[test]
fn open_restores_a_log_start_trimmed_inside_the_active_segment() {
    let dir = tempdir().unwrap();
    {
        let mut log = trimmed_active_log(dir.path());
        log.sync().unwrap();
    }

    let log = crate::test_support::open_log(dir.path());

    assert!(log.log_start_offset() == Offset(3));
    assert!(log.log_end_offset() == Offset(5));
}

#[test]
fn open_rewrites_a_checkpoint_past_the_log_end_so_appends_cannot_revive_it() {
    // The hazard: a checkpoint above the log end is inert against the log that
    // reads it, but appends move the log end. Left on disk, it comes back in
    // range on a later open and hides records appended after it was written.
    let dir = tempdir().unwrap();
    {
        let mut log = crate::test_support::open_log(dir.path());
        log.set_log_start_offset(Offset(7)).unwrap();
        log.sync().unwrap();
    }

    // Reopen empty: 7 is past the log end, so it resolves to the log end.
    {
        let mut log = crate::test_support::open_log(dir.path());
        assert!(log.log_start_offset() == Offset(0));
        // Records the stale checkpoint would have hidden.
        log.append(&mut sample_batch(9)).unwrap();
        log.sync().unwrap();
    }

    let log = crate::test_support::open_log(dir.path());

    assert!(log.log_start_offset() == Offset(0));
    assert!(log.log_end_offset() == Offset(9));
}

#[test]
fn open_resolves_a_checkpoint_against_what_the_log_holds() {
    // Reopening caps the checkpoint at the log end and leaves it alone below
    // the derived start.
    //
    // Past the log end, every record still present is below a start that was
    // already acknowledged -- they are all trimmed -- so the log start is the
    // log end and the log reads empty. A crash between a trim and the fsync of
    // the records it trimmed past arrives there.
    //
    // Below the derived start the checkpoint stands, because KIP-405 gives a
    // tiered partition a global floor that belongs under its oldest local
    // segment: raising it to meet the surviving files would hide the band the
    // archive serves. `local_log_start_offset` is the floor that follows the
    // files, and it is what a local read is measured against either way.
    enum Case {
        BelowDerivedStart,
        PastLogEnd,
    }

    for (case, expected_start, expected_local_start) in [
        (Case::BelowDerivedStart, Offset(1), Offset(2)),
        (Case::PastLogEnd, Offset(3), Offset(3)),
    ] {
        let dir = tempdir().unwrap();
        {
            let mut log = rolling_test_log(dir.path());
            crate::log::test_support::append_samples(&mut log, 3, 1);
            // Drops the sealed segments below offset 2, so the derived start
            // is 2 and the log end is 3.
            log.trim_to_offset(Offset(2)).unwrap();
            log.sync().unwrap();
        }
        let checkpointed = match case {
            Case::BelowDerivedStart => Offset(1),
            Case::PastLogEnd => Offset(9),
        };
        crate::log_start_offset_checkpoint::write(&crate::io::FileIo, dir.path(), checkpointed)
            .unwrap();

        let log = crate::test_support::open_log(dir.path());

        assert!(log.log_start_offset() == expected_start);
        assert!(log.local_log_start_offset() == expected_local_start);
        // Whatever it resolved to is what is now on disk, so the next open
        // reads a value that already agrees with the log.
        drop(log);
        assert!(
            crate::log_start_offset_checkpoint::read(dir.path()).unwrap() == Some(expected_start)
        );
    }
}

#[test]
fn reset_to_drops_the_checkpoint_so_a_reopen_starts_at_the_new_base() {
    let dir = tempdir().unwrap();
    {
        let mut log = trimmed_active_log(dir.path());
        log.reset_to(Offset(100)).unwrap();
        log.sync().unwrap();
        assert!(!name::log_start_offset_checkpoint_path(dir.path()).exists());
    }

    let log = crate::test_support::open_log(dir.path());

    assert!(log.log_start_offset() == Offset(100));
    assert!(log.log_end_offset() == Offset(100));
}

/// A two-record batch from producer `producer_id`, sequence 0.
fn producer_batch(producer_id: i64) -> RecordBatch {
    let mut batch = sample_batch(2);
    batch.producer_id = producer_id;
    batch.producer_epoch = 0;
    batch.base_sequence = 0;
    batch
}

/// Producers 1, 2 and 3 each append two records, one segment each: the
/// segments start at 0, 2 and 4, and each roll leaves a snapshot at the new
/// base, so there are snapshots at 2 (producer 1) and 4 (producers 1 and 2).
fn three_producer_log(dir: &Path) -> Log {
    let config = tiny_segments();
    let mut log = Log::open(dir, config).unwrap();
    for producer_id in [1, 2, 3] {
        log.append(&mut producer_batch(producer_id)).unwrap();
    }
    log
}

fn producer_ids(log: &Log) -> Vec<i64> {
    let mut ids: Vec<i64> = log
        .producer_state_snapshot()
        .into_iter()
        .map(|entry| entry.producer_id.get())
        .collect();
    ids.sort_unstable();
    ids
}

fn snapshot_offsets(dir: &Path) -> Vec<i64> {
    producer_snapshot::list(dir)
        .unwrap()
        .into_iter()
        .map(|(offset, _)| offset.0)
        .collect()
}

/// Reopening reloads against the trimmed log start the way Kafka's
/// `truncateAndReload` does: every snapshot at or below the log start is
/// deleted, the newest one above it loads, and the replay starts at that
/// snapshot or, with none, at the log start -- inside a batch if the trim
/// landed inside one, replaying that whole batch.
///
/// Advancing the log start never evicts a producer from memory
/// (`onLogStartOffsetIncremented`); only the reload can lose one whose last
/// batch is below the log start and whom no surviving snapshot carries.
/// Every reload ends with a snapshot at the log end, 6, as Kafka's
/// `rebuildProducerState` ends with `takeSnapshot()`.
#[test]
fn reopen_reloads_producer_state_against_the_trimmed_log_start() {
    for (log_start, snapshots_after_trim, snapshots_after_reopen, reloaded) in [
        // Snapshots at 2 and 4 are both above the log start: 4 loads.
        (1, vec![2, 4], vec![2, 4, 6], vec![1, 2, 3]),
        // The snapshot at the log start is deleted, and the replay from the
        // log start finds only producer 3.
        (4, vec![4], vec![6], vec![3]),
        // The snapshot below the log start is deleted, and the replay starts
        // inside producer 3's batch at 4..=5.
        (5, vec![4], vec![6], vec![3]),
    ] {
        let dir = tempdir().unwrap();
        let mut log = three_producer_log(dir.path());
        log.sync().unwrap();
        check!(snapshot_offsets(dir.path()) == vec![2, 4]);

        check!(log.trim_to_offset(Offset(log_start)).unwrap() == Offset(log_start));
        check!(producer_ids(&log) == vec![1, 2, 3], "log start {log_start}");
        check!(
            snapshot_offsets(dir.path()) == snapshots_after_trim,
            "log start {log_start}"
        );
        let config = log.config.read().unwrap().clone();
        drop(log);

        let reopened = Log::open(dir.path(), config).unwrap();
        check!(reopened.log_start_offset() == Offset(log_start));
        check!(producer_ids(&reopened) == reloaded, "log start {log_start}");
        check!(
            snapshot_offsets(dir.path()) == snapshots_after_reopen,
            "log start {log_start}"
        );
    }
}

/// Kafka's `rebuildProducerState` ends with `updateMapEndOffset(logEnd)` and
/// `takeSnapshot()`: an open that rebuilt producer state leaves a snapshot at
/// the log end, and one with nothing past the log start to cover takes none:
/// an empty log, or one trimmed to its end.
#[test]
fn a_reload_takes_a_snapshot_at_the_log_end() {
    for (name, batches, trim_to, expected) in [
        ("an empty log takes none", 0, None, vec![]),
        ("a log with records takes one at its end", 3, None, vec![6]),
        ("a log trimmed to its end takes none", 3, Some(6), vec![]),
    ] {
        let (dir, mut log) = test_log();
        for _ in 0..batches {
            log.append(&mut sample_batch(2)).unwrap();
        }
        if let Some(target) = trim_to {
            log.trim_to_offset(Offset(target)).unwrap();
        }
        drop(log);

        let reopened = crate::test_support::open_log(dir.path());
        check!(snapshot_offsets(dir.path()) == expected, "{name}");
        drop(reopened);
    }
}

/// Kafka's `removeStraySnapshots` runs before the reload: a snapshot no
/// segment starts at is deleted, unless it is the newest and above every
/// segment -- the snapshot a clean shutdown leaves at the log end, which the
/// reload then loads with nothing left to replay.
#[test]
fn reopen_removes_stray_snapshots_and_loads_one_at_the_log_end() {
    let dir = tempdir().unwrap();
    let log = three_producer_log(dir.path());
    let config = log.config.read().unwrap().clone();
    drop(log);
    let shutdown = ProducerSnapshotEntry::empty(ProducerId(99), 0);
    let shutdown_state = HashMap::from([(shutdown.producer_id, shutdown)]);
    for offset in [3, 6] {
        producer_snapshot::write(&FileIo, dir.path(), Offset(offset), &shutdown_state).unwrap();
    }

    let reopened = Log::open(dir.path(), config).unwrap();

    check!(snapshot_offsets(dir.path()) == vec![2, 4, 6]);
    check!(reopened.producer_state_snapshot() == vec![shutdown]);
}

/// #981: Kafka's `UnifiedLog.rebuildProducerState` replays the tail past the
/// loaded snapshot through `ProducerStateEntry.addBatch`, so a producer's
/// retained batches after a reopen are the snapshot's batch and the replayed
/// ones, up to `NUM_BATCHES_TO_RETAIN` (5). Six batches, then a stop that
/// leaves only the snapshot a segment roll wrote before the last four:
/// the reopen retains batches 1 to 5.
#[test]
fn reopen_retains_the_snapshot_batch_and_the_replayed_tail() {
    let dir = tempdir().unwrap();
    let config = tiny_segments();
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    for sequence in 0..6 {
        let mut batch = sample_batch(1);
        batch.producer_id = 42;
        batch.producer_epoch = 0;
        batch.base_sequence = sequence;
        batch.max_timestamp = 100 + i64::from(sequence);
        log.append(&mut batch).unwrap();
    }
    drop(log);
    // Every append rolled a segment and wrote a snapshot at its base. Keep
    // the ones up to offset 2, as if the stop came before the later ones.
    for offset in 3..=6 {
        let _ = std::fs::remove_file(crate::name::producer_snapshot_path(dir.path(), offset));
    }

    let reopened = Log::open(dir.path(), config).unwrap();
    let batch = |sequence: i32| crate::ProducerBatchMetadata {
        last_sequence: sequence,
        last_offset: Offset(i64::from(sequence)),
        offset_delta: 0,
        timestamp: 100 + i64::from(sequence),
    };
    check!(
        reopened.recovered_producers()
            == vec![crate::RecoveredProducer {
                entry: ProducerSnapshotEntry {
                    producer_id: ProducerId(42),
                    producer_epoch: 0,
                    last_sequence: 5,
                    last_offset: Offset(5),
                    offset_delta: 0,
                    timestamp: 105,
                    coordinator_epoch: -1,
                    current_txn_first_offset: None,
                },
                earlier: (1..5).map(batch).collect(),
            }]
    );
}

/// Kafka's `ProducerStateEntry`: a live append keeps the four batches before
/// the last one, oldest first, and a new producer epoch clears them.
#[test]
fn appends_retain_four_earlier_batches_until_the_epoch_moves() {
    let (_dir, mut log) = test_log();
    let append = |log: &mut Log, epoch: i16, sequence: i32| {
        let mut batch = sample_batch(1);
        batch.producer_id = 7;
        batch.producer_epoch = epoch;
        batch.base_sequence = sequence;
        log.append(&mut batch).unwrap();
    };
    let earlier_offsets = |log: &Log| -> Vec<i64> {
        log.recovered_producers()[0]
            .earlier
            .iter()
            .map(|batch| batch.last_offset.0)
            .collect()
    };
    for sequence in 0..7 {
        append(&mut log, 0, sequence);
    }
    check!(earlier_offsets(&log) == vec![2, 3, 4, 5]);
    append(&mut log, 1, 0);
    check!(earlier_offsets(&log).is_empty());
    append(&mut log, 1, 1);
    check!(earlier_offsets(&log) == vec![7]);
}
