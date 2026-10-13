//! Unit tests for the internal stamp coordinate: what a durable append
//! stamps, what a commit marker stamps later, and the guarantee that an
//! unstamped partition writes identical bytes.

use {
    super::*,
    crate::{
        log::test_support::{
            abort_marker, append_transaction, check_stamps, commit_marker, install_stamps,
            rolling_test_log, sample_batch, stamped_test_log, test_log, transactional_batch,
        },
        test_support::{open_log, stamp_entry},
    },
    assert2::check,
    krabka_units::prelude::mebibytes,
    tempfile::tempdir,
};

// ---- .stampindex append-path wiring tests ----

/// Append three data batches with distinct offset ranges to a
/// stamp-enabled log. The `.stampindex` records one entry for each batch,
/// in order, with the successive stamps from the source.
/// `stamp_for_offset` resolves every covered offset, including interior
/// offsets and the inclusive end offset. Offsets past the end resolve to
/// `None`.
#[test]
fn stampindex_records_appended_batches_and_resolves_offsets() {
    let (dir, mut log) = stamped_test_log(1_000, 10);
    check!(log.stamp_source().is_some());

    log.append(&mut sample_batch(2)).unwrap(); // offsets 0..=1, stamp 1000
    log.append(&mut sample_batch(1)).unwrap(); // offset  2,     stamp 1010
    log.append(&mut sample_batch(3)).unwrap(); // offsets 3..=5, stamp 1020

    // Query surface resolves every covered offset to its batch's stamp.
    check_stamps(&log, &[
        (0, Some(1_000)),
        (1, Some(1_000)),
        (2, Some(1_010)),
        (3, Some(1_020)),
        (5, Some(1_020)),
        (6, None),
    ]);

    // The durable on-disk sidecar holds exactly one entry per data batch.
    assert2::assert!(
        crate::log::test_support::stamp_entries(dir.path(), 0)
            == [
                crate::test_support::stamp_entry(0, 1, 1_000),
                crate::test_support::stamp_entry(2, 2, 1_010),
                crate::test_support::stamp_entry(3, 5, 1_020),
            ]
    );
}

/// A log with no injected stamp source stamps nothing. `stamp_for_offset`
/// is always `None` and the log never creates a `.stampindex` file. This
/// is the unchanged-behavior guarantee for pure-Kafka partitions.
#[test]
fn no_stamp_source_stamps_nothing() {
    let (dir, mut log) = test_log();
    log.append(&mut sample_batch(2)).unwrap();
    log.append(&mut sample_batch(3)).unwrap();

    check_stamps(&log, &[(0, None), (4, None)]);
    assert2::assert!(!dir.path().join("00000000000000000000.stampindex").exists());
}

/// Transactional data stays unstamped until commit. The commit marker is
/// not stamped because a stamp is a coordinate for data records.
#[test]
fn control_markers_are_not_stamped() {
    let (dir, mut log) = stamped_test_log(1, 1);

    // Transactional data at offsets 0..=1, then its commit marker at 2.
    append_transaction(&mut log, (1000, 0), &["a", "b"]);
    check_stamps(&log, &[(0, None), (1, None)]);
    log.append(&mut commit_marker(1000, 0)).unwrap();
    // A following non-txn data batch at offset 3.
    log.append(&mut sample_batch(1)).unwrap();

    check_stamps(&log, &[
        (0, Some(1)), // txn data
        (1, Some(1)),
        (2, None),    // commit marker: unstamped
        (3, Some(2)), // next data batch
    ]);

    assert2::assert!(
        crate::log::test_support::stamp_entries(dir.path(), 0)
            == [
                crate::test_support::stamp_entry(0, 1, 1),
                crate::test_support::stamp_entry(3, 3, 2),
            ]
    );
}

#[test]
fn commit_stamps_only_matching_interleaved_transaction_ranges() {
    let (_dir, mut log) = stamped_test_log(10, 10);

    append_transaction(&mut log, (1000, 0), &["a"]); // offset 0
    append_transaction(&mut log, (2000, 0), &["b", "c"]); // offsets 1..=2
    append_transaction(&mut log, (1000, 0), &["d"]); // offset 3

    log.append(&mut commit_marker(2000, 0)).unwrap(); // offset 4
    check_stamps(&log, &[(0, None), (1, Some(10)), (2, Some(10)), (3, None)]);

    log.append(&mut commit_marker(1000, 0)).unwrap(); // offset 5
    check_stamps(&log, &[(0, Some(20)), (3, Some(20)), (4, None), (5, None)]);
}

#[test]
fn abort_leaves_transactional_data_unstamped() {
    let (_dir, mut log) = stamped_test_log(7, 1);

    append_transaction(&mut log, (1000, 0), &["a", "b"]);
    log.append(&mut abort_marker(1000, 0)).unwrap();
    log.append(&mut sample_batch(1)).unwrap();

    check_stamps(&log, &[(0, None), (1, None), (2, None), (3, Some(7))]);
}

#[test]
fn commit_succeeds_after_transaction_data_is_retained_away() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    install_stamps(&mut log, 7, 1);
    append_transaction(&mut log, (1000, 0), &["old"]);
    log.append(&mut sample_batch(1)).unwrap();
    log.trim_to_offset(Offset(1)).unwrap();

    log.append(&mut commit_marker(1000, 0)).unwrap();

    let log_end = log.log_end_offset();
    check!(log.last_stable_offset(log_end) == log_end);
    check!(log.stamp_for_offset(Offset(0)) == None);
}

#[test]
fn supplied_commit_stamp_is_recorded_and_observed() {
    let (_dir, mut log) = stamped_test_log(1, 1);

    append_transaction(&mut log, (1000, 0), &["a", "b"]);
    log.append_with_commit_stamp(&mut commit_marker(1000, 0), 100)
        .unwrap();
    log.append(&mut sample_batch(1)).unwrap();

    check_stamps(&log, &[
        (0, Some(100)),
        (1, Some(100)),
        (2, None),
        (3, Some(101)),
    ]);
}

#[test]
fn supplied_commit_stamp_rejects_invalid_marker_paths() {
    let (_dir, mut log) = test_log();
    let error = log
        .append_with_commit_stamp(&mut commit_marker(1000, 0), 100)
        .unwrap_err();
    assert2::assert!(let LogError::InvalidArgument(_) = error);
    check!(log.log_end_offset() == Offset(0));

    install_stamps(&mut log, 1, 1);
    let error = log
        .append_with_commit_stamp(&mut abort_marker(1000, 0), 100)
        .unwrap_err();
    assert2::assert!(let LogError::InvalidArgument(_) = error);
    let error = log
        .append_with_commit_stamp(&mut sample_batch(1), 100)
        .unwrap_err();
    assert2::assert!(let LogError::InvalidArgument(_) = error);
    check!(log.log_end_offset() == Offset(0));
}

#[test]
fn replicated_commit_uses_supplied_stamp() {
    let (_dir, mut log) = stamped_test_log(1, 1);
    append_transaction(&mut log, (1000, 0), &["a"]);

    log.append_at_with_commit_stamp(&mut commit_marker(1000, 0), Offset(1), 40)
        .unwrap();
    log.append(&mut sample_batch(1)).unwrap();

    check_stamps(&log, &[(0, Some(40)), (1, None), (2, Some(41))]);
}

#[test]
fn installing_source_observes_durable_stamp_horizon() {
    let dir = tempdir().unwrap();
    {
        let mut log = open_log(dir.path());
        install_stamps(&mut log, 100, 1);
        log.append(&mut sample_batch(1)).unwrap();
    }

    let mut reopened = open_log(dir.path());
    install_stamps(&mut reopened, 1, 1);
    reopened.append(&mut sample_batch(1)).unwrap();

    check_stamps(&reopened, &[(0, Some(100)), (1, Some(101))]);
}

#[test]
fn startup_hides_legacy_append_stamp_for_open_transaction() {
    let dir = tempdir().unwrap();
    {
        let mut log = open_log(dir.path());
        append_transaction(&mut log, (1000, 0), &["a"]);
    }
    let path = dir.path().join("00000000000000000000.stampindex");
    let mut legacy = StampIndex::open(path).unwrap();
    legacy.append(stamp_entry(0, 0, 5)).unwrap();

    let mut reopened = open_log(dir.path());
    install_stamps(&mut reopened, 10, 1);
    check!(reopened.stamp_for_offset(Offset(0)) == None);

    reopened.append(&mut commit_marker(1000, 0)).unwrap();
    check!(reopened.stamp_for_offset(Offset(0)) == Some(10));
}

#[test]
fn transaction_commit_stamps_data_in_sealed_segments() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    install_stamps(&mut log, 50, 1);

    append_transaction(&mut log, (1000, 0), &["a"]); // segment 0
    append_transaction(&mut log, (1000, 0), &["b"]); // segment 1
    log.append(&mut commit_marker(1000, 0)).unwrap(); // segment 2

    check_stamps(&log, &[(0, Some(50)), (1, Some(50)), (2, None)]);
    assert2::assert!(
        crate::log::test_support::stamp_entries(dir.path(), 0)
            == [crate::test_support::stamp_entry(0, 0, 50)]
    );
    assert2::assert!(
        crate::log::test_support::stamp_entries(dir.path(), 1)
            == [crate::test_support::stamp_entry(1, 1, 50)]
    );
}

/// Wire-exactness invariance. This test appends an identical mixed
/// sequence to a stamp-enabled log and to an unstamped log: non-txn,
/// transactional data, commit marker, non-txn. Both logs give
/// byte-for-byte identical `.log` output, and identical assigned offsets
/// and LSO at every step. The stamp is only an added sidecar and cannot
/// change any client-facing coordinate. The high-watermark comes from
/// these values, so it too stays the same.
#[test]
fn stamping_does_not_change_offsets_lso_or_log_bytes() {
    // Build the same append script for both logs.
    fn script() -> Vec<RecordBatch> {
        vec![
            sample_batch(2),                      // non-txn, offsets 0..=1
            transactional_batch(1000, 0, &["a"]), // txn data, offset 2
            commit_marker(1000, 0),               // commit marker, offset 3
            sample_batch(3),                      // non-txn, offsets 4..=6
        ]
    }

    let (_dir_plain, mut plain) = test_log();

    let (_dir_stamped, mut stamped) = stamped_test_log(7, 3);

    let mut plain_bases = Vec::new();
    let mut stamped_bases = Vec::new();
    let mut plain_lsos = Vec::new();
    let mut stamped_lsos = Vec::new();
    for (mut pb, mut sb) in script().into_iter().zip(script()) {
        plain_bases.push(plain.append(&mut pb).unwrap());
        stamped_bases.push(stamped.append(&mut sb).unwrap());
        plain_lsos.push(plain.lso());
        stamped_lsos.push(stamped.lso());
    }

    // Identical offset assignment and LSO progression at every step.
    assert2::assert!(plain_bases == stamped_bases);
    assert2::assert!(plain_lsos == stamped_lsos);
    assert2::assert!(plain.log_end_offset() == stamped.log_end_offset());
    // The unstamped log never sees a stamp; the stamped one does.
    check!(plain.stamp_for_offset(Offset(0)) == None);
    check!(stamped.stamp_for_offset(Offset(0)) == Some(7));

    // Byte-for-byte identical client-facing `.log` output.
    let end = plain.log_end_offset();
    let plain_raw = plain.read_raw(Offset(0), end, mebibytes(10)).unwrap();
    let stamped_raw = stamped.read_raw(Offset(0), end, mebibytes(10)).unwrap();
    assert2::assert!(plain_raw.bytes == stamped_raw.bytes);
}
