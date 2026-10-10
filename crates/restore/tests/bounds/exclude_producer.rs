//! `--exclude-producer-id`, the one bound that decides a whole batch at once,
//! and the control batch it must never decide against.
//!
//! A batch carries a single producer id, so excluding a producer empties the
//! batch rather than filtering it. The emptied batch still claims its offset
//! range, and a transaction marker written by that same producer still
//! survives.

use assert2::check;
use krabka_ids::{Offset, ProducerId};
use krabka_log::LogConfig;

use crate::{
    archive::build_archive,
    fixtures::{commit_marker, producer_batch, transactional_batch, value_record},
    harness::{reopen, run_restore},
};

// ---------------------------------------------------------------------
// Scenario 7: --exclude-producer-id drops one producer's whole batch and
// leaves the other producer's batch untouched.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_producer_id_drops_only_that_producers_batch() {
    let mut fixture = vec![
        producer_batch(
            101,
            vec![value_record(0, "p101-1"), value_record(1, "p101-2")],
        ),
        producer_batch(202, vec![value_record(0, "p202-1")]),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) =
        run_restore(archive.path(), &["--exclude-producer-id", "101"]).await;

    let expected = crate::harness::empty_first_batch(&fixture);
    crate::harness::check_batches(&target_dir, 3, &expected);
}

// ---------------------------------------------------------------------
// Scenario 9: a control (transaction marker) batch must never be excluded.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_producer_id_never_drops_a_control_batch() {
    let mut fixture = vec![
        transactional_batch(77, vec![value_record(0, "tx-a"), value_record(1, "tx-b")]),
        commit_marker(77),
    ];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--exclude-producer-id", "77"]).await;

    let mut log = reopen(&target_dir, "orders", 0);
    check!(log.pending_transaction_start(ProducerId(77)) == None);
    let log_end = log.log_end_offset();
    check!(log.last_stable_offset(log_end) == log_end);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    // Desired: the producer's ordinary data is excluded (a bare header),
    // but its commit marker survives intact at its own original offset.
    let expected = crate::harness::empty_first_batch(&fixture);
    check!(read.batches == expected);
}
