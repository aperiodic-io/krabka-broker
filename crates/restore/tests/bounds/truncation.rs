//! The two bounds that name where the restore stops: `--to-offset` and
//! `--to-timestamp`.
//!
//! Both truncate a tail rather than punching a hole, so what they prove is the
//! inclusivity of the boundary itself and that everything after it is absent
//! from the restored log.

use assert2::{assert, check};
use krabka_ids::Offset;
use krabka_log::LogConfig;
use krabka_protocol::records::RecordBatch;
use krabka_restore::{RestoreError, restore};

use crate::{
    archive::build_archive,
    fixtures::{BASE_TIMESTAMP, plain_batch, timestamped_record, value_record},
    harness::{reopen, restore_args, run_restore},
};

// ---------------------------------------------------------------------
// Scenario 1: --to-offset is boundary-inclusive and truncates the tail.
// ---------------------------------------------------------------------

#[tokio::test]
async fn to_offset_bound_is_inclusive_and_truncates_the_tail() {
    let mut fixture: Vec<RecordBatch> = (0..10)
        .map(|i: i32| plain_batch(vec![value_record(0, &format!("v{i}"))]))
        .collect();
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--to-offset", "orders:0=5"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(6));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    check!(read.batches == fixture[..=5].to_vec());
}

#[tokio::test]
async fn to_offset_filters_records_past_a_bound_inside_one_batch() {
    let mut fixture = vec![plain_batch(vec![
        value_record(0, "v0"),
        value_record(1, "v1"),
        value_record(2, "v2"),
        value_record(3, "v3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--to-offset", "orders:0=2"]).await;

    let log = reopen(&target_dir, "orders", 0);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![RecordBatch {
        records: fixture[0].records[..=2].to_vec(),
        ..fixture[0].clone()
    }];
    check!(read.batches == expected);
}

// ---------------------------------------------------------------------
// Scenario 1b: a bound naming a partition the archive lacks is rejected.
// ---------------------------------------------------------------------

#[tokio::test]
async fn bounds_on_absent_partitions_are_rejected() {
    for (extra, expected_topic, expected_partition) in [
        (&["--to-offset", "orders:3=100"][..], "orders", 3),
        // Restricting the scan to absent payments must report UnknownPartition
        // before EmptyArchive, so the error still names the mistaken bound.
        (
            &["--topic", "payments", "--to-offset", "payments:0=10"][..],
            "payments",
            0,
        ),
    ] {
        let mut fixture = vec![plain_batch(vec![value_record(0, "v0")])];
        let archive = build_archive("orders", 0, &mut fixture);
        let target = tempfile::tempdir().expect("target tempdir");
        let target_dir = target.path().join("restored");
        let args = restore_args(archive.path(), &target_dir, extra);
        let error = restore(&args).await.expect_err("unknown partition");
        assert!(let RestoreError::UnknownPartition { topic, partition } = error);
        check!(topic == expected_topic);
        check!(partition == expected_partition);
    }
}

#[tokio::test]
async fn to_timestamp_keeps_only_records_strictly_before_the_bound() {
    let mut fixture = vec![plain_batch(vec![
        timestamped_record(0, 0, "t0"),
        timestamped_record(1, 10, "t1"),
        timestamped_record(2, 20, "t2"),
        timestamped_record(3, 30, "t3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);
    let bound = (BASE_TIMESTAMP + 25).to_string();

    let (_target, target_dir) = run_restore(archive.path(), &["--to-timestamp", &bound]).await;

    let log = reopen(&target_dir, "orders", 0);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![RecordBatch {
        records: vec![
            timestamped_record(0, 0, "t0"),
            timestamped_record(1, 10, "t1"),
            timestamped_record(2, 20, "t2"),
        ],
        ..fixture[0].clone()
    }];
    check!(read.batches == expected);
}
