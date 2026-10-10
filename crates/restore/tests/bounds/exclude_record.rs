//! The exclude flags that select individual records inside a batch:
//! `--exclude-key`, `--exclude-header`, and `--exclude-offset`.
//!
//! Each one forces a batch to be re-encoded without the records it matched,
//! and the claim every scenario here makes is the same: the surviving records
//! keep their own original offsets, and a batch archived after a filtered one
//! is untouched.

use assert2::check;
use krabka_ids::Offset;
use krabka_log::LogConfig;
use krabka_protocol::records::RecordBatch;

use crate::{
    archive::build_archive,
    fixtures::{headered_record, keyed_record, plain_batch, value_record},
    harness::{reopen, run_restore},
};

// ---------------------------------------------------------------------
// Scenario 3: --exclude-key drops a MIDDLE record without shifting
// anything else.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_drops_a_middle_record_without_shifting_later_offsets() {
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "keep-1", "v0"),
            keyed_record(1, "drop-2", "v1"),
            keyed_record(2, "keep-3", "v2"),
        ]),
        plain_batch(vec![value_record(0, "keep-4")]),
    ];
    let (_archive, _target, target_dir) =
        crate::harness::restore_excluding_keys(&mut fixture).await;

    let expected = vec![
        RecordBatch {
            records: vec![
                keyed_record(0, "keep-1", "v0"),
                keyed_record(2, "keep-3", "v2"),
            ],
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    crate::harness::check_batches(&target_dir, 4, &expected);
}

// ---------------------------------------------------------------------
// Scenario 4: --exclude-key emptying a WHOLE batch still claims its
// offsets, and an adjacent batch is untouched.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_matching_every_record_of_one_batch_still_claims_its_offsets() {
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "drop-a", "va"),
            keyed_record(1, "drop-b", "vb"),
        ]),
        plain_batch(vec![value_record(0, "keep-c")]),
    ];
    let (_archive, _target, target_dir) =
        crate::harness::restore_excluding_keys(&mut fixture).await;

    let expected = crate::harness::empty_first_batch(&fixture);
    crate::harness::check_batches(&target_dir, 3, &expected);
}

// ---------------------------------------------------------------------
// Scenario 5: dropping a batch's TRAILING record does not strand the
// batch archived after it, end to end.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_key_dropping_a_batchs_trailing_record_survives_the_full_pipeline() {
    // `materialize.rs`'s own
    // `a_filtered_batch_that_drops_its_trailing_record_does_not_strand_the_next_batch`
    // covers this at the `write_segment` level directly. This drives the
    // same shape through `discover -> verify -> bound -> materialize`.
    let mut fixture = vec![
        plain_batch(vec![
            keyed_record(0, "keep-x", "v0"),
            keyed_record(1, "drop-y", "v1"),
        ]),
        plain_batch(vec![value_record(0, "keep-z")]),
    ];
    let (_archive, _target, target_dir) =
        crate::harness::restore_excluding_keys(&mut fixture).await;

    let expected = vec![
        RecordBatch {
            records: vec![keyed_record(0, "keep-x", "v0")],
            ..fixture[0].clone()
        },
        fixture[1].clone(),
    ];
    crate::harness::check_batches(&target_dir, 3, &expected);
}

// ---------------------------------------------------------------------
// Scenario 6: --exclude-header matches on name AND value, never on name
// alone or on an absent header.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_header_matches_on_name_and_value_not_name_alone() {
    let mut fixture = vec![plain_batch(vec![
        headered_record(0, "v0", "trace", "bad-1"),
        headered_record(1, "v1", "trace", "good-1"),
        value_record(2, "v2"), // no headers at all
    ])];
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) =
        run_restore(archive.path(), &["--exclude-header", "trace=^bad"]).await;

    let expected = vec![RecordBatch {
        records: vec![
            headered_record(1, "v1", "trace", "good-1"),
            value_record(2, "v2"),
        ],
        ..fixture[0].clone()
    }];
    crate::harness::check_batches(&target_dir, 3, &expected);
}

// ---------------------------------------------------------------------
// Scenario 8: --exclude-offset's half-open and inclusive spellings
// exclude the same offsets, over the same archived data.
// ---------------------------------------------------------------------

#[tokio::test]
async fn exclude_offset_half_open_and_inclusive_spellings_agree() {
    let mut fixture = vec![plain_batch(vec![
        value_record(0, "keep-0"),
        value_record(1, "drop-1"),
        value_record(2, "drop-2"),
        value_record(3, "keep-3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);
    let expected = vec![RecordBatch {
        records: vec![value_record(0, "keep-0"), value_record(3, "keep-3")],
        ..fixture[0].clone()
    }];

    for spelling in ["orders:0=1..3", "orders:0=1..=2"] {
        let (_target, target_dir) =
            run_restore(archive.path(), &["--exclude-offset", spelling]).await;

        let log = reopen(&target_dir, "orders", 0);
        check!(log.log_end_offset() == Offset(4), "{spelling}");
        let read = log
            .read(Offset(0), LogConfig::default().segment_size)
            .expect("read back");
        check!(read.batches == expected, "{spelling}");
    }
}
