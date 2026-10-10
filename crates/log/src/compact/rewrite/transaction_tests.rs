//! Unit tests for how the rewrite treats transactions: the records of an
//! aborted transaction, the abort markers and `.txnindex` entries that stay,
//! and the per-transaction aging of commit markers.

use std::{collections::HashMap, fs};

use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::millis;

use super::{
    CleanedTransactionMetadata, ProducerLastRecord, RewriteOutput, RewriteRetention, Segment,
    tests::decode_all,
};
use crate::{
    compact::{
        build_offset_map,
        test_support::{
            RETENTION, control_batch, make_record, rewrite_all, rewrite_with_map, round_over,
            transactional_record, write_sealed_batches,
        },
    },
    txn_index::{AbortedTxn, TxnIndex},
};

fn aborted_entry(producer_id: i64, start: i64, marker: i64) -> AbortedTxn {
    AbortedTxn {
        start_offset: Offset(start),
        last_offset: Offset(marker),
        producer_id: ProducerId(producer_id),
        last_stable_offset: Offset(marker + 1),
    }
}

fn rewrite_at(
    dir: &std::path::Path,
    segment_refs: &[&Segment],
    txn: &mut CleanedTransactionMetadata,
    now_ms: i64,
    active: &HashMap<ProducerId, ProducerLastRecord>,
) -> RewriteOutput {
    rewrite_all(
        dir,
        segment_refs,
        txn,
        RewriteRetention {
            now_ms,
            delete_retention: millis(50),
        },
        active,
    )
}

/// Kafka's `Cleaner` drops every record of an aborted transaction and keeps
/// the newest committed one. Key `k` holds a committed value at offset 5 and
/// an aborted one at offset 10. The committed value must survive, and the
/// abort marker stays, with its `.txnindex` entry, because the pass still met
/// the aborted batches.
#[test]
fn an_aborted_transactions_records_are_dropped_and_the_committed_value_survives() {
    let dir = tempfile::tempdir().unwrap();
    let committed = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        offset: crate::Offset(5),
        payload: (b"k", b"committed"),
        ..Default::default()
    });
    let commit_marker = control_batch(6, 1000, 1 /* COMMIT */);
    let aborted_k = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        offset: crate::Offset(10),
        producer: crate::ProducerId(2000),
        payload: (b"k", b"aborted"),
    });
    let aborted_j = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        offset: crate::Offset(11),
        producer: crate::ProducerId(2000),
        payload: (b"j", b"aborted"),
    });
    let abort_marker = control_batch(12, 2000, 0 /* ABORT */);
    let seg = write_sealed_batches(
        dir.path(),
        &[
            committed.clone(),
            commit_marker.clone(),
            aborted_k,
            aborted_j,
            abort_marker.clone(),
        ],
    );
    let segment_refs = vec![&seg];
    let entry = aborted_entry(2000, 10, 12);
    let map = build_offset_map(&segment_refs, vec![entry], None).unwrap();
    assert2::assert!(map.get(b"k".as_slice()) == Some(&Offset(5)));
    assert2::assert!(!map.contains_key(b"j".as_slice()));

    let mut txn = CleanedTransactionMetadata::default();
    txn.add_aborted_transactions([entry]);
    let out = rewrite_with_map(
        dir.path(),
        &segment_refs,
        &map,
        &mut txn,
        RewriteRetention {
            now_ms: 0,
            delete_retention: RETENTION,
        },
        round_over(&segment_refs, &HashMap::new()),
    );

    let batches = decode_all(&fs::read(&out.log_swap).unwrap());
    assert2::assert!(batches == vec![committed, commit_marker, abort_marker]);
    let swap = out
        .txnindex_swap
        .expect("the abort marker is kept, so its entry is kept");
    assert2::assert!(TxnIndex::open(swap).unwrap().entries() == [entry]);
}

/// A transaction can span two output groups of one pass. The aborted batch is
/// in the first group and its abort marker in the second, so the tracker has to
/// carry what it saw in group one into group two: the marker is kept, and its
/// entry lands in the second group's `.txnindex` only.
#[test]
fn an_aborted_transaction_spanning_two_groups_keeps_its_marker_and_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let aborted = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        producer: crate::ProducerId(2000),
        payload: (b"k", b"aborted"),
        ..Default::default()
    });
    let abort_marker = control_batch(1, 2000, 0 /* ABORT */);
    let seg_a = write_sealed_batches(dir.path(), std::slice::from_ref(&aborted));
    let seg_b = write_sealed_batches(dir.path(), std::slice::from_ref(&abort_marker));
    let entry = aborted_entry(2000, 0, 1);

    let mut txn = CleanedTransactionMetadata::default();
    txn.add_aborted_transactions([entry]);
    let out_a = rewrite_at(dir.path(), &[&seg_a], &mut txn, 0, &HashMap::new());
    assert2::assert!(
        out_a.txnindex_swap.is_none(),
        "the marker is not in the first group, so neither is the entry"
    );

    txn.add_aborted_transactions([entry]);
    let out_b = rewrite_at(dir.path(), &[&seg_b], &mut txn, 0, &HashMap::new());
    let batches = decode_all(&fs::read(&out_b.log_swap).unwrap());
    assert2::assert!(batches == vec![abort_marker]);
    let swap = out_b
        .txnindex_swap
        .expect("the first group's aborted batch keeps the second group's marker");
    assert2::assert!(TxnIndex::open(swap).unwrap().entries() == [entry]);
}

/// Kafka decides marker retention per transaction, not per producer
/// (`CleanedTransactionMetadata.onControlBatchRead`). Producer 1000 keeps its
/// id across two transactions. The first one's data is gone, and only its
/// marker is left; the second one still has live data. The first marker has no
/// batch of its transaction behind it, so it gets a delete horizon and later
/// goes, while the second marker stays plain because its data is right in
/// front of it. A producer-wide survivor set kept both markers forever.
///
/// Each case is `(label, first marker horizon, now, expected (base offset,
/// delete horizon) of every output batch)`.
#[test]
fn a_marker_ages_out_once_its_own_transactions_data_is_gone() {
    for (label, first_marker_horizon, now_ms, want) in [
        (
            "no horizon yet: stamp now + delete.retention.ms",
            None,
            100,
            vec![(0, Some(150)), (1, None), (2, None)],
        ),
        (
            "horizon not reached: keep as it is",
            Some(500),
            100,
            vec![(0, Some(500)), (1, None), (2, None)],
        ),
        (
            "horizon reached: drop the marker, keep the live data and its marker",
            Some(100),
            200,
            vec![(1, None), (2, None)],
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut old_marker = control_batch(0, 1000, 1 /* COMMIT */);
        if let Some(horizon) = first_marker_horizon {
            old_marker.base_timestamp = horizon;
            old_marker.attributes = old_marker.attributes.with_delete_horizon(true);
        }
        let live = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
            offset: crate::Offset(1),
            payload: (b"k1", b"v2"),
            ..Default::default()
        });
        let live_marker = control_batch(2, 1000, 1 /* COMMIT */);
        let seg = write_sealed_batches(dir.path(), &[old_marker, live, live_marker]);
        let mut txn = CleanedTransactionMetadata::default();
        let out = rewrite_at(dir.path(), &[&seg], &mut txn, now_ms, &HashMap::new());
        let batches = decode_all(&fs::read(&out.log_swap).unwrap());
        let got: Vec<(i64, Option<i64>)> = batches
            .iter()
            .map(|batch| (batch.base_offset, batch.delete_horizon_ms()))
            .collect();
        assert2::assert!(got == want, "case {label}");
    }
}

/// Kafka reads a batch before it filters the batch's records, so the marker of
/// a transaction whose records all die in this pass is kept, and only the next
/// pass stamps it. That makes the delete horizon start one pass after the data
/// is gone.
#[test]
fn a_marker_is_stamped_the_pass_after_its_data_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let superseded = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        payload: (b"k", b"old"),
        ..Default::default()
    });
    let marker = control_batch(1, 1000, 1 /* COMMIT */);
    let newer = RecordBatch {
        base_offset: 2,
        last_offset_delta: 0,
        producer_id: -1,
        records: vec![make_record(0, Some(b"k"), Some(b"new"))],
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[superseded, marker.clone(), newer.clone()]);
    let mut txn = CleanedTransactionMetadata::default();
    let first_pass = rewrite_at(dir.path(), &[&seg], &mut txn, 100, &HashMap::new());
    let first = decode_all(&fs::read(&first_pass.log_swap).unwrap());
    assert2::assert!(first == vec![marker, newer]);

    // The second pass reads the first pass's output: no batch of the
    // transaction is left, so the marker is stamped.
    let second_dir = tempfile::tempdir().unwrap();
    let seg = write_sealed_batches(second_dir.path(), &first);
    let mut txn = CleanedTransactionMetadata::default();
    let second_pass = rewrite_at(second_dir.path(), &[&seg], &mut txn, 100, &HashMap::new());
    let second = decode_all(&fs::read(&second_pass.log_swap).unwrap());
    let horizons: Vec<Option<i64>> = second.iter().map(RecordBatch::delete_horizon_ms).collect();
    assert2::assert!(horizons == vec![Some(150), None]);
}
