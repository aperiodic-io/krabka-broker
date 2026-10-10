//! Fixtures the bound unit tests share: a `RestoreArgs` parsed from the bound
//! flags under test, the records and batches a test judges, and the harness
//! that runs `Predicates::decide_batch` and `Predicates::decide_record` over
//! one batch the way `materialize` does.

use bytes::{Bytes, BytesMut};
use krabka_ids::ProducerId;
use krabka_protocol::{
    DecodeBorrow as _,
    records::{Attributes, Record, RecordBatch, RecordBatchBorrowed, RecordHeader},
};

use crate::{
    args::{PartitionRef, RestoreArgs},
    bound::{BatchDecision, Predicates, RecordDecision, record_coordinates},
    error::RestoreError,
};

const BASE_OFFSET: i64 = 1_000;
pub(super) const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

pub(super) use crate::args::test_support::partition;

/// Parses `RestoreArgs` the same way the binary does, with a fixed
/// archive source and target so a test only has to state the bound
/// flags under test.
pub(super) fn args_from(extra: &[&str]) -> RestoreArgs {
    crate::args::test_support::args_from(extra).expect("valid command line")
}

pub(super) fn predicates(extra: &[&str]) -> Predicates {
    Predicates::from_args(&args_from(extra)).expect("valid predicates")
}

/// A minimal record at `offset_delta`, with an arbitrary value and no key
/// or headers. Override fields with struct-update syntax.
pub(super) fn record(offset_delta: i32) -> Record {
    Record {
        offset_delta,
        value: Some(Bytes::from_static(b"v")),
        ..Record::default()
    }
}

/// Numbered records with the exact optional static keys under test.
pub(super) fn keyed_records(keys: &[Option<&'static [u8]>]) -> Vec<Record> {
    keys.iter()
        .enumerate()
        .map(|(offset, key)| Record {
            key: key.map(Bytes::from_static),
            ..record(i32::try_from(offset).unwrap())
        })
        .collect()
}

pub(super) fn timestamped_records(deltas: &[i64]) -> Vec<Record> {
    deltas
        .iter()
        .enumerate()
        .map(|(offset, &timestamp_delta)| Record {
            timestamp_delta,
            ..record(i32::try_from(offset).unwrap())
        })
        .collect()
}

pub(super) fn header(name: &str, value: &[u8]) -> RecordHeader {
    RecordHeader {
        key: name.to_owned(),
        value: Some(Bytes::copy_from_slice(value)),
    }
}

// Every header field holds a distinctive value, matching
// `krabka_log::filter::tests::batch`'s convention, so a mistaken swap
// between two header fields would show up as a wrong test outcome.
pub(super) fn batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    RecordBatch {
        base_offset: BASE_OFFSET,
        partition_leader_epoch: 3,
        attributes: Attributes::default(),
        last_offset_delta: records.iter().map(|r| r.offset_delta).max().unwrap_or(0),
        base_timestamp: BASE_TIMESTAMP,
        max_timestamp: BASE_TIMESTAMP
            + records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0),
        producer_id,
        producer_epoch: 0,
        base_sequence: 0,
        records,
    }
}

fn encode(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).expect("encode");
    buf.to_vec()
}

fn borrow(bytes: &[u8]) -> RecordBatchBorrowed<'_> {
    let mut cursor = bytes;
    // `version` is unused by the v2 batch decoder; any value does.
    RecordBatchBorrowed::decode_borrow(&mut cursor, 0)
        .expect("decode a borrowed batch back out of its own encoding")
}

/// Runs both `decide_batch` and `decide_record` for every record, the way
/// `materialize.rs` would: a batch-level verdict, and the per-record
/// verdicts in batch order.
pub(super) fn decide(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
) -> (BatchDecision, Vec<RecordDecision>) {
    try_decide(predicates, partition, owned).expect("valid record coordinates")
}

/// Assert both independent verdicts returned by the batch/record decision harness.
pub(super) fn check_decision(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
    expected_batch: BatchDecision,
    expected_records: &[RecordDecision],
) {
    let (batch_decision, records) = decide(predicates, partition, owned);
    assert2::check!(batch_decision == expected_batch);
    assert2::check!(records == expected_records);
}

pub(super) fn try_decide(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
) -> Result<(BatchDecision, Vec<RecordDecision>), RestoreError> {
    let encoded = encode(owned);
    let borrowed = borrow(&encoded);
    let header = borrowed.header();
    let producer_id = ProducerId(header.producer_id.get());

    let batch_decision = predicates.decide_batch(partition, &borrowed)?;
    let record_decisions = borrowed
        .iter()
        .map(|parsed| {
            let record = parsed?;
            let (offset, timestamp_ms) = record_coordinates(header, &record)?;
            Ok(predicates.decide_record(partition, offset, timestamp_ms, producer_id, &record))
        })
        .collect::<Result<Vec<_>, RestoreError>>()?;
    Ok((batch_decision, record_decisions))
}

pub(super) fn check_record_parse_error(
    predicates: &Predicates,
    partition: &PartitionRef,
    owned: &RecordBatch,
) {
    let error = try_decide(predicates, partition, owned).unwrap_err();
    assert2::check!(matches!(
        error,
        RestoreError::Records(krabka_protocol::records::RecordsError::RecordParse(_))
    ));
}
