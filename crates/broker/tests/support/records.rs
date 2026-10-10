//! Record fixtures that retain the protocol's complete default headers.

use bytes::Bytes;
use krabka_protocol::records::{Record, RecordBatch};

pub fn value_record(offset_delta: i32, value: Option<Bytes>) -> Record {
    Record {
        offset_delta,
        value,
        ..Default::default()
    }
}

/// Keep caller-specified offsets and values separate from batch header overrides.
/// The default partition leader epoch here is zero, as in the wire protocol.
pub fn batch_from_records(records: Vec<Record>) -> RecordBatch {
    RecordBatch {
        records,
        ..Default::default()
    }
}

/// Sequential records whose keys and values are absent, with the original empty-batch delta.
///
/// # Panics
/// Panics on the original signed offset arithmetic overflow.
pub fn empty_record_batch(n: i32) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    for offset_delta in 0..n {
        batch.records.push(Record {
            offset_delta,
            ..Default::default()
        });
    }
    batch
}

/// The idempotent fixtures' full producer header and sequential owned string values.
///
/// # Panics
/// Panics if the count or an index does not fit its original i32 field.
/// A producer's sequence coordinate, independent of log offsets.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub struct ProducerSequence(pub i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ProducerValuesSetup<'a> {
    #[default(krabka_ids::ProducerId(7))]
    pub pid: krabka_ids::ProducerId,
    pub epoch: crate::support::transactions::ProducerEpoch,
    pub base_seq: ProducerSequence,
    #[default(&["v"])]
    pub values: &'a [&'a str],
}

pub fn producer_values_batch(setup: ProducerValuesSetup<'_>) -> RecordBatch {
    let ProducerValuesSetup {
        pid,
        epoch,
        base_seq,
        values,
    } = setup;
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let mut records = Vec::with_capacity(values.len());
    for (i, value) in values.iter().enumerate() {
        records.push(value_record(
            i32::try_from(i).expect("index fits i32"),
            Some(Bytes::from(value.to_string())),
        ));
    }
    RecordBatch {
        producer_id: pid.0,
        producer_epoch: epoch.0,
        base_sequence: base_seq.0,
        last_offset_delta: n - 1,
        max_timestamp: i64::from(n),
        ..batch_from_records(records)
    }
}

/// Count v2 records in optional fetch payloads; absent and legacy payloads contribute zero.
pub fn record_count(payload: Option<&krabka_protocol::records::RecordsPayload>) -> usize {
    payload
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .map_or(0, |batches| {
            batches.iter().map(|batch| batch.records.len()).sum()
        })
}

// Current replication-fixture timestamp with the original checked epoch conversion.
//
// # Panics
// Panics if the clock is before the epoch or its milliseconds do not fit an i64.
krabka_macros::unix_millis_fixture!(
    pub now_ms, "clock after the epoch", "milliseconds fit an i64"
);

/// Lazily decode each metadata batch so callers retain their image-application order.
pub fn metadata_batches(mut wire: &[u8]) -> impl Iterator<Item = RecordBatch> + '_ {
    std::iter::from_fn(move || {
        if wire.is_empty() {
            None
        } else {
            Some(RecordBatch::decode(&mut wire).expect("decode a metadata batch"))
        }
    })
}

/// Number of records in a transaction-version probe batch.
#[derive(Clone, Copy)]
pub struct ProbeRecordCount(pub i32);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchTransaction {
    #[default]
    Transactional,
    Ordinary,
}

/// The producer headers and record count needed by transaction-version probes.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TransactionProbeBatchSetup {
    pub producer: crate::support::transactions::ProducerIdentity,
    pub sequence: ProducerSequence,
    #[default(ProbeRecordCount(1))]
    pub records: ProbeRecordCount,
    pub transaction: BatchTransaction,
}

pub fn transaction_probe_batch(setup: TransactionProbeBatchSetup) -> RecordBatch {
    RecordBatch {
        attributes: krabka_protocol::records::Attributes::default()
            .with_transactional(setup.transaction == BatchTransaction::Transactional),
        producer_id: setup.producer.id.0,
        producer_epoch: setup.producer.epoch.0,
        base_sequence: setup.sequence.0,
        last_offset_delta: setup.records.0 - 1,
        max_timestamp: 1,
        ..batch_from_records(
            (0..setup.records.0)
                .map(|offset| value_record(offset, Some(Bytes::from_static(b"v"))))
                .collect(),
        )
    }
}
