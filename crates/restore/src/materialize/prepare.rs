//! Deciding one archived record batch's fate under the bound, and shaping what
//! the target log is asked to append for it.
//!
//! Segment writing hands every decoded batch to this module and gets back a
//! `PreparedBatch`: either the archived bytes untouched, for the log's
//! zero-copy verbatim path, or an owned `RecordBatch`, for the log's
//! decode-and-append path. It comes with the `BatchTally` that says how the
//! batch folds into the segment's counts. Nothing here touches the log, so a
//! dry run takes exactly the path a real run takes.

use bytes::{Bytes, BytesMut};
use krabka_ids::ProducerId;
use krabka_log::{FilteredBatch, VerbatimBatch, filter_batch};
use krabka_protocol::records::{
    Attributes, RecordBatch, RecordBatchBorrowed, RecordBatchHeader, RecordsError,
    validate_one_v2_batch,
};

use crate::{
    args::PartitionRef,
    bound::{BatchDecision, Predicates, RecordDecision, record_coordinates, timestamp_type},
    error::RestoreError,
};

/// What one archived batch becomes when it is appended to the target log: the exact bytes the log's zero-copy path needs, or an owned batch for the log's decode-and-append path.
pub(crate) enum PreparedBatch {
    Verbatim(VerbatimBatch),
    Owned {
        batch: RecordBatch,
        encoded_len: usize,
    },
}

impl PreparedBatch {
    pub(crate) fn last_offset_delta(&self) -> i32 {
        match self {
            Self::Verbatim(batch) => batch.last_offset_delta,
            Self::Owned { batch, .. } => batch.last_offset_delta,
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Self::Verbatim(batch) => batch.bytes.len(),
            Self::Owned { encoded_len, .. } => *encoded_len,
        }
    }
}

/// How [`prepare_batch`]'s outcome folds into
/// [`SegmentOutcome`](super::SegmentOutcome)'s counts.
pub(crate) enum BatchTally {
    Kept,
    Rewritten { kept: u64, dropped: u64 },
    Emptied,
}

/// Decide one archived batch's fate under `predicates`, and prepare what gets appended to the target log without touching the log itself, so `--dry-run` can share this exact path with a real run.
pub(crate) fn prepare_batch(
    partition_ref: &PartitionRef,
    predicates: &Predicates,
    batch: &RecordBatchBorrowed<'_>,
    batch_bytes: Bytes,
    records_in_batch: u64,
) -> Result<(PreparedBatch, BatchTally), RestoreError> {
    let header = batch.header();
    match predicates.decide_batch(partition_ref, batch)? {
        BatchDecision::Keep if batch.attributes().is_control_batch() => {
            Ok((prepare_owned_batch(batch.to_owned()?)?, BatchTally::Kept))
        }
        BatchDecision::Keep => {
            let verbatim = VerbatimBatch::from_header(header, batch_bytes);
            Ok((PreparedBatch::Verbatim(verbatim), BatchTally::Kept))
        }
        BatchDecision::Empty => Ok((
            prepare_owned_batch(bare_header_batch(header))?,
            BatchTally::Emptied,
        )),
        BatchDecision::Filter => prepare_filtered_batch(
            partition_ref,
            predicates,
            batch,
            batch_bytes,
            records_in_batch,
        ),
    }
}

/// The [`BatchDecision::Filter`] arm of [`prepare_batch`], split out because it is the one path that has to decide record by record before it can decide the whole batch.
fn prepare_filtered_batch(
    partition_ref: &PartitionRef,
    predicates: &Predicates,
    batch: &RecordBatchBorrowed<'_>,
    batch_bytes: Bytes,
    records_in_batch: u64,
) -> Result<(PreparedBatch, BatchTally), RestoreError> {
    let header = batch.header();
    let mut keep_flags = Vec::with_capacity(usize::try_from(records_in_batch).unwrap_or(0));
    for record in batch {
        let record = record?;
        let (record_offset, timestamp_ms) = record_coordinates(header, &record)?;
        let producer_id = ProducerId(header.producer_id.get());
        let decision = predicates.decide_record(
            partition_ref,
            record_offset,
            timestamp_ms,
            producer_id,
            &record,
        );
        keep_flags.push(decision == RecordDecision::Keep);
    }

    let owned = batch.to_owned()?;
    let mut flags = keep_flags.into_iter();
    let filtered = filter_batch(&owned, |_record| {
        flags.next().expect(
            "filter_batch calls keep exactly once per record, matching the per-record walk \
             that built keep_flags",
        )
    });

    Ok(match filtered {
        FilteredBatch::Unchanged => {
            let verbatim = VerbatimBatch::from_header(header, batch_bytes);
            (PreparedBatch::Verbatim(verbatim), BatchTally::Kept)
        }
        FilteredBatch::Filtered(rewritten) => {
            let kept = u64::try_from(rewritten.records.len()).unwrap_or(0);
            let dropped = records_in_batch.saturating_sub(kept);
            // The generic record filter shrinks the span to its survivors.
            // Restore preserves the archived span instead, as Kafka's
            // MemoryRecords.buildRetainedRecordsInto does with overrideLastOffset:
            // last_offset_delta is also the producer's sequence delta. Losing
            // the tail must not change retry identity or the next expected
            // sequence after producer-state reconstruction.
            let rewritten = RecordBatch {
                last_offset_delta: owned.last_offset_delta,
                ..rewritten
            };
            (
                prepare_owned_batch(rewritten)?,
                BatchTally::Rewritten { kept, dropped },
            )
        }
        FilteredBatch::Empty => {
            let bare = RecordBatch {
                records: Vec::new(),
                ..owned
            };
            (prepare_owned_batch(bare)?, BatchTally::Emptied)
        }
    })
}

/// Validate and pre-encode an owned rewrite before either dry-run accounting
/// or target-log mutation. This is the adapter boundary for the two restore
/// rewrite proofs. Encoding recomputes `records_count` and CRC; validating the
/// encoded bytes proves that the exact framed size counted by dry-run carries
/// that fresh checksum.
fn prepare_owned_batch(batch: RecordBatch) -> Result<PreparedBatch, RestoreError> {
    let records_count = i32::try_from(batch.records.len())
        .map_err(|_| invalid_rewrite("retained record count exceeds i32"))?;
    krabka_verified::restore_rewritten_batch_header(
        krabka_verified::RestoreLayout {
            base_offset: batch.base_offset,
            last_offset_delta: batch.last_offset_delta,
            records_count,
        },
        krabka_verified::RestoreProducer {
            control: batch.attributes.is_control_batch(),
            transactional: batch.attributes.is_transactional(),
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
        },
    )
    .ok_or_else(|| invalid_rewrite("header or producer fields are inconsistent"))?;

    let frame = krabka_verified::RestoreBatchFrame {
        base_offset: batch.base_offset,
        last_offset_delta: batch.last_offset_delta,
        timestamp_type: timestamp_type(batch.attributes),
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
    };
    let mut previous_offset_delta = None;
    for record in &batch.records {
        krabka_verified::restore_rewritten_record(
            previous_offset_delta,
            frame,
            krabka_verified::RestoreRecordDeltas {
                offset_delta: record.offset_delta,
                timestamp_delta: record.timestamp_delta,
            },
        )
        .ok_or_else(|| invalid_rewrite("record delta or timestamp contradicts the header"))?;
        previous_offset_delta = Some(record.offset_delta);
    }

    let mut encoded = BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut encoded)?;
    let validated = validate_one_v2_batch(&encoded)?;
    if validated.total_len != encoded.len()
        || validated.header.records_count.get() != records_count
        || validated.header.base_offset.get() != batch.base_offset
        || validated.header.partition_leader_epoch.get() != batch.partition_leader_epoch
        || validated.header.attributes.get() != batch.attributes.0
        || validated.header.last_offset_delta.get() != batch.last_offset_delta
        || validated.header.base_timestamp.get() != batch.base_timestamp
        || validated.header.max_timestamp.get() != batch.max_timestamp
        || validated.header.producer_id.get() != batch.producer_id
        || validated.header.producer_epoch.get() != batch.producer_epoch
        || validated.header.base_sequence.get() != batch.base_sequence
    {
        return Err(invalid_rewrite(
            "encoded framing does not reproduce the admitted header",
        ));
    }

    Ok(PreparedBatch::Owned {
        batch,
        encoded_len: encoded.len(),
    })
}

fn invalid_rewrite(reason: &str) -> RestoreError {
    RecordsError::RecordParse(format!("invalid restored batch rewrite: {reason}")).into()
}

/// Build a zero-record batch that claims `header`'s archived offset range without holding any of its records: `base_offset` and `last_offset_delta` are copied unchanged, so the target log's end offset still advances by the batch's full archived span. See [`BatchDecision::Empty`].
fn bare_header_batch(header: &RecordBatchHeader) -> RecordBatch {
    RecordBatch {
        base_offset: header.base_offset.get(),
        partition_leader_epoch: header.partition_leader_epoch.get(),
        attributes: Attributes(header.attributes.get()),
        last_offset_delta: header.last_offset_delta.get(),
        base_timestamp: header.base_timestamp.get(),
        max_timestamp: header.max_timestamp.get(),
        producer_id: header.producer_id.get(),
        producer_epoch: header.producer_epoch.get(),
        base_sequence: header.base_sequence.get(),
        records: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::{Offset, ProducerId};
    use krabka_log::{Log, LogConfig};
    use krabka_protocol::records::{Attributes, Record, TimestampType};

    use super::{
        BatchTally, Bytes, BytesMut, PartitionRef, Predicates, PreparedBatch, RecordBatch,
        RecordBatchBorrowed, prepare_batch, prepare_owned_batch, validate_one_v2_batch,
    };
    use crate::materialize::test_support::args_from;

    fn rewrite(records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            base_offset: 10,
            partition_leader_epoch: 2,
            attributes: Attributes::default(),
            last_offset_delta: 4,
            base_timestamp: 100,
            max_timestamp: 110,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }

    fn record(offset_delta: i32, timestamp_delta: i64) -> Record {
        Record {
            offset_delta,
            timestamp_delta,
            ..Record::default()
        }
    }

    #[test]
    fn owned_rewrite_preflights_exact_count_crc_and_size() {
        let input = rewrite(vec![record(0, 0), record(3, 8)]);
        let first = prepare_owned_batch(input.clone()).expect("valid rewrite");
        let second = prepare_owned_batch(input).expect("repeat is deterministic");
        check!(first.encoded_len() == second.encoded_len());

        let PreparedBatch::Owned { batch, encoded_len } = first else {
            panic!("rewrite must stay owned");
        };
        let mut encoded = BytesMut::new();
        batch.encode(&mut encoded).expect("re-encode");
        let validated = validate_one_v2_batch(&encoded).expect("fresh CRC");
        check!(validated.total_len == encoded_len);
        check!(validated.header.records_count.get() == 2);
        check!(validated.header.last_offset_delta.get() == 4);
    }

    #[test]
    fn owned_rewrite_rejects_stale_malformed_and_overflowing_headers() {
        let mut stale_timestamp = rewrite(vec![record(0, 11)]);
        check!(prepare_owned_batch(stale_timestamp.clone()).is_err());

        stale_timestamp.records[0].timestamp_delta = 0;
        stale_timestamp.records.push(record(0, 1));
        check!(prepare_owned_batch(stale_timestamp).is_err());

        let mut overflow = rewrite(Vec::new());
        overflow.base_offset = i64::MAX;
        overflow.last_offset_delta = 0;
        check!(prepare_owned_batch(overflow).is_err());
    }

    /// Kafka reads a `LogAppendTime` record's timestamp as the batch
    /// `max_timestamp` and leaves the producer's deltas untouched, so a
    /// producer clock ahead of the broker is legal there and a contradiction
    /// under `CreateTime`.
    #[test]
    fn owned_rewrite_checks_timestamps_by_kafka_timestamp_type() {
        for (name, timestamp_type, admitted) in [
            ("log append time", TimestampType::LogAppendTime, true),
            ("create time", TimestampType::CreateTime, false),
        ] {
            let mut batch = rewrite(vec![record(0, 0), record(3, 50)]);
            batch.attributes = batch.attributes.with_timestamp_type(timestamp_type);
            batch.base_timestamp = 2_000;
            batch.max_timestamp = 1_000;
            check!(prepare_owned_batch(batch).is_ok() == admitted, "{name}");
        }
    }

    #[test]
    fn owned_rewrite_rejects_control_and_illegal_producer_semantics() {
        let mut control = rewrite(vec![record(0, 0)]);
        control.attributes = control.attributes.with_control(true);
        control.last_offset_delta = 0;
        check!(prepare_owned_batch(control).is_ok());

        let mut transaction_marker = rewrite(vec![record(0, 0)]);
        transaction_marker.attributes = transaction_marker
            .attributes
            .with_transactional(true)
            .with_control(true);
        transaction_marker.last_offset_delta = 0;
        transaction_marker.producer_id = 7;
        transaction_marker.producer_epoch = 2;
        transaction_marker.base_sequence = -1;
        check!(prepare_owned_batch(transaction_marker).is_ok());

        let mut transactional_without_producer = rewrite(vec![record(0, 0)]);
        transactional_without_producer.attributes = transactional_without_producer
            .attributes
            .with_transactional(true);
        check!(prepare_owned_batch(transactional_without_producer).is_err());

        let mut malformed_producer = rewrite(vec![record(0, 0)]);
        malformed_producer.producer_id = 7;
        malformed_producer.producer_epoch = -1;
        malformed_producer.base_sequence = 0;
        check!(prepare_owned_batch(malformed_producer).is_err());

        let mut malformed_marker = rewrite(vec![record(0, 0)]);
        malformed_marker.attributes = malformed_marker
            .attributes
            .with_transactional(true)
            .with_control(true);
        malformed_marker.producer_id = 7;
        malformed_marker.producer_epoch = 0;
        malformed_marker.base_sequence = 0;
        check!(prepare_owned_batch(malformed_marker).is_err());
    }

    #[test]
    fn kept_transaction_marker_uses_owned_append_and_closes_the_transaction() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let predicates = Predicates::from_args(&args).expect("predicates");
        let partition_ref = PartitionRef {
            topic: "orders".to_owned(),
            partition: 0,
        };

        let mut data = rewrite(vec![record(0, 0)]);
        data.base_offset = 0;
        data.last_offset_delta = 0;
        data.attributes = data.attributes.with_transactional(true);
        data.producer_id = 7;
        data.producer_epoch = 2;
        data.base_sequence = 0;

        let marker = RecordBatch {
            base_offset: 1,
            partition_leader_epoch: 2,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            last_offset_delta: 0,
            base_timestamp: 100,
            max_timestamp: 100,
            producer_id: 7,
            producer_epoch: 2,
            base_sequence: -1,
            records: vec![Record {
                key: Some(Bytes::from_static(&[0, 0, 0, 1])),
                value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 3])),
                ..record(0, 0)
            }],
        };
        let mut encoded = BytesMut::new();
        marker.encode(&mut encoded).expect("encode marker");
        let marker_bytes = encoded.freeze();
        let mut cursor: &[u8] = &marker_bytes;
        let borrowed = RecordBatchBorrowed::decode_borrow_with_policy(&mut cursor, <_>::default())
            .expect("borrow marker");
        let (mut prepared, tally) = prepare_batch(
            &partition_ref,
            &predicates,
            &borrowed,
            marker_bytes.clone(),
            1,
        )
        .expect("prepare marker");
        check!(matches!(tally, BatchTally::Kept));

        let log_dir = tempfile::tempdir().expect("log dir");
        let mut log = Log::open(log_dir.path(), LogConfig::default()).expect("open log");
        log.append_at(&mut data, Offset(0)).expect("append data");
        check!(log.pending_transaction_start(ProducerId(7)) == Some(Offset(0)));
        let PreparedBatch::Owned { batch, .. } = &mut prepared else {
            panic!("control batch must use the owned append path");
        };
        log.append_at(batch, Offset(1)).expect("append marker");
        check!(log.pending_transaction_start(ProducerId(7)) == None);
        let log_end = log.log_end_offset();
        check!(log.last_stable_offset(log_end) == log_end);
    }
}
