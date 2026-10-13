//! The batch a leader appends after it drops duplicates from a client batch.

use {
    crate::partition::ProduceData,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_protocol::records::{RecordBatch, RecordsError, TimestampType},
};

/// Remove the records that `dropped` marks from a client batch.
///
/// Each kept record keeps its `offset_delta`, and the batch keeps its
/// `last_offset_delta` and producer fields. So a dropped record leaves a gap in
/// the offsets, as compaction leaves one: the log places every kept record at
/// the offset the producer was told, and an idempotent producer's next
/// sequence number stays the one that it expects.
///
/// When every record is dropped, the result is an empty batch, as Kafka's log
/// cleaner writes to keep a producer's last sequence. It is uncompressed, has
/// no records, and carries the original `max_timestamp` as both of its
/// timestamps.
///
/// A verbatim batch is decoded, so the writer re-encodes the result in the
/// topic's codec. That cost is what the `dedup_batches_rebuilt` metric counts.
///
/// Returns `None` for a batch that is not a client data batch, which keeps
/// all of its records.
///
/// # Errors
///
/// Returns the decode error of a verbatim batch that does not decode. The
/// produce handler validated the batch, so this does not occur in practice.
pub(super) fn drop_records(
    data: &ProduceData,
    dropped: &[bool],
    policy: RecordDecompressionPolicy,
) -> Result<Option<RecordBatch>, RecordsError> {
    let mut batch = match data {
        ProduceData::Verbatim(verbatim) => {
            let mut bytes = verbatim.bytes.clone();
            let mut batch = RecordBatch::decode_with_policy(&mut bytes, policy)?;
            batch.partition_leader_epoch = verbatim.leader_epoch.0;
            batch
        }
        ProduceData::Owned(batch) => batch.clone(),
        _ => return Ok(None),
    };
    let records = std::mem::take(&mut batch.records);
    batch.records = records
        .into_iter()
        .zip(dropped.iter().copied().chain(std::iter::repeat(false)))
        .filter_map(|(record, dropped)| (!dropped).then_some(record))
        .collect();
    if batch.records.is_empty() {
        batch.attributes = batch.attributes.with_compression(CompressionType::None);
        batch.base_timestamp = batch.max_timestamp;
    } else if batch.attributes.timestamp_type() == TimestampType::CreateTime {
        // The batch header must describe the records that remain, as the
        // compaction rewrite keeps it.
        batch.max_timestamp = batch
            .records
            .iter()
            .map(|record| batch.base_timestamp.saturating_add(record.timestamp_delta))
            .max()
            .unwrap_or(batch.max_timestamp);
    }
    Ok(Some(batch))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        bytes::{Bytes, BytesMut},
        krabka_log::{LeaderEpoch, ProducerId, VerbatimBatch},
        krabka_protocol::records::{Attributes, Record, validate_one_v2_batch},
    };

    fn record(offset_delta: i32, timestamp_delta: i64, key: &'static [u8]) -> Record {
        Record {
            timestamp_delta,
            offset_delta,
            key: Some(Bytes::from_static(key)),
            value: Some(Bytes::from_static(b"v")),
            ..Record::default()
        }
    }

    fn client_batch(compression: CompressionType) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(compression),
            last_offset_delta: 3,
            base_timestamp: 1_000,
            max_timestamp: 1_030,
            producer_id: 7,
            producer_epoch: 2,
            base_sequence: 40,
            records: vec![
                record(0, 0, b"a"),
                record(1, 10, b"b"),
                record(2, 20, b"c"),
                record(3, 30, b"d"),
            ],
            ..RecordBatch::default()
        }
    }

    fn verbatim(batch: &RecordBatch) -> ProduceData {
        let mut bytes = BytesMut::new();
        batch.encode(&mut bytes).unwrap();
        let bytes = bytes.freeze();
        let header = validate_one_v2_batch(&bytes).unwrap().header;
        let mut verbatim = VerbatimBatch::from_header(header, bytes.clone());
        verbatim.leader_epoch = LeaderEpoch(9);
        assert!(verbatim.producer_id == ProducerId(7));
        ProduceData::Verbatim(verbatim)
    }

    fn policy() -> RecordDecompressionPolicy { RecordDecompressionPolicy::default() }

    #[test]
    fn a_partial_drop_keeps_offsets_sequences_and_the_codec() {
        for (name, compression, verbatim_input) in [
            ("owned, uncompressed", CompressionType::None, false),
            ("verbatim, uncompressed", CompressionType::None, true),
            ("verbatim, gzip", CompressionType::Gzip, true),
        ] {
            let original = client_batch(compression);
            let data = if verbatim_input {
                verbatim(&original)
            } else {
                ProduceData::Owned(original.clone())
            };
            let rewritten = drop_records(&data, &[false, true, false, true], policy())
                .unwrap()
                .unwrap();
            let expected = RecordBatch {
                partition_leader_epoch: if verbatim_input { 9 } else { 0 },
                max_timestamp: 1_020,
                records: vec![original.records[0].clone(), original.records[2].clone()],
                ..original
            };
            assert!(rewritten == expected, "{name}");
        }
    }

    #[test]
    fn an_all_duplicate_batch_becomes_an_empty_uncompressed_batch() {
        let original = client_batch(CompressionType::Gzip);
        let rewritten = drop_records(&verbatim(&original), &[true; 4], policy())
            .unwrap()
            .unwrap();
        let expected = RecordBatch {
            partition_leader_epoch: 9,
            attributes: original.attributes.with_compression(CompressionType::None),
            base_timestamp: 1_030,
            records: Vec::new(),
            ..original
        };
        assert!(rewritten == expected);
    }

    #[test]
    fn a_control_batch_is_not_rewritten() {
        let marker = RecordBatch {
            attributes: Attributes::default().with_control(true),
            ..client_batch(CompressionType::None)
        };
        let rewritten =
            drop_records(&ProduceData::OwnedControl(marker), &[true; 4], policy()).unwrap();
        assert!(rewritten.is_none());
    }
}
