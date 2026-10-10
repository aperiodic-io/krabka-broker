//! The first compaction pass. It builds the key-to-newest-offset dedup map
//! over the sealed segments, before any rewrite starts.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_units::prelude::ByteSize;
use tracing::instrument;

use super::{CleanedTransactionMetadata, batch_reader::read_all_batches, should_index_key};
use crate::{
    error::LogError, record_limit::check_records_read, segment::Segment, txn_index::AbortedTxn,
};

/// Build a map of `key → latest absolute offset` across the given sealed
/// segments in input order.
///
/// The map excludes records with `key.is_none()`, because
/// [`rewrite_segments`] drops them. The map's value is the absolute offset of
/// the **newest** record seen for each key. Later writes overwrite earlier ones.
///
/// A batch of an aborted transaction never enters the map, as in Kafka's
/// `Cleaner.buildOffsetMapForSegment`: the rewrite drops its records, so it
/// must not shadow the committed record that came before it. `aborted` lists
/// the aborted transactions that overlap the segments, whether their abort
/// marker sits inside the segments or after them.
///
/// `limit` is Kafka trunk's `max.decompressed.message.bytes`. The map reads
/// every record of every batch it indexes, as `buildOffsetMapForSegment`
/// iterates each one, so a compressed batch with a record above the limit fails
/// the pass with [`LogError::RecordTooLarge`]. The batch of an aborted
/// transaction is not iterated, and is not checked.
#[instrument(
    level = "debug",
    skip_all,
    fields(segments = segments.len(), keys = tracing::field::Empty),
    err,
)]
pub fn build_offset_map(
    segments: &[&Segment],
    aborted: Vec<AbortedTxn>,
    limit: Option<ByteSize>,
) -> Result<HashMap<Bytes, Offset>, LogError> {
    // Keyed by `Bytes` (cheap refcounted clone of the record key) rather
    // than `Vec<u8>` to avoid a heap copy of every key. Zero-length keys
    // are legal in Kafka and dedup as a distinct "empty key" like any other.
    let mut map: HashMap<Bytes, Offset> = HashMap::new();
    let mut txn_meta = CleanedTransactionMetadata::default();
    txn_meta.add_aborted_transactions(aborted);
    for seg in segments {
        for batch in read_all_batches(seg)? {
            // Control batches (txn commit/abort markers) carry a control-type
            // key that must NEVER enter the dedup map. Skip them entirely:
            // indexing their key silently dropped all-but-newest markers and
            // broke read_committed (the control-batch data-loss bug).
            if batch.attributes.is_control_batch() {
                txn_meta.on_control_batch_read(&batch)?;
                continue;
            }
            if txn_meta.on_batch_read(&batch) {
                continue;
            }
            check_records_read(&batch, batch.records.len(), limit)?;
            for record in &batch.records {
                if !should_index_key(record.key.as_deref(), false) {
                    continue;
                }
                let key_bytes = record.key.as_ref().expect("should_index_key checked Some");
                let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
                map.insert(key_bytes.clone(), absolute);
            }
        }
    }
    tracing::Span::current().record("keys", map.len());
    Ok(map)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_ids::Offset;
    use krabka_protocol::records::{Attributes, RecordBatch};
    use tempfile::tempdir;

    use super::*;
    use crate::compact::test_support::{
        control_batch, make_record, superseded_records, transactional_record, write_sealed_batches,
        write_sealed_segment,
    };

    fn offset_map_for(segments: &[&Segment], aborted: Vec<AbortedTxn>) -> HashMap<Bytes, Offset> {
        build_offset_map(segments, aborted, None).unwrap()
    }

    fn offset_map_of(records: Vec<krabka_protocol::records::Record>) -> HashMap<Bytes, Offset> {
        let dir = tempdir().unwrap();
        let segment = write_sealed_segment(dir.path(), 0, records);
        offset_map_for(&[&segment], vec![])
    }

    #[test]
    fn control_batch_key_is_not_indexed() {
        let dir = tempdir().unwrap();
        // A control batch (commit marker) at offset 0, then keyed data at
        // offset 1. Only the data key should appear in the map; the control
        // marker's key must be absent.
        let mut data = RecordBatch {
            base_offset: 1,
            last_offset_delta: 0,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            attributes: Attributes::default(),
            ..RecordBatch::default()
        };
        data.records[0].offset_delta = 0;
        let seg = write_sealed_batches(dir.path(), &[control_batch(0, 1000, 1 /* COMMIT */), data]);
        let map = offset_map_for(&[&seg], vec![]);
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(1)});
    }

    #[test]
    fn build_offset_map_keeps_newest_offset_per_key() {
        let map = offset_map_of(superseded_records());
        assert2::assert!(
            map == maplit::hashmap! {
            Bytes::from_static(b"k1") => Offset(2),
            Bytes::from_static(b"k2") => Offset(1)}
        );
    }

    #[test]
    fn build_offset_map_drops_null_key_records() {
        let map = offset_map_of(vec![
            make_record(0, None, Some(b"no-key-1")),
            make_record(1, Some(b"k1"), Some(b"v1")),
            make_record(2, None, Some(b"no-key-2")),
        ]);
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(1)});
    }

    #[test]
    fn build_offset_map_across_segments_uses_newest() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        );
        let second_segment = write_sealed_segment(
            dir.path(),
            10,
            vec![make_record(0, Some(b"k1"), Some(b"v2"))],
        );
        let map = offset_map_for(&[&first_segment, &second_segment], vec![]);
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k1") => Offset(10)});
    }

    /// Kafka's `Cleaner.buildOffsetMapForSegment` skips a batch whose
    /// transaction aborted. Key `k` holds a committed value at offset 5 and an
    /// aborted one at offset 10, so the map points at the committed record and
    /// the aborted one never shadows it. A key that only an aborted batch
    /// wrote stays out of the map altogether.
    #[test]
    fn an_aborted_batch_never_enters_the_map() {
        let dir = tempdir().unwrap();
        let seg = write_sealed_batches(
            dir.path(),
            &[
                transactional_record(crate::compact::test_support::TransactionalRecordSetup {
                    offset: crate::Offset(5),
                    payload: (b"k", b"committed"),
                    ..Default::default()
                }),
                control_batch(6, 1000, 1 /* COMMIT */),
                transactional_record(crate::compact::test_support::TransactionalRecordSetup {
                    offset: crate::Offset(10),
                    producer: crate::ProducerId(2000),
                    payload: (b"k", b"aborted"),
                }),
                transactional_record(crate::compact::test_support::TransactionalRecordSetup {
                    offset: crate::Offset(11),
                    producer: crate::ProducerId(2000),
                    payload: (b"only-aborted", b"v"),
                }),
                control_batch(12, 2000, 0 /* ABORT */),
            ],
        );
        let aborted = vec![crate::test_support::aborted_txn(
            crate::test_support::AbortedTxnSetup {
                producer: crate::ProducerId(2000),
                bounds: crate::Offset(10)..=crate::Offset(12),
                stable: crate::Offset(13),
            },
        )];
        let map = offset_map_for(&[&seg], aborted);
        assert2::assert!(map == maplit::hashmap! {Bytes::from_static(b"k") => Offset(5)});
    }
}
