//! Kafka trunk's `max.decompressed.message.bytes` on the Produce path.
//!
//! `LogValidator` hands the topic's limit to the batch iterator that
//! decompresses a produced batch, and `DefaultRecord.readFrom` refuses a
//! record whose declared body is larger before it allocates the body. The
//! `InvalidRecordException` reaches the client as `INVALID_RECORD`, with no
//! per-record errors and no message, and nothing is appended. Only a
//! compressed batch is iterated that way: an uncompressed one is bounded by
//! `max.message.bytes` alone, and so is a record that the producer sent
//! uncompressed however large it is.
//!
//! krabka has already decompressed the batch by the time this runs
//! ([`super::prepare::prepare_batch`] parses every record of it under the
//! broker-wide `RecordDecompressionPolicy`), so the check is not an allocation
//! guard here. It is the limit the operator set, answered the way Kafka
//! answers it. A verbatim batch is walked a second time to read the record
//! sizes; that second decompression is paid only by a compressed batch on a
//! topic whose limit is below Kafka's default, which is a limit an operator
//! chose on purpose. ponytail: threading the limit into
//! `ValidatedBatch::validate_records_with` would make it one pass, at the cost
//! of a change in krabka-protocol.

use {
    super::prepare::{PreparedBatch, PreparedSource},
    crate::{api_catalog::UnstableApiVersions, config_keys::resolve_max_decompressed_record_bytes},
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_protocol::records::validate_one_v2_batch,
};

/// Whether `prepared` breaks the topic's `max.decompressed.message.bytes`,
/// which `(image, node, topic)` resolve: a compressed batch with a record whose
/// body is larger. Kafka 4.3.1 has no such limit, so a broker not serving
/// trunk's keys (`unstable`) never refuses on it, and the lookup is skipped for
/// a batch the producer did not compress.
pub(super) fn exceeds_decompressed_limit(
    prepared: &PreparedBatch,
    image: &krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
    topic: &str,
    unstable: UnstableApiVersions,
    policy: RecordDecompressionPolicy,
) -> bool {
    unstable == UnstableApiVersions::Enabled
        && prepared.attributes.compression() != CompressionType::None
        && resolve_max_decompressed_record_bytes(image, node, topic)
            .is_some_and(|limit| has_oversized_record(prepared, limit, policy))
}

/// Whether `prepared` is a compressed batch with a record whose body is larger
/// than `limit` bytes.
///
/// A batch the producer did not compress never has one: Kafka's iterator reads
/// it in place and bounds it by `max.message.bytes` alone. `policy` is the
/// decompression bound the verbatim walk runs under; a batch that fails it
/// here already failed [`super::prepare::prepare_batch`], so a failure reads as
/// "not oversized" and leaves the verdict to the paths that own it.
fn has_oversized_record(
    prepared: &PreparedBatch,
    limit: usize,
    policy: RecordDecompressionPolicy,
) -> bool {
    if prepared.attributes.compression() == CompressionType::None {
        return false;
    }
    match &prepared.source {
        PreparedSource::Owned(batch) => {
            batch.records.iter().any(|record| record.body_len() > limit)
        }
        PreparedSource::Verbatim(bytes) => {
            let Ok(batch) = validate_one_v2_batch(bytes) else {
                return false;
            };
            let mut oversized = false;
            let walked = batch.validate_records_with(policy, |record| {
                oversized |= record.body_len() > limit;
            });
            walked.is_ok() && oversized
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        bytes::BytesMut,
        krabka_protocol::records::{Attributes, RecordBatch},
    };

    krabka_macros::record_limit_fixture!(record);

    /// A batch of one record with a `value_len`-byte value and a header, encoded
    /// under `codec`.
    fn batch(codec: CompressionType, value_len: usize) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: 0,
            records: vec![record(value_len)],
            ..Default::default()
        }
    }

    fn prepared(batch: &RecordBatch, verbatim: bool) -> PreparedBatch {
        let source = if verbatim {
            let mut buf = BytesMut::new();
            batch.encode(&mut buf).expect("encode");
            PreparedSource::Verbatim(buf.freeze())
        } else {
            PreparedSource::Owned(batch.clone())
        };
        PreparedBatch {
            source,
            attributes: batch.attributes,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            last_offset_delta: 0,
            max_timestamp: 0,
            keyless_records: Vec::new(),
            invalid_timestamp_records: Vec::new(),
            dedup: None,
        }
    }

    /// The limit refuses a compressed record above it, on the verbatim and the
    /// owned path alike, and never an uncompressed one.
    #[test]
    fn only_a_compressed_batch_is_held_to_the_limit() {
        let policy = RecordDecompressionPolicy::default();
        // The record body is 114 bytes with a 100-byte value: its attributes
        // byte, a two-byte timestamp delta, a one-byte offset delta, `1 + 3`
        // bytes of key, `2 + 100` of value, a one-byte header count, and one
        // header of `1 + 1` bytes of key with a null value. Its encoding is
        // that behind a two-byte length prefix, so a limit of 114 admits it and
        // 113 refuses it.
        for (codec, verbatim, limit, expected) in [
            (CompressionType::Gzip, true, 113, true),
            (CompressionType::Gzip, true, 114, false),
            (CompressionType::Gzip, false, 113, true),
            (CompressionType::Gzip, false, 114, false),
            (CompressionType::Zstd, true, 50, true),
            (CompressionType::None, true, 50, false),
            (CompressionType::None, false, 50, false),
        ] {
            check!(
                has_oversized_record(&prepared(&batch(codec, 100), verbatim), limit, policy)
                    == expected,
                "{codec:?} verbatim={verbatim} limit={limit}"
            );
        }
    }
}
