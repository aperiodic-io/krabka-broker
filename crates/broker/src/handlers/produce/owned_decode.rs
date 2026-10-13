//! The owned-decode fallback: it turns a legacy or pre-decoded
//! `RecordsPayload` into exactly one owned v2 record batch, up-converting a
//! v0/v1 `MessageSet` on the way.

use {
    crate::{codes, metrics::BrokerMetrics},
    krabka_compression::RecordDecompressionPolicy,
    krabka_protocol::records::{RecordBatch, RecordsPayload},
    std::sync::Arc,
};

/// Decode or up-convert a legacy or pre-decoded `RecordsPayload` into one
/// owned record batch.
///
/// The function up-converts a v0/v1 `MessageSet` and counts it once. A v2
/// sequence with anything other than one batch gives `INVALID_RECORD`, as does
/// a failed up-conversion.
pub(super) fn decode_owned_batch(
    payload: RecordsPayload,
    topic_name: &Arc<str>,
    metrics: &BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<RecordBatch, i16> {
    krabka_macros::records_payload_match! { match payload {
        RecordsPayload::V2(batches) => exactly_one_v2_batch(batches),
        RecordsPayload::Raw(bytes) => krabka_macros::records_payload_match! { match RecordsPayload::from_bytes_with_policy(bytes, policy) {
            Ok(RecordsPayload::V2(batches)) => exactly_one_v2_batch(batches),
            Ok(RecordsPayload::Raw(_) | RecordsPayload::Legacy(_)) | Err(_) => {
                Err(codes::INVALID_RECORD)
            }
            Ok(RecordsPayload::FileRegions(_)) => Err(codes::INVALID_RECORD),
        } },
        RecordsPayload::FileRegions(_) => Err(codes::INVALID_REQUEST),
        RecordsPayload::Legacy(bytes) => decode_legacy_batch(&bytes, topic_name, metrics, policy),
    } }
}

fn decode_legacy_batch(
    bytes: &bytes::Bytes,
    topic_name: &Arc<str>,
    metrics: &BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<RecordBatch, i16> {
    match krabka_records_legacy::legacy_to_v2_with_policy(bytes, policy) {
        Ok(rb) => {
            if !topic_name.is_empty() {
                metrics.record_produce_message_conversion(topic_name);
            }
            let mut rb = rb;
            rb.base_offset = 0;
            rb.last_offset_delta = i32::try_from(rb.records.len())
                .map_err(|_| codes::INVALID_RECORD)?
                .checked_sub(1)
                .ok_or(codes::INVALID_RECORD)?;
            for (offset, record) in rb.records.iter_mut().enumerate() {
                record.offset_delta = i32::try_from(offset).map_err(|_| codes::INVALID_RECORD)?;
            }
            Ok(rb)
        }
        Err(e) => {
            tracing::warn!(error = %e, "legacy_to_v2 failed");
            Err(codes::INVALID_RECORD)
        }
    }
}

fn exactly_one_v2_batch(mut batches: Vec<RecordBatch>) -> Result<RecordBatch, i16> {
    if batches.len() != 1 {
        return Err(codes::INVALID_RECORD);
    }
    Ok(batches.pop().expect("length checked"))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::produce::{
                framing::PartitionPayload,
                hot_path::TimestampPolicy,
                prepare::{DecodeEnv, PreparedSource, prepare_batch},
            },
            metrics::BrokerMetrics,
        },
        assert2::{assert, check},
        bytes::{Bytes, BytesMut},
        krabka_ids::Offset,
        krabka_protocol::records::Record,
        std::sync::Arc,
    };

    #[test]
    fn decode_owned_batch_preserves_non_default_header_and_record_fields() {
        let batch = RecordBatch {
            last_offset_delta: 1,
            max_timestamp: 9876,
            producer_id: 22,
            producer_epoch: 3,
            base_sequence: 11,
            records: [b"a", b"b"]
                .map(|v| Record {
                    value: Some(Bytes::from_static(v)),
                    ..Default::default()
                })
                .to_vec(),
            ..Default::default()
        };
        let decoded = decode_owned_batch(
            RecordsPayload::V2(vec![batch]),
            &Arc::from("orders"),
            &BrokerMetrics::new(),
            RecordDecompressionPolicy::default(),
        )
        .expect("decode owned batch");

        check!(decoded.last_offset_delta == 1);
        check!(decoded.max_timestamp == 9876);
        check!(decoded.producer_id == 22);
        check!(decoded.producer_epoch == 3);
        check!(decoded.base_sequence == 11);
        assert!(decoded.records.len() == 2);
        check!(decoded.records[0].value.as_deref() == Some(&b"a"[..]));
        check!(decoded.records[1].value.as_deref() == Some(&b"b"[..]));
    }

    #[test]
    fn decode_owned_batch_rejects_empty_v2_payload() {
        let err = decode_owned_batch(
            RecordsPayload::V2(Vec::new()),
            &Arc::from("orders"),
            &BrokerMetrics::new(),
            RecordDecompressionPolicy::default(),
        )
        .unwrap_err();
        assert!(err == crate::codes::INVALID_RECORD);
    }

    #[test]
    fn legacy_produce_offsets_are_reassigned_consecutively() {
        let records = vec![
            krabka_records_legacy::ParsedRecord {
                offset: Offset(10),
                timestamp: Some(100),
                key: None,
                value: Some(Bytes::from_static(b"a")),
            },
            krabka_records_legacy::ParsedRecord {
                offset: Offset(20),
                timestamp: Some(200),
                key: None,
                value: Some(Bytes::from_static(b"b")),
            },
        ];
        let mut legacy = BytesMut::new();
        krabka_records_legacy::encode_flat_message_set(
            records,
            krabka_records_legacy::Magic::V1,
            &mut legacy,
        );

        let prepared = prepare_batch(
            PartitionPayload::Owned(RecordsPayload::Legacy(legacy.freeze())),
            None,
            TimestampPolicy::default(),
            false,
            DecodeEnv {
                topic_name: &Arc::from("orders"),
                metrics: &BrokerMetrics::new(),
                policy: RecordDecompressionPolicy::default(),
                dedup: None,
            },
            13,
        )
        .unwrap();
        match prepared.source {
            PreparedSource::Owned(batch) => {
                check!(batch.base_offset == 0);
                check!(batch.last_offset_delta == 1);
                check!(batch.records[0].offset_delta == 0);
                check!(batch.records[1].offset_delta == 1);
            }
            PreparedSource::Verbatim(_) => panic!("expected one converted owned batch"),
        }
    }
}
