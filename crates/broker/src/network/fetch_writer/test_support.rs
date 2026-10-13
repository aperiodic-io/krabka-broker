//! Fixtures shared by the unit tests of the fetch response writer and of its
//! child modules: the sample responses, the raw record batches behind them,
//! and the temp-file records payload the sendfile tests need.

use {
    super::WriteOp,
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        records::{Record, RecordBatch, RecordsPayload},
    },
};

crate::sendfile_cfg! {
    use std::{io::Write as _, sync::Arc};

    use krabka_protocol::records::FileRegion;
}

pub(super) fn raw_batch(base: i64) -> Bytes {
    let rb = RecordBatch {
        base_offset: base,
        records: vec![Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"value-payload")),
            ..Default::default()
        }],
        ..RecordBatch::default()
    };
    let mut buf = BytesMut::new();
    rb.encode(&mut buf).unwrap();
    buf.freeze()
}

/// Extract the bytes of an `Inline` op. It panics on a `File` op. This
/// avoids a `match`/`let-else` that is infallible on Windows, with one
/// variant, but refutable on SENDFILE-alias platforms, with two variants.
/// Clippy stays happy on both.
pub(super) fn inline_bytes(op: &WriteOp) -> &Bytes {
    krabka_macros::sendfile_match! { match op {
        WriteOp::Inline(b) => b,
        WriteOp::File(_) => panic!("expected an inline op"),
    }}
}

pub(super) fn sample_response(version: i16) -> FetchResponse {
    let p0 = PartitionData {
        partition_index: 0,
        high_watermark: 1,
        last_stable_offset: 1,
        log_start_offset: 0,
        records: Some(RecordsPayload::Raw(raw_batch(0))),
        ..PartitionData::default()
    };
    let p1 = PartitionData {
        partition_index: 1,
        high_watermark: 2,
        last_stable_offset: 2,
        log_start_offset: 0,
        records: Some(RecordsPayload::Raw(raw_batch(1))),
        ..PartitionData::default()
    };
    response_with_partitions(version, vec![p0, p1])
}

crate::sendfile_cfg! {
    /// Write `bytes` to a temp file and return a single-region
    /// `RecordsPayload::FileRegions` that covers the whole file from
    /// offset 0.
    pub(super) fn file_payload(bytes: &[u8]) -> (tempfile::NamedTempFile, RecordsPayload) {
        let mut tf = tempfile::NamedTempFile::new().unwrap();
        tf.write_all(bytes).unwrap();
        tf.flush().unwrap();
        let file = Arc::new(tf.reopen().unwrap());
        let payload = RecordsPayload::FileRegions(vec![FileRegion {
            file,
            offset: 0,
            len: bytes.len(),
        }]);
        (tf, payload)
    }
}

/// A one-topic, one-partition response whose partition serves `records`.
///
/// The counterpart to [`sample_response`], which gives every partition a
/// batch: this one exists so a test can say what a partition with *nothing*
/// to serve puts on the wire.
pub(super) fn one_partition_response(
    version: i16,
    records: Option<RecordsPayload>,
) -> FetchResponse {
    response_with_partitions(version, vec![PartitionData {
        partition_index: 0,
        error_code: 0,
        high_watermark: 5,
        last_stable_offset: 5,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records,
        ..PartitionData::default()
    }])
}

fn response_with_partitions(version: i16, partitions: Vec<PartitionData>) -> FetchResponse {
    FetchResponse {
        throttle_time_ms: 0,
        session_id: 7,
        responses: vec![FetchableTopicResponse {
            topic: if version <= 12 {
                "t".to_string()
            } else {
                String::new()
            },
            topic_id: if version >= 13 {
                krabka_protocol::primitives::uuid::Uuid([5u8; 16])
            } else {
                krabka_protocol::primitives::uuid::Uuid([0u8; 16])
            },
            partitions,
            ..FetchableTopicResponse::default()
        }],
        ..FetchResponse::default()
    }
}
