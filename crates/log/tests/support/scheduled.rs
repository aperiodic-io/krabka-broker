use {
    bytes::Bytes,
    krabka_protocol::records::{Record, RecordBatch},
};

/// A two-record batch whose activation time is `ts`.
pub fn batch_at(ts: i64) -> RecordBatch {
    RecordBatch {
        base_timestamp: ts,
        max_timestamp: ts,
        last_offset_delta: 1,
        records: (0..2)
            .map(|offset_delta| Record {
                offset_delta,
                key: Some(Bytes::from(format!("k{offset_delta}"))),
                value: Some(Bytes::from(vec![b'v'; 96])),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}
