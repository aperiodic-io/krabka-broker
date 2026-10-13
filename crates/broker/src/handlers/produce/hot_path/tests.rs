//! Tests for the produce hot-path benchmark seam.
//!
//! They pin the two things a benchmark reading this seam depends on: that
//! `Dispatch` reproduces the pipeline's verbatim-versus-owned decision, and
//! that both paths actually land their records in the log. A seam that
//! silently stopped appending would still produce a plausible-looking number.

use {
    super::{HotPathSettings, PathChoice, ProducePath, TimestampPolicy, append_one_batch},
    crate::{codes, handlers::produce::test_support::encode_batch, metrics::BrokerMetrics},
    assert2::assert,
    bytes::Bytes,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_log::{Log, LogConfig},
    krabka_protocol::records::{Record, RecordBatch},
    std::sync::Arc,
    tempfile::tempdir,
};

const RECORDS: i32 = 4;

fn sample_batch() -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: RECORDS - 1,
        ..RecordBatch::default()
    };
    for i in 0..RECORDS {
        batch.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i}"))),
            value: Some(Bytes::from(vec![0xAB; 128])),
            ..Record::default()
        });
    }
    batch
}

fn v1_message_set() -> Bytes {
    krabka_records_legacy::v2_to_legacy(&sample_batch(), krabka_records_legacy::Magic::V1)
        .expect("down-convert to a v1 message set")
}

fn settings(
    metrics: &BrokerMetrics,
    topic_compression: Option<CompressionType>,
) -> HotPathSettings<'_> {
    HotPathSettings {
        topic_name: Arc::from("bench-topic"),
        topic_compression,
        timestamps: TimestampPolicy::default(),
        decompression_policy: RecordDecompressionPolicy::default(),
        metrics,
        leader_epoch: 7,
        version: 13,
    }
}

struct AppendFixture {
    metrics: BrokerMetrics,
    log: Log,
    dir: tempfile::TempDir,
}

fn append_fixture() -> AppendFixture {
    let metrics = BrokerMetrics::new();
    let dir = tempdir().unwrap();
    let log = Log::open(dir.path(), LogConfig::default()).unwrap();
    AppendFixture { metrics, log, dir }
}

#[test]
fn seam_reproduces_the_pipeline_decision_and_appends_on_both_paths() {
    let cases = [
        (
            "native v2 under producer pass-through stays verbatim",
            encode_batch(&sample_batch()),
            PathChoice::Dispatch,
            None,
            ProducePath::Verbatim,
        ),
        (
            "the forced fallback decodes the same bytes",
            encode_batch(&sample_batch()),
            PathChoice::ForceOwned,
            None,
            ProducePath::Owned,
        ),
        (
            "a topic that recompresses loses the passthrough",
            encode_batch(&sample_batch()),
            PathChoice::Dispatch,
            Some(CompressionType::Zstd),
            ProducePath::Owned,
        ),
        (
            "a legacy v1 message set is up-converted",
            v1_message_set(),
            PathChoice::Dispatch,
            None,
            ProducePath::Owned,
        ),
    ];

    for (name, records, choice, topic_compression, expected) in cases {
        let AppendFixture {
            metrics,
            dir: _dir,
            mut log,
        } = append_fixture();

        let path = append_one_batch(
            records,
            choice,
            &settings(&metrics, topic_compression),
            &mut log,
        )
        .unwrap_or_else(|code| panic!("{name}: rejected with {code}"));

        assert!(path == expected, "{}", name);
        assert!(log.log_end_offset().0 == i64::from(RECORDS), "{}", name);
    }
}

/// A malformed records field answers the code Kafka gives it and appends
/// nothing. The 18 bytes of text carry a magic byte no batch has, which
/// `ByteBufferLogInputStream.nextBatchSize` raises as `CorruptRecordException`,
/// where a field that stops before the first size field holds no batch at all,
/// which `ProduceRequest.validateRecords` calls an `InvalidRecordException`.
#[test]
fn a_malformed_records_field_returns_the_response_error_code() {
    let cases = [
        (
            "a magic byte no batch has",
            &b"not a record batch"[..],
            codes::CORRUPT_MESSAGE,
        ),
        ("no first batch", &b"not a batch"[..], codes::INVALID_RECORD),
    ];
    for (name, records, want) in cases {
        let AppendFixture {
            metrics,
            dir: _dir,
            mut log,
        } = append_fixture();

        let error = append_one_batch(
            Bytes::copy_from_slice(records),
            PathChoice::Dispatch,
            &settings(&metrics, None),
            &mut log,
        )
        .unwrap_err();

        assert!(error == want, "{name}");
        assert!(log.log_end_offset().0 == 0, "{name}");
    }
}
