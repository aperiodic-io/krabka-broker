//! Increment D end-to-end validation of a large-message fetch.
//!
//! The test produces a records run larger than 64 KiB. It then consumes the
//! run over the real loopback TCP socket and asserts that the record values
//! round-trip **byte-for-byte**, and that the broker drained them on the path
//! this target is supposed to take.
//!
//! On Linux this fetch is far above the `sendfile_min` threshold on a
//! plaintext `TcpStream`, so the kernel sends the records region through the
//! `sendfile(2)` zero-copy path. The consumer's CRC check and the value
//! comparison below fail if sendfile sends the wrong file range, or if a
//! partial-write loop bug drops or duplicates bytes. On Windows and on TLS
//! the same test exercises the portable vectored Increment C fallback. The
//! wire bytes are identical either way, so those assertions hold on every
//! platform — which is exactly why they cannot tell the paths apart. The
//! `fetch_response_drain_total` assertion can, and it is the one that fails if
//! a regression quietly routes every plaintext fetch onto the copy path.

use assert2::assert;

use crate::support::{
    client::create_topic,
    fetch::{fetch_partition, single_partition_fetch},
    records::value_record,
};
mod support;

use bytes::Bytes;
use krabka_broker::metrics::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel};
use krabka_protocol::{
    owned::fetch_request::FetchRequest,
    records::{Record, RecordBatch},
};
use support::topic_id_for;

/// Build `n` records whose values are distinct, large, and content-addressed
/// by index. Any misplaced byte is then detectable.
fn large_records(n: i32, value_len: usize) -> (RecordBatch, Vec<Bytes>) {
    let mut batch = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    let mut expected = Vec::with_capacity(usize::try_from(n.max(0)).unwrap_or(0));
    for i in 0..n {
        // Fill each value with a per-record byte pattern so a swapped/duplicated
        // range is caught, not just a length mismatch.
        let mut v = vec![0u8; value_len];
        let tag = (u8::try_from(i & 0xff).unwrap_or(0))
            .wrapping_mul(31)
            .wrapping_add(7);
        for (j, b) in v.iter_mut().enumerate() {
            *b = tag ^ u8::try_from(j & 0xff).unwrap_or(0);
        }
        let value = Bytes::from(v);
        expected.push(value.clone());
        batch.records.push(Record {
            key: Some(Bytes::from(format!("key-{i}"))),
            ..value_record(i, Some(value))
        });
    }
    (batch, expected)
}

/// The path this target must drain a large plaintext fetch on.
///
/// A platform with a file-to-socket `sendfile(2)` and a plaintext listener
/// takes the kernel path for a run this far above `sendfile_min`. Windows has
/// no such call, so its fetch is `vectored` — and neither target may reach
/// `pread`, which is the drain's own fallback for a stream that promises a
/// socket and then withholds it.
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
const EXPECTED_DRAIN_PATH: FetchDrainPath = FetchDrainPath::Sendfile;
#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
)))]
const EXPECTED_DRAIN_PATH: FetchDrainPath = FetchDrainPath::Vectored;

/// The drain counts of all three paths, in `FetchDrainPath::ALL` order.
fn drain_counts(metrics: &BrokerMetrics) -> [u64; 3] {
    FetchDrainPath::ALL.map(|path| {
        metrics
            .fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .get()
    })
}

/// The counts one drain on `path` adds, as a whole three-path split.
fn one_drain_on(path: FetchDrainPath) -> [u64; 3] {
    FetchDrainPath::ALL.map(|p| u64::from(p == path))
}

#[tokio::test]
async fn large_message_fetch_round_trips_byte_exact() {
    let p = support::start().await;
    create_topic(&p.client, "big", 1).await;
    let tid = topic_id_for(&p.client, "big").await;

    // 64 records × 2 KiB ≈ 128 KiB of records — far over `sendfile_min`, so
    // the Linux plaintext fetch goes zero-copy.
    let (batch, expected) = large_records(64, 2 * 1024);

    let prod = crate::support::produce::send_batch(
        &p.client,
        batch,
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("big").into(),
            topic_id: tid,
            ..Default::default()
        },
    )
    .await;
    assert!(prod.responses[0].partition_responses[0].error_code == 0);

    // Fetch with a generous byte budget so the whole run comes back in one go.
    let before = drain_counts(p.broker.metrics());
    let r = p
        .client
        .send(FetchRequest {
            session_id: 0,
            session_epoch: -1,
            ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
                topic: "big".into(),
                topic_id: tid,
                partition: fetch_partition(crate::support::fetch::FetchPartitionSetup {
                    maximum: crate::support::fetch::FetchByteLimit(8 * 1024 * 1024),
                    ..Default::default()
                }),
                limits: crate::support::fetch::FetchLimits::wait_for_data_with_maximum(
                    crate::support::fetch::RequestWaitMillis(200),
                    crate::support::fetch::FetchByteLimit(8 * 1024 * 1024),
                ),
            })
        })
        .await
        .expect("Fetch");

    assert!(r.error_code == 0);
    assert!(r.responses.len() == 1);
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .expect("v2 records decoded from the fetch response");

    // Flatten all returned records and compare their values byte-for-byte to
    // what we produced. Any sendfile range/partial-write bug corrupts this.
    let got_values: Vec<&Bytes> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|rec| rec.value.as_ref())
        .collect();
    assert!(
        got_values.len() == expected.len(),
        "expected {} records, got {}",
        expected.len(),
        got_values.len()
    );
    for (i, (got, want)) in got_values.iter().zip(expected.iter()).enumerate() {
        assert!(
            got.as_ref() == &want[..],
            "record {i} value mismatch (sendfile byte corruption?)"
        );
    }

    // The bytes above are identical on every path, so they cannot say which
    // one served them. This can: exactly one response was drained, and it went
    // out the way this target is supposed to send it.
    let after = drain_counts(p.broker.metrics());
    let drained = [
        after[0] - before[0],
        after[1] - before[1],
        after[2] - before[2],
    ];
    assert!(
        drained == one_drain_on(EXPECTED_DRAIN_PATH),
        "the fetch must drain once on the {} path",
        EXPECTED_DRAIN_PATH.as_str()
    );

    p.broker.shutdown().await;
}
