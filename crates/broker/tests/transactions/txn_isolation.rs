//! Consumer-isolation outcomes of a committed and an aborted transaction.
//!
//! A `read_committed` consumer must see every record of a committed
//! transaction and none of an aborted one, while a `read_uncommitted` consumer
//! sees both. The interleaved case reuses one `transactional_id` across three
//! back-to-back transactions.

use std::time::Duration;

use assert2::assert;
use krabka_client_consumer::IsolationLevel;
use krabka_protocol::owned::fetch_request::FetchRequest;
use krabka_units::bytes;

use crate::{
    support::{
        client::connect_client, discovery::topic_metadata_request, fetch::single_partition_fetch,
        topics::metadata_topic,
    },
    txn_harness::{boot_single, create_topic, create_topic_with_segment_bytes, rec, send_ok},
};

/// Commits a transaction, after which a `read_committed` consumer sees all 3
/// records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_then_read_committed_sees_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "t").await;

    let producer =
        crate::support::producer::transactional_producer(bootstrap.clone(), "my-tid").await;
    let txn = producer.begin_transaction().await.unwrap();
    crate::support::producer::enqueue_string_values(&producer, "t", &["a", "b", "c"]).await;
    txn.commit().await.unwrap();

    let (consumer, seen) =
        crate::support::transaction_wire::committed_values(bootstrap, "g1", "t", None).await;
    assert!(seen == vec!["a", "b", "c"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 2 ────────────────────────────────────────────────────────────────────

const SEGMENT_BYTES: u64 = 128;

/// Aborts a transaction. `read_committed` then sees 0 records, and
/// `read_uncommitted` sees 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_then_read_committed_skips_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    // Every batch is about 75 bytes, so 128 seals a segment behind each one, and
    // a smaller value would refuse the batch itself with RECORD_LIST_TOO_LARGE, as
    // `UnifiedLog.append` does for a batch above `segmentSize()`.
    create_topic_with_segment_bytes(&bootstrap, "ta", SEGMENT_BYTES).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !broker
        .partition_log_config_for_test("ta", 0)
        .is_some_and(|config| config.segment_size == bytes(u32::try_from(SEGMENT_BYTES).unwrap()))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "internal.segment.bytes did not reach the transaction log"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let producer =
        crate::support::producer::transactional_producer(bootstrap.clone(), "abort-tid").await;
    let txn = producer.begin_transaction().await.unwrap();
    // Wait for each acknowledgement: like Kafka's, the producer's abort
    // discards the batches it has not sent yet, so the records would otherwise
    // never reach the log and there would be no abort marker to skip.
    for v in ["x", "y", "z"] {
        send_ok(&producer, rec("ta", v)).await;
    }
    txn.abort().await.unwrap();

    // The next append rolls the abort marker and its transaction index into a
    // sealed segment. A lagging fetch must still receive that abort entry.
    let later = crate::support::producer::default_producer(bootstrap.clone()).await;
    send_ok(&later, rec("ta", "after")).await;
    broker.wait_until_high_watermark("ta", 0, 5).await;

    let client = connect_client(bootstrap.clone(), None).await;
    let metadata = client
        .send(topic_metadata_request(Some(vec![metadata_topic(
            Some("ta".into()),
            krabka_protocol::primitives::uuid::Uuid::default(),
        )])))
        .await
        .unwrap();
    let topic_id = metadata.topics[0].topic_id;
    let fetched = client
        .send(FetchRequest {
            replica_id: -1,
            isolation_level: 1,
            ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
                topic: "ta".into(),
                topic_id,
                limits: crate::support::fetch::FetchLimits::one_mebibyte(
                    crate::support::fetch::RequestWaitMillis(1_000),
                ),
                ..Default::default()
            })
        })
        .await
        .unwrap();
    let aborted = fetched.responses[0].partitions[0]
        .aborted_transactions
        .as_deref()
        .unwrap_or_default();
    assert!(
        aborted.len() == 1 && aborted[0].first_offset == 0,
        "read_committed fetch must describe the abort from the sealed segment: {aborted:?}"
    );

    // read_committed: must skip the three aborted records and see the later one.
    let (consumer, seen) = crate::support::transaction_wire::read_committed_at_least(
        bootstrap.clone(),
        "g-abort",
        "ta",
        1,
    )
    .await;
    assert!(seen == ["after"], "read_committed exposed aborted records");
    consumer.close().await.unwrap();

    // read_uncommitted: sees all 4 data records (including aborted ones).
    let (consumer_uc, seen2) = crate::support::transaction_wire::observed_values(
        bootstrap,
        "g-abort-uc",
        "ta",
        (IsolationLevel::ReadUncommitted, None),
        Duration::from_secs(30),
        |seen| seen.len() >= 4,
        None,
    )
    .await;
    assert!(
        seen2 == ["x", "y", "z", "after"],
        "read_uncommitted must see aborted records"
    );
    consumer_uc.close().await.unwrap();

    later.close().await.unwrap();
    producer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// commit("a","b","c"), abort("X","Y"), commit("d","e","f","g"):
/// `read_committed` sees exactly \["a","b","c","d","e","f","g"\].
///
/// Exercises rapid reuse of one `transactional_id` across three back-to-back
/// transactions. This used to flake with `Server(48)` (`INVALID_TXN_STATE`)
/// because `flush` returned before an in-flight Produce had transitioned the
/// coordinator to `Ongoing`, so the following `EndTxn` arrived while the entry
/// was still `CompleteCommit`/`CompleteAbort`. `Producer::flush` now waits for
/// in-flight batches, so the partition-register Produce is always acked before
/// `EndTxn` is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_commit_and_abort() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ti").await;

    let producer =
        crate::support::producer::transactional_producer(bootstrap.clone(), "interleave-tid").await;

    // First txn: commit ["a", "b", "c"].
    let txn = producer.begin_transaction().await.unwrap();
    crate::support::producer::enqueue_string_values(&producer, "ti", &["a", "b", "c"]).await;
    txn.commit().await.unwrap();

    // Second txn: abort ["X", "Y"].
    let txn = producer.begin_transaction().await.unwrap();
    // Acknowledged, so the aborted records are in the log: the producer's
    // abort discards the batches it has not sent yet.
    for v in ["X", "Y"] {
        send_ok(&producer, rec("ti", v)).await;
    }
    txn.abort().await.unwrap();

    // Third txn: commit ["d", "e", "f", "g"].
    let txn = producer.begin_transaction().await.unwrap();
    crate::support::producer::enqueue_string_values(&producer, "ti", &["d", "e", "f", "g"]).await;
    txn.commit().await.unwrap();

    let (consumer, seen) = crate::support::transaction_wire::read_committed_at_least(
        bootstrap,
        "g-interleave",
        "ti",
        7,
    )
    .await;
    assert!(seen == vec!["a", "b", "c", "d", "e", "f", "g"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}
