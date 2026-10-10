//! The one append a freeze deliberately lets through: the marker of a
//! transaction that enlisted the partition before the freeze landed.
//!
//! The commit decision is already durable in `__transaction_state` by the time
//! the marker is written, so refusing the marker would not undo the
//! transaction. It would leave it permanently open, which pins the last stable
//! offset and stops every `read_committed` consumer of a topic the freeze was
//! meant to keep readable.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_client_producer::Producer;
use krabka_protocol::{
    krabka::freeze::PATTERN_TYPE_LITERAL,
    owned::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
};

use crate::{
    control_plane::freeze_scope,
    support,
    wire::{CONTROL, accepted, create_topic, refused},
};

/// The `read_committed` end of one partition.
///
/// This is `ListOffsets(LATEST)` at `isolation_level=1`, which is what a
/// `read_committed` consumer calling `endOffsets` sends. The broker answers it
/// with the partition's last stable offset, so an open transaction pins the
/// value and a resolved one releases it -- which is the whole reading this
/// suite takes.
async fn stable_offset(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            // -1 is `CONSUMER_REPLICA_ID`; the isolation level is read only for
            // a client, so a request that forgot it would be answered from the
            // unbounded log end offset instead.
            replica_id: -1,
            // 1 is `read_committed`, the isolation level an open transaction
            // holds back.
            isolation_level: 1,
            topics: vec![ListOffsetsTopic {
                name: topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    // -1 is `LATEST_TIMESTAMP`, the end of the partition.
                    timestamp: -1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    let partition = &response.topics[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "ListOffsets({topic}): {partition:?}"
    );
    partition.offset
}

async fn wait_for_stable_offset(client: &Client, topic: &str, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let offset = stable_offset(client, topic).await;
        if offset == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "stable offset for {topic} never reached {want}; last={offset}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A transaction that enlisted the partition before the freeze still commits,
/// and the `read_committed` last stable offset advances past its marker.
///
/// This is the sharp edge of the rule, and the one place where a freeze
/// deliberately lets an append through. The commit decision is already durable
/// in `__transaction_state` by the time the marker is written, so refusing the
/// marker would not undo the transaction -- it would leave it permanently open,
/// which pins the last stable offset and stops every `read_committed` consumer
/// of the partition. A freeze exists to keep a topic readable while it is not
/// writable, so a freeze that pinned the LSO forever would break the half of
/// the feature it was meant to keep.
///
/// The case asserts the LSO on both sides of the commit rather than only after
/// it. A broker that had never pinned the LSO at all would pass a
/// one-sided assertion while proving nothing about the marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_that_enlisted_before_the_freeze_still_commits() {
    let p = support::start().await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    p.broker.wait_until_transaction_coordinator_ready().await;
    let bootstrap = p.broker.listen_addr().to_string();
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .transactional_id("cutover-tid")
        .build()
        .await
        .expect("producer build");
    producer
        .init_transactions()
        .await
        .expect("init_transactions");
    let txn = producer
        .begin_transaction()
        .await
        .expect("begin_transaction");
    producer
        .send(crate::support::producer::producer_record(
            crate::support::producer::ProducerRecordSetup {
                value: Some(Bytes::from_static(b"in-flight")),
                ..Default::default()
            },
        ))
        .await
        .expect("the in-flight record is acknowledged");
    p.broker
        .wait_until_local_log_end_offset("orders", 0, 1)
        .await;
    // The transaction is open, so `read_committed` cannot see past its first
    // record yet.
    check!(stable_offset(&p.client, "orders").await == 0);

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;

    // The freeze is live while the transaction is open: a new plain write is
    // refused, and it does not move the log.
    crate::wire::check_produce!(&p.broker, &p.client, "orders", frozen => refused("literal", "orders", "cutover", 1));

    txn.commit()
        .await
        .expect("a transaction that enlisted before the freeze still commits");

    // The marker was appended, so the log grew by one and `read_committed`
    // reached the end of it. A freeze that refused the marker would leave both
    // of these at their pre-commit values forever.
    p.broker
        .wait_until_local_log_end_offset("orders", 0, 2)
        .await;
    wait_for_stable_offset(&p.client, "orders", 2).await;

    crate::wire::check_produce!(&p.broker, &p.client, CONTROL, control => accepted(1));
    producer.close().await.expect("producer close");
    p.broker.shutdown().await;
}
