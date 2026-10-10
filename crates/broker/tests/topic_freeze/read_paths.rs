//! The paths a freeze must leave working: fetch, metadata, metrics, and the
//! offset commits of a consumer that is still draining the frozen topic.
//!
//! "The cluster is up, every read works, and the broker must not accept a new
//! write" is the state this feature exists to give. A freeze that also broke
//! reads would be a deny ACL with extra steps, and one that stopped
//! `OffsetCommit` would strand every group at its last pre-freeze position, so
//! both are asserted rather than assumed.

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::freeze::PATTERN_TYPE_LITERAL,
    owned::offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    control_plane::freeze_scope,
    support,
    support::{
        client::connect_owned,
        fetch::single_partition_fetch,
        offsets::{offset_commit_partition, offset_commit_topic},
    },
    wire::{CONTROL, accepted, create_topic, refused},
};

/// [`support::start`] with the Prometheus listener bound.
///
/// The harness leaves `metrics_listen_addr` unset, and the one case that
/// scrapes `/metrics` over HTTP needs a socket to scrape.
async fn start_with_metrics() -> (BrokerHandle, Client, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("a loopback address"));
    let broker = Broker::start(config).await.expect("broker start");
    let client = connect_owned(
        broker.listen_addr().to_string(),
        "krabka-broker-test",
        "client build",
    )
    .await;
    (broker, client, tempdir)
}

/// Scrape the `OpenMetrics` body from the broker's `/metrics` endpoint.
async fn scrape(addr: std::net::SocketAddr) -> String {
    crate::support::client::scrape_metrics(addr).await
}

/// The number of records a fetch from offset zero returns.
async fn fetch_record_count(client: &Client, topic: &str, topic_id: WireUuid) -> usize {
    let response = client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: topic.into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .expect("Fetch");
    assert!(response.error_code == codes::NONE, "Fetch: {response:?}");
    let partition = &response.responses[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "Fetch partition: {partition:?}"
    );
    crate::support::records::record_count(partition.records.as_ref())
}

/// A frozen topic stays readable, stays visible, and stays observable.
///
/// "The cluster is up, every read works, and the broker must not accept a new
/// write" is the state this feature exists to give. A freeze that also broke
/// reads would be a deny ACL with extra steps, so the read paths are asserted
/// rather than assumed. The metrics half closes the gap KFC-7's suite found
/// late: both counters were declared, registered and documented, and a live
/// broker scraped zero for them, because nothing on a real request moved them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_metadata_and_the_metrics_endpoint_still_answer_for_a_frozen_topic() {
    let (broker, client, _dir) = start_with_metrics().await;
    let metrics_addr = broker
        .metrics_addr()
        .expect("the metrics listener is bound");
    let (frozen, control) =
        crate::wire::frozen_topic_with_record(&broker, &client, "orders", "cutover").await;

    // The record written before the freeze is still readable, and the topic is
    // still in the metadata a client routes on.
    check!(fetch_record_count(&client, "orders", frozen).await == 1);
    let metadata = client
        .send(crate::support::discovery::named_topic_metadata("orders"))
        .await
        .expect("Metadata");
    let topic = &metadata.topics[0];
    check!(topic.error_code == codes::NONE, "Metadata: {topic:?}");
    check!(topic.partitions.len() == 1);

    broker
        .wait_for_metrics("topic_freezes_active reaches 1", |m| {
            m.topic_freezes_active.get() == 1
        })
        .await;
    let body = scrape(metrics_addr).await;
    for needle in [
        "krabka_broker_topic_freezes_active 1",
        "krabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
    ] {
        check!(body.contains(needle), "missing {needle} in:\n{body}");
    }

    crate::wire::check_produce!(&broker, &client, CONTROL, control => accepted(1));
    broker.shutdown().await;
}

/// A consumer of a frozen topic can still record where it got to.
///
/// `OffsetCommit` appends to `__consumer_offsets` and not to the frozen topic,
/// and a cutover is exactly when the reader positions matter most: the whole
/// point of freezing rather than deleting is that consumers drain the frozen
/// prefix and commit as they go. A freeze that stopped the commits would strand
/// every group at its last pre-freeze position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_commit_still_works_against_a_frozen_topic() {
    let p = support::start().await;
    // The raw OffsetCommit makes no coordinator lookup. Create
    // `__consumer_offsets` and load it first, as a client lookup does.
    p.broker.wait_until_group_coordinator_ready().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    crate::wire::check_produce!(&p.broker, &p.client, "orders", frozen => accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    crate::wire::check_produce!(&p.broker, &p.client, "orders", frozen => refused("literal", "orders", "cutover", 1));

    for (label, topic, topic_id) in [
        ("the frozen topic", "orders", frozen),
        ("the control topic", CONTROL, control),
    ] {
        let response = p
            .client
            .send(OffsetCommitRequest {
                group_id: "drainers".into(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics: vec![offset_commit_topic(
                    topic,
                    topic_id,
                    vec![OffsetCommitRequestPartition {
                        committed_leader_epoch: -1,
                        ..offset_commit_partition(0, 1, Some(String::new()))
                    }],
                )],
                ..Default::default()
            })
            .await
            .expect("OffsetCommit");
        check!(
            response.topics[0].partitions[0].error_code == codes::NONE,
            "{label}: {response:?}"
        );
    }

    p.broker.shutdown().await;
}
