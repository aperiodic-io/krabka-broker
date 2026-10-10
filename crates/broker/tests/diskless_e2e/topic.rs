//! The diskless topic itself: creating it through the real `CreateTopics`
//! handler, and the wait that says its WAL quorum is actually running.
//!
//! Creating the topic through the handler rather than through an injected
//! override map is the point. `krabka.diskless` has to survive
//! `validate_topic_config_map`, reach `V1TopicConfig` in the metadata log, and
//! come back out of the image on every broker's reconcile pass. A test that
//! stuffs the flag straight into a `Partition` proves none of that.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::NodeId;
use krabka_client_core::Client;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::{
    TOPIC, VOTERS,
    cluster::{DisklessCluster, wait_for},
    support,
};

/// Create the suite's diskless topic and return its assigned id.
///
/// The replication factor covers every broker. The WAL voter set and the
/// partition replica set are separate things -- the first is chosen by
/// `wal::quorum::placement` from the racks, the second by `CreateTopics` --
/// but the failover case needs a promoted broker that is in both, and rf=3 on
/// a 3-broker cluster is the assignment that guarantees it.
pub(crate) async fn create_diskless_topic(client: &Client) -> WireUuid {
    let response = client
        .send(crate::support::topics::diskless_topic_request(
            TOPIC,
            i16::try_from(VOTERS).expect("small cluster"),
        ))
        .await
        .expect("CreateTopics");

    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics rejected the diskless topic: {response:?}"
    );

    // Resolve the id by polling. `CreateTopics` returns once the metadata
    // record is committed, but the image this connection's `Metadata` reads
    // can still be one apply behind, and a zero topic id would then be sent on
    // every later Produce and Fetch -- which resolve the topic by id alone at
    // v13 and above, so they would fail with UNKNOWN_TOPIC_OR_PARTITION for as
    // long as the test kept retrying.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let topic_id = support::topic_id_for(client, TOPIC).await;
        if topic_id != WireUuid::default() {
            return topic_id;
        }
        assert!(
            Instant::now() <= deadline,
            "the metadata image never reported a topic id for {TOPIC}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Block until the partition has a leader, then until that leader's WAL shard
/// registry reports the full voter set with every follower already fetching.
///
/// Returns the leader's node id.
///
/// The registry check is what makes this more than a metadata wait.
/// `diskless_wal_ready_for_test` reads the runtime placement, the registered
/// shard engine and the follower-fetcher count, so a produce that follows it
/// cannot race asynchronous placement reconciliation and time out on a
/// high-watermark that no follower is positioned to advance.
pub(crate) async fn await_wal_quorum(cluster: &DisklessCluster) -> NodeId {
    for node in cluster.node_ids() {
        if let Some(broker) = cluster.handle_for_node(node) {
            broker.wait_until_partition_present(TOPIC, 0).await;
        }
    }
    let leader = cluster
        .handle_for_node(cluster.node_ids()[0])
        .expect("broker 1 is up")
        .partition_leader_for_test(TOPIC, 0)
        .map(NodeId)
        .expect("the partition has a leader");

    let leader_broker = cluster
        .handle_for_node(leader)
        .expect("the partition leader is up");
    wait_for(
        &format!("the diskless WAL quorum on broker {}", leader.0),
        Duration::from_mins(1),
        || async { leader_broker.diskless_wal_ready_for_test(TOPIC, 0, leader, VOTERS) },
    )
    .await;
    leader
}

pub(crate) async fn seed_workload(
    cluster: &DisklessCluster,
) -> (Client, WireUuid, NodeId, Vec<bytes::Bytes>, Client) {
    let admin = support::sasl_client(
        &cluster.bootstrap_for_node(cluster.node_ids()[0]),
        crate::CLIENT_PRINCIPAL,
        crate::PASSWORD,
    )
    .await;
    let topic_id = create_diskless_topic(&admin).await;
    let leader = await_wal_quorum(cluster).await;
    let values: Vec<bytes::Bytes> = (0..crate::RECORDS).map(crate::wire::value_at).collect();
    let producer = support::sasl_client(
        &cluster.bootstrap_for_node(leader),
        crate::CLIENT_PRINCIPAL,
        crate::PASSWORD,
    )
    .await;
    crate::wire::produce_all(&producer, topic_id, &values).await;
    (admin, topic_id, leader, values, producer)
}

/// Seed the same acknowledged workload after the requested flush cadence is ready.
pub(crate) async fn flushing_workload(
    interval: krabka_units::Time,
) -> (
    DisklessCluster,
    Client,
    WireUuid,
    NodeId,
    Vec<bytes::Bytes>,
    Client,
) {
    let cluster = Box::pin(crate::cluster::start_flushing_cluster(interval)).await;
    cluster.await_ready().await;
    let (admin, topic, leader, values, producer) = Box::pin(seed_workload(&cluster)).await;
    (cluster, admin, topic, leader, values, producer)
}

/// The live leader and the size of this suite's acknowledged seed prefix.
pub(crate) fn seeded_leader(
    cluster: &DisklessCluster,
    leader: NodeId,
) -> (&krabka_broker::BrokerHandle, i64) {
    let broker = cluster
        .handle_for_node(leader)
        .expect("the diskless leader is up");
    let committed = i64::try_from(crate::RECORDS).expect("small count");
    (broker, committed)
}
