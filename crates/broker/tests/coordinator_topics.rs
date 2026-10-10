//! The coordinator topics on a cold three-broker cluster.
//!
//! Kafka's `KafkaApis.getCoordinator` creates `__consumer_offsets` and
//! `__transaction_state` on the first `FindCoordinator` that needs them, with
//! their configured replication factor, through
//! `DefaultAutoTopicCreationManager`. Until the topic exists and has leaders,
//! the lookup answers `COORDINATOR_NOT_AVAILABLE`. No broker creates either
//! topic when it starts, so the first broker to boot cannot create them with
//! a replication factor of one.
//!
//! The first case proves the whole failure it guards against: a group whose
//! coordinator stops still has a coordinator, because every partition of
//! `__consumer_offsets` has a replica on every broker. The second proves the
//! replication factor is never lowered to fit the cluster. The third proves
//! that a broker stopped by a controlled shutdown still counts toward it.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_broker::{BrokerConfig, BrokerHandle, NodeId};
use krabka_client_core::Client;
use krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse;
use tempfile::TempDir;

use crate::support::{
    client::connect_owned,
    discovery::coordinator_lookup_request,
    topics::{creatable_topic, create_topic_request},
};

mod support;

const OFFSETS_TOPIC: &str = "__consumer_offsets";
const TRANSACTION_TOPIC: &str = "__transaction_state";
const SHARE_STATE_TOPIC: &str = "__share_group_state";

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

const COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// The time a lookup may take to see a creation or a failover settle.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "coordinator-topics",
        "client",
    )
    .await
}

async fn find(client: &Client, key_type: i8, key: &str) -> FindCoordinatorResponse {
    client
        .send(coordinator_lookup_request(key, key_type, vec![key.into()]))
        .await
        .expect("FindCoordinator")
}

/// Look `key` up until a broker other than `excluded` answers as its
/// coordinator, and return that broker's node id.
async fn coordinator_of(client: &Client, key_type: i8, key: &str, excluded: Option<i32>) -> i32 {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = find(client, key_type, key).await;
        let [row] = found.coordinators.as_slice() else {
            panic!("one coordinator row: {found:?}");
        };
        if row.error_code == 0 && Some(row.node_id) != excluded {
            return row.node_id;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: a client has no awaiter for the creation or the
        // failover; it retries, as Kafka's clients do.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Three brokers whose coordinator topics are sized as Kafka sizes them,
/// with `configure` applied on top.
async fn start_three(configure: impl Fn(&mut BrokerConfig)) -> Cluster {
    let cluster = support::start_n_node_with(3, |_, config| {
        *config = config.clone().with_internal_topics_for(3);
        config.offsets_topic_num_partitions = 3;
        config.transaction_state_num_partitions = 3;
        configure(config);
    })
    .await
    .expect("start the cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    cluster
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cold_cluster_replicates_the_offsets_topic_on_every_broker() {
    let mut cluster = start_three(|_| {}).await;
    let image = cluster[0].0.controller_image_for_test();
    assert!(image.topic(OFFSETS_TOPIC).is_none());

    let admin = client(&cluster[0].0).await;
    let first = find(&admin, KEY_TYPE_GROUP, "g").await;
    check!(first.coordinators[0].error_code == COORDINATOR_NOT_AVAILABLE);
    let coordinator = coordinator_of(&admin, KEY_TYPE_GROUP, "g", None).await;
    admin.close();

    let image = cluster[0].0.controller_image_for_test();
    let replicas: Vec<Vec<NodeId>> = (0..3)
        .map(|partition| {
            let mut replicas = image
                .partition(OFFSETS_TOPIC, partition)
                .expect("an offsets partition")
                .replicas
                .clone();
            replicas.sort_unstable();
            replicas
        })
        .collect();
    let every_broker = vec![NodeId(1), NodeId(2), NodeId(3)];
    assert!(replicas == vec![every_broker; 3]);

    // Stop the coordinator of the group. A survivor leads its partition next.
    let position = cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == u64::try_from(coordinator).unwrap())
        .expect("the coordinator is a cluster member");
    let (stopped, _, stopped_dir) = cluster.remove(position);
    stopped.shutdown().await;
    let survivor = client(&cluster[0].0).await;
    let next = coordinator_of(&survivor, KEY_TYPE_GROUP, "g", Some(coordinator)).await;
    check!(next != coordinator);
    survivor.close();

    crate::support::shutdown_cluster(cluster).await;
    drop(stopped_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replication_factor_above_the_broker_count_leaves_the_coordinator_unavailable() {
    let cluster = start_three(|config| config.transaction_state_replication_factor = 4).await;
    let admin = client(&cluster[0].0).await;

    // The group coordinator's topic fits the cluster, so the cluster serves
    // groups while its transaction coordinator stays unavailable.
    coordinator_of(&admin, KEY_TYPE_GROUP, "g", None).await;
    for _ in 0..10 {
        let found = find(&admin, KEY_TYPE_TRANSACTION, "t").await;
        check!(found.coordinators[0].error_code == COORDINATOR_NOT_AVAILABLE);
        // intentional: each lookup asks for the topic again, and each
        // creation must be refused.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for (handle, _, _) in &cluster {
        check!(
            handle
                .controller_image_for_test()
                .topic(TRANSACTION_TOPIC)
                .is_none()
        );
    }
    admin.close();

    crate::support::shutdown_cluster(cluster).await;
}

/// A broker that stops through a controlled shutdown stays registered,
/// fenced, and Kafka's `StripedReplicaPlacer` takes a fenced broker as a last
/// resort. A coordinator topic that is first asked for after the stop
/// therefore still gets its configured replication factor, with the stopped
/// broker as a replica outside the ISR.
///
/// This is the start of `ShareConsumerTest.test_broker_failure`: the share
/// group asks for `__share_group_state` only after one of three brokers has
/// stopped cleanly. The registration of that broker keeps
/// `InControlledShutdown`, and krabka once read that flag as "still shutting
/// down", left the broker out of the placement, and refused the topic for as
/// long as the broker stayed down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cleanly_stopped_broker_still_counts_toward_the_replication_factor() {
    const LED_TOPIC: &str = "led";
    let mut cluster =
        start_three(|config| config.share_coordinator.state_topic_num_partitions = 3).await;

    // Stop a broker that does not lead the raft quorum. The topic gives every
    // broker one partition to lead, so the stop goes through controlled
    // shutdown rather than straight to a fence.
    let raft_leader = cluster[0].0.wait_until_controller_leader().await;
    let position = cluster
        .iter()
        .position(|(_, config, _)| config.node_id != raft_leader)
        .expect("a broker that is not the raft leader");
    let admin = client(&cluster[(position + 1) % cluster.len()].0).await;
    let created = admin
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (LED_TOPIC).into(),
                partitions: crate::support::topics::TopicPartitionCount(3),
                replicas: crate::support::topics::TopicReplicationFactor(3),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    admin.close();
    for (handle, _, _) in &cluster {
        for partition in 0..3 {
            handle
                .wait_until_partition_present(LED_TOPIC, partition)
                .await;
        }
    }

    let (stopped, _, stopped_dir) = cluster.remove(position);
    let stopped_id = NodeId(stopped.node_id());
    stopped
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("the controlled shutdown drains");
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|image| image.broker(stopped_id).is_some_and(|broker| broker.fenced))
            .await;
    }
    let registration = cluster[0]
        .0
        .controller_image_for_test()
        .broker(stopped_id)
        .cloned()
        .expect("the stopped broker stays registered");
    assert!((registration.fenced, registration.in_controlled_shutdown) == (true, true));

    let survivor = client(&cluster[0].0).await;
    let key = format!("share-group:{}:0", URL_SAFE_NO_PAD.encode([7u8; 16]));
    coordinator_of(&survivor, KEY_TYPE_SHARE, &key, None).await;
    survivor.close();

    let image = cluster[0].0.controller_image_for_test();
    let mut survivors = cluster
        .iter()
        .map(|(handle, _, _)| NodeId(handle.node_id()))
        .collect::<Vec<_>>();
    survivors.sort_unstable();
    let sorted = |nodes: &[NodeId]| {
        let mut nodes = nodes.to_vec();
        nodes.sort_unstable();
        nodes
    };
    let placed: Vec<(Vec<NodeId>, Vec<NodeId>)> = (0..3)
        .map(|partition| {
            let record = image
                .partition(SHARE_STATE_TOPIC, partition)
                .expect("a share-state partition");
            (sorted(&record.replicas), sorted(&record.isr))
        })
        .collect();
    let every_broker = vec![NodeId(1), NodeId(2), NodeId(3)];
    assert!(placed == vec![(every_broker, survivors); 3]);

    crate::support::shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
