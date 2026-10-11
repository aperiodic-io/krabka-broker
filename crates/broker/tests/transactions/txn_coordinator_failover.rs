//! The transaction coordinator moves with the leadership of
//! `__transaction_state`.
//!
//! Kafka's `TransactionCoordinator.onElection` loads a state partition on the
//! broker that becomes its leader, so a transaction that the old leader opened
//! commits through the new one. The test opens a transaction on the
//! coordinator of a three-broker cluster, stops that broker, commits the
//! transaction through the broker that is elected next, and reads the records
//! with `read_committed`.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_broker::{BrokerConfig, BrokerHandle, NodeId};
use krabka_client_core::Client;
use krabka_protocol::owned::end_txn_response::EndTxnResponse;
use tempfile::TempDir;

use crate::{
    support::{
        client::connect_owned,
        discovery::{coordinator_lookup_request, topic_metadata_request},
        shutdown_cluster,
        topics::{AssignedTopicSetup, metadata_topic},
        transaction_wire::{
            AssignedTransactionTopicSetup, TransactionProduceSetup, add_partition_request,
            coordinator_loading, create_assigned_topic, init_producer_request,
            initialized_identity, partition_added, partition_leader, produce_fixture,
            read_committed_through, retry_coordinator,
        },
        transactions::{EndTransactionSetup, ProducerIdentity, end_transaction_request},
    },
    txn_harness::registered_transaction_cluster,
};

const TID: &str = "txn-coordinator-failover";
const TOPIC: &str = "txn-coordinator-failover";
const STATE_TOPIC: &str = "__transaction_state";

const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

fn retriable(code: i16) -> bool {
    coordinator_loading(code)
}

async fn client(address: &str) -> Client {
    connect_owned(address, "txn-coordinator-failover", "client").await
}

fn address_of(cluster: &Cluster, node: u64) -> String {
    cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == node)
        .map(|(handle, _, _)| handle.listen_addr().to_string())
        .expect("a broker of the cluster")
}

/// The leader of `topic-0` in the image of `handle`.
async fn leader_of(handle: &BrokerHandle, topic: &str) -> u64 {
    partition_leader(handle, topic, SETTLE).await
}

/// Metadata leadership can precede installation of the Produce readiness gate.
async fn data_leader_client(cluster: &Cluster) -> Client {
    let leader = leader_of(&cluster[0].0, TOPIC).await;
    let (handle, _, _) = cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == leader)
        .expect("the data leader is a cluster member");
    handle
        .wait_until_local_partition_leader(TOPIC, 0, NodeId(leader))
        .await;
    client(&handle.listen_addr().to_string()).await
}

/// Create `__transaction_state` with `FindCoordinator`.
async fn find_coordinator(client: &Client) {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = client
            .send(coordinator_lookup_request(TID, 1, vec![TID.into()]))
            .await
            .expect("FindCoordinator");
        if found
            .coordinators
            .first()
            .is_some_and(|row| row.error_code == 0)
        {
            return;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: the topic creation has no awaiter from this client.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn init_producer(client: &Client) -> (i64, i16) {
    initialized_identity(
        || async {
            client
                .send(init_producer_request(TID))
                .await
                .expect("InitProducerId")
        },
        retriable,
        SETTLE,
    )
    .await
}

async fn add_partition(client: &Client, producer: (i64, i16)) {
    partition_added(client.send(add_partition_request(TID, TOPIC, producer))).await;
}

async fn produce(client: &Client, producer: Option<(i64, i16)>, values: &[&'static str]) {
    let topic_id = client
        .send(topic_metadata_request(Some(vec![metadata_topic(
            Some(TOPIC.into()),
            krabka_protocol::primitives::uuid::Uuid::default(),
        )])))
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(TOPIC))
        .map(|row| row.topic_id)
        .expect("topic in metadata");
    produce_fixture(
        TransactionProduceSetup {
            transactional_id: TID,
            topic: TOPIC,
            topic_id,
            producer: producer.map(ProducerIdentity::from_wire),
            values,
        },
        |request| client.send(request),
    )
    .await;
}

async fn end_txn(client: &Client, (producer_id, epoch): (i64, i16)) -> EndTxnResponse {
    retry_coordinator(
        || async {
            client
                .send(end_transaction_request(
                    TID,
                    EndTransactionSetup {
                        producer: ProducerIdentity::from_wire((producer_id, epoch)),
                        ..Default::default()
                    },
                ))
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        |code| retriable(code) || code == CONCURRENT_TRANSACTIONS,
        SETTLE,
    )
    .await
}

async fn read_committed(bootstrap: &str, last: &str) -> Vec<String> {
    read_committed_through(bootstrap, TOPIC, last, SETTLE).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_commits_through_the_next_coordinator() {
    let mut cluster = registered_transaction_cluster(|_, config| {
        config.transaction_state_num_partitions = 1;
        config.transaction_state_replication_factor = 3;
    })
    .await;
    let admin = client(&cluster[0].0.listen_addr().to_string()).await;
    find_coordinator(&admin).await;
    let coordinator = leader_of(&cluster[0].0, STATE_TOPIC).await;
    create_assigned_topic(
        &admin,
        AssignedTransactionTopicSetup {
            topic: AssignedTopicSetup::leader_first(TOPIC, krabka_metadata::NodeId(coordinator)),
            context: Some("CreateTopics"),
        },
    )
    .await;
    cluster[0].0.wait_until_isr_len(STATE_TOPIC, 0, 3).await;
    cluster[0].0.wait_until_isr_len(TOPIC, 0, 3).await;

    // Open the transaction on the coordinator.
    let first = client(&address_of(&cluster, coordinator)).await;
    let producer = init_producer(&first).await;
    add_partition(&first, producer).await;
    let data_leader = data_leader_client(&cluster).await;
    produce(&data_leader, Some(producer), &["a", "b", "c"]).await;
    first.close();
    data_leader.close();
    admin.close();

    // Stop the coordinator. A surviving broker leads the state partition next.
    let position = cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == coordinator)
        .expect("the coordinator is a cluster member");
    let (stopped, _, stopped_dir) = cluster.remove(position);
    stopped.shutdown().await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed(STATE_TOPIC, 0, NodeId(coordinator))
        .await;
    cluster[0]
        .0
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(coordinator))
        .await;
    let next = leader_of(&cluster[0].0, STATE_TOPIC).await;

    // Commit through the next coordinator. Transaction version 2 bumps the
    // epoch on completion.
    let second = client(&address_of(&cluster, next)).await;
    let committed = end_txn(&second, producer).await;
    assert!(
        committed
            == EndTxnResponse {
                producer_id: producer.0,
                producer_epoch: producer.1 + 1,
                ..Default::default()
            },
        "EndTxn through the next coordinator"
    );
    second.close();

    let data_leader = data_leader_client(&cluster).await;
    produce(&data_leader, None, &["z"]).await;
    data_leader.close();
    let bootstrap = cluster[0].0.listen_addr().to_string();
    let seen = read_committed(&bootstrap, "z").await;
    assert!(seen == ["a", "b", "c", "z"]);

    shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
