//! A coordinator answers a transactional write only once it is committed.
//!
//! Kafka's `TransactionStateManager.appendTransactionToLog` appends each
//! `__transaction_state` record with `acks=-1`, and its group coordinator
//! completes a `TxnOffsetCommit` or an `OffsetDelete` write once the high
//! watermark passes it. Each answers the client only then. A coordinator that
//! answered at its local append could acknowledge a commit decision, or the
//! offsets of a transaction, that the next leader of the partition never gets.
//! Kafka's transactions system test bounces brokers under transactional
//! copiers, and such an answer breaks exactly-once on the failover after it.
//!
//! The same rule holds for a transaction marker. Kafka's `WriteTxnMarkers`
//! handler appends each marker with `acks=-1`, and the coordinator records the
//! transaction complete only after every marker is committed.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use krabka_client_core::{Client, Connection, ConnectionOptions};
use krabka_protocol::{
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        describe_transactions_request::DescribeTransactionsRequest, end_txn_request::EndTxnRequest,
        end_txn_response::EndTxnResponse, offset_commit_request::OffsetCommitRequest,
        offset_delete_response::OffsetDeleteResponse,
        txn_offset_commit_request::TxnOffsetCommitRequest,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tempfile::TempDir;

use crate::support::{
    self,
    client::connect_owned,
    offsets::{
        offset_commit_partition, offset_commit_topic, offset_delete_partition,
        offset_delete_request, offset_delete_topic,
    },
    relay::Relay,
    transaction_wire::{TransactionProduceSetup, produce_fixture},
    transactions::{
        ProducerIdentity, end_transaction_request, txn_offset_partition, txn_offset_topic,
    },
};

const TID: &str = "txn-uncommitted-writes";
const TOPIC: &str = "txn-uncommitted-writes";
const GROUP: &str = "txn-uncommitted-writes";
const STATE_TOPIC: &str = "__transaction_state";
const OFFSETS_TOPIC: &str = "__consumer_offsets";

const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

fn retriable(code: i16) -> bool {
    crate::support::transaction_wire::coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS
}

/// One connection to the broker that binds `address`, past any relay, so a
/// request reaches that broker whatever the cluster's metadata advertises.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(
        address,
        ConnectionOptions {
            client_id: "txn-uncommitted-writes".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("connect")
}

async fn client(address: SocketAddr) -> Client {
    connect_owned(address.to_string(), "txn-uncommitted-writes", "client").await
}

/// The leader of `topic-0` in the image of `handle`.
async fn leader_of(handle: &BrokerHandle, topic: &str) -> u64 {
    crate::support::transaction_wire::partition_leader(handle, topic, SETTLE).await
}

async fn create_topic(
    client: &Client,
    topic: crate::support::topics::AssignedTopicSetup,
) -> WireUuid {
    crate::support::transaction_wire::create_assigned_topic(
        client,
        crate::support::transaction_wire::AssignedTransactionTopicSetup {
            topic,
            ..Default::default()
        },
    )
    .await
}

async fn init_producer(connection: &Connection) -> (i64, i16) {
    crate::support::transaction_wire::initialized_identity(
        || async {
            connection
                .send(crate::support::transaction_wire::init_producer_request(TID))
                .await
                .expect("InitProducerId")
        },
        retriable,
        SETTLE,
    )
    .await
}

async fn add_partition(connection: &Connection, producer: (i64, i16)) {
    crate::support::transaction_wire::partition_added(connection.send(
        crate::support::transaction_wire::add_partition_request(TID, TOPIC, producer),
    ))
    .await;
}

async fn produce(
    connection: &Connection,
    topic_id: WireUuid,
    producer: Option<(i64, i16)>,
    values: &[&'static str],
) {
    produce_fixture(
        TransactionProduceSetup {
            transactional_id: TID,
            topic: TOPIC,
            topic_id,
            producer: producer.map(ProducerIdentity::from_wire),
            values,
        },
        |request| connection.send(request),
    )
    .await;
}

fn end_txn_request((producer_id, epoch): (i64, i16)) -> EndTxnRequest {
    end_transaction_request(
        TID,
        crate::support::transactions::EndTransactionSetup {
            producer: crate::support::transactions::ProducerIdentity::from_wire((
                producer_id,
                epoch,
            )),
            ..Default::default()
        },
    )
}

/// Commit, and retry while the coordinator answers a retriable error.
async fn end_txn(connection: &Connection, producer: (i64, i16)) -> EndTxnResponse {
    crate::support::transaction_wire::retry_coordinator(
        || async {
            connection
                .send(end_txn_request(producer))
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        retriable,
        SETTLE,
    )
    .await
}

/// The state of a transaction as its coordinator describes it, and the
/// partitions whose markers are outstanding.
type Described = (String, Vec<(String, Vec<i32>)>);

/// Describe the transaction until its coordinator answers `expected`, and
/// return the last answer. A coordinator that loads its partition answers a
/// retriable error first.
async fn describe_until(connection: &Connection, expected: &Described) -> Option<Described> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let described = connection
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![TID.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = &described.transaction_states[0];
        assert!(
            row.error_code == 0 || retriable(row.error_code),
            "DescribeTransactions: {described:?}"
        );
        let answer = (row.error_code == 0).then(|| {
            (
                row.transaction_state.clone(),
                row.topics
                    .iter()
                    .map(|topic| (topic.topic.clone(), topic.partitions.clone()))
                    .collect(),
            )
        });
        if answer.as_ref() == Some(expected) || Instant::now() >= deadline {
            return answer;
        }
        // intentional: the completion has no awaiter; the answer is the signal.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn read_committed(bootstrap: SocketAddr, last: &str) -> Vec<String> {
    crate::support::transaction_wire::read_committed_through(
        &bootstrap.to_string(),
        TOPIC,
        last,
        SETTLE,
    )
    .await
}

/// Three brokers whose data listeners each sit behind a [`Relay`]: every
/// broker advertises its relay, and binds its real listener behind it. A cut
/// relay takes that broker off the data plane as its peers see it, while it
/// runs and while its controller plane, which no relay fronts, keeps its
/// session and its leaderships.
struct RelayedCluster {
    brokers: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    relays: Vec<Relay>,
    _dirs: Vec<TempDir>,
}

impl RelayedCluster {
    async fn start(configure: impl Fn(&mut BrokerConfig)) -> Self {
        let mut last_error = None;
        for attempt in 1..=3 {
            match Self::try_start(&configure).await {
                Ok(cluster) => return cluster,
                Err(error) => {
                    tracing::warn!(attempt, %error, "relayed cluster start failed");
                    last_error = Some(error);
                    // intentional: a fresh start needs the failed one's ports
                    // released, which has no awaiter.
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
        panic!("relayed cluster start failed after 3 attempts: {last_error:?}");
    }

    async fn try_start(configure: &impl Fn(&mut BrokerConfig)) -> Result<Self, BrokerError> {
        support::init_tracing();
        let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
            support::bind_and_hold_ports(3).await;
        let mut relays = Vec::with_capacity(3);
        for address in &client_addrs {
            relays.push(Relay::start(*address).await);
        }
        let voters: Vec<(u64, SocketAddr)> = (0..3)
            .map(|index| (u64::try_from(index + 1).unwrap(), controller_addrs[index]))
            .collect();
        let topology = support::RoleTopology::new(&client_addrs, &controller_addrs, &voters);

        let mut starts = Vec::with_capacity(3);
        let mut metas = Vec::with_capacity(3);
        for (index, (data, controller)) in
            support::listener_pairs(client_listeners, controller_listeners)
        {
            let dir = TempDir::new().expect("tempdir");
            let mut config = support::broker_config(
                dir.path(),
                topology.node_setup(crate::support::ClusterBootstrapSetup {
                    index: crate::support::NodeIndex(index),
                    ..Default::default()
                }),
            )
            .with_internal_topics_for(3);
            config.advertised_listener = relays[index].addr().to_string();
            config.directory_id = uuid::Uuid::from_u128(u128::from(config.node_id.0));
            config.auto_join = false;
            config.bootstrap_servers = vec![];
            configure(&mut config);
            let spawned = config.clone();
            starts.push(tokio::spawn(async move {
                Broker::start_with_listeners(spawned, Some(controller), Some(data)).await
            }));
            metas.push((config, dir));
        }
        let mut brokers = Vec::with_capacity(3);
        let mut configs = Vec::with_capacity(3);
        let mut dirs = Vec::with_capacity(3);
        for (start, (config, dir)) in starts.into_iter().zip(metas) {
            let handle = support::await_broker_start(start).await?;
            brokers.push(Some(handle));
            configs.push(config);
            dirs.push(dir);
        }
        for handle in brokers.iter().flatten() {
            handle.wait_until_brokers_registered(3).await;
        }
        Ok(Self {
            brokers,
            configs,
            relays,
            _dirs: dirs,
        })
    }

    fn index_of(&self, node: u64) -> usize {
        self.configs
            .iter()
            .position(|config| config.node_id == NodeId(node))
            .expect("a cluster member")
    }

    fn handle(&self, node: u64) -> &BrokerHandle {
        self.brokers[self.index_of(node)]
            .as_ref()
            .expect("the broker runs")
    }

    /// The real listen address of `node`, past its relay.
    fn address(&self, node: u64) -> SocketAddr {
        self.configs[self.index_of(node)].listen_addr
    }

    fn relay(&self, node: u64) -> &Relay {
        &self.relays[self.index_of(node)]
    }

    fn take(&mut self, node: u64) -> BrokerHandle {
        let index = self.index_of(node);
        self.brokers[index].take().expect("the broker runs")
    }

    /// Start `node` again on the addresses it vacated, from the logs it left.
    async fn restart(&mut self, node: u64) {
        let index = self.index_of(node);
        assert!(self.brokers[index].is_none(), "broker {node} still runs");
        let mut config = self.configs[index].clone();
        config.bootstrap_mode = BootstrapMode::Rejoin;
        let handle = support::start_reusing_addrs(&config, &format!("restart broker {node}")).await;
        handle.wait_until_brokers_registered(3).await;
        self.brokers[index] = Some(handle);
    }

    async fn shutdown(mut self) {
        for handle in self.brokers.drain(..).flatten() {
            handle.shutdown().await;
        }
        for relay in self.relays.drain(..) {
            relay.shutdown().await;
        }
    }
}

async fn connected_transaction(address: SocketAddr) -> (Connection, (i64, i16)) {
    let connection = connect(address).await;
    let producer = init_producer(&connection).await;
    add_partition(&connection, producer).await;
    (connection, producer)
}

async fn cut_and_append_commit(
    cluster: &RelayedCluster,
    coordinator: u64,
    connection: Connection,
    producer: (i64, i16),
    (topic, offset): (&str, i64),
) -> tokio::task::JoinHandle<Result<EndTxnResponse, krabka_client_core::ClientError>> {
    cluster.relay(coordinator).cut();
    let commit = tokio::spawn(async move { connection.send(end_txn_request(producer)).await });
    cluster
        .handle(coordinator)
        .wait_until_local_log_end_offset(topic, 0, offset)
        .await;
    commit
}

async fn relayed_transaction_cluster(
    configure: impl Fn(&mut BrokerConfig),
) -> (RelayedCluster, Client, u64) {
    let cluster = RelayedCluster::start(configure).await;
    // __consumer_offsets needs three brokers for replication, including readers after a broker dies.
    for node in 1..=3 {
        cluster
            .handle(node)
            .wait_until_group_coordinator_ready()
            .await;
    }
    let admin = client(cluster.address(1)).await;
    let coordinator = support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, TID)
        .await
        .node_id;
    let coordinator = u64::try_from(coordinator).expect("a node id");
    (cluster, admin, coordinator)
}

/// An `EndTxn` whose `PrepareCommit` cannot reach a follower gets no answer,
/// and the transaction commits through the next coordinator when the
/// producer retries.
///
/// The coordinator's relay is cut while it leads `__transaction_state-0`, so
/// its followers stop fetching and stay in the ISR. The coordinator then dies
/// with the `PrepareCommit` in its log only. A coordinator that answered at
/// the local append told the producer the transaction committed, and the
/// follower that leads the partition next still holds the transaction as
/// `Ongoing` at the epoch before the commit's bump, which fences the producer's
/// next transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_end_txn_is_answered_only_once_its_commit_decision_is_replicated() {
    let (mut cluster, admin, coordinator) = relayed_transaction_cluster(|config| {
        config.transaction_state_num_partitions = 1;
        // The coordinator keeps its followers in the ISR while they cannot
        // reach it, so the high watermark stops below what it appends then.
        config.isr_scan_interval = krabka_units::hours(1);
    })
    .await;
    cluster
        .handle(coordinator)
        .wait_until_isr_len(STATE_TOPIC, 0, 3)
        .await;
    // The data partition lives on another broker, so its markers do not go
    // through the coordinator's relay, and it survives the coordinator.
    let data_node = (1..=3)
        .find(|node| *node != coordinator)
        .expect("another broker");
    let topic_id = create_topic(
        &admin,
        crate::support::topics::AssignedTopicSetup::on_replicas(
            TOPIC,
            vec![krabka_metadata::NodeId(data_node)],
        ),
    )
    .await;
    admin.close();
    cluster
        .handle(data_node)
        .wait_until_local_partition_leader(TOPIC, 0, NodeId(data_node))
        .await;

    let (to_coordinator, producer) = connected_transaction(cluster.address(coordinator)).await;
    let to_data = connect(cluster.address(data_node)).await;
    produce(&to_data, topic_id, Some(producer), &["a", "b", "c"]).await;

    // Cut the coordinator off from its followers, and commit.
    let state_end = cluster
        .handle(coordinator)
        .local_log_end_offset(STATE_TOPIC, 0)
        .expect("the coordinator hosts the state partition");
    let mut commit = cut_and_append_commit(
        &cluster,
        coordinator,
        to_coordinator,
        producer,
        (STATE_TOPIC, state_end + 1),
    )
    .await;
    let answered = tokio::time::timeout(Duration::from_secs(2), &mut commit).await;
    assert!(
        answered.is_err(),
        "EndTxn answered for a PrepareCommit that only its coordinator holds: {answered:?}"
    );
    let high_watermark = cluster
        .handle(coordinator)
        .high_watermark_for_test(STATE_TOPIC, 0)
        .await
        .expect("the coordinator hosts the state partition");
    assert!(high_watermark == state_end);

    // The coordinator dies with the decision, and the request with it.
    cluster.take(coordinator).crash_for_test().await;
    let lost = commit.await.expect("the EndTxn task");
    assert!(lost.is_err(), "EndTxn has no answer: {lost:?}");

    // A follower leads the state partition next. It never got the
    // PrepareCommit, so the producer's retry is what commits the transaction.
    // Transaction version 2 bumps the epoch on completion.
    let survivor = cluster.handle(data_node);
    survivor
        .wait_until_partition_leader_changed(STATE_TOPIC, 0, NodeId(coordinator))
        .await;
    let next = leader_of(survivor, STATE_TOPIC).await;
    let to_next = connect(cluster.address(next)).await;
    let committed = end_txn(&to_next, producer).await;
    assert!(
        committed
            == EndTxnResponse {
                producer_id: producer.0,
                producer_epoch: producer.1 + 1,
                ..Default::default()
            }
    );

    produce(&to_data, topic_id, None, &["z"]).await;
    let seen = read_committed(cluster.address(data_node), "z").await;
    assert!(seen == ["a", "b", "c", "z"]);

    cluster.shutdown().await;
}

/// A transaction completes only once its `COMMIT` marker is committed on the
/// data partition, so a marker lost with its leader is written again.
///
/// The coordinator leads `__transaction_state-0`, which has no other replica,
/// and the data partition, which has one follower. The coordinator's relay is
/// cut after the follower fetched the transaction's records. The follower
/// stays in the ISR but fetches nothing more, so the marker reaches the
/// coordinator's log only, and the `EndTxn` gets no answer. The coordinator
/// then dies, and the follower leads the data partition without the marker.
///
/// A coordinator that counted the marker at the local append recorded
/// `CompleteCommit` before it died. The transaction then stayed open on the
/// follower permanently. Its last stable offset stopped at the first record of
/// the transaction, and a `read_committed` consumer read nothing after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_completes_only_once_its_markers_are_committed() {
    let (mut cluster, admin, coordinator) = relayed_transaction_cluster(|config| {
        config.transaction_state_num_partitions = 1;
        // The coordinator is the only replica of the state partition, so a
        // state record commits at its append, and the cut below stops only
        // the data partition.
        config.transaction_state_replication_factor = 1;
        config.transaction_state_min_isr = 1;
        // The coordinator keeps its follower in the ISR while the follower
        // cannot reach it, so the high watermark stops below the marker.
        config.isr_scan_interval = krabka_units::hours(1);
    })
    .await;
    let follower = (1..=3)
        .find(|node| *node != coordinator)
        .expect("another broker");
    let topic_id = create_topic(
        &admin,
        crate::support::topics::AssignedTopicSetup::on_replicas(
            TOPIC,
            vec![
                krabka_metadata::NodeId(coordinator),
                krabka_metadata::NodeId(follower),
            ],
        ),
    )
    .await;
    admin.close();
    cluster
        .handle(coordinator)
        .wait_until_local_partition_leader(TOPIC, 0, NodeId(coordinator))
        .await;
    cluster
        .handle(coordinator)
        .wait_until_isr_len(TOPIC, 0, 2)
        .await;

    let (to_coordinator, producer) = connected_transaction(cluster.address(coordinator)).await;
    produce(&to_coordinator, topic_id, Some(producer), &["a", "b", "c"]).await;

    // Cut the follower off from the coordinator, and commit. The marker comes
    // after the three records.
    let mut commit =
        cut_and_append_commit(&cluster, coordinator, to_coordinator, producer, (TOPIC, 4)).await;
    let answered = tokio::time::timeout(Duration::from_secs(2), &mut commit).await;
    assert!(
        answered.is_err(),
        "EndTxn answered for a marker that only its leader holds: {answered:?}"
    );
    let to_coordinator = connect(cluster.address(coordinator)).await;
    let preparing: Described = (
        "PrepareCommit".to_owned(),
        vec![(TOPIC.to_owned(), vec![0])],
    );
    assert!(describe_until(&to_coordinator, &preparing).await == Some(preparing));
    to_coordinator.close();

    // The coordinator dies with the marker, and the follower leads the data
    // partition without it.
    cluster.take(coordinator).crash_for_test().await;
    let lost = commit.await.expect("the EndTxn task");
    assert!(lost.is_err(), "EndTxn has no answer: {lost:?}");
    cluster
        .handle(follower)
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(coordinator))
        .await;

    // The coordinator comes back with the `PrepareCommit`, and writes the
    // marker again on the follower that now leads the partition.
    cluster.relay(coordinator).heal();
    cluster.restart(coordinator).await;
    let to_follower = connect(cluster.address(follower)).await;
    produce(&to_follower, topic_id, None, &["z"]).await;
    let seen = read_committed(cluster.address(follower), "z").await;
    assert!(seen == ["a", "b", "c", "z"]);
    let to_coordinator = connect(cluster.address(coordinator)).await;
    let completed: Described = ("CompleteCommit".to_owned(), vec![]);
    assert!(describe_until(&to_coordinator, &completed).await == Some(completed));

    cluster.shutdown().await;
}

/// While a follower of `__consumer_offsets-0` is down but still in its ISR, no
/// offsets record commits. The group coordinator then answers neither a
/// `TxnOffsetCommit` nor an `OffsetDelete` with success: each write times out
/// after Kafka's `offsets.commit.timeout.ms`, which
/// `CoordinatorOperationExceptionHelper` answers `COORDINATOR_NOT_AVAILABLE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_coordinator_writes_are_answered_only_once_committed() {
    let mut cluster = crate::txn_harness::registered_transaction_cluster(|_, config| {
        config.offsets_topic_num_partitions = 1;
        config.transaction_state_num_partitions = 1;
        // A stopped follower stays in the ISR: the leader does not shrink it
        // on lag, and the controller does not fence it, while the test runs.
        config.isr_scan_interval = krabka_units::hours(1);
        config.heartbeat_timeout = krabka_units::minutes(10);
    })
    .await;
    let address = |cluster: &[(BrokerHandle, BrokerConfig, TempDir)], node: u64| {
        cluster
            .iter()
            .find(|(handle, _, _)| handle.node_id() == node)
            .map(|(_, config, _)| config.listen_addr)
            .expect("a cluster member")
    };

    let admin = client(cluster[0].1.listen_addr).await;
    let group_node = u64::try_from(
        support::find_coordinator(&admin, support::KEY_TYPE_GROUP, GROUP)
            .await
            .node_id,
    )
    .expect("a node id");
    let txn_node = u64::try_from(
        support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, TID)
            .await
            .node_id,
    )
    .expect("a node id");
    let topic_id = create_topic(
        &admin,
        crate::support::topics::AssignedTopicSetup::on_replicas(
            TOPIC,
            vec![krabka_metadata::NodeId(group_node)],
        ),
    )
    .await;
    admin.close();
    // The group coordinator resolves the offsets below by topic id, so its
    // broker has to hold the topic first.
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(TOPIC, 0).await;
    }
    cluster[0].0.wait_until_isr_len(OFFSETS_TOPIC, 0, 3).await;
    cluster[0].0.wait_until_isr_len(STATE_TOPIC, 0, 3).await;
    assert!(leader_of(&cluster[0].0, OFFSETS_TOPIC).await == group_node);

    // A committed offset, for the delete below, and an open transaction that
    // holds the group's offsets partition.
    let to_group = connect(address(&cluster, group_node)).await;
    let committed = to_group
        .send(OffsetCommitRequest {
            group_id: GROUP.into(),
            generation_id_or_member_epoch: -1,
            topics: vec![offset_commit_topic(
                TOPIC,
                topic_id,
                vec![offset_commit_partition(0, 5, None)],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        committed.topics[0].partitions[0].error_code == 0,
        "{committed:?}"
    );
    let to_txn = connect(address(&cluster, txn_node)).await;
    let (producer_id, producer_epoch) = init_producer(&to_txn).await;
    let added = to_txn
        .send(AddOffsetsToTxnRequest {
            transactional_id: TID.into(),
            producer_id,
            producer_epoch,
            group_id: GROUP.into(),
            ..Default::default()
        })
        .await
        .expect("AddOffsetsToTxn");
    assert!(added.error_code == 0, "{added:?}");

    // Stop a follower of the offsets partition that is neither coordinator.
    let follower = cluster
        .iter()
        .position(|(handle, _, _)| ![group_node, txn_node].contains(&handle.node_id()))
        .expect("a follower");
    let (stopped, _, stopped_dir) = cluster.remove(follower);
    stopped.crash_for_test().await;

    let offsets = to_group
        .send(TxnOffsetCommitRequest {
            transactional_id: TID.into(),
            group_id: GROUP.into(),
            producer_id,
            producer_epoch,
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![txn_offset_topic(
                TOPIC,
                topic_id,
                vec![txn_offset_partition(
                    krabka_ids::PartitionIndex(0),
                    krabka_ids::Offset(7),
                )],
            )],
            ..Default::default()
        })
        .await
        .expect("TxnOffsetCommit");
    let codes: Vec<(i32, i16)> = offsets
        .topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| (row.partition_index, row.error_code))
        .collect();
    assert!(codes == [(0, COORDINATOR_NOT_AVAILABLE)], "{offsets:?}");

    let deleted = to_group
        .send(offset_delete_request(
            GROUP,
            vec![offset_delete_topic(TOPIC, vec![offset_delete_partition(0)])],
        ))
        .await
        .expect("OffsetDelete");
    assert!(
        deleted
            == OffsetDeleteResponse {
                error_code: COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            }
    );

    crate::support::shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
