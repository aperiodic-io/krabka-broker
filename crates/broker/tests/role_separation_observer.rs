//! Component B integration test, with a controller-only node and broker-only
//! observers.
//!
//! An observer replicates metadata through fetch, not through openraft. A
//! `CreateTopics` forwarded through the observer reaches the controller and
//! comes back to the observer's image. The observer never joins the voter
//! set.
//!
//! The second test covers the other half of role separation: the brokers stay
//! *unfenced*. A broker's `BrokerHeartbeat` goes to the controller leader's
//! CONTROLLER listener, which is the only endpoint a controller-only node
//! publishes for itself, and the controller's liveness registry fences every
//! registered broker it does not hear from within `heartbeat_timeout`. It also
//! covers what the metadata surface then advertises: `controller_id` has to
//! name a broker the caller can resolve out of the same response, which the
//! controller-only node's own id never is. The controller-only node itself
//! opens no client listener, so a client never reaches it at all.

use {
    crate::support::{
        NodeIndex, RoleNodeSetup,
        client::{connect_client, create_topic},
        quorum::metadata_quorum_request,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    krabka_broker::{
        BootstrapMode, Broker, BrokerConfig, BrokerHandle,
        config::{DEFAULT_READINESS_MAX_METADATA_LAG, NodeRole},
        health::HealthState,
    },
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_log_dirs_request::DescribeLogDirsRequest, metadata_request::MetadataRequest,
    },
    std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

mod support;

/// A booted role-separated cluster: one controller-only node that is the sole
/// voter, and `n` broker-only observers.
struct RoleSeparated {
    controller: BrokerHandle,
    /// The client address in the controller-only node's config. The harness
    /// hands the node a listener bound on it, which the node closes.
    controller_client_addr: std::net::SocketAddr,
    brokers: Vec<BrokerHandle>,
    /// The broker-only nodes' configs, index-aligned with `brokers`, so a test
    /// can stop one and start it again on the same ports and log dir.
    broker_configs: Vec<BrokerConfig>,
    /// The controller-only node's metadata log directory, where its
    /// `__cluster_metadata-0` log and checkpoints live.
    controller_metadata_dir: std::path::PathBuf,
    // Dropping these removes the log dirs the nodes still hold open.
    _dirs: Vec<TempDir>,
}

impl RoleSeparated {
    /// Every node's handle, controller first. The fencing state is replicated,
    /// so an assertion about it has to hold on all of them.
    fn nodes(&self) -> impl Iterator<Item = &BrokerHandle> {
        std::iter::once(&self.controller).chain(&self.brokers)
    }

    async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

/// Boot node 1 as controller-only and nodes `2..=brokers + 1` as broker-only.
///
/// Node 1 is the whole voter set, so it elects itself and the observers reach
/// it by fetching `__cluster_metadata`. The controller is up and leading
/// before the first observer starts, so an observer's first fetch already has
/// a committed log to replicate.
async fn start_role_separated(brokers: usize) -> RoleSeparated {
    start_role_separated_with(brokers, |_, _| {}).await
}

/// Like [`start_role_separated`], but each node's config passes through
/// `customize` first. It is called with the node's index, so a test can settle
/// something on the controller (index 0) without also changing the observers.
async fn start_role_separated_with(
    brokers: usize,
    customize: impl Fn(usize, &mut BrokerConfig),
) -> RoleSeparated {
    let nodes = brokers + 1;
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(nodes).await;
    let voters = vec![(1u64, controller_addrs[0])];
    let topology = support::RoleTopology::new(&client_addrs, &controller_addrs, &voters);
    let mut data_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let mut dirs = Vec::with_capacity(nodes);

    let ctrl_dir = TempDir::new().unwrap();
    let mut ctrl_cfg = topology.config(ctrl_dir.path(), RoleNodeSetup {
        role: NodeRole::Controller,
        ..Default::default()
    });
    customize(0, &mut ctrl_cfg);
    let controller_metadata_dir = ctrl_cfg.metadata_dir().to_path_buf();
    let controller_client_addr = ctrl_cfg.listen_addr;
    // Boxed, as is each broker's start below: a node's start future holds
    // its whole `BrokerConfig`, and every test that awaits this helper would
    // otherwise carry it inline, past `clippy::large_futures`.
    let controller = Box::pin(support::start_held_node(
        ctrl_cfg,
        &mut ctrl_ls,
        &mut data_ls,
        "controller-only start",
    ))
    .await;
    dirs.push(ctrl_dir);
    controller.wait_until_controller_leader().await;

    let mut observers = Vec::with_capacity(brokers);
    let mut broker_configs = Vec::with_capacity(brokers);
    for index in 1..nodes {
        let dir = TempDir::new().unwrap();
        let mut cfg = topology.config(dir.path(), RoleNodeSetup {
            index: NodeIndex(index),
            mode: BootstrapMode::Join,
            ..Default::default()
        });
        customize(index, &mut cfg);
        broker_configs.push(cfg.clone());
        observers.push(
            Box::pin(support::start_held_node(
                cfg,
                &mut ctrl_ls,
                &mut data_ls,
                "broker-only start",
            ))
            .await,
        );
        dirs.push(dir);
    }

    // A broker-only node self-registers (it IS a broker) by forwarding the
    // registration to the controller. Wait until the controller's committed
    // image reflects every one of them, so `CreateTopics` has brokers to place
    // replicas on.
    controller.wait_until_brokers_registered(brokers).await;
    RoleSeparated {
        controller,
        controller_client_addr,
        brokers: observers,
        broker_configs,
        controller_metadata_dir,
        _dirs: dirs,
    }
}

/// Long enough for a controller to notice that a broker has gone silent:
/// `heartbeat_timeout` (2s under the test config) for the session to expire,
/// plus a `liveness_tick_interval` (1s) for the tick that publishes the
/// decision, plus slack.
const FENCING_WINDOW: Duration = Duration::from_secs(4);

/// Every broker any node currently reports as fenced.
///
/// The fencing decision is replicated on the broker's registration, so
/// an observer's image carries it as surely as the controller's.
fn fenced_anywhere(cluster: &RoleSeparated) -> BTreeSet<u64> {
    cluster
        .nodes()
        .flat_map(BrokerHandle::fenced_broker_ids_for_test)
        .collect()
}

/// Sleep past [`FENCING_WINDOW`], then require that no broker is fenced.
///
/// Sampling right after boot proves nothing: a leadership change seeds every
/// registered broker alive, so a cluster whose heartbeats never arrive still
/// looks healthy until the first session expires. Waiting out that window
/// first is what makes the assertion mean "the heartbeats are landing".
async fn assert_settled_unfenced(cluster: &RoleSeparated) {
    tokio::time::sleep(FENCING_WINDOW).await;
    let fenced = fenced_anywhere(cluster);
    assert!(
        fenced.is_empty(),
        "brokers fenced in a role-separated cluster: {fenced:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_observes_and_forwards() {
    support::init_tracing();

    let cluster = start_role_separated(1).await;
    let broker_only = &cluster.brokers[0];
    let broker_only_id = broker_only.node_id();

    // Settle before asserting anything else. Without this the suite finishes
    // inside the seed window, so it would pass while every broker was on its
    // way to being fenced forever.
    assert_settled_unfenced(&cluster).await;

    // CreateTopics against the broker-only node — forwarded to the controller
    // quorum via the observer's write path.
    let topic = "rolesep-observed";
    let client = connect_client(broker_only.listen_addr().to_string(), None).await;
    let resp = client
        .send(create_topic_request(
            creatable_topic().name((topic).into()).call(),
        ))
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 0,
        "create via broker-only node forwards to the controller and succeeds"
    );

    // Assertion 1: the topic propagates back to the broker-only node's image
    // via observer fetch (it is not a voter, so this cannot be a raft apply).
    broker_only.wait_until_partition_present(topic, 0).await;

    // Assertion 2: the controller itself committed the forwarded topic.
    assert!(
        cluster.controller.has_partition(topic, 0),
        "controller committed the forwarded CreateTopics"
    );

    // Assertion 3: the broker-only node is NOT in the controller's voter set.
    let quorum_voters: BTreeSet<u64> = cluster
        .controller
        .quorum_voters_for_test()
        .into_iter()
        .map(|n| n.0)
        .collect();
    assert!(quorum_voters.contains(&1), "the controller is a voter");
    assert!(
        !quorum_voters.contains(&broker_only_id),
        "the broker-only node must never join the voter quorum"
    );

    cluster.shutdown().await;
}

/// A restarted node and the health state its startup marked.
struct RestartedBroker {
    handle: BrokerHandle,
    health: HealthState,
}

/// A broker row of a `DescribeCluster` answer.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ListedBroker {
    broker_id: i32,
    is_fenced: bool,
}

/// Restart a broker-only node on the same ports and log dir it was using, and
/// return the [`HealthState`] its startup marked, so the caller can read the
/// readiness verdict the node would serve on `/readyz`.
///
/// The vacated ports are not always immediately bindable on Linux, so this
/// retries `AddrInUse` the way `support::start_reusing_addrs` does.
async fn restart_broker_only(cfg: &BrokerConfig) -> RestartedBroker {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let health = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
        match Broker::start_with_health(cfg.clone(), health.clone()).await {
            Ok(handle) => return RestartedBroker { handle, health },
            Err(error) if Instant::now() < deadline => {
                tracing::warn!(%error, "broker-only restart failed; retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("broker-only restart: {error:?}"),
        }
    }
}

/// A broker-only node that comes back after the controller has snapshotted and
/// pruned `__cluster_metadata` past the offset it asks for.
///
/// The observer fetches by offset, and the controller drops the log prefix
/// behind every KIP-630 snapshot. A restarted observer therefore asks for
/// records that no longer exist. Answered with an empty slice it re-asks for
/// the same gone offset on every poll: the image stays empty forever, the node
/// never registers, and `/readyz` reports a lag it can never close — until
/// somebody deletes the controller's checkpoints by hand.
///
/// So the controller answers such a fetch with the id of the snapshot that
/// replaced those records, and the observer installs it before resuming. This
/// test drives exactly that: a snapshot interval low enough that a handful of
/// topics crosses it several times, then a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_only_node_recovers_after_the_controller_prunes_past_its_fetch_offset() {
    support::init_tracing();

    // Only the controller snapshots aggressively, and it keeps no log beyond
    // its newest snapshot, so each cleaning moves its log start up. The
    // observer keeps the default checkpoint interval, so it writes none of its
    // own over a handful of topics and restarts at the log start — which is
    // the offset the controller has pruned away, and the case this test is
    // about.
    let cluster = start_role_separated_with(1, |index, cfg| {
        if index == 0 {
            cfg.metadata_snapshot_interval_records = 4;
            cfg.metadata_log.max_retention_size = Some(krabka_units::bytes(0));
        }
    })
    .await;
    let broker_only_id = cluster.brokers[0].node_id();

    // Enough topics that the committed offset crosses the 4-record interval
    // several times, so the controller has snapshotted and pruned well past 0.
    let topics: Vec<String> = (0..6).map(|i| format!("pruned-topic-{i}")).collect();
    let client = connect_client(cluster.brokers[0].listen_addr().to_string(), None).await;
    create_topics(&client, &topics).await;
    for topic in &topics {
        cluster.brokers[0]
            .wait_until_partition_present(topic, 0)
            .await;
    }

    // The controller really has taken a snapshot — the prune that strands a
    // restarting observer happens in the same step that writes this file.
    let checkpoints = krabka_raft::metadata_partition_dir(&cluster.controller_metadata_dir);
    let snapshot_written = std::fs::read_dir(&checkpoints).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".checkpoint"))
        })
    });
    assert!(
        snapshot_written,
        "the controller should have snapshotted and pruned at a 4-record interval; {checkpoints:?}"
    );

    // Stop the broker-only node and bring it back on the same ports and dir.
    let cfg = cluster.broker_configs[0].clone();
    let RoleSeparated {
        controller,
        mut brokers,
        _dirs,
        ..
    } = cluster;
    brokers.remove(0).shutdown().await;
    let RestartedBroker {
        handle: restarted,
        health,
    } = restart_broker_only(&cfg).await;

    // It rebuilt its whole image from the snapshot the controller pointed it
    // at: every topic committed before the restart is there, including the ones
    // whose records were pruned away.
    for topic in &topics {
        restarted.wait_until_partition_present(topic, 0).await;
    }
    // And it registered again, which is what an empty image would have
    // stopped: the controller places replicas from `image.brokers()`.
    restarted.wait_until_brokers_registered(1).await;
    assert!(
        controller
            .controller_image_for_test()
            .brokers()
            .any(|broker| broker.node_id.0 == broker_only_id),
        "the restarted broker-only node re-registers with the controller"
    );
    // Startup marked every readiness condition, so `/readyz` answers 200
    // instead of reporting a metadata lag this node could never close.
    assert!(
        health.readiness().is_ok(),
        "restarted broker-only node is not ready: {:?}",
        health.readiness()
    );

    restarted.shutdown().await;
    controller.shutdown().await;
}

/// A controller-only node never registers itself as a broker, so the only
/// address it publishes is the CONTROLLER endpoint of its
/// `ControllerRegistrationRecord`. When the heartbeat client looked the leader
/// up as a broker instead, every tick bailed out, no heartbeat ever reached
/// the controller, and roughly one `liveness_tick_interval` after boot the
/// controller fenced the registration of every broker in the cluster —
/// permanently, since nothing could ever unfence them.
///
/// So this holds the assertion across several liveness ticks and past
/// `heartbeat_timeout`, rather than sampling it once: the broken behaviour
/// takes a session expiry to show up, and a single early sample would see the
/// seeded-alive state and pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_keep_brokers_unfenced_in_a_role_separated_cluster() {
    support::init_tracing();

    let cluster = start_role_separated(2).await;
    assert_settled_unfenced(&cluster).await;

    // Then hold it across several more liveness ticks, so a cluster that
    // fences on any later tick — rather than on the first expiry — is caught
    // too.
    let hold_until = Instant::now() + FENCING_WINDOW;
    while Instant::now() < hold_until {
        let fenced = fenced_anywhere(&cluster);
        assert!(
            fenced.is_empty(),
            "brokers fenced while every one of them was heartbeating: {fenced:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The client-visible half (KIP-1073): `DescribeCluster` hides fenced
    // brokers unless the caller opts in, so a fenced cluster answers with no
    // broker rows at all.
    for broker in &cluster.brokers {
        let client = connect_client(broker.listen_addr().to_string(), None).await;
        let resp = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        assert!(resp.error_code == 0);
        let rows: BTreeSet<ListedBroker> = resp
            .brokers
            .iter()
            .map(|row| ListedBroker {
                broker_id: row.broker_id,
                is_fenced: row.is_fenced,
            })
            .collect();
        assert!(
            rows == BTreeSet::from([
                ListedBroker {
                    broker_id: 2,
                    is_fenced: false,
                },
                ListedBroker {
                    broker_id: 3,
                    is_fenced: false,
                },
            ]),
            "DescribeCluster on node {} must list both brokers unfenced",
            broker.node_id()
        );
        // The advertised controller has to be one a client can resolve. A
        // client reads `controller_id` back out of the `brokers` array of the
        // same response, so the controller-only node's own id would be as
        // useless to it as the -1 a wholly fenced cluster answers with.
        assert!(
            resp.controller_id == 2 || resp.controller_id == 3,
            "DescribeCluster on node {} advertised controller_id {}, which is not a listed broker",
            broker.node_id(),
            resp.controller_id
        );
    }

    assert_metadata_names_a_reachable_controller(&cluster).await;

    cluster.shutdown().await;
}

/// `Metadata` has to name a `controller_id` the caller can resolve.
///
/// In `KRaft` the field is not the quorum leader: `apache/kafka:4.3.1` answers it
/// with `metadataCache.getRandomAliveBrokerId().orElse(-1)`, an unfenced
/// registered broker. A role-separated cluster is where the difference bites,
/// because the quorum leader is a controller-only node that never appears in
/// the `brokers` array the client resolves the id against.
///
/// So this asserts the id resolves *within the same response*, and that the
/// endpoint it resolves to is one a client can actually reach.
async fn assert_metadata_names_a_reachable_controller(cluster: &RoleSeparated) {
    for broker in &cluster.brokers {
        let client = connect_client(broker.listen_addr().to_string(), None).await;
        let resp = client.send(MetadataRequest::default()).await.unwrap();

        let listed: BTreeSet<i32> = resp.brokers.iter().map(|row| row.node_id).collect();
        assert!(
            listed == BTreeSet::from([2, 3]),
            "Metadata on node {} must list both brokers: {listed:?}",
            broker.node_id()
        );
        let named = resp
            .brokers
            .iter()
            .find(|row| row.node_id == resp.controller_id);
        assert!(
            named.is_some(),
            "Metadata on node {} advertised controller_id {}, which is absent from {listed:?}",
            broker.node_id(),
            resp.controller_id
        );

        // Reachable, not merely listed: the advertised endpoint answers.
        let endpoint = named.unwrap();
        let controller_client =
            connect_client(format!("{}:{}", endpoint.host, endpoint.port), None).await;
        let echoed = controller_client
            .send(MetadataRequest::default())
            .await
            .unwrap();
        assert!(echoed.cluster_id == resp.cluster_id);
    }
}

/// The controller-only node opens no client listener, as Kafka's
/// `ControllerServer` opens only the listeners that `controller.listener.names`
/// names. The harness hands it a listener bound on its client address, and the
/// node closes it, so a connect there is refused. Its one listener is the
/// controller listener.
///
/// [`assert_metadata_names_a_reachable_controller`] asks the observers which
/// node is the controller. No client can ask the controller-only node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_only_node_opens_no_client_listener() {
    support::init_tracing();

    let cluster = start_role_separated(1).await;
    let connect = tokio::net::TcpStream::connect(cluster.controller_client_addr)
        .await
        .map(drop)
        .map_err(|error| error.kind());

    assert!(
        (
            cluster.controller.data_plane_addr(),
            cluster.controller.listen_addr(),
            connect,
        ) == (
            None,
            cluster.controller.controller_addr(),
            Err(std::io::ErrorKind::ConnectionRefused),
        )
    );

    cluster.shutdown().await;
}

/// A broker-only node forwards `DescribeQuorum` to the active controller
/// quorum leader (#392). The response reports the controller's real term,
/// leader, and voter set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_node_forwards_describe_quorum_to_controller() {
    support::init_tracing();

    let cluster = start_role_separated(1).await;
    assert_settled_unfenced(&cluster).await;

    let broker = &cluster.brokers[0];
    let client = connect_client(broker.listen_addr().to_string(), None).await;

    let req = metadata_quorum_request();

    let resp = client.send(req).await.unwrap();
    assert!(resp.error_code == 0, "top-level error_code must be NONE");
    assert!(resp.topics.len() == 1);
    let topic = &resp.topics[0];
    assert!(topic.topic_name == "__cluster_metadata");
    assert!(topic.partitions.len() == 1);
    let part = &topic.partitions[0];
    assert!(part.error_code == 0);
    assert!(
        part.leader_id == 1,
        "controller-only node 1 is the leader; got {}",
        part.leader_id
    );
    assert!(part.leader_epoch >= 1, "leader epoch must be >= 1");
    assert!(
        part.current_voters.iter().any(|v| v.replica_id == 1),
        "voters must include node 1"
    );

    cluster.shutdown().await;
}

/// Every broker-only node fetches `__cluster_metadata` from the controller, so
/// the controller's `DescribeQuorum` lists each one as an observer under its
/// node id, with the time of its last fetch. Kafka lists a broker the same way,
/// and `kafka-metadata-quorum describe --replication` shows them under
/// `Observer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_only_nodes_are_described_as_quorum_observers() {
    support::init_tracing();

    let cluster = start_role_separated(2).await;
    let broker_ids: BTreeSet<i32> = cluster
        .brokers
        .iter()
        .map(|broker| i32::try_from(broker.node_id()).expect("small node id"))
        .collect();
    let client = connect_client(cluster.brokers[0].listen_addr().to_string(), None).await;
    let request = metadata_quorum_request;

    // The observers appear once each has completed a fetch, and their caught-up
    // time is set once each has fetched to the end of a log that nothing is
    // appending to any more, so poll for both.
    let deadline = Instant::now() + Duration::from_secs(30);
    let observers = loop {
        let resp = client.send(request()).await.unwrap();
        let partition = &resp.topics[0].partitions[0];
        let listed: BTreeSet<i32> = partition
            .observers
            .iter()
            .map(|observer| observer.replica_id)
            .collect();
        if partition.error_code == 0
            && listed == broker_ids
            && partition
                .observers
                .iter()
                .all(|observer| observer.last_caught_up_timestamp > 0)
        {
            break partition.observers.clone();
        }
        assert!(
            Instant::now() < deadline,
            "the brokers never appeared as observers: {:?}",
            partition.observers
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    for observer in &observers {
        assert!(
            observer.last_fetch_timestamp > 1_700_000_000_000,
            "observer {} has a real fetch time, got {}",
            observer.replica_id,
            observer.last_fetch_timestamp
        );
        // Issue #1193's main complaint: an observer that had caught up read -1.
        assert!(
            observer.last_caught_up_timestamp > 1_700_000_000_000,
            "observer {} has a real caught-up time, got {}",
            observer.replica_id,
            observer.last_caught_up_timestamp
        );
        assert!(
            observer.last_caught_up_timestamp <= observer.last_fetch_timestamp,
            "observer {} caught up at {}, after its last fetch at {}",
            observer.replica_id,
            observer.last_caught_up_timestamp,
            observer.last_fetch_timestamp
        );
        assert!(
            observer.log_end_offset >= 0,
            "observer {} has a fetch offset",
            observer.replica_id
        );
    }

    cluster.shutdown().await;
}

/// `UnregisterBroker` builds partition records from the image of the node that
/// runs it, so Kafka runs it only on the active controller: `KafkaApis`
/// forwards it. A broker-only node's image can trail the controller's, and a
/// record built from a trailing image would roll back what the controller
/// committed since.
///
/// The controller alone names break-glass approvers here, so a broker-only node
/// that ran the request itself would let it through. The controller refuses it,
/// which shows that the controller decided.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_broker_sent_to_a_broker_only_node_is_decided_by_the_controller() {
    use krabka_protocol::owned::unregister_broker_request::UnregisterBrokerRequest;

    support::init_tracing();

    let cluster = start_role_separated_with(2, |index, cfg| {
        if index == 0 {
            cfg.break_glass.approvers = vec!["User:alice".into(), "User:bob".into()];
        }
    })
    .await;
    let client = connect_client(cluster.brokers[0].listen_addr().to_string(), None).await;
    let doomed = i32::try_from(cluster.brokers[1].node_id()).expect("small node id");

    let resp = client
        .send(UnregisterBrokerRequest {
            broker_id: doomed,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(
        resp.error_code == 44,
        "the controller's two-person rule refuses it: {resp:?}"
    );
    assert!(
        resp.error_message
            == Some(format!(
                "break-glass refused unregister_broker on {doomed}: no approved proposal covers the request"
            ))
    );
    assert!(
        cluster
            .controller
            .controller_image_for_test()
            .broker(krabka_broker::NodeId(u64::try_from(doomed).unwrap()))
            .is_some(),
        "the refused unregistration left the registration alone"
    );

    cluster.shutdown().await;
}

/// The same request against a cluster that gates nothing is forwarded through
/// the `Envelope` and answered by the controller, and it drops the
/// registration from the controller's image.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_broker_sent_to_a_broker_only_node_reaches_the_controller() {
    use krabka_protocol::owned::unregister_broker_request::UnregisterBrokerRequest;

    support::init_tracing();

    let cluster = start_role_separated(2).await;
    let client = connect_client(cluster.brokers[0].listen_addr().to_string(), None).await;
    let doomed = cluster.brokers[1].node_id();

    let resp = client
        .send(UnregisterBrokerRequest {
            broker_id: i32::try_from(doomed).expect("small node id"),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(resp.error_code == 0, "{resp:?}");
    cluster
        .controller
        .wait_for_image(|image| image.broker(krabka_broker::NodeId(doomed)).is_none())
        .await;

    cluster.shutdown().await;
}

/// The names of the entries directly in `dir`, sorted.
fn entries(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir).map_or_default(|entries| {
        entries
            .flatten()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect()
    });
    names.sort();
    names
}

/// Whether `dir` holds a KIP-630 snapshot, the file Kafka's system test looks
/// for with `ls __cluster_metadata-0/*.checkpoint`.
fn holds_a_checkpoint(dir: &std::path::Path) -> bool {
    entries(dir).iter().any(|name| {
        std::path::Path::new(name)
            .extension()
            .is_some_and(|ext| ext == "checkpoint")
    })
}

/// Kafka's `metadata.log.dir`, apart from the data directories, on every node.
///
/// This is what Kafka's `snapshot_test.py` checks. The controller keeps the
/// `__cluster_metadata-0` log and its snapshots in its metadata directory. Its
/// KIP-835 no-op records roll the log by `metadata.log.segment.ms`, and
/// `metadata.max.retention.bytes` cleans it until the first segment file is
/// gone. No partition lands in a metadata directory, and `DescribeLogDirs`
/// reports the data directory only. A broker-only node whose directories are
/// wiped then comes back and rebuilds its image from the controller's
/// snapshot, because the records it would replay are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_separate_metadata_log_directory_rolls_cleans_and_restores_a_wiped_node() {
    support::init_tracing();

    let cluster = start_role_separated_with(1, |index, cfg| {
        let root = cfg.log_dir.clone();
        cfg.log_dir = root.join("data");
        cfg.metadata_log_dir = Some(root.join("metadata"));
        if index == 0 {
            // The snapshot test's settings, with a shorter roll interval and
            // a faster idle interval so the test does not wait for minutes.
            cfg.metadata_max_bytes_between_snapshots = krabka_units::bytes(2048);
            cfg.metadata_log = krabka_raft::MetadataLogConfig {
                segment_roll_interval: krabka_units::secs(1),
                max_retention_size: Some(krabka_units::bytes(2048)),
                max_idle_interval: krabka_units::millis(50),
                ..krabka_raft::MetadataLogConfig::default()
            };
        }
    })
    .await;
    let broker_cfg = cluster.broker_configs[0].clone();
    let broker_data = broker_cfg.log_dir.clone();
    let broker_metadata = broker_cfg.metadata_dir().to_path_buf();

    let topic_names = [
        "metadata-dir-topic-0",
        "metadata-dir-topic-1",
        "metadata-dir-topic-2",
    ];
    let client = connect_client(cluster.brokers[0].listen_addr().to_string(), None).await;
    create_topics(&client, &topic_names.map(String::from)).await;
    for &topic in &topic_names {
        cluster.brokers[0]
            .wait_until_local_log_end_offset(topic, 0, 0)
            .await;
    }

    // The controller cleans its metadata log: the first segment goes and a
    // snapshot stays, within the snapshot test's 100 seconds.
    let partition_dir = krabka_raft::metadata_partition_dir(&cluster.controller_metadata_dir);
    let deadline = Instant::now() + Duration::from_secs(100);
    while partition_dir.join("00000000000000000000.log").exists()
        || !holds_a_checkpoint(&partition_dir)
    {
        assert!(
            Instant::now() < deadline,
            "the first metadata segment was never cleaned: {:?}",
            entries(&partition_dir)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The partitions are in the data directory. The controller's metadata
    // directory holds the metadata partition only, and the broker-only node's
    // holds no partition: its observer keeps a checkpoint there only once it
    // installs or writes one.
    let mut want_data: Vec<String> = topic_names
        .iter()
        .map(|topic| format!("{topic}-0"))
        .collect();
    want_data.sort();
    let partitions_in = |dir: &std::path::Path| -> Vec<String> {
        entries(dir)
            .into_iter()
            .filter(|name| name.starts_with("metadata-dir-topic-"))
            .collect()
    };
    assert!(
        (
            partitions_in(&broker_data),
            partitions_in(&broker_metadata),
            entries(&cluster.controller_metadata_dir),
            krabka_raft::metadata_partition_dir(&broker_data).exists(),
        ) == (
            want_data,
            Vec::<String>::new(),
            vec![krabka_raft::METADATA_PARTITION_DIR.to_owned()],
            false,
        )
    );
    let described = client
        .send(DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        described
            .results
            .iter()
            .map(|result| result.log_dir.clone())
            .collect::<Vec<_>>()
            == vec![broker_data.display().to_string()]
    );

    // Wipe the broker-only node and bring it back on the same ports. Its log
    // starts empty, below the controller's log start, so it has to install
    // the controller's snapshot.
    let RoleSeparated {
        controller,
        mut brokers,
        _dirs,
        ..
    } = cluster;
    brokers.remove(0).shutdown().await;
    std::fs::remove_dir_all(&broker_data).unwrap();
    std::fs::remove_dir_all(&broker_metadata).unwrap();
    let RestartedBroker {
        handle: restarted,
        health: _health,
    } = restart_broker_only(&broker_cfg).await;
    for topic in &topic_names {
        restarted.wait_until_partition_present(topic, 0).await;
    }

    restarted.shutdown().await;
    controller.shutdown().await;
}

async fn create_topics(client: &Client, topics: &[String]) {
    for topic in topics {
        create_topic(client, topic, 1).await;
    }
}
