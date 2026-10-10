//! The three-broker cluster the fault schedule runs on: its boot, the topic it
//! creates, and the metadata waits and edits the schedule needs.
//!
//! The boot differs from a plain `start_n_node` cluster in four ways, and each
//! one is a requirement of the diskless WAL rather than a preference.
//!
//! * **Distinct racks.** `wal::quorum::placement` refuses to weaken the
//!   AZ-loss failure budget, so two brokers that share a rack yield a short
//!   voter list and the reconcile loop never runs a three-voter quorum.
//! * **One shared object store.** A flush written by one broker is read back
//!   by another.
//! * **A topic-backed metadata log.** The WAL flush index is a Kafka topic
//!   that the whole cluster consumes.
//! * **An authenticated inter-broker listener.** A WAL follower's Fetch is
//!   authorized against the caller's principal. See the suite's module
//!   comment for why that listener is separate from the client one here.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use assert2::assert;
use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, NodeId,
    RemoteStorageBackend, RlmmKind, config::ListenerSpec,
};
use krabka_metadata::MetadataRecord;
use krabka_security::ListenerProtocol;
use tempfile::TempDir;

use crate::{PASSWORD, TOPIC, VOTERS, support, support::client::connect_owned};

/// The listener every client in this suite and every JVM container speaks to.
const CLIENT_LISTENER: &str = "PLAINTEXT";

/// The listener the brokers speak to each other on, and the one
/// `inter_broker_listener_name` selects.
const INTER_BROKER_LISTENER: &str = "SASL_PLAINTEXT";

/// One booted broker, the config it booted from, and the log directory that
/// must outlive it.
///
/// `handle` is `None` once the broker is crashed. The config stays, because
/// the schedule keeps reading the crashed node's ids and paths after it dies.
pub(crate) struct TestNode {
    pub(crate) handle: Option<BrokerHandle>,
    pub(crate) config: BrokerConfig,
    /// The port of the client-facing `PLAINTEXT` listener. It is read off the
    /// listener rather than off `config.listen_addr`, which names the
    /// inter-broker listener on a multi-listener broker.
    client_port: u16,
    _data_dir: TempDir,
}

impl TestNode {
    pub(crate) fn handle(&self) -> &BrokerHandle {
        self.handle.as_ref().expect("broker is live")
    }

    /// What a client on this host bootstraps against.
    pub(crate) fn client_bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.client_port)
    }

    /// What a client inside a container bootstraps against. Containers resolve
    /// the name through `--add-host=host.docker.internal:host-gateway`; the
    /// host resolves it through an `/etc/hosts` entry pointing at loopback,
    /// which CI adds before running the container suites.
    pub(crate) fn docker_bootstrap(&self) -> String {
        format!("host.docker.internal:{}", self.client_port)
    }

    /// This broker's canonical partition log for the suite's one partition.
    pub(crate) fn partition_dir(&self) -> PathBuf {
        self.config.log_dir.join(format!("{TOPIC}-0"))
    }
}

/// Boot `VOTERS` brokers that can run a diskless quorum between them, all
/// flushing into `object_dir`.
pub(crate) async fn start_jepsen_cluster(object_dir: &Path) -> Vec<TestNode> {
    support::init_tracing();

    // Loopback addresses for the inter-broker listener and the controller,
    // held live until each broker adopts them. Both go into committed state
    // before the data plane binds -- the controller addresses into the static
    // voter set, the inter-broker address into the registration record -- so
    // neither can be `:0`.
    let (inter_addrs, controller_addrs, inter_listeners, controller_listeners) =
        support::bind_and_hold_ports(VOTERS).await;

    // The client-facing endpoint, in the form the container suites use.
    // `JvmListeners` also allocates a controller port, which this suite does
    // not use: its controller ports come from `bind_and_hold_ports` above,
    // which hands over the live socket instead of releasing it.
    let jvm_listeners: Vec<support::JvmListeners> = (0..VOTERS)
        .map(|_| support::JvmListeners::allocate())
        .collect();

    let voters: Vec<(NodeId, String)> = (0..VOTERS)
        .map(|index| {
            (
                NodeId(u64::try_from(index + 1).expect("small cluster")),
                controller_addrs[index].to_string(),
            )
        })
        .collect();

    let rlmm_bootstrap = inter_addrs[0].to_string();
    let mut starts = Vec::with_capacity(VOTERS);
    let mut pending = Vec::with_capacity(VOTERS);
    for (index, (inter_listener, controller_listener)) in inter_listeners
        .into_iter()
        .zip(controller_listeners)
        .enumerate()
    {
        let data_dir = TempDir::new().expect("broker data dir");
        let addrs = NodeAddrs {
            client: jvm_listeners[index]
                .listen
                .parse()
                .expect("JvmListeners binds a concrete address"),
            client_advertised: jvm_listeners[index].advertised.clone(),
            inter: inter_addrs[index],
            controller: controller_addrs[index],
        };
        let client_addr = addrs.client;
        let config = broker_config(
            data_dir.path(),
            object_dir,
            DisklessNodeSetup {
                index: crate::support::NodeIndex(index),
                addrs,
                rlmm_bootstrap: &rlmm_bootstrap,
                voters: voters.clone(),
            },
        );

        let start_config = config.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(
                start_config,
                Some(controller_listener),
                // The `PLAINTEXT` listener is not handed over: it binds all
                // interfaces, which `bind_and_hold_ports` does not reserve.
                [inter_listener],
            )
            .await
        }));
        pending.push((config, data_dir, client_addr.port()));
    }

    let mut cluster = Vec::with_capacity(VOTERS);
    for (start, (config, data_dir, client_port)) in starts.into_iter().zip(pending) {
        let handle = start
            .await
            .expect("broker start task")
            .expect("three-broker start");
        cluster.push(TestNode {
            handle: Some(handle),
            config,
            client_port,
            _data_dir: data_dir,
        });
    }
    cluster
}

/// The four addresses one broker needs: what each of its two data-plane
/// listeners binds, what the client-facing one advertises, and where its raft
/// controller listens.
#[derive(krabka_macros::FieldDefaults)]
struct NodeAddrs {
    #[default("127.0.0.1:0".parse().expect("loopback listener"))]
    client: SocketAddr,
    #[default("127.0.0.1:0".into())]
    client_advertised: String,
    #[default("127.0.0.1:0".parse().expect("loopback listener"))]
    inter: SocketAddr,
    #[default("127.0.0.1:0".parse().expect("loopback listener"))]
    controller: SocketAddr,
}

/// One broker's config: the two listeners, the static-voter bootstrap, the
/// distinct rack the WAL placement policy requires, the shared object store,
/// and the topic-backed metadata log the flush index rides on.
#[derive(krabka_macros::FieldDefaults)]
struct DisklessNodeSetup<'a> {
    index: crate::support::NodeIndex,
    addrs: NodeAddrs,
    #[default("127.0.0.1:0")]
    rlmm_bootstrap: &'a str,
    voters: Vec<(NodeId, String)>,
}

fn broker_config(log_dir: &Path, object_dir: &Path, setup: DisklessNodeSetup<'_>) -> BrokerConfig {
    let DisklessNodeSetup {
        index,
        addrs,
        rlmm_bootstrap,
        voters,
    } = setup;
    let node = NodeId(u64::try_from(index.0 + 1).expect("small cluster"));
    let mut config = support::addressed_node_config(
        log_dir,
        support::AddressedNodeSetup {
            node,
            client: addrs.inter,
            controller: addrs.controller,
        },
    );
    config.directory_id = uuid::Uuid::from_u128(u128::from(node.0));
    // `listen_addr` and `advertised_listener` name the inter-broker endpoint.
    // The broker self-registers that pair before it binds the data plane, and
    // `Metadata` still answers a client on `PLAINTEXT` with the `PLAINTEXT`
    // endpoint, which is the one that carries `host.docker.internal`.
    config.controller_quorum_voters = voters;
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.auto_join = false;
    config.bootstrap_servers.clear();
    config.audit_enabled = false;
    config.default_min_insync_replicas = 1;

    config.listeners = vec![
        ListenerSpec {
            name: CLIENT_LISTENER.to_owned(),
            bind_addr: addrs.client,
            advertised: addrs.client_advertised.clone(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: krabka_broker::SslPrincipalMapper::default(),
        },
        crate::support::listeners::listener(
            INTER_BROKER_LISTENER,
            addrs.inter,
            ListenerProtocol::SaslPlaintext,
        ),
    ];
    support::diskless::configure_authentication(
        &mut config,
        support::diskless::DisklessAuthenticationSetup {
            listener: INTER_BROKER_LISTENER,
            identity: support::diskless::DisklessIdentitySetup {
                index,
                node,
                voters: support::diskless::WalVoterCount(VOTERS),
                password: PASSWORD,
            },
            ..Default::default()
        },
    );
    config.diskless_wal_flush_interval = krabka_units::millis(100);
    config.diskless_wal_index_projection_timeout = krabka_units::secs(10);
    config.diskless_wal_trim_safety_lag = 1_024;
    config.heartbeat_interval = krabka_units::millis(250);
    config.heartbeat_timeout = krabka_units::secs(2);
    config.liveness_tick_interval = krabka_units::millis(100);
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: object_dir.to_path_buf(),
    });
    // The flush index is a Kafka topic. One partition replicated across all
    // three brokers keeps it cheap and survives the leader loss this schedule
    // injects. Every broker bootstraps that client against broker 1's
    // inter-broker endpoint, because the client authenticates as this node.
    config.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: rlmm_bootstrap.to_owned(),
        num_partitions: 1,
        replication: i32::try_from(VOTERS).expect("small cluster"),
        snapshot_interval: krabka_units::hours(1),
        snapshot_dir: PathBuf::new(), // derived from log_dir
        security: None,
        ..KafkaRlmmConfig::default()
    });
    config
}

/// Wait until every broker's metadata image lists all `VOTERS` brokers.
/// `CreateTopics` reads that broker set to place the replica.
pub(crate) async fn await_brokers_registered(cluster: &[TestNode]) {
    for node in cluster {
        node.handle().wait_until_brokers_registered(VOTERS).await;
    }
}

/// Create the suite's RF=1 diskless topic through the real `CreateTopics`
/// handler.
///
/// Going through the handler is the point. `krabka.diskless` has to survive
/// `validate_topic_config_map`, reach `V1TopicConfig` in the metadata log, and
/// come back out of the image on every broker's reconcile pass.
pub(crate) async fn create_diskless_topic(bootstrap: &str) {
    let client = connect_owned(bootstrap, "diskless-jepsen-admin", "admin client").await;
    let response = client
        .send(crate::support::topics::diskless_topic_request(TOPIC, 1))
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "create diskless RF=1 topic failed: {response:?}"
    );
    client.close();
}

/// Wait until every broker sees the single-replica placement and has its own
/// diskless index projection and object flusher running. A flush that races
/// the projection has no index to publish into.
pub(crate) async fn await_topic_placement(cluster: &[TestNode]) {
    for node in cluster {
        node.handle()
            .wait_for_image(|image| {
                image.partition(TOPIC, 0).is_some_and(|partition| {
                    partition.replicas.len() == 1 && partition.isr.len() == 1
                })
            })
            .await;
        node.handle().wait_until_diskless_flusher_ready().await;
    }
}

/// The controller leader, once all three brokers name the same one.
pub(crate) async fn converged_controller_leader(cluster: &[TestNode]) -> NodeId {
    let leader = cluster[0].handle().wait_until_controller_leader().await;
    for node in &cluster[1..] {
        let observed = node.handle().wait_until_controller_leader().await;
        assert!(
            observed == leader,
            "controller leader did not converge: {leader} vs {observed}"
        );
    }
    leader
}

/// The index of the broker whose node id is `node`.
pub(crate) fn index_of(cluster: &[TestNode], node: NodeId) -> usize {
    cluster
        .iter()
        .position(|item| item.config.node_id == node)
        .expect("node id names a broker in this cluster")
}

/// Rewrite the partition record so that `leader` is its sole replica, its
/// leader and its whole ISR, then wait until every broker in `live` sees it.
///
/// The schedule needs one exact broker to own the classic replica, both
/// before the crash and after the promotion, and `CreateTopics` does not let a
/// caller choose which. Submitting the record directly is the narrowest way to
/// get there; the epochs are bumped the way the controller bumps them, so the
/// partition runtimes install it as an ordinary leader change.
pub(crate) async fn force_partition_owner(
    cluster: &[TestNode],
    live: &[usize],
    submitter: usize,
    leader: NodeId,
) {
    let current = cluster[submitter]
        .handle()
        .partition_record_for_test(TOPIC, 0)
        .expect("diskless partition record");
    if current.leader != leader || current.replicas != [leader] {
        let mut forced = current;
        forced.leader = leader;
        forced.replicas = vec![leader];
        forced.isr = vec![leader];
        forced.adding_replicas.clear();
        forced.removing_replicas.clear();
        forced.directories = vec![uuid::Uuid::nil()];
        forced.leader_epoch = krabka_metadata::LeaderEpoch(forced.leader_epoch.0 + 1);
        forced.partition_epoch += 1;
        cluster[submitter]
            .handle()
            .submit_metadata_record_for_test(MetadataRecord::V1Partition(forced))
            .await
            .expect("assign the sole classic replica");
    }
    for &index in live {
        cluster[index]
            .handle()
            .wait_for_image(|image| {
                image.partition(TOPIC, 0).is_some_and(|partition| {
                    partition.leader == leader
                        && partition.replicas == [leader]
                        && partition.isr == [leader]
                })
            })
            .await;
    }
}

/// Wait until the accepting broker's WAL shard registry reports the full voter
/// set with every follower already fetching.
///
/// This reads the runtime placement rather than the metadata image, so a
/// produce that follows it cannot race asynchronous placement reconciliation
/// and time out on a high watermark that no follower is positioned to advance.
pub(crate) async fn await_wal_runtime(node: &TestNode, leader: NodeId) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if node
                .handle()
                .diskless_wal_ready_for_test(TOPIC, 0, leader, VOTERS)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("diskless WAL runtime placement did not become ready");
}

/// Require that `owner` holds the only classic replica, and that the other two
/// brokers have removed the partition directory they had before the rewrite.
///
/// Without this the whole schedule proves less than it looks like it does: a
/// survivor that still ran an ordinary follower replicator would hold the
/// batches in its own partition log, and could serve the post-crash readback
/// from there even if WAL hydration adopted nothing.
pub(crate) async fn assert_sole_classic_owner(
    cluster: &[TestNode],
    victim: usize,
    survivors: &[usize],
    owner: NodeId,
) {
    let record = cluster[victim]
        .handle()
        .partition_record_for_test(TOPIC, 0)
        .expect("sole classic owner record");
    assert!(record.replicas == [owner]);
    assert!(
        survivors
            .iter()
            .all(|index| !record.replicas.contains(&cluster[*index].config.node_id)),
        "a WAL survivor unexpectedly remained a classic replica"
    );
    for &index in survivors {
        await_path_absent(&cluster[index].partition_dir()).await;
    }
}

/// Wait until the promoted broker leads the partition locally and has the
/// whole acknowledged prefix both appended and committed.
pub(crate) async fn await_promoted_leader(node: &TestNode, leader: NodeId, end_offset: i64) {
    node.handle()
        .wait_until_local_partition_leader(TOPIC, 0, leader)
        .await;
    node.handle()
        .wait_until_local_log_end_offset(TOPIC, 0, end_offset)
        .await;
    node.handle()
        .wait_until_high_watermark(TOPIC, 0, end_offset)
        .await;
}

/// Shut every live broker down, so the tempdirs are not removed underneath a
/// running writer.
pub(crate) async fn shutdown(cluster: Vec<TestNode>) {
    for node in cluster {
        if let Some(handle) = node.handle {
            handle.shutdown().await;
        }
    }
}

async fn await_path_absent(path: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("stale classic partition remained at {}", path.display()));
}
