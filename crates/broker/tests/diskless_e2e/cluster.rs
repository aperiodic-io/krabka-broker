//! The three-broker cluster every case in this suite runs on: its boot, the
//! readiness waits that gate the first produce, and the crash-and-restart the
//! fault-injection cases need.
//!
//! The boot is specific enough to sit apart from the cases that drive it. A
//! diskless quorum needs three things a plain `start_n_node` cluster does not
//! give it. Every broker needs a **distinct rack**, because
//! `wal::quorum::placement` refuses to weaken the AZ-loss failure budget and
//! returns a short voter list when two candidates share one. Every broker
//! needs the **same object store**, because a flush written by one broker is
//! read back by another. Every broker needs a **topic-backed metadata log**,
//! because the WAL flush index is a Kafka topic the whole cluster consumes.
//! And every broker needs an **authenticated inter-broker listener**, because
//! a WAL follower's Fetch is authorized against the caller's principal: the
//! leader resolves it to a node id and serves the shard only to a voter that
//! is who it claims to be, so an anonymous follower is refused and a
//! plaintext cluster would never form a quorum at all.

use std::{net::SocketAddr, path::Path, time::Duration};

use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, NodeId,
    RemoteStorageBackend, RlmmKind,
};
use krabka_security::ListenerProtocol;
use tempfile::TempDir;

use crate::{CLIENT_PRINCIPAL, PASSWORD, VOTERS, support};

/// Name of the one data-plane listener every broker binds. It carries both the
/// suite's client traffic and the inter-broker traffic, including the
/// authenticated WAL follower fetches.
const LISTENER: &str = "SASL_PLAINTEXT";

/// Three booted brokers, the configs they booted from, and the directories
/// that must outlive them.
///
/// A handle is `None` while its broker is down. The configs stay because a
/// restart re-binds the same client and controller ports: a fresh ephemeral
/// port would leave the surviving peers dialing an address nobody answers.
pub(crate) struct DisklessCluster {
    brokers: Vec<Option<BrokerHandle>>,
    configs: Vec<BrokerConfig>,
    log_dirs: Vec<TempDir>,
    /// The object store all three brokers flush into and read back from.
    _remote_dir: TempDir,
}

impl DisklessCluster {
    /// The live handle for the broker whose node id is `node`, if it is up.
    pub(crate) fn handle_for_node(&self, node: NodeId) -> Option<&BrokerHandle> {
        self.brokers[Self::index_of(node)].as_ref()
    }

    /// Wait on the selected surviving voter for the departure's promoted leader.
    pub(crate) async fn promoted_leader(&self, survivor: NodeId, former: NodeId) -> NodeId {
        let broker = self.handle_for_node(survivor).expect("the survivor is up");
        broker
            .wait_until_partition_leader_changed(crate::TOPIC, 0, former)
            .await;
        broker
            .partition_leader_for_test(crate::TOPIC, 0)
            .map(NodeId)
            .expect("a promoted leader")
    }

    /// The `host:port` a client bootstraps against to talk to `node`. This
    /// reads the config rather than the handle, so it answers for a broker
    /// that is currently down as well as one that is up.
    pub(crate) fn bootstrap_for_node(&self, node: NodeId) -> String {
        self.configs[Self::index_of(node)].listen_addr.to_string()
    }

    /// The `log.dir` root of `node`. [`crate::voter_dir`] walks down from here
    /// into the `__diskless_wal_quorum` tree.
    pub(crate) fn log_dir_for_node(&self, node: NodeId) -> &Path {
        self.log_dirs[Self::index_of(node)].path()
    }

    /// Every node id in the cluster, up or down.
    pub(crate) fn node_ids(&self) -> Vec<NodeId> {
        (0..self.configs.len())
            .map(|index| NodeId(u64::try_from(index + 1).expect("small cluster")))
            .collect()
    }

    /// Wait until every live broker's metadata image lists all `VOTERS`
    /// brokers, and until each one's diskless index projection and object
    /// flusher have started. Both gates matter before the first produce:
    /// `CreateTopics` reads the broker set to place replicas, and a flush that
    /// races the projection has no index to publish into.
    pub(crate) async fn await_ready(&self) {
        for broker in self.brokers.iter().flatten() {
            broker.wait_until_brokers_registered(VOTERS).await;
        }
        for broker in self.brokers.iter().flatten() {
            broker.wait_until_diskless_flusher_ready().await;
        }
    }

    /// Stop `node` the way a power cut would: no controlled shutdown, no
    /// leadership handover, no final flush.
    pub(crate) async fn crash(&mut self, node: NodeId) {
        let index = Self::index_of(node);
        let broker = self.brokers[index]
            .take()
            .unwrap_or_else(|| panic!("broker {} is already down", node.0));
        broker.crash_for_test().await;
    }

    /// Bring `node` back up on the addresses it just vacated, so the surviving
    /// peers reconnect to the endpoint they already hold.
    pub(crate) async fn restart(&mut self, node: NodeId) {
        let index = Self::index_of(node);
        assert2::assert!(
            self.brokers[index].is_none(),
            "broker {} is still running",
            node.0
        );
        // The raft log this broker left behind already encodes the cluster's
        // membership, so it comes back in `Rejoin` rather than replaying the
        // cold-boot bootstrap it started from.
        let mut config = self.configs[index].clone();
        config.bootstrap_mode = BootstrapMode::Rejoin;
        self.brokers[index] = Some(
            support::start_reusing_addrs(&config, &format!("restart broker {}", node.0)).await,
        );
    }

    /// Shut every live broker down. Cases call this at the end so the
    /// tempdirs are not removed underneath a running writer.
    pub(crate) async fn shutdown(mut self) {
        for broker in &mut self.brokers {
            if let Some(broker) = broker.take() {
                broker.shutdown().await;
            }
        }
    }

    fn index_of(node: NodeId) -> usize {
        usize::try_from(node.0)
            .expect("node id fits usize")
            .checked_sub(1)
            .expect("node ids are 1-based")
    }
}

/// Boot `VOTERS` brokers that can run a diskless quorum between them.
///
/// `customize` runs on each broker's config after the shared diskless wiring
/// and before the broker starts, so a case can set its own flush cadence and
/// trim policy without restating the rest.
pub(crate) async fn start_diskless_cluster(
    customize: impl Fn(&mut BrokerConfig),
) -> DisklessCluster {
    support::init_tracing();

    // Concrete ports up front, held live until each broker adopts them. The
    // advertised client address is registered into the controller image before
    // the data listener binds, and the controller addresses go into the static
    // voter set, so neither can be `:0`.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(VOTERS).await;
    let log_dirs: Vec<TempDir> = (0..VOTERS)
        .map(|_| TempDir::new().expect("broker log dir"))
        .collect();
    let remote_dir = TempDir::new().expect("shared object store dir");

    let voters: Vec<(u64, SocketAddr)> = (0..VOTERS)
        .map(|index| {
            (
                u64::try_from(index + 1).expect("small cluster"),
                controller_addrs[index],
            )
        })
        .collect();

    let topology = support::RoleTopology::new(&client_addrs, &controller_addrs, &voters);

    let configs: Vec<BrokerConfig> = (0..VOTERS)
        .map(|index| {
            let mut config = broker_config(
                log_dirs[index].path(),
                remote_dir.path(),
                topology.node_setup(crate::support::ClusterBootstrapSetup {
                    index: crate::support::NodeIndex(index),
                    ..Default::default()
                }),
            );
            customize(&mut config);
            config
        })
        .collect();

    // Static cold boot: all three start concurrently. A sequential start would
    // deadlock, because the first broker cannot elect a leader alone.
    let mut starts = Vec::with_capacity(VOTERS);
    for ((config, client), controller) in configs
        .iter()
        .cloned()
        .zip(client_listeners)
        .zip(controller_listeners)
    {
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(config, Some(controller), Some(client)).await
        }));
    }
    let mut brokers = Vec::with_capacity(VOTERS);
    for start in starts {
        brokers.push(Some(
            start
                .await
                .expect("broker start task")
                .expect("broker start"),
        ));
    }

    DisklessCluster {
        brokers,
        configs,
        log_dirs,
        _remote_dir: remote_dir,
    }
}

/// One broker's config: the static-voter bootstrap, the distinct rack the WAL
/// placement policy requires, the shared object store, and the topic-backed
/// metadata log the diskless flush index rides on.
fn broker_config(
    log_dir: &Path,
    remote_dir: &Path,
    setup: crate::support::ClusterNodeSetup<'_>,
) -> BrokerConfig {
    let index = setup.index.0;
    let client = setup.client_addrs[index];
    let rlmm_bootstrap = setup.client_addrs[0].to_string();
    let node = NodeId(u64::try_from(index + 1).expect("small cluster"));
    let mut config = support::broker_config(log_dir, setup);
    config.directory_id = uuid::Uuid::from_u128(u128::try_from(index + 1).expect("small cluster"));
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.auto_join = false;
    config.bootstrap_servers = vec![];
    // One SASL PLAIN data listener carrying both client and inter-broker
    // traffic. Broker `i` dials its peers as `broker-<i+1>`, which
    // `conventional_node_id` reads straight back as a node id, so the WAL
    // leader can tie an incoming shard fetch to a voter. Every broker holds
    // every principal, because any of them can end up leading the shard and
    // having to authenticate the other two.
    config.listeners = vec![crate::support::listeners::listener(
        LISTENER,
        client,
        ListenerProtocol::SaslPlaintext,
    )];
    support::diskless::configure_authentication(
        &mut config,
        support::diskless::DisklessAuthenticationSetup {
            listener: LISTENER,
            identity: support::diskless::DisklessIdentitySetup {
                index: crate::support::NodeIndex(index),
                node,
                voters: support::diskless::WalVoterCount(VOTERS),
                password: PASSWORD,
            },
            additional_credentials: vec![(CLIENT_PRINCIPAL.to_owned(), PASSWORD.to_owned())],
        },
    );
    // One shared object store for all three brokers: a flush written by the
    // leader has to be readable by whichever broker serves the cold read.
    config.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.to_path_buf(),
    });
    // The diskless WAL flush index is a Kafka topic, so it needs the
    // topic-backed metadata log. One partition replicated across all three
    // brokers keeps the index cheap and survives the leader loss the failover
    // case injects. Every broker bootstraps that client against broker 1.
    config.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: rlmm_bootstrap,
        num_partitions: 1,
        replication: i32::try_from(VOTERS).expect("small cluster"),
        snapshot_interval: krabka_units::hours(1),
        snapshot_dir: std::path::PathBuf::new(), // derived from log_dir
        security: None,
        ..KafkaRlmmConfig::default()
    });
    config
}

/// A bounded poll for a condition the cluster reaches asynchronously. Every
/// wait in this suite that has no watch channel behind it goes through here,
/// so a timeout names what it was waiting for instead of hanging the run.
///
/// The predicate returns a future because some of the state this suite waits
/// on -- the projected flush frontier, for one -- sits behind an async lock.
pub(crate) async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if ready().await {
            return;
        }
        assert2::assert!(
            std::time::Instant::now() <= deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Flush the WAL at a chosen cadence and trim directly to its committed frontier.
pub(crate) async fn start_flushing_cluster(interval: krabka_units::Time) -> DisklessCluster {
    start_diskless_cluster(|config| {
        config.diskless_wal_flush_interval = interval;
        config.diskless_wal_trim_safety_lag = 0;
    })
    .await
}
