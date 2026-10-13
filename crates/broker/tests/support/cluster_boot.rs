//! Boot wrappers and readiness waits for an `n`-broker cluster.
//!
//! [`start_n_node`] and its retrying twin drive the static-voter bootstrap in
//! [`super::cluster`], [`start_reusing_addrs`] brings one node back up on the
//! addresses it has just vacated, and [`wait_for_all_brokers_registered`]
//! blocks until every controller image has seen the whole cluster.
//! [`broker_config`] builds one node's config for the suites that drive
//! membership changes themselves.

use {
    super::cluster::start_n_node_with,
    crate::support::{NodeIndex, controller_voters},
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId},
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// Shut brokers down in vector order, retaining the original tuple destructuring.
pub async fn shutdown_cluster(cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>) {
    for (handle, _, _) in cluster {
        handle.shutdown().await;
    }
}

// The functions below are only meaningful on non-Windows targets because
// openraft's debug_assert! races on the hosted Windows task scheduler.
// Individual test files gate their use with ``.

/// Build a `BrokerConfig` for broker `i` (0-indexed) in an `n`-broker
/// cluster from the supplied ephemeral port lists and static voter map.
/// This is the *static-voter* bootstrap-then-join helper. It exists for tests
/// such as `elect_leaders` that drive `add_learner` and `change_membership`
/// manually and need extra config overrides per broker. `start_n_node`'s
/// auto-join path cannot support that flow.
const DEFAULT_ENDPOINTS: [SocketAddr; 1] = [SocketAddr::V4(std::net::SocketAddrV4::new(
    std::net::Ipv4Addr::LOCALHOST,
    0,
))];

#[derive(krabka_macros::FieldDefaults)]
pub struct ClusterNodeSetup<'a> {
    pub index: NodeIndex,
    #[default(&DEFAULT_ENDPOINTS)]
    pub client_addrs: &'a [SocketAddr],
    #[default(&DEFAULT_ENDPOINTS)]
    pub controller_addrs: &'a [SocketAddr],
    pub voters: Vec<(NodeId, String)>,
    #[default(BootstrapMode::Bootstrap)]
    pub mode: BootstrapMode,
}

#[bon::builder]
pub fn broker_config(
    log_dir: &std::path::Path,
    #[builder(into, default)] index: NodeIndex,
    #[builder(default = & DEFAULT_ENDPOINTS)] client_addrs: &'_ [SocketAddr],
    #[builder(default = & DEFAULT_ENDPOINTS)] controller_addrs: &'_ [SocketAddr],
    #[builder(default)] voters: Vec<(NodeId, String)>,
    #[builder(default = BootstrapMode :: Bootstrap)] mode: BootstrapMode,
) -> BrokerConfig {
    let setup = ClusterNodeSetup {
        index,
        client_addrs,
        controller_addrs,
        voters,
        mode,
    };
    let mut config = addressed_node_config()
        .log_dir(log_dir)
        .node(u64::try_from(setup.index.0 + 1).expect("one-based node id"))
        .client(setup.client_addrs[setup.index.0])
        .controller(setup.controller_addrs[setup.index.0])
        .call();
    config.controller_quorum_voters = setup.voters;
    config.bootstrap_mode = setup.mode;
    config
}

/// Listener addresses and node identity common to formatted bootstrap nodes.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct AddressedNodeSetup {
    #[default(NodeId(1))]
    pub node: NodeId,
    #[default(DEFAULT_ENDPOINTS[0])]
    pub client: SocketAddr,
    #[default(DEFAULT_ENDPOINTS[0])]
    pub controller: SocketAddr,
}

#[bon::builder]
pub fn addressed_node_config(
    log_dir: &std::path::Path,
    #[builder(into, default = NodeId (1))] node: NodeId,
    #[builder(default = DEFAULT_ENDPOINTS [0])] client: SocketAddr,
    #[builder(default = DEFAULT_ENDPOINTS [0])] controller: SocketAddr,
) -> BrokerConfig {
    let setup = AddressedNodeSetup {
        node,
        client,
        controller,
    };
    let mut config = BrokerConfig::for_tests(log_dir.to_path_buf());
    config.broker_id = i32::try_from(setup.node.0).expect("node id");
    config.node_id = setup.node;
    config.listen_addr = setup.client;
    config.advertised_listener = setup.client.to_string();
    config.controller_listen_addr = setup.controller;
    config
}

/// Address and voter configuration kept alive independently of the adopted listeners.
pub struct RoleEndpoints {
    clients: Vec<SocketAddr>,
    controllers: Vec<SocketAddr>,
    voters: Vec<(u64, SocketAddr)>,
}

impl RoleEndpoints {
    pub fn topology(&self) -> RoleTopology<'_> {
        RoleTopology::new(&self.clients, &self.controllers, &self.voters)
    }
}

pub async fn single_controller_endpoints(
    nodes: usize,
) -> (
    RoleEndpoints,
    std::vec::IntoIter<tokio::net::TcpListener>,
    std::vec::IntoIter<tokio::net::TcpListener>,
) {
    let (clients, controllers, client_listeners, controller_listeners) =
        super::bind_and_hold_ports(nodes).await;
    let voters = vec![(1, controllers[0])];
    (
        RoleEndpoints {
            clients,
            controllers,
            voters,
        },
        client_listeners.into_iter(),
        controller_listeners.into_iter(),
    )
}

/// The held endpoints and voter map shared by the nodes of a role-separated cluster.
pub struct RoleTopology<'a> {
    clients: &'a [SocketAddr],
    controllers: &'a [SocketAddr],
    voters: &'a [(u64, SocketAddr)],
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct RoleNodeSetup {
    pub index: NodeIndex,
    #[default(BootstrapMode::Bootstrap)]
    pub mode: BootstrapMode,
    #[default(krabka_broker::config::NodeRole::Broker)]
    pub role: krabka_broker::config::NodeRole,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ClusterBootstrapSetup {
    pub index: NodeIndex,
    #[default(BootstrapMode::Bootstrap)]
    pub mode: BootstrapMode,
}

impl<'a> RoleTopology<'a> {
    pub fn new(
        clients: &'a [SocketAddr],
        controllers: &'a [SocketAddr],
        voters: &'a [(u64, SocketAddr)],
    ) -> Self {
        Self {
            clients,
            controllers,
            voters,
        }
    }

    /// Resolve one node's held addresses and voter endpoints from this topology.
    pub fn node_setup(&self, setup: ClusterBootstrapSetup) -> ClusterNodeSetup<'a> {
        ClusterNodeSetup {
            index: setup.index,
            client_addrs: self.clients,
            controller_addrs: self.controllers,
            voters: controller_voters(self.voters),
            mode: setup.mode,
        }
    }

    /// Apply exactly one role after the ordinary static-voter configuration.
    ///
    /// # Panics
    /// Panics if the node index or checked broker id is out of range.
    pub fn config(&self, log_dir: &std::path::Path, setup: RoleNodeSetup) -> BrokerConfig {
        let node = self.node_setup(ClusterBootstrapSetup {
            index: setup.index,
            mode: setup.mode,
        });
        let mut config = broker_config()
            .log_dir(log_dir)
            .index(node.index.0)
            .client_addrs(node.client_addrs)
            .controller_addrs(node.controller_addrs)
            .voters(node.voters)
            .mode(node.mode)
            .call();
        config.roles = vec![setup.role];
        config
    }
}

/// Consume the held controller and data sockets in the same order before startup.
///
/// # Panics
/// Panics if either listener iterator is exhausted or startup fails.
pub async fn start_held_node(
    config: BrokerConfig,
    controllers: &mut std::vec::IntoIter<tokio::net::TcpListener>,
    clients: &mut std::vec::IntoIter<tokio::net::TcpListener>,
    context: &str,
) -> BrokerHandle {
    // Keep node startup off the cluster fixture's stack as LogConfig grows.
    Box::pin(Broker::start_with_listeners(
        config,
        Some(controllers.next().unwrap()),
        Some(clients.next().unwrap()),
    ))
    .await
    .expect(context)
}

/// Boot an `n`-broker cluster with ephemeral ports and short raft timings
/// through **static multi-voter bootstrap** (KIP-595 static-quorum bootstrap):
///
/// * All `n` brokers boot in `Bootstrap` mode (`auto_join = false`), each
///   configured with the *same* `controller_quorum_voters` = the full
///   `[(1, ctrl_addr_1), …, (n, ctrl_addr_n)]` set.
/// * Each node seeds the full static voter set, and the nodes elect a leader
///   among themselves over the real KIP-595 wire. There is no `AddRaftVoter`
///   and no auto-join.
///
/// Blocks until a leader emerges and reports the full `n`-voter committed set.
/// Returns `(handle, config, tempdir)` triples in spawn order.
/// `cluster[0]` is `broker_id` 1.
pub async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    start_n_node_with(n, |_, _| {}).await
}

/// Retry `start_n_node` up to 3 times. Short raft timings sometimes
/// split-vote on slow runners. A fresh tempdir and port set on retry
/// clears the openraft state and usually succeeds within 2 attempts.
pub async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    start_n_node_customized_with_retry(n, |_, _| {}, "cluster").await
}

/// Retry startup with the same per-node customization on each fresh cluster.
pub async fn start_n_node_customized_with_retry(
    n: u64,
    mut customize: impl FnMut(usize, &mut BrokerConfig),
    label: &str,
) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node_with(n, &mut customize).await {
            Ok(cluster) => return cluster,
            Err(error) => {
                tracing::warn!(attempt, %error, "{label} start failed; retrying");
                last_err = Some(error);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("{label} start failed after 3 attempts; last error: {last_err:?}");
}

/// Boot a static-voter cluster and await its registration on every broker.
///
/// # Panics
/// Panics if the cluster cannot start or its broker count cannot fit `usize`.
pub async fn registered_cluster(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let cluster = start_n_node_with_retry(n).await;
    wait_for_all_brokers_registered(&cluster, usize::try_from(n).expect("broker count")).await;
    cluster
}

/// Start a broker on listen addresses another broker has just vacated.
///
/// [`BrokerHandle::shutdown`] awaits its listener tasks, so the sockets are
/// closed by the time it returns, but the port can still be unbindable for a
/// moment afterwards, and a concurrently-running test binary can win the race
/// for the freed ephemeral port. Both surface as `AddrInUse` on the re-bind.
/// Retry briefly instead of failing the test on a port-reuse race, in the
/// spirit of [`start_n_node_with_retry`].
pub async fn start_reusing_addrs(cfg: &BrokerConfig, what: &str) -> BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let attempt = async {
            // Hold reused ports before startup can initialize the metadata log.
            // A bind failure then leaves a fresh Join log empty for the retry.
            let controller = if cfg.is_controller() && cfg.controller_listen_addr.port() != 0 {
                Some(tokio::net::TcpListener::bind(cfg.controller_listen_addr).await?)
            } else {
                None
            };
            let mut data = Vec::new();
            for spec in cfg.effective_listeners() {
                if spec.bind_addr.port() != 0 {
                    data.push(tokio::net::TcpListener::bind(spec.bind_addr).await?);
                }
            }
            Broker::start_with_listeners(cfg.clone(), controller, data).await
        }
        .await;
        match attempt {
            Ok(handle) => return handle,
            Err(BrokerError::Io(e))
                if e.kind() == std::io::ErrorKind::AddrInUse && Instant::now() < deadline =>
            {
                tracing::warn!(%what, error = %e, "vacated port not yet bindable; retrying");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("{what}: {e:?}"),
        }
    }
}

/// Await every broker's controller image until each one sees `n` brokers
/// registered. Call this before any test that needs the partition's replica
/// set to include all `n` nodes. `CreateTopics` reads `image.brokers()` to pick
/// replicas, and a race here silently degrades to a smaller replica set.
///
/// This helper uses the panicking `wait_until_brokers_registered` awaiter on
/// purpose. Tests call this helper directly, not through the
/// `start_n_node_with_retry` path, so a timeout must fail the test.
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(n).await;
    }
}

/// Selects two non-controller nodes in ascending cluster order.
/// Removing the second leaves the first node's index unchanged.
///
/// # Panics
/// Panics unless exactly two nodes are followers of the elected controller.
pub async fn two_controller_followers(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
) -> (NodeId, usize, usize) {
    let leader = cluster[0].0.wait_until_controller_leader().await;
    let followers: Vec<usize> = (0..cluster.len())
        .filter(|&i| cluster[i].0.node_id() != leader.0)
        .collect();
    assert2::assert!(
        followers.len() == 2,
        "a three-node cluster has two non-controller nodes"
    );
    (leader, followers[0], followers[1])
}

/// Start the first held client/controller pair, releasing spare client sockets first.
///
/// # Panics
/// Panics if either vector is empty or broker startup fails.
pub async fn start_first_held(
    config: BrokerConfig,
    clients: Vec<tokio::net::TcpListener>,
    controllers: Vec<tokio::net::TcpListener>,
    context: &str,
) -> BrokerHandle {
    let data_listener = clients.into_iter().next().unwrap();
    let controller_listener = controllers.into_iter().next().unwrap();
    Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener))
        .await
        .expect(context)
}

/// Wait for this survivor's leader watch to replace the departed node.
pub async fn await_controller_replacement(handle: &BrokerHandle, departed: NodeId, context: &str) {
    let mut leaders = handle.watch_leader_for_test();
    tokio::time::timeout(
        Duration::from_secs(30),
        leaders
            .wait_for(|leader| matches!(leader, Some(id) if *id != NodeId(0) && *id != departed)),
    )
    .await
    .expect(context)
    .expect("leader channel closed");
}
