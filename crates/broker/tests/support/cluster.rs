//! Static-voter bootstrap of an `n`-broker cluster.
//!
//! `start_n_node_with` is the one helper that boots a whole cluster: it
//! reserves and holds a client and a controller port per broker, builds each
//! node's `BrokerConfig` for the shared static voter set, starts every broker
//! concurrently, and waits for a leader. It is long enough, and self-contained
//! enough, to sit in its own file, with the per-broker config builder it is the
//! only caller of.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerError, BrokerHandle, NodeId};
use tempfile::TempDir;

/// Set the one-based broker and node identities without changing cluster policy.
///
/// # Panics
/// Panics if the one-based index cannot be represented as a broker or node ID.
#[derive(Clone, Copy, Default)]
pub struct NodeIndex(pub usize);

pub fn node_config(index: NodeIndex, log_dir: &std::path::Path) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.broker_id = i32::try_from(index.0 + 1).unwrap();
    cfg.node_id = NodeId(u64::try_from(index.0 + 1).unwrap());
    cfg
}

/// Advertise each controller's supplied endpoint in the original voter order.
pub fn controller_voters(voters: &[(u64, SocketAddr)]) -> Vec<(NodeId, String)> {
    voters
        .iter()
        .map(|(id, addr)| (NodeId(*id), addr.to_string()))
        .collect()
}

/// Pair held data and controller listeners in their original node order.
pub fn listener_pairs(
    clients: Vec<tokio::net::TcpListener>,
    controllers: Vec<tokio::net::TcpListener>,
) -> impl Iterator<Item = (usize, (tokio::net::TcpListener, tokio::net::TcpListener))> {
    clients.into_iter().zip(controllers).enumerate()
}

/// Keep broker start failures and task panics distinct while joining starts in order.
///
/// # Errors
/// Returns the broker's startup error, or a Startup error when its task panics.
pub async fn await_broker_start(
    start: tokio::task::JoinHandle<Result<BrokerHandle, BrokerError>>,
) -> Result<BrokerHandle, BrokerError> {
    start
        .await
        .map_err(|error| BrokerError::Startup(format!("broker start task panicked: {error}")))?
}

/// Build a `BrokerConfig` for broker `i` (0-indexed) in a static `n`-voter
/// cluster. Every broker boots in `Bootstrap` mode with the *same* configured
/// `controller_quorum_voters` set, so each node seeds the full voter set and
/// elects among the configured peers over the real KIP-595 wire. There is no
/// auto-join, because KIP-853 dynamic reconfiguration is a separate work stream.
#[derive(krabka_macros::FieldDefaults)]
struct StaticVoterSetup {
    node: crate::support::AddressedNodeSetup,
    voters: Vec<(NodeId, String)>,
}

fn static_voter_broker_config(log_dir: &std::path::Path, setup: StaticVoterSetup) -> BrokerConfig {
    let mut cfg = crate::support::addressed_node_config(log_dir, setup.node);
    // Bind a concrete (pre-bound) client port. The broker self-registers its
    // `advertised_listener` host:port into the controller image *before* it
    // binds its listeners and rewrites a `:0` advertised port to the real one
    // — so a `:0` here would register port 0 and break the inter-broker
    // heartbeat / replication dial. Give it a real port up front.
    // The controller listener must bind the *same* concrete port that this
    // node advertises in the shared voter set, or its peers can't dial it.
    cfg.directory_id = uuid::Uuid::from_u128(u128::from(cfg.node_id.0));
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    cfg.controller_quorum_voters = setup.voters;
    cfg.auto_join = false;
    cfg.bootstrap_servers = vec![];
    cfg
}

/// Like [`start_n_node`], but it invokes `customize(i, &mut cfg)` on each
/// broker's `BrokerConfig` before start. A test can then add per-broker
/// overrides such as `rack` or `replica_selector` and still keep the race-free
/// held-listener bootstrap. There is no `bind_and_drop_ports` TOCTOU window in
/// which a concurrently running test can steal a just-released port
/// (`AddrInUse`).
pub async fn start_n_node_with(
    n: u64,
    mut customize: impl FnMut(usize, &mut BrokerConfig),
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, BrokerError> {
    super::init_tracing();

    let n_usize = usize::try_from(n).unwrap();

    // Reserve concrete client + controller ports for every broker by binding
    // ephemeral loopback listeners and *holding them live* until each broker
    // adopts its pair via `Broker::start_with_listeners`. The ports must be
    // concrete up front: each controller addr goes into the shared static
    // voter set so peers can dial it, and each broker self-registers its
    // advertised client `host:port` into the controller image *before* it
    // binds its data-plane listener — a `:0` there would register port 0 and
    // break the inter-broker heartbeat / replication dial.
    //
    // Unlike the bind-and-drop trick (`bind_and_drop_ports`), these sockets are
    // never dropped before the broker re-binds them, so there is no TOCTOU
    // window for a concurrently-running test to steal a just-released port —
    // the `AddrInUse` flake under parallel `cargo test` / `cargo llvm-cov`.
    let mut client_listeners = Vec::with_capacity(n_usize);
    let mut controller_listeners = Vec::with_capacity(n_usize);
    for _ in 0..n_usize {
        client_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
        controller_listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let client_addrs: Vec<SocketAddr> = client_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;
    let controller_addrs: Vec<SocketAddr> = controller_listeners
        .iter()
        .map(tokio::net::TcpListener::local_addr)
        .collect::<std::io::Result<_>>()?;

    // The shared static voter set every node is configured with.
    let voters: Vec<(u64, SocketAddr)> = (0..n_usize)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Start all n brokers in Bootstrap mode with the same voter set,
    // *concurrently*. `Broker::start*` blocks until the cold-boot controller
    // sees a committed leader (step 2: it waits on `watch_leader` before
    // submitting its self-registration), and a leader can only be elected once
    // a majority of the static voter set is up and dialable. So a sequential
    // `start().await` on the first broker would deadlock — it can never elect
    // alone. Spawn every broker's `start` and join them.
    let mut starts = Vec::with_capacity(n_usize);
    let mut metas: Vec<(BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (i, (data_listener, controller_listener)) in
        listener_pairs(client_listeners, controller_listeners)
    {
        let dir = TempDir::new().unwrap();
        // Size the coordinator topics for the cluster, as Kafka's defaults
        // size them for a production one: replication factor `min(n, 3)`.
        // `customize` runs after, so a suite can still override them.
        let mut cfg = static_voter_broker_config(
            dir.path(),
            StaticVoterSetup {
                node: crate::support::AddressedNodeSetup {
                    node: NodeId(u64::try_from((i) + 1).expect("one-based node id")),
                    client: client_addrs[i],
                    controller: controller_addrs[i],
                },
                voters: crate::support::controller_voters(&voters),
            },
        )
        .with_internal_topics_for(n_usize);
        customize(i, &mut cfg);
        let cfg_for_spawn = cfg.clone();
        starts.push(tokio::spawn(async move {
            Broker::start_with_listeners(
                cfg_for_spawn,
                Some(controller_listener),
                Some(data_listener),
            )
            .await
        }));
        metas.push((cfg, dir));
    }

    let mut out: Vec<(BrokerHandle, BrokerConfig, TempDir)> = Vec::with_capacity(n_usize);
    for (handle, (cfg, dir)) in starts.into_iter().zip(metas) {
        let broker = await_broker_start(handle).await?;
        out.push((broker, cfg, dir));
    }

    // Wait (event-driven, bounded) for the static set to elect a leader. We await
    // the first broker's controller leader watch channel rather than the panicking
    // `wait_until_controller_leader()` helper, because a timeout here must return
    // `Err` so `start_n_node_with_retry` can retry (a panic would not be retried).
    let mut leader_rx = out[0].0.watch_leader_for_test();
    let elected = tokio::time::timeout(
        Duration::from_secs(30),
        leader_rx.wait_for(|l| matches!(l, Some(id) if *id != 0)),
    )
    .await;
    let timed_out = match &elected {
        Err(_elapsed) => true,      // tokio::time::timeout fired
        Ok(Err(_recv_err)) => true, // watch channel closed unexpectedly
        Ok(Ok(_)) => false,
    };
    if timed_out {
        let counts: Vec<usize> = out
            .iter()
            .map(|(h, _, _)| h.voter_count_for_test())
            .collect();
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n_usize} voters within 30s \
             (voter counts={counts:?})"
        )));
    }
    assert!(
        out.iter()
            .any(|(h, _, _)| h.voter_count_for_test() >= n_usize),
        "leader elected but voter set not committed to {n_usize}"
    );

    Ok(out)
}

/// Start the static cluster before connecting its original loopback admin client.
///
/// # Panics
/// Panics if cluster startup or the client connection fails.
pub async fn start_n_node_client(
    n: u64,
    client_id: &str,
) -> (
    Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    krabka_client_core::Client,
) {
    let cluster = crate::support::start_n_node(n).await.expect("start_n_node");
    let client = crate::support::client::connect_owned(
        format!("127.0.0.1:{}", cluster[0].1.listen_addr.port()),
        client_id,
        "client build",
    )
    .await;
    (cluster, client)
}

/// Three brokers whose internal-topic ISR stays fixed while one broker is stopped.
///
/// # Panics
/// Panics if the cluster cannot start or register its brokers.
pub async fn fixed_internal_isr_cluster(
    customize: impl Fn(&mut krabka_broker::BrokerConfig),
) -> Vec<(
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerConfig,
    tempfile::TempDir,
)> {
    let cluster = start_n_node_with(3, |_, config| {
        *config = config.clone().with_internal_topics_for(3);
        config.share_coordinator.state_topic_num_partitions = 1;
        customize(config);
        config.replica_lag_time_max = krabka_units::secs(30);
        config.isr_scan_interval = krabka_units::hours(1);
        config.heartbeat_timeout = krabka_units::minutes(10);
    })
    .await
    .expect("start the cluster");
    crate::support::wait_for_all_brokers_registered(&cluster, 3).await;
    cluster
}
