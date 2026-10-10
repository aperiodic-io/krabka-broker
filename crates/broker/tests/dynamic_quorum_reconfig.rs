//! A KIP-853 dynamic quorum grown from a `--standalone` controller, end to
//! end, as Apache Kafka's `quorum_reconfiguration_test.py` builds and changes
//! it.
//!
//! Every node names the quorum through `controller.quorum.bootstrap.servers`
//! alone, and none through `controller.quorum.voters`. Controller 3001 is
//! formatted with `--standalone` and elects itself. Brokers 1 and 2, which
//! hold no voter set of their own, have to find it through the bootstrap
//! servers, and then reach its controller listener for their registrations and
//! heartbeats through the voter set they read from the log. Controller 3002 is
//! formatted with `--no-initial-controllers` and starts as an observer.
//! `AddRaftVoter` then makes it a voter, and `RemoveRaftVoter` removes 3001, so
//! 3002 leads and 3001 observes. Every step reads the quorum back with
//! `DescribeQuorum` through a broker, which is what `kafka-metadata-quorum
//! describe --status` sends.
//!
//! The nodes are configured the way the broker binary configures them from a
//! formatted directory: the cluster and directory ids come from the
//! `meta.properties` that `krabka-format` wrote, read as the binary reads it,
//! and a fresh directory starts in `Bootstrap` mode.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerHandle, NodeId, config::NodeRole};
use krabka_client_admin::{AdminClient, AdminError, RaftVoterEndpoint};
use tempfile::TempDir;
use tokio::net::TcpListener;

mod support;

/// The cluster id `kafka-storage format` is given in the system test.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node gets to start, and the quorum to reach a described state.
const DEADLINE: Duration = Duration::from_secs(30);

/// Long enough for a controller to fence a broker it does not hear from: the
/// two-second `heartbeat_timeout` of the test config, a one-second liveness
/// tick to publish the decision, and slack.
const FENCING_WINDOW: Duration = Duration::from_secs(4);

/// One node's listeners and log directory.
struct Node {
    id: u64,
    client: TcpListener,
    /// The controller listener, held from the start. `None` for a node whose
    /// port stays closed until it starts, so a dial to it is refused.
    controller: Option<TcpListener>,
    client_addr: SocketAddr,
    controller_addr: SocketAddr,
    dir: TempDir,
}

/// Formats this node's log directory with `krabka-format` and the dynamic
/// quorum flags in `quorum`, in process, as the system test runs
/// `kafka-storage format` before it starts each node.
async fn format(node: &Node, quorum: &[&str]) -> PathBuf {
    let log_dir = node.dir.path().join("log");
    let node_id = node.id.to_string();
    let mut argv = vec![
        "krabka-format",
        "--log-dir",
        log_dir.to_str().expect("utf-8 temp path"),
        "--cluster-id",
        CLUSTER_ID,
        "--node-id",
        &node_id,
    ];
    argv.extend_from_slice(quorum);
    let code = krabka_format::run_from_args(argv).await;
    assert!(
        code == 0,
        "krabka-format exited {code} for node {}",
        node.id
    );
    log_dir
}

/// Starts `node` with `roles` on its formatted `log_dir`, naming the quorum
/// through `bootstrap` alone, and fails the test when it does not finish
/// starting.
async fn start(
    node: Node,
    roles: Vec<NodeRole>,
    log_dir: &Path,
    bootstrap: &[SocketAddr],
) -> (BrokerHandle, TempDir) {
    let meta = krabka_broker::bootstrap::initialize_log_dirs(
        log_dir,
        &[log_dir.to_path_buf()],
        NodeId(node.id),
        None,
    )
    .expect("krabka-format wrote meta.properties");
    let mut config = crate::support::addressed_node_config(
        log_dir,
        crate::support::AddressedNodeSetup {
            node: krabka_broker::NodeId(node.id),
            client: node.client_addr,
            controller: node.controller_addr,
        },
    );
    config.controller_quorum_voters = vec![];
    config.bootstrap_servers = bootstrap.iter().map(ToString::to_string).collect();
    config.roles = roles;
    config.bootstrap_mode = BootstrapMode::Bootstrap;
    config.cluster_id = Some(meta.cluster_id);
    config.directory_id = meta.directory_id;
    let handle = tokio::time::timeout(
        DEADLINE,
        Broker::start_with_listeners(config, node.controller, Some(node.client)),
    )
    .await
    .unwrap_or_else(|_| panic!("node {} did not finish starting in {DEADLINE:?}", node.id))
    .unwrap_or_else(|error| panic!("node {} failed to start: {error}", node.id));
    (handle, node.dir)
}

/// The quorum as `kafka-metadata-quorum describe --status` reads it through a
/// broker: the leader id, the voter ids and the observer ids.
type Described = (i32, BTreeSet<i32>, BTreeSet<i32>);

async fn describe(broker: SocketAddr) -> Result<Described, String> {
    let mut admin = AdminClient::connect(&[broker.to_string()])
        .await
        .map_err(|error| error.to_string())?;
    let quorum = admin
        .describe_metadata_quorum()
        .await
        .map_err(|error| error.to_string())?;
    let ids = |replicas: &[krabka_client_admin::QuorumReplica]| {
        replicas.iter().map(|replica| replica.node_id).collect()
    };
    Ok((
        quorum.leader_id,
        ids(&quorum.voters),
        ids(&quorum.observers),
    ))
}

/// Waits until `DescribeQuorum` through `broker` names `leader`, exactly the
/// `voters` and exactly the `observers`, as the system test's
/// `check_describe_quorum_output` does.
async fn wait_for_quorum(broker: SocketAddr, leader: i32, voters: &[i32], observers: &[i32]) {
    let expected: Described = (
        leader,
        voters.iter().copied().collect(),
        observers.iter().copied().collect(),
    );
    let deadline = Instant::now() + DEADLINE;
    loop {
        let described = describe(broker).await;
        if described.as_ref() == Ok(&expected) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the quorum described through {broker} is {described:?}, not {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Retries `operation` until it succeeds. A voter change is refused while the
/// leader's epoch or an earlier change is uncommitted, or while the new voter
/// has not caught up, and `kafka-metadata-quorum` is simply run again.
async fn until_ok<F, Fut>(what: &str, mut operation: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let deadline = Instant::now() + DEADLINE;
    loop {
        let outcome = operation().await;
        if outcome.is_ok() {
            return;
        }
        assert!(Instant::now() <= deadline, "{what}: {outcome:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The result of one voter change. A retry after an attempt whose response was
/// lost answers `applied`, `DUPLICATE_VOTER` for an add or `VOTER_NOT_FOUND`
/// for a remove, because the change is already in. That counts as done:
/// `wait_for_quorum` then checks the whole voter set.
fn voter_change(result: Result<(), AdminError>, applied: i16) -> Result<(), String> {
    match result {
        Err(AdminError::Broker { code, .. }) if code == applied => Ok(()),
        other => other.map_err(|error| error.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brokers_and_controllers_find_a_standalone_quorum_through_bootstrap_servers() {
    support::init_tracing();
    let (client_addrs, controller_addrs, clients, controllers) =
        support::bind_and_hold_ports(4).await;
    let mut nodes = [3001, 3002, 1, 2]
        .into_iter()
        .zip(clients.into_iter().zip(controllers))
        .zip(client_addrs.into_iter().zip(controller_addrs))
        .map(
            |((id, (client, controller)), (client_addr, controller_addr))| Node {
                id,
                client,
                controller: Some(controller),
                client_addr,
                controller_addr,
                dir: TempDir::new().expect("tempdir"),
            },
        );
    let (first, mut second, broker_one, broker_two) = (
        nodes.next().expect("node 3001"),
        nodes.next().expect("node 3002"),
        nodes.next().expect("node 1"),
        nodes.next().expect("node 2"),
    );
    let (first_controller, second_controller) = (first.controller_addr, second.controller_addr);
    // Every node names both controllers, as the system test does, while 3002
    // is not running yet: its port refuses, and it is in its own list.
    drop(second.controller.take());
    let bootstrap = [first_controller, second_controller];

    // The standalone controller is the whole voter set and elects itself.
    let listener = first_controller.to_string();
    let log_dir = format(
        &first,
        &["--standalone", "--controller-listener", &listener],
    )
    .await;
    let (first, first_dir) = start(first, vec![NodeRole::Controller], &log_dir, &bootstrap).await;
    first.wait_until_controller_leader().await;
    let first_directory = first
        .voter_directory_id_for_test(NodeId(3001))
        .expect("the standalone voter");

    // The brokers know only the bootstrap server. Each finishes starting,
    // which waits for a leader, and registers; the controller unfences them
    // only once their heartbeats reach its controller listener.
    let mut brokers = Vec::new();
    for broker in [broker_one, broker_two] {
        let log_dir = format(&broker, &[]).await;
        brokers.push(start(broker, vec![NodeRole::Broker], &log_dir, &bootstrap).await);
    }
    first.wait_until_brokers_registered(2).await;
    let through = brokers[0].0.listen_addr();
    wait_for_quorum(through, 3001, &[3001], &[1, 2]).await;

    // A controller formatted to join no initial quorum observes it.
    let log_dir = format(&second, &["--no-initial-controllers"]).await;
    let (second, second_dir) =
        start(second, vec![NodeRole::Controller], &log_dir, &bootstrap).await;
    wait_for_quorum(through, 3001, &[3001], &[1, 2, 3002]).await;

    // `kafka-metadata-quorum add-controller` sends `AddRaftVoter` for the new
    // controller through the bootstrap controller.
    let second_directory = krabka_broker::bootstrap::read_directory_id(&log_dir)
        .expect("the joining controller's meta.properties");
    let endpoint = RaftVoterEndpoint::new(
        "CONTROLLER",
        second_controller.ip().to_string(),
        second_controller.port(),
    )
    .expect("endpoint");
    until_ok("AddRaftVoter(3002)", || async {
        let mut admin = AdminClient::connect_controller(&[first_controller.to_string()])
            .await
            .map_err(|error| error.to_string())?;
        let added = admin
            .add_raft_voter(
                None,
                3002,
                second_directory,
                std::slice::from_ref(&endpoint),
            )
            .await;
        voter_change(added, krabka_broker::codes::DUPLICATE_VOTER)
    })
    .await;
    wait_for_quorum(through, 3001, &[3001, 3002], &[1, 2]).await;

    // `remove-controller` takes the leader out: 3002 leads, and 3001 observes.
    until_ok("RemoveRaftVoter(3001)", || async {
        let mut admin = AdminClient::connect_controller(&[
            first_controller.to_string(),
            second_controller.to_string(),
        ])
        .await
        .map_err(|error| error.to_string())?;
        let removed = admin.remove_raft_voter(None, 3001, first_directory).await;
        voter_change(removed, krabka_broker::codes::VOTER_NOT_FOUND)
    })
    .await;
    wait_for_quorum(through, 3002, &[3002], &[1, 2, 3001]).await;

    // The brokers stay in service under the new leader: their heartbeats
    // reach it on the endpoint the voter set names. A new leader starts every
    // registered broker alive, so the check waits out a heartbeat session
    // first, after which a broker whose heartbeats go elsewhere is fenced.
    tokio::time::sleep(FENCING_WINDOW).await;
    let fenced = second.fenced_broker_ids_for_test();
    assert!(fenced.is_empty(), "fenced under the new leader: {fenced:?}");

    for (broker, _dir) in brokers {
        broker.shutdown().await;
    }
    second.shutdown().await;
    first.shutdown().await;
    drop((first_dir, second_dir));
}

/// A controller bound to a wildcard address, as the system test starts every
/// controller with `--controller-listen-addr 0.0.0.0:9592`, advertises this
/// machine's host name in the voter set, as Kafka advertises the canonical
/// host name of a wildcard controller listener.
///
/// It once advertised `127.0.0.1` there. The leader's `UpdateRaftVoter`
/// committed that address over the one `--controller-listener` gave the
/// format, and a broker on another machine then sent every heartbeat to its
/// own loopback address: it stayed fenced and never finished starting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wildcard_controller_advertises_the_host_name_in_the_voter_set() {
    support::init_tracing();
    let controller = TcpListener::bind("0.0.0.0:0").await.expect("bind");
    let controller_addr = controller.local_addr().expect("bound address");
    let client = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let node = Node {
        id: 3001,
        client_addr: client.local_addr().expect("bound address"),
        client,
        controller: Some(controller),
        controller_addr,
        dir: TempDir::new().expect("tempdir"),
    };
    let reachable: SocketAddr = ([127, 0, 0, 1], controller_addr.port()).into();
    let listener = reachable.to_string();
    let log_dir = format(&node, &["--standalone", "--controller-listener", &listener]).await;
    let (controller, _dir) = start(node, vec![NodeRole::Controller], &log_dir, &[reachable]).await;
    controller.wait_until_controller_leader().await;

    let host_name = hostname::get()
        .expect("this machine's host name")
        .into_string()
        .expect("a UTF-8 host name");
    let advertised = vec![krabka_client_admin::QuorumNode {
        node_id: 3001,
        endpoints: vec![
            RaftVoterEndpoint::new("CONTROLLER", host_name, controller_addr.port())
                .expect("endpoint"),
        ],
    }];
    let deadline = Instant::now() + DEADLINE;
    loop {
        let nodes = async {
            let mut admin = AdminClient::connect_controller(&[reachable.to_string()])
                .await
                .map_err(|error| error.to_string())?;
            admin
                .describe_metadata_quorum()
                .await
                .map(|quorum| quorum.nodes)
                .map_err(|error| error.to_string())
        }
        .await;
        if nodes.as_ref() == Ok(&advertised) {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "the voter set advertises {nodes:?}, not {advertised:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    controller.shutdown().await;
}
