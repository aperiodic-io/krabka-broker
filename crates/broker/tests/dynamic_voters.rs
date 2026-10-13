//! KIP-853 dynamic-voters end-to-end integration tests.
//!
//! These tests exercise the *real* auto-join path. Broker 0 self-bootstraps as
//! the sole voter. Brokers 1 to n then boot in `Join` mode with
//! `auto_join = true` and grow the quorum by sending `AddRaftVoter(self)` to
//! the leader over the wire. The shrink test then removes one voter through
//! the controller leader and asserts that the committed voter set contracts on
//! every node.
//!
//! openraft's debug assertions race on the hosted Windows scheduler, so these
//! tests are gated off Windows, like the other multi-node suites.

use {
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, NodeId},
    krabka_raft::{
        RaftError,
        reconfig::{ReconfigOutcome, RemoveVoter},
    },
    std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// A started node, and the directory that backs it.
struct DynamicNode {
    handle: BrokerHandle,
    dir: TempDir,
}

async fn start_dynamic_cluster(n: u64) -> Vec<DynamicNode> {
    let cluster_id = uuid::Uuid::from_u128(853);
    let mut cluster = Vec::new();
    let mut bootstrap_controller: Option<std::net::SocketAddr> = None;

    for id in 1..=n {
        let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let data_addr = data_listener.local_addr().unwrap();
        let controller_addr = controller_listener.local_addr().unwrap();
        let dir = TempDir::new().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.broker_id = i32::try_from(id).unwrap();
        config.node_id = NodeId(id);
        config.directory_id = uuid::Uuid::from_u128(u128::from(id));
        config.cluster_id = Some(cluster_id);
        config.listen_addr = data_addr;
        config.advertised_listener = data_addr.to_string();
        config.controller_listen_addr = controller_addr;
        config.auto_join_retry_backoff = krabka_units::millis(20);
        config.startup_leader_wait_timeout = krabka_units::secs(10);

        if let Some(bootstrap) = bootstrap_controller {
            config.bootstrap_mode = BootstrapMode::Join;
            config.controller_quorum_voters = vec![(NodeId(1), bootstrap.to_string())];
            config.bootstrap_servers = vec![bootstrap.to_string()];
            config.auto_join = true;
        } else {
            config.bootstrap_mode = BootstrapMode::Bootstrap;
            config.controller_quorum_voters = vec![(NodeId(1), controller_addr.to_string())];
        }

        let handle = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            Broker::start_with_listeners(config, Some(controller_listener), Some(data_listener)),
        )
        .await
        .expect("dynamic controller start timed out")
        .expect("dynamic controller start");
        if bootstrap_controller.is_none() {
            bootstrap_controller = Some(handle.controller_addr());
            let outcome = handle
                .finalize_kraft_version_for_test(1)
                .await
                .expect("activate kraft.version 1");
            assert!(matches!(outcome, ReconfigOutcome::Committed));
        }
        eprintln!(
            "started node {id}: voters={} kraft.version={} leader={:?}",
            handle.voter_count_for_test(),
            handle.kraft_version_for_test(),
            handle.controller_leader_id()
        );
        cluster.push(DynamicNode { handle, dir });
    }
    cluster
}

/// Auto-join must grow a fresh cluster from one voter to three. Broker 0
/// bootstraps alone, and brokers 1 and 2 join over the wire.
///
/// `start_n_node` already waits for convergence. This test asserts again
/// against the leader's committed image, so that a convergence regression
/// fails here rather than through the harness's `Startup` error.
#[tokio::test]
async fn auto_join_grows_quorum_to_three() {
    let cluster = start_dynamic_cluster(3).await;

    // The last join can start an election, so a node may not lead yet.
    let leader = wait_for_leader(&cluster).await;

    leader.wait_for_image(|img| img.voters().len() == 3).await;

    // Every node should eventually agree on the 3-voter set, not just the
    // leader.
    for node in &cluster {
        node.handle
            .wait_for_image(|img| img.voters().len() == 3)
            .await;
    }
}

/// The voter that the shrink test removes.
#[derive(Debug, Clone, Copy)]
enum Victim {
    /// A voter that does not lead when the test starts.
    Follower,
    /// The voter that leads when the test starts. Its removal moves the
    /// leadership: a leader that commits its own removal resigns, as Kafka's
    /// `RemoveVoterHandler.highWatermarkUpdated` does.
    Leader,
}

/// The node that believes that it leads the controller quorum, if one does.
fn self_reported_leader(cluster: &[DynamicNode]) -> Option<&BrokerHandle> {
    cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| handle.controller_leader_id() == Some(NodeId(handle.node_id())))
}

/// Wait until a node believes that it leads the controller quorum. An
/// election can be in progress when the caller looks.
async fn wait_for_leader(cluster: &[DynamicNode]) -> &BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(leader) = self_reported_leader(cluster) {
            return leader;
        }
        assert!(
            Instant::now() <= deadline,
            "no controller leader within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn node(cluster: &[DynamicNode], id: NodeId) -> Option<&BrokerHandle> {
    cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| NodeId(handle.node_id()) == id)
}

/// Remove `victim` the way a Kafka admin client that uses
/// `bootstrap.controllers` does. The first attempt goes to a controller that
/// does not lead. Every later attempt goes to the leader that the last answer
/// named, or to a node that believes that it leads.
///
/// The loop retries these answers:
///
/// - `Ok(NotLeader)`: the node did not lead when it read the request, so it
///   appended nothing. Kafka answers `NOT_LEADER_OR_FOLLOWER`.
/// - `Err(ReconfigInProgress)`: an earlier voter change or the leader's new
///   epoch is not committed yet. Kafka answers `REQUEST_TIMED_OUT`.
/// - `Err(NotLeader)`: the node appended the removal and then lost the
///   leadership before the removal committed. Kafka's `LeaderState.close`
///   answers the pending request with `NOT_LEADER_OR_FOLLOWER`. A later leader
///   can still commit that record, because the record can be in its log.
///
/// `Err(VoterNotFound(victim))` ends the loop only after an `Err(NotLeader)`,
/// and only when the answering leader's committed voter set is the expected
/// set. A leader decides `VoterNotFound` after the check that its own epoch
/// is committed, so its committed voter set holds every voter change from
/// before its epoch. The victim can be missing from that set only because an
/// earlier attempt's removal committed. Kafka's `RemoveVoterHandler` answers
/// `VOTER_NOT_FOUND` in the same case.
///
/// Returns the answers, in order, for the test log.
async fn remove_through_leader(
    cluster: &[DynamicNode],
    request: &RemoveVoter,
    expected: &BTreeSet<NodeId>,
) -> Vec<String> {
    let first = cluster
        .iter()
        .map(|node| &node.handle)
        .find(|handle| handle.controller_leader_id() != Some(NodeId(handle.node_id())))
        .expect("a controller that does not lead");
    let mut target = Some(NodeId(first.node_id()));
    let mut appended_then_lost_leadership = false;
    let mut answers = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() <= deadline,
            "the removal of {} did not finish; answers: {answers:?}",
            request.id,
        );
        let Some(handle) = target
            .and_then(|id| node(cluster, id))
            .or_else(|| self_reported_leader(cluster))
        else {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        };
        let outcome = handle.remove_voter_for_test(request.clone()).await;
        answers.push(format!("node {}: {outcome:?}", handle.node_id()));
        target = match outcome {
            Ok(ReconfigOutcome::Committed) => return answers,
            Ok(ReconfigOutcome::NotLeader { leader }) => leader,
            Err(RaftError::ReconfigInProgress) => Some(NodeId(handle.node_id())),
            Err(RaftError::NotLeader { current_leader }) => {
                appended_then_lost_leadership = true;
                current_leader
            }
            Err(RaftError::VoterNotFound(id)) if id == request.id => {
                let committed = handle.voter_ids_for_test();
                assert!(
                    appended_then_lost_leadership && committed == *expected,
                    "VoterNotFound({id}) with no earlier removal that could commit, or with \
                     committed voters {committed:?}; answers: {answers:?}",
                );
                return answers;
            }
            Err(error) => panic!("remove_voter RPC: {error:?}; answers: {answers:?}"),
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// After the cluster grows to three, the removal of one voter through the
/// controller leader must shrink the committed voter set to two on every node.
///
/// The test does not depend on a leader that stays still. The removal starts
/// at a controller that does not lead, follows the leader through
/// `NotLeader` answers, and survives a leadership change. The leader case
/// moves the leadership on every run.
#[tokio::test]
async fn remove_voter_shrinks_quorum() {
    for victim_kind in [Victim::Follower, Victim::Leader] {
        let cluster = start_dynamic_cluster(3).await;
        for node in &cluster {
            node.handle
                .wait_for_image(|img| img.voters().len() == 3)
                .await;
        }

        let leader = wait_for_leader(&cluster).await;
        let voters = leader.voter_ids_for_test();
        let leader_id = NodeId(leader.node_id());
        let victim = match victim_kind {
            Victim::Leader => leader_id,
            Victim::Follower => voters
                .iter()
                .copied()
                .find(|&id| id != leader_id)
                .expect("a follower voter to remove"),
        };
        // `remove_voter` keys on (id, directory_id). Read the directory id
        // from the committed image.
        let request = RemoveVoter {
            id: victim,
            directory_id: leader
                .voter_directory_id_for_test(victim)
                .expect("victim's directory id present in image"),
        };
        let expected: BTreeSet<NodeId> = voters.into_iter().filter(|&id| id != victim).collect();

        let answers = remove_through_leader(&cluster, &request, &expected).await;
        eprintln!("{victim_kind:?} {victim}: {answers:?}");

        // Every node commits the same two-voter set. A node that still held
        // the victim, or that lost the other voter, fails here. The remaining
        // voters come first. The removed node follows: it keeps fetching as an
        // observer, as a removed Kafka voter does.
        for node in &cluster {
            if NodeId(node.handle.node_id()) != victim {
                node.handle
                    .wait_for_image(|img| img.voters().ids() == expected)
                    .await;
            }
        }
        let removed = node(&cluster, victim).expect("the removed node");
        removed
            .wait_for_image(|img| img.voters().ids() == expected)
            .await;
        // A leader from the remaining voters must lead the smaller quorum.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self_reported_leader(&cluster)
            .is_some_and(|handle| expected.contains(&NodeId(handle.node_id())))
        {
            assert!(
                Instant::now() <= deadline,
                "no voter from {expected:?} leads after the removal of {victim}",
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Stop this cluster before the next case starts its own.
        for DynamicNode { handle, dir: _dir } in cluster {
            handle.shutdown().await;
        }
    }
}
