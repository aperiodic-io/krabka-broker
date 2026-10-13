//! `DescribeQuorum` forwarding failure answers a typed `NOT_LEADER_OR_FOLLOWER`
//! `DescribeQuorumResponse`, not a dropped connection (review of #1034).
//!
//! `describe_quorum::handle`'s forward path used to `map_err(BrokerError::from)`
//! and propagate the failure. The registry dispatch loop
//! (`network::dispatch::registry::send_registry_response`) has no response
//! shape to build for a bare `Err(BrokerError)` from a
//! `DispatchKind::Context` handler: it logs a warning and closes the
//! connection. A Kafka client asking `DescribeQuorum` during a dial failure
//! or before a leader is known expects the typed answer instead.

mod support;

use {
    crate::support::{client::connect_owned, shutdown_cluster, start_n_node_with_retry},
    assert2::{assert, check},
};

/// Kafka's `NOT_LEADER_OR_FOLLOWER`.
const NOT_LEADER_OR_FOLLOWER: i16 = 6;

async fn build_client(addr: std::net::SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "describe-quorum-no-leader-test",
        "client build",
    )
    .await
}

use crate::support::quorum::metadata_quorum_request as describe_quorum_request;

/// A follower whose leader hint just went dead answers `DescribeQuorum` with
/// a typed `NOT_LEADER_OR_FOLLOWER` instead of closing the connection: the
/// follower still believes the shut-down node is the leader (it only learns
/// otherwise from its own election timeout, which this test outruns), so
/// `ControllerHandle::forward_raw`'s dial to it fails with
/// `RaftError::Network`, exactly the "dial itself failed" case the fix
/// catches and answers instead of propagating.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_whose_leader_just_died_answers_not_leader_instead_of_closing() {
    let mut cluster = start_n_node_with_retry(2).await;

    let node0_client = build_client(cluster[0].1.listen_addr).await;
    let first_resp = node0_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from node 0");
    check!(first_resp.error_code == 0, "top-level error_code");
    let leader_id = first_resp.topics[0].partitions[0].leader_id;
    assert!(
        leader_id == 1 || leader_id == 2,
        "a 2-node cluster has an elected leader; got {leader_id}"
    );
    node0_client.close();

    let leader_index = cluster
        .iter()
        .position(|(_, cfg, _)| cfg.broker_id == leader_id)
        .expect("the leader broker");
    let (leader_handle, _, _leader_dir) = cluster.remove(leader_index);
    // Shut the leader down and ask the follower right away: it still holds
    // the now-dead leader as its hint (only its own election timeout would
    // change that), so the forward's dial fails immediately.
    leader_handle.shutdown().await;

    let (_, follower_cfg, _dir1) = &cluster[0];
    let follower_client = build_client(follower_cfg.listen_addr).await;
    let follower_resp = follower_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum must get a typed response, not a dropped connection");

    check!(
        follower_resp.error_code == NOT_LEADER_OR_FOLLOWER,
        "expected the typed top-level NOT_LEADER_OR_FOLLOWER once the leader \
         hint is dead, not a dropped connection; got {follower_resp:?}"
    );

    shutdown_cluster(cluster).await;
}
