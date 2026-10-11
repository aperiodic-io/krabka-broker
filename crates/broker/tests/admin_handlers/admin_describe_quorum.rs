//! `DescribeQuorum` (`api_key` 55, KIP-595): the dispatch glue, the ACL allow
//! path, and the response encoding, driven against the `__cluster_metadata`
//! topic of a one-broker cluster; and the broker-listener forwarding to the
//! active controller (#1034) on a two-broker combined cluster.

use assert2::{assert, check};
use krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse;

use crate::{
    admin_harness::build_client,
    support::{
        quorum::{
            describe_quorum_request as support_describe_quorum_request, follower_config,
            metadata_quorum_request as describe_quorum_request, two_node_leader,
        },
        shutdown_cluster, start_n_node_client, start_n_node_with_retry,
    },
};

/// Zero out the parts of a response that legitimately keep advancing between
/// two requests answered moments apart by the same live leader: the
/// per-replica wall-clock timestamps, each replica's raft log position, and
/// the partition's high watermark. The leader's raft log keeps taking new
/// entries (periodic quorum-internal writes) the whole time the cluster
/// runs, so two `DescribeQuorum` snapshots taken microseconds apart can
/// legitimately disagree on exactly how far the log has moved even though
/// they name the same leader, epoch, and voter set -- that agreement is what
/// this comparison is for.
fn without_replica_progress(mut resp: DescribeQuorumResponse) -> DescribeQuorumResponse {
    for topic in &mut resp.topics {
        for partition in &mut topic.partitions {
            partition.high_watermark = 0;
            for replica in partition
                .current_voters
                .iter_mut()
                .chain(&mut partition.observers)
            {
                replica.log_end_offset = 0;
                replica.last_fetch_timestamp = 0;
                replica.last_caught_up_timestamp = 0;
            }
        }
    }
    resp
}

/// `DescribeQuorum` against the cluster-metadata topic on a 1-broker
/// cluster returns one partition row carrying the broker's voter id with
/// `leader_id` == 1. This test verifies the dispatch glue, the ACL allow
/// path, and the response encoding. The pure `build_topic_responses` helper
/// has its own unit tests in
/// `crates/broker/src/handlers/describe_quorum.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_quorum_reports_cluster_metadata_voter_set() {
    let (_cluster, client) = start_n_node_client(1, "admin-handlers-test").await;

    let req = describe_quorum_request();
    let resp = client.send(req).await.expect("describe_quorum");
    check!(resp.error_code == 0, "top-level error_code");
    assert!(resp.topics.len() == 1, "exactly one topic row");
    check!(resp.topics[0].topic_name == "__cluster_metadata");
    let pd = &resp.topics[0].partitions[0];
    check!(pd.partition_index == 0);
    check!(pd.error_code == 0, "metadata partition 0 succeeds");
    check!(
        pd.leader_id == 1,
        "1-broker cluster: bootstrap voter id=1 is leader"
    );
    check!(
        pd.leader_epoch >= 1,
        "openraft term must be >= 1 once a leader is elected; got {}",
        pd.leader_epoch,
    );
    check!(
        pd.high_watermark >= 0,
        "last_applied_index is non-negative once any record applies; got {}",
        pd.high_watermark,
    );
    assert!(
        pd.current_voters.len() == 1,
        "single voter for 1-broker cluster"
    );
    check!(pd.current_voters[0].replica_id == 1);
    check!(
        pd.current_voters[0].log_end_offset >= 0,
        "leader knows its own matched index; got {}",
        pd.current_voters[0].log_end_offset,
    );
    check!(
        pd.observers.is_empty(),
        "a lone combined node has no observer fetching from it"
    );
}

/// A combined node that is not the active controller forwards `DescribeQuorum`
/// to the active controller instead of answering from its own (follower) raft
/// view (#1034). Before the fix, the follower answered locally and reported
/// every voter's `log_end_offset` as `-1` (openraft only fills the
/// replication map on the leader); after the fix it gets the leader's exact
/// answer, so the two decoded responses name the same leader, epoch, and
/// voter set (apart from each replica's log position, high watermark, and
/// wall-clock timestamps, which the live leader may have advanced between
/// the two requests).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_follower_forwards_describe_quorum_to_the_active_controller() {
    let cluster = start_n_node_with_retry(2).await;

    // Ask node 0 first: whichever role it holds, the fix means it now answers
    // correctly (forwarded if it is the follower, local if it is the leader),
    // so its response names the true leader.
    let (_, first_cfg, _dir0) = &cluster[0];
    let first_client = build_client(first_cfg.listen_addr).await;
    let first_resp = first_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from node 0");
    assert!(first_resp.error_code == 0, "top-level error_code");
    let leader_id = two_node_leader(&first_resp, (1, 2));
    let follower_cfg = follower_config(&cluster, leader_id);
    let follower_client = build_client(follower_cfg.listen_addr).await;
    let follower_resp = follower_client
        .send(describe_quorum_request())
        .await
        .expect("describe_quorum from the follower");

    check!(follower_resp.error_code == 0, "top-level error_code");
    let follower_partition = &follower_resp.topics[0].partitions[0];
    check!(
        follower_partition.error_code == 0,
        "forwarded to the leader instead of answered locally"
    );
    check!(
        follower_partition.leader_id == leader_id,
        "the follower's forwarded answer names the same leader"
    );
    assert!(
        follower_partition
            .current_voters
            .iter()
            .all(|v| v.log_end_offset >= 0),
        "a follower answering locally would report -1 for every voter; \
         the forwarded answer carries the leader's real matched indexes: {:?}",
        follower_partition.current_voters,
    );

    assert!(without_replica_progress(follower_resp) == without_replica_progress(first_resp));

    shutdown_cluster(cluster).await;
}

/// A request for a topic other than `__cluster_metadata`, or a partition
/// other than 0, gets Kafka's `UNKNOWN_TOPIC_OR_PARTITION` (3) with its
/// message -- not the `INVALID_TOPIC_EXCEPTION` (17) the broker listener used
/// to answer with locally before it forwarded (#1034). The check runs
/// against a node forwarding to the active controller (node 0 of a 2-node
/// cluster is a follower about half the time; either way `forward_raw`
/// routes the request to whichever node answers it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_quorum_for_another_topic_is_unknown_topic_or_partition() {
    let cluster = start_n_node_with_retry(2).await;
    let (_, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    let req = support_describe_quorum_request("not-the-metadata-topic", vec![0]);
    let resp = client.send(req).await.expect("describe_quorum");

    check!(resp.error_code == 0, "top-level error_code");
    assert!(resp.topics.len() == 1);
    let pd = &resp.topics[0].partitions[0];
    check!(pd.error_code == 3, "UNKNOWN_TOPIC_OR_PARTITION, not 17");
    check!(
        pd.error_message.as_deref() == Some("This server does not host this topic-partition."),
        "Kafka's Errors.UNKNOWN_TOPIC_OR_PARTITION.message(); got {:?}",
        pd.error_message,
    );

    shutdown_cluster(cluster).await;
}
