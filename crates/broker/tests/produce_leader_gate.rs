//! Multi-broker coverage for the Produce leadership gate.
//!
//! Kafka semantics: only the partition LEADER accepts a Produce. The broker
//! must reject a Produce misrouted to a non-leader with
//! `NOT_LEADER_OR_FOLLOWER` (6). It must NOT append that Produce to a local
//! follower replica. The client then refreshes metadata and re-routes to the
//! real leader.
//!
//! Two discriminating cases:
//!
//!  * **rf=3 (follower replica present):** every broker hosts a replica, so a
//!    Produce to a non-leader lands on a *follower's* local log if the handler
//!    skips the leadership check. Before the fix, the broker silently appended
//!    to the follower. It returned `error_code=0`, the follower log grew, and
//!    the leader never saw the record. That is silent data loss. The fix
//!    returns 6 and leaves the follower log untouched.
//!
//!  * **rf=1 (no replica on the target):** the non-leader broker holds no
//!    replica at all. Before the fix this returned
//!    `UNKNOWN_TOPIC_OR_PARTITION` (3) because the local registry lookup
//!    missed. Kafka returns 6, because the partition exists cluster-wide and
//!    this broker just isn't its leader. The fix now returns 6 as well. It
//!    consults the metadata image rather than the local registry.
//!
//! The same Produce sent to the actual leader must succeed with code 0 and a
//! durably stored record. This proves the leader path is unaffected.
//!
//! These tests are Windows-gated like the other multi-broker tests. openraft
//! `debug_assert!` races on the hosted Windows scheduler.

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{create_topics_request::CreateTopicsRequest, produce_response::NodeEndpoint},
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use support::cluster_lock;

use crate::support::{
    client::connect_client,
    records::{batch_from_records, value_record},
    topics::creatable_topic,
};

mod support;

/// Serialize the multi-broker tests in this binary. Each one boots a 3-node
/// loopback cluster, and running them concurrently exhausts ephemeral ports
/// and starves openraft election timing. The reason is the same as for the
/// `cluster_lock` in `producer_leader_routing.rs`.
/// The listener the test clients connect on. A KIP-951 `NodeEndpoints` entry
/// advertises the address of the listener the request arrived on, so this is
/// the endpoint name the refusal must answer with.
const PLAINTEXT_LISTENER: &str = "PLAINTEXT";

fn one_record_batch(v: &str) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        ..batch_from_records(vec![value_record(
            0,
            Some(bytes::Bytes::from(v.to_string())),
        )])
    }
}

/// Send a single one-record Produce (`acks=1`) for `(topic, partition)` over
/// `client`'s bootstrap connection. That is, send it directly at whichever
/// broker the client was built against, and bypass any leader routing. Returns
/// `(error_code, current_leader_id, node_endpoints)`. The last is the KIP-951
/// companion to the hint: without it a Java producer cannot resolve the node id
/// the hint names and falls back to the full `Metadata` round-trip.
async fn produce_one(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    value: &str,
) -> (i16, i32, Vec<NodeEndpoint>) {
    let resp = client
        .send(crate::support::produce::batch_request(
            one_record_batch(value),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id,
                partition: krabka_ids::PartitionIndex(partition),
                ..Default::default()
            },
        ))
        .await
        .expect("produce round-trip");
    let pr = &resp.responses[0].partition_responses[0];
    (
        pr.error_code,
        pr.current_leader.leader_id,
        resp.node_endpoints.clone(),
    )
}

use crate::support::partitions::wait_for_local_replica;

/// Case A of [`produce_to_non_leader_is_rejected`]: a Produce sent to a
/// non-leader that *does* hold a follower replica.
///
/// It is the case that can fail silently. A broker that appends to its own
/// follower replica answers as if it had led the write, and only the local log
/// end offset says otherwise -- which is why this asserts the follower's log
/// did not grow, alongside the refusal, the leader hint and the KIP-951
/// endpoint that makes the hint usable.
async fn rf3_produce_to_a_follower_is_refused(
    cluster: &[(BrokerHandle, krabka_broker::BrokerConfig, tempfile::TempDir)],
    rf3_id: WireUuid,
) -> u64 {
    let rf3_leader = cluster[0]
        .0
        .partition_leader_for_test("gate-rf3", 0)
        .unwrap();
    let follower_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != rf3_leader)
        .expect("a non-leader broker exists at rf=3");
    let follower_node = cluster[follower_idx].0.node_id();
    let follower_addr = cluster[follower_idx].1.listen_addr.to_string();

    // Wait for the follower to materialize its LOCAL replica (supervisor
    // reconcile lags the controller image). Without a local replica the rf=3
    // case would degenerate into the rf=1 case and not prove the
    // "don't append to a follower" property.
    wait_for_local_replica(&cluster[follower_idx].0, "gate-rf3", 0).await;
    let follower_leo_before = cluster[follower_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .expect("follower hosts gate-rf3");

    let follower_client = connect_client(follower_addr, None).await;
    let (code, leader_hint, node_endpoints) =
        produce_one(&follower_client, "gate-rf3", rf3_id, 0, "rf3-to-follower").await;
    assert!(
        code == 6,
        "rf=3 Produce to follower node{follower_node} (leader=node{rf3_leader}) must be \
         NOT_LEADER_OR_FOLLOWER (6); got {code}"
    );
    assert!(
        leader_hint == i32::try_from(rf3_leader).unwrap(),
        "current_leader hint must name the real leader node{rf3_leader}; got {leader_hint}"
    );
    // KIP-951's second half: the hint is a node id, and the producer can only
    // re-target if the same response tells it that node's address. The
    // endpoint must be the one the refusing broker's own image advertises for
    // the listener the request arrived on.
    let image = cluster[follower_idx].0.controller_image_for_test();
    let registration = image
        .broker(krabka_metadata::NodeId(rf3_leader))
        .expect("the leader is registered in the follower's image");
    let advertised = registration
        .endpoints
        .iter()
        .find(|endpoint| endpoint.name == PLAINTEXT_LISTENER)
        .expect("the leader advertises a PLAINTEXT endpoint");
    assert!(
        node_endpoints
            == vec![NodeEndpoint {
                node_id: leader_hint,
                host: advertised.host.clone(),
                port: i32::from(advertised.port),
                rack: registration.rack.clone(),
                ..Default::default()
            }],
        "the refused Produce must carry the leader's endpoint; got {node_endpoints:?}"
    );
    // The load-bearing anti-silent-append assertion: the follower's local log
    // MUST NOT have grown. Pre-fix it advanced by one (silent follower append).
    let follower_leo_after = cluster[follower_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .expect("follower hosts gate-rf3");
    assert!(
        follower_leo_after == follower_leo_before,
        "rejected Produce must NOT append to the follower's local log: \
         before={follower_leo_before} after={follower_leo_after}"
    );

    rf3_leader
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_to_non_leader_is_rejected() {
    let _g = cluster_lock().lock().await;
    let cluster = crate::support::registered_cluster(3).await;

    let bootstrap = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(bootstrap.clone(), None).await;

    // rf=3 topic (a replica on every broker) and a rf=1, 6-partition topic
    // (each partition on exactly one broker, so non-leaders hold no replica).
    let cr = admin
        .send(CreateTopicsRequest {
            topics: vec![
                creatable_topic(crate::support::topics::ConfiguredTopicSetup {
                    name: ("gate-rf3").into(),
                    replicas: crate::support::topics::TopicReplicationFactor(3),
                    ..Default::default()
                }),
                creatable_topic(crate::support::topics::ConfiguredTopicSetup {
                    name: ("gate-rf1").into(),
                    partitions: crate::support::topics::TopicPartitionCount(6),
                    ..Default::default()
                }),
            ],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        cr.topics.iter().all(|t| t.error_code == 0),
        "create: {cr:?}"
    );
    // v13 Produce drops topic.name and carries only topic_id; echo the ids.
    let rf3_id = cr
        .topics
        .iter()
        .find(|t| t.name == "gate-rf3")
        .unwrap()
        .topic_id;
    let rf1_id = cr
        .topics
        .iter()
        .find(|t| t.name == "gate-rf1")
        .unwrap()
        .topic_id;

    // Wait until node 1's image knows every partition's leader. Mirrors the
    // `partition_leader_for_test(..).is_some()` predicate exactly: partition
    // present in the image AND its leader field elected (non-zero).
    cluster[0]
        .0
        .wait_for_image(|img| {
            img.partition("gate-rf3", 0)
                .is_some_and(|pr| pr.leader != 0)
                && (0..6).all(|p| {
                    img.partition("gate-rf1", p)
                        .is_some_and(|pr| pr.leader != 0)
                })
        })
        .await;

    let rf3_leader = rf3_produce_to_a_follower_is_refused(&cluster, rf3_id).await;

    // ───────────────────────────────────────────────────────────────────
    // Case B: rf=1 — Produce to a NON-leader that holds NO replica.
    // ───────────────────────────────────────────────────────────────────
    let n1 = cluster[0].0.node_id();
    let off_node_part = (0..6)
        .find(|&p| {
            cluster[0]
                .0
                .partition_leader_for_test("gate-rf1", p)
                .is_some_and(|l| l != n1)
        })
        .expect("a gate-rf1 partition led off node 1");
    let rf1_leader = cluster[0]
        .0
        .partition_leader_for_test("gate-rf1", off_node_part)
        .unwrap();
    // Node 1 holds no replica for this rf=1 partition.
    assert!(
        cluster[0]
            .0
            .local_log_end_offset("gate-rf1", off_node_part)
            .is_none(),
        "test premise: node1 must NOT host gate-rf1/{off_node_part} (rf=1)"
    );
    let (code, leader_hint, _) = produce_one(
        &admin,
        "gate-rf1",
        rf1_id,
        off_node_part,
        "rf1-to-nonreplica",
    )
    .await;
    assert!(
        code == 6,
        "rf=1 Produce to non-replica node{n1} (leader=node{rf1_leader}) must be \
         NOT_LEADER_OR_FOLLOWER (6); got {code}"
    );
    assert!(
        leader_hint == i32::try_from(rf1_leader).unwrap(),
        "current_leader hint must name node{rf1_leader}; got {leader_hint}"
    );

    // ───────────────────────────────────────────────────────────────────
    // Leader path unchanged: the SAME Produce sent to the real leader of each
    // partition succeeds (code 0) and the record is durably stored.
    // ───────────────────────────────────────────────────────────────────
    let rf3_leader_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() == rf3_leader)
        .unwrap();
    let leader3_client =
        connect_client(cluster[rf3_leader_idx].1.listen_addr.to_string(), None).await;
    // Gate on the leader broker's own replica/image catch-up before producing,
    // so the success Produce can't race the image apply (UNKNOWN_TOPIC_ID 100 /
    // transient NOT_LEADER 6) — and so the `leo_before` read below is non-None.
    wait_for_local_replica(&cluster[rf3_leader_idx].0, "gate-rf3", 0).await;
    let leader3_leo_before = cluster[rf3_leader_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .expect("leader hosts gate-rf3");
    let (code, _, _) = produce_one(&leader3_client, "gate-rf3", rf3_id, 0, "rf3-to-leader").await;
    assert!(
        code == 0,
        "rf=3 Produce to the leader must succeed; got {code}"
    );
    let leader3_leo_after = cluster[rf3_leader_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .expect("leader hosts gate-rf3");
    assert!(
        leader3_leo_after == leader3_leo_before + 1,
        "leader's local log must grow by one on a successful Produce: \
         before={leader3_leo_before} after={leader3_leo_after}"
    );

    let rf1_leader_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() == rf1_leader)
        .unwrap();
    let leader1_client =
        connect_client(cluster[rf1_leader_idx].1.listen_addr.to_string(), None).await;
    // The observed flake: without this gate the Produce can reach the rf=1
    // leader before its image has applied the gate-rf1 topic_id, so the v13
    // topic_id resolution misses and returns UNKNOWN_TOPIC_ID (100) instead of
    // the required success. The rf=1 partition has no replica elsewhere, so
    // nothing else forces this broker's image/replica to be warm by now.
    wait_for_local_replica(&cluster[rf1_leader_idx].0, "gate-rf1", off_node_part).await;
    let (code, _, _) = produce_one(
        &leader1_client,
        "gate-rf1",
        rf1_id,
        off_node_part,
        "rf1-to-leader",
    )
    .await;
    assert!(
        code == 0,
        "rf=1 Produce to the leader must succeed; got {code}"
    );
    let leader1_leo = cluster[rf1_leader_idx]
        .0
        .local_log_end_offset("gate-rf1", off_node_part)
        .expect("leader hosts gate-rf1");
    assert!(
        leader1_leo >= 1,
        "rf=1 record must be durably stored on its leader; leo={leader1_leo}"
    );

    crate::support::shutdown_cluster(cluster).await;
}
