// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` (see
// elect_leaders.rs / acl_handlers.rs preamble).

//! `BrokerHandle::controlled_shutdown` integration test.
//!
//! The test uses a 3-broker PLAINTEXT cluster and an rf=3 topic. Broker 1 is
//! the preferred leader of every partition. `controlled_shutdown(broker 1)`
//! must:
//! 1. Move leadership of every partition off broker 1, and
//! 2. Return `Ok(())` after the controller acknowledges
//!    `should_shut_down=true`. Broker 1 then leads zero partitions.
//!
//! The test is gated to non-Windows to match the multi-broker convention established by the existing integration suites.`debug_assert!` races on the hosted Windows
//! task scheduler are unrelated to the protocol under test.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_metadata::NodeId as MetadataNodeId;

use crate::support::{
    registered_cluster, shutdown_cluster,
    topics::{CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor},
};

mod kafka_wire;
mod support;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-controlled-shutdown-test";

async fn create_topic(addr: SocketAddr, setup: CreateTopicSetup<'_>) {
    kafka_wire::create_topic_from_setup(addr, CLIENT_ID, setup).await;
}

/// Waits until `(topic, partition)` appears in the metadata image of
/// `handle`.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

#[derive(krabka_macros::FieldDefaults)]
struct ForcedLeadershipSetup<'a> {
    #[default("drain-me")]
    topic: &'a str,
    #[default(crate::support::topics::TopicPartitionCount(4))]
    partitions: TopicPartitionCount,
    #[default(krabka_metadata::NodeId(1))]
    leader: krabka_metadata::NodeId,
    #[default(vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2), krabka_metadata::NodeId(3)])]
    replicas: Vec<MetadataNodeId>,
}

/// Injects a `PartitionRecord` that makes `target` the leader for every
/// partition of `topic`. It uses `submit_metadata_record_for_test` to bypass
/// the public wire path, so the test does not have to drive `ElectLeaders`.
/// It returns after the image shows `leader=target` for every partition.
async fn force_leadership_for_test(leader_handle: &BrokerHandle, setup: ForcedLeadershipSetup<'_>) {
    use krabka_metadata::{MetadataRecord, PartitionRecord};

    let topic = setup.topic;
    let partitions = setup.partitions.0;
    let target = setup.leader.0;
    let replicas = setup.replicas;

    let image = leader_handle.controller_image_for_test();
    for p in 0..partitions {
        let Some(pr) = image.partition(topic, p) else {
            continue;
        };
        let record = MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_string(),
            partition: p,
            leader: krabka_metadata::NodeId(target),
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: pr.leader_epoch.next(),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        });
        leader_handle
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit forced leadership");
    }

    // Wait until every partition reports `target` as leader.
    leader_handle
        .wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target)
            })
        })
        .await;
}

/// Returns the number of partitions in `topic` that `target` currently leads,
/// according to the image of `observer`.
fn leader_count(observer: &BrokerHandle, topic: &str, partitions: i32, target: u64) -> usize {
    let mut count = 0usize;
    for p in 0..partitions {
        if observer.partition_leader_for_test(topic, p) == Some(target) {
            count += 1;
        }
    }
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_shutdown_drains_leadership_and_returns_ok() {
    let _g = support::cluster_lock().lock().await;

    support::init_tracing();

    let mut cluster = registered_cluster(3).await;

    // Resolve the raft (controller) leader. We then pick a *follower*
    // broker as the controlled-shutdown target. Picking the controller
    // leader would also exercise the path — its heartbeat to itself is
    // a local round-trip — but the bootstrap broker is also the sole
    // replica of `__consumer_offsets/0` (rf=1, replicas=[node_id]),
    // which cannot be drained. Targeting a follower keeps the test
    // focused on the controllable wire path.
    let raft_leader_idx = {
        let leader = cluster[0].0.wait_until_controller_leader().await;
        cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("controller leader must be a cluster member")
    };
    let target_idx = (raft_leader_idx + 1) % cluster.len();
    let target_node_id = cluster[target_idx].1.node_id;
    eprintln!(
        "controlled shutdown target: broker_id={target_node_id} (cluster idx {target_idx}, \
         raft leader is idx {raft_leader_idx})"
    );

    // Create topic on the cluster leader's listen addr.
    let addr = cluster[raft_leader_idx].1.listen_addr;
    let topic: &str = "drain-me";
    let partitions: i32 = 4;
    create_topic(
        addr,
        CreateTopicSetup {
            topic,
            num_partitions: TopicPartitionCount(partitions),
            replication_factor: TopicReplicationFactor(3),
            ..Default::default()
        },
    )
    .await;
    for p in 0..partitions {
        for (h, _, _) in &cluster {
            wait_partition_exists(h, topic, p).await;
        }
    }

    // Force the target to be leader of every partition. The natural
    // round-robin assignment with 3 brokers / 4 partitions would
    // leader-balance across the cluster — we want to prove draining is
    // exhaustive, so concentrate every leader onto the target first.
    let mut replicas: Vec<u64> = (1..=3).collect();
    // Put target first so its `replicas[0]` is itself (preferred).
    replicas.sort_by_key(|n| i32::from(*n != target_node_id));
    force_leadership_for_test(
        &cluster[raft_leader_idx].0,
        ForcedLeadershipSetup {
            topic,
            partitions: TopicPartitionCount(partitions),
            leader: krabka_metadata::NodeId(target_node_id.0),
            replicas: replicas
                .iter()
                .copied()
                .map(krabka_metadata::NodeId)
                .collect(),
        },
    )
    .await;

    // Sanity: target leads everything before we start. Use the raft
    // leader's image since the target is about to be drained.
    for (h, _, _) in &cluster {
        h.wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target_node_id)
            })
        })
        .await;
    }
    assert!(
        leader_count(
            &cluster[raft_leader_idx].0,
            topic,
            partitions,
            target_node_id.0
        ) == usize::try_from(partitions).unwrap(),
        "target should lead all partitions before shutdown"
    );

    // Pop the target out of the cluster vec — `controlled_shutdown`
    // consumes the handle, and we need to keep the surviving brokers
    // alive afterward to verify the post-shutdown image.
    let (target_handle, target_cfg, _target_dir) = cluster.remove(target_idx);
    drop(target_cfg);

    // Drive controlled shutdown. The handler-side leader transfer
    // submits records on each heartbeat tick (default 200ms in
    // for_tests config). Allow several ticks plus raft commit
    // latency.
    target_handle
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("controlled_shutdown should drain and exit cleanly");

    // After the controlled-shutdown future resolves, the target's
    // heartbeat client has observed should_shut_down=true. That means
    // the controller's image, at the moment of that response, had zero
    // partitions led by the target. Verify on a surviving broker.
    for (observer, _, _) in &cluster {
        observer
            .wait_for_image(|img| {
                (0..partitions)
                    .all(|p| img.partition(topic, p).map(|pr| pr.leader) != Some(target_node_id))
            })
            .await;
    }

    // Tidy up surviving brokers.
    shutdown_cluster(cluster).await;
}

/// A controller-only node has no partitions to drain and no heartbeat client
/// to carry the request, so the drain must not wait for an answer that never
/// comes. Before, it waited out the whole timeout and returned
/// `ShutdownTimeout`, so every stop of an isolated controller took the full
/// drain timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controlled_shutdown_of_a_controller_only_node_stops_at_once() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut config = krabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    config.roles = vec![krabka_broker::config::NodeRole::Controller];
    let controller = krabka_broker::Broker::start(config)
        .await
        .expect("controller-only start");
    controller.wait_until_controller_leader().await;

    let stopped = tokio::time::timeout(
        Duration::from_secs(20),
        controller.controlled_shutdown(Duration::from_secs(10)),
    )
    .await
    .expect("the stop does not wait out the drain timeout");

    assert!(let Ok(()) = stopped);
}
