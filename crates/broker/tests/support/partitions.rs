//! Waits for exact partition state in the local registry or committed image.

use {
    crate::support::{
        client::{connect_client, create_topic_with},
        registered_cluster,
        topics::{CreateTopicSetup, TopicPartitionCount},
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::HashSet,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// Waits until `handle` sees `(topic, partition)` in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

/// Wait until every listed broker has materialized the partition in its image.
pub async fn wait_for_partition_on_all<'a>(
    brokers: impl IntoIterator<Item = &'a BrokerHandle>,
    topic: &str,
    partition: i32,
) {
    for broker in brokers {
        broker.wait_until_partition_present(topic, partition).await;
    }
}

/// Waits for the supervisor to materialize the local writer actor.
///
/// # Panics
/// Panics if the local replica does not appear within 30 seconds.
pub async fn wait_for_local_replica(broker: &BrokerHandle, topic: &str, partition: i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while broker.local_log_end_offset(topic, partition).is_none() {
        assert!(
            Instant::now() <= deadline,
            "broker never materialized a local replica for {topic}/{partition}"
        );
        // This gates on the LOCAL writer actor, which lags the metadata image.
        // The image awaiter cannot observe local-registry materialization.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits until the image's ISR equals the supplied set, including its members.
pub async fn wait_partition_isr_only(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    expected: &[u64],
) {
    let expected_set: HashSet<u64> = expected.iter().copied().collect();
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                p.isr.iter().map(|n| n.0).collect::<HashSet<u64>>() == expected_set
            })
        })
        .await;
}

/// Waits until the image reports the specified leader for this partition.
pub async fn wait_partition_leader(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.leader.0 == leader)
        })
        .await;
}

/// Waits until the image knows a leader for every partition in `0..n_partitions`.
pub async fn wait_for_partition_leaders(handle: &BrokerHandle, topic: &str, n_partitions: i32) {
    handle
        .wait_for_image(|img| {
            (0..n_partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|partition| partition.leader != 0)
            })
        })
        .await;
}

/// Waits until the ISR contains the specified member.
pub async fn wait_partition_isr_contains(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    member: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&krabka_broker::NodeId(member)))
        })
        .await;
}

/// Wait for the writer's applied compression override rather than the earlier image update.
///
/// # Panics
/// Panics if the override has not reached the writer within 10 seconds.
pub async fn wait_for_compression(
    handle: &BrokerHandle,
    topic: &str,
    expected: Option<krabka_compression::CompressionType>,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(cfg) = handle.partition_log_config_for_test(topic, 0)
            && cfg.compression_type == expected
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "compression_type={expected:?} never propagated to partition LogConfig within 10s"
        );
        // intentional: this polls the partition writer's applied LogConfig
        // (partition_log_config_for_test), not the metadata image. No awaiter
        // captures "the reconcile loop has pushed the compression override into
        // the writer"; waiting on the image alone would fire strictly earlier
        // and reintroduce the produce-before-override race this helper exists to
        // prevent. The loop is bounded by the 10s deadline asserted above.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The routing fixtures must exercise a leader outside their bootstrap broker.
///
/// # Panics
/// Panics if every partition is led by the bootstrap broker.
pub fn require_non_bootstrap_partitions(
    broker: &BrokerHandle,
    topic: &str,
    n_partitions: i32,
) -> Vec<i32> {
    let bootstrap_node = broker.node_id();
    let non_bootstrap_partitions: Vec<_> = (0..n_partitions)
        .filter(|&partition| {
            broker
                .partition_leader_for_test(topic, partition)
                .is_some_and(|leader| leader != bootstrap_node)
        })
        .collect();
    assert!(
        !non_bootstrap_partitions.is_empty(),
        "all {n_partitions} partitions are led by the bootstrap node — \
         no cross-broker routing to exercise; test would be vacuous"
    );
    eprintln!(
        "partitions led by non-bootstrap brokers: {non_bootstrap_partitions:?} \
         (bootstrap = node {bootstrap_node})"
    );
    non_bootstrap_partitions
}

/// Starts the rf=1 cluster used by native per-leader routing tests.
pub async fn start_leader_routing_cluster(
    topic: &str,
) -> (
    Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    String,
    Uuid,
    i32,
) {
    let cluster = registered_cluster(3).await;
    let bootstrap = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(bootstrap.clone(), None).await;
    let n_partitions = 6;
    let topic_id = create_topic_with(&admin, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(n_partitions),
        ..Default::default()
    })
    .await;
    wait_for_partition_leaders(&cluster[0].0, topic, n_partitions).await;
    (cluster, bootstrap, topic_id, n_partitions)
}
