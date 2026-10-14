//! The active controller cut off from both of its peers, while a partition
//! it only follows keeps its leader on the connected side.
//!
//! The two connected nodes elect a new controller, which fences the isolated
//! one and drops it from the ISR, and the partition goes on taking and
//! deduplicating produces. A dedup config change made meanwhile reaches the
//! isolated node once it is healed. After the heal the formerly isolated node
//! forwards a config change and a topic creation to the new controller, and
//! leads the new topic with a working index.

use {
    crate::{
        harness::{
            Cluster, Topic, assert_indexes_match, at, await_isr, connect, converged, dedup_topic,
            offsets_and_keys, produce, send, until,
        },
        support::{
            cluster_lock,
            configs::{
                ConfigOperation, incremental_config, incremental_request, incremental_resource,
            },
            dedup::{Rec, keyed_batch},
            produce::ProduceAcknowledgements::AllReplicas,
        },
    },
    assert2::assert,
    krabka_broker::NodeId,
};

const KEY_HEADER: &str = "dedup.key.header";

/// Set the topic's `dedup.key.header` to `header`, or delete it for `None`,
/// through broker `via`, which forwards the change to the controller.
async fn set_key_header(cluster: &Cluster, via: u64, topic: &str, header: Option<&str>) {
    let operation = match header {
        Some(_) => ConfigOperation::Set,
        None => ConfigOperation::Delete,
    };
    let change = incremental_config(KEY_HEADER, header.map(str::to_owned), operation);
    let response = connect(cluster, via)
        .await
        .send(
            incremental_request()
                .resources(vec![incremental_resource(2, topic, vec![change])])
                .call(),
        )
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses.iter().all(|row| row.error_code == 0),
        "{response:?}"
    );
}

/// Wait until every broker of `observers` sees the topic's key header as
/// `header`.
async fn await_key_header(cluster: &Cluster, topic: &str, observers: &[u64], header: Option<&str>) {
    until(
        format!("{observers:?} see {KEY_HEADER}={header:?} on {topic}"),
        || {
            observers
                .iter()
                .all(|node| {
                    cluster
                        .broker(*node)
                        .controller_image_for_test()
                        .topic_config(topic)
                        .and_then(|configs| configs.get(KEY_HEADER))
                        .map(String::as_str)
                        == header
                })
                .then_some(())
        },
    )
    .await;
}

/// Produce one batch whose records carry the dedup header, as `(key, id)`.
async fn produce_identified(
    cluster: &Cluster,
    node: u64,
    topic: &Topic,
    records: &[(&str, &str)],
) -> i64 {
    let records: Vec<Rec> = records
        .iter()
        .map(|(key, id)| Rec::key(key).dedup_id(id))
        .collect();
    let row = send(
        cluster,
        node,
        topic,
        keyed_batch().records(&records).call(),
        AllReplicas,
        30_000,
    )
    .await
    .expect("Produce");
    assert!(row.error_code == 0, "{row:?}");
    row.base_offset
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_controller_leaves_dedup_working_and_honours_changes_after_the_heal() {
    let _guard = cluster_lock().lock().await;
    let cluster = Cluster::start().await;
    let controller = cluster.broker(1).wait_until_controller_leader().await.0;
    let mut others = Cluster::nodes().filter(|node| *node != controller);
    let (leader, other) = (
        others.next().expect("a node"),
        others.next().expect("a node"),
    );
    let topic = dedup_topic()
        .cluster(&cluster)
        .name("isolated-controller")
        .replicas(&[leader, other, controller])
        .call()
        .await;
    assert!(produce(&cluster, leader, &topic, &["c0", "c1"], AllReplicas).await == 0);

    cluster.links().isolate(controller);
    until(
        format!("brokers {leader} and {other} elect a controller other than {controller}"),
        || {
            [leader, other]
                .iter()
                .all(|node| {
                    let elected = *cluster.broker(*node).watch_leader_for_test().borrow();
                    elected.is_some_and(|id| id != NodeId(0) && id != NodeId(controller))
                })
                .then_some(())
        },
    )
    .await;
    // The new controller fences the isolated node and shrinks the ISR; the
    // partition keeps its leader.
    await_isr(&cluster, &topic.name, &[leader, other], &[leader, other]).await;
    let during = produce(&cluster, leader, &topic, &["d0", "c0"], AllReplicas).await;
    // Switch to header identities while the old controller cannot hear it.
    set_key_header(&cluster, leader, &topic.name, Some("dedup-id")).await;
    await_key_header(&cluster, &topic.name, &[leader, other], Some("dedup-id")).await;
    let by_header = produce_identified(&cluster, leader, &topic, &[
        ("h1", "id-1"),
        ("h2", "id-1"),
        ("h3", "id-2"),
    ])
    .await;
    let leader_held = cluster
        .broker(leader)
        .partition_record_for_test(&topic.name, 0)
        .map(|partition| partition.leader.0);
    assert!((during, by_header, leader_held) == (2, 4, Some(leader)));

    cluster.links().heal(controller);
    let all: Vec<u64> = Cluster::nodes().collect();
    await_key_header(&cluster, &topic.name, &all, Some("dedup-id")).await;
    let healed = offsets_and_keys(&converged(&cluster, &topic.name).await);
    assert!(healed == at(&[(0, "c0"), (1, "c1"), (2, "d0"), (4, "h1"), (6, "h3")]));

    // Back to key identities, through the node that was cut off.
    set_key_header(&cluster, controller, &topic.name, None).await;
    await_key_header(&cluster, &topic.name, &all, None).await;
    assert!(produce(&cluster, leader, &topic, &["d0", "d1"], AllReplicas).await == 7);
    let last = offsets_and_keys(&converged(&cluster, &topic.name).await);
    assert!(
        last == at(&[
            (0, "c0"),
            (1, "c1"),
            (2, "d0"),
            (4, "h1"),
            (6, "h3"),
            (8, "d1")
        ])
    );
    assert_indexes_match(&cluster, &topic.name, &last).await;

    // A topic created through the formerly isolated node, which leads it.
    let led = dedup_topic()
        .cluster(&cluster)
        .name("led-by-the-old-controller")
        .replicas(&[controller, leader, other])
        .call()
        .await;
    assert!(produce(&cluster, controller, &led, &["e0", "e0", "e1"], AllReplicas).await == 0);
    let led_log = offsets_and_keys(&converged(&cluster, &led.name).await);
    assert!(led_log == at(&[(0, "e0"), (2, "e1")]));
    assert_indexes_match(&cluster, &led.name, &led_log).await;
    cluster.finish().await;
}
