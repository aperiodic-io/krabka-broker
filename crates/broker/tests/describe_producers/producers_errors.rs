//! The per-partition error paths, where the response still carries a row for
//! every partition the request named.
//!
//! An unknown topic and an out-of-range index both report
//! `UNKNOWN_TOPIC_OR_PARTITION`, while a partition the broker knows about but
//! does not host reports `NOT_LEADER_OR_FOLLOWER`.

use assert2::{assert, check};

use crate::{producers_harness::create_topic, support};

#[tokio::test]
async fn unknown_topic_returns_unknown_topic_or_partition() {
    let p = support::start().await;

    let resp = p
        .client
        .send(crate::support::admin::describe_producers_request(
            "ghost".into(),
            vec![0, 1],
        ))
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 2);
    for part in &resp.topics[0].partitions {
        assert!(
            part.error_code == 3,
            "unknown topic must surface UNKNOWN_TOPIC_OR_PARTITION (3) per partition, got {part:?}"
        );
        assert!(part.active_producers.is_empty());
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_range_partition_returns_unknown_topic_or_partition() {
    let p = support::start().await;
    create_topic(
        &p.client,
        crate::support::topics::CreateTopicSetup {
            topic: "small",
            ..Default::default()
        },
    )
    .await;

    // Partition 5 doesn't exist (topic was created with 1 partition).
    let resp = p
        .client
        .send(crate::support::admin::describe_producers_request(
            "small".into(),
            vec![0, 5],
        ))
        .await
        .expect("DescribeProducers");

    assert!(resp.topics[0].partitions.len() == 2);
    // Partition 0 exists → error_code 0.
    let p0 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .expect("p0");
    assert!(p0.error_code == 0, "{p0:?}");
    // Partition 5 doesn't → UNKNOWN_TOPIC_OR_PARTITION.
    let p5 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 5)
        .expect("p5");
    assert!(p5.error_code == 3, "{p5:?}");

    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_known_partition_not_hosted_locally_returns_not_leader() {
    let cluster = crate::support::registered_cluster(2).await;

    let admin = crate::support::client::connect_with_context(
        cluster[0].0.listen_addr().to_string(),
        None,
        "admin client",
    )
    .await;
    create_topic(
        &admin,
        crate::support::topics::CreateTopicSetup {
            topic: "remote",
            ..Default::default()
        },
    )
    .await;
    cluster[0].0.wait_until_partition_present("remote", 0).await;

    let leader = cluster[0]
        .0
        .partition_leader_for_test("remote", 0)
        .expect("remote-0 leader");
    let nonleader = cluster
        .iter()
        .find(|(broker, _, _)| broker.node_id() != leader)
        .expect("two-node cluster has a nonleader");
    nonleader.0.wait_until_partition_present("remote", 0).await;
    assert!(
        !nonleader.0.partition_exists_for_test("remote", 0),
        "rf=1 nonleader must not host remote-0"
    );

    let client = crate::support::client::connect_with_context(
        nonleader.0.listen_addr().to_string(),
        None,
        "nonleader client",
    )
    .await;

    let resp = client
        .send(crate::support::admin::describe_producers_request(
            "remote".into(),
            vec![0],
        ))
        .await
        .expect("DescribeProducers");

    let partition = &resp.topics[0].partitions[0];
    assert!(partition.partition_index == 0);
    check!(partition.error_code == 6, "expected NOT_LEADER_OR_FOLLOWER");
    check!(partition.active_producers.is_empty());

    crate::support::shutdown_cluster(cluster).await;
}
