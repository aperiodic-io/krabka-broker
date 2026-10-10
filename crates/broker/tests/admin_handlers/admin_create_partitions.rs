//! `CreatePartitions` (`api_key` 37): extending a topic's partition count, both
//! with broker-chosen placement and with an explicit `assignments` list, and
//! the `INVALID_REPLICA_ASSIGNMENT` path that must add no partition at all.

use assert2::assert;
use krabka_protocol::owned::create_partitions_request::{
    CreatePartitionsRequest, CreatePartitionsTopic,
};

use crate::{
    admin_harness::{build_client, create_topic_helper},
    support::topics::{creatable_topic, create_topic_request},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_placement_takes_a_fenced_broker_only_as_a_last_resort() {
    let mut cluster = crate::support::registered_cluster(3).await;
    // Stop a broker that is neither the one the test talks to nor the
    // controller leader. Its controlled shutdown ends fenced, as Kafka's
    // `processBrokerHeartbeat` fences a broker that may shut down, and it
    // never heartbeats again to unfence.
    let leader = cluster[0].0.controller_leader_id();
    let stopped_index = if leader == Some(krabka_broker::NodeId(3)) {
        1
    } else {
        2
    };
    let (stopped, stopped_cfg, _stopped_dir) = cluster.remove(stopped_index);
    let fenced = stopped_cfg.node_id;
    stopped.shutdown().await;
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;
    broker
        .wait_for_image(|_| broker.fenced_broker_ids_for_test().contains(&fenced.0))
        .await;

    let created = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("t-usable-brokers").into(),
                replicas: crate::support::topics::TopicReplicationFactor(2),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(created.topics[0].error_code == 0);
    broker
        .wait_until_partition_present("t-usable-brokers", 0)
        .await;

    let expanded = client
        .send(CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: "t-usable-brokers".into(),
                count: 3,
                assignments: None,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(expanded.results[0].error_code == 0);

    for partition in 0..3 {
        broker
            .wait_until_partition_present("t-usable-brokers", partition)
            .await;
        let record = broker
            .partition_record_for_test("t-usable-brokers", partition)
            .unwrap();
        assert!(!record.replicas.contains(&fenced));
        assert!(!record.isr.contains(&fenced));
    }

    // Kafka's placer counts a fenced broker toward the replication factor and
    // takes it last, so a replication factor of 3 on the 3 registered brokers
    // succeeds. The fenced replica stays out of the ISR and never leads.
    let last_resort = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("t-last-resort").into(),
                replicas: crate::support::topics::TopicReplicationFactor(3),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(last_resort.topics[0].error_code == 0);
    broker
        .wait_until_partition_present("t-last-resort", 0)
        .await;
    let record = broker
        .partition_record_for_test("t-last-resort", 0)
        .unwrap();
    assert!(record.replicas.len() == 3);
    assert!(record.replicas[2] == fenced);
    assert!(record.isr == record.replicas[..2]);
    assert!(record.leader == record.replicas[0]);

    let rejected = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("t-too-many-replicas").into(),
                replicas: crate::support::topics::TopicReplicationFactor(4),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(rejected.topics[0].error_code == 38);
}

/// `CreatePartitions`: a request that extends a 1-partition topic to 3
/// returns `error_code == 0`. All three partitions then materialise in the
/// broker's local registry within a few seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_extends_topic() {
    let (cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    create_topic_helper(&client, "t-cp", 1).await;

    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cp".into(),
            count: 3,
            assignments: None,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("create_partitions");
    assert!(
        resp.results[0].error_code == 0,
        "create_partitions result: {:?}",
        resp.results[0].error_message
    );

    // Wait for the supervisor reconcile to materialise all three partitions.
    for p in 0..3 {
        broker.wait_until_partition_present("t-cp", p).await;
    }
}

/// `CreatePartitions`: explicit `assignments` list. The topic's rf is 1 on a
/// single-broker cluster, and the operator pins the new partition to
/// broker 0. The handler must accept it (`error_code` == 0) and materialise
/// the partition. A second call with a wrong-length assignment list must
/// return `INVALID_REPLICA_ASSIGNMENT` (39).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_partitions_honors_explicit_assignments() {
    use krabka_protocol::owned::create_partitions_request::CreatePartitionsAssignment;

    let (cluster, client) = crate::support::start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    create_topic_helper(&client, "t-cpa", 1).await;

    // Happy path: 1 existing partition → 2 partitions, explicit assignment
    // pins broker 0 (the only one available).
    let req = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 2,
            assignments: Some(vec![CreatePartitionsAssignment {
                broker_ids: vec![1],
                ..Default::default()
            }]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let resp = client
        .send(req)
        .await
        .expect("create_partitions (explicit)");
    assert!(
        resp.results[0].error_code == 0,
        "explicit assignment must succeed: {:?}",
        resp.results[0].error_message
    );

    // Wait for the new partition to materialise.
    broker.wait_until_partition_present("t-cpa", 1).await;

    // Invalid path: ask for 1 more partition (total 3) but supply 2
    // assignments. Must surface INVALID_REPLICA_ASSIGNMENT and NOT add a
    // partition.
    let bad = CreatePartitionsRequest {
        topics: vec![CreatePartitionsTopic {
            name: "t-cpa".into(),
            count: 3,
            assignments: Some(vec![
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
                CreatePartitionsAssignment {
                    broker_ids: vec![1],
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        validate_only: false,
        ..Default::default()
    };
    let bad_resp = client
        .send(bad)
        .await
        .expect("create_partitions (length-mismatch)");
    assert!(
        bad_resp.results[0].error_code == 39,
        "length-mismatch must return INVALID_REPLICA_ASSIGNMENT (39): {:?}",
        bad_resp.results[0].error_message
    );
    assert!(
        !broker.partition_exists_for_test("t-cpa", 2),
        "partition 2 must NOT have been created on an INVALID_REPLICA_ASSIGNMENT path",
    );
}
