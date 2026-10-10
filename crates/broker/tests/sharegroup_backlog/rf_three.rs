//! The RF=3 case: a share coordinator that is not the data partition's leader
//! must sample the committed high watermark, never the leader's log end offset.
//!
//! The setup searches the cluster image for an offsets partition whose
//! coordinator sits on a different broker than one of the data partitions, and
//! picks the group name that hashes onto that offsets partition — Kafka's own
//! `String.hashCode` placement, which `java_hash` reproduces. The third broker
//! is stopped so that a produce to the leader stays uncommitted, which is what
//! makes the two candidate samples differ.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_broker::metrics::ShareGroupLabel;
use krabka_protocol::{
    owned::{
        fetch_request::FetchPartition, fetch_response::FetchResponse,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    harness::{TOPIC, create_topic, scrape, test_lock},
    support,
    support::{
        client::connect_client,
        fetch::{fetch_partition, single_partition_fetch},
    },
};

const OFFSETS_TOPIC: &str = "__consumer_offsets";
const SHARE_STATE_TOPIC: &str = "__share_group_state";

fn share_coordinator_key(group: &str, topic_id: uuid::Uuid, partition: i32) -> String {
    format!(
        "{group}:{}:{partition}",
        URL_SAFE_NO_PAD.encode(topic_id.as_bytes())
    )
}

fn java_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn group_for_partition(partition: i32, partition_count: i32) -> String {
    (0..100)
        .map(|i| format!("backlog-rf3-{i}"))
        .find(|group| {
            let hash = java_hash(group);
            let positive = if hash == i32::MIN { 0 } else { hash.abs() };
            positive % partition_count == partition
        })
        .expect("each offsets partition receives a candidate group")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn rf_three_remote_leader_uses_committed_high_watermark() {
    let _guard = test_lock().lock().await;
    let mut attempt = 0;
    let mut cluster = loop {
        attempt += 1;
        match support::start_n_node_with(3, |_, config| {
            config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
            config.offsets_topic_num_partitions = 3;
            config.share_coordinator.state_topic_num_partitions = 1;
            config.share_group.backlog_poll_interval = Duration::from_millis(50);
            // Everything below the poll interval exists to keep the stopped
            // follower inside the ISR for the whole probe. That is what holds
            // the data leader's high watermark at 0 while its log end offset
            // runs on to 5, and the gap between those two numbers is the only
            // thing that tells a committed-HWM sample apart from a LEO sample.
            // Once the follower leaves the ISR the watermark catches up and
            // the distinction is gone -- so an ISR shrink does not merely
            // upset an assertion, it dissolves the fixture.
            //
            // Two independent clocks propose that shrink, and both used to be
            // shorter than the work between the shutdown and the sample:
            //
            // * `isr_maintenance` scans every `isr_scan_interval` (1s by test
            //   default) and drops any follower whose last fetch is older than
            //   `replica_lag_time_max`. `tokio::time::interval` fires its
            //   first tick immediately, so an hour-long interval means each
            //   broker scans exactly once, at startup, with an empty partition
            //   registry -- and never again while the test runs.
            // * The controller fails a broker over once its heartbeat session
            //   expires after `heartbeat_timeout` (2s by test default), which
            //   rewrites the partition with the dead node removed from the
            //   ISR. Ten minutes puts that expiry far beyond every bounded
            //   wait the test performs after the shutdown -- the longest is a
            //   single 30s `wait_for_metrics` -- so the test cannot reach an
            //   assertion on the far side of it.
            config.replica_lag_time_max = krabka_units::secs(30);
            config.isr_scan_interval = krabka_units::hours(1);
            config.heartbeat_timeout = krabka_units::minutes(10);
        })
        .await
        {
            Ok(cluster) => break cluster,
            Err(error) if attempt < 3 => {
                eprintln!("backlog RF=3 cluster start attempt {attempt} failed: {error}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => panic!("backlog RF=3 cluster failed to start: {error}"),
        }
    };
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin = Arc::new(
        connect_client(
            cluster[0].0.listen_addr().to_string(),
            Some("backlog-rf3-admin"),
        )
        .await,
    );
    create_topic(&admin, 1, 3).await;
    for (broker, _, _) in &cluster {
        broker.wait_until_partition_present(TOPIC, 0).await;
    }
    let topic_id = cluster[0]
        .0
        .controller_image_for_test()
        .topic(TOPIC)
        .expect("topic metadata")
        .topic_id;
    // No broker creates `__consumer_offsets` or `__share_group_state` when it
    // starts. The candidate search below reads both topics out of the image,
    // and the group and share coordinators of every broker must serve.
    for (broker, _, _) in &cluster {
        broker.wait_until_group_coordinator_ready().await;
        broker.wait_until_share_coordinator_ready().await;
    }
    support::find_coordinator(
        &admin,
        support::KEY_TYPE_SHARE,
        &share_coordinator_key("backlog-rf3-bootstrap", topic_id, 0),
    )
    .await;
    for (broker, _, _) in &cluster {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, 0)
            .await;
    }

    let image = cluster[0].0.controller_image_for_test();
    let offsets_partitions = image.topic_partition_count(OFFSETS_TOPIC);
    let state_leader = image
        .partition(SHARE_STATE_TOPIC, 0)
        .expect("share-state partition 0")
        .leader;
    let controller_leader = cluster[0].0.controller_leader_id();
    let nodes = [
        krabka_broker::NodeId(1),
        krabka_broker::NodeId(2),
        krabka_broker::NodeId(3),
    ];
    let mut candidates = Vec::new();
    for offsets_partition in 0..offsets_partitions {
        let coordinator_id = image
            .partition(OFFSETS_TOPIC, offsets_partition)
            .expect("offsets partition")
            .leader;
        for data_partition in 0..1 {
            let data_leader_id = image
                .partition(TOPIC, data_partition)
                .expect("data partition")
                .leader;
            if coordinator_id == data_leader_id {
                continue;
            }
            let stopped_id = nodes
                .into_iter()
                .find(|id| *id != coordinator_id && *id != data_leader_id)
                .expect("third broker");
            if stopped_id != state_leader {
                candidates.push((
                    offsets_partition,
                    coordinator_id,
                    data_partition,
                    data_leader_id,
                    stopped_id,
                ));
            }
        }
    }
    let candidate = candidates
        .iter()
        .find(|candidate| Some(candidate.4) != controller_leader)
        .or_else(|| candidates.first())
        .copied()
        .expect("remote data leader with a live share-state leader");
    let (offsets_partition, coordinator_id, data_partition, data_leader_id, stopped_id) = candidate;
    let leader_epoch = image
        .partition(TOPIC, data_partition)
        .expect("data partition metadata")
        .leader_epoch;
    drop(image);
    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    let group_id = group_for_partition(offsets_partition, offsets_partitions);
    let coordinator_client = Arc::new(
        connect_client(
            cluster[coordinator_index].0.listen_addr().to_string(),
            Some("backlog-rf3-coordinator"),
        )
        .await,
    );
    support::find_coordinator(
        &coordinator_client,
        support::KEY_TYPE_SHARE,
        &share_coordinator_key(&group_id, topic_id, data_partition),
    )
    .await;
    let joined = coordinator_client
        .send(ShareGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id: uuid::Uuid::new_v4().to_string(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    let member_id = joined.member_id.expect("the broker echoes the member id");
    let mut member_epoch = joined.member_epoch;
    let initialized = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let heartbeat = coordinator_client
                .send(ShareGroupHeartbeatRequest {
                    group_id: group_id.clone(),
                    member_id: member_id.clone(),
                    member_epoch,
                    subscribed_topic_names: Some(vec![TOPIC.into()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(heartbeat.error_code == 0, "{heartbeat:?}");
            member_epoch = heartbeat.member_epoch;
            let mut present = false;
            for (broker, _, _) in &cluster {
                present |= broker
                    .share_state_summary_for_test(&group_id, topic_id, data_partition)
                    .await
                    .is_some();
            }
            if present {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(initialized.is_ok(), "RF=3 share state did not initialize");

    let label = ShareGroupLabel {
        group_id: group_id.clone(),
        topic: TOPIC.into(),
        partition: data_partition,
    };
    // `get` does not create the series: only a sample of an initialized
    // partition does. The group coordinator records the partition as
    // initialized only when its write to `__consumer_offsets` commits, after
    // the share state commits. The follower stays up until that point.
    cluster[coordinator_index]
        .0
        .wait_for_metrics("initial RF=3 backlog sample", |metrics| {
            metrics
                .share_group_backlog
                .get(&label)
                .is_some_and(|backlog| backlog.get() == 0)
        })
        .await;

    let stopped_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == stopped_id)
        .unwrap();
    let (stopped, _, _) = cluster.remove(stopped_index);
    stopped.shutdown().await;

    let data_leader_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == data_leader_id)
        .unwrap();
    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "stopped follower must remain in the ISR during the probe"
    );
    let last = cluster[data_leader_index]
        .0
        .produce_records_for_test(TOPIC, data_partition, 5)
        .await
        .unwrap();
    assert!(last == 4);
    assert!(
        cluster[data_leader_index]
            .0
            .local_log_end_offset(TOPIC, data_partition)
            == Some(5)
    );

    let data_client = connect_client(
        cluster[data_leader_index].0.listen_addr().to_string(),
        Some("backlog-rf3-data"),
    )
    .await;
    let fetched: FetchResponse = data_client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: TOPIC.into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partition: FetchPartition {
                    current_leader_epoch: leader_epoch.0,
                    ..fetch_partition(crate::support::fetch::FetchPartitionSetup {
                        partition: krabka_ids::PartitionIndex(data_partition),
                        offset: krabka_ids::Offset(i64::MAX),
                        maximum: crate::support::fetch::FetchByteLimit(0),
                    })
                },
                limits: crate::support::fetch::FetchLimits {
                    wait: crate::support::fetch::RequestWaitMillis(0),
                    minimum: crate::support::fetch::FetchByteLimit(0),
                    maximum: crate::support::fetch::FetchByteLimit(0),
                },
            },
        ))
        .await
        .unwrap();
    let partition = &fetched.responses[0].partitions[0];
    assert!(partition.error_code == 0, "{fetched:?}");
    assert!(partition.high_watermark == 0, "{fetched:?}");

    // A sentinel makes the poll completion observable: the next real sample
    // must replace it with HWM(0) - SPSO/log-start(0), not the leader LEO(5).
    cluster[coordinator_index]
        .0
        .metrics()
        .share_group_backlog
        .get_or_create(&label)
        .set(99);
    cluster[coordinator_index]
        .0
        .wait_for_metrics("remote RF=3 committed-HWM sample", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "the committed-HWM sample must land before ISR shrink can expose LEO"
    );

    let expected = format!(
        "krabka_broker_share_group_backlog{{group_id=\"{group_id}\",topic=\"{TOPIC}\",partition=\"{data_partition}\"}} 0"
    );
    assert!(
        scrape(cluster[coordinator_index].0.metrics_addr().unwrap())
            .await
            .contains(&expected)
    );
    for (index, (broker, _, _)) in cluster.iter().enumerate() {
        if index != coordinator_index {
            assert!(
                !scrape(broker.metrics_addr().unwrap())
                    .await
                    .contains(&format!("group_id=\"{group_id}\"")),
                "non-coordinator emitted a backlog series"
            );
        }
    }

    crate::support::shutdown_cluster(cluster).await;
}
