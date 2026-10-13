//! KIP-112 / KIP-858 `offlineReplicas` reporting.
//!
//! `kafka-topics --describe --unavailable-partitions` and
//! `--under-replicated-partitions`, Cruise Control and every `AdminClient`
//! dashboard read `offlineReplicas` off `Metadata` and
//! `DescribeTopicPartitions`. This suite boots a single broker over two log
//! dirs, spreads a topic across both, flips one dir offline through the
//! log-dir registry, and asserts that the whole partition row of both
//! responses names the replica on the dead disk — and that a partition on the
//! surviving disk still reports none.
//!
//! The same row also has to stop calling that replica a leader and an in-sync
//! member, because those are the two columns the tools actually filter on.
//! `crates/broker/tests/unavailable_partitions_jvm.rs` drives the real
//! `kafka-topics` over this behaviour and compares it against Apache Kafka.
//!
//! The other half of Kafka's rule is the fencing state
//! (`KRaftMetadataCache.isReplicaOffline` is `fenced() || !hasOnlineDir(dir)`).
//! Only the controller leader holds the heartbeat registry that decides it, so
//! the second test boots a three-node cluster, kills a broker, and asks a node
//! that is *not* the controller the same question. Clients reach whichever
//! broker they are bootstrapped at, so the two answers have to agree.

mod kafka_wire;

use {
    crate::support::{
        discovery::named_topic_metadata, registered_cluster, shutdown_cluster,
        storage::two_dir_config, wire::ApiRequestSetup,
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            describe_topic_partitions_request::{DescribeTopicPartitionsRequest, TopicRequest},
            describe_topic_partitions_response::{
                DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponsePartition,
            },
            metadata_request::MetadataRequest,
            metadata_response::{MetadataResponse, MetadataResponsePartition},
        },
    },
    std::{collections::HashSet, net::SocketAddr, time::Duration},
    tempfile::TempDir,
    tokio::net::TcpStream,
};

const CLIENT_ID: &str = "krabka-offline-replicas-test";
const TOPIC: &str = "kip112-offline-replicas";
/// Six partitions spread across both dirs under the least-loaded placement
/// algorithm; `jbod.rs` relies on the same premise.
const PARTITIONS: i32 = 6;
const BROKER_ID: i32 = 1;
/// Kafka's `MetadataResponse.NO_LEADER_ID`, which `kafka-topics` prints as
/// `Leader: none`.
const NO_LEADER_ID: i32 = -1;
/// `LEADER_NOT_AVAILABLE`, the code Kafka's `Metadata` carries beside a `-1`
/// leader.
const LEADER_NOT_AVAILABLE: i16 = 5;
const METADATA_VERSION: i16 = 12;
const DESCRIBE_TOPIC_PARTITIONS_VERSION: i16 = 0;
/// The heartbeat that carries the offline dir to the controller runs every
/// 200 ms under `BrokerConfig::for_tests`.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(15);

// Flexible headers and correlation ID 1 for every request in this suite.
krabka_macros::wire_test_driver!(flexible round_trip, CLIENT_ID, 1);

/// A running broker, the two log directories behind it, and its address.
struct TwoDirBroker {
    handle: BrokerHandle,
    primary: TempDir,
    extra: TempDir,
    addr: SocketAddr,
}

/// Boots one broker over `primary` + `extra`.
///
/// The address is reserved before the boot and advertised verbatim, because
/// this suite depends on the broker reaching *itself* over the inter-broker
/// path: the replicator supervisor's `AssignReplicasToDirs` report and the
/// heartbeat that carries the offline dir both dial the controller leader at
/// the endpoint its registration advertises. The `for_tests` default
/// advertises `127.0.0.1:0`, which no one can connect to.
async fn start_two_dir_broker() -> TwoDirBroker {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    // Hold the listeners until `start_with_listeners` adopts them, so a
    // concurrently running test binary cannot steal the port in between.
    let data_plane = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = data_plane.local_addr().unwrap();
    let controller_addr = controller.local_addr().unwrap();
    let mut cfg = two_dir_config(primary.path(), extra.path());
    cfg.listen_addr = addr;
    cfg.advertised_listener = addr.to_string();
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    let handle = Broker::start_with_listeners(cfg, Some(controller), [data_plane])
        .await
        .expect("broker start");
    TwoDirBroker {
        handle,
        primary,
        extra,
        addr,
    }
}

async fn create_topic(addr: SocketAddr) { create_topic_named(addr, TOPIC, PARTITIONS, 1).await; }

async fn create_topic_named(addr: SocketAddr, name: &str, partitions: i32, replication: i16) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    kafka_wire::create_topic_on(
        &mut stream,
        CLIENT_ID,
        kafka_wire::topic()
            .name((name).into())
            .partitions(partitions)
            .replicas(replication)
            .call(),
    )
    .await;
}

async fn metadata_partitions(addr: SocketAddr) -> Vec<MetadataResponsePartition> {
    metadata_partitions_of(addr, TOPIC).await
}

async fn metadata_partitions_of(addr: SocketAddr, name: &str) -> Vec<MetadataResponsePartition> {
    let req = MetadataRequest {
        allow_auto_topic_creation: false,
        ..named_topic_metadata(name.to_string())
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, METADATA_VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(METADATA_VERSION),
        },
        &body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = MetadataResponse::decode(&mut cur, METADATA_VERSION).unwrap();
    let topic = resp
        .topics
        .into_iter()
        .find(|t| t.name.as_deref() == Some(name))
        .expect("topic row in Metadata response");
    assert!(topic.error_code == 0);
    let mut partitions = topic.partitions;
    partitions.sort_by_key(|p| p.partition_index);
    partitions
}

async fn describe_topic_partitions(
    addr: SocketAddr,
) -> Vec<DescribeTopicPartitionsResponsePartition> {
    let req = DescribeTopicPartitionsRequest {
        topics: vec![TopicRequest {
            name: TOPIC.to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_TOPIC_PARTITIONS_VERSION)
        .unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(75),
            version: WireApiVersion(DESCRIBE_TOPIC_PARTITIONS_VERSION),
        },
        &body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeTopicPartitionsResponse::decode(&mut cur, DESCRIBE_TOPIC_PARTITIONS_VERSION)
        .unwrap();
    let topic = resp
        .topics
        .into_iter()
        .find(|t| t.name.as_deref() == Some(TOPIC))
        .expect("topic row in DescribeTopicPartitions response");
    assert!(topic.error_code == 0);
    let mut partitions = topic.partitions;
    partitions.sort_by_key(|p| p.partition_index);
    partitions
}

/// Polls `condition` against the live controller image until it holds.
async fn wait_for_image(
    handle: &BrokerHandle,
    what: &str,
    condition: impl Fn(&krabka_metadata::MetadataImage) -> bool,
) {
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if condition(&handle.controller_image_for_test()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("controller image never converged: {what}");
}

/// The partition indices of `TOPIC` whose replica sits on `directory`.
fn partitions_on_dir(handle: &BrokerHandle, directory: uuid::Uuid) -> Vec<i32> {
    let image = handle.controller_image_for_test();
    let mut indices: Vec<i32> = image
        .partitions_of(TOPIC)
        .filter(|p| p.directories.first() == Some(&directory))
        .map(|p| p.partition)
        .collect();
    indices.sort_unstable();
    indices
}

fn leader_epoch(handle: &BrokerHandle, partition: i32) -> i32 {
    handle
        .controller_image_for_test()
        .partition(TOPIC, partition)
        .expect("partition in image")
        .leader_epoch
        .0
}

#[tokio::test]
async fn offline_log_dir_is_reported_as_an_offline_replica() {
    let TwoDirBroker {
        handle,
        primary,
        extra,
        addr,
    } = start_two_dir_broker().await;
    handle.wait_until_controller_leader().await;
    create_topic(addr).await;
    for partition in 0..PARTITIONS {
        handle.wait_until_partition_present(TOPIC, partition).await;
    }

    // The replicator supervisor reports every local replica's owning dir with
    // `AssignReplicasToDirs`; until it has, `directories` holds the unassigned
    // sentinel and no replica can be attributed to a disk.
    wait_for_image(&handle, "all replica directories assigned", |image| {
        image
            .partitions_of(TOPIC)
            .filter(|p| p.directories.first().is_some_and(|d| !d.is_nil()))
            .count()
            == usize::try_from(PARTITIONS).unwrap()
    })
    .await;

    let ids = krabka_broker::log_dir_id::LogDirIds::resolve(&[
        primary.path().to_path_buf(),
        extra.path().to_path_buf(),
    ]);
    let extra_id = ids.id_for(extra.path()).expect("extra dir id");
    let primary_id = ids.id_for(primary.path()).expect("primary dir id");

    let doomed = partitions_on_dir(&handle, extra_id);
    let survivors = partitions_on_dir(&handle, primary_id);
    assert!(
        !doomed.is_empty() && !survivors.is_empty(),
        "test premise: JBOD placement must spread {TOPIC} across both dirs \
         (primary={survivors:?} extra={doomed:?})"
    );

    // Flip only the extra dir offline: the primary stays online, so the broker
    // does not take the all-dirs-offline self-shutdown path.
    assert!(
        handle.test_mark_log_dir_offline(extra.path()),
        "mark_offline must return true (dir was registered and online)"
    );

    // The heartbeat carries the offline dir to the controller, which retires
    // it from the broker registration's online set.
    wait_for_image(&handle, "offline dir retired from registration", |image| {
        image
            .broker(krabka_raft::NodeId(u64::try_from(BROKER_ID).unwrap()))
            .is_some_and(|b| b.log_dirs == vec![primary_id])
    })
    .await;

    let expected_offline: HashSet<i32> = doomed.iter().copied().collect();

    // The whole row, for both APIs. The sole replica of a doomed partition is
    // on the dead disk, so it is reported offline, it does not lead, and it is
    // not in-sync -- which is the shape Apache Kafka 4.3.1 answers for the
    // same cluster: `Leader: none  Replicas: 1  Isr:`. `Metadata` carries
    // `LEADER_NOT_AVAILABLE` beside the `-1`; `DescribeTopicPartitions` does
    // not. Its ELR columns are what `PartitionChangeBuilder` writes when the
    // last ISR member leaves: the replica is eligible, even at the default
    // `min.insync.replicas` of 1, and it is the last-known leader, which is
    // what `Leader: none` means to the controller.
    let metadata = metadata_partitions(addr).await;
    let expected_metadata: Vec<MetadataResponsePartition> = (0..PARTITIONS)
        .map(|partition| {
            let doomed = expected_offline.contains(&partition);
            MetadataResponsePartition {
                error_code: if doomed { LEADER_NOT_AVAILABLE } else { 0 },
                partition_index: partition,
                leader_id: if doomed { NO_LEADER_ID } else { BROKER_ID },
                leader_epoch: leader_epoch(&handle, partition),
                replica_nodes: vec![BROKER_ID],
                isr_nodes: if doomed { vec![] } else { vec![BROKER_ID] },
                offline_replicas: if doomed { vec![BROKER_ID] } else { vec![] },
                ..Default::default()
            }
        })
        .collect();
    assert!(metadata == expected_metadata);

    let described = describe_topic_partitions(addr).await;
    let expected_described: Vec<DescribeTopicPartitionsResponsePartition> = (0..PARTITIONS)
        .map(|partition| {
            let doomed = expected_offline.contains(&partition);
            let active_nodes = if doomed { vec![] } else { vec![BROKER_ID] };
            let leader = if doomed { NO_LEADER_ID } else { BROKER_ID };
            let epoch = leader_epoch(&handle, partition);
            let elr_nodes = if doomed { vec![BROKER_ID] } else { vec![] };
            DescribeTopicPartitionsResponsePartition {
                error_code: 0,
                partition_index: partition,
                leader_id: leader,
                leader_epoch: epoch,
                replica_nodes: vec![BROKER_ID],
                isr_nodes: active_nodes,
                eligible_leader_replicas: Some(elr_nodes.clone()),
                last_known_elr: Some(if doomed { vec![BROKER_ID] } else { vec![] }),
                offline_replicas: if doomed { vec![BROKER_ID] } else { vec![] },
                ..Default::default()
            }
        })
        .collect();
    assert!(described == expected_described);

    handle.shutdown().await;
}

/// The three-node suite below drives the fencing half of the projection.
mod support;

const FENCED_TOPIC: &str = "kip112-fenced-broker";
/// Broker 3 crashes; with a two-second heartbeat timeout and a one-second
/// liveness tick under `BrokerConfig::for_tests`, the controller notices,
/// publishes the fencing state and the follower applies it well inside this.
const FENCING_TIMEOUT: Duration = Duration::from_secs(30);

fn node_id_of(handle: &BrokerHandle) -> i32 {
    i32::try_from(handle.node_id()).expect("node id fits an i32")
}

/// The partition row `observer` must eventually serve for `FENCED_TOPIC`: the
/// leader, epoch, replicas and ISR its own image carries, with `dead` reported
/// offline.
fn expected_partition(observer: &BrokerHandle, dead: i32) -> MetadataResponsePartition {
    let image = observer.controller_image_for_test();
    let record = image
        .partition(FENCED_TOPIC, 0)
        .expect("partition in the observer's image");
    let ids = |nodes: &[krabka_metadata::NodeId]| -> Vec<i32> {
        nodes
            .iter()
            .map(|node| i32::try_from(node.0).expect("node id fits an i32"))
            .collect()
    };
    // A fenced broker keeps whatever seat the image still gives it: only a
    // replica on a dead *directory* is projected out of the leader and ISR
    // columns, because that is the one conclusion the controller cannot write
    // down. See `krabka_broker::handlers::offline_replicas`.
    MetadataResponsePartition {
        error_code: 0,
        partition_index: 0,
        leader_id: i32::try_from(record.leader.0).expect("node id fits an i32"),
        leader_epoch: record.leader_epoch.0,
        replica_nodes: ids(&record.replicas),
        isr_nodes: ids(&record.isr),
        offline_replicas: vec![dead],
        ..Default::default()
    }
}

/// A `Metadata` response served by a broker that is not the controller must
/// report the replicas of a dead broker offline. The fencing state lives in
/// the controller's heartbeat registry, so it only reaches this node if the
/// controller writes it to the metadata log.
#[tokio::test]
async fn a_follower_reports_the_replicas_of_a_fenced_broker_offline() {
    let mut cluster = registered_cluster(3).await;
    create_topic_named(cluster[0].0.listen_addr(), FENCED_TOPIC, 1, 3).await;
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(FENCED_TOPIC, 0).await;
    }

    // The controller leader answers this correctly out of its own registry.
    // The interesting node is one of the two that do not hold one: it serves
    // clients all the same.
    let (_leader, observer_index, victim_index) = support::two_controller_followers(&cluster).await;
    let victim_id = node_id_of(&cluster[victim_index].0);
    let observer_addr = cluster[observer_index].0.listen_addr();

    // The config and the log dir stay alive for the rest of the test: a
    // crashed broker's teardown must not race a deleted directory.
    let (victim, _victim_config, _victim_dir) = cluster.remove(victim_index);
    victim.crash_for_test().await;

    let observer = &cluster[observer_index].0;
    let deadline = tokio::time::Instant::now() + FENCING_TIMEOUT;
    loop {
        let expected = vec![expected_partition(observer, victim_id)];
        let actual = metadata_partitions_of(observer_addr, FENCED_TOPIC).await;
        if actual == expected {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker {victim_id} died but a non-controller node never reported \
             its replica offline: {actual:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The answer above is only worth anything if it came from a node that
    // does not hold the heartbeat registry.
    assert!(
        observer.controller_leader_id() != Some(krabka_raft::NodeId(observer.node_id())),
        "the observer must still be a follower for this test to mean anything"
    );

    shutdown_cluster(cluster).await;
}
