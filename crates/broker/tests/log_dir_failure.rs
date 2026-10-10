//! A follower whose log directory fails under live replication leaves the ISR
//! and stays alive.
//!
//! This is Kafka's `LogDirFailureTest.test_replication_with_disk_failure` for
//! a follower, in process. One controller-only node and three broker-only
//! nodes run with two log directories each. The test makes the second log
//! directory of one follower read-only, as the system test does with
//! `chmod a-w -R`. A file that is already open stays writable, so the follower
//! sees the failure only when it creates a file there. The nodes take Kafka's
//! `log.roll.ms` from their `server.properties` keys, as the system test sets
//! it, so the next segment roll creates that file.
//!
//! KIP-112 and KIP-858 then apply. The follower marks the directory offline,
//! stops following the partition, and reports the directory in its
//! heartbeat. The controller removes the follower from the ISR and removes the
//! directory from the follower's registration. The leader's own lag check
//! cannot do it in this test, because the nodes wait ten minutes before they
//! call a follower slow.

use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{
    BootstrapMode, BrokerConfig, BrokerHandle, config::NodeRole, file_config::FileConfig,
    log_dir_id::LogDirIds,
};
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use tempfile::TempDir;

mod support;

/// The topic that takes the first placement on every broker, so the topic
/// under test lands in the second log directory, as in the system test.
const FILLER: &str = "ldf-filler";
/// The topic whose follower replica loses its log directory.
const TOPIC: &str = "ldf-target";
/// The `log.roll.ms` every broker runs with: shorter than the gap between the
/// record timestamps the test produces, so every append rolls a segment.
const LOG_ROLL_MS: &str = "100";
/// How far apart the produced record timestamps are, in milliseconds.
const RECORD_GAP_MS: i64 = 1_000;

/// One broker-only node and its two log directories.
struct BrokerNode {
    handle: BrokerHandle,
    primary: TempDir,
    extra: TempDir,
}

impl BrokerNode {
    /// The log directory that holds `partition`, if this node hosts it.
    fn dir_of(&self, partition: &str) -> Option<&Path> {
        [self.primary.path(), self.extra.path()]
            .into_iter()
            .find(|dir| dir.join(partition).is_dir())
    }
}

/// A booted role-separated cluster: node 1 is the controller-only voter, and
/// nodes 2, 3 and 4 are broker-only nodes with two log directories each.
struct Cluster {
    controller: BrokerHandle,
    brokers: Vec<BrokerNode>,
    _controller_dir: TempDir,
}

impl Cluster {
    async fn shutdown(self) {
        for broker in self.brokers {
            broker.handle.shutdown().await;
        }
        self.controller.shutdown().await;
    }
}

/// Boots the controller-only node, waits until it leads, then boots the three
/// broker-only nodes with Kafka's `log.roll.ms` and waits until all three are
/// registered.
async fn start_cluster() -> Cluster {
    const NODES: usize = 4;
    let (endpoints, mut data_listeners, mut ctrl_listeners) =
        support::single_controller_endpoints(NODES).await;
    let topology = endpoints.topology();

    let controller_dir = TempDir::new().unwrap();
    let ctrl_cfg = topology.config(
        controller_dir.path(),
        crate::support::RoleNodeSetup {
            role: NodeRole::Controller,
            ..Default::default()
        },
    );
    let controller = support::start_held_node(
        ctrl_cfg,
        &mut ctrl_listeners,
        &mut data_listeners,
        "controller-only start",
    )
    .await;
    controller.wait_until_controller_leader().await;

    let mut brokers = Vec::with_capacity(NODES - 1);
    for index in 1..NODES {
        let primary = TempDir::new().unwrap();
        let extra = TempDir::new().unwrap();
        let mut cfg = topology.config(
            primary.path(),
            crate::support::RoleNodeSetup {
                index: crate::support::NodeIndex(index),
                mode: BootstrapMode::Join,
                ..Default::default()
            },
        );
        cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
        cfg.replica_lag_time_max = krabka_units::minutes(10);
        apply_server_properties(&mut cfg);
        let handle = support::start_held_node(
            cfg,
            &mut ctrl_listeners,
            &mut data_listeners,
            "broker-only start",
        )
        .await;
        brokers.push(BrokerNode {
            handle,
            primary,
            extra,
        });
    }
    controller.wait_until_brokers_registered(NODES - 1).await;
    Cluster {
        controller,
        brokers,
        _controller_dir: controller_dir,
    }
}

/// Applies the `server.properties` keys the system test gives every broker
/// and that this test depends on, through the loader the broker binary uses.
fn apply_server_properties(cfg: &mut BrokerConfig) {
    FileConfig {
        server_properties: BTreeMap::from([("log.roll.ms".to_owned(), LOG_ROLL_MS.to_owned())]),
        ..FileConfig::default()
    }
    .apply_to(cfg)
    .expect("server properties");
}

/// Waits until every broker holds `partition` in one of its log directories.
async fn wait_until_materialized(brokers: &[BrokerNode], partition: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !brokers
            .iter()
            .all(|broker| broker.dir_of(partition).is_some())
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{partition} was not materialized on every broker"));
}

/// The directory id `krabka format` gave `dir`.
fn directory_id(dir: &Path) -> uuid::Uuid {
    LogDirIds::resolve(&[dir.to_path_buf()])
        .id_for(dir)
        .expect("a formatted log directory has an id")
}

/// `chmod a-w -R` over a directory tree, undone when dropped so the temporary
/// directory can be removed.
struct ReadOnlyTree {
    paths: Vec<(PathBuf, u32)>,
}

impl ReadOnlyTree {
    fn new(root: &Path) -> Self {
        let mut paths = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            if path.is_dir() {
                pending.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
            }
            paths.push((path, mode));
        }
        for (path, mode) in &paths {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & !0o222)).unwrap();
        }
        Self { paths }
    }

    /// False when this process can create a file under the tree anyway, as
    /// root can, and the test cannot make the directory fail.
    fn refuses_writes(&self) -> bool {
        let probe = self.paths[0].0.join(".write-probe");
        let created = std::fs::write(&probe, b"probe").is_ok();
        if created {
            let _ = std::fs::remove_file(probe);
        }
        !created
    }
}

impl Drop for ReadOnlyTree {
    fn drop(&mut self) {
        for (path, mode) in &self.paths {
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(*mode));
        }
    }
}

/// Produces one record to partition 0 of [`TOPIC`] with the timestamp
/// `timestamp_ms`, and waits for the leader to acknowledge it.
async fn produce(producer: &Producer, timestamp_ms: i64) {
    producer
        .send(ProducerRecord {
            timestamp_ms: Some(timestamp_ms),
            ..crate::support::producer::producer_record(
                crate::support::producer::ProducerRecordSetup {
                    topic: TOPIC.to_owned(),
                    partition: Some(krabka_ids::PartitionIndex(0)),
                    value: Some(Bytes::from_static(b"log-dir-failure")),
                    ..Default::default()
                },
            )
        })
        .await
        .expect("the leader acknowledges the record");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_whose_log_directory_fails_leaves_the_isr() {
    support::init_tracing();
    let cluster = start_cluster().await;
    crate::support::client::create_topic_for_broker(
        &cluster.brokers[0].handle,
        crate::support::topics::CreateTopicSetup {
            topic: FILLER,
            replication_factor: crate::support::topics::TopicReplicationFactor(3),
            ..Default::default()
        },
    )
    .await;
    wait_until_materialized(&cluster.brokers, &format!("{FILLER}-0")).await;
    crate::support::client::create_topic_for_broker(
        &cluster.brokers[0].handle,
        crate::support::topics::CreateTopicSetup {
            topic: TOPIC,
            replication_factor: crate::support::topics::TopicReplicationFactor(3),
            ..Default::default()
        },
    )
    .await;
    let target = format!("{TOPIC}-0");
    wait_until_materialized(&cluster.brokers, &target).await;

    let record = cluster
        .controller
        .partition_record_for_test(TOPIC, 0)
        .expect("the partition is in the image");
    let leader = record.leader.0;
    // The system test fails the second log directory and checks first that
    // the partition lives there. An internal topic can take the first
    // placement on one broker, so pick a follower where the premise holds.
    let follower = cluster
        .brokers
        .iter()
        .find(|broker| {
            broker.handle.node_id() != leader && broker.dir_of(&target) == Some(broker.extra.path())
        })
        .expect("a follower holds the partition in its second log directory");
    let follower_id = follower.handle.node_id();
    let primary_id = directory_id(follower.primary.path());
    let failed_id = directory_id(follower.extra.path());
    let slot = record
        .replicas
        .iter()
        .position(|replica| replica.0 == follower_id)
        .expect("the follower is a replica");
    // KIP-858: the controller can map the failed directory to the partition
    // only once the follower has reported where the replica lives.
    cluster
        .controller
        .wait_for_image(|image| {
            image
                .partition(TOPIC, 0)
                .is_some_and(|partition| partition.directories.get(slot) == Some(&failed_id))
        })
        .await;

    let producer = Producer::builder()
        .bootstrap(cluster.brokers[0].handle.listen_addr().to_string())
        .acks(Acks::One)
        .build()
        .await
        .expect("producer");
    let first_timestamp = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    produce(&producer, first_timestamp).await;
    follower
        .handle
        .wait_until_local_log_end_offset(TOPIC, 0, 1)
        .await;

    let read_only = ReadOnlyTree::new(follower.extra.path());
    if !read_only.refuses_writes() {
        // Root writes through the permission bits, so nothing fails.
        drop(read_only);
        cluster.shutdown().await;
        return;
    }
    // The next record is a second past the first. The follower rolls the
    // segment to append it, and the roll creates a file in the read-only
    // directory.
    produce(&producer, first_timestamp + RECORD_GAP_MS).await;

    let mut survivors: Vec<u64> = record
        .replicas
        .iter()
        .map(|replica| replica.0)
        .filter(|replica| *replica != follower_id)
        .collect();
    survivors.sort_unstable();
    cluster
        .controller
        .wait_for_image(|image| {
            image.partition(TOPIC, 0).is_some_and(|partition| {
                !partition.isr.iter().any(|replica| replica.0 == follower_id)
            })
        })
        .await;
    let image = cluster.controller.controller_image_for_test();
    let partition = image.partition(TOPIC, 0).expect("the partition");
    let mut isr: Vec<u64> = partition.isr.iter().map(|replica| replica.0).collect();
    isr.sort_unstable();
    let registered_dirs = image
        .broker(krabka_raft::NodeId(follower_id))
        .expect("the follower stays registered")
        .log_dirs
        .clone();
    assert!(
        (
            partition.leader.0,
            isr,
            registered_dirs,
            cluster.controller.broker_alive_for_test(follower_id).await,
        ) == (leader, survivors, vec![primary_id], true)
    );

    drop(producer);
    drop(read_only);
    cluster.shutdown().await;
}
