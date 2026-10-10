//! The whole disaster-recovery round trip, in the order an operator lives it.
//!
//! Every other suite here tests one stage. This one is the runbook in
//! `docs/operations/runbooks/restore-from-archive.md` executed end to end:
//!
//! 1. A cluster is up, a consumer group has committed offsets in it, and its
//!    node holds the two files a restore needs — the RLMM snapshot and the
//!    controller's metadata checkpoint.
//! 2. `krabka-backup capture` copies all three into the archive, and
//!    `krabka-backup verify` says the copy is whole.
//! 3. The cluster and its disks are destroyed.
//! 4. `krabka restore` rebuilds a log directory out of the archive, with both
//!    captured snapshots, and a broker boots on it.
//! 5. The restored cluster has the archived records and the restored topic
//!    configuration, and NO committed offsets: `__consumer_offsets` is
//!    compacted and never tiered, so nothing in the archive holds them. A
//!    consumer here would start from `auto.offset.reset`.
//! 6. `krabka-backup restore-offsets` commits the captured offsets, and the
//!    group's position is the position it had before the disaster.
//!
//! Step 5 is the assertion that matters most, because it is the failure this
//! suite exists to keep fixed: it fails if committed offsets ever start
//! arriving with the log, which would make step 6 pass for the wrong reason.
//!
//! The suite is hermetic. It builds the archive with the same `LocalTieredStorage`
//! fixture the other round-trip suites use, keeps the archive and the capture in
//! temp directories, and boots the broker in process, so it needs no container
//! and runs in the default `bazel test //...` set.

// The three modules below are the round-trip suite's own, included by path.
// This binary uses one archive and one partition of it, so most of what they
// offer is unused here, the same way `crates/broker/tests/support` is unused in
// part by every binary that includes it.
#[path = "support/topic_configuration.rs"]
mod topic_configuration;

#[path = "support/archive_fixture.rs"]
mod archive_fixture;
#[path = "roundtrip/args.rs"]
mod args;
#[allow(dead_code)]
#[path = "roundtrip/batches.rs"]
mod batches;
#[allow(dead_code)]
#[path = "roundtrip/fixture.rs"]
mod fixture;

use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_backup::{archive::ArchiveArgs, capture::capture_key, manifest::RLMM_SNAPSHOT, run};
use krabka_broker::{BrokerConfig, BrokerHandle};
use krabka_client_admin::AdminClient;
use krabka_client_core::{
    Client, CoordinatorKeyType, build_find_coordinator, coordinator_endpoint,
};
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, TopicConfigRecord, TopicRecord};
use krabka_remote_storage::{PartitionDump, RlmmCacheDump, TopicIdPartition};
use krabka_remote_storage_topic::Snapshot;
use krabka_restore::restore;
use uuid::Uuid;

use crate::{
    args::ControllerListener,
    fixture::{Fixture, build_fixture},
};

krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// The group whose position must survive the disaster.
const GROUP: &str = "orders-consumers";

/// The topic the group reads.
const TOPIC: &str = "orders";

/// The partition the group has a position in.
const PARTITION: i32 = 0;

/// Where the group had got to. `orders-0` archives offsets 0, 1 and 2, so a
/// group resuming at 2 has one record left to read and a group that fell back
/// to `auto.offset.reset=earliest` would read three.
const COMMITTED_OFFSET: i64 = 2;

/// A topic config that only the metadata checkpoint can carry back.
const RETENTION_MS: &str = "604800000";

/// What `OffsetFetch` answers for a partition with no committed offset. It is
/// the value that makes a consumer fall back to `auto.offset.reset`.
const NO_COMMITTED_OFFSET: i64 = -1;

/// Build the RLMM snapshot the fixture's own archive is consistent with.
///
/// A restore reconciles the bucket scan against this file and stops on any
/// disagreement, so it has to name every archived segment and no other.
fn rlmm_snapshot(fixture: &Fixture) -> Vec<u8> {
    let partitions = fixture
        .partitions()
        .into_iter()
        .map(|partition| PartitionDump {
            topic_id_partition: TopicIdPartition::new(
                partition.topic_id,
                partition.topic,
                partition.partition,
            ),
            segments: partition
                .segments
                .iter()
                .map(|segment| segment.metadata.clone())
                .collect(),
            delete_state: None,
        })
        .collect();
    Snapshot {
        committed_offsets: vec![0],
        dump: RlmmCacheDump { partitions },
    }
    .encode()
}

/// Build the controller metadata checkpoint the pre-disaster cluster would
/// have written, carrying the topic configuration a restore must bring back.
fn metadata_checkpoint(fixture: &Fixture) -> Vec<u8> {
    let mut image = MetadataImage::new(Uuid::new_v4());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_owned(),
        topic_id: fixture.topic_id(TOPIC),
        partitions: 2,
        replication_factor: 1,
    }));
    for partition in 0..2 {
        image.apply(&MetadataRecord::V1Partition(single_replica_partition(
            TOPIC,
            krabka_ids::PartitionIndex(partition),
            NodeId(1),
        )));
    }
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.to_owned(),
        overrides: maplit::btreemap! {"retention.ms".to_owned() => RETENTION_MS.to_owned()},
    }));
    krabka_raft::serialize_metadata_snapshot(&image, 1_700_000_000_000)
        .expect("serialize the metadata checkpoint")
        .to_vec()
}

/// Lay the two files out under a log directory the way a broker holds them, so
/// the capture finds them by the paths it looks for on a real node.
fn write_node_files(log_dir: &std::path::Path, fixture: &Fixture) {
    let rlmm = log_dir.join("remote-log-metadata");
    std::fs::create_dir_all(&rlmm).expect("create the rlmm dir");
    std::fs::write(rlmm.join("snapshot"), rlmm_snapshot(fixture)).expect("write the rlmm snapshot");

    let metadata = krabka_raft::metadata_partition_dir(log_dir);
    std::fs::create_dir_all(&metadata).expect("create the metadata dir");
    std::fs::write(
        metadata.join("00000000000000000042-0000000001.checkpoint"),
        metadata_checkpoint(fixture),
    )
    .expect("write the metadata checkpoint");
}

fn archive_args(root: &std::path::Path) -> ArchiveArgs {
    ArchiveArgs {
        local: Some(root.to_path_buf()),
        ..ArchiveArgs::default()
    }
}

/// Start a broker on `log_dir`, its controller listening on `controller`, and
/// connect a client to it.
async fn boot(
    controller: &mut ControllerListener,
    log_dir: std::path::PathBuf,
) -> (BrokerHandle, Client) {
    let broker = controller.start(BrokerConfig::for_tests(log_dir)).await;
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("dr-roundtrip")
        .build()
        .await
        .expect("client");
    (broker, client)
}

/// The topic id the cluster gave `orders`. `OffsetCommit` v10 puts the id on
/// the wire instead of the name, so a commit without it reaches no partition.
async fn topic_id(bootstrap: &str) -> BTreeMap<String, krabka_protocol::primitives::uuid::Uuid> {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin client");
    admin
        .metadata(&[TOPIC])
        .await
        .expect("Metadata")
        .topics
        .into_iter()
        .filter_map(|topic| {
            topic.topic_id.map(|id| {
                (
                    topic.name,
                    krabka_protocol::primitives::uuid::Uuid(id.into_bytes()),
                )
            })
        })
        .collect()
}

/// Commit one offset for the group, the way a running consumer would.
async fn commit_position(client: &Client, bootstrap: &str, offset: i64) {
    let group = krabka_backup::offsets::GroupOffsets {
        group: GROUP.to_owned(),
        offsets: vec![krabka_backup::offsets::CommittedOffset {
            topic: TOPIC.to_owned(),
            partition: PARTITION,
            offset,
        }],
    };
    let found = client
        .send(build_find_coordinator(GROUP, CoordinatorKeyType::Group))
        .await
        .expect("FindCoordinator");
    let coordinator = coordinator_endpoint(GROUP, found).expect("a group coordinator");
    let response = client
        .broker(coordinator.node_id)
        .send(krabka_backup::offsets::commit_request(
            &group,
            &topic_id(bootstrap).await,
        ))
        .await
        .expect("OffsetCommit");
    assert!(
        krabka_backup::offsets::commit_refusals(&response).is_empty(),
        "the pre-disaster commit was refused: {response:?}"
    );
}

/// The group's committed offset for `orders-0`, as `OffsetFetch` answers it.
async fn committed_offset(bootstrap: &str) -> i64 {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin client");
    admin
        .list_consumer_group_offsets(GROUP)
        .await
        .expect("OffsetFetch")
        .get(&(TOPIC.to_owned(), PARTITION))
        .copied()
        .unwrap_or(NO_COMMITTED_OFFSET)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_captured_cluster_restores_with_its_configuration_and_its_group_positions() {
    let fixture = build_fixture();
    let workspace = tempfile::tempdir().expect("workspace");
    let backup_root = tempfile::tempdir().expect("backup root");
    let backup = archive_args(backup_root.path());

    // 1. The cluster before the disaster: the archive's own history, a group
    //    with a position in it, and the two files on the node's disk.
    // The restored node comes back at the address the lost one had.
    let mut controller = ControllerListener::bind().await;
    let pre_log_dir = workspace.path().join("pre");
    restore(&controller.restore_args(fixture.archive_root.path(), &pre_log_dir, &[]))
        .await
        .expect("stand the pre-disaster cluster up");
    let node_files = workspace.path().join("node");
    std::fs::create_dir_all(&node_files).expect("create the node dir");
    write_node_files(&node_files, &fixture);

    let (pre_broker, pre_client) = boot(&mut controller, pre_log_dir.clone()).await;
    pre_broker
        .wait_until_partition_present(TOPIC, PARTITION)
        .await;
    // No broker creates `__consumer_offsets` at startup. It is created on the
    // first coordinator lookup, as Kafka's `IntegrationTestHarness` creates it
    // before a test commits an offset.
    pre_broker.wait_until_group_coordinator_ready().await;
    let pre_bootstrap = pre_broker.listen_addr().to_string();
    commit_position(&pre_client, &pre_bootstrap, COMMITTED_OFFSET).await;
    check!(committed_offset(&pre_bootstrap).await == COMMITTED_OFFSET);

    // 2. The capture, and the check that it is whole.
    let capture = run::capture(Some(&node_files), Some(&pre_bootstrap), &backup)
        .await
        .expect("capture the restore inputs");
    run::verify(&capture, &backup)
        .await
        .expect("the capture verifies");

    // 3. The disaster. The cluster is gone and so are its disks.
    drop(pre_client);
    pre_broker.shutdown().await;
    std::fs::remove_dir_all(&pre_log_dir).expect("destroy the pre-disaster data directory");
    std::fs::remove_dir_all(&node_files).expect("destroy the node's other files");

    // 4. The restore, with both captured snapshots.
    let rlmm = backup_root
        .path()
        .join(capture_key(&capture, RLMM_SNAPSHOT));
    let checkpoint = backup_root.path().join(capture_key(
        &capture,
        krabka_backup::manifest::METADATA_CHECKPOINT,
    ));
    let post_log_dir = workspace.path().join("post");
    let report = restore(&controller.restore_args(
        fixture.archive_root.path(),
        &post_log_dir,
        &[
            "--rlmm-snapshot",
            &rlmm.display().to_string(),
            "--metadata-snapshot",
            &checkpoint.display().to_string(),
        ],
    ))
    .await
    .expect("restore from the archive");
    check!(report.metadata.topic_configs == 1);

    let (post_broker, post_client) = boot(&mut controller, post_log_dir).await;
    post_broker
        .wait_until_partition_present(TOPIC, PARTITION)
        .await;
    post_broker.wait_until_group_coordinator_ready().await;
    let post_bootstrap = post_broker.listen_addr().to_string();

    // 5. The restored cluster has the configuration back and the group's
    //    position gone. Nothing in a KIP-405 archive holds a committed offset.
    crate::topic_configuration::check_topic_configuration(
        &post_client,
        TOPIC,
        "retention.ms",
        RETENTION_MS,
    )
    .await;

    check!(committed_offset(&post_bootstrap).await == NO_COMMITTED_OFFSET);

    // 6. The offsets go back, and the group is where it was.
    let committed = run::restore_offsets(&capture, &post_bootstrap, false, &backup)
        .await
        .expect("commit the captured offsets");
    check!(committed == 1);
    check!(committed_offset(&post_bootstrap).await == COMMITTED_OFFSET);

    post_broker.shutdown().await;
}
