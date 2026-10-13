//! Adding a replica to a tiered partition whose local segments are gone.
//!
//! Growing or healing a storage-heavy cluster means moving tiered partitions
//! onto brokers that hold none of their history. KIP-405 says the new replica
//! must not be handed that history: the leader answers a follower fetching
//! into `[log_start, local_log_start)` with `OFFSET_MOVED_TO_TIERED_STORAGE`,
//! and the follower restarts its log at the leader's local log start. What the
//! new replica then pulls is bounded by the leader's *local* retention, not by
//! the size of the archive.
//!
//! Without that answer the leader serves the follower out of the tier, one
//! batch per fetch, exactly as it serves a consumer -- so every archived byte
//! is re-materialised on the new broker's disk at replication rate, through
//! the leader's bounded reader pool. This suite is what says that does not
//! happen: it archives a partition, evicts it locally, adds a replica, and
//! measures both what the new replica pulled over the wire and where its log
//! begins.
//!
//! The partition is assigned to broker 1 by hand. Broker 1 is the address
//! every broker's RLMM client bootstraps against, so it has to keep running
//! for any copy to complete; making it the leader keeps it running and keeps
//! brokers 2 and 3 free to be the replica this test adds.

use {
    crate::{
        multi_client::topic_id_for,
        multi_cluster::{ready_admin, start_three_tiered_brokers_with_segment_sizes},
        multi_workload::local_segment_bases,
        support::{
            client::connect_owned,
            storage::{local_log_segment_metadata, topic_remote_log_files},
            topics::AssignedTopicSetup,
        },
        topic_fixture::{create_assigned_topic, produce_records as produce_tiered_records},
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, metrics::PartitionLabel},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        path::Path,
        time::{Duration, Instant},
    },
};

/// The topic this suite produces into.
const TOPIC: &str = "tiered-reassign-itest";
/// Records produced before the replica is added. Each is a batch of its own,
/// so the 1 KiB segments roll many times and the archive grows well past what
/// local retention will ever keep.
const RECORDS: usize = 400;
/// The payload each record carries. Wide enough that `RECORDS` of them fill
/// many segments, so "bounded by local retention" and "bounded by the archive"
/// are far apart.
const PAYLOAD: usize = 256;

/// The total size of every `*.log` object the shared store holds for `TOPIC`.
fn archive_bytes(root: &Path) -> u64 {
    let files = topic_remote_log_files(root, TOPIC);
    files
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|meta| meta.len())
        .sum()
}

/// The total size of the `*.log` files in one replica's partition directory.
fn local_log_bytes(partition_dir: &Path) -> u64 {
    local_log_segment_metadata(partition_dir)
        .into_iter()
        .map(|meta| meta.len())
        .sum()
}

/// Produces `count` single-record batches through `client`, one request each.
async fn produce_records(client: &Client, topic_id: WireUuid, count: usize) {
    produce_tiered_records()
        .client(client)
        .value(|index| {
            let mut value = format!("record-{index}-").into_bytes();
            value.resize(PAYLOAD, b'x');
            bytes::Bytes::from(value)
        })
        .topic(TOPIC)
        .topic_id(topic_id)
        .count(count)
        .call()
        .await;
}

/// Creates the tiered topic on broker 1 alone, with local retention set so
/// tight that every sealed segment is evicted as soon as it is archived.
async fn create_single_replica_tiered_topic(admin: &Client, leader: &BrokerHandle) {
    let assignment = AssignedTopicSetup::on_replicas(TOPIC, vec![krabka_metadata::NodeId(1)]);
    create_assigned_topic()
        .client(admin)
        .topic(assignment)
        .maybe_segment_bytes(None)
        .call()
        .await;
    leader
        .wait_for_image(|img| img.partition(TOPIC, 0).is_some())
        .await;
}

/// Sends `AlterPartitionReassignments` for `TOPIC-0` to the controller leader.
async fn reassign_to(controller: &Client, replicas: Vec<i32>) {
    let response = controller
        .send(AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change: true,
            topics: vec![ReassignableTopic {
                name: TOPIC.into(),
                partitions: vec![ReassignablePartition {
                    partition_index: 0,
                    replicas: Some(replicas),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterPartitionReassignments");
    assert!(
        response.error_code == 0,
        "AlterPartitionReassignments failed: {response:?}"
    );
    assert!(
        response.responses[0].partitions[0].error_code == 0,
        "AlterPartitionReassignments partition row failed: {response:?}"
    );
}

/// A replica added to a tiered partition inherits the archive by reference,
/// not by copy.
///
/// Broker 1 archives 400 records over many 1 KiB segments and evicts every one
/// of them locally, so its log begins far above zero and everything below that
/// exists only in the shared object store. Broker 3 is then added as a
/// replica. It starts from an empty log at offset 0, which is squarely inside
/// the band the leader keeps only in the tier.
///
/// What it must do is restart at the leader's local log start and replicate
/// forward from there.
///
/// The measurement that says so is the cumulative
/// `replication_bytes_in` counter on the new replica: it has to stay below the
/// size of the archive. That is the one number a later eviction cannot
/// flatter, and it is exactly what fails when the leader serves the follower
/// out of the tier instead -- the replica then pulls every archived byte over
/// the wire and drops it again on its next local-retention tick, so its disk
/// and its log start end up looking just as they do here. The disk and
/// log-start checks below hold the rest of the shape: the replica's log begins
/// at or above the leader's local log start and its files are bounded by local
/// retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_added_to_a_tiered_partition_does_not_pull_the_archive() {
    let (b1, b2, b3, log_dirs, remote_dir) = start_three_tiered_brokers_with_segment_sizes([
        krabka_units::kibibytes(1),
        krabka_units::kibibytes(1),
        krabka_units::kibibytes(1),
    ])
    .await;
    let admin = ready_admin([&b1, &b2, &b3], "tiered-reassign-admin").await;
    create_single_replica_tiered_topic(&admin, &b1).await;
    assert!(
        b1.partition_leader_for_test(TOPIC, 0) == Some(b1.node_id()),
        "the manual assignment makes broker 1 the only replica and so the leader"
    );

    let topic_id = topic_id_for(&admin, TOPIC).await;
    produce_records(&admin, topic_id, RECORDS).await;

    let leader_dir = log_dirs[0].path().join(format!("{TOPIC}-0"));
    let new_replica_dir = log_dirs[2].path().join(format!("{TOPIC}-0"));

    // Every sealed segment archived and then evicted: the leader's log start
    // is what tells us so, and it is the offset the new replica must land on.
    let eviction_deadline = Instant::now() + Duration::from_mins(3);
    let leader_local_start = loop {
        // Every sealed segment gone, not merely the first: production has
        // stopped, so local retention converges on the active segment alone,
        // and reading the archive before it settles would measure a tier the
        // copy pass has not finished filling.
        let bases = local_segment_bases(&leader_dir);
        if let [base] = bases[..]
            && base > 0
            && archive_bytes(remote_dir.path()) > 0
        {
            break base;
        }
        assert!(
            Instant::now() <= eviction_deadline,
            "the leader never archived and evicted every sealed segment; \
             segments={bases:?} archive={}",
            archive_bytes(remote_dir.path()),
        );
        // intentional: the copy pass and local retention both run on the
        // broker's own 1s interval, and the object store is a directory.
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let archive = archive_bytes(remote_dir.path());
    let leader_local = local_log_bytes(&leader_dir);
    eprintln!(
        "ITEST: the leader's log starts at {leader_local_start}; \
         archive={archive} bytes, leader local={leader_local} bytes"
    );
    // The whole test rests on these being far apart. If local retention ever
    // stopped evicting, the follower could copy everything and still look
    // bounded.
    assert!(
        archive > leader_local * 4,
        "the archive ({archive} bytes) is not meaningfully larger than what the \
         leader still holds locally ({leader_local} bytes)"
    );

    // Add broker 3 as a replica. The request goes to the controller leader's
    // own listener, as `kafka-reassign-partitions` sends it.
    let controller_leader = b1.wait_until_controller_leader().await;
    let controller_addr = [&b1, &b2, &b3]
        .iter()
        .find(|handle| handle.node_id() == controller_leader.0)
        .map(|handle| format!("127.0.0.1:{}", handle.listen_addr().port()))
        .expect("the controller leader is one of the three brokers");
    let controller_client = connect_owned(
        &controller_addr,
        "tiered-reassign-controller",
        "controller client",
    )
    .await;
    reassign_to(&controller_client, vec![1, 3]).await;

    // Broker 3 hosts the partition and catches up to the leader's log end.
    let leader_end = b1
        .local_log_end_offset(TOPIC, 0)
        .expect("the leader holds the partition");
    let catch_up_deadline = Instant::now() + Duration::from_mins(3);
    loop {
        if b3
            .local_log_end_offset(TOPIC, 0)
            .is_some_and(|end| end >= leader_end)
        {
            break;
        }
        assert!(
            Instant::now() <= catch_up_deadline,
            "the new replica reached {:?} of the leader's {leader_end}; log_start={:?}",
            b3.local_log_end_offset(TOPIC, 0),
            b3.partition_log_start_for_test(TOPIC, 0),
        );
        // intentional: replication has no in-process completion signal here.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Where the new replica's log begins. Zero means it took the archive.
    let new_replica_start = *local_segment_bases(&new_replica_dir)
        .first()
        .expect("the new replica holds a segment");
    eprintln!(
        "ITEST: the new replica's segments are {:?} against the leader's {:?}",
        local_segment_bases(&new_replica_dir),
        local_segment_bases(&leader_dir),
    );
    check!(
        new_replica_start >= leader_local_start,
        "the new replica's log starts at {new_replica_start}, below the leader's local log \
         start of {leader_local_start}: it replicated the archive"
    );

    // What it pulled to get there. The counter is cumulative over the
    // partition's whole life on this broker, so it cannot be flattered by a
    // later eviction the way an on-disk measurement can.
    let label = PartitionLabel {
        topic: std::sync::Arc::from(TOPIC),
        partition: 0,
    };
    let replicated = b3
        .metrics()
        .replication_bytes_in
        .get_or_create(&label)
        .get();
    eprintln!("ITEST: the new replica pulled {replicated} bytes over the replication path");
    // Half the archive is a deliberately loose bound on "did not pull the
    // archive": a replica that starts at the leader's local log start pulls
    // only what local retention still holds, which here is one active segment.
    check!(
        replicated * 2 < archive,
        "the new replica pulled {replicated} bytes over the replication path against an \
         archive of {archive} bytes: it replicated the tier"
    );

    // And its disk is bounded by local retention, not by the archive.
    let new_replica_local = local_log_bytes(&new_replica_dir);
    check!(
        new_replica_local < archive,
        "the new replica holds {new_replica_local} bytes on disk against an archive of \
         {archive} bytes"
    );

    drop(admin);
    drop(controller_client);
    b1.shutdown().await;
    b2.shutdown().await;
    b3.shutdown().await;
}
