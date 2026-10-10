// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by jbod.rs / compaction.rs.

//! KIP-112 runtime log-dir failure path.
//!
//! The test boots a single broker with two log dirs, `primary` and `extra`,
//! and creates a 6-partition topic so that JBOD placement spreads the
//! partitions across both dirs. It then flips the `extra` dir offline through
//! the test seam and asserts that:
//!
//!   1. A Produce to a partition that lives on the now-offline `extra` dir
//!      returns `KAFKA_STORAGE_ERROR` (error code 56).
//!   2. A Produce to a partition that lives on the still-online `primary` dir
//!      returns error code 0.

mod kafka_wire;

mod support;
#[path = "support/two_dir_topic.rs"]
mod two_dir_topic;

use std::net::SocketAddr;

use assert2::{assert, check};
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse, produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as ProtocolUuid,
};
use tokio::net::TcpStream;

use crate::support::{
    records::{batch_from_records, value_record},
    wire::ApiRequestSetup,
};

krabka_macros::assignment_dirs_fixture!(assignment_dirs_request);

const CLIENT_ID: &str = "krabka-jbod-disk-failure-test";
const PRODUCE_VERSION: i16 = 9; // flexible, acks=1

// Flexible headers and correlation ID 1 for every request in this suite.
crate::flexible_round_trip_fixture!(round_trip, CLIENT_ID, 1);

/// Lists the partition indices of `topic` whose data dir lives directly under
/// `dir`.
fn partitions_in_dir(dir: &std::path::Path, topic: &str) -> Vec<i32> {
    let prefix = format!("{topic}-");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            s.strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<i32>().ok())
        })
        .collect()
}

/// Produces one record to `(topic, partition)` and returns the per-partition
/// `error_code` from the response.
async fn produce_and_get_error(addr: SocketAddr, topic: &str, partition: i32) -> i16 {
    let batch = batch_from_records(vec![value_record(
        0,
        Some(bytes::Bytes::from_static(b"kip-112-test")),
    )]);
    let req = crate::support::produce::batch_request(
        batch,
        crate::support::produce::SinglePartitionProduceSetup {
            topic: topic.to_string(),
            partition: krabka_ids::PartitionIndex(partition),
            ..Default::default()
        },
    );
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(0),
            version: WireApiVersion(PRODUCE_VERSION),
        },
        &body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, PRODUCE_VERSION).unwrap();
    resp.responses[0].partition_responses[0].error_code
}

#[tokio::test]
async fn produce_to_partition_on_offline_dir_returns_storage_error() {
    const TOPIC: &str = "kip112-offline";
    // 6 partitions: jbod.rs shows this is enough to guarantee spread across
    // both dirs under the least-loaded placement algorithm.
    const N: i32 = 6;

    let (handle, primary, extra, addr) = crate::two_dir_topic::start(crate::two_dir_topic::Setup {
        client_id: CLIENT_ID,
        topic: TOPIC,
        partitions: crate::support::topics::TopicPartitionCount(N),
        ..Default::default()
    })
    .await;

    // Confirm spread: both dirs must hold at least one partition of the topic.
    let in_extra = partitions_in_dir(extra.path(), TOPIC);
    let in_primary = partitions_in_dir(primary.path(), TOPIC);
    assert!(
        !in_extra.is_empty(),
        "test premise: at least one partition must land on the extra dir \
         (primary={} extra={})",
        in_primary.len(),
        in_extra.len()
    );
    assert!(
        !in_primary.is_empty(),
        "test premise: at least one partition must land on the primary dir"
    );

    // Pick the smallest partition index on each dir for determinism.
    let mut extra_parts = in_extra.clone();
    extra_parts.sort_unstable();
    let offline_partition = extra_parts[0];

    let mut primary_parts = in_primary.clone();
    primary_parts.sort_unstable();
    let online_partition = primary_parts[0];

    // Flip ONLY the extra dir offline. The primary dir stays online, so the
    // broker does NOT trigger the all-dirs-offline self-shutdown path.
    assert!(
        handle.test_mark_log_dir_offline(extra.path()),
        "mark_offline must return true (dir was registered and online)"
    );

    // Case 1: Produce to the offline-dir partition must return KAFKA_STORAGE_ERROR (56).
    let code = produce_and_get_error(addr, TOPIC, offline_partition).await;
    assert!(
        code == 56,
        "partition {offline_partition} on offline extra dir must return \
         KAFKA_STORAGE_ERROR (56); got {code}"
    );

    // Case 2 (sanity): Produce to the still-online primary-dir partition must succeed.
    let code = produce_and_get_error(addr, TOPIC, online_partition).await;
    assert!(
        code == 0,
        "partition {online_partition} on online primary dir must succeed (0); got {code}"
    );

    handle.shutdown().await;
}

/// KIP-112: when ALL configured log dirs go offline, the broker must shut
/// itself down and latch `should_shutdown` to `true`. This test uses a
/// single-dir broker, so flipping that one dir offline immediately satisfies
/// the all-dirs condition.
///
/// The `for_tests` heartbeat interval is 200 ms, so the check fires well
/// inside the 15-second timeout below.
#[tokio::test]
async fn all_log_dirs_offline_triggers_self_shutdown() {
    let primary = tempfile::tempdir().unwrap();
    // Single-dir broker: no extra_log_dirs.
    let cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker start");

    // Subscribe before flipping so we can't miss the transition.
    let mut shutdown_rx = handle.should_shutdown_rx();

    // Flip the only log dir offline. This is the all-dirs condition.
    assert!(
        handle.test_mark_log_dir_offline(primary.path()),
        "mark_offline must return true (dir was registered and online)"
    );

    // Wait up to 15 s for the heartbeat client to detect the all-dirs
    // condition and latch should_shutdown to true.
    let woke = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if *shutdown_rx.borrow_and_update() {
                return;
            }
            if shutdown_rx.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(
        woke.is_ok(),
        "broker did not signal self-shutdown when all log dirs went offline"
    );
    assert!(
        *shutdown_rx.borrow(),
        "should_shutdown must be true after all dirs offline"
    );

    // Shutdown should complete without hanging: the supervisor was already
    // cancelled by the self-shutdown path, and cancelling an already-
    // cancelled token is idempotent.
    handle.shutdown().await;
}

/// KIP-112 and KIP-858: the controller-leader broker accepts
/// `AssignReplicasToDirs` (`api_key=73`), records the assignment, and echoes
/// the request back with `error_code=0` on every partition.
///
/// This exercises the real async `handle` path: decode, the leader gate,
/// `plan_assignments`, `submit_change`, and encode.
async fn single_directory_broker() -> (tempfile::TempDir, BrokerHandle, SocketAddr) {
    let primary = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_tests(primary.path().to_path_buf());
    let broker = Box::pin(Broker::start(config)).await.expect("broker start");
    let address = broker.listen_addr();
    (primary, broker, address)
}

#[tokio::test]
async fn assign_replicas_to_dirs_reports_and_echoes() {
    const VERSION: i16 = 0; // AssignReplicasToDirs only has version 0

    const TOPIC: &str = "kip112-assign";
    const N: i32 = 2;
    // Use a single-dir broker so the broker IS the controller leader.
    let (_primary, handle, addr) = single_directory_broker().await;

    crate::two_dir_topic::create_and_wait(
        &handle,
        addr,
        crate::two_dir_topic::Setup {
            client_id: CLIENT_ID,
            topic: TOPIC,
            partitions: crate::support::topics::TopicPartitionCount(N),
            ..Default::default()
        },
    )
    .await;

    // Look up the topic UUID from the controller image so we can reference
    // the partition correctly in the request.
    let image = handle.controller_image_for_test();
    let topic_uuid = image
        .topics()
        .find(|t| t.name == TOPIC)
        .map(|t| t.topic_id)
        .expect("topic must be in the image after wait_all_partitions");
    let broker_epoch = image
        .broker(krabka_raft::NodeId(1))
        .expect("broker 1 is self-registered")
        .broker_epoch;

    // Choose an arbitrary dir UUID to assign partition 0 on broker 1.
    let dir_uuid = uuid::Uuid::from_u128(0xCAFE_BABE);

    // for_tests default broker_id; assign partition 0 to the arbitrary directory.
    let req = assignment_dirs_request(DirectoryAssignmentSetup {
        broker_epoch,
        dir: dir_uuid,
        topic: topic_uuid,
        ..Default::default()
    });

    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(73),
            version: WireApiVersion(VERSION),
        },
        &body,
    )
    .await
    .unwrap();

    let mut cur: &[u8] = &resp_bytes;
    let resp = AssignReplicasToDirsResponse::decode(&mut cur, VERSION).unwrap();

    check!(
        resp.error_code == 0,
        "AssignReplicasToDirs top-level error_code must be NONE (0), got {}",
        resp.error_code
    );
    assert!(
        !resp.directories.is_empty(),
        "response must echo at least one directory"
    );
    check!(
        resp.directories[0].topics[0].partitions[0].error_code == 0,
        "per-partition error_code must be NONE (0)"
    );

    handle.shutdown().await;
}

/// KIP-112: the controller accepts a `BrokerHeartbeat` (`api_key=63`) that
/// sets `offline_log_dirs`. In a single-broker cluster with no ISR peers, the
/// failover scan finds no live ISR alternative, so `plan.changes` is empty,
/// the handler skips `submit_change`, and the response carries
/// `error_code=0`.
///
/// This exercises the heartbeat handler's offline-dir failover block end to
/// end, on the no-change path.
#[tokio::test]
async fn heartbeat_with_offline_log_dirs_is_accepted() {
    use krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION as HB_MAX_VERSION;
    let (_primary, handle, addr) = single_directory_broker().await;

    // Wait until the broker has registered itself and elected a raft leader
    // (so the heartbeat handler reaches the leader branch, not NOT_CONTROLLER).
    handle.wait_until_controller_leader().await;
    let broker_epoch = handle
        .controller_image_for_test()
        .broker(krabka_raft::NodeId(handle.node_id()))
        .expect("registered broker")
        .broker_epoch;

    // Send a heartbeat with a made-up offline dir UUID. The broker is the
    // only replica so alive_isr is empty → no change → no error.
    let fake_offline_dir = uuid::Uuid::from_u128(0xDEAD_1234);
    let req = BrokerHeartbeatRequest {
        broker_id: 1, // for_tests default broker_id
        broker_epoch,
        current_metadata_offset: 0,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: vec![ProtocolUuid(fake_offline_dir.into_bytes())],
        cordoned_log_dirs: None,
        ..Default::default()
    };

    let mut body = BytesMut::new();
    req.encode(&mut body, HB_MAX_VERSION).unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(63),
            version: WireApiVersion(HB_MAX_VERSION),
        },
        &body,
    )
    .await
    .unwrap();

    let mut cur: &[u8] = &resp_bytes;
    let resp = BrokerHeartbeatResponse::decode(&mut cur, HB_MAX_VERSION).unwrap();

    assert!(
        resp.error_code == 0,
        "BrokerHeartbeat with offline_log_dirs must be accepted (error_code=0), got {}",
        resp.error_code
    );

    handle.shutdown().await;
}
