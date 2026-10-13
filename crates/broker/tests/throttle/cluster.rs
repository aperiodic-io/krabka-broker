//! Cluster fixtures: bringing up the single broker each test runs against, in
//! either its `SASL_PLAINTEXT` or its PLAINTEXT flavour, plus topic creation
//! and the wait that absorbs raft commit latency.
//!
//! The suite needs both listener flavours — the config tests want a named
//! principal, the fetch tests only want the shortest path to a leader — so the
//! two starters and their two matching `CreateTopics` drivers are collected
//! here rather than duplicated per test module.

/// Create a topic through PLAINTEXT. There is no SASL, and the compat shim
/// allows everything.
pub use crate::kafka_wire::create_automatic_topic_plaintext as create_topic_plaintext;
/// Create a topic through SASL/PLAIN as the given admin user.
/// Copied from `partition_reassignment.rs`.
pub use crate::kafka_wire::create_topic_as_admin;
/// Start a single-broker PLAINTEXT cluster (no SASL).
/// Returns `(handle, _dir, addr)`.
pub use crate::support::sasl::start_single_broker_plaintext;
pub use crate::support::sasl::start_single_broker_sasl_plaintext_with_users;
use krabka_broker::BrokerHandle;

/// Await until `handle` sees `(topic, partition)` present in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

/// Add `follower` to the replicas of partition 0 of `topic`, outside the ISR.
///
/// Kafka's leader answers a replica fetch from an id outside the assignment
/// with `NOT_LEADER_OR_FOLLOWER` (`Partition.followerReplicaOrThrow`), so a
/// throttle test must fetch as an assigned follower. The single-broker cluster
/// has no broker `follower`, so the test writes the assignment to the metadata
/// log directly.
pub async fn add_follower(handle: &BrokerHandle, topic: &str, follower: u64) {
    assign_follower(handle, topic, follower, false).await;
}

async fn assign_follower(handle: &BrokerHandle, topic: &str, follower: u64, in_isr: bool) {
    let follower = krabka_metadata::NodeId(follower);
    let mut record = handle
        .controller_image_for_test()
        .partition(topic, 0)
        .expect("the partition is in the image")
        .clone();
    if record.directories.len() == record.replicas.len() {
        record.directories.push(uuid::Uuid::nil());
    }
    record.replicas.push(follower);
    if in_isr {
        record.isr.push(follower);
    }
    record.partition_epoch += 1;
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(record))
        .await
        .expect("submit the partition record");
    handle
        .wait_for_image(|img| {
            img.partition(topic, 0).is_some_and(|partition| {
                if in_isr {
                    partition.isr.contains(&follower)
                } else {
                    partition.replicas.contains(&follower)
                }
            })
        })
        .await;
}

/// Add `follower` to the replicas of partition 0 of `topic` and to its ISR, as
/// a follower that has caught up.
pub async fn add_follower_in_isr(handle: &BrokerHandle, topic: &str, follower: u64) {
    assign_follower(handle, topic, follower, true).await;
}
