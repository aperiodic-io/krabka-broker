//! Error codes, broker boot and wire helpers shared by the classic-to-streams
//! upgrade scenarios in this suite.
//!
//! Both scenarios boot a single-node broker, finalize `streams.version`, create
//! the source topic and then read back topic ids or committed offsets, so those
//! steps live here rather than being repeated per scenario module.

use std::sync::Arc;

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::support::offsets::{offset_fetch_group, offset_fetch_request, offset_fetch_topic};

// ── error codes ──────────────────────────────────────────────────────────────
pub const ERR_NONE: i16 = 0;
pub const ERR_MEMBER_ID_REQUIRED: i16 = 79;
pub const ERR_GROUP_ID_NOT_FOUND: i16 = 69;

// ── boot / connect helpers ────────────────────────────────────────────────────

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    crate::support::streams::boot(crate::support::streams::ElectionReadiness::BrokerElectable).await
}

pub async fn connect(bootstrap: &str) -> Arc<Client> {
    crate::support::client::connect(bootstrap, "c1").await
}

pub async fn assert_committed_offset(client: &Client, topic_id: WireUuid, expected: i64) {
    let response = client
        .send(offset_fetch_request(offset_fetch_group(
            "g",
            Some(vec![offset_fetch_topic("in", topic_id, vec![0])]),
        )))
        .await
        .expect("OffsetFetch");
    let group = response
        .groups
        .iter()
        .find(|g| g.group_id == "g")
        .expect("group g");
    let topic = group
        .topics
        .iter()
        .find(|t| t.topic_id == topic_id)
        .expect("topic in");
    let partition = topic.partitions.first().expect("partition 0");
    assert!(
        partition.error_code == ERR_NONE,
        "OffsetFetch failed: {partition:?}"
    );
    assert!(
        partition.committed_offset == expected,
        "committed offset was not preserved"
    );
}

pub async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    crate::support::client::create_topic(client, topic, partitions).await;
}

/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
pub async fn finalize_streams_version(client: &Client) {
    crate::support::streams::finalize_streams_version(client).await;
}

pub use crate::support::topic_id_for;
