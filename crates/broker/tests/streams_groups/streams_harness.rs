//! Broker boot, feature-finalization and heartbeat helpers shared by the
//! KIP-1071 streams-group scenarios in this suite.
//!
//! Every scenario boots a single-node broker, finalizes `streams.version` to
//! level 1 so the streams handlers stop returning `UNSUPPORTED_VERSION`, and
//! then drives `StreamsGroupHeartbeat` until the coordinator hands out active
//! tasks. Those steps, and the small accessors that read an assignment out of a
//! heartbeat response, live here so each scenario module holds only its own
//! assertions.

use krabka_client_core::Client;
use krabka_protocol::owned::{
    streams_group_describe_request::StreamsGroupDescribeRequest,
    streams_group_heartbeat_request::Topology,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

pub use crate::support::streams::{
    active_partition_count, finalize_streams_version, first_join, follow_up, topology,
};

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    crate::support::streams::boot(crate::support::streams::ElectionReadiness::CoordinatorReady)
        .await
}

pub use crate::support::client::{connect_c1 as connect, create_topic};

/// Active-task partitions for a given subtopology id, sorted.
pub fn active_partitions_for(resp: &StreamsGroupHeartbeatResponse, sub: &str) -> Vec<i32> {
    let mut parts: Vec<i32> = resp
        .active_tasks
        .as_ref()
        .map(|v| {
            v.iter()
                .filter(|t| t.subtopology_id == sub)
                .flat_map(|t| t.partitions.clone())
                .collect()
        })
        .unwrap_or_default();
    parts.sort_unstable();
    parts
}

/// The response status codes. In the KIP-1071 status enum, 3 is
/// `MISSING_INTERNAL_TOPICS`.
pub fn status_codes(resp: &StreamsGroupHeartbeatResponse) -> Vec<i8> {
    resp.status
        .as_ref()
        .map(|v| v.iter().map(|s| s.status_code).collect())
        .unwrap_or_default()
}

pub async fn describe(
    client: &Client,
    group: &str,
) -> krabka_protocol::owned::streams_group_describe_response::StreamsGroupDescribeResponse {
    client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![group.into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe")
}

/// Drive a single member to its first join, then re-heartbeat until convergence
/// returns. The returned tuple is `(member_id, last_response)`.
pub async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    crate::support::streams::streams_join_and_converge(
        client,
        group,
        topo,
        want_active,
        tries,
        true,
    )
    .await
}
