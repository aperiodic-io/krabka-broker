//! The streams-consumer side of the type flip: the topology and
//! `StreamsGroupHeartbeat` request builders, the loop that drives one member to
//! a converged task assignment, and the leave heartbeat that drains the group
//! before a classic `JoinGroup` may convert it.

use {
    crate::support::streams::topology as build_topology,
    krabka_client_core::Client,
    krabka_protocol::owned::streams_group_heartbeat_request::{
        StreamsGroupHeartbeatRequest, Topology,
    },
};

pub(crate) fn topology(source_topic: &str) -> Topology { build_topology(source_topic, vec![]) }

/// Drives one streams member to convergence, which means at least
/// `want_active` active-task partitions. It returns
/// `(member_id, last_response)`.
pub use crate::support::streams::join_until_assigned as streams_join_and_converge;

/// Sends a streams `LeaveGroup`, with `member_epoch` -1, so that the group
/// drains.
pub(crate) async fn streams_leave(client: &Client, group: &str, member_id: &str) {
    let _ = client
        .send(StreamsGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("streams leave heartbeat");
}
