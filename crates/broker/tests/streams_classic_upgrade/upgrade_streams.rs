//! Client-side drivers for the streams (`StreamsGroupHeartbeat`) side of the
//! upgrade scenarios.
//!
//! Both scenarios send a streams heartbeat at the group id a classic group
//! already owns, so the topology builder and the bounded convergence loop are
//! kept here apart from the classic-side `JoinGroup` drivers.

pub use crate::support::streams::first_join;
use {
    crate::support::streams::topology as build_topology,
    krabka_protocol::owned::streams_group_heartbeat_request::Topology,
};

pub fn topology(source_topic: &str) -> Topology { build_topology(source_topic, vec![]) }

/// Drive a single streams member to convergence (at least `want_active`
/// active-task partitions). Returns `(member_id, last_response)`.
pub use crate::support::streams::join_until_assigned as streams_join_and_converge;
