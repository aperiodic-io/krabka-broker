//! Uniform group-assignor kernels (KIP-848 `UniformAssignor`).
//!
//! Kafka's server-side `UniformAssignor`
//! (`org.apache.kafka.coordinator.group.assignor`) picks one of two builders.
//! `UniformHomogeneousAssignmentBuilder` runs when every member subscribes to
//! the same topics, and `UniformHeterogeneousAssignmentBuilder` runs
//! otherwise. Both rank balance ahead of stickiness, and neither consults
//! replica racks. This module holds the two decisions that the builders make
//! about members: the quota each member gets in the homogeneous builder, and
//! the least-loaded subscriber that receives an unassigned partition in the
//! heterogeneous builder. The host in `krabka-broker` owns the partition
//! bookkeeping around them.

use creusot_std::prelude::*;

/// Kafka's floor-and-remainder split of a partition total over the members.
#[krabka_macros::model_type(proof(std::clone::Clone, Copy))]
pub struct UniformQuotaSplit {
    /// `minimumMemberQuota`: every member gets at least this many partitions.
    pub minimum_quota: usize,
    /// `remainingMembersToGetAnExtraPartition`: this many members get one
    /// partition more than `minimum_quota`.
    pub extra_quotas: usize,
}

/// One member's quota in the homogeneous builder.
#[krabka_macros::model_type(proof(std::clone::Clone, Copy))]
pub struct HomogeneousMemberQuota {
    /// Whether the member's target is `minimum_quota + 1`, not
    /// `minimum_quota`.
    pub takes_extra: bool,
    /// How many partitions the member keeps from its current assignment.
    pub retain: usize,
    /// How many unassigned partitions the member receives.
    pub fill: usize,
}

/// A subscriber's load while the heterogeneous builder assigns one topic.
#[krabka_macros::model_type(proof(std::clone::Clone, Copy))]
pub struct SubscriberLoad {
    /// Partitions the member holds now, across every topic.
    pub assigned: usize,
    /// Partitions the member held when the builder started on this topic.
    pub assigned_at_topic_start: usize,
}

mod select_least_loaded;
#[cfg(creusot)]
pub use select_least_loaded::{
    extras_left_before, keeps_extra_quota, load_precedes, member_target,
};
pub use select_least_loaded::{
    homogeneous_member_quotas, select_least_loaded, uniform_quota_split,
};

#[cfg(test)]
mod tests;
