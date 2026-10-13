//! Classic Kafka ISR decisions: `AlterPartition` validation on the controller,
//! ISR maintenance and follower catch-up tracking on the partition leader, and
//! the high-watermark computation.

use creusot_std::prelude::*;

/// How a proposed ISR relates to the partition's assignment and leader.
#[krabka_macros::model_type]
pub enum ProposedIsr {
    /// Kafka's `Replicas.validateIsr` fails: a member is negative, repeated,
    /// or not an assigned replica.
    Invalid,
    /// Every member is a distinct assigned replica, but the current leader is
    /// not among them.
    WithoutLeader,
    /// Every member is a distinct assigned replica, and the leader is one.
    Valid,
}

/// What the controller knows about one `AlterPartition` partition row, as
/// Kafka's `ReplicationControlManager.validateAlterPartitionData` reads it.
#[krabka_macros::model_type]
pub struct AlterPartitionFacts {
    pub request_leader_epoch: i32,
    pub current_leader_epoch: i32,
    pub request_partition_epoch: i32,
    pub current_partition_epoch: i32,
    /// The request's `brokerId` is the partition's current leader.
    pub requester_is_leader: bool,
    pub proposed_isr: ProposedIsr,
    /// The requested leader recovery state is a known value, a `RECOVERING`
    /// request proposes at most one member, and a `RECOVERED` partition is not
    /// asked to go back to `RECOVERING`.
    pub recovery_state_valid: bool,
    /// Kafka's `ineligibleReplicasForIsr` is empty: every proposed member is
    /// registered, unfenced, not in controlled shutdown, and carries either
    /// the -1 sentinel or its registered broker epoch.
    pub replicas_eligible: bool,
}

/// The first `AlterPartition` validation failure of one partition row, or
/// `Admit`.
#[krabka_macros::model_type]
pub enum IsrAdmission {
    /// `NOT_CONTROLLER`: the request carries a leader or partition epoch the
    /// controller has not seen, so this controller is likely stale.
    NotController,
    /// `FENCED_LEADER_EPOCH`: the request's leader epoch is older.
    FencedLeaderEpoch,
    /// `INVALID_UPDATE_VERSION`: the request's partition epoch is older.
    InvalidUpdateVersion,
    /// `INVALID_REQUEST`: the requester is not the leader, the proposed ISR is
    /// invalid or omits the leader, or the recovery-state change is illegal.
    InvalidRequest,
    /// `INELIGIBLE_REPLICA`: a proposed member may not join an ISR.
    IneligibleReplica,
    Admit,
}

/// Which fetch time, if any, a follower fetch proves the follower was caught
/// up at.
#[krabka_macros::model_type]
pub enum CaughtUpCredit {
    /// The fetch reached the leader's log end offset: caught up now.
    ThisFetch,
    /// The fetch reached the leader's log end offset as of the previous fetch:
    /// caught up at the previous fetch's time.
    PreviousFetch,
    /// The fetch proves nothing new.
    Unchanged,
}

/// Where one candidate stands in the partition the leader maintains.
#[krabka_macros::model_type]
pub enum IsrMemberRole {
    /// A current ISR member that is no longer assigned.
    Unassigned,
    /// The assigned leader.
    Leader,
    /// An assigned follower in the current ISR.
    InSyncFollower,
    /// An assigned follower outside the current ISR.
    OutOfSyncFollower,
}

/// The inputs of [`isr_maintenance_selected`], superseded by
/// [`IsrCandidateFacts`]. Every time bound is `replica.lag.time.max.ms`,
/// measured from the scan's single timestamp.
#[krabka_macros::model_type]
pub struct IsrMemberFacts {
    pub role: IsrMemberRole,
    /// The follower's log end offset equals the leader's at this scan.
    pub log_end_matches_leader: bool,
    /// The follower's last caught-up time is within the bound.
    pub caught_up_within_lag: bool,
    /// The follower's last fetch is within the bound.
    pub fetch_within_lag: bool,
}

/// What the leader's metadata cache and the follower's last Fetch say about
/// whether one replica may join the ISR, the inputs of Kafka's
/// `Partition.isReplicaIsrEligible` (KIP-841).
#[krabka_macros::model_type]
pub struct IsrEligibilityFacts {
    /// Kafka's `metadataCache.isBrokerFenced`.
    pub fenced: bool,
    /// Kafka's `metadataCache.isBrokerShuttingDown`.
    pub shutting_down: bool,
    /// Kafka's `ReplicaState.brokerEpoch`: the broker epoch the follower's
    /// last Fetch to this leader carried, -1 when that Fetch carried none, and
    /// `None` before this leader received one.
    pub fetch_broker_epoch: Option<i64>,
    /// Kafka's `metadataCache.getAliveBrokerEpoch`: the broker's registered
    /// epoch while it is registered and unfenced, `None` otherwise.
    pub alive_broker_epoch: Option<i64>,
}

/// What the leader knows about one candidate at an ISR maintenance scan,
/// read once at the scan's single timestamp.
#[krabka_macros::model_type]
pub struct IsrCandidateFacts {
    pub role: IsrMemberRole,
    /// Kafka's `ReplicaState.logEndOffset`: the follower's last fetch offset,
    /// -1 before it fetched from this leader.
    pub follower_log_end: i64,
    /// The leader's log end offset.
    pub leader_log_end: i64,
    /// The leader's high watermark.
    pub leader_high_watermark: i64,
    /// Kafka's `Partition.leaderEpochStartOffsetOpt`: the log end offset at
    /// which this leader's epoch began, `None` while unknown.
    pub leader_epoch_start: Option<i64>,
    /// The follower's last caught-up time is within
    /// `replica.lag.time.max.ms`.
    pub caught_up_within_lag: bool,
    pub eligibility: IsrEligibilityFacts,
}

/// One remote replica as Kafka's `Partition.maybeIncrementLeaderHW` weighs
/// it.
#[krabka_macros::model_type]
pub struct HwmReplica {
    /// Kafka's `ReplicaState.logEndOffset`, -1 before the replica fetched from
    /// this leader.
    pub log_end: i64,
    /// The replica is in the leader's ISR.
    pub in_isr: bool,
    /// The replica's last caught-up time is within `replica.lag.time.max.ms`.
    pub caught_up_within_lag: bool,
    pub eligibility: IsrEligibilityFacts,
}

/// The partition-wide inputs of Kafka's `Partition.maybeIncrementLeaderHW`.
#[krabka_macros::model_type]
pub struct HighWatermarkFacts {
    /// The leader's high watermark before this step.
    pub current: i64,
    /// The leader's log end offset.
    pub leader_log_end: i64,
    /// Kafka's `partitionState.isr.size`: the ISR, leader included.
    pub isr_size: usize,
    /// Kafka's `Partition.effectiveMinIsr`: `min.insync.replicas` capped by
    /// the replica count.
    pub effective_min_isr: usize,
}

mod admission;
pub use admission::{follower_caught_up_credit, isr_admission, isr_maintenance_selected};

mod watermark;
pub use watermark::{
    isr_candidate_selected, isr_proposal_changed, leader_high_watermark, replica_isr_eligible,
};

#[cfg(test)]
mod tests;
