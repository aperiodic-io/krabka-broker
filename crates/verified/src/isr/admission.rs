use {
    super::{
        AlterPartitionFacts, CaughtUpCredit, IsrAdmission, IsrMemberFacts, IsrMemberRole,
        ProposedIsr,
    },
    creusot_std::prelude::*,
};

/// Kafka's `ReplicationControlManager.validateAlterPartitionData`, check for
/// check, after its unknown-partition check.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_validate_alter_partition_data(facts: AlterPartitionFacts) -> IsrAdmission {
    pearlite! {
        if facts.request_leader_epoch@ > facts.current_leader_epoch@
            || facts.request_partition_epoch@ > facts.current_partition_epoch@
        {
            IsrAdmission::NotController
        } else if facts.request_leader_epoch@ < facts.current_leader_epoch@ {
            IsrAdmission::FencedLeaderEpoch
        } else if !facts.requester_is_leader {
            IsrAdmission::InvalidRequest
        } else if facts.request_partition_epoch@ < facts.current_partition_epoch@ {
            IsrAdmission::InvalidUpdateVersion
        } else {
            match facts.proposed_isr {
                ProposedIsr::Valid => {
                    if !facts.recovery_state_valid {
                        IsrAdmission::InvalidRequest
                    } else if !facts.replicas_eligible {
                        IsrAdmission::IneligibleReplica
                    } else {
                        IsrAdmission::Admit
                    }
                }
                _ => IsrAdmission::InvalidRequest,
            }
        }
    }
}

/// Apply Kafka's ordered `AlterPartition` checks to one partition row.
///
/// A row is admitted exactly when it names the controller's current leader and
/// partition epochs, comes from the leader, proposes a valid ISR that keeps
/// the leader, requests a legal recovery state, and names only eligible
/// replicas. In particular a row whose partition epoch is behind the
/// controller's never overwrites the newer ISR.
#[ensures(result == kafka_validate_alter_partition_data(facts))]
#[ensures((result == IsrAdmission::Admit) == (
    facts.request_leader_epoch@ == facts.current_leader_epoch@
        && facts.request_partition_epoch@ == facts.current_partition_epoch@
        && facts.requester_is_leader
        && facts.proposed_isr == ProposedIsr::Valid
        && facts.recovery_state_valid
        && facts.replicas_eligible
))]
#[must_use]
pub fn isr_admission(facts: AlterPartitionFacts) -> IsrAdmission {
    if facts.request_leader_epoch > facts.current_leader_epoch
        || facts.request_partition_epoch > facts.current_partition_epoch
    {
        return IsrAdmission::NotController;
    }
    if facts.request_leader_epoch < facts.current_leader_epoch {
        return IsrAdmission::FencedLeaderEpoch;
    }
    if !facts.requester_is_leader {
        return IsrAdmission::InvalidRequest;
    }
    if facts.request_partition_epoch < facts.current_partition_epoch {
        return IsrAdmission::InvalidUpdateVersion;
    }
    match facts.proposed_isr {
        ProposedIsr::Valid => {}
        ProposedIsr::Invalid | ProposedIsr::WithoutLeader => {
            return IsrAdmission::InvalidRequest;
        }
    }
    if !facts.recovery_state_valid {
        return IsrAdmission::InvalidRequest;
    }
    if facts.replicas_eligible {
        IsrAdmission::Admit
    } else {
        IsrAdmission::IneligibleReplica
    }
}

/// Kafka's `Replica.updateFetchStateOrThrow` choice of `lastCaughtUpTimeMs`.
///
/// `fetch_offset` is the follower's fetch offset, `leader_log_end` the
/// leader's log end offset when the fetch arrived, and
/// `last_fetch_leader_log_end` the leader's log end offset recorded at the
/// follower's previous fetch (-1 before the first). The host keeps the later
/// of the credited time and the recorded one.
#[ensures((result == CaughtUpCredit::ThisFetch) == (fetch_offset@ >= leader_log_end@))]
#[ensures((result == CaughtUpCredit::PreviousFetch) == (
    fetch_offset@ < leader_log_end@ && fetch_offset@ >= last_fetch_leader_log_end@
))]
#[must_use]
pub fn follower_caught_up_credit(
    fetch_offset: i64,
    leader_log_end: i64,
    last_fetch_leader_log_end: i64,
) -> CaughtUpCredit {
    if fetch_offset >= leader_log_end {
        CaughtUpCredit::ThisFetch
    } else if fetch_offset >= last_fetch_leader_log_end {
        CaughtUpCredit::PreviousFetch
    } else {
        CaughtUpCredit::Unchanged
    }
}

/// Kafka's `ReplicaState.isCaughtUp`: the follower has the leader's log end
/// offset, or it last caught up within `replica.lag.time.max.ms`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_caught_up(facts: IsrMemberFacts) -> bool {
    pearlite! { facts.log_end_matches_leader || facts.caught_up_within_lag }
}

/// krabka's former ISR scan rule, superseded by [`crate::isr::isr_candidate_selected`].
///
/// The leader always stays. An in-sync follower stays exactly while Kafka's
/// `ReplicaState.isCaughtUp` holds. An out-of-sync follower is admitted when
/// it fetched and caught up within the bound, which is not Kafka's
/// `Partition.isFollowerInSync`. The broker no longer calls this; the one
/// remaining caller is the leader-failover model's expansion stand-in, and
/// this function and [`IsrMemberFacts`] go once that model moves to
/// [`crate::isr::isr_candidate_selected`].
#[ensures(match facts.role {
    IsrMemberRole::Unassigned => !result,
    IsrMemberRole::Leader => result,
    IsrMemberRole::InSyncFollower => result == kafka_is_caught_up(facts),
    IsrMemberRole::OutOfSyncFollower =>
        result == (facts.fetch_within_lag && facts.caught_up_within_lag),
})]
#[must_use]
pub fn isr_maintenance_selected(facts: IsrMemberFacts) -> bool {
    match facts.role {
        IsrMemberRole::Unassigned => false,
        IsrMemberRole::Leader => true,
        IsrMemberRole::InSyncFollower => facts.log_end_matches_leader || facts.caught_up_within_lag,
        IsrMemberRole::OutOfSyncFollower => facts.fetch_within_lag && facts.caught_up_within_lag,
    }
}
