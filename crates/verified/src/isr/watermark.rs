use {
    super::{
        HighWatermarkFacts, HwmReplica, IsrCandidateFacts, IsrEligibilityFacts, IsrMemberRole,
    },
    creusot_std::prelude::*,
};

/// Kafka's `Partition.isReplicaIsrEligible`: the broker is neither fenced nor
/// in controlled shutdown, and `isBrokerEpochIsrEligible` holds -- both epochs
/// are known, and the Fetch carried either no epoch (-1) or the alive one.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_replica_isr_eligible(facts: IsrEligibilityFacts) -> bool {
    pearlite! {
        !facts.fenced && !facts.shutting_down
            && match facts.fetch_broker_epoch {
                Some(stored) => match facts.alive_broker_epoch {
                    Some(alive) => stored@ == -1 || stored@ == alive@,
                    None => false,
                },
                None => false,
            }
    }
}

/// Kafka's `ReplicaState.isCaughtUp`: the follower's log end offset is the
/// leader's, or it last caught up within `replica.lag.time.max.ms`. A
/// follower that has not fetched from this leader has log end -1, Kafka's
/// `UNKNOWN_OFFSET`, which no leader log end equals.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_replica_is_caught_up(
    follower_log_end: Int,
    leader_log_end: Int,
    caught_up_within_lag: bool,
) -> bool {
    pearlite! { follower_log_end == leader_log_end || caught_up_within_lag }
}

/// Decide whether one replica may join the ISR, Kafka's
/// `Partition.isReplicaIsrEligible`.
#[ensures(result == kafka_is_replica_isr_eligible(facts))]
#[must_use]
pub fn replica_isr_eligible(facts: IsrEligibilityFacts) -> bool {
    if facts.fenced || facts.shutting_down {
        return false;
    }
    match (facts.fetch_broker_epoch, facts.alive_broker_epoch) {
        (Some(stored), Some(alive)) => stored == -1 || stored == alive,
        _ => false,
    }
}

/// Kafka's `Partition.isFollowerInSync`: the follower's log end offset has
/// reached both the leader's high watermark and the start of the leader's
/// current epoch. The second half keeps out a follower that could otherwise
/// be elected before it fetched committed data between the high watermark
/// and the leader's log end.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_follower_in_sync(facts: IsrCandidateFacts) -> bool {
    pearlite! {
        facts.follower_log_end@ >= facts.leader_high_watermark@
            && match facts.leader_epoch_start {
                Some(start) => facts.follower_log_end@ >= start@,
                None => false,
            }
    }
}

/// Decide whether one candidate belongs in the next ISR proposal.
///
/// The leader always stays, and a member that is no longer assigned leaves.
/// An in-sync follower stays exactly while Kafka's `ReplicaState.isCaughtUp`
/// holds, which is `Partition.getOutOfSyncReplicas`: a follower that keeps
/// fetching but never catches up leaves once its last caught-up time is older
/// than the bound. An out-of-sync follower joins exactly when Kafka's
/// `Partition.needsExpandIsr` would add it: it is ISR-eligible
/// ([`replica_isr_eligible`]) and in sync by `Partition.isFollowerInSync`.
#[ensures(match facts.role {
    IsrMemberRole::Unassigned => !result,
    IsrMemberRole::Leader => result,
    IsrMemberRole::InSyncFollower => result == kafka_replica_is_caught_up(
        facts.follower_log_end@, facts.leader_log_end@, facts.caught_up_within_lag),
    IsrMemberRole::OutOfSyncFollower => result == (
        kafka_is_replica_isr_eligible(facts.eligibility) && kafka_is_follower_in_sync(facts)),
})]
#[must_use]
pub fn isr_candidate_selected(facts: IsrCandidateFacts) -> bool {
    match facts.role {
        IsrMemberRole::Unassigned => false,
        IsrMemberRole::Leader => true,
        IsrMemberRole::InSyncFollower => {
            facts.follower_log_end == facts.leader_log_end || facts.caught_up_within_lag
        }
        IsrMemberRole::OutOfSyncFollower => {
            let in_sync = facts.follower_log_end >= facts.leader_high_watermark
                && match facts.leader_epoch_start {
                    Some(start) => facts.follower_log_end >= start,
                    None => false,
                };
            in_sync && replica_isr_eligible(facts.eligibility)
        }
    }
}

/// Kafka's `Partition.isUnderMinIsr` on the leader.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_is_under_min_isr(facts: HighWatermarkFacts) -> bool {
    pearlite! { facts.isr_size@ < facts.effective_min_isr@ }
}

/// Whether `replica` holds the high watermark back: it is in the ISR, or, in
/// Kafka's `shouldWaitForReplicaToJoinIsr`, it is caught up and ISR-eligible,
/// so the watermark waits for it rather than run away from a follower that is
/// about to join.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_holds_watermark(replica: HwmReplica, leader_log_end: Int) -> bool {
    pearlite! {
        replica.in_isr
            || (kafka_replica_is_caught_up(replica.log_end@, leader_log_end, replica.caught_up_within_lag)
                && kafka_is_replica_isr_eligible(replica.eligibility))
    }
}

/// The candidate Kafka's `maybeIncrementLeaderHW` loop builds over the first
/// `n` replicas: the leader's log end, lowered to every log end of a replica
/// that holds the watermark back.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(n)]
fn kafka_watermark_candidate(leader_log_end: Int, replicas: Seq<HwmReplica>, n: Int) -> Int {
    pearlite! {
        if n <= 0 {
            leader_log_end
        } else {
            if kafka_holds_watermark(replicas[n - 1], leader_log_end)
                && replicas[n - 1].log_end@ < kafka_watermark_candidate(leader_log_end, replicas, n - 1)
            {
                replicas[n - 1].log_end@
            } else {
                kafka_watermark_candidate(leader_log_end, replicas, n - 1)
            }
        }
    }
}

/// Kafka's `Partition.maybeIncrementLeaderHW` followed by
/// `UnifiedLog.maybeIncrementHighWatermark`: nothing moves while the ISR is
/// under min ISR, and otherwise the watermark rises to the candidate if that
/// is higher and never falls.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_leader_high_watermark(facts: HighWatermarkFacts, replicas: Seq<HwmReplica>) -> Int {
    pearlite! {
        if kafka_is_under_min_isr(facts) {
            facts.current@
        } else if kafka_watermark_candidate(facts.leader_log_end@, replicas, replicas.len())
            > facts.current@
        {
            kafka_watermark_candidate(facts.leader_log_end@, replicas, replicas.len())
        } else {
            facts.current@
        }
    }
}

/// Compute the leader's next high watermark.
///
/// While the ISR is below the effective `min.insync.replicas` the watermark
/// stays where it is, so every record below it reached the HWM with at least
/// min ISR replicas holding it -- the fact KIP-966 builds the eligible-leader
/// set on. Otherwise it is the lowest log end among the leader, the ISR and
/// every caught-up ISR-eligible replica outside it, if that is higher than
/// the current watermark. A replica that has not fetched from this leader
/// (log end -1) in the ISR holds it where it is.
///
/// The caller keeps `current` at or below the leader's log end, as Kafka's
/// `UnifiedLog.truncateTo` does when it truncates.
#[requires(facts.current@ <= facts.leader_log_end@)]
#[ensures(result@ == kafka_leader_high_watermark(facts, replicas@))]
#[ensures(facts.current@ <= result@ && result@ <= facts.leader_log_end@)]
#[ensures(kafka_is_under_min_isr(facts) ==> result == facts.current)]
#[must_use]
pub fn leader_high_watermark(facts: HighWatermarkFacts, replicas: &[HwmReplica]) -> i64 {
    if facts.isr_size < facts.effective_min_isr {
        return facts.current;
    }
    let mut candidate = facts.leader_log_end;
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= replicas@.len()))]
    #[cfg_attr(creusot, invariant(candidate@ <= facts.leader_log_end@))]
    #[cfg_attr(creusot, invariant(candidate@
        == kafka_watermark_candidate(facts.leader_log_end@, replicas@, i@)))]
    #[cfg_attr(creusot, variant(replicas@.len() - i@))]
    while i < replicas.len() {
        let replica = replicas[i];
        let caught_up = replica.log_end == facts.leader_log_end || replica.caught_up_within_lag;
        let holds = replica.in_isr || (caught_up && replica_isr_eligible(replica.eligibility));
        if holds && replica.log_end < candidate {
            candidate = replica.log_end;
        }
        i += 1;
    }
    if candidate > facts.current {
        candidate
    } else {
        facts.current
    }
}

/// Report a proposal only when at least one unique member is added or removed.
#[ensures(result == (removed@ > 0 || added@ > 0))]
#[must_use]
pub fn isr_proposal_changed(removed: usize, added: usize) -> bool { removed > 0 || added > 0 }
