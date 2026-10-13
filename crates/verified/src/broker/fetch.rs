use {
    super::{
        FetchVisibility, FetchWatermarks, PreferredLeaderChange, ReplicaFetchFacts,
        ReplicaFetchMutation,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A row answers a request this follower no longer stands behind: the epoch
/// moved while it was in flight, the replication target changed, or the leader
/// names a different target than the one this follower fetched from.
pub fn replica_fetch_fenced(facts: ReplicaFetchFacts) -> bool {
    pearlite! {
        facts.request_leader_epoch@ != facts.current_leader_epoch@
            || !facts.target_matches
            || !facts.reported_target_matches
    }
}

/// Fence one follower Fetch response row against its in-flight request epoch,
/// its current metadata target and any reported leader identity; then select
/// one exclusive response action.
///
/// A follower's request covers every partition it follows on one leader, so
/// the caller applies this once per row of the answer, with that row's own
/// partition configuration and the epoch it was asked under. The guarantee is
/// therefore per partition and does not weaken as a response carries more of
/// them: a fenced row mutates nothing, whatever the rows beside it say. An
/// unfenced row is retried on an error, truncates on a KIP-320 divergence
/// (which Kafka sends with no records), and appends otherwise.
#[ensures(result == if replica_fetch_fenced(facts) {
    ReplicaFetchMutation::Reject
} else if facts.error_code@ != 0 {
    ReplicaFetchMutation::Retry
} else if facts.diverging_epoch@ >= 0 {
    ReplicaFetchMutation::Truncate
} else {
    ReplicaFetchMutation::Append
})]
#[must_use]
pub fn replica_fetch_mutation(facts: ReplicaFetchFacts) -> ReplicaFetchMutation {
    if facts.request_leader_epoch != facts.current_leader_epoch
        || !facts.target_matches
        || !facts.reported_target_matches
    {
        ReplicaFetchMutation::Reject
    } else if facts.error_code != 0 {
        ReplicaFetchMutation::Retry
    } else if facts.diverging_epoch >= 0 {
        ReplicaFetchMutation::Truncate
    } else {
        ReplicaFetchMutation::Append
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// KIP-460's preferred election: leadership moves to the first assigned
/// replica, and only when that replica is alive, in the ISR, and able to lead.
pub fn preferred_change_eligible(change: PreferredLeaderChange) -> bool {
    pearlite! {
        change.preferred_replica == Some(change.new_leader)
            && change.leader_in_isr
            && change.leader_alive
            && !change.leader_is_witness
    }
}

/// Admit one preferred-leader rebalance batch only when it is nonempty, within
/// the per-tick election cap, and every change in it is a KIP-460 preferred
/// election.
///
/// There is no imbalance-ratio threshold among the facts. That gate is the
/// `ZooKeeper` controller's `leader.imbalance.per.broker.percentage`; the
/// `KRaft` controller krabka follows restores every partition whose preferred
/// replica is eligible, and bounds the work by a per-tick election count
/// (`QuorumController.MAX_ELECTIONS_PER_IMBALANCE`), which is `max_changes`.
#[ensures(result == (
    0 < changes@.len()
        && changes@.len() <= max_changes@
        && forall<i: Int> 0 <= i && i < changes@.len()
            ==> preferred_change_eligible(changes@[i])
))]
#[must_use]
pub fn preferred_rebalance_admission(
    changes: &[PreferredLeaderChange],
    max_changes: usize,
) -> bool {
    // `len` rather than `is_empty`: Creusot specifies `<[T]>::len` only.
    let count = changes.len();
    if count == 0 || count > max_changes {
        return false;
    }
    let mut index = 0_usize;
    #[invariant(index@ <= changes@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < index@ ==> preferred_change_eligible(changes@[k]))]
    #[variant(changes@.len() - index@)]
    while index < changes.len() {
        let change = changes[index];
        let installs_preferred = match change.preferred_replica {
            Some(preferred) => preferred == change.new_leader,
            None => false,
        };
        if !installs_preferred
            || !change.leader_in_isr
            || !change.leader_alive
            || change.leader_is_witness
        {
            return false;
        }
        index += 1;
    }
    true
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The lower of two offsets.
pub fn offset_min(a: Int, b: Int) -> Int {
    pearlite! { if a <= b { a } else { b } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The exclusive upper offset one fetch may read.
///
/// A follower reads to the leader's log end (Kafka's `FetchIsolation.LOG_END`).
/// A consumer reads below the high watermark (`HIGH_WATERMARK`), below the
/// last stable offset as well under `read_committed` (`TXN_COMMITTED`), and,
/// on a scheduled topic, below KFC-1's delivery watermark too.
pub fn fetch_limit_model(is_follower: bool, read_committed: bool, w: FetchWatermarks) -> Int {
    pearlite! {
        if is_follower {
            w.log_end@
        } else if read_committed {
            offset_min(offset_min(w.hw@, w.deliverable@), w.lso@)
        } else {
            offset_min(w.hw@, w.deliverable@)
        }
    }
}

/// Compute Kafka's consumer/follower Fetch visibility window.
///
/// The reported watermarks are the partition's own, for every fetcher:
/// `response_hw` is the high watermark and `response_lso` is the last stable
/// offset, capped at the high watermark as Kafka's `UnifiedLog.lastStableOffset`
/// caps it. Kafka's `Partition.readRecords` reads both before the fetch and
/// puts them in the `LogReadInfo` whatever the isolation level and whoever is
/// fetching, so a follower learns the leader's committed bound rather than its
/// log end. That is what keeps a KIP-392 follower from exposing an offset the
/// leader may still truncate.
///
/// [`FetchWatermarks::deliverable`] is KFC-1's delivery watermark: the first
/// offset a consumer may not see yet, because the batch that starts there has
/// not reached its activation time. It caps a consumer, and it caps nothing
/// else:
///
/// - A follower is never gated. Replication carries a scheduled record to the
///   ISR, and it counts toward the high watermark, long before any consumer can
///   read it.
/// - `response_hw` and `response_lso` do not move with it, so consumer lag
///   stays honest and KIP-227 watermark monotonicity is untouched.
///
/// Offsets below the log start or above the log end are out of range.
/// An offset equal to the log end remains a valid empty fetch.
///
/// There is no precondition. The consumer bound is proved outright: whatever
/// the caller passes, a consumer never reads at or past the high watermark, the
/// delivery watermark, or, under `read_committed`, the last stable offset.
#[ensures(result.out_of_range == (fetch_offset@ < w.log_start@ || fetch_offset@ > w.log_end@))]
#[ensures(result.empty == (!result.out_of_range
    && fetch_offset@ >= if is_follower { w.log_end@ } else { offset_min(w.hw@, w.deliverable@) }))]
#[ensures(result.limit_offset@ == fetch_limit_model(is_follower, read_committed, w))]
#[ensures(!is_follower ==> result.limit_offset@ <= w.hw@
    && result.limit_offset@ <= w.deliverable@
    && (read_committed ==> result.limit_offset@ <= w.lso@))]
#[ensures(result.read_committed_aborts == (read_committed && !is_follower))]
#[ensures(result.effective_lso@ == if result.read_committed_aborts {
    offset_min(w.lso@, w.hw@)
} else {
    w.lso@
})]
#[ensures(result.response_hw == w.hw)]
#[ensures(result.response_lso@ == offset_min(w.lso@, w.hw@))]
#[must_use]
pub fn fetch_visibility(
    is_follower: bool,
    read_committed: bool,
    w: FetchWatermarks,
    fetch_offset: i64,
) -> FetchVisibility {
    // The delivery watermark caps a consumer and never a follower.
    let visible = if w.deliverable < w.hw {
        w.deliverable
    } else {
        w.hw
    };
    let stable = if w.lso < w.hw { w.lso } else { w.hw };
    let read_committed_aborts = read_committed && !is_follower;
    let limit_offset = if is_follower {
        w.log_end
    } else if read_committed && stable < visible {
        stable
    } else {
        visible
    };
    let upper_bound = if is_follower { w.log_end } else { visible };
    let out_of_range = fetch_offset < w.log_start || fetch_offset > w.log_end;
    FetchVisibility {
        out_of_range,
        empty: !out_of_range && fetch_offset >= upper_bound,
        limit_offset,
        effective_lso: if read_committed_aborts { stable } else { w.lso },
        read_committed_aborts,
        response_hw: w.hw,
        response_lso: stable,
    }
}
