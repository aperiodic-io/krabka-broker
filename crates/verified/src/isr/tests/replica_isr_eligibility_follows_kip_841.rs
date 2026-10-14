use super::*;

/// `Partition.isReplicaIsrEligible` and `isBrokerEpochIsrEligible`.
#[yare::parameterized(
    alive_fetch_epoch = { ELIGIBLE, true },
    fetch_without_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), ..ELIGIBLE },
        true,
    },
    previous_broker_incarnation = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(6), ..ELIGIBLE },
        false,
    },
    no_fetch_for_leader = { IsrEligibilityFacts { fetch_broker_epoch: None, ..ELIGIBLE }, false },
    no_alive_epoch = {
        IsrEligibilityFacts { fetch_broker_epoch: Some(-1), alive_broker_epoch: None, ..ELIGIBLE },
        false,
    },
    fenced_broker = { IsrEligibilityFacts { fenced: true, ..ELIGIBLE }, false },
    controlled_shutdown = { IsrEligibilityFacts { shutting_down: true, ..ELIGIBLE }, false },
)]
fn replica_isr_eligibility_follows_kip_841(facts: IsrEligibilityFacts, expected: bool) {
    assert2::check!(replica_isr_eligible(facts) == expected);
}

/// One candidate at a leader whose log ends at 120, whose high watermark is
/// 100, and whose current epoch began at offset 90.
fn candidate_at(
    role: IsrMemberRole,
    follower_log_end: i64,
    caught_up_within_lag: bool,
) -> IsrCandidateFacts {
    IsrCandidateFacts {
        role,
        follower_log_end,
        leader_log_end: 120,
        leader_high_watermark: 100,
        leader_epoch_start: Some(90),
        caught_up_within_lag,
        eligibility: ELIGIBLE,
    }
}

/// In-sync rows follow `Partition.getOutOfSyncReplicas`; out-of-sync rows
/// follow `Partition.needsExpandIsr`.
#[yare::parameterized(
    leader = { "the leader stays", candidate_at(IsrMemberRole::Leader, -1, false), true },
    reassigned_away = {
        "a reassigned-away member leaves",
        candidate_at(IsrMemberRole::Unassigned, 120, true),
        false,
    },
    idle_at_leader_end = {
        "an idle follower at the leader's log end stays",
        candidate_at(IsrMemberRole::InSyncFollower, 120, false),
        true,
    },
    recently_caught_up = {
        "a follower that caught up recently stays",
        candidate_at(IsrMemberRole::InSyncFollower, 50, true),
        true,
    },
    behind_bound = {
        "a follower behind for longer than the bound leaves",
        candidate_at(IsrMemberRole::InSyncFollower, 119, false),
        false,
    },
    never_fetched = {
        "a follower that never fetched from this leader leaves after the bound",
        candidate_at(IsrMemberRole::InSyncFollower, -1, false),
        false,
    },
    at_high_watermark = {
        "a follower at the high watermark joins",
        candidate_at(IsrMemberRole::OutOfSyncFollower, 100, false),
        true,
    },
    short_of_high_watermark = {
        "a follower one short of the high watermark stays out",
        candidate_at(IsrMemberRole::OutOfSyncFollower, 99, true),
        false,
    },
    before_epoch_start = {
        "a follower past the high watermark but before the epoch start stays out",
        IsrCandidateFacts {
            leader_epoch_start: Some(110),
            ..candidate_at(IsrMemberRole::OutOfSyncFollower, 105, true)
        },
        false,
    },
    unknown_epoch_start = {
        "a leader that does not know its epoch start admits nobody",
        IsrCandidateFacts {
            leader_epoch_start: None,
            ..candidate_at(IsrMemberRole::OutOfSyncFollower, 120, true)
        },
        false,
    },
    fenced_follower = {
        "a fenced follower at the log end stays out",
        IsrCandidateFacts {
            eligibility: IsrEligibilityFacts {
                fenced: true,
                ..ELIGIBLE
            },
            ..candidate_at(IsrMemberRole::OutOfSyncFollower, 120, true)
        },
        false,
    },
    out_of_sync_never_fetched = {
        "a follower that never fetched from this leader stays out",
        candidate_at(IsrMemberRole::OutOfSyncFollower, -1, true),
        false,
    },
)]
fn isr_candidates_follow_kafkas_shrink_and_expand_rules(
    label: &str,
    facts: IsrCandidateFacts,
    expected: bool,
) {
    assert2::check!(isr_candidate_selected(facts) == expected, "{label}");
}
