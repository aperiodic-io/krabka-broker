use super::*;

/// `Replica.updateFetchStateOrThrow` over a leader whose log end was 100
/// at the follower's previous fetch and is 120 now.
#[yare::parameterized(
    reaches_current_log_end = { 120, 100, CaughtUpCredit::ThisFetch },
    reaches_previous_log_end = { 100, 100, CaughtUpCredit::PreviousFetch },
    below_both_log_ends = { 99, 100, CaughtUpCredit::Unchanged },
    first_fetch_after_leadership_change = { 0, -1, CaughtUpCredit::PreviousFetch },
)]
fn caught_up_credit_follows_kafkas_fetch_state_update(
    fetch_offset: i64,
    last_fetch_leader_log_end: i64,
    expected: CaughtUpCredit,
) {
    assert2::check!(
        follower_caught_up_credit(fetch_offset, 120, last_fetch_leader_log_end) == expected
    );
}

/// `Partition.getOutOfSyncReplicas` for the in-sync rows; krabka's
/// expansion rule for the out-of-sync rows.
fn member_facts(
    role: IsrMemberRole,
    log_end_matches_leader: bool,
    caught_up_within_lag: bool,
    fetch_within_lag: bool,
) -> IsrMemberFacts {
    IsrMemberFacts {
        role,
        log_end_matches_leader,
        caught_up_within_lag,
        fetch_within_lag,
    }
}

#[yare::parameterized(
    leader = { "the leader stays", member_facts(IsrMemberRole::Leader, false, false, false), true },
    reassigned_away = {
        "a reassigned-away member leaves",
        member_facts(IsrMemberRole::Unassigned, true, true, true),
        false,
    },
    idle_at_leader_end = {
        "an idle follower at the leader's log end stays",
        member_facts(IsrMemberRole::InSyncFollower, true, false, false),
        true,
    },
    recently_caught_up = {
        "a follower that caught up recently stays",
        member_facts(IsrMemberRole::InSyncFollower, false, true, true),
        true,
    },
    fetching_behind = {
        "a follower fetching but behind for too long leaves",
        member_facts(IsrMemberRole::InSyncFollower, false, false, true),
        false,
    },
    stopped_fetching = {
        "a follower that stopped fetching behind the leader leaves",
        member_facts(IsrMemberRole::InSyncFollower, false, false, false),
        false,
    },
    out_of_sync_caught_up = {
        "a follower that fetched and caught up rejoins",
        member_facts(IsrMemberRole::OutOfSyncFollower, false, true, true),
        true,
    },
    old_catchup_fetching = {
        "a follower caught up long ago but fetching again stays out",
        member_facts(IsrMemberRole::OutOfSyncFollower, true, false, true),
        false,
    },
    old_catchup_stopped = {
        "a follower that caught up but stopped fetching stays out",
        member_facts(IsrMemberRole::OutOfSyncFollower, false, true, false),
        false,
    },
)]
fn isr_maintenance_keeps_caught_up_followers_and_drops_the_rest(
    label: &str,
    member: IsrMemberFacts,
    expected: bool,
) {
    assert2::check!(isr_maintenance_selected(member) == expected, "{label}");
}
