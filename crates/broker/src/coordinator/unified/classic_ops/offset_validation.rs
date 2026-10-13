//! Classic-membership validation for an `OffsetCommit` and a
//! `TxnOffsetCommit`.
//!
//! [`validate_offset_commit`] is Kafka's `ClassicGroup.validateOffsetCommit`
//! with `isTransactional = false`, and [`refresh_committer_session`] is the
//! session refresh that `OffsetMetadataManager.validateOffsetCommit` runs
//! after it. [`validate_commit`] is the transactional rule.

use {
    crate::{
        codes,
        coordinator::unified::classic_state::{ClassicGroup as ClassicState, GroupState},
    },
    std::time::Instant,
};

/// Kafka's `ClassicGroup.validateOffsetCommit` for an `OffsetCommit`
/// (`isTransactional = false`).
///
/// A negative generation commits on an `Empty` group: that is the admin client
/// or a consumer that does not use group management. Any member id, instance
/// id, or generation at or above zero goes through `validateMember` and must
/// name the current generation. A commit that names none of them on a group
/// that has members answers `UNKNOWN_MEMBER_ID`. A valid member answers
/// `REBALANCE_IN_PROGRESS` while the group is `CompletingRebalance`.
///
/// # Errors
/// Returns the Kafka error code of the failed check.
pub(crate) fn validate_offset_commit(
    state: &ClassicState,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_id: i32,
) -> Result<(), i16> {
    let empty = matches!(state.state, GroupState::Empty);
    if generation_id < 0 && empty {
        return Ok(());
    }
    if generation_id >= 0 || !member_id.is_empty() || group_instance_id.is_some() {
        state.validate_member(member_id, group_instance_id)?;
        if generation_id != state.generation_id {
            return Err(codes::ILLEGAL_GENERATION);
        }
    } else if !empty {
        return Err(codes::UNKNOWN_MEMBER_ID);
    }
    if matches!(state.state, GroupState::CompletingRebalance) {
        return Err(codes::REBALANCE_IN_PROGRESS);
    }
    Ok(())
}

/// Refreshes the session of the member that committed, as
/// `OffsetMetadataManager.validateOffsetCommit` does through
/// `rescheduleClassicGroupMemberHeartbeat` when the group is `Stable` or
/// `PreparingRebalance`. A member id the group does not hold changes nothing.
pub(crate) fn refresh_committer_session(state: &mut ClassicState, member_id: &str) {
    if !matches!(
        state.state,
        GroupState::Stable | GroupState::PreparingRebalance
    ) {
        return;
    }
    if let Some(member) = state.members.get_mut(member_id) {
        member.last_heartbeat = Instant::now();
    }
}

/// The classic fence of a `TxnOffsetCommit`. It returns `Some(code)` to
/// reject.
pub(crate) fn validate_commit(
    state: &ClassicState,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_id: i32,
) -> Option<i16> {
    if member_id.is_empty() && group_instance_id.is_none() {
        return None; // simple consumer
    }
    if let Some(iid) = group_instance_id {
        match state.current_member_id_for_instance(iid) {
            None => return Some(codes::UNKNOWN_MEMBER_ID),
            Some(pinned) => {
                if !member_id.is_empty() && pinned != member_id {
                    return Some(codes::FENCED_INSTANCE_ID);
                }
            }
        }
    } else if !state.members.contains_key(member_id) {
        return Some(codes::UNKNOWN_MEMBER_ID);
    }
    if state.generation_id != generation_id {
        return Some(codes::ILLEGAL_GENERATION);
    }
    None
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::coordinator::unified::classic_ops::test_support::stable_two_member_group,
        assert2::assert, std::time::Duration,
    };

    #[test]
    fn validate_commit_branches() {
        let mut g = stable_two_member_group();
        g.state = GroupState::Stable;
        for (member, instance, gen_id, want) in [
            // Simple consumer (no member, no instance) → allowed.
            ("", None, -1, None),
            // Unknown member → UNKNOWN_MEMBER_ID.
            (
                "ghost",
                None,
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
            // Wrong generation → ILLEGAL_GENERATION.
            (
                "m1",
                None,
                g.generation_id + 9,
                Some(codes::ILLEGAL_GENERATION),
            ),
            // Correct → allowed.
            ("m1", None, g.generation_id, None),
            // Instance set but unknown → UNKNOWN_MEMBER_ID.
            (
                "",
                Some("nope"),
                g.generation_id,
                Some(codes::UNKNOWN_MEMBER_ID),
            ),
        ] {
            assert!(validate_commit(&g, member, instance, gen_id) == want);
        }
    }

    /// One row of the `OffsetCommit` table: the group state, the request's
    /// member id, instance id and generation (`None` is the group's current
    /// generation), and the result.
    struct OffsetCase {
        state: GroupState,
        member: &'static str,
        instance: Option<&'static str>,
        generation: Option<i32>,
        want: Result<(), i16>,
    }

    /// Kafka's `ClassicGroup.validateOffsetCommit` with
    /// `isTransactional = false`, row by row. The group holds `m1` and `m2`,
    /// and the static instance `i1` is pinned to `m1`.
    #[test]
    fn offset_commit_follows_kafka_classic_group_rule() {
        let cases: [OffsetCase; 16] = [
            // The admin client on an empty group commits.
            OffsetCase {
                state: GroupState::Empty,
                member: "",
                instance: None,
                generation: Some(-1),
                want: Ok(()),
            },
            // A negative generation on an empty group commits, whatever it
            // names.
            OffsetCase {
                state: GroupState::Empty,
                member: "ghost",
                instance: None,
                generation: Some(-1),
                want: Ok(()),
            },
            // The admin client on a group with members is refused.
            OffsetCase {
                state: GroupState::Stable,
                member: "",
                instance: None,
                generation: Some(-1),
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            OffsetCase {
                state: GroupState::PreparingRebalance,
                member: "",
                instance: None,
                generation: Some(-1),
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            // A generation at or above zero with no member id is validated.
            OffsetCase {
                state: GroupState::Stable,
                member: "",
                instance: None,
                generation: None,
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            OffsetCase {
                state: GroupState::Empty,
                member: "",
                instance: None,
                generation: Some(0),
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            // An unknown member is refused.
            OffsetCase {
                state: GroupState::Stable,
                member: "ghost",
                instance: None,
                generation: None,
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            // A known member with a negative generation is validated.
            OffsetCase {
                state: GroupState::Stable,
                member: "m1",
                instance: None,
                generation: Some(-1),
                want: Err(codes::ILLEGAL_GENERATION),
            },
            // A known member on another generation is refused.
            OffsetCase {
                state: GroupState::Stable,
                member: "m1",
                instance: None,
                generation: Some(99),
                want: Err(codes::ILLEGAL_GENERATION),
            },
            // A known member on the current generation commits.
            OffsetCase {
                state: GroupState::Stable,
                member: "m1",
                instance: None,
                generation: None,
                want: Ok(()),
            },
            OffsetCase {
                state: GroupState::PreparingRebalance,
                member: "m1",
                instance: None,
                generation: None,
                want: Ok(()),
            },
            // ... but not while the group waits for the leader's SyncGroup.
            OffsetCase {
                state: GroupState::CompletingRebalance,
                member: "m1",
                instance: None,
                generation: None,
                want: Err(codes::REBALANCE_IN_PROGRESS),
            },
            // The instance pinned to another member id fences the request.
            OffsetCase {
                state: GroupState::Stable,
                member: "",
                instance: Some("i1"),
                generation: None,
                want: Err(codes::FENCED_INSTANCE_ID),
            },
            OffsetCase {
                state: GroupState::Stable,
                member: "m2",
                instance: Some("i1"),
                generation: None,
                want: Err(codes::FENCED_INSTANCE_ID),
            },
            // An instance no member holds is unknown.
            OffsetCase {
                state: GroupState::Stable,
                member: "m1",
                instance: Some("i9"),
                generation: None,
                want: Err(codes::UNKNOWN_MEMBER_ID),
            },
            OffsetCase {
                state: GroupState::Stable,
                member: "m1",
                instance: Some("i1"),
                generation: None,
                want: Ok(()),
            },
        ];
        let mut group = stable_two_member_group();
        group.static_members.insert("i1".into(), "m1".into());
        let current = group.generation_id;
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for OffsetCase {
            state,
            member,
            instance,
            generation,
            want,
        } in cases
        {
            group.state = state;
            let generation = generation.unwrap_or(current);
            let got = validate_offset_commit(&group, member, instance, generation);
            actual.push((state, member, instance, generation, got));
            expected.push((state, member, instance, generation, want));
        }
        assert!(actual == expected);
    }

    /// The commit refreshes the member's session only in `Stable` and
    /// `PreparingRebalance`.
    #[test]
    fn commit_refreshes_the_session_in_stable_and_preparing_rebalance() {
        let stale = Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (state, refreshed) in [
            (GroupState::Empty, false),
            (GroupState::PreparingRebalance, true),
            (GroupState::CompletingRebalance, false),
            (GroupState::Stable, true),
        ] {
            let mut group = stable_two_member_group();
            group.state = state;
            group.members.get_mut("m1").unwrap().last_heartbeat = stale;
            refresh_committer_session(&mut group, "m1");
            refresh_committer_session(&mut group, "ghost");
            actual.push((state, group.members["m1"].last_heartbeat > stale));
            expected.push((state, refreshed));
        }
        assert!(actual == expected);
    }
}
