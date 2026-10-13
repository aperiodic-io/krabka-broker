//! The classic `LeaveGroup` transition.
//!
//! `handle_leave` resolves each requested identity through the KIP-345 static
//! instance index or the member index, removes the members it resolved, and
//! reopens a rebalance when a survivor remains in a `Stable` or
//! `CompletingRebalance` group, as Kafka's `classicGroupLeaveToClassicGroup`
//! does through `maybePrepareRebalanceOrCompleteJoin`. It returns the
//! per-member responses in request order.

use {
    crate::{
        codes,
        coordinator::unified::classic_state::{ClassicGroup as ClassicState, GroupState},
    },
    krabka_protocol::owned::{
        leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
    },
    std::time::{Duration, Instant},
};

struct MemberIdentityIn {
    member_id: String,
    group_instance_id: Option<String>,
}

/// Port of `handlers/leave_group.rs`. It removes the resolved members. If the
/// group was `Stable` or `CompletingRebalance` and members survive, it reopens
/// a rebalance and sets the deadline. It returns the per-member responses.
pub(crate) fn handle_leave(
    state: &mut ClassicState,
    req: &LeaveGroupRequest,
    version: i16,
    initial_rebalance_delay: Duration,
) -> Vec<MemberResponse> {
    let inputs: Vec<MemberIdentityIn> = if version >= 3 {
        req.members
            .iter()
            .map(|m| MemberIdentityIn {
                member_id: m.member_id.clone(),
                group_instance_id: m.group_instance_id.clone(),
            })
            .collect()
    } else {
        vec![MemberIdentityIn {
            member_id: req.member_id.clone(),
            group_instance_id: None,
        }]
    };

    let mut member_responses: Vec<MemberResponse> = Vec::with_capacity(inputs.len());
    let mut any_removed = false;
    for ident in &inputs {
        // Kafka checks `isPendingMember` before it validates the member: an id
        // that a `JoinGroup` answered with `MEMBER_ID_REQUIRED` and that has
        // not joined yet leaves cleanly, and counts as a valid leave below.
        if !ident.member_id.is_empty() && state.pending_members.remove(&ident.member_id).is_some() {
            any_removed = true;
            member_responses.push(MemberResponse {
                member_id: ident.member_id.clone(),
                group_instance_id: ident.group_instance_id.clone(),
                error_code: codes::NONE,
                ..Default::default()
            });
            continue;
        }
        let (resolved_id, code): (Option<String>, i16) =
            match (ident.group_instance_id.as_deref(), ident.member_id.as_str()) {
                (Some(iid), "") => match state.current_member_id_for_instance(iid) {
                    Some(pinned) => (Some(pinned.to_string()), codes::NONE),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (Some(iid), mid) => match state.current_member_id_for_instance(iid) {
                    Some(pinned) if pinned == mid => (Some(pinned.to_string()), codes::NONE),
                    Some(_) => (None, codes::FENCED_INSTANCE_ID),
                    None => (None, codes::UNKNOWN_MEMBER_ID),
                },
                (None, mid) => {
                    if state.members.contains_key(mid) {
                        (Some(mid.to_string()), codes::NONE)
                    } else {
                        (None, codes::UNKNOWN_MEMBER_ID)
                    }
                }
            };
        if let Some(id) = resolved_id {
            state.remove_member(&id);
            any_removed = true;
        }
        member_responses.push(MemberResponse {
            member_id: ident.member_id.clone(),
            group_instance_id: ident.group_instance_id.clone(),
            error_code: code,
            ..Default::default()
        });
    }
    if any_removed
        && !state.members.is_empty()
        && matches!(
            state.state,
            GroupState::Stable | GroupState::CompletingRebalance
        )
    {
        // A member left a live group: this is a membership-change rebalance,
        // not a start-from-empty herd, so the survivors eager-complete.
        state.prepare_rebalance(initial_rebalance_delay, Instant::now());
    }
    member_responses
}

#[cfg(test)]
mod tests {
    use {
        super::{super::test_support::prelude::*, *},
        assert2::assert,
        krabka_protocol::owned::leave_group_request::MemberIdentity,
    };

    /// (state before, state after) when one of two members leaves at v2.
    /// Kafka's `maybePrepareRebalanceOrCompleteJoin` reopens a rebalance from
    /// `Stable` and from `CompletingRebalance`, so the leader's `SyncGroup`
    /// cannot install an assignment that still names the departed member.
    #[test]
    fn leave_reopens_a_rebalance_from_stable_and_completing_rebalance() {
        let rows = [
            (GroupState::Stable, GroupState::PreparingRebalance),
            (
                GroupState::CompletingRebalance,
                GroupState::PreparingRebalance,
            ),
            (
                GroupState::PreparingRebalance,
                GroupState::PreparingRebalance,
            ),
        ];
        for (before, after) in rows {
            let mut g = stable_two_member_group();
            g.state = before;
            let req = LeaveGroupRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                ..Default::default()
            };
            let out = handle_leave(&mut g, &req, 2, Duration::from_secs(3));
            check!(
                out == vec![MemberResponse {
                    member_id: "m1".into(),
                    group_instance_id: None,
                    error_code: codes::NONE,
                    ..Default::default()
                }],
                "{before:?}"
            );
            check!(!g.members.contains_key("m1"), "{before:?}");
            check!(g.state == after, "{before:?}");
        }
    }

    /// Kafka's `classicGroupLeaveToClassicGroup` checks `isPendingMember`
    /// before it validates the member: an id that a `JoinGroup` answered with
    /// `MEMBER_ID_REQUIRED` leaves with `NONE` at every version and is
    /// forgotten, and its leave counts as a valid one, so a `Stable` or
    /// `CompletingRebalance` group prepares a rebalance for it.
    #[test]
    fn leave_of_a_pending_member_id_answers_none_and_forgets_the_id() {
        for (version, state) in [
            (2, GroupState::PreparingRebalance),
            (3, GroupState::PreparingRebalance),
            (3, GroupState::CompletingRebalance),
            (3, GroupState::Stable),
        ] {
            let mut g = stable_two_member_group();
            g.state = state;
            g.add_pending_member("pending".into(), Instant::now() + Duration::from_mins(1));
            let req = LeaveGroupRequest {
                group_id: "g".into(),
                member_id: "pending".into(),
                members: vec![MemberIdentity {
                    member_id: "pending".into(),
                    ..Default::default()
                }],
                ..Default::default()
            };

            let out = handle_leave(&mut g, &req, version, Duration::from_secs(3));

            check!(
                out == vec![MemberResponse {
                    member_id: "pending".into(),
                    group_instance_id: None,
                    error_code: codes::NONE,
                    ..Default::default()
                }],
                "v{version} {state:?}"
            );
            check!(g.pending_members.is_empty(), "v{version} {state:?}");
            check!(g.members.len() == 2, "v{version} {state:?}");
            check!(
                g.state == GroupState::PreparingRebalance,
                "v{version} {state:?}"
            );
        }
        // An id that is neither a member nor pending is still unknown.
        let mut g = stable_two_member_group();
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            member_id: "ghost".into(),
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 2, Duration::from_secs(3));
        check!(out[0].error_code == codes::UNKNOWN_MEMBER_ID);
        check!(g.state == GroupState::CompletingRebalance);
    }

    #[test]
    fn leave_v3_list_with_instance_resolution_and_unknown() {
        let mut g = ClassicState::new("g");
        let _ = handle_join(&mut g, &join_req("m1", Some("inst-a")), "h");
        let req = LeaveGroupRequest {
            group_id: "g".into(),
            members: vec![
                MemberIdentity {
                    member_id: String::new(),
                    group_instance_id: Some("inst-a".into()),
                    ..Default::default()
                },
                MemberIdentity {
                    member_id: "ghost".into(),
                    group_instance_id: None,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let out = handle_leave(&mut g, &req, 3, Duration::from_secs(3));
        assert!(out.len() == 2);
        check!(out[0].error_code == codes::NONE); // resolved via instance index
        check!(out[1].error_code == codes::UNKNOWN_MEMBER_ID);
        check!(!g.members.contains_key("m1"));
    }
}
