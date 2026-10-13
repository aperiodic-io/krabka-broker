//! The classic `SyncGroup` transition.
//!
//! `handle_sync` is a port of Kafka's `classicGroupSyncToClassicGroup` and
//! `validateSyncGroup`. It fences the request, answers `REBALANCE_IN_PROGRESS`
//! while the group prepares a rebalance, installs the leader's assignments over
//! every current member in `CompletingRebalance`, and returns a [`SyncAction`]
//! that tells the actor whether to reply, to park the follower until the leader
//! arrives, or to drain the parked followers. `read_sync_result` reads one
//! member's installed assignment back out.

use {
    crate::{
        codes,
        coordinator::unified::{
            actor::SyncResult,
            classic_state::{ClassicGroup as ClassicState, GroupState},
        },
    },
    bytes::Bytes,
    krabka_protocol::owned::sync_group_request::SyncGroupRequest,
};

/// What the actor should do with a `ClassicSync`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SyncAction {
    /// Reply right away: a validation error, `REBALANCE_IN_PROGRESS` while the
    /// group prepares a rebalance, or the current assignment while it is
    /// `Stable`.
    Immediate(SyncResult),
    /// Park the follower until the leader's `SyncGroup` installs assignments.
    Park,
    /// The leader installed assignments. Reply with this result to the leader
    /// and drain the parked followers.
    LeaderInstalled(SyncResult),
}

/// Port of Kafka's `classicGroupSyncToClassicGroup`. It operates on
/// `ClassicState`.
pub(crate) fn handle_sync(state: &mut ClassicState, req: &SyncGroupRequest) -> SyncAction {
    if let Err(code) = validate_sync(state, req) {
        return SyncAction::Immediate(sync_err(code));
    }

    match state.state {
        // Kafka answers every `SyncGroup` in `PreparingRebalance` at once, the
        // leader's included: a late leader of the previous round must not end
        // the round that gathers members now. An `Empty` group has no member,
        // so `validate_sync` has already answered `UNKNOWN_MEMBER_ID`.
        GroupState::PreparingRebalance | GroupState::Empty => {
            SyncAction::Immediate(sync_err(codes::REBALANCE_IN_PROGRESS))
        }
        // Only the leader's `SyncGroup` in `CompletingRebalance` installs
        // assignments. A follower waits for it. Either way the member has now
        // sent its `SyncGroup`, which is Kafka's `removePendingSyncMember`.
        GroupState::CompletingRebalance => {
            state.remove_pending_sync_member(&req.member_id);
            if state.leader_id.as_deref() == Some(&req.member_id) {
                install_leader_assignments(state, req);
                SyncAction::LeaderInstalled(read_current(state, &req.member_id))
            } else {
                SyncAction::Park
            }
        }
        // In `Stable` every member, the leader included, reads its current
        // assignment. A KIP-814 leader that skipped the assignment sends none.
        GroupState::Stable => {
            state.remove_pending_sync_member(&req.member_id);
            SyncAction::Immediate(read_current(state, &req.member_id))
        }
    }
}

/// Kafka's `validateSyncGroup`: the member and instance
/// (`ClassicGroup.validateMember`), then the generation, then the protocol type
/// and name that the request names against the group's.
fn validate_sync(state: &ClassicState, req: &SyncGroupRequest) -> Result<(), i16> {
    state.validate_member(&req.member_id, req.group_instance_id.as_deref())?;
    if state.generation_id != req.generation_id {
        return Err(codes::ILLEGAL_GENERATION);
    }
    if is_protocol_inconsistent(req.protocol_type.as_deref(), state.protocol_type.as_deref())
        || is_protocol_inconsistent(req.protocol_name.as_deref(), state.protocol_name.as_deref())
    {
        return Err(codes::INCONSISTENT_GROUP_PROTOCOL);
    }
    Ok(())
}

/// Kafka's `isProtocolInconsistent`: both sides are set and differ.
fn is_protocol_inconsistent(requested: Option<&str>, group: Option<&str>) -> bool {
    matches!((requested, group), (Some(requested), Some(group)) if requested != group)
}

/// Installs the leader's assignments over every current member. A member that
/// the leader omitted gets an empty assignment instead of keeping bytes from the
/// previous generation, as Kafka's `membersWithMissingAssignment`.
fn install_leader_assignments(state: &mut ClassicState, req: &SyncGroupRequest) {
    let supplied: std::collections::HashMap<&str, &Bytes> = req
        .assignments
        .iter()
        .map(|a| (a.member_id.as_str(), &a.assignment))
        .collect();
    let assignments = state
        .members
        .keys()
        .map(|member_id| {
            (
                member_id.clone(),
                supplied
                    .get(member_id.as_str())
                    .map_or_else(Bytes::new, |assignment| (*assignment).clone()),
            )
        })
        .collect();
    state.install_assignments(assignments);
}

/// One member's current assignment with the group's protocol type and name.
fn read_current(state: &ClassicState, member_id: &str) -> SyncResult {
    read_sync_result(
        state,
        member_id,
        state.protocol_type.clone(),
        state.protocol_name.clone(),
    )
}

/// Read back one member's installed assignment. It returns
/// `REBALANCE_IN_PROGRESS`, with no protocol fields, if the group is not
/// `Stable`.
pub(crate) fn read_sync_result(
    state: &ClassicState,
    member_id: &str,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
) -> SyncResult {
    if !matches!(state.state, GroupState::Stable) {
        return sync_err(codes::REBALANCE_IN_PROGRESS);
    }
    let assignment = state
        .members
        .get(member_id)
        .and_then(|m| m.assignment.clone())
        .unwrap_or_default();
    SyncResult {
        error_code: codes::NONE,
        assignment,
        protocol_type,
        protocol_name,
    }
}

/// Kafka's error replies set only `error_code`: the protocol type and name
/// stay null and the assignment stays empty.
fn sync_err(code: i16) -> SyncResult {
    SyncResult {
        error_code: code,
        ..SyncResult::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::coordinator::unified::classic_ops::test_support::prelude::*,
        assert2::assert, krabka_protocol::owned::sync_group_request::SyncGroupRequestAssignment,
    };

    fn sync_req(member_id: &str, generation: i32) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: member_id.into(),
            ..Default::default()
        }
    }

    fn ok(assignment: &'static [u8]) -> SyncResult {
        SyncResult {
            error_code: codes::NONE,
            assignment: Bytes::from_static(assignment),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
        }
    }

    fn err(code: i16) -> SyncResult {
        SyncResult {
            error_code: code,
            ..SyncResult::default()
        }
    }

    /// The group state a `handle_sync` table row starts from.
    #[derive(Clone, Copy, Debug)]
    enum Start {
        /// `stable_two_member_group`: generation 1, leader `m1`, awaiting the
        /// leader's `SyncGroup`.
        Completing,
        /// `Completing` after the leader installed `L` for `m1` and `F` for
        /// `m2`.
        Stable,
        /// `Stable` after `m3` joined: the round that opened keeps generation
        /// 1, so a `SyncGroup` of that generation still passes validation.
        Preparing,
    }

    fn group(start: Start) -> ClassicState {
        let mut g = stable_two_member_group();
        if matches!(start, Start::Stable | Start::Preparing) {
            g.install_assignments(std::collections::HashMap::from([
                ("m1".to_string(), Bytes::from_static(b"L")),
                ("m2".to_string(), Bytes::from_static(b"F")),
            ]));
        }
        if matches!(start, Start::Preparing) {
            let _ = handle_join(&mut g, &join_req("m3", None), "h");
            assert!(g.state == GroupState::PreparingRebalance);
        }
        g
    }

    /// Kafka's `classicGroupSyncToClassicGroup` and `validateSyncGroup`, row by
    /// row: the whole action and the group state after it.
    #[test]
    fn sync_follows_kafka_state_and_validation_rules() {
        struct Row {
            name: &'static str,
            start: Start,
            member: &'static str,
            instance: Option<&'static str>,
            generation_delta: i32,
            protocol_type: Option<&'static str>,
            protocol_name: Option<&'static str>,
            want: SyncAction,
            state_after: GroupState,
        }
        let row = |name, start, member, want, state_after| Row {
            name,
            start,
            member,
            instance: None,
            generation_delta: 0,
            protocol_type: None,
            protocol_name: None,
            want,
            state_after,
        };
        let immediate = |code| SyncAction::Immediate(err(code));
        let rows = [
            // #793: a late leader of the previous round in `PreparingRebalance`
            // gets `REBALANCE_IN_PROGRESS` and installs nothing.
            row(
                "preparing leader",
                Start::Preparing,
                "m1",
                immediate(codes::REBALANCE_IN_PROGRESS),
                GroupState::PreparingRebalance,
            ),
            row(
                "preparing follower",
                Start::Preparing,
                "m2",
                immediate(codes::REBALANCE_IN_PROGRESS),
                GroupState::PreparingRebalance,
            ),
            row(
                "completing follower parks",
                Start::Completing,
                "m2",
                SyncAction::Park,
                GroupState::CompletingRebalance,
            ),
            row(
                "completing leader installs",
                Start::Completing,
                "m1",
                SyncAction::LeaderInstalled(ok(b"new-L")),
                GroupState::Stable,
            ),
            row(
                "stable follower reads current",
                Start::Stable,
                "m2",
                SyncAction::Immediate(ok(b"F")),
                GroupState::Stable,
            ),
            row(
                "stable leader reads current",
                Start::Stable,
                "m1",
                SyncAction::Immediate(ok(b"L")),
                GroupState::Stable,
            ),
            Row {
                protocol_type: Some("consumer"),
                protocol_name: Some("range"),
                ..row(
                    "matching protocol",
                    Start::Stable,
                    "m2",
                    SyncAction::Immediate(ok(b"F")),
                    GroupState::Stable,
                )
            },
            Row {
                protocol_type: Some("connect"),
                ..row(
                    "other protocol type",
                    Start::Completing,
                    "m1",
                    immediate(codes::INCONSISTENT_GROUP_PROTOCOL),
                    GroupState::CompletingRebalance,
                )
            },
            Row {
                protocol_name: Some("roundrobin"),
                ..row(
                    "other protocol name",
                    Start::Stable,
                    "m2",
                    immediate(codes::INCONSISTENT_GROUP_PROTOCOL),
                    GroupState::Stable,
                )
            },
            Row {
                generation_delta: 1,
                protocol_type: Some("connect"),
                ..row(
                    "generation before protocol",
                    Start::Stable,
                    "m2",
                    immediate(codes::ILLEGAL_GENERATION),
                    GroupState::Stable,
                )
            },
            row(
                "unknown member",
                Start::Stable,
                "ghost",
                immediate(codes::UNKNOWN_MEMBER_ID),
                GroupState::Stable,
            ),
            Row {
                instance: Some("i-unknown"),
                ..row(
                    "unknown instance",
                    Start::Preparing,
                    "m1",
                    immediate(codes::UNKNOWN_MEMBER_ID),
                    GroupState::PreparingRebalance,
                )
            },
        ];

        for r in rows {
            let mut g = group(r.start);
            let req = SyncGroupRequest {
                group_instance_id: r.instance.map(String::from),
                protocol_type: r.protocol_type.map(String::from),
                protocol_name: r.protocol_name.map(String::from),
                assignments: vec![
                    SyncGroupRequestAssignment {
                        member_id: "m1".into(),
                        assignment: Bytes::from_static(b"new-L"),
                        ..Default::default()
                    },
                    SyncGroupRequestAssignment {
                        member_id: "m2".into(),
                        assignment: Bytes::from_static(b"new-F"),
                        ..Default::default()
                    },
                ],
                ..sync_req(r.member, g.generation_id + r.generation_delta)
            };
            let before: Vec<_> = ["m1", "m2"].map(|m| g.members[m].assignment.clone()).into();

            let got = handle_sync(&mut g, &req);

            check!(got == r.want, "{}", r.name);
            check!(g.state == r.state_after, "{}", r.name);
            if !matches!(r.want, SyncAction::LeaderInstalled(_)) {
                let after: Vec<_> = ["m1", "m2"].map(|m| g.members[m].assignment.clone()).into();
                check!(after == before, "{}: assignments must not change", r.name);
            }
        }
    }

    #[test]
    fn sync_unknown_member_and_wrong_generation() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        // KIP-345, as Kafka's `ClassicGroup.validateMember`: an instance id no
        // member holds (a static member whose session expired) is unknown, and
        // an instance id another member holds is fenced.
        g.static_members.insert("i2".into(), "m2".into());
        for (member, instance, want) in [
            ("m1", "i-expired", codes::UNKNOWN_MEMBER_ID),
            ("m1", "i2", codes::FENCED_INSTANCE_ID),
        ] {
            let req = SyncGroupRequest {
                group_instance_id: Some(instance.into()),
                ..sync_req(member, cur_gen)
            };
            check!(
                handle_sync(&mut g, &req) == SyncAction::Immediate(err(want)),
                "{member} {instance}"
            );
        }
    }

    #[test]
    fn sync_leader_installs_follower_parks_then_reads() {
        let mut g = stable_two_member_group();
        let cur_gen = g.generation_id;
        let leader = g.leader_id.clone().unwrap();
        let follower = if leader == "m1" { "m2" } else { "m1" };
        // Follower before the leader syncs → Park (not yet Stable).
        assert!(matches!(
            handle_sync(&mut g, &sync_req(follower, cur_gen)),
            SyncAction::Park
        ));
        // Leader installs assignments.
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![
            SyncGroupRequestAssignment {
                member_id: leader.clone(),
                assignment: Bytes::from_static(b"L"),
                ..Default::default()
            },
            SyncGroupRequestAssignment {
                member_id: follower.into(),
                assignment: Bytes::from_static(b"F"),
                ..Default::default()
            },
        ];
        assert!(handle_sync(&mut g, &req) == SyncAction::LeaderInstalled(ok(b"L")));
        assert!(g.state == GroupState::Stable);
        // Now the follower (re-sync) reads its assignment immediately.
        assert!(
            handle_sync(&mut g, &sync_req(follower, cur_gen)) == SyncAction::Immediate(ok(b"F"))
        );
    }

    #[test]
    fn sync_leader_clears_omitted_member_assignment() {
        let mut g = stable_two_member_group();
        let generation = g.generation_id;
        let leader = g.leader_id.clone().unwrap();
        let omitted = if leader == "m1" { "m2" } else { "m1" };
        g.members.get_mut(omitted).unwrap().assignment = Some(Bytes::from_static(b"stale"));

        let mut req = sync_req(&leader, generation);
        req.assignments = vec![SyncGroupRequestAssignment {
            member_id: leader,
            assignment: Bytes::from_static(b"leader"),
            ..Default::default()
        }];
        assert!(matches!(
            handle_sync(&mut g, &req),
            SyncAction::LeaderInstalled(_)
        ));

        check!(g.members[omitted].assignment.as_deref() == Some(&b""[..]));
    }

    /// Outside `Stable`, the read answers `REBALANCE_IN_PROGRESS` with null
    /// protocol fields, as Kafka's error replies set only `error_code`.
    #[test]
    fn read_sync_result_rebalance_in_progress_when_not_stable() {
        let mut g = stable_two_member_group(); // CompletingRebalance, not Stable
        let r = read_sync_result(&g, "m1", Some("consumer".into()), Some("range".into()));
        assert!(r == err(codes::REBALANCE_IN_PROGRESS));
        // Drive to Stable, then it returns NONE.
        let leader = g.leader_id.clone().unwrap();
        let cur_gen = g.generation_id;
        let mut req = sync_req(&leader, cur_gen);
        req.assignments = vec![SyncGroupRequestAssignment {
            member_id: leader.clone(),
            assignment: Bytes::new(),
            ..Default::default()
        }];
        let _ = handle_sync(&mut g, &req);
        let r = read_sync_result(&g, &leader, None, None);
        assert!(r.error_code == codes::NONE);
    }

    /// Kafka's `removePendingSyncMember`: a member's own `SyncGroup` takes it
    /// off the set of members that owe one, in `CompletingRebalance` and in
    /// `Stable`, and the timer goes with the last of them. A `SyncGroup` that
    /// only answers `REBALANCE_IN_PROGRESS` takes nobody off.
    #[test]
    fn sync_group_removes_the_member_from_the_pending_sync_set() {
        let mut g = stable_two_member_group();
        let generation = g.generation_id;
        let pending = |g: &ClassicState| {
            let mut ids: Vec<String> = g.pending_sync_members.iter().cloned().collect();
            ids.sort_unstable();
            ids
        };
        check!(pending(&g) == ["m1", "m2"]);
        check!(g.sync_deadline.is_some());

        // The leader installs the assignments and the group is `Stable`, but
        // the follower still owes its `SyncGroup`, so the timer keeps running.
        let _ = handle_sync(&mut g, &sync_req("m1", generation));
        check!(g.state == GroupState::Stable);
        check!(pending(&g) == ["m2"]);
        check!(g.sync_deadline.is_some());

        // The follower reads its assignment from the `Stable` group.
        let _ = handle_sync(&mut g, &sync_req("m2", generation));
        check!(pending(&g).is_empty());
        check!(g.sync_deadline.is_none());

        let mut g = group(Start::Preparing);
        g.pending_sync_members.insert("m1".into());
        let generation = g.generation_id;
        let _ = handle_sync(&mut g, &sync_req("m1", generation));
        check!(pending(&g) == ["m1"]);
    }

    /// KIP-814: a leader that rejoined a `Stable` group with
    /// `skip_assignment` sends `SyncGroup` with no assignments, and reads
    /// its current one back instead of clearing every member's.
    #[test]
    fn leader_sync_in_stable_reads_current_assignment() {
        let mut g = group(Start::Stable);
        let generation = g.generation_id;
        check!(handle_sync(&mut g, &sync_req("m1", generation)) == SyncAction::Immediate(ok(b"L")));
        check!(g.members["m2"].assignment.as_deref() == Some(&b"F"[..]));
    }
}
