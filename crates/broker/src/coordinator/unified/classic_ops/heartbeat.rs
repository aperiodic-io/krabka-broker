//! The classic `Heartbeat` transition.
//!
//! `handle_heartbeat` is Kafka's `classicGroupHeartbeatToClassicGroup`: it
//! validates the member and the generation, then answers by group state and
//! refreshes the member's session in every state with members.

use {
    crate::{
        codes,
        coordinator::unified::classic_state::{ClassicGroup as ClassicState, GroupState},
    },
    krabka_protocol::owned::heartbeat_request::HeartbeatRequest,
    std::time::Instant,
};

/// Kafka's `classicGroupHeartbeatToClassicGroup`. It returns the error code.
///
/// The session refreshes in `PreparingRebalance`, `CompletingRebalance` and
/// `Stable`. `PreparingRebalance` answers `REBALANCE_IN_PROGRESS` so the member
/// joins again. `CompletingRebalance` answers `NONE`, because a consumer
/// heartbeats between its `JoinGroup` response and its `SyncGroup`.
pub(crate) fn handle_heartbeat(state: &mut ClassicState, req: &HeartbeatRequest) -> i16 {
    if let Err(code) = state.validate_member(&req.member_id, req.group_instance_id.as_deref()) {
        return code;
    }
    if state.generation_id != req.generation_id {
        return codes::ILLEGAL_GENERATION;
    }
    let code = match state.state {
        GroupState::Empty => return codes::UNKNOWN_MEMBER_ID,
        GroupState::PreparingRebalance => codes::REBALANCE_IN_PROGRESS,
        GroupState::CompletingRebalance | GroupState::Stable => codes::NONE,
    };
    if let Some(member) = state.members.get_mut(&req.member_id) {
        member.last_heartbeat = Instant::now();
    }
    code
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::coordinator::unified::classic_ops::test_support::stable_two_member_group,
        assert2::check, std::time::Duration,
    };

    /// #797: Kafka's `classicGroupHeartbeatToClassicGroup`, per group state
    /// and member check.
    #[test]
    fn heartbeat_answers_by_state_and_refreshes_the_session() {
        struct Row {
            name: &'static str,
            state: GroupState,
            member: &'static str,
            instance: Option<&'static str>,
            generation_offset: i32,
            want: i16,
            refreshed: bool,
        }
        let rows = [
            Row {
                name: "preparing",
                state: GroupState::PreparingRebalance,
                member: "m1",
                instance: None,
                generation_offset: 0,
                want: codes::REBALANCE_IN_PROGRESS,
                refreshed: true,
            },
            Row {
                name: "completing",
                state: GroupState::CompletingRebalance,
                member: "m1",
                instance: None,
                generation_offset: 0,
                want: codes::NONE,
                refreshed: true,
            },
            Row {
                name: "stable",
                state: GroupState::Stable,
                member: "m1",
                instance: None,
                generation_offset: 0,
                want: codes::NONE,
                refreshed: true,
            },
            Row {
                name: "unknown member",
                state: GroupState::Stable,
                member: "ghost",
                instance: None,
                generation_offset: 0,
                want: codes::UNKNOWN_MEMBER_ID,
                refreshed: false,
            },
            Row {
                name: "wrong generation",
                state: GroupState::Stable,
                member: "m1",
                instance: None,
                generation_offset: 9,
                want: codes::ILLEGAL_GENERATION,
                refreshed: false,
            },
            Row {
                name: "instance nobody holds",
                state: GroupState::Stable,
                member: "m1",
                instance: Some("i-expired"),
                generation_offset: 0,
                want: codes::UNKNOWN_MEMBER_ID,
                refreshed: false,
            },
            Row {
                name: "instance another member holds",
                state: GroupState::Stable,
                member: "m1",
                instance: Some("i2"),
                generation_offset: 0,
                want: codes::FENCED_INSTANCE_ID,
                refreshed: false,
            },
            Row {
                name: "own instance",
                state: GroupState::Stable,
                member: "m2",
                instance: Some("i2"),
                generation_offset: 0,
                want: codes::NONE,
                refreshed: true,
            },
        ];
        for row in rows {
            let mut g = stable_two_member_group();
            g.state = row.state;
            g.static_members.insert("i2".into(), "m2".into());
            let stale = Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
            for member in g.members.values_mut() {
                member.last_heartbeat = stale;
            }
            let req = HeartbeatRequest {
                group_id: "g".into(),
                generation_id: g.generation_id + row.generation_offset,
                member_id: row.member.into(),
                group_instance_id: row.instance.map(str::to_string),
                ..Default::default()
            };
            check!(handle_heartbeat(&mut g, &req) == row.want, "{}", row.name);
            let refreshed = g
                .members
                .get(row.member)
                .is_some_and(|m| m.last_heartbeat > stale);
            check!(refreshed == row.refreshed, "{}", row.name);
        }
    }

    /// #797: a heartbeat during `PreparingRebalance` keeps the member past
    /// the session timeout it would otherwise have run out.
    #[test]
    fn heartbeat_during_rebalance_keeps_the_member() {
        let mut g = stable_two_member_group();
        g.state = GroupState::PreparingRebalance;
        for member in g.members.values_mut() {
            member.session_timeout = Duration::from_secs(5);
            member.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(10)).unwrap();
        }
        let req = HeartbeatRequest {
            group_id: "g".into(),
            generation_id: g.generation_id,
            member_id: "m1".into(),
            ..Default::default()
        };
        check!(handle_heartbeat(&mut g, &req) == codes::REBALANCE_IN_PROGRESS);

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));

        check!(dropped == vec!["m2".to_string()]);
        check!(g.members.contains_key("m1"));
    }
}
