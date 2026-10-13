//! Fixture builders shared by the unit tests of the `classic_ops` submodules.
//!
//! The builders drive a [`ClassicState`] through the real `handle_join` and
//! `try_complete` transitions, so a test in any submodule starts from a group
//! that the production code itself produced.

use {
    super::join::{JoinAction, JoinContext, try_complete},
    crate::coordinator::unified::{
        classic_state::ClassicGroup as ClassicState, config::DEFAULT_CLASSIC_MAX_SIZE,
    },
    bytes::Bytes,
    krabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    std::time::{Duration, Instant},
};

pub(in crate::coordinator::unified::classic_ops) mod prelude {
    pub(in crate::coordinator::unified::classic_ops) use {
        super::{handle_join, join_req, stable_two_member_group},
        assert2::check,
    };
}

/// A `JoinGroup` v5 context from client `client-a` on `client_host`.
pub(super) fn join_ctx(client_host: &str) -> JoinContext<'_> {
    JoinContext {
        client_id: "client-a",
        client_host,
        version: 5,
        initial_rebalance_delay: Duration::from_secs(3),
        max_size: DEFAULT_CLASSIC_MAX_SIZE,
        now: Instant::now(),
    }
}

/// Joins `req` into `state` under the member id the request names.
///
/// Test fixtures name their members, where the group generates member ids. A
/// dynamic member's id is registered as the pending id a `MEMBER_ID_REQUIRED`
/// round trip leaves behind. A new static member joins with an empty id, and
/// the generated id is then replaced by the named one.
pub(super) fn handle_join(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    client_host: &str,
) -> JoinAction {
    let mut req = req.clone();
    let ctx = join_ctx(client_host);
    if !req.member_id.is_empty() && !state.members.contains_key(&req.member_id) {
        match req.group_instance_id.clone() {
            None => {
                state.add_pending_member(req.member_id.clone(), ctx.now + Duration::from_mins(1));
            }
            Some(instance_id) if state.current_member_id_for_instance(&instance_id).is_none() => {
                let wanted = std::mem::take(&mut req.member_id);
                let outcome = super::join::handle_join(state, &mut req, &ctx);
                let awaiting = state.joined_this_round.contains(&req.member_id);
                state.replace_static_member(&instance_id, &req.member_id, &wanted);
                if awaiting {
                    state.mark_awaiting_join(&wanted);
                }
                return outcome.action;
            }
            Some(_) => {}
        }
    }
    super::join::handle_join(state, &mut req, &ctx).action
}

pub(super) fn join_req(member_id: &str, instance: Option<&str>) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: "g".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: member_id.into(),
        group_instance_id: instance.map(String::from),
        protocol_type: "consumer".into(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: Bytes::from_static(b"meta"),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A group in `CompletingRebalance` at generation 1 with members `m1`, the
/// leader, and `m2`.
pub(super) fn stable_two_member_group() -> ClassicState {
    let mut g = ClassicState::new("g");
    let _ = handle_join(&mut g, &join_req("m1", None), "h");
    let _ = handle_join(&mut g, &join_req("m2", None), "h");
    try_complete(&mut g, Instant::now()).unwrap();
    g
}
