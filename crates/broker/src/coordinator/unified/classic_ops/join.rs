//! The classic `JoinGroup` transition.
//!
//! `handle_join` is a port of Kafka's `GroupMetadataManager`
//! `classicGroupJoinToClassicGroup` and the methods it calls: the group size
//! gate (`acceptJoiningMember`), the protocol gate (`supportsProtocols`), the
//! KIP-394 member-id bootstrap, the KIP-345 static-member replacement, and the
//! per-state rules for a member that joins again. It returns a [`JoinOutcome`]
//! that tells the actor whether to reply at once, persist and then reply, park
//! the reply, or complete the round now. `build_join_result` renders the reply
//! from post-rebalance state, and `try_complete` runs Kafka's
//! `completeClassicGroupJoin`.

use {
    crate::{
        codes,
        coordinator::unified::{
            actor::{JoinResult, JoinResultMember},
            classic_state::{ClassicGroup as ClassicState, GroupState, Member, select_protocol},
            persistence::MAX_STRING_BYTES,
        },
        metadata_epoch::next_i32,
    },
    bytes::Bytes,
    krabka_protocol::owned::join_group_request::JoinGroupRequest,
    std::time::{Duration, Instant},
    uuid::Uuid,
};

const DEFAULT_SESSION_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 60_000;

/// The first `JoinGroup` version that requires a known member id from a
/// dynamic member, Kafka's `JoinGroupRequest.requiresKnownMemberId` (KIP-394).
const FIRST_KNOWN_MEMBER_ID_VERSION: i16 = 4;

/// The first `JoinGroup` version with `SkipAssignment`, Kafka's
/// `JoinGroupRequest.supportsSkippingAssignment` (KIP-814).
const FIRST_SKIP_ASSIGNMENT_VERSION: i16 = 9;

/// The request-independent inputs of a `JoinGroup`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JoinContext<'a> {
    pub client_id: &'a str,
    pub client_host: &'a str,
    pub version: i16,
    /// `group.initial.rebalance.delay.ms`.
    pub initial_rebalance_delay: Duration,
    /// `group.max.size`.
    pub max_size: usize,
    pub now: Instant,
}

/// What the actor should do with a `ClassicJoin`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JoinAction {
    /// Reply right away with no state to persist.
    Immediate(JoinResult),
    /// A static member replaced its old member id in a `Stable` group
    /// without a rebalance. Persist the group metadata, then reply. If the
    /// write fails, roll the group back and reply with the error.
    PersistThenReply(JoinResult),
    /// Park the reply until the rebalance deadline or an early completion.
    Park,
    /// Every member has joined a round that did not open from `Empty`.
    /// Complete the rebalance now and drain all parked joiners.
    CompleteNow,
}

/// The result of [`handle_join`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct JoinOutcome {
    pub action: JoinAction,
    /// The old member id of a static member that joined again with an empty
    /// member id. Its parked `JoinGroup` and `SyncGroup` get
    /// `FENCED_INSTANCE_ID`, as Kafka's `replaceStaticMember` answers them.
    pub fenced_member: Option<String>,
}

impl JoinOutcome {
    fn of(action: JoinAction) -> Self {
        Self {
            action,
            fenced_member: None,
        }
    }

    fn error(error_code: i16, member_id: impl Into<String>) -> Self {
        Self::of(JoinAction::Immediate(JoinResult {
            error_code,
            member_id: member_id.into(),
            ..JoinResult::default()
        }))
    }
}

/// Kafka's `JoinGroupRequest.requiresKnownMemberId` (KIP-394): `true` for a
/// dynamic member that joins with no member id at v4 or later.
///
/// The coordinator answers such a join with `MEMBER_ID_REQUIRED` and the id
/// that the member must join with again. A classic group and a consumer group
/// that hosts classic members both apply this rule.
pub(crate) fn requires_known_member_id(req: &JoinGroupRequest, version: i16) -> bool {
    req.group_instance_id.is_none()
        && req.member_id.is_empty()
        && version >= FIRST_KNOWN_MEMBER_ID_VERSION
}

/// Kafka's `classicGroupJoinToClassicGroup`. On a new member id the request's
/// `member_id` becomes the id the group gave it, so the actor parks the reply
/// under that id.
pub(crate) fn handle_join(
    state: &mut ClassicState,
    req: &mut JoinGroupRequest,
    ctx: &JoinContext<'_>,
) -> JoinOutcome {
    if !accept_joining_member(state, &req.member_id, ctx.max_size) {
        state.remove_member(&req.member_id);
        JoinOutcome::error(codes::GROUP_MAX_SIZE_REACHED, "")
    } else if req.member_id.is_empty() {
        join_new_member(state, req, ctx)
    } else {
        join_existing_member(state, req, ctx)
    }
}

/// Kafka's `acceptJoiningMember`.
fn accept_joining_member(state: &ClassicState, member_id: &str, max_size: usize) -> bool {
    match state.state {
        GroupState::Empty => true,
        // The members that wait in `JoinGroup` count here: the group size can
        // be above the limit after the limit shrank, and this lets the last
        // members to join again be the ones turned away.
        GroupState::PreparingRebalance => {
            (state.members.contains_key(member_id) && state.joined_this_round.contains(member_id))
                || state.joined_this_round.len() < max_size
        }
        GroupState::CompletingRebalance | GroupState::Stable => {
            state.members.contains_key(member_id) || state.members.len() < max_size
        }
    }
}

fn supports_request_protocols(state: &ClassicState, req: &JoinGroupRequest) -> bool {
    state.supports_protocols(
        &req.protocol_type,
        req.protocols.iter().map(|p| p.name.as_str()),
    )
}

fn request_protocols(req: &JoinGroupRequest) -> Vec<(String, Bytes)> {
    req.protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.clone()))
        .collect()
}

fn session_timeout(req: &JoinGroupRequest) -> Duration {
    Duration::from_millis(
        u64::try_from(req.session_timeout_ms).unwrap_or(DEFAULT_SESSION_TIMEOUT_MS),
    )
}

fn rebalance_timeout(req: &JoinGroupRequest) -> Duration {
    Duration::from_millis(
        u64::try_from(req.rebalance_timeout_ms).unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS),
    )
}

/// Kafka's `classicGroupJoinNewMember`, `classicGroupJoinNewStaticMember` and
/// `classicGroupJoinNewDynamicMember`.
fn join_new_member(
    state: &mut ClassicState,
    req: &mut JoinGroupRequest,
    ctx: &JoinContext<'_>,
) -> JoinOutcome {
    if !supports_request_protocols(state, req) {
        return JoinOutcome::error(codes::INCONSISTENT_GROUP_PROTOCOL, "");
    }
    // Kafka's `ClassicGroup.generateMemberId`: the instance id or the client
    // id, a hyphen, and a fresh unique suffix. The prefix is not decoration:
    // `kafka-consumer-groups --describe` prints it as CONSUMER-ID.
    let prefix = req.group_instance_id.as_deref().unwrap_or(ctx.client_id);
    let new_member_id = format!("{prefix}-{}", Uuid::new_v4());
    // Kafka never bounds the id, and a prefix of 32731 bytes or more, which
    // the instance id and the header client id both admit, makes one that its
    // generated writer refuses to serialize, in the response and in the group
    // metadata record. There the join fails with the internal error; the id is
    // refused here, before it changes any state.
    if new_member_id.len() > MAX_STRING_BYTES {
        return JoinOutcome::error(codes::UNKNOWN_SERVER_ERROR, "");
    }
    if let Some(instance_id) = req.group_instance_id.clone() {
        if let Some(old_member_id) = state
            .current_member_id_for_instance(&instance_id)
            .map(str::to_string)
        {
            return update_static_member(
                state,
                req,
                ctx,
                &instance_id,
                old_member_id,
                new_member_id,
            );
        }
        req.member_id = new_member_id;
        return JoinOutcome::of(add_member_then_rebalance(state, req, ctx));
    }
    if requires_known_member_id(req, ctx.version) {
        state.add_pending_member(new_member_id.clone(), ctx.now + session_timeout(req));
        return JoinOutcome::error(codes::MEMBER_ID_REQUIRED, new_member_id);
    }
    req.member_id = new_member_id;
    JoinOutcome::of(add_member_then_rebalance(state, req, ctx))
}

/// Kafka's `classicGroupJoinExistingMember`.
fn join_existing_member(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    ctx: &JoinContext<'_>,
) -> JoinOutcome {
    let member_id = req.member_id.as_str();
    if !supports_request_protocols(state, req) {
        return JoinOutcome::error(codes::INCONSISTENT_GROUP_PROTOCOL, member_id);
    }
    if state.pending_members.contains_key(member_id) {
        // A pending member is never static. Kafka throws
        // `IllegalStateException` here, which its runtime answers with
        // `UNKNOWN_SERVER_ERROR`.
        if req.group_instance_id.is_some() {
            return JoinOutcome::error(codes::UNKNOWN_SERVER_ERROR, member_id);
        }
        return JoinOutcome::of(add_member_then_rebalance(state, req, ctx));
    }
    if let Err(error_code) = state.validate_member(member_id, req.group_instance_id.as_deref()) {
        return JoinOutcome::error(error_code, member_id);
    }
    let unchanged = state
        .members
        .get(member_id)
        .is_some_and(|m| m.protocols == request_protocols(req));
    let is_leader = state.leader_id.as_deref() == Some(member_id);
    let rebalance = match state.state {
        GroupState::PreparingRebalance => true,
        // A member that joins again with the same metadata, which it does when
        // it lost its `JoinGroup` response, gets the current generation.
        GroupState::CompletingRebalance => !unchanged,
        // The leader's `JoinGroup` always rebalances, so it can react to
        // changes that do not show in member metadata, such as new topics.
        GroupState::Stable => is_leader || !unchanged,
        GroupState::Empty => return JoinOutcome::error(codes::UNKNOWN_MEMBER_ID, member_id),
    };
    let action = if rebalance {
        update_member_then_rebalance(state, req, ctx)
    } else {
        JoinAction::Immediate(build_join_result(state, member_id))
    };
    JoinOutcome::of(action)
}

/// Kafka's `addMemberThenRebalanceOrCompleteJoin`, for `req.member_id`.
fn add_member_then_rebalance(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    ctx: &JoinContext<'_>,
) -> JoinAction {
    let mut member = Member::new(
        req.member_id.clone(),
        ctx.client_id.to_string(),
        ctx.client_host.to_string(),
        session_timeout(req),
        rebalance_timeout(req),
        request_protocols(req),
    )
    .with_instance_id(req.group_instance_id.clone());
    member.is_new = true;
    member.last_heartbeat = ctx.now;
    // A new member during the initial delay extends it once more.
    if state.state == GroupState::PreparingRebalance && state.rebalance_from_empty {
        state.new_member_added = true;
    }
    state.insert_joining_member(member, &req.protocol_type);
    prepare_rebalance_or_complete_join(state, &req.member_id, ctx)
}

/// Kafka's `updateMemberThenRebalanceOrCompleteJoin`.
fn update_member_then_rebalance(
    state: &mut ClassicState,
    req: &JoinGroupRequest,
    ctx: &JoinContext<'_>,
) -> JoinAction {
    state.update_joining_member(
        &req.member_id,
        request_protocols(req),
        rebalance_timeout(req),
        session_timeout(req),
    );
    prepare_rebalance_or_complete_join(state, &req.member_id, ctx)
}

/// Kafka's `maybePrepareRebalanceOrCompleteJoin`, with `member_id` waiting in
/// `JoinGroup` from here on.
fn prepare_rebalance_or_complete_join(
    state: &mut ClassicState,
    member_id: &str,
    ctx: &JoinContext<'_>,
) -> JoinAction {
    if state.can_rebalance() {
        let initial = state.state == GroupState::Empty;
        state.prepare_rebalance(ctx.initial_rebalance_delay, ctx.now);
        state.mark_awaiting_join(member_id);
        // Kafka's `maybeCompleteJoinElseSchedule`. An initial round always
        // waits out its delay.
        if !initial && state.has_all_members_joined() {
            JoinAction::CompleteNow
        } else {
            JoinAction::Park
        }
    } else {
        state.mark_awaiting_join(member_id);
        maybe_complete_join_phase(state)
    }
}

/// Kafka's `maybeCompleteJoinPhase`: a round that did not open from `Empty`
/// completes as soon as every member waits in `JoinGroup`.
fn maybe_complete_join_phase(state: &ClassicState) -> JoinAction {
    if state.state == GroupState::PreparingRebalance
        && !state.rebalance_from_empty
        && state.has_all_members_joined()
    {
        JoinAction::CompleteNow
    } else {
        JoinAction::Park
    }
}

/// Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin`.
fn update_static_member(
    state: &mut ClassicState,
    req: &mut JoinGroupRequest,
    ctx: &JoinContext<'_>,
    instance_id: &str,
    old_member_id: String,
    new_member_id: String,
) -> JoinOutcome {
    let current_leader = state.leader_id.clone();
    state.replace_static_member(instance_id, &old_member_id, &new_member_id);
    req.member_id.clone_from(&new_member_id);
    if let Some(member) = state.members.get_mut(&new_member_id) {
        member.last_heartbeat = ctx.now;
    }
    state.update_joining_member(
        &new_member_id,
        request_protocols(req),
        rebalance_timeout(req),
        session_timeout(req),
    );
    let action = match state.state {
        // The generation stays when the protocol the next generation would
        // select does not change. The new member id is persisted first.
        GroupState::Stable if state.protocol_name == select_protocol(&state.members) => {
            let is_leader = state.leader_id.as_deref() == Some(new_member_id.as_str());
            let result = if ctx.version >= FIRST_SKIP_ASSIGNMENT_VERSION {
                // KIP-814: the leader gets the member list but must not
                // assign again.
                JoinResult {
                    skip_assignment: is_leader,
                    ..build_join_result(state, &new_member_id)
                }
            } else {
                JoinResult {
                    members: Vec::new(),
                    leader: current_leader.unwrap_or_default(),
                    ..build_join_result(state, &new_member_id)
                }
            };
            JoinAction::PersistThenReply(result)
        }
        // `CompletingRebalance` rebalances again: the leader may already have
        // the old member id and would assign nothing to the new one.
        GroupState::Stable | GroupState::CompletingRebalance => {
            prepare_rebalance_or_complete_join(state, &new_member_id, ctx)
        }
        GroupState::PreparingRebalance => {
            state.mark_awaiting_join(&new_member_id);
            maybe_complete_join_phase(state)
        }
        // A group with a static member is never `Empty`. Kafka throws
        // `IllegalStateException`, which its runtime answers with
        // `UNKNOWN_SERVER_ERROR`.
        GroupState::Empty => JoinAction::Immediate(JoinResult {
            error_code: codes::UNKNOWN_SERVER_ERROR,
            member_id: new_member_id,
            ..JoinResult::default()
        }),
    };
    JoinOutcome {
        action,
        fenced_member: Some(old_member_id),
    }
}

/// Build a successful `JoinResult` for the current generation. The leader
/// gets the member list, and followers get an empty list.
pub(crate) fn build_join_result(state: &ClassicState, member_id: &str) -> JoinResult {
    let is_leader = state.leader_id.as_deref() == Some(member_id);
    let mut members: Vec<JoinResultMember> = if is_leader {
        state
            .members
            .values()
            .map(|m| JoinResultMember {
                member_id: m.id.clone(),
                group_instance_id: m.group_instance_id.clone(),
                metadata: m.protocol_metadata.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };
    // Kafka lists the members in its map order; sorting keeps the leader's
    // list the same from run to run.
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    JoinResult {
        error_code: codes::NONE,
        generation_id: state.generation_id,
        protocol_type: state.protocol_type.clone(),
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone().unwrap_or_default(),
        skip_assignment: false,
        member_id: member_id.to_string(),
        members,
    }
}

/// Why a round could not complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompleteError {
    /// No protocol is common to all members. The protocol gate in
    /// `handle_join` keeps a joined group out of this state, but a group
    /// loaded from the log or converted from a consumer group can be in it.
    InconsistentProtocol,
    EpochExhausted,
}

/// What [`try_complete`] did to the group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Completion {
    /// The group was not in `PreparingRebalance`.
    NotPreparing,
    /// A new generation started, in `CompletingRebalance`, and every member
    /// owes a `SyncGroup` before the group rebalance timeout runs out.
    Generation {
        /// The dynamic members that had not joined again, now removed.
        removed: Vec<String>,
    },
    /// Every member was a dynamic member that had not joined again. The group
    /// is `Empty` in a new generation, which must be persisted.
    Emptied { removed: Vec<String> },
    /// No member waits in `JoinGroup`. The deadline moved out by the group
    /// rebalance timeout, until session expiry removes the silent members.
    Postponed { removed: Vec<String> },
}

/// Kafka's `completeClassicGroupJoin`. It removes each dynamic member that did
/// not join again, keeps the leader while it joined, runs the protocol vote,
/// and starts the next generation.
///
/// # Errors
/// Returns why the round could not complete. The group is unchanged then,
/// except for the removed members.
pub(crate) fn try_complete(
    state: &mut ClassicState,
    now: Instant,
) -> Result<Completion, CompleteError> {
    if state.state != GroupState::PreparingRebalance {
        return Ok(Completion::NotPreparing);
    }
    let mut removed: Vec<String> = state
        .members
        .values()
        .filter(|m| !m.is_static() && !state.joined_this_round.contains(&m.id))
        .map(|m| m.id.clone())
        .collect();
    removed.sort_unstable();
    let rebalance_timeout = state.rebalance_timeout();
    for member_id in &removed {
        state.members.remove(member_id);
        state.joined_this_round.remove(member_id);
    }
    if state.members.is_empty() {
        let generation_id = next_i32(state.generation_id).ok_or(CompleteError::EpochExhausted)?;
        state.generation_id = generation_id;
        state.state = GroupState::Empty;
        state.leader_id = None;
        state.protocol_name = None;
        state.rebalance_deadline = None;
        state.joined_this_round.clear();
        state.rebalance_from_empty = false;
        state.initial_join = None;
        state.new_member_added = false;
        return Ok(Completion::Emptied { removed });
    }
    if !state.maybe_elect_new_joined_leader() {
        state.rebalance_deadline = Some(now + rebalance_timeout);
        state.initial_join = None;
        return Ok(Completion::Postponed { removed });
    }
    let chosen = select_protocol(&state.members).ok_or(CompleteError::InconsistentProtocol)?;
    state.resolve_selected_protocol_metadata(&chosen);
    if !state.complete_rebalance(chosen) {
        return Err(CompleteError::EpochExhausted);
    }
    // Kafka reschedules every member's heartbeat as the round completes, and
    // the members stop being new.
    for member in state.members.values_mut() {
        member.last_heartbeat = now;
        member.is_new = false;
    }
    // Kafka's `addPendingSyncMember` and `schedulePendingSync`: every member
    // owes a `SyncGroup` within the group rebalance timeout.
    state.arm_pending_sync(now);
    Ok(Completion::Generation { removed })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::classic_ops::{
            handle_heartbeat,
            test_support::{handle_join as join_as, join_ctx, join_req, stable_two_member_group},
        },
        assert2::{assert, check},
        krabka_protocol::owned::{
            heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequestProtocol,
        },
        std::collections::HashMap,
    };

    fn protocols(names: &[&str]) -> Vec<JoinGroupRequestProtocol> {
        names
            .iter()
            .map(|name| JoinGroupRequestProtocol {
                name: (*name).into(),
                metadata: Bytes::from_static(b"meta"),
                ..Default::default()
            })
            .collect()
    }

    fn sorted_members(state: &ClassicState) -> Vec<String> {
        let mut members: Vec<String> = state.members.keys().cloned().collect();
        members.sort();
        members
    }

    /// A `Stable` group at generation 1 with members `m1`, the leader, and
    /// `m2`.
    fn stable_group() -> ClassicState {
        let mut g = stable_two_member_group();
        g.install_assignments(HashMap::from([
            ("m1".to_string(), Bytes::from_static(b"a1")),
            ("m2".to_string(), Bytes::from_static(b"a2")),
        ]));
        g
    }

    fn current(state: &ClassicState, member_id: &str) -> JoinOutcome {
        JoinOutcome::of(JoinAction::Immediate(build_join_result(state, member_id)))
    }

    fn parked() -> JoinOutcome { JoinOutcome::of(JoinAction::Park) }

    fn error(error_code: i16, member_id: &str) -> JoinOutcome {
        JoinOutcome::error(error_code, member_id)
    }

    /// #788: Kafka's `supportsProtocols` gate turns away only the joining
    /// member, and the group does not change.
    #[test]
    fn protocol_gate_rejects_only_the_joining_member() {
        struct Row {
            name: &'static str,
            stable: bool,
            member_id: &'static str,
            protocol_type: &'static str,
            protocols: &'static [&'static str],
            want: JoinOutcome,
            members_after: &'static [&'static str],
        }
        let rows = [
            Row {
                name: "empty group, empty protocol type",
                stable: false,
                member_id: "",
                protocol_type: "",
                protocols: &["range"],
                want: error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
                members_after: &[],
            },
            Row {
                name: "empty group, no protocols",
                stable: false,
                member_id: "",
                protocol_type: "consumer",
                protocols: &[],
                want: error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
                members_after: &[],
            },
            Row {
                name: "stable group, new member with a disjoint protocol",
                stable: true,
                member_id: "",
                protocol_type: "consumer",
                protocols: &["roundrobin"],
                want: error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
                members_after: &["m1", "m2"],
            },
            Row {
                name: "stable group, new member with another protocol type",
                stable: true,
                member_id: "",
                protocol_type: "connect",
                protocols: &["range"],
                want: error(codes::INCONSISTENT_GROUP_PROTOCOL, ""),
                members_after: &["m1", "m2"],
            },
            Row {
                name: "stable group, known member switches to a disjoint protocol",
                stable: true,
                member_id: "m2",
                protocol_type: "consumer",
                protocols: &["roundrobin"],
                want: error(codes::INCONSISTENT_GROUP_PROTOCOL, "m2"),
                members_after: &["m1", "m2"],
            },
            Row {
                name: "stable group, pending member with one shared protocol",
                stable: true,
                member_id: "m3",
                protocol_type: "consumer",
                protocols: &["roundrobin", "range"],
                want: parked(),
                members_after: &["m1", "m2", "m3"],
            },
        ];
        for row in rows {
            let mut g = if row.stable {
                stable_group()
            } else {
                ClassicState::new("g")
            };
            if row.member_id == "m3" {
                g.add_pending_member("m3".into(), Instant::now() + Duration::from_mins(1));
            }
            let mut req = join_req(row.member_id, None);
            req.protocol_type = row.protocol_type.into();
            req.protocols = protocols(row.protocols);
            let outcome = handle_join(&mut g, &mut req, &join_ctx("h"));
            check!(outcome == row.want, "{}", row.name);
            check!(sorted_members(&g) == row.members_after, "{}", row.name);
        }
    }

    /// #790: a known member's `JoinGroup` per group state, Kafka's
    /// `classicGroupJoinExistingMember`.
    #[test]
    fn existing_member_join_follows_group_state() {
        struct Row {
            name: &'static str,
            stable: bool,
            member_id: &'static str,
            changed: bool,
            want: fn(&ClassicState) -> JoinOutcome,
            state_after: GroupState,
        }
        let rows = [
            Row {
                name: "stable follower, unchanged: current generation",
                stable: true,
                member_id: "m2",
                changed: false,
                want: |g| current(g, "m2"),
                state_after: GroupState::Stable,
            },
            Row {
                name: "stable leader, unchanged: rebalance",
                stable: true,
                member_id: "m1",
                changed: false,
                want: |_| parked(),
                state_after: GroupState::PreparingRebalance,
            },
            Row {
                name: "stable follower, changed: rebalance",
                stable: true,
                member_id: "m2",
                changed: true,
                want: |_| parked(),
                state_after: GroupState::PreparingRebalance,
            },
            Row {
                name: "completing follower, unchanged: current generation",
                stable: false,
                member_id: "m2",
                changed: false,
                want: |g| current(g, "m2"),
                state_after: GroupState::CompletingRebalance,
            },
            Row {
                name: "completing leader, unchanged: current generation with members",
                stable: false,
                member_id: "m1",
                changed: false,
                want: |g| current(g, "m1"),
                state_after: GroupState::CompletingRebalance,
            },
            Row {
                name: "completing follower, changed: rebalance",
                stable: false,
                member_id: "m2",
                changed: true,
                want: |_| parked(),
                state_after: GroupState::PreparingRebalance,
            },
            Row {
                name: "unknown member id",
                stable: true,
                member_id: "ghost",
                changed: false,
                want: |_| error(codes::UNKNOWN_MEMBER_ID, "ghost"),
                state_after: GroupState::Stable,
            },
        ];
        for row in rows {
            let mut g = if row.stable {
                stable_group()
            } else {
                stable_two_member_group()
            };
            let want = (row.want)(&g);
            let mut req = join_req(row.member_id, None);
            if row.changed {
                req.protocols[0].metadata = Bytes::from_static(b"changed");
            }
            let outcome = handle_join(&mut g, &mut req, &join_ctx("h"));
            check!(outcome == want, "{}", row.name);
            check!(g.state == row.state_after, "{}", row.name);
            check!(g.generation_id == 1, "{}", row.name);
        }
    }

    /// Kafka's `JoinGroupRequest.requiresKnownMemberId`: only a dynamic member
    /// with no member id at v4 or later is sent back for an id.
    #[test]
    fn requires_known_member_id_follows_kafka() {
        // (member id, group instance id, version, expected)
        let rows = [
            ("", None, 4, true),
            ("", None, 9, true),
            ("", None, 3, false),
            ("", Some("instance-1"), 9, false),
            ("m1", None, 9, false),
        ];
        for (member_id, instance_id, version, want) in rows {
            check!(
                requires_known_member_id(&join_req(member_id, instance_id), version) == want,
                "{member_id:?} {instance_id:?} v{version}"
            );
        }
    }

    /// #790: KIP-394. A dynamic member gets `MEMBER_ID_REQUIRED` at v4+ and
    /// joins with that id; an id the group never gave out is unknown.
    #[test]
    fn member_id_required_then_join_with_the_given_id() {
        let mut g = ClassicState::new("g");
        let outcome = handle_join(&mut g, &mut join_req("", None), &join_ctx("h"));
        let JoinAction::Immediate(required) = outcome.action else {
            panic!("expected MEMBER_ID_REQUIRED, got {outcome:?}");
        };
        check!(required.member_id.starts_with("client-a-"));
        check!(
            required
                == JoinResult {
                    error_code: codes::MEMBER_ID_REQUIRED,
                    member_id: required.member_id.clone(),
                    ..JoinResult::default()
                }
        );
        check!(g.members.is_empty());
        check!(g.state == GroupState::Empty);

        check!(
            handle_join(&mut g, &mut join_req("made-up", None), &join_ctx("h"))
                == error(codes::UNKNOWN_MEMBER_ID, "made-up")
        );

        let outcome = handle_join(
            &mut g,
            &mut join_req(&required.member_id, None),
            &join_ctx("h"),
        );
        check!(outcome == parked());
        check!(sorted_members(&g) == vec![required.member_id.clone()]);
        check!(g.pending_members.is_empty());
    }

    /// Before v4 a dynamic member joins at once with a generated id.
    #[test]
    fn legacy_join_with_empty_member_id_adds_generated_member() {
        let mut g = ClassicState::new("g");
        let mut request = join_req("", None);
        let ctx = JoinContext {
            version: 3,
            ..join_ctx("h")
        };
        check!(handle_join(&mut g, &mut request, &ctx) == parked());
        check!(request.member_id.starts_with("client-a-"));
        check!(sorted_members(&g) == vec![request.member_id.clone()]);
    }

    /// Kafka's `generateMemberId` does not bound the id it builds: the instance
    /// id or the client id, a hyphen and a UUID of 36 characters. Both prefixes
    /// take up to 32767 bytes, and Kafka's generated writers refuse an id over
    /// 32767 bytes in the response and in the group metadata record, so a join
    /// that generates one fails there with an internal error. Here it answers
    /// `UNKNOWN_SERVER_ERROR` with no member id, and it leaves the group as it
    /// was. The longest prefix that fits gives an id of exactly 32767 bytes.
    #[test]
    fn a_generated_member_id_over_32767_bytes_is_refused_before_any_state_change() {
        /// The bytes that `generateMemberId` adds to its prefix.
        const SUFFIX: usize = 37;
        const LONGEST_PREFIX: usize = MAX_STRING_BYTES - SUFFIX;

        struct Row {
            name: &'static str,
            /// `true` when the prefix is the instance id of a static member,
            /// and `false` when it is the client id of a dynamic member.
            static_member: bool,
            version: i16,
            prefix: usize,
            refused: bool,
        }
        let rows = [
            Row {
                name: "static, the longest instance id that fits",
                static_member: true,
                version: 5,
                prefix: LONGEST_PREFIX,
                refused: false,
            },
            Row {
                name: "static, one byte more",
                static_member: true,
                version: 5,
                prefix: LONGEST_PREFIX + 1,
                refused: true,
            },
            Row {
                name: "static, the longest instance id",
                static_member: true,
                version: 9,
                prefix: MAX_STRING_BYTES,
                refused: true,
            },
            Row {
                name: "dynamic v5, the longest client id that fits",
                static_member: false,
                version: 5,
                prefix: LONGEST_PREFIX,
                refused: false,
            },
            Row {
                name: "dynamic v5, one byte more",
                static_member: false,
                version: 5,
                prefix: LONGEST_PREFIX + 1,
                refused: true,
            },
            Row {
                name: "dynamic v5, the longest client id",
                static_member: false,
                version: 5,
                prefix: MAX_STRING_BYTES,
                refused: true,
            },
            Row {
                name: "dynamic v3, the longest client id that fits",
                static_member: false,
                version: 3,
                prefix: LONGEST_PREFIX,
                refused: false,
            },
            Row {
                name: "dynamic v3, one byte more",
                static_member: false,
                version: 3,
                prefix: LONGEST_PREFIX + 1,
                refused: true,
            },
        ];
        for row in rows {
            let long = "p".repeat(row.prefix);
            let mut g = ClassicState::new("g");
            let mut req = join_req("", row.static_member.then_some(long.as_str()));
            let ctx = JoinContext {
                client_id: if row.static_member {
                    "client-a"
                } else {
                    long.as_str()
                },
                version: row.version,
                ..join_ctx("h")
            };

            let outcome = handle_join(&mut g, &mut req, &ctx);

            let ids: Vec<usize> = g
                .members
                .keys()
                .chain(g.pending_members.keys())
                .map(String::len)
                .collect();
            if row.refused {
                check!(
                    outcome == error(codes::UNKNOWN_SERVER_ERROR, ""),
                    "{}",
                    row.name
                );
                check!(ids.is_empty(), "{}", row.name);
                check!(g.state == GroupState::Empty, "{}", row.name);
            } else {
                check!(
                    outcome != error(codes::UNKNOWN_SERVER_ERROR, ""),
                    "{}",
                    row.name
                );
                check!(ids == vec![MAX_STRING_BYTES], "{}", row.name);
            }
        }
    }

    /// #789: a static member that joins again with an empty member id gets a
    /// new member id at every version, and the old id is fenced.
    #[test]
    fn static_rejoin_replaces_member_id_and_fences_the_old_one() {
        struct Row {
            name: &'static str,
            version: i16,
            stable: bool,
            want: fn(&ClassicState, &str) -> JoinAction,
            state_after: GroupState,
        }
        let rows = [
            Row {
                name: "stable, v4: persist, leader from before the join",
                version: 4,
                stable: true,
                want: |g, new| {
                    JoinAction::PersistThenReply(JoinResult {
                        members: Vec::new(),
                        leader: "m1".into(),
                        ..build_join_result(g, new)
                    })
                },
                state_after: GroupState::Stable,
            },
            Row {
                name: "stable, v9: persist, leader skips assignment",
                version: 9,
                stable: true,
                want: |g, new| {
                    JoinAction::PersistThenReply(JoinResult {
                        skip_assignment: true,
                        ..build_join_result(g, new)
                    })
                },
                state_after: GroupState::Stable,
            },
            Row {
                name: "completing, v5: rebalance",
                version: 5,
                stable: false,
                want: |_, _| JoinAction::Park,
                state_after: GroupState::PreparingRebalance,
            },
        ];
        for row in rows {
            let mut g = ClassicState::new("g");
            let _ = join_as(&mut g, &join_req("m1", Some("i1")), "h");
            let _ = join_as(&mut g, &join_req("m2", None), "h");
            try_complete(&mut g, Instant::now()).unwrap();
            if row.stable {
                g.install_assignments(HashMap::from([
                    ("m1".to_string(), Bytes::from_static(b"a1")),
                    ("m2".to_string(), Bytes::from_static(b"a2")),
                ]));
            }

            let mut req = join_req("", Some("i1"));
            let ctx = JoinContext {
                version: row.version,
                ..join_ctx("h")
            };
            let outcome = handle_join(&mut g, &mut req, &ctx);

            let new_id = req.member_id.clone();
            check!(new_id.starts_with("i1-"), "{}", row.name);
            check!(
                outcome
                    == JoinOutcome {
                        action: (row.want)(&g, &new_id),
                        fenced_member: Some("m1".into()),
                    },
                "{}",
                row.name
            );
            check!(g.state == row.state_after, "{}", row.name);
            check!(g.generation_id == 1, "{}", row.name);
            check!(
                g.leader_id.as_deref() == Some(new_id.as_str()),
                "{}",
                row.name
            );
            check!(
                g.current_member_id_for_instance("i1") == Some(new_id.as_str()),
                "{}",
                row.name
            );
            let old_heartbeat = HeartbeatRequest {
                group_id: "g".into(),
                generation_id: 1,
                member_id: "m1".into(),
                group_instance_id: Some("i1".into()),
                ..Default::default()
            };
            check!(
                handle_heartbeat(&mut g, &old_heartbeat) == codes::FENCED_INSTANCE_ID,
                "{}",
                row.name
            );
        }
    }

    /// #789: a new static member joins at once, without `MEMBER_ID_REQUIRED`.
    #[test]
    fn new_static_member_joins_without_member_id_required() {
        let mut g = ClassicState::new("g");
        let mut req = join_req("", Some("i1"));
        check!(handle_join(&mut g, &mut req, &join_ctx("h")) == parked());
        check!(req.member_id.starts_with("i1-"));
        check!(g.current_member_id_for_instance("i1") == Some(req.member_id.as_str()));
    }

    /// #791: Kafka's `acceptJoiningMember` at `group.max.size`.
    #[test]
    fn full_group_turns_away_only_new_members() {
        for (name, member_id) in [("new member", ""), ("known member", "m2")] {
            let mut g = stable_group();
            let want = if member_id.is_empty() {
                error(codes::GROUP_MAX_SIZE_REACHED, "")
            } else {
                current(&g, member_id)
            };
            let ctx = JoinContext {
                max_size: 2,
                ..join_ctx("h")
            };
            let outcome = handle_join(&mut g, &mut join_req(member_id, None), &ctx);
            check!(outcome == want, "{name}");
            check!(sorted_members(&g) == vec!["m1", "m2"], "{name}");
        }
    }

    /// #790: at the deadline the dynamic members that did not join again
    /// leave, and the leader stays while it joined.
    #[test]
    fn completion_removes_members_that_did_not_rejoin() {
        for (name, rejoined, leader) in [
            ("leader rejoined", "m1", "m1"),
            ("only the follower rejoined", "m2", "m2"),
        ] {
            let mut g = stable_group();
            // The leader's `JoinGroup` opens the round.
            check!(
                handle_join(&mut g, &mut join_req("m1", None), &join_ctx("h")) == parked(),
                "{name}"
            );
            g.joined_this_round.clear();
            g.mark_awaiting_join(rejoined);
            let removed = if rejoined == "m1" { "m2" } else { "m1" };
            check!(
                try_complete(&mut g, Instant::now())
                    == Ok(Completion::Generation {
                        removed: vec![removed.to_string()],
                    }),
                "{name}"
            );
            check!(sorted_members(&g) == vec![rejoined.to_string()], "{name}");
            check!(g.leader_id.as_deref() == Some(leader), "{name}");
            check!(g.generation_id == 2, "{name}");
            check!(
                build_join_result(&g, leader).members
                    == vec![JoinResultMember {
                        member_id: rejoined.into(),
                        group_instance_id: None,
                        metadata: Bytes::from_static(b"meta"),
                    }],
                "{name}"
            );
        }
    }

    #[test]
    fn completion_of_a_round_nobody_rejoined_empties_the_group() {
        let mut g = stable_group();
        let _ = handle_join(&mut g, &mut join_req("m1", None), &join_ctx("h"));
        g.joined_this_round.clear();
        check!(
            try_complete(&mut g, Instant::now())
                == Ok(Completion::Emptied {
                    removed: vec!["m1".into(), "m2".into()],
                })
        );
        check!(g.state == GroupState::Empty);
        check!(g.generation_id == 2);
        check!(g.leader_id == None);
    }

    #[test]
    fn all_members_rejoined_completes_now() {
        let mut g = stable_group();
        let mut changed = join_req("m2", None);
        changed.protocols[0].metadata = Bytes::from_static(b"changed");
        check!(handle_join(&mut g, &mut changed, &join_ctx("h")) == parked());
        check!(
            handle_join(&mut g, &mut join_req("m1", None), &join_ctx("h"))
                == JoinOutcome::of(JoinAction::CompleteNow)
        );
    }

    /// #790: a round from `Empty` runs Kafka's `InitialDelayedJoin`, and does
    /// not complete early even when every member joined.
    #[test]
    fn round_from_empty_waits_out_the_initial_delay() {
        let mut g = ClassicState::new("g");
        let before = Instant::now();
        let _ = join_as(&mut g, &join_req("m1", None), "h");
        check!(g.rebalance_from_empty);
        check!(
            g.initial_join.map(|join| (join.delay, join.remaining))
                == Some((Duration::from_secs(3), Duration::from_secs(57)))
        );
        let deadline = g.rebalance_deadline.expect("rebalance deadline");
        check!(deadline >= before + Duration::from_secs(3));
        check!(!g.new_member_added);
        check!(join_as(&mut g, &join_req("m2", None), "h") == JoinAction::Park);
        check!(g.new_member_added);
    }

    #[test]
    fn try_complete_with_no_common_protocol_is_inconsistent() {
        let mut g = stable_group();
        g.members.get_mut("m2").unwrap().protocols = vec![("roundrobin".into(), Bytes::new())];
        g.state = GroupState::PreparingRebalance;
        g.mark_awaiting_join("m1");
        g.mark_awaiting_join("m2");
        check!(try_complete(&mut g, Instant::now()) == Err(CompleteError::InconsistentProtocol));
    }

    #[test]
    fn build_join_result_leader_lists_members_follower_empty() {
        let g = stable_two_member_group();
        assert!(build_join_result(&g, "m1").members.len() == 2);
        assert!(build_join_result(&g, "m2").members.is_empty());
    }
}
