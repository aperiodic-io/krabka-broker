//! The `StreamsGroupHeartbeat` exchange and the session-expiry tick.
//!
//! Every membership change a streams group makes arrives here: a first join
//! that mints a member id, a steady-state heartbeat that reports owned tasks
//! and changelog offsets, a leave at `member_epoch == -1`, and the eviction of
//! members that went silent past the session timeout. Each path reconciles
//! when the group is dirty and then writes the resulting records as one batch,
//! so a failed log write ends the actor.

use {
    super::{
        ActorState, chrono_now_ms,
        reconciliation::{
            assignment_delay, configure_after_load, reconcile, validation_or_configs_changed,
        },
        records::{StreamsRecorder, flush_pending},
        request::{build_member, task_ids_to_map, task_offsets_to_map, update_client_tags},
        response::{ResponseDelta, build_assignment_resp, endpoint_to_partitions, error_resp},
    },
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        coordinator::unified::{
            ClientIdentity, GroupCoordinator,
            offsets_log::OffsetsLog,
            streams::{
                config::StreamsGroupConfig,
                persistence::{
                    PendingStreamsRecords, StoredSubtopology, StreamsGroupTopologyValue,
                },
                state::{OwnedTasks, RoleTasks, StoredTopologyHandle, StreamsMemberState},
                topology,
            },
        },
        error::BrokerError,
        metadata_source::MetadataSource,
    },
    krabka_protocol::owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::{collections::BTreeMap, sync::Arc, time::Instant},
};

/// Fences each member silent past the session timeout, and each member past
/// its rebalance timeout, on its own, with a batch of its own, as each of
/// Kafka's timers runs `streamsGroupFenceMember` for its member. Returns
/// `Err` if a log write fails (the actor exits).
pub(super) async fn handle_session_tick(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), BrokerError> {
    let now = Instant::now();
    let mut fenced = actor.state.expired_members(now, config.session_timeout);
    // A member that did not revoke its tasks within its rebalance timeout is
    // fenced like a member whose session expired
    // (`scheduleStreamsGroupRebalanceTimeout`).
    for member_id in actor.state.rebalance_timeouts_due(now) {
        if !fenced.contains(&member_id) {
            fenced.push(member_id);
        }
    }
    for member_id in fenced {
        let pending = fence_member(actor, &member_id);
        flush_pending(actor, pending, offsets_log, coordinator, chrono_now_ms()).await?;
    }
    Ok(())
}

/// Kafka's `streamsGroupFenceMember`: the member's current assignment, target
/// assignment and member tombstones, and the group epoch bumped. An unknown
/// member writes nothing.
fn fence_member(actor: &mut ActorState, member_id: &str) -> PendingStreamsRecords {
    let recorder = StreamsRecorder::start(actor, &[member_id]);
    if !actor.state.fence_member(member_id) {
        return PendingStreamsRecords::default();
    }
    recorder.finish(actor)
}

pub(super) async fn handle_heartbeat(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
) -> Result<StreamsGroupHeartbeatResponse, BrokerError> {
    let ClientIdentity {
        id: client_id,
        host: client_host,
    } = client;
    let now = Instant::now();
    let now_ms = chrono_now_ms();
    // Kafka's `groups.containsKey`: a group that this heartbeat creates
    // reports endpoint information epoch 0. A new group already holds its
    // initial group epoch, so the epoch alone does not tell.
    let group_existed = !actor.holds_nothing();

    // ─── Leave path ──────────────────────────────────────────────
    // -1 leaves, and -2 is the temporary leave of a static member.
    if req.member_epoch < 0 {
        return handle_leave(actor, offsets_log, coordinator, req, now_ms).await;
    }

    // ─── Static membership ───────────────────────────────────────
    // Kafka's `getOrMaybeCreateStaticStreamsGroupMember`: resolve the instance
    // id before the member id. A released static member is replaced by the
    // joining member.
    let mut static_recorder = None;
    if let Some(instance_id) = &req.instance_id {
        let StaticMemberCheck { existing, error } = checked_static_member(actor, req, instance_id);
        if let Some(response) = error {
            return Ok(response);
        }
        if req.member_epoch == 0
            && let Some(previous) = existing
        {
            if let Some(resp) = topology_error(actor, req, metadata_source) {
                return Ok(resp);
            }
            static_recorder = Some(StreamsRecorder::start(actor, &[&previous, &req.member_id]));
            replace_static_member(actor, &previous, &req.member_id);
        }
    }

    // ─── Group size ──────────────────────────────────────────────
    // Kafka's `throwIfStreamsGroupIsFull` runs on every join. Kafka trunk does
    // not count a member that is already in the group, and 4.3.1 does: a
    // known member that rejoins a full group is refused too.
    let known_member = actor.state.members.contains_key(&req.member_id);
    if req.member_epoch == 0
        && actor.state.members.len() >= config.max_size
        && (!known_member || config.unstable_api_versions == UnstableApiVersions::Disabled)
    {
        return Ok(error_resp(
            codes::GROUP_MAX_SIZE_REACHED,
            Some(format!(
                "The streams group has reached its maximum capacity of {} members.",
                config.max_size
            )),
        ));
    }

    // ─── First-join path ─────────────────────────────────────────
    // KIP-1071 mirrors KIP-848: epoch 0 from an unknown member is a first
    // join, with the member id that the client generated. Epoch 0 from a
    // known member is a rejoin and takes the existing-member path below.
    if req.member_epoch == 0 && !known_member {
        if let Some(resp) = topology_error(actor, req, metadata_source) {
            return Ok(resp);
        }
        let new_member_id = req.member_id.clone();
        let recorder = StreamsRecorder::start(actor, &[&new_member_id]);
        // Kafka schedules the initial rebalance delay when a member joins an
        // empty group, unless it is already scheduled.
        if actor.state.members.is_empty()
            && !config.initial_rebalance_delay.is_zero()
            && actor.initial_rebalance_deadline.is_none()
        {
            actor.initial_rebalance_deadline =
                Some(tokio::time::Instant::now() + config.initial_rebalance_delay);
        }
        let m = build_member(&new_member_id, req, client_id, client_host, now);
        actor.state.add_or_update_member(m);
        // Kafka's `maybeUpdateTopology`: a join initializes the topology of a
        // group that has none. A join with an older topology keeps the group
        // topology, and its responses carry `STALE_TOPOLOGY`.
        if actor.topology.is_none()
            && let Some(topo) = &req.topology
        {
            accept_topology(actor, topo);
        }
        reconcile(actor, config, metadata_source);
        if req.shutdown_application {
            actor.state.request_shutdown(&new_member_id);
        }
        if actor
            .state
            .reconcile_member(&new_member_id, owned_role_tasks(req).as_ref())
        {
            actor.state.track_rebalance_timeout(&new_member_id, now);
        }
        let pending = recorder.finish(actor);
        flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
        return Ok(accepted_response(
            actor,
            config,
            metadata_source,
            req,
            &new_member_id,
            &MemberBefore::default(),
            group_existed,
        ));
    }

    // ─── Existing-member: validate epoch ─────────────────────────
    // The owned-task maps matter only for a heartbeat at the previous member
    // epoch, so a heartbeat at epoch 0 or at the member epoch builds none.
    let needs_owned = actor.state.members.get(&req.member_id).is_some_and(|m| {
        req.member_epoch != 0
            && req.member_epoch != m.member_epoch
            && req.member_epoch == m.previous_member_epoch
    });
    let owned_maps = needs_owned.then(|| {
        (
            req.active_tasks.as_deref().map(task_ids_to_map),
            req.standby_tasks.as_deref().map(task_ids_to_map),
            req.warmup_tasks.as_deref().map(task_ids_to_map),
        )
    });
    let owned =
        owned_maps
            .as_ref()
            .map_or_else(OwnedTasks::default, |(active, standby, warmup)| {
                OwnedTasks {
                    active: active.as_ref(),
                    standby: standby.as_ref(),
                    warmup: warmup.as_ref(),
                }
            });
    if let Err(error_code) =
        actor
            .state
            .validate_heartbeat_epoch(&req.member_id, req.member_epoch, owned)
    {
        return Ok(error_resp(
            error_code,
            epoch_error_message(actor, req, error_code),
        ));
    }
    let before = MemberBefore::of(&actor.state.members[&req.member_id]);
    if let Some(resp) = topology_error(actor, req, metadata_source) {
        return Ok(resp);
    }
    let recorder =
        static_recorder.unwrap_or_else(|| StreamsRecorder::start(actor, &[req.member_id.as_str()]));

    // ─── Steady state ────────────────────────────────────────────
    update_member_steady_state(actor, req, client_id, client_host, now);
    refresh_topic_metadata(actor, config, metadata_source);

    if actor.state.dirty || actor.assignment_pending() {
        reconcile(actor, config, metadata_source);
    }
    // Kafka's `maybeReconcile`: move the member toward the target, and arm
    // or cancel its rebalance timeout when its assignment changed.
    if actor
        .state
        .reconcile_member(&req.member_id, owned_role_tasks(req).as_ref())
    {
        actor.state.track_rebalance_timeout(&req.member_id, now);
    }
    if req.shutdown_application {
        actor.state.request_shutdown(&req.member_id);
    }
    let pending = recorder.finish(actor);
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    Ok(accepted_response(
        actor,
        config,
        metadata_source,
        req,
        &req.member_id,
        &before,
        group_existed,
    ))
}

/// The owned tasks of a heartbeat, when it reports all three roles, as
/// Kafka's `TasksTuple.fromHeartbeatRequest` builds them.
fn owned_role_tasks(req: &StreamsGroupHeartbeatRequest) -> Option<RoleTasks> {
    match (&req.active_tasks, &req.standby_tasks, &req.warmup_tasks) {
        (Some(active), Some(standby), Some(warmup)) => Some(RoleTasks {
            active: task_ids_to_map(active),
            standby: task_ids_to_map(standby),
            warmup: task_ids_to_map(warmup),
        }),
        _ => None,
    }
}

/// The user endpoint of a member before a heartbeat changed it. A joining
/// member starts from none.
#[derive(Default)]
struct MemberBefore {
    user_endpoint: Option<(String, u16)>,
}

impl MemberBefore {
    fn of(member: &StreamsMemberState) -> Self {
        Self {
            user_endpoint: member.user_endpoint.clone(),
        }
    }
}

/// Builds the response of an accepted heartbeat, as the end of Kafka's
/// `streamsGroupHeartbeat` does.
///
/// The task lists go out when the member joins or its tasks changed. The
/// group's endpoint information epoch goes up when the member's endpoint
/// changed, or its tasks changed and it has an endpoint. Kafka compares the
/// tasks before and after the heartbeat, because only the member's own
/// heartbeat changes them. Here a new target also trims the tasks of the
/// other members, so the comparison is with the tasks that the last response
/// sent. A member whose last
/// seen epoch differs from the group's gets the endpoint information of the
/// whole group. A group that this heartbeat creates keeps epoch 0.
fn accepted_response(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    req: &StreamsGroupHeartbeatRequest,
    member_id: &str,
    before: &MemberBefore,
    group_existed: bool,
) -> StreamsGroupHeartbeatResponse {
    let member = &actor.state.members[member_id];
    let tasks = [
        member.active.clone(),
        member.standby.clone(),
        member.warmup.clone(),
    ];
    let tasks_changed = member.sent_tasks != tasks;
    let endpoint_changed = before.user_endpoint != member.user_endpoint;
    let mut endpoint_epoch = actor.state.endpoint_information_epoch;
    if endpoint_changed || (tasks_changed && member.user_endpoint.is_some()) {
        endpoint_epoch = endpoint_epoch.saturating_add(1);
    }
    let partitions_by_user_endpoint =
        (endpoint_epoch != req.endpoint_information_epoch).then(|| {
            let image = metadata_source.map(|source| source.current_image());
            let configured = actor
                .topology
                .as_ref()
                .zip(image.as_ref())
                .and_then(|(topology, image)| topology::configure_topics(topology, image).ok());
            endpoint_to_partitions(
                &actor.state,
                member_id,
                configured.as_ref().and_then(|c| c.subtopologies.as_ref()),
                image.as_deref(),
                config.unstable_api_versions,
            )
        });
    if group_existed {
        actor.state.endpoint_information_epoch = endpoint_epoch;
    }
    if let Some(member) = actor.state.members.get_mut(member_id) {
        member.sent_tasks = tasks;
    }
    build_assignment_resp(&actor.state, member_id, config, ResponseDelta {
        send_tasks: req.member_epoch == 0 || tasks_changed,
        assignment_delayed: assignment_delay(actor, config, tokio::time::Instant::now()),
        endpoint_information_epoch: actor.state.endpoint_information_epoch,
        partitions_by_user_endpoint,
    })
}

/// The `error_message` of a heartbeat that the member epoch check refused, in
/// the words of Kafka's `getMemberOrThrow` and
/// `throwIfStreamsGroupMemberEpochIsInvalid`.
fn epoch_error_message(
    actor: &ActorState,
    req: &StreamsGroupHeartbeatRequest,
    error_code: i16,
) -> Option<String> {
    let Some(member) = actor.state.members.get(&req.member_id) else {
        return Some(format!(
            "Member {} is not a member of group {}.",
            req.member_id, actor.state.group_id
        ));
    };
    (error_code == codes::FENCED_MEMBER_EPOCH).then(|| {
        let relation = if req.member_epoch > member.member_epoch {
            "greater"
        } else {
            "smaller"
        };
        format!(
            "The streams group member has a {relation} member epoch ({}) than the one known by \
             the group coordinator ({}). The member must abandon all its partitions and rejoin.",
            req.member_epoch, member.member_epoch
        )
    })
}

/// The error response for a heartbeat that Kafka refuses inside the
/// coordinator because of its topology or its owned tasks, or `None`.
///
/// In Kafka's order:
///
/// 1. `maybeUpdateTopology`: a join whose topology differs from the group
///    topology, at the same or a higher topology epoch, gets
///    `INVALID_REQUEST`, because topology updates are not supported.
/// 2. `configureTopics`: a topology that cannot be configured against the
///    current metadata image gets Kafka's error. The topology is the group
///    topology, or the topology of the join that initializes the group.
/// 3. `throwIfRequestContainsInvalidTasks`: once the topology is ready, an
///    owned task of an unknown subtopology or with a partition out of range
///    gets `INVALID_REQUEST`.
///
/// Kafka writes nothing for such a heartbeat.
fn topology_error(
    actor: &ActorState,
    req: &StreamsGroupHeartbeatRequest,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) -> Option<StreamsGroupHeartbeatResponse> {
    let from_request = req.topology.as_ref().map(topology::to_stored_topology);
    if let (Some(group), Some(requested)) = (actor.topology.as_ref(), from_request.as_ref())
        && requested.epoch >= group.epoch
        && !same_topology(group, requested)
    {
        return Some(error_resp(
            codes::INVALID_REQUEST,
            Some("Topology updates are not supported yet.".into()),
        ));
    }
    let source = metadata_source?;
    let topology = actor.topology.as_ref().or(from_request.as_ref())?;
    let configured = match topology::configure_topics(topology, &source.current_image()) {
        Ok(configured) => configured,
        Err(error) => return Some(error_resp(error.error_code(), error.error_message())),
    };
    let subtopologies = configured
        .subtopologies
        .as_ref()
        .filter(|_| configured.is_ready())?;
    [&req.active_tasks, &req.standby_tasks, &req.warmup_tasks]
        .into_iter()
        .flatten()
        .flatten()
        .find_map(|task| {
            let Some(subtopology) = subtopologies.get(&task.subtopology_id) else {
                return Some(format!(
                    "Subtopology {} does not exist in the topology.",
                    task.subtopology_id
                ));
            };
            let number_of_tasks = subtopology.number_of_tasks;
            task.partitions
                .iter()
                .find(|partition| **partition < 0 || **partition >= number_of_tasks)
                .map(|partition| {
                    format!(
                        "Task {partition} for subtopology {} is invalid. Number of tasks for this \
                         subtopology: {number_of_tasks}",
                        task.subtopology_id
                    )
                })
        })
        .map(|message| error_resp(codes::INVALID_REQUEST, Some(message)))
}

/// Kafka's `StreamsTopology.equals`: the same epoch and the same subtopologies
/// by id.
fn same_topology(a: &StreamsGroupTopologyValue, b: &StreamsGroupTopologyValue) -> bool {
    a.epoch == b.epoch && subtopologies_by_id(a) == subtopologies_by_id(b)
}

fn subtopologies_by_id(topology: &StreamsGroupTopologyValue) -> BTreeMap<&str, &StoredSubtopology> {
    topology
        .subtopologies
        .iter()
        .map(|subtopology| (subtopology.subtopology_id.as_str(), subtopology))
        .collect()
}

/// Marks the group for a reconcile when a topic that the topology needs
/// changed since the last reconcile, when the topology it validated changed,
/// or when its assignment configuration changed.
///
/// Kafka's `onMetadataUpdate` requests a metadata refresh for every streams
/// group that uses a created, changed or deleted topic, and the next heartbeat
/// computes the metadata hash again. A new hash configures the topology again
/// and bumps the group epoch. The heartbeat then bumps the epoch when the
/// validated topology epoch or the assignment configuration differs from the
/// one that the group's last `StreamsGroupMetadataValue` recorded.
fn refresh_topic_metadata(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if let Some(source) = metadata_source {
        if !actor.state.dirty {
            configure_after_load(actor, source);
        }
        if let Some(topology) = actor.topology.as_ref()
            && topology::metadata_hash(topology, &source.current_image()) != actor.metadata_hash
        {
            actor.state.dirty = true;
        }
    }
    if !actor.state.dirty && validation_or_configs_changed(actor, config) {
        actor.state.dirty = true;
    }
}

/// Updates a steady-state member's reported ownership, catch-up offsets, and
/// `last_seen`. Returns `true` if anything that needs persistence changed.
fn update_member_steady_state(
    actor: &mut ActorState,
    req: &StreamsGroupHeartbeatRequest,
    client_id: &str,
    client_host: &str,
    now: Instant,
) -> bool {
    let Some(m) = actor.state.members.get_mut(&req.member_id) else {
        return false;
    };
    m.last_seen = now;
    let mut changed = false;

    // Kafka's `hasStreamsMemberMetadataChanged` compares the whole member, its
    // client id and host included, and a dynamic member that changed bumps
    // the group epoch.
    if m.client_id != client_id || m.client_host != client_host {
        m.client_id = client_id.to_string();
        m.client_host = client_host.to_string();
        if req.instance_id.is_none() {
            actor.state.dirty = true;
        }
        changed = true;
    }
    let epoch_relevant = |m: &StreamsMemberState| {
        (
            m.topology_epoch,
            m.rack_id.clone(),
            m.client_tags.clone(),
            m.process_id.clone(),
        )
    };
    let before = epoch_relevant(m);
    if update_member_metadata(m, req) {
        // Kafka's `hasStreamsMemberMetadataChanged`: a changed member bumps
        // the group epoch, so the assignor sees the new process, rack, tags
        // and endpoint. A static member bumps it only for a change that the
        // assignment reads (`hasEpochRelevantMemberConfigChanged`).
        if req.instance_id.is_none() || before != epoch_relevant(m) {
            actor.state.dirty = true;
        }
        changed = true;
    }

    if let Some(offsets) = &req.task_offsets {
        let map = task_offsets_to_map(offsets);
        if map != m.task_offsets {
            m.task_offsets = map;
            changed = true;
        }
    }
    if let Some(end_offsets) = &req.task_end_offsets {
        let map = task_offsets_to_map(end_offsets);
        if map != m.task_end_offsets {
            m.task_end_offsets = map;
            changed = true;
        }
    }
    changed
}

type MetadataFields = (
    Option<String>,
    Option<String>,
    i32,
    i32,
    String,
    Option<(String, u16)>,
    Vec<(String, String)>,
);

/// The fields whose replacement changes this member's durable metadata.
fn metadata_fields(m: &StreamsMemberState) -> MetadataFields {
    (
        m.instance_id.clone(),
        m.rack_id.clone(),
        m.rebalance_timeout_ms,
        m.topology_epoch,
        m.process_id.clone(),
        m.user_endpoint.clone(),
        m.client_tags.clone(),
    )
}

/// Applies the member fields of a heartbeat to a known member, as Kafka's
/// `StreamsGroupMember.Builder.maybeUpdate*` calls do: a field that the
/// request carries replaces the stored value, and an absent field (or a
/// rebalance timeout of -1) keeps it. A rejoin at epoch 0 sets the user
/// endpoint also when the request has none. Returns `true` if a field changed.
fn update_member_metadata(m: &mut StreamsMemberState, req: &StreamsGroupHeartbeatRequest) -> bool {
    let before = metadata_fields(m);
    if req.instance_id.is_some() {
        m.instance_id.clone_from(&req.instance_id);
    }
    if req.rack_id.is_some() {
        m.rack_id.clone_from(&req.rack_id);
    }
    if req.rebalance_timeout_ms != -1 {
        m.rebalance_timeout_ms = req.rebalance_timeout_ms;
    }
    if let Some(topology) = &req.topology {
        m.topology_epoch = topology.epoch;
    }
    if let Some(process_id) = &req.process_id {
        m.process_id.clone_from(process_id);
    }
    let endpoint = req
        .user_endpoint
        .as_ref()
        .map(|endpoint| (endpoint.host.clone(), endpoint.port));
    if req.member_epoch == 0 || endpoint.is_some() {
        m.user_endpoint = endpoint;
    }
    update_client_tags(m, req);
    before != metadata_fields(m)
}

/// Handles a leave-group heartbeat, where `member_epoch == -1`.
async fn handle_leave(
    actor: &mut ActorState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<StreamsGroupHeartbeatResponse, BrokerError> {
    // Kafka's `streamsGroupLeave` records the shutdown request before it looks
    // the member up: an unknown member gets `UNKNOWN_MEMBER_ID`, and nothing
    // is written.
    if req.shutdown_application {
        actor.state.request_shutdown(&req.member_id);
    }
    if let Some(instance_id) = &req.instance_id {
        let StaticMemberCheck { error, .. } = checked_static_member(actor, req, instance_id);
        if let Some(response) = error {
            return Ok(response);
        }
        if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
            return leave_static_member(actor, offsets_log, coordinator, req, now_ms).await;
        }
    }
    if !actor.state.members.contains_key(&req.member_id) {
        return Ok(error_resp(
            codes::UNKNOWN_MEMBER_ID,
            Some(format!(
                "Member {} is not a member of group {}.",
                req.member_id, actor.state.group_id
            )),
        ));
    }
    let pending = fence_member(actor, &req.member_id);
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    // Kafka's leave response echoes the member id and epoch, and sends an
    // empty status list and no group configuration.
    Ok(StreamsGroupHeartbeatResponse {
        member_id: req.member_id.clone(),
        member_epoch: req.member_epoch,
        status: Some(Vec::new()),
        ..Default::default()
    })
}

/// `LEAVE_GROUP_STATIC_MEMBER_EPOCH`: the epoch of a static member that left
/// for a while and keeps its assignment.
const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// The id of the member that holds `instance_id`.
fn static_member_id(actor: &ActorState, instance_id: &str) -> Option<String> {
    actor
        .state
        .members
        .values()
        .find(|member| member.instance_id.as_deref() == Some(instance_id))
        .map(|member| member.member_id.clone())
}

/// The member that holds an instance id, and the error that the instance id
/// earns, if any.
struct StaticMemberCheck {
    existing: Option<String>,
    error: Option<StreamsGroupHeartbeatResponse>,
}

/// Resolves the instance before checking its ownership, for joins and leaves alike.
fn checked_static_member(
    actor: &ActorState,
    req: &StreamsGroupHeartbeatRequest,
    instance_id: &str,
) -> StaticMemberCheck {
    let existing = static_member_id(actor, instance_id);
    let error = static_member_error(req, instance_id, existing.as_deref(), actor);
    StaticMemberCheck { existing, error }
}

/// Kafka's static member checks: a join may not take an instance id that a
/// member still holds (`throwIfInstanceIdIsUnreleased`), and any other
/// heartbeat must come from the member that holds a known instance id
/// (`throwIfStaticMemberIsUnknown`, `throwIfInstanceIdIsFenced`).
fn static_member_error(
    req: &StreamsGroupHeartbeatRequest,
    instance_id: &str,
    existing: Option<&str>,
    actor: &ActorState,
) -> Option<StreamsGroupHeartbeatResponse> {
    if req.member_epoch == 0 {
        let existing = existing?;
        let released =
            actor.state.members[existing].member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        return (!released).then(|| {
            error_resp(
                codes::UNRELEASED_INSTANCE_ID,
                Some(format!(
                    "Static member {} with instance id {instance_id} cannot join the group \
                     because the instance id is owned by {existing} member.",
                    req.member_id
                )),
            )
        });
    }
    let Some(existing) = existing else {
        return Some(error_resp(
            codes::UNKNOWN_MEMBER_ID,
            Some(format!("Instance id {instance_id} is unknown.")),
        ));
    };
    (existing != req.member_id).then(|| {
        error_resp(
            codes::FENCED_INSTANCE_ID,
            Some(format!(
                "Static member {} with instance id {instance_id} was fenced by member {existing}.",
                req.member_id
            )),
        )
    })
}

/// Kafka's static member replacement: the joining member `member_id` takes
/// the place of the released member `previous`, with its assignment, its
/// target and its metadata, at epoch 0. The group epoch does not change.
///
/// Kafka writes the copy over any member that already holds `member_id`. That
/// member goes first, with its target, so that it leaves nothing of its own
/// behind and the group reassigns its tasks.
fn replace_static_member(actor: &mut ActorState, previous: &str, member_id: &str) {
    let state = &mut actor.state;
    let Some(mut member) = state.members.remove(previous) else {
        return;
    };
    state.rebalance_deadlines.remove(previous);
    if member_id != previous {
        state.remove_member(member_id);
    }
    member.member_id = member_id.to_string();
    member.member_epoch = 0;
    member.previous_member_epoch = 0;
    for role in [
        &mut state.target.active,
        &mut state.target.standby,
        &mut state.target.warmup,
    ] {
        match role.remove(previous) {
            Some(tasks) => {
                role.insert(member_id.to_string(), tasks);
            }
            None => {
                role.remove(member_id);
            }
        }
    }
    state.members.insert(member_id.to_string(), member);
}

/// Kafka's `streamsGroupStaticMemberGroupLeave`: the static member stays in
/// the group at epoch -2 with its assignment, so that its instance can come
/// back without a rebalance. Its tasks pending revocation are dropped.
async fn leave_static_member(
    actor: &mut ActorState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    req: &StreamsGroupHeartbeatRequest,
    now_ms: i64,
) -> Result<StreamsGroupHeartbeatResponse, BrokerError> {
    let recorder = StreamsRecorder::start(actor, &[req.member_id.as_str()]);
    if let Some(member) = actor.state.members.get_mut(&req.member_id) {
        member.member_epoch = LEAVE_GROUP_STATIC_MEMBER_EPOCH;
        member.active_pending_revocation.clear();
        member.standby_pending_revocation.clear();
        member.warmup_pending_revocation.clear();
    }
    actor.state.rebalance_deadlines.remove(&req.member_id);
    let pending = recorder.finish(actor);
    flush_pending(actor, pending, offsets_log, coordinator, now_ms).await?;
    Ok(StreamsGroupHeartbeatResponse {
        member_id: req.member_id.clone(),
        member_epoch: LEAVE_GROUP_STATIC_MEMBER_EPOCH,
        status: Some(Vec::new()),
        ..Default::default()
    })
}

/// Accepts a client-supplied topology. It stores the resolved value for
/// persistence and reconcile, stamps the epoch on the state handle, and marks
/// the group dirty.
fn accept_topology(
    actor: &mut ActorState,
    wire_topology: &krabka_protocol::owned::streams_group_heartbeat_request::Topology,
) {
    let stored = topology::to_stored_topology(wire_topology);
    actor.state.topology = Some(StoredTopologyHandle {
        epoch: stored.epoch,
    });
    actor.state.topology_epoch = stored.epoch;
    actor.topology = Some(stored);
    actor.state.dirty = true;
}
