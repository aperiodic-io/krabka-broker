//! KIP-1071 streams-group coordinator actor: a per-group tokio task that drives
//! the heartbeat epoch exchange, reconciliation, and persistence.
//!
//! This actor mirrors the overall shape of the KIP-932 share-group actor
//! ([`super::super::share::actor`]): a `tokio::select!` loop over an mpsc
//! message channel plus a `heartbeat_interval` session tick, the
//! `Pending*Records` → `RecordBatch` → `OffsetsLog::append` flush, and a
//! last-known-good cache hand-off through
//! `GroupCoordinator::update_streams_cache`.
//!
//! Two things differ. This actor assigns *tasks* `(subtopology, partition)`
//! across the active, standby, and warmup roles instead of topic partitions.
//! It also reconciles against a full `MetadataImage` through the
//! [`MetadataSource`], which resolves the topology and creates internal
//! topics, instead of the consumer `MetadataProvider`.
//!
//! Reconciliation needs the coordinator's [`MetadataSource`]. The actor reads
//! it from the coordinator when it wakes, not when it starts: the
//! `__consumer_offsets` replay spawns the actors of the loaded groups before
//! the broker connects the source. The pure-coordinator unit tests have no
//! source, so the group stays `NotReady` with empty assignments. Members there
//! still mint a `member_id` and advance their epoch, but the actor assigns no
//! tasks.

use std::{collections::BTreeMap, sync::Arc};

use krabka_protocol::owned::{
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

mod description;
mod heartbeat;
mod reconciliation;
mod records;
mod request;
pub(crate) mod response;

#[cfg(test)]
mod streams_group_model;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_support;

pub use self::description::{DescriptionPush, PushAnswer};
use self::{
    heartbeat::{handle_heartbeat, handle_session_tick},
    records::{apply_seed, reconcile_and_flush},
    response::build_describe,
};
use super::{
    config::StreamsGroupConfig,
    description::{DescriptionEpochs, SolicitationBackoff, StoredDescription, TopologyDescription},
    persistence::StreamsGroupTopologyValue,
    state::{self, StreamsGroupState},
};
use crate::{
    codes,
    coordinator::unified::{
        actor::CommitFence,
        offsets_log::{OffsetsLog, write_failure_code},
    },
    metadata_source::MetadataSource,
    task_util::spawn_mailbox,
};

/// Messages accepted by a [`StreamsGroupActorHandle`].
#[derive(Debug)]
pub enum StreamsGroupActorMessage {
    Heartbeat {
        request: Box<StreamsGroupHeartbeatRequest>,
        /// The request's API version. The `MISSING_CLIENT_TAGS` status goes
        /// out only at version 1 and above.
        version: i16,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<StreamsHeartbeatResult>,
    },
    Describe {
        reply: oneshot::Sender<StreamsDescribeView>,
    },
    /// Validates an `OffsetCommit` or `TxnOffsetCommit` against the streams
    /// group's membership, as Kafka's `StreamsGroup.validateOffsetCommit`
    /// does. `Ok(())` allows the commit, and `Err(code)` rejects it.
    ValidateCommit {
        member_id: String,
        /// The request's `generation_id_or_member_epoch` field, interpreted as
        /// the streams `member_epoch`.
        member_epoch: i32,
        fence: CommitFence,
        /// The `(topic name, partition)` of every partition the commit
        /// writes, which an older epoch is checked against one by one.
        partitions: Vec<(String, i32)>,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    /// KIP-1331: a member's `StreamsGroupTopologyDescriptionUpdate`, past the
    /// handler's protocol gate, group `Read` grant and request checks.
    PushDescription {
        push: Box<DescriptionPush>,
        reply: oneshot::Sender<PushAnswer>,
    },
    Seed(super::super::StreamsGroupSeed),
    Shutdown(oneshot::Sender<()>),
}

/// Kafka's `StreamsGroupHeartbeatResult`: the response, and the internal
/// topics that the handler must create through `CreateTopics`.
#[derive(Debug, Default)]
pub struct StreamsHeartbeatResult {
    pub response: StreamsGroupHeartbeatResponse,
    pub creatable_topics: Vec<super::topology::InternalTopicSpec>,
}

/// Read-only projection of [`StreamsGroupState`] for the
/// `StreamsGroupDescribe` handler.
#[derive(Debug, Clone)]
pub struct StreamsDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology_epoch: i32,
    pub group_state: String,
    /// The topology that the members sent: the subtopologies and their
    /// topics. It is `None` only before any topology is initialized.
    pub topology: Option<StreamsGroupTopologyValue>,
    /// The topology as the last configuration sized it, when it is ready:
    /// Kafka describes this one, with the decided partition count of every
    /// internal topic.
    pub configured_topology: Option<super::topology::ConfiguredTopology>,
    /// The members, by member id.
    pub members: Vec<StreamsDescribeMember>,
    /// KIP-1331: the topology description that the plugin holds for the
    /// group's current topology epoch.
    pub topology_description: Option<TopologyDescription>,
}

/// One member of a [`StreamsDescribeView`].
#[derive(Debug, Clone, Default)]
pub struct StreamsDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub topology_epoch: i32,
    pub process_id: String,
    pub user_endpoint: Option<(String, u16)>,
    pub client_tags: Vec<(String, String)>,
    /// The task offsets that the member last reported, by
    /// `(subtopology, partition)`.
    pub task_offsets: BTreeMap<(String, i32), i64>,
    /// The task end offsets that the member last reported.
    pub task_end_offsets: BTreeMap<(String, i32), i64>,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
    /// The member's target assignment.
    pub target_active: BTreeMap<String, Vec<i32>>,
    pub target_standby: BTreeMap<String, Vec<i32>>,
    pub target_warmup: BTreeMap<String, Vec<i32>>,
}

#[derive(Debug)]
pub struct StreamsGroupActorHandle {
    pub tx: mpsc::Sender<StreamsGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl StreamsGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<StreamsGroupConfig>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, task) = spawn_mailbox(config.actor_mailbox_capacity, |rx| {
            actor_loop(group_id, config, coordinator, rx)
        });
        Self { tx, _task: task }
    }
}

/// Validates a `TxnOffsetCommit` of `partitions`, each a `(topic name,
/// partition)`, against a streams group's membership by sending a message to
/// its actor, as [`validate_offset_commit`] does with
/// [`CommitFence::Transactional`].
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
///
/// The shared `validate_commit` knows only about the classic and
/// consumer `GroupActorHandle`. A streams-group consumer keeps its membership
/// in the streams actor, not a classic one, so this function must validate it
/// instead. Otherwise the broker fences the commit against an empty classic
/// actor and rejects it with `UNKNOWN_MEMBER_ID`.
pub(crate) async fn validate_streams_group_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    send_validate_commit(
        handle,
        member_id,
        member_epoch,
        CommitFence::Transactional,
        partitions,
    )
    .await
}

/// Validates an `OffsetCommit` at `api_version` of `partitions`, each a
/// `(topic name, partition)`, against a streams group's membership by sending
/// a message to its actor, as [`validate_offset_commit`] does with
/// [`CommitFence::Offset`].
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
pub(crate) async fn validate_streams_group_offset_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    send_validate_commit(
        handle,
        member_id,
        member_epoch,
        CommitFence::Offset { api_version },
        partitions,
    )
    .await
}

async fn send_validate_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    fence: CommitFence,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(StreamsGroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            member_epoch,
            fence,
            partitions,
            reply: tx,
        })
        .await
        .is_err()
    {
        return Some(codes::UNKNOWN_SERVER_ERROR);
    }
    match rx.await {
        Ok(Ok(())) => None,
        Ok(Err(code)) => Some(code),
        Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
    }
}

/// The first `OffsetCommit` version that a member of the streams protocol may
/// use.
const FIRST_STREAMS_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// Kafka's `StreamsGroup.validateOffsetCommit`.
///
/// A negative epoch commits on a group with no members: that is the admin
/// client or a consumer that does not use group management. A
/// `TxnOffsetCommit` with no member id and the unknown generation carries no
/// member to check. Otherwise the member must exist, an `OffsetCommit` must be
/// v9 or later, and the member's own epoch commits every partition; a newer
/// epoch is `STALE_MEMBER_EPOCH`. `TxnOffsetCommit` passes no group instance
/// id here, so the transactional skip does not check it.
///
/// An older epoch goes through Kafka's `createAssignmentEpochValidator`
/// (KIP-1251) over `partitions`, each a `(topic name, partition)`: see
/// [`assignment_epoch_error`].
///
/// # Errors
///
/// Returns the error code of a refused commit, which refuses every partition
/// of it.
pub(crate) fn validate_offset_commit(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
    member_id: &str,
    member_epoch: i32,
    fence: CommitFence,
    partitions: &[(String, i32)],
) -> Result<(), i16> {
    if member_epoch < 0 && state.members.is_empty() {
        return Ok(());
    }
    if fence == CommitFence::Transactional && member_epoch == -1 && member_id.is_empty() {
        return Ok(());
    }
    let member = state
        .members
        .get(member_id)
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if let CommitFence::Offset { api_version } = fence
        && api_version < FIRST_STREAMS_PROTOCOL_COMMIT_VERSION
    {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    match member_epoch.cmp(&member.member_epoch) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(codes::STALE_MEMBER_EPOCH),
        std::cmp::Ordering::Less => {
            assignment_epoch_error(member, topology, member_epoch, partitions).map_or(Ok(()), Err)
        }
    }
}

/// Kafka 4.3.1's `StreamsGroup.createAssignmentEpochValidator` (KIP-1251),
/// which lets a member commit at an epoch older than its own.
///
/// The group must hold a topology. Each partition's topic must be a source or
/// repartition source topic of one of its subtopologies
/// (`StreamsTopology.sourceTopicMap`), and the partition must be an active task
/// of that subtopology which the member holds, assigned or pending
/// revocation. The epoch at which the member was assigned that task, the
/// `AssignmentEpochs` of its current assignment record, must not be newer than
/// `member_epoch`. A failure of any of these is `STALE_MEMBER_EPOCH`, and it
/// refuses the whole commit.
///
/// Kafka's `sourceTopicMap` is a `HashMap` that each subtopology overwrites in
/// turn, so a topic that two subtopologies read maps to the last one written.
/// This function takes the last subtopology in the topology's order. A
/// topology that Kafka accepts never has such a topic.
fn assignment_epoch_error(
    member: &state::StreamsMemberState,
    topology: Option<&StreamsGroupTopologyValue>,
    member_epoch: i32,
    partitions: &[(String, i32)],
) -> Option<i16> {
    let Some(topology) = topology else {
        return Some(codes::STALE_MEMBER_EPOCH);
    };
    let holds = |tasks: &BTreeMap<String, Vec<i32>>, subtopology: &str, partition: i32| {
        tasks
            .get(subtopology)
            .is_some_and(|partitions| partitions.contains(&partition))
    };
    let refused = partitions.iter().any(|(topic, partition)| {
        let Some(subtopology) = topology.subtopologies.iter().rev().find(|subtopology| {
            subtopology.source_topics.contains(topic)
                || subtopology
                    .repartition_source_topics
                    .iter()
                    .any(|source| &source.name == topic)
        }) else {
            return true;
        };
        let id = subtopology.subtopology_id.as_str();
        if !holds(&member.active, id, *partition)
            && !holds(&member.active_pending_revocation, id, *partition)
        {
            return true;
        }
        let assigned_at = member.active_task_epochs(id, &[*partition]);
        assigned_at
            .first()
            .is_none_or(|&epoch| member_epoch < epoch)
    });
    refused.then_some(codes::STALE_MEMBER_EPOCH)
}

/// The actor's full mutable state.
///
/// It holds the in-memory state machine, the in-flight
/// `StreamsGroupTopologyValue`, and the last-derived partition metadata. The
/// actor keeps the resolved topology for persistence and reconcile, because
/// [`StreamsGroupState`] tracks only its presence and epoch.
#[derive(Clone)]
struct ActorState {
    state: StreamsGroupState,
    /// The full stored topology. It sits beside `state.topology`, which
    /// carries only the epoch. It is `None` until the first member supplies a
    /// topology.
    topology: Option<StreamsGroupTopologyValue>,
    /// Kafka's `StreamsGroup.metadataHash`: the hash of the required topics in
    /// the image that the most recent reconcile configured the topology
    /// against. A heartbeat that sees another hash reconciles again.
    metadata_hash: i64,
    /// Kafka's `StreamsGroup.validatedTopologyEpoch`: the epoch of the
    /// topology that the group last found configured and ready in the
    /// metadata image when it bumped its epoch, or -1 when it was not ready.
    /// A new group holds 0, as Kafka's `TimelineInteger` starts. A heartbeat
    /// that validates another epoch bumps the group epoch.
    validated_topology_epoch: i32,
    /// Kafka's `StreamsGroup.lastAssignmentConfigs`: the assignment
    /// configuration of the last group epoch bump. A heartbeat that sees
    /// another configuration bumps the group epoch.
    last_assignment_configs: BTreeMap<String, String>,
    /// The internal topics that the topology needs and the metadata image
    /// does not hold. Every heartbeat answer carries them, as Kafka's
    /// `StreamsGroupHeartbeatResult.creatableTopics` does, and `KafkaApis`
    /// sends them to the controller as a `CreateTopics` request.
    creatable_topics: Vec<super::topology::InternalTopicSpec>,
    /// The members whose target the last target assignment changed, set when
    /// a reconcile installed a target and taken by the records of the
    /// transition that installed it.
    target_changed: Option<Vec<String>>,
    /// Whether the topology was configured against the metadata image since
    /// the actor started. A seeded actor has not, so its first heartbeat
    /// configures the topology again, as Kafka does when the configured
    /// topology of a loaded group is empty.
    configured: bool,
    /// Kafka's `StreamsGroup.configuredTopology`: the topology as the most
    /// recent configuration against the metadata image sized it. It is
    /// `None` until a configuration succeeds.
    configured_topology: Option<super::topology::ConfiguredTopology>,
    /// When Kafka's initial rebalance delay of the group ends: set when the
    /// first member joins an empty group, and cleared when the delayed
    /// assignment runs.
    initial_rebalance_deadline: Option<tokio::time::Instant>,
    /// Kafka's `StreamsGroup.assignmentTimestamp`: the wall-clock time in
    /// milliseconds at which the last target assignment calculation
    /// finished, or 0 when there is no previous assignment or its time is
    /// unknown. It is the `AssignmentTimestamp` of the group's target
    /// assignment metadata record, and Kafka's assignment interval runs from
    /// it.
    assignment_timestamp_ms: i64,
    /// KIP-1331: what the topology description plugin holds for the group.
    /// A group metadata record carries it only in trunk mode: see
    /// [`Self::trunk_records`].
    description_epochs: DescriptionEpochs,
    /// Whether the group writes Kafka trunk's KIP-1331 tags 2 and 3 of its
    /// group metadata record: whether
    /// `group.streams.topology.description.plugin.class` is set. Kafka trunk
    /// moves the description epochs away from -1, and so writes the tags,
    /// only through its plugin paths; without a plugin the group writes the
    /// record as Kafka 4.3.1 does, whatever epochs a replayed record held.
    trunk_records: bool,
    /// The description that the in-memory plugin holds for the group. It is
    /// not persisted: the plugin loses it with the broker.
    description: Option<StoredDescription>,
    /// The window in which no other member is asked for the description.
    description_backoff: SolicitationBackoff,
}

impl ActorState {
    /// Apply a published group override and restart its session tick only on change.
    fn refresh_configuration(
        &mut self,
        defaults: &StreamsGroupConfig,
        image: &krabka_metadata::MetadataImage,
        config: &mut StreamsGroupConfig,
        tick: &mut tokio::time::Interval,
    ) {
        let next = resolve_group_config_from_image(defaults, image, &self.state.group_id);
        if next != *config {
            *config = next;
            self.trunk_records = config.topology_description_plugin.is_configured();
            *tick = session_tick(config);
        }
    }

    fn new(group_id: String) -> Self {
        Self {
            state: StreamsGroupState::new(group_id),
            topology: None,
            metadata_hash: 0,
            validated_topology_epoch: 0,
            last_assignment_configs: BTreeMap::new(),
            creatable_topics: Vec::new(),
            target_changed: None,
            configured: false,
            configured_topology: None,
            initial_rebalance_deadline: None,
            assignment_timestamp_ms: 0,
            description_epochs: DescriptionEpochs::default(),
            trunk_records: false,
            description: None,
            description_backoff: SolicitationBackoff::default(),
        }
    }
}

/// What woke the actor loop.
enum Wake {
    /// A mailbox message.
    Message(Box<StreamsGroupActorMessage>),
    /// The session tick, or an armed rebalance timeout that fired.
    SessionCheck,
    /// Kafka's initial rebalance delay ended.
    InitialDelayEnded,
    /// A new metadata image, or `None` once the source closed its watch.
    Image(Option<Arc<krabka_metadata::MetadataImage>>),
}

/// The coordinator's metadata source and its image watch, once the actor
/// holds them.
#[derive(Default)]
struct MetadataLink {
    source: Option<Arc<dyn MetadataSource>>,
    images: Option<tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>>,
}

impl MetadataLink {
    /// Takes the coordinator's metadata source when the link has none, and
    /// returns the current image of the source that it took.
    ///
    /// The broker connects the source after the `__consumer_offsets` replay
    /// has spawned the actors of the loaded groups, so an actor can start
    /// without one. Kafka's coordinator gives every loaded group the metadata
    /// image, so the loop takes the source before it handles each wake-up.
    fn attach(
        &mut self,
        coordinator: &super::super::GroupCoordinator,
    ) -> Option<Arc<krabka_metadata::MetadataImage>> {
        if self.source.is_some() {
            return None;
        }
        let source = coordinator.metadata_source()?;
        self.images = Some(source.watch_image());
        let image = source.current_image();
        self.source = Some(source);
        Some(image)
    }
}

/// The session tick of `config`: the actor looks for expired sessions once
/// per heartbeat interval.
fn session_tick(config: &StreamsGroupConfig) -> tokio::time::Interval {
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick
}

async fn actor_loop(
    group_id: String,
    default_config: Arc<StreamsGroupConfig>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<StreamsGroupActorMessage>,
) {
    let offsets_log = coordinator.offsets_log.clone();
    let mut metadata = MetadataLink::default();
    let mut config = metadata.attach(&coordinator).map_or_else(
        || (*default_config).clone(),
        |image| resolve_group_config_from_image(&default_config, &image, &group_id),
    );
    let mut actor = ActorState::new(group_id);
    actor.trunk_records = config.topology_description_plugin.is_configured();
    let mut tick = session_tick(&config);
    loop {
        let wake = tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => Wake::Message(Box::new(msg)),
                // Every handle is gone.
                None => break,
            },
            _ = tick.tick() => Wake::SessionCheck,
            () = crate::time_util::sleep_until_opt(actor.initial_rebalance_deadline) => {
                Wake::InitialDelayEnded
            }
            () = crate::time_util::sleep_until_opt(actor.state.next_rebalance_deadline()) => {
                Wake::SessionCheck
            }
            image = wait_for_metadata_change(&mut metadata.images) => Wake::Image(image),
        };
        if let Some(image) = metadata.attach(&coordinator) {
            actor.refresh_configuration(&default_config, &image, &mut config, &mut tick);
        }
        let metadata_source = metadata.source.as_ref();
        match wake {
            Wake::Message(msg) => {
                let refused = match handle_message(
                    &mut actor,
                    &config,
                    &*offsets_log,
                    metadata_source,
                    &coordinator,
                    *msg,
                )
                .await
                {
                    Step::Continue => continue,
                    Step::Stop => break,
                    Step::RefusedJoin(refused) => refused,
                };
                // Kafka writes no record for a heartbeat that its coordinator
                // refuses, so such a heartbeat never creates the group. With
                // nothing queued behind it, the actor of a group that holds
                // nothing closes its mailbox, answers what raced in, and
                // leaves no group behind.
                if !rx.is_empty() {
                    refused.send();
                    continue;
                }
                rx.close();
                while let Some(msg) = rx.recv().await {
                    if let Step::RefusedJoin(other) = handle_message(
                        &mut actor,
                        &config,
                        &*offsets_log,
                        metadata_source,
                        &coordinator,
                        msg,
                    )
                    .await
                    {
                        other.send();
                    }
                }
                if actor.holds_nothing() {
                    forget_abandoned_group(&coordinator, &actor.state.group_id);
                }
                refused.send();
                break;
            }
            Wake::SessionCheck => {
                if handle_session_tick(&mut actor, &config, &*offsets_log, &coordinator)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Wake::InitialDelayEnded => {
                // Kafka's `computeDelayedTargetAssignment`.
                actor.initial_rebalance_deadline = None;
                if actor.state.members.is_empty() || !actor.assignment_pending() {
                    continue;
                }
                if reconcile_and_flush(
                    &mut actor,
                    &config,
                    metadata_source,
                    &*offsets_log,
                    &coordinator,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Wake::Image(image) => {
                let Some(image) = image else {
                    metadata.images = None;
                    continue;
                };
                // Kafka reads a group's configuration when it needs it. A
                // changed assignment configuration bumps the group epoch at
                // the next heartbeat, which compares it with the
                // `LastAssignmentConfigs` of the last bump.
                actor.refresh_configuration(&default_config, &image, &mut config, &mut tick);
            }
        }
    }
}

/// What the actor loop does after one message.
enum Step {
    Continue,
    Stop,
    /// A heartbeat that the coordinator refused, for a group that holds
    /// nothing. The loop sends the answer once it decided whether the group
    /// goes away.
    RefusedJoin(Box<DeferredReply>),
}

/// A heartbeat answer that the loop sends later.
struct DeferredReply {
    reply: oneshot::Sender<StreamsHeartbeatResult>,
    response: StreamsGroupHeartbeatResponse,
}

impl DeferredReply {
    fn send(self) {
        let _ = self.reply.send(StreamsHeartbeatResult {
            response: self.response,
            creatable_topics: Vec::new(),
        });
    }
}

impl ActorState {
    /// Whether the target assignment is behind the group epoch.
    fn assignment_pending(&self) -> bool {
        self.state.target.epoch < self.state.group_epoch
    }

    /// The configured topology, when it is ready for an assignment.
    fn ready_topology(&self) -> Option<&super::topology::ConfiguredTopology> {
        self.configured_topology
            .as_ref()
            .filter(|configured| configured.is_ready())
    }

    /// Whether the group holds nothing that a record wrote: its initial group
    /// epoch, no member and no topology. Such a group exists only because a
    /// heartbeat reached its actor.
    fn holds_nothing(&self) -> bool {
        self.state.group_epoch == state::INITIAL_EPOCH
            && self.state.members.is_empty()
            && self.topology.is_none()
    }
}

/// Drops the registry entry and the `Streams` type lock of a group whose
/// actor closed without writing a record. A newer actor for the same id, whose
/// mailbox is open, keeps both.
fn forget_abandoned_group(coordinator: &super::super::GroupCoordinator, group_id: &str) {
    coordinator
        .streams_groups
        .remove_if(group_id, |_, handle| handle.tx.is_closed());
    coordinator
        .group_types
        .remove_if(group_id, |_, group_type| {
            *group_type == super::super::GroupType::Streams
                && !coordinator.streams_groups.contains_key(group_id)
        });
}

/// Handles one mailbox message.
async fn handle_message(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &Arc<super::super::GroupCoordinator>,
    msg: StreamsGroupActorMessage,
) -> Step {
    match msg {
        StreamsGroupActorMessage::Heartbeat {
            request,
            version,
            client_id,
            client_host,
            reply,
        } => {
            match handle_heartbeat(
                actor,
                config,
                offsets_log,
                metadata_source,
                coordinator,
                &request,
                super::super::ClientIdentity {
                    id: &client_id,
                    host: &client_host,
                },
            )
            .await
            {
                Ok(response) if response.error_code != codes::NONE && actor.holds_nothing() => {
                    return Step::RefusedJoin(Box::new(DeferredReply { reply, response }));
                }
                Ok(mut response) => {
                    response::add_missing_client_tags(
                        &mut response,
                        &actor.state,
                        config,
                        &request,
                        version,
                    );
                    description::maybe_request_description(
                        actor,
                        config,
                        &request,
                        version,
                        &mut response,
                        std::time::Instant::now(),
                    );
                    // Kafka answers the internal topics to create only with a
                    // response that the group accepted.
                    let creatable_topics = if response.error_code == codes::NONE {
                        actor.creatable_topics.clone()
                    } else {
                        Vec::new()
                    };
                    let _ = reply.send(StreamsHeartbeatResult {
                        response,
                        creatable_topics,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = %actor.state.group_id,
                        error = %e,
                        "streams-group actor exiting after log-write failure",
                    );
                    let _ = reply.send(StreamsHeartbeatResult {
                        response: response::error_resp(write_failure_code(&e), None),
                        creatable_topics: Vec::new(),
                    });
                    return Step::Stop;
                }
            }
        }
        StreamsGroupActorMessage::Describe { reply } => {
            let mut view = build_describe(
                &actor.state,
                actor.topology.as_ref(),
                actor.ready_topology(),
            );
            view.topology_description = description::stored_description(actor);
            let _ = reply.send(view);
        }
        StreamsGroupActorMessage::PushDescription { push, reply } => {
            match description::handle_push(actor, offsets_log, coordinator, &push).await {
                Ok(answer) => {
                    let _ = reply.send(answer);
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = %actor.state.group_id,
                        error = %e,
                        "streams-group actor exiting after log-write failure",
                    );
                    let _ = reply.send((write_failure_code(&e), None));
                    return Step::Stop;
                }
            }
        }
        StreamsGroupActorMessage::ValidateCommit {
            member_id,
            member_epoch,
            fence,
            partitions,
            reply,
        } => {
            let _ = reply.send(validate_offset_commit(
                &actor.state,
                actor.topology.as_ref(),
                &member_id,
                member_epoch,
                fence,
                &partitions,
            ));
        }
        StreamsGroupActorMessage::Seed(seed) => {
            apply_seed(actor, seed);
        }
        StreamsGroupActorMessage::Shutdown(reply) => {
            let _ = reply.send(());
            return Step::Stop;
        }
    }
    Step::Continue
}

/// The streams config of `group_id`: the group config overrides in `image`
/// over the broker `defaults`.
pub(crate) fn resolve_group_config_from_image(
    defaults: &StreamsGroupConfig,
    image: &krabka_metadata::MetadataImage,
    group_id: &str,
) -> StreamsGroupConfig {
    let Some(overrides) = image.group_config(group_id) else {
        return defaults.clone();
    };
    match defaults.with_group_overrides(overrides) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(group_id, %error, "ignoring invalid persisted streams group config");
            defaults.clone()
        }
    }
}

async fn wait_for_metadata_change(
    rx: &mut Option<tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>>,
) -> Option<Arc<krabka_metadata::MetadataImage>> {
    match rx {
        Some(rx) => {
            rx.changed().await.ok()?;
            Some(rx.borrow_and_update().clone())
        }
        None => std::future::pending().await,
    }
}

use crate::txn::util::now_millis as chrono_now_ms;
