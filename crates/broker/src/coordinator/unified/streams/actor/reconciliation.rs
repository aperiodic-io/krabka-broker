//! Recomputation of a streams group's group epoch and target assignment.
//!
//! Reconciliation is the one place that bumps the group epoch. It resolves the
//! stored topology against the current [`MetadataImage`], creates the internal
//! topics the topology needs, records any blocking topology status, and bumps
//! the group epoch. It then runs the assignor to produce the new active and
//! standby target, unless Kafka's initial rebalance delay or assignment
//! interval holds the assignment back; the group is `Assigning` meanwhile.
//!
//! [`MetadataImage`]: krabka_metadata::MetadataImage

use {
    super::ActorState,
    crate::{
        coordinator::unified::{
            can_compute_next_target_assignment,
            streams::{
                assignor::{self, AssignorInput, AssignorMember},
                config::StreamsGroupConfig,
                persistence::{NO_VALIDATED_TOPOLOGY_EPOCH, StreamsGroupTopologyValue},
                state::{StreamsGroupStatePhase, StreamsTargetAssignment},
                topology,
            },
            wall_clock_ms,
        },
        metadata_source::MetadataSource,
    },
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        sync::Arc,
    },
    tokio::time::Instant,
};

/// Kafka's status detail while the initial rebalance delay holds the first
/// assignment of a group back.
pub(super) const INITIAL_DELAY_DETAIL: &str =
    "Assignment delayed due to the configured initial rebalance delay.";

/// Kafka's status detail while the assignment interval holds the next
/// assignment back.
pub(super) const ASSIGNMENT_INTERVAL_DETAIL: &str =
    "Assignment delayed due to the configured assignment interval.";

/// Bumps the group epoch when the group is dirty, and computes the target
/// assignment when it is behind the group epoch and nothing delays it.
///
/// With no connected [`MetadataSource`], as in the unit tests, or before any
/// member supplies a topology, the group stays `NotReady` with an empty
/// target. Members still advance their epoch but get no tasks.
///
/// Otherwise the function configures the topology against the current image
/// ([`topology::configure_topics`]), records the internal topics that the
/// heartbeat must create, and runs the assignor. A topology that cannot be
/// configured leaves the group `NotReady`: the heartbeat checks the topology
/// before it gets here, so that only a metadata change between the two checks
/// reaches that path.
pub(super) fn reconcile(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if actor.state.dirty {
        update_group_epoch(actor, config, metadata_source);
    }
    if actor.assignment_pending() && assignment_delay(actor, config, Instant::now()).is_none() {
        update_target_assignment(actor, config);
    }
}

/// The status detail of Kafka's `ASSIGNMENT_DELAYED` when a delay holds the
/// assignment back at `now`, or `None`.
///
/// The initial rebalance delay holds any assignment back while it runs. The
/// assignment interval holds back a pending assignment until it elapsed since
/// the last one; a zero interval, or a group that never computed one, does not
/// wait (Kafka's `canComputeNextTargetAssignment`).
pub(super) fn assignment_delay(
    actor: &ActorState,
    config: &StreamsGroupConfig,
    now: Instant,
) -> Option<&'static str> {
    if actor
        .initial_rebalance_deadline
        .is_some_and(|deadline| now < deadline)
    {
        return Some(INITIAL_DELAY_DETAIL);
    }
    let interval_running = !can_compute_next_target_assignment(
        actor.assignment_timestamp_ms,
        config.assignment_interval,
        wall_clock_ms(),
    );
    (actor.assignment_pending() && interval_running).then_some(ASSIGNMENT_INTERVAL_DETAIL)
}

/// Configures the topology of a seeded group against the current image
/// without a new target, as Kafka's first heartbeat after a load does.
///
/// The function records the topology status and the internal topics that the
/// image does not hold, so that the heartbeat asks for them again. It runs
/// once per actor, and a reconcile makes it unnecessary.
pub(super) fn configure_after_load(actor: &mut ActorState, source: &Arc<dyn MetadataSource>) {
    if actor.configured {
        return;
    }
    let Some(topology) = actor.topology.clone() else {
        return;
    };
    actor.configured = true;
    let image = source.current_image();
    // A topology that cannot be configured gets its error from the heartbeat
    // check, so it records nothing here.
    let Ok(configured) = topology::configure_topics(&topology, &image) else {
        return;
    };
    actor.creatable_topics = topology::internal_topic_specs(&configured);
    actor.state.status.clone_from(&configured.status);
    actor.configured_topology = Some(configured);
}

/// Kafka's `streamsGroupAssignmentConfigs`: the assignment configuration that
/// Kafka 4.3.1 records in `LastAssignmentConfigs` and compares on every
/// heartbeat, `num.standby.replicas` alone.
#[must_use]
pub(super) fn assignment_configs(config: &StreamsGroupConfig) -> BTreeMap<String, String> {
    BTreeMap::from([(
        "num.standby.replicas".to_owned(),
        config.num_standby_replicas.to_string(),
    )])
}

/// The `validatedTopologyEpoch` that Kafka's `streamsGroupHeartbeat` derives
/// from the configured topology: the topology epoch when the configured
/// topology is ready, and -1 otherwise.
#[must_use]
pub(super) fn validated_topology_epoch(actor: &ActorState) -> i32 {
    actor
        .ready_topology()
        .and(actor.topology.as_ref())
        .map_or(NO_VALIDATED_TOPOLOGY_EPOCH, |topology| topology.epoch)
}

/// Kafka's checks in `streamsGroupHeartbeat` that bump the group epoch of a
/// group whose members and topology did not change: a topology epoch that
/// the group validated, or stopped validating, since its last bump, and an
/// assignment configuration other than the last one.
#[must_use]
pub(super) fn validation_or_configs_changed(
    actor: &ActorState,
    config: &StreamsGroupConfig,
) -> bool {
    validated_topology_epoch(actor) != actor.validated_topology_epoch
        || assignment_configs(config) != actor.last_assignment_configs
}

/// Configures the topology against the current image and bumps the group
/// epoch, which leaves the target assignment behind it. The bump records the
/// validated topology epoch and the assignment configuration, which Kafka's
/// `newStreamsGroupMetadataRecord` writes beside the epoch.
fn update_group_epoch(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if let (Some(source), Some(topology)) = (metadata_source, actor.topology.clone()) {
        let image = source.current_image();
        actor.configured = true;
        actor.metadata_hash = topology::metadata_hash(&topology, &image);
        actor.creatable_topics.clear();
        match topology::configure_topics(&topology, &image) {
            Ok(configured) => {
                // The heartbeat hands the internal topics that the image does
                // not hold to `CreateTopics`, as Kafka's `KafkaApis` does.
                actor.creatable_topics = topology::internal_topic_specs(&configured);
                actor.state.status.clone_from(&configured.status);
                actor.configured_topology = Some(configured);
            }
            Err(error) => {
                tracing::warn!(
                    group_id = %actor.state.group_id,
                    %error,
                    "streams topology cannot be configured",
                );
                actor.state.status = None;
                actor.configured_topology = None;
            }
        }
    } else {
        // No metadata source or no topology yet: the group cannot be assigned.
        actor.configured_topology = None;
    }
    if !actor.state.bump_epoch() {
        return;
    }
    actor.validated_topology_epoch = validated_topology_epoch(actor);
    actor.last_assignment_configs = assignment_configs(config);
    actor.state.dirty = false;
    actor.state.phase = if actor.state.members.is_empty() {
        StreamsGroupStatePhase::Empty
    } else if actor.ready_topology().is_none() {
        StreamsGroupStatePhase::NotReady
    } else {
        StreamsGroupStatePhase::Assigning
    };
}

/// Installs the target assignment of the group epoch: the assignor's output
/// for a ready topology, and an empty target otherwise.
fn update_target_assignment(actor: &mut ActorState, config: &StreamsGroupConfig) {
    let ready = actor
        .ready_topology()
        .map(topology::ConfiguredTopology::number_of_tasks)
        .zip(actor.topology.clone());
    if let Some((number_of_tasks, topology)) = ready {
        install_computed_target(actor, config, &topology, &number_of_tasks);
    } else {
        actor.target_changed = Some(
            actor
                .state
                .install_target(StreamsTargetAssignment::default()),
        );
        actor.state.phase = if actor.state.members.is_empty() {
            StreamsGroupStatePhase::Empty
        } else {
            StreamsGroupStatePhase::NotReady
        };
    }
    // Kafka's `TargetAssignmentBuilder.build` stamps the record with the time
    // at which the calculation finished, whether the topology was ready or
    // not.
    actor.assignment_timestamp_ms = wall_clock_ms();
}

/// Bumps the group epoch and installs the assignor's target at once, with no
/// delay, for the reconciliation model.
#[cfg(test)]
pub(super) fn compute_and_install_target(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) {
    if !actor.state.bump_epoch() {
        return;
    }
    actor.state.dirty = false;
    install_computed_target(actor, config, topology, num_tasks);
}

/// Runs the assignor over the resolved topology and installs its output as the
/// target of the group epoch.
///
/// Installing the target computes the active revoke-split. The phase becomes
/// `Reconciling` while any member still owns un-revoked active tasks, and
/// `Stable` otherwise.
fn install_computed_target(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) {
    let members: Vec<AssignorMember> = actor
        .state
        .members
        .values()
        .map(|m| AssignorMember {
            member_id: m.member_id.clone(),
            process_id: m.process_id.clone(),
            current_active: m.active.clone(),
            current_standby: m.standby.clone(),
            current_warmup: m.warmup.clone(),
            task_offsets: m
                .task_offsets
                .iter()
                .map(|(task, offset)| (task.clone(), offset.0))
                .collect(),
        })
        .collect();

    let stateful: BTreeSet<String> = topology
        .subtopologies
        .iter()
        .filter(|s| !s.state_changelog_topics.is_empty())
        .map(|s| s.subtopology_id.clone())
        .collect();

    let input = AssignorInput {
        tasks: topology::task_set(num_tasks),
        stateful,
        num_standby_replicas: config.num_standby_replicas,
    };
    let assignment = assignor::assign(&members, &input);

    // Kafka's default assignment refiner hands out no warmup tasks.
    let target = StreamsTargetAssignment {
        epoch: 0,
        active: assignment.active,
        standby: assignment.standby,
        warmup: HashMap::new(),
    };
    actor.target_changed = Some(actor.state.install_target(target));
    // A computed target ends `NotReady`; the members then reconcile toward it.
    actor.state.phase = StreamsGroupStatePhase::Reconciling;
    actor.state.refresh_phase();
}
