//! Projection of the actor's state onto the persisted `__consumer_offsets`
//! record values, and hydration back from a seed.
//!
//! One heartbeat can change the group epoch, the topology, the target
//! assignment, and several members at once, so the actor collects the whole
//! change as a [`PendingStreamsRecords`] and writes it as one batch. The same
//! per-member projections feed the last-known-good
//! [`StreamsGroupSeed`] handed to the coordinator cache, and
//! [`apply_seed`] is their inverse for bootstrap replay and respawn.

use super::ActorState;
use crate::coordinator::unified::{
    GroupCoordinator, StreamsGroupSeed,
    member_records::MemberValues,
    offsets_log::OffsetsLog,
    streams::{
        persistence::{
            DescriptionEpochs, LastAssignmentConfig, PendingStreamsRecords, StreamsEndpoint,
            StreamsGroupCurrentMemberAssignmentValue, StreamsGroupMemberMetadataValue,
            StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMemberValue,
            StreamsGroupTargetAssignmentMetadataValue,
        },
        state::{
            StoredTopologyHandle, StreamsGroupState, StreamsGroupStatePhase, StreamsMemberState,
        },
    },
};

/// The records of one streams-group transition, as Kafka's
/// `GroupMetadataManager` writes them: a record only where the transition
/// changed what it holds.
///
/// [`StreamsRecorder::start`] takes the group epoch, whether the group holds a
/// topology, and the values of the members that the transition may change.
/// [`StreamsRecorder::finish`] compares them with the group after it: a member
/// that went gets `removeStreamsMember`'s tombstones, a changed member a
/// member record (`hasStreamsMemberMetadataChanged`), a topology that the
/// transition initialized its record (`maybeUpdateTopology`), a moved epoch
/// the group record (`newStreamsGroupMetadataRecord`), the targets that the
/// transition's target assignment changed with the target metadata
/// (`TargetAssignmentBuilder`), and a changed current assignment its record
/// (`maybeReconcile`).
pub(super) struct StreamsRecorder {
    group_epoch: i32,
    had_topology: bool,
    members:
        MemberValues<StreamsGroupMemberMetadataValue, StreamsGroupCurrentMemberAssignmentValue>,
}

/// The values of `member_id` that [`StreamsRecorder`] compares, if the group
/// holds the member.
fn member_values(
    state: &StreamsGroupState,
    member_id: &str,
) -> Option<(
    StreamsGroupMemberMetadataValue,
    StreamsGroupCurrentMemberAssignmentValue,
)> {
    state.members.get(member_id).map(|member| {
        (
            member_metadata_value(member),
            current_assignment_value(member),
        )
    })
}

impl StreamsRecorder {
    pub(super) fn start(actor: &ActorState, member_ids: &[&str]) -> Self {
        Self {
            group_epoch: actor.state.group_epoch,
            had_topology: actor.topology.is_some(),
            members: MemberValues::take(member_ids.iter().copied(), |member_id| {
                member_values(&actor.state, member_id)
            }),
        }
    }

    /// The records of the transition. The members whose target changed come
    /// from the target assignment that the transition computed, if it did.
    pub(super) fn finish(self, actor: &mut ActorState) -> PendingStreamsRecords {
        let mut pending = PendingStreamsRecords::default();
        self.members.record_changes(&mut pending, |member_id| {
            member_values(&actor.state, member_id)
        });
        if !self.had_topology {
            pending.topology.clone_from(&actor.topology);
        }
        if actor.state.group_epoch != self.group_epoch {
            pending.group_metadata = Some(group_metadata_value(actor));
        }
        if let Some(changed) = actor.target_changed.take() {
            let state = &actor.state;
            pending.target_metadata = Some(StreamsGroupTargetAssignmentMetadataValue {
                assignment_epoch: state.target.epoch,
                assignment_timestamp_ms: actor.assignment_timestamp_ms,
            });
            for member_id in changed {
                let value = target_member_value(state, &member_id).unwrap_or_default();
                pending.target_per_member.push((member_id, Some(value)));
            }
        }
        pending
    }
}

/// Kafka's `newStreamsGroupMetadataRecord`: the group epoch, the metadata
/// hash, the validated topology epoch, and the assignment configuration as a
/// list in key order, which Kafka never writes as null. In trunk mode it also
/// carries the KIP-1331 description epochs, as Kafka trunk's record does;
/// otherwise it leaves them at -1, out of the record, as Kafka 4.3.1 has no
/// such tags.
pub(super) fn group_metadata_value(actor: &ActorState) -> StreamsGroupMetadataValue {
    StreamsGroupMetadataValue {
        epoch: actor.state.group_epoch,
        metadata_hash: actor.metadata_hash,
        validated_topology_epoch: actor.validated_topology_epoch,
        last_assignment_configs: Some(
            actor
                .last_assignment_configs
                .iter()
                .map(|(key, value)| LastAssignmentConfig {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        ),
        description: if actor.trunk_records {
            actor.description_epochs
        } else {
            DescriptionEpochs::default()
        },
    }
}

fn member_metadata_value(m: &StreamsMemberState) -> StreamsGroupMemberMetadataValue {
    StreamsGroupMemberMetadataValue {
        instance_id: m.instance_id.clone(),
        rack_id: m.rack_id.clone(),
        client_id: m.client_id.clone(),
        client_host: m.client_host.clone(),
        process_id: m.process_id.clone(),
        user_endpoint: m
            .user_endpoint
            .as_ref()
            .map(|(host, port)| StreamsEndpoint {
                host: host.clone(),
                port: *port,
            }),
        client_tags: m.client_tags.clone(),
        rebalance_timeout_ms: m.rebalance_timeout_ms,
        topology_epoch: m.topology_epoch,
    }
}

fn current_assignment_value(m: &StreamsMemberState) -> StreamsGroupCurrentMemberAssignmentValue {
    StreamsGroupCurrentMemberAssignmentValue {
        member_epoch: m.member_epoch,
        previous_member_epoch: m.previous_member_epoch,
        state: m.assignment_state.into(),
        active: m.active.clone(),
        standby: m.standby.clone(),
        warmup: m.warmup.clone(),
        active_pending_revocation: m.active_pending_revocation.clone(),
        standby_pending_revocation: m.standby_pending_revocation.clone(),
        warmup_pending_revocation: m.warmup_pending_revocation.clone(),
        active_epochs: active_epochs(m, &m.active),
        active_pending_revocation_epochs: active_epochs(m, &m.active_pending_revocation),
    }
}

/// The `AssignmentEpochs` of each entry of `tasks`, as Kafka's
/// `toTaskIdsWithEpochs` writes one for every active entry.
fn active_epochs(
    m: &StreamsMemberState,
    tasks: &std::collections::BTreeMap<String, Vec<i32>>,
) -> std::collections::BTreeMap<String, Vec<i32>> {
    tasks
        .iter()
        .map(|(subtopology_id, partitions)| {
            (
                subtopology_id.clone(),
                m.active_task_epochs(subtopology_id, partitions),
            )
        })
        .collect()
}

/// Kafka's `TasksTupleWithEpochs.parseActiveTasksWithEpochs`: the epoch of
/// each active task of a current assignment record. An entry whose
/// `AssignmentEpochs` is null, or does not match its partitions in length,
/// gives every partition the member epoch; Kafka logs the mismatch.
fn replayed_active_epochs(
    group_id: &str,
    member_epoch: i32,
    tasks: &std::collections::BTreeMap<String, Vec<i32>>,
    epochs: &std::collections::BTreeMap<String, Vec<i32>>,
) -> impl Iterator<Item = ((String, i32), i32)> {
    let mut out = Vec::new();
    for (subtopology_id, partitions) in tasks {
        match epochs.get(subtopology_id) {
            Some(epochs) if epochs.len() == partitions.len() => {
                out.extend(
                    partitions
                        .iter()
                        .zip(epochs)
                        .map(|(partition, epoch)| ((subtopology_id.clone(), *partition), *epoch)),
                );
            }
            mismatched => {
                if let Some(epochs) = mismatched {
                    tracing::error!(
                        group_id,
                        "Size of assignment epochs {} is not equal to partitions {} for \
                         subtopology {subtopology_id}. Using default epoch {member_epoch} for \
                         all partitions.",
                        epochs.len(),
                        partitions.len(),
                    );
                }
                out.extend(
                    partitions
                        .iter()
                        .map(|partition| ((subtopology_id.clone(), *partition), member_epoch)),
                );
            }
        }
    }
    out.into_iter()
}

fn target_member_value(
    state: &StreamsGroupState,
    member_id: &str,
) -> Option<StreamsGroupTargetAssignmentMemberValue> {
    let active = state.target.active.get(member_id).cloned();
    let standby = state.target.standby.get(member_id).cloned();
    let warmup = state.target.warmup.get(member_id).cloned();
    if active.is_none() && standby.is_none() && warmup.is_none() {
        return None;
    }
    Some(StreamsGroupTargetAssignmentMemberValue {
        active: active.unwrap_or_default(),
        standby: standby.unwrap_or_default(),
        warmup: warmup.unwrap_or_default(),
    })
}

crate::coordinator::unified::persistence::flush_pending_records! {
    actor: &ActorState, pending: PendingStreamsRecords;
    offsets_log, coordinator, now_ms;
    group &actor.state.group_id;
    encode pending.into_batch(&actor.state.group_id, now_ms);
    cache coordinator.update_streams_cache(&actor.state.group_id, snapshot_seed(actor));
}

/// Persist the reconciler's current delta after a timer or configuration change.
pub(super) async fn reconcile_and_flush(
    actor: &mut ActorState,
    config: &super::super::config::StreamsGroupConfig,
    metadata_source: Option<&std::sync::Arc<dyn crate::metadata_source::MetadataSource>>,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    let recorder = StreamsRecorder::start(actor, &[]);
    super::reconciliation::reconcile(actor, config, metadata_source);
    let pending = recorder.finish(actor);
    flush_pending(
        actor,
        pending,
        offsets_log,
        coordinator,
        super::chrono_now_ms(),
    )
    .await
}

/// Snapshots the full actor state into a `StreamsGroupSeed` for the cache and
/// for a respawned actor. The result matches what bootstrap replay produces.
pub(super) fn snapshot_seed(actor: &ActorState) -> StreamsGroupSeed {
    let state = &actor.state;
    crate::coordinator::unified::seeds::snapshot_member_maps! { state;
        members, current_per_member, target_per_member;
        member_metadata_value, current_assignment_value; |mid, m| {
            if let Some(tv) = target_member_value(state, mid) {
                target_per_member.insert(mid.clone(), tv);
            }
        }
    }
    StreamsGroupSeed {
        group_epoch: state.group_epoch,
        metadata_hash: actor.metadata_hash,
        validated_topology_epoch: actor.validated_topology_epoch,
        last_assignment_configs: actor.last_assignment_configs.clone(),
        description_epochs: actor.description_epochs,
        assignment_epoch: state.target.epoch,
        assignment_timestamp_ms: actor.assignment_timestamp_ms,
        topology: actor.topology.clone(),
        members,
        target_per_member,
        current_per_member,
    }
}

/// Hydrates the actor from a `StreamsGroupSeed`, on bootstrap replay or on a
/// respawn.
pub(super) fn apply_seed(actor: &mut ActorState, seed: StreamsGroupSeed) {
    let state = &mut actor.state;
    state.group_epoch = seed.group_epoch;
    actor.metadata_hash = seed.metadata_hash;
    actor.validated_topology_epoch = seed.validated_topology_epoch;
    actor.description_epochs = seed.description_epochs;
    actor.last_assignment_configs = seed.last_assignment_configs;
    state.target.epoch = seed.assignment_epoch;
    state.assignment_epoch = seed.assignment_epoch;
    actor.assignment_timestamp_ms = seed.assignment_timestamp_ms;
    if let Some(topology) = &seed.topology {
        state.topology = Some(StoredTopologyHandle {
            epoch: topology.epoch,
        });
        state.topology_epoch = topology.epoch;
    }
    actor.topology = seed.topology;

    for (mid, meta) in seed.members {
        let mut m = StreamsMemberState::joining(mid.clone(), meta.client_id, meta.client_host);
        m.instance_id = meta.instance_id;
        m.rack_id = meta.rack_id;
        m.process_id = meta.process_id;
        m.user_endpoint = meta.user_endpoint.map(|ep| (ep.host, ep.port));
        m.client_tags = meta.client_tags;
        m.rebalance_timeout_ms = meta.rebalance_timeout_ms;
        m.topology_epoch = meta.topology_epoch;
        state.members.insert(mid, m);
    }
    crate::coordinator::unified::seeds::hydrate_member_epochs!(state, seed; m, cur {
            m.assignment_state = cur.state.into();
            m.active_epochs = replayed_active_epochs(
                &state.group_id,
                cur.member_epoch,
                &cur.active,
                &cur.active_epochs,
            )
            .chain(replayed_active_epochs(
                &state.group_id,
                cur.member_epoch,
                &cur.active_pending_revocation,
                &cur.active_pending_revocation_epochs,
            ))
            .collect();
            m.active = cur.active;
            m.standby = cur.standby;
            m.warmup = cur.warmup;
            m.active_pending_revocation = cur.active_pending_revocation;
            m.standby_pending_revocation = cur.standby_pending_revocation;
            m.warmup_pending_revocation = cur.warmup_pending_revocation;
            // The member got this assignment before the load.
            m.sent_tasks = [m.active.clone(), m.standby.clone(), m.warmup.clone()];
    });
    for (mid, tv) in seed.target_per_member {
        // A target record, an empty one included, gives the member a target,
        // as Kafka's replay of `StreamsGroupTargetAssignmentMemberValue` does.
        state.target.active.insert(mid.clone(), tv.active);
        if !tv.standby.is_empty() {
            state.target.standby.insert(mid.clone(), tv.standby);
        }
        if !tv.warmup.is_empty() {
            state.target.warmup.insert(mid, tv.warmup);
        }
    }
    state.arm_loaded_rebalance_timeouts(std::time::Instant::now());
    state.phase = if actor.topology.is_none() {
        StreamsGroupStatePhase::NotReady
    } else {
        StreamsGroupStatePhase::Reconciling
    };
    state.refresh_phase();
    state.dirty = false;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        StreamsGroupTopologyValue, StreamsMemberWireState,
    };

    #[test]
    fn seed_hydrates_state() {
        let mut actor = ActorState::new("g".into());
        let mut members = std::collections::HashMap::new();
        members.insert(
            "m1".to_string(),
            StreamsGroupMemberMetadataValue {
                instance_id: Some("i1".into()),
                rack_id: Some("r1".into()),
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                process_id: "p1".into(),
                user_endpoint: Some(StreamsEndpoint {
                    host: "h".into(),
                    port: 9092,
                }),
                client_tags: vec![],
                rebalance_timeout_ms: 60_000,
                topology_epoch: 2,
            },
        );
        let mut current = std::collections::HashMap::new();
        current.insert(
            "m1".to_string(),
            StreamsGroupCurrentMemberAssignmentValue {
                active_epochs: maplit::btreemap! {"0".to_string() => vec![3, 4]},
                ..crate::coordinator::unified::test_support::stable_streams_assignment(
                    (4, 3),
                    maplit::btreemap! {"0".to_string() => vec![0, 1]},
                )
            },
        );
        let mut target = std::collections::HashMap::new();
        target.insert(
            "m1".to_string(),
            StreamsGroupTargetAssignmentMemberValue {
                active: maplit::btreemap! {"0".to_string() => vec![0, 1]},
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
            },
        );
        let seed = StreamsGroupSeed {
            group_epoch: 4,
            metadata_hash: 11,
            validated_topology_epoch: 2,
            last_assignment_configs: maplit::btreemap! {
                "num.standby.replicas".to_string() => "1".to_string(),
            },
            description_epochs: DescriptionEpochs {
                stored: 2,
                failed: -1,
            },
            assignment_epoch: 4,
            assignment_timestamp_ms: 0,
            topology: Some(StreamsGroupTopologyValue {
                epoch: 2,
                subtopologies: vec![],
            }),
            members,
            target_per_member: target,
            current_per_member: current,
        };
        apply_seed(&mut actor, seed);

        check!(actor.state.group_epoch == 4);
        check!(actor.metadata_hash == 11);
        check!(actor.validated_topology_epoch == 2);
        check!(
            actor.description_epochs
                == DescriptionEpochs {
                    stored: 2,
                    failed: -1,
                }
        );
        check!(
            actor.last_assignment_configs
                == maplit::btreemap! {
                    "num.standby.replicas".to_string() => "1".to_string(),
                }
        );
        check!(actor.state.target.epoch == 4);
        check!(actor.state.topology_epoch == 2);
        let m = actor.state.members.get("m1").expect("member restored");
        check!(m.member_epoch == 4);
        check!(m.previous_member_epoch == 3);
        check!(m.process_id == "p1");
        check!(m.active == maplit::btreemap! {"0".to_string() => vec![0, 1]});
        check!(
            m.active_epochs
                == maplit::btreemap! {("0".to_string(), 0) => 3, ("0".to_string(), 1) => 4}
        );
        check!(
            actor.state.target.active["m1"] == maplit::btreemap! {"0".to_string() => vec![0, 1]}
        );
        check!(actor.state.phase == StreamsGroupStatePhase::Stable);
    }

    /// Kafka's `parseActiveTasksWithEpochs` on replay: the epochs of an entry
    /// that lists one per partition, else the member epoch for each partition
    /// of the entry, for the active tasks and the active tasks pending
    /// revocation alike.
    #[test]
    fn a_seed_restores_the_active_task_epochs() {
        let task = |partition| ("0".to_string(), partition);
        // (case, AssignmentEpochs of the active entry, expected epochs)
        let rows = [
            (
                "one epoch per partition",
                Some(vec![3, 4]),
                maplit::btreemap! {task(0) => 3, task(1) => 4, task(2) => 6},
            ),
            (
                "no epochs: a record from before KIP-1251",
                None,
                maplit::btreemap! {task(0) => 7, task(1) => 7, task(2) => 6},
            ),
            (
                "a length that does not match",
                Some(vec![3]),
                maplit::btreemap! {task(0) => 7, task(1) => 7, task(2) => 6},
            ),
        ];
        for (case, epochs, expected) in rows {
            let mut actor = ActorState::new("g".into());
            let seed = StreamsGroupSeed {
                members: [(
                    "m1".to_string(),
                    crate::coordinator::unified::test_support::plain_streams_member(0),
                )]
                .into(),
                current_per_member: [(
                    "m1".to_string(),
                    StreamsGroupCurrentMemberAssignmentValue {
                        member_epoch: 7,
                        previous_member_epoch: 6,
                        state: StreamsMemberWireState::UnrevokedTasks,
                        active: maplit::btreemap! {"0".to_string() => vec![0, 1]},
                        active_pending_revocation: maplit::btreemap! {"0".to_string() => vec![2]},
                        active_epochs: epochs
                            .map(|epochs| maplit::btreemap! {"0".to_string() => epochs})
                            .unwrap_or_default(),
                        active_pending_revocation_epochs: maplit::btreemap! {
                            "0".to_string() => vec![6],
                        },
                        ..StreamsGroupCurrentMemberAssignmentValue::default()
                    },
                )]
                .into(),
                ..StreamsGroupSeed::default()
            };
            apply_seed(&mut actor, seed);
            check!(
                actor.state.members["m1"].active_epochs == expected,
                "{case}"
            );
        }
    }
}
