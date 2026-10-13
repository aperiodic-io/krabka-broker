//! Per-row planning for `AlterPartitionReassignments`: it turns one alter row
//! into the `PartitionRecord` the controller submits, or into a wire error
//! code.
//!
//! The logic here is pure and follows Kafka's controller. A start
//! (`ReplicationControlManager.changePartitionReassignment`) validates the
//! target, computes `adding` and `removing` against the full current replica
//! list, and writes the target replicas followed by the removing ones
//! (`PartitionReassignmentReplicas`). A cancel
//! (`cancelPartitionReassignment`) reverts to the replicas the reassignment
//! started from (`PartitionReassignmentRevert`). Both then pass through the
//! same step as Kafka's `PartitionChangeBuilder.build`: complete the
//! reassignment at once when the ISR already allows it, keep or elect a leader,
//! and bump the leader epoch when a replica leaves.

use {
    crate::{
        codes::{
            ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, INVALID_REPLICATION_FACTOR,
            INVALID_REQUEST, NO_REASSIGNMENT_IN_PROGRESS, POLICY_VIOLATION,
            UNKNOWN_TOPIC_OR_PARTITION,
        },
        config_keys::resolve_unclean_leader_election_enabled,
        metadata_epoch::next_partition_change,
        reassignment::{CompletedReassignment, maybe_complete_reassignment, remap_directories},
    },
    krabka_metadata::{MetadataImage, PartitionRecord},
    krabka_raft::NodeId,
    std::collections::BTreeSet,
};

/// Per-row rejection: a Kafka wire error code and a readable message.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RowError {
    pub(super) code: i16,
    pub(super) message: String,
}

/// The cancel triple [`krabka_verified::reassignment_plan_admission`] takes.
#[derive(Clone, Copy)]
struct PlanMode {
    /// Whether this is a cancel.
    cancel: bool,
    /// Whether an approved break-glass proposal covers the cancel.
    approved: bool,
    /// Whether a reassignment is in progress.
    in_progress: bool,
}

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request.
///
/// `cancel_approved` is KFC-9's answer for this row: whether an approved
/// break-glass proposal covers a cancel of this partition. The caller resolves
/// it against the metadata image, so the per-row decision stays in this pure
/// function. It is `true` on a broker that gates nothing, and it is read only
/// on the cancel path.
///
/// The return values are:
///   - `Ok(Some(PartitionRecord))`: submit this record
///   - `Ok(None)`: do nothing, because the row changes nothing
///   - `Err(RowError)`: reject this row with its wire code and message
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
    cancel_approved: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    let pr = image
        .partition(topic, partition)
        .ok_or_else(|| unknown_partition(image, topic, partition))?;

    match target {
        None => cancel_path(
            pr,
            cancel_approved,
            resolve_unclean_leader_election_enabled(image, topic),
        ),
        Some(target_slice) => {
            let target = validate_target(target_slice, image)?;
            if !allow_rf_change {
                validate_replication_factor_unchanged(pr, target.len())?;
            }
            start_path(pr, &target)
        }
    }
}

/// The row message that an unapproved cancel carries when the caller has no
/// refusal text of its own to put there.
///
/// The handler replaces it with the gate's own text, which names the proposal
/// that nearly authorized the cancel. This constant is what a caller that
/// resolved the gate elsewhere still gets.
const CANCEL_NEEDS_APPROVAL: &str = "a reassignment cancel needs an approved break-glass proposal";

/// Kafka's text for `NO_REASSIGNMENT_IN_PROGRESS`, which
/// `cancelPartitionReassignment` uses as its message.
const NO_REASSIGNMENT_MESSAGE: &str = "No partition reassignment is in progress.";

/// Kafka's `alterPartitionReassignment` answer for a row the image does not
/// hold: one message for an unknown topic, another for an unknown partition.
fn unknown_partition(image: &MetadataImage, topic: &str, partition: i32) -> RowError {
    let message = if image.topic(topic).is_none() {
        format!("Unable to find a topic named {topic}.")
    } else {
        format!("Unable to find partition {topic}:{partition}.")
    };
    RowError {
        code: UNKNOWN_TOPIC_OR_PARTITION,
        message,
    }
}

/// Kafka's `validateManualPartitionAssignment` for a reassignment target.
///
/// Kafka walks the target in broker id order, so for a target with two faults
/// the lowest broker id decides which one the row reports.
fn validate_target(target: &[i32], image: &MetadataImage) -> Result<Vec<NodeId>, RowError> {
    if target.is_empty() {
        return Err(RowError {
            code: INVALID_REPLICA_ASSIGNMENT,
            message: "The manual partition assignment includes an empty replica list.".into(),
        });
    }
    let mut sorted = target.to_vec();
    sorted.sort_unstable();
    let mut previous = None;
    for id in sorted {
        let registered = u64::try_from(id).is_ok_and(|node| image.broker(NodeId(node)).is_some());
        if !registered {
            return Err(RowError {
                code: INVALID_REPLICA_ASSIGNMENT,
                message: format!(
                    "The manual partition assignment includes broker {id}, but no such broker \
                     is registered."
                ),
            });
        }
        if previous == Some(id) {
            return Err(RowError {
                code: INVALID_REPLICA_ASSIGNMENT,
                message: format!(
                    "The manual partition assignment includes the broker {id} more than once."
                ),
            });
        }
        previous = Some(id);
    }
    target
        .iter()
        .map(|&id| u64::try_from(id).map(NodeId))
        .collect::<Result<_, _>>()
        .map_err(|_| RowError {
            code: INVALID_REPLICA_ASSIGNMENT,
            message: "negative broker id".into(),
        })
}

/// Kafka's `validatePartitionReplicationFactorUnchanged`: with KIP-860's
/// `allow_replication_factor_change` false, the target must be as long as the
/// replica set the partition is headed for.
fn validate_replication_factor_unchanged(
    pr: &PartitionRecord,
    target_len: usize,
) -> Result<(), RowError> {
    let current = if reassignment_in_progress(pr) {
        let mut heading: BTreeSet<NodeId> = pr
            .replicas
            .iter()
            .chain(&pr.adding_replicas)
            .copied()
            .collect();
        for replica in &pr.removing_replicas {
            heading.remove(replica);
        }
        heading.len()
    } else {
        pr.replicas.len()
    };
    if current == target_len {
        Ok(())
    } else {
        Err(RowError {
            code: INVALID_REPLICATION_FACTOR,
            message: format!("The replication factor is changed from {current} to {target_len}"),
        })
    }
}

fn reassignment_in_progress(pr: &PartitionRecord) -> bool {
    !pr.adding_replicas.is_empty() || !pr.removing_replicas.is_empty()
}

fn cancel_path(
    pr: &PartitionRecord,
    approved: bool,
    unclean_enabled: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    // KFC-9: the two-person rule is an authority gate, so it answers before any
    // question about the partition's own state. Reading the reassignment first
    // would make "does this need an approval" depend on state that a
    // concurrent reassignment can change between the check and the append.
    if !approved {
        return Err(RowError {
            code: POLICY_VIOLATION,
            message: CANCEL_NEEDS_APPROVAL.into(),
        });
    }
    if !reassignment_in_progress(pr) {
        return Err(RowError {
            code: NO_REASSIGNMENT_IN_PROGRESS,
            message: NO_REASSIGNMENT_MESSAGE.into(),
        });
    }
    // `PartitionReassignmentRevert`: drop every adding replica from the
    // replica list and from the ISR, and keep the removing ones.
    let replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let mut isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    if isr.is_empty() {
        // Every ISR member was an adding replica. The revert puts the first
        // remaining replica in the ISR, which is an unclean election, so the
        // topic has to allow one.
        let Some(&first) = replicas.first() else {
            return Err(RowError {
                code: INVALID_REPLICA_ASSIGNMENT,
                message: "Invalid replica assignment: addingReplicas contains all replicas.".into(),
            });
        };
        if !unclean_enabled {
            return Err(RowError {
                code: INVALID_REPLICA_ASSIGNMENT,
                message: format!(
                    "Unable to revert partition assignment for {}:{} because it would require \
                     an unclean leader election.",
                    pr.topic, pr.partition
                ),
            });
        }
        isr.push(first);
    }
    let change = PartitionChange {
        replicas,
        isr,
        adding: vec![],
        removing: vec![],
    };
    build(pr, change, PlanMode {
        cancel: true,
        approved,
        in_progress: true,
    })
}

fn start_path(
    pr: &PartitionRecord,
    target: &[NodeId],
) -> Result<Option<PartitionRecord>, RowError> {
    // `PartitionReassignmentReplicas`: `adding` and `removing` are sorted
    // differences against the full current replica list, and the replica list
    // is the target, in the operator's order, followed by the removing ones.
    let mut adding = BTreeSet::new();
    let mut removing = BTreeSet::new();
    for &node in pr.replicas.iter().chain(target) {
        let membership = krabka_verified::reassignment_set_membership(
            pr.replicas.contains(&node),
            target.contains(&node),
        );
        if membership.adding {
            adding.insert(node);
        }
        if membership.removing {
            removing.insert(node);
        }
    }
    let mut replicas = target.to_vec();
    replicas.extend(removing.iter().copied());
    // `changePartitionReassignment` sets the adding and removing lists only
    // when they are non-empty, so an empty difference keeps the partition's
    // current one.
    let change = PartitionChange {
        replicas,
        isr: pr.isr.clone(),
        adding: if adding.is_empty() {
            pr.adding_replicas.clone()
        } else {
            adding.into_iter().collect()
        },
        removing: if removing.is_empty() {
            pr.removing_replicas.clone()
        } else {
            removing.into_iter().collect()
        },
    }
    .complete_if_ready();
    build(pr, change, PlanMode {
        cancel: false,
        approved: false,
        in_progress: false,
    })
}

/// The replica state a row asks for, before the leader is chosen.
struct PartitionChange {
    replicas: Vec<NodeId>,
    isr: Vec<NodeId>,
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
}

impl PartitionChange {
    /// Complete a reassignment in the same record when
    /// [`maybe_complete_reassignment`] allows it: the background completion
    /// task applies the same predicate.
    fn complete_if_ready(self) -> Self {
        match maybe_complete_reassignment(&self.replicas, &self.isr, &self.adding, &self.removing) {
            Some(CompletedReassignment { replicas, isr }) => Self {
                replicas,
                isr,
                adding: vec![],
                removing: vec![],
            },
            None => self,
        }
    }
}

/// Kafka's `PartitionChangeBuilder.build` over a planned change: keep the
/// leader when it stays in the ISR or elect the first replica that is in it,
/// bump the leader epoch when the leader changes or a replica leaves, and plan
/// nothing when the partition would not change.
///
/// `mode` is the cancel triple [`krabka_verified::reassignment_plan_admission`]
/// takes: whether this is a cancel, whether it is approved, and whether a
/// reassignment is in progress.
fn build(
    pr: &PartitionRecord,
    change: PartitionChange,
    mode: PlanMode,
) -> Result<Option<PartitionRecord>, RowError> {
    let leader = if change.isr.contains(&pr.leader) {
        pr.leader
    } else {
        let Some(&leader) = change.replicas.iter().find(|n| change.isr.contains(n)) else {
            return Err(RowError {
                code: ELIGIBLE_LEADERS_NOT_AVAILABLE,
                message: "no eligible leader for the reassignment".into(),
            });
        };
        leader
    };
    if leader == pr.leader
        && change.replicas == pr.replicas
        && change.isr == pr.isr
        && change.adding == pr.adding_replicas
        && change.removing == pr.removing_replicas
    {
        return Ok(None);
    }
    // `triggerLeaderEpochBumpForReplicaReassignmentIfNeeded`: a replica that
    // leaves the list must see a new leader epoch.
    let bump_leader_epoch =
        leader != pr.leader || !pr.replicas.iter().all(|n| change.replicas.contains(n));
    let (partition_epoch, leader_epoch) =
        next_partition_change(pr.partition_epoch, pr.leader_epoch, bump_leader_epoch).ok_or(
            RowError {
                code: INVALID_REQUEST,
                message: "partition metadata epoch is exhausted".into(),
            },
        )?;
    if !krabka_verified::reassignment_plan_admission(
        (mode.cancel, mode.approved, mode.in_progress),
        (true, true),
        (true, true),
    ) {
        return Err(RowError {
            code: INVALID_REQUEST,
            message: "reassignment admission failed".into(),
        });
    }
    let directories = remap_directories(&pr.replicas, &pr.directories, &change.replicas);
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: change.replicas,
        isr: change.isr,
        leader_epoch,
        adding_replicas: change.adding,
        removing_replicas: change.removing,
        directories,
        partition_epoch,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE,
            handlers::alter_partition_reassignments::test_support::{
                ReplicaBrokerId, ReplicationFactorPolicy, img_with, pending_reassignment_image,
            },
            test_support::ReassignmentSetup,
        },
        assert2::{assert, check},
        krabka_metadata::{LeaderEpoch, MetadataRecord, TopicConfigRecord},
        uuid::Uuid,
    };

    /// The record the fixture partition `foo-0` becomes.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct PlanRecordSetup<'a> {
        #[default(NodeId(1))]
        leader: NodeId,
        #[default(&[NodeId(1), NodeId(2), NodeId(3)])]
        replicas: &'a [NodeId],
        #[default(&[NodeId(1), NodeId(2), NodeId(3)])]
        isr: &'a [NodeId],
        adding: &'a [NodeId],
        removing: &'a [NodeId],
        leader_epoch: LeaderEpoch,
    }

    #[bon::builder]
    fn record(
        #[builder(into, default = NodeId (1))] leader: NodeId,
        #[builder(default = & [NodeId (1) , NodeId (2) , NodeId (3)])] replicas: &'_ [NodeId],
        #[builder(default = & [NodeId (1) , NodeId (2) , NodeId (3)])] isr: &'_ [NodeId],
        #[builder(default)] adding: &'_ [NodeId],
        #[builder(default)] removing: &'_ [NodeId],
        #[builder(into, default)] leader_epoch: LeaderEpoch,
    ) -> PartitionRecord {
        let setup = PlanRecordSetup {
            leader,
            replicas,
            isr,
            adding,
            removing,
            leader_epoch,
        };
        let PlanRecordSetup {
            leader,
            replicas,
            isr,
            adding,
            removing,
            leader_epoch,
        } = setup;
        PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch,
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
            directories: vec![Uuid::nil(); replicas.len()],
            partition_epoch: 12,
        }
    }

    fn reverted_leader_record() -> PartitionRecord {
        record().isr(&[NodeId(1)]).leader_epoch(6).call()
    }

    const FOUR_REPLICAS: &[NodeId] = &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)];

    /// Independent completed-removal expectation shared by start and cancel rows.
    fn completed_two_replica_record() -> PartitionRecord {
        record()
            .replicas(&[NodeId(1), NodeId(2)])
            .isr(&[NodeId(1), NodeId(2)])
            .leader_epoch(6)
            .call()
    }

    #[derive(Clone, Copy)]
    enum UncleanElection {
        Forbidden,
        Allowed,
    }

    /// Named input assignment for one reassignment row.
    type State<'a> = ReassignmentSetup<'a>;

    /// The in-flight assignment whose leader disappears when its addition is cancelled.
    fn added_leader_assignment() -> ReassignmentSetup<'static> {
        ReassignmentSetup {
            replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            isr: vec![NodeId(1), NodeId(4)],
            adding: vec![NodeId(4)],
            removing: vec![NodeId(2), NodeId(3)],
            leader: NodeId(4),
            ..Default::default()
        }
    }

    /// The in-flight assignment whose ISR holds only the adding replica.
    fn adding_isr_assignment() -> ReassignmentSetup<'static> {
        ReassignmentSetup {
            isr: vec![NodeId(3)],
            adding: vec![NodeId(3)],
            leader: NodeId(3),
            ..Default::default()
        }
    }

    /// A start row: label, partition state, target, the KIP-860 flag, and the
    /// planned record.
    struct StartCase<'a> {
        label: &'a str,
        state: State<'a>,
        target: &'a [ReplicaBrokerId],
        policy: ReplicationFactorPolicy,
        expected: Option<PartitionRecord>,
    }

    /// A cancel row: label, partition state, the topic's
    /// `unclean.leader.election.enable`, and the planned outcome.
    struct CancelCase<'a> {
        label: &'a str,
        state: State<'a>,
        unclean: UncleanElection,
        expected: Result<Option<PartitionRecord>, RowError>,
    }

    /// A rejected row: label, topic, partition, target, the KIP-860 flag, and
    /// the row error.
    struct RejectCase<'a> {
        label: &'a str,
        topic: &'a str,
        partition: krabka_ids::PartitionIndex,
        target: Option<&'a [ReplicaBrokerId]>,
        allow_rf: ReplicationFactorPolicy,
        expected: RowError,
    }

    /// Kafka's replica arithmetic for a start: the target order, a reorder, a
    /// new target over an in-flight reassignment, and a completion in the same
    /// record when `maybeCompleteReassignment` allows it. The fixture leader
    /// epoch is 5 and the partition epoch 11.
    #[test]
    fn a_start_writes_kafkas_replica_list() {
        let cases: [StartCase<'_>; 11] = [
            StartCase {
                label: "a reorder writes the new order",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(3), ReplicaBrokerId(2), ReplicaBrokerId(1)],
                policy: ReplicationFactorPolicy::Maintain,
                expected: Some(
                    record()
                        .replicas(&[NodeId(3), NodeId(2), NodeId(1)])
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "the target comes first, then the removing replicas",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(3), ReplicaBrokerId(4), ReplicaBrokerId(1)],
                policy: ReplicationFactorPolicy::Maintain,
                expected: Some(
                    record()
                        .replicas(&[NodeId(3), NodeId(4), NodeId(1), NodeId(2)])
                        .adding(&[NodeId(4)])
                        .removing(&[NodeId(2)])
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "a new target over an in-flight reassignment keeps every replica",
                state: ReassignmentSetup {
                    replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
                    adding: vec![NodeId(4)],
                    removing: vec![NodeId(2), NodeId(3)],
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(5), ReplicaBrokerId(6)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(
                    record()
                        .replicas(&[
                            NodeId(5),
                            NodeId(6),
                            NodeId(1),
                            NodeId(2),
                            NodeId(3),
                            NodeId(4),
                        ])
                        .adding(&[NodeId(5), NodeId(6)])
                        .removing(FOUR_REPLICAS)
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "a pure removal completes at once and bumps the leader epoch",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(1), ReplicaBrokerId(2)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(completed_two_replica_record()),
            },
            StartCase {
                label: "the adding and removing lists are sorted",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(5), ReplicaBrokerId(4), ReplicaBrokerId(1)],
                policy: ReplicationFactorPolicy::Maintain,
                expected: Some(
                    record()
                        .replicas(&[NodeId(5), NodeId(4), NodeId(1), NodeId(2), NodeId(3)])
                        .adding(&[NodeId(4), NodeId(5)])
                        .removing(&[NodeId(2), NodeId(3)])
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "a leader only in the old removing set stays in the union",
                state: ReassignmentSetup {
                    replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
                    adding: vec![NodeId(4)],
                    removing: vec![NodeId(1), NodeId(2), NodeId(3)],
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(5)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(
                    record()
                        .replicas(&[NodeId(5), NodeId(1), NodeId(2), NodeId(3), NodeId(4)])
                        .adding(&[NodeId(5)])
                        .removing(FOUR_REPLICAS)
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "an addition waits for the new replica to join the ISR",
                state: ReassignmentSetup {
                    replicas: vec![NodeId(1), NodeId(2)],
                    isr: vec![NodeId(1), NodeId(2)],
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(1), ReplicaBrokerId(2), ReplicaBrokerId(3)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(
                    record()
                        .isr(&[NodeId(1), NodeId(2)])
                        .adding(&[NodeId(3)])
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "a completion that removes the leader elects the first target in the ISR",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(2), ReplicaBrokerId(3)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(
                    record()
                        .leader(2)
                        .replicas(&[NodeId(2), NodeId(3)])
                        .isr(&[NodeId(2), NodeId(3)])
                        .leader_epoch(6)
                        .call(),
                ),
            },
            StartCase {
                label: "an empty difference keeps the in-flight adding list and can complete",
                state: ReassignmentSetup {
                    replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
                    isr: vec![NodeId(1), NodeId(3), NodeId(4)],
                    adding: vec![NodeId(4)],
                    removing: vec![NodeId(2)],
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(1), ReplicaBrokerId(3), ReplicaBrokerId(4)],
                policy: ReplicationFactorPolicy::Maintain,
                expected: Some(
                    record()
                        .replicas(&[NodeId(1), NodeId(3), NodeId(4)])
                        .isr(&[NodeId(1), NodeId(3), NodeId(4)])
                        .leader_epoch(6)
                        .call(),
                ),
            },
            StartCase {
                label: "a decrease waits while the ISR lacks a target replica",
                state: ReassignmentSetup {
                    isr: vec![NodeId(1), NodeId(2)],
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(1), ReplicaBrokerId(3)],
                policy: ReplicationFactorPolicy::PermitChange,
                expected: Some(
                    record()
                        .replicas(&[NodeId(1), NodeId(3), NodeId(2)])
                        .isr(&[NodeId(1), NodeId(2)])
                        .removing(&[NodeId(2)])
                        .leader_epoch(5)
                        .call(),
                ),
            },
            StartCase {
                label: "the current order is already the target",
                state: ReassignmentSetup {
                    ..Default::default()
                },
                target: &[ReplicaBrokerId(1), ReplicaBrokerId(2), ReplicaBrokerId(3)],
                policy: ReplicationFactorPolicy::Maintain,
                expected: None,
            },
        ];
        for StartCase {
            label,
            state: assignment,
            target,
            policy: allow_rf,
            expected,
        } in cases
        {
            let image = img_with().assignment(assignment).partition_epoch(11).call();
            let planned = process_one_partition(
                &image,
                "foo",
                0,
                Some(&target.iter().map(|id| id.0).collect::<Vec<_>>()),
                matches!(allow_rf, ReplicationFactorPolicy::PermitChange),
                true,
            );
            check!(planned == Ok(expected), "case {label}");
        }
    }

    /// Kafka's `PartitionReassignmentRevert`: a cancel drops the adding
    /// replicas, keeps the removing ones, and needs the topic to allow an
    /// unclean election when the ISR held only adding replicas.
    #[test]
    fn a_cancel_reverts_like_kafka() {
        let unclean_message = "Unable to revert partition assignment for foo:0 because it would \
                               require an unclean leader election.";
        let cases: [CancelCase<'_>; 6] = [
            CancelCase {
                label: "a leader in the adding set moves to the reverted ISR",
                state: added_leader_assignment(),
                unclean: UncleanElection::Forbidden,
                expected: Ok(Some(reverted_leader_record())),
            },
            CancelCase {
                label: "a leader outside the reverted ISR is replaced",
                state: ReassignmentSetup {
                    removing: vec![NodeId(3)],
                    leader: NodeId(2),
                    ..added_leader_assignment()
                },
                unclean: UncleanElection::Forbidden,
                expected: Ok(Some(reverted_leader_record())),
            },
            CancelCase {
                label: "only removing replicas keeps the leader epoch",
                state: ReassignmentSetup {
                    removing: vec![NodeId(3)],
                    ..Default::default()
                },
                unclean: UncleanElection::Forbidden,
                expected: Ok(Some(record().leader_epoch(5).call())),
            },
            CancelCase {
                label: "a dropped adding replica bumps the leader epoch",
                state: ReassignmentSetup {
                    adding: vec![NodeId(3)],
                    ..Default::default()
                },
                unclean: UncleanElection::Forbidden,
                expected: Ok(Some(completed_two_replica_record())),
            },
            CancelCase {
                label: "an ISR of adding replicas needs unclean election",
                state: adding_isr_assignment(),
                unclean: UncleanElection::Forbidden,
                expected: Err(RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message: unclean_message.into(),
                }),
            },
            CancelCase {
                label: "an ISR of adding replicas reverts uncleanly when the topic allows it",
                state: adding_isr_assignment(),
                unclean: UncleanElection::Allowed,
                expected: Ok(Some(
                    record()
                        .replicas(&[NodeId(1), NodeId(2)])
                        .isr(&[NodeId(1)])
                        .leader_epoch(6)
                        .call(),
                )),
            },
        ];
        for CancelCase {
            label,
            state: assignment,
            unclean,
            expected,
        } in cases
        {
            let mut image = img_with().assignment(assignment).partition_epoch(11).call();
            if matches!(unclean, UncleanElection::Allowed) {
                image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: "foo".into(),
                    overrides: [(
                        UNCLEAN_LEADER_ELECTION_ENABLE.to_string(),
                        "true".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                }));
            }
            let planned = process_one_partition(&image, "foo", 0, None, true, true);
            check!(planned == expected, "case {label}");
        }
    }

    /// Kafka's validation codes and messages for a row.
    #[test]
    fn a_rejected_row_answers_kafkas_code_and_message() {
        let cases: [RejectCase<'_>; 9] = [
            RejectCase {
                label: "an unknown topic",
                topic: "bar",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(1)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: UNKNOWN_TOPIC_OR_PARTITION,
                    message: "Unable to find a topic named bar.".into(),
                },
            },
            RejectCase {
                label: "an unknown partition",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(1),
                target: Some(&[ReplicaBrokerId(1)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: UNKNOWN_TOPIC_OR_PARTITION,
                    message: "Unable to find partition foo:1.".into(),
                },
            },
            RejectCase {
                label: "an empty target",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message: "The manual partition assignment includes an empty replica list."
                        .into(),
                },
            },
            RejectCase {
                label: "a duplicate replica",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(1), ReplicaBrokerId(1)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message:
                        "The manual partition assignment includes the broker 1 more than once."
                            .into(),
                },
            },
            RejectCase {
                label: "an unregistered broker",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(99)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message:
                        "The manual partition assignment includes broker 99, but no such broker is \
                     registered."
                            .into(),
                },
            },
            RejectCase {
                label: "a negative broker id",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(-1)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message:
                        "The manual partition assignment includes broker -1, but no such broker is \
                     registered."
                            .into(),
                },
            },
            RejectCase {
                label: "the lowest broker id decides between two faults",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(99), ReplicaBrokerId(1), ReplicaBrokerId(1)]),
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: INVALID_REPLICA_ASSIGNMENT,
                    message:
                        "The manual partition assignment includes the broker 1 more than once."
                            .into(),
                },
            },
            RejectCase {
                label: "a replication factor change without the flag",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: Some(&[ReplicaBrokerId(1), ReplicaBrokerId(2)]),
                allow_rf: ReplicationFactorPolicy::Maintain,
                expected: RowError {
                    code: INVALID_REPLICATION_FACTOR,
                    message: "The replication factor is changed from 3 to 2".into(),
                },
            },
            RejectCase {
                label: "a cancel with nothing in progress",
                topic: "foo",
                partition: krabka_ids::PartitionIndex(0),
                target: None,
                allow_rf: ReplicationFactorPolicy::PermitChange,
                expected: RowError {
                    code: NO_REASSIGNMENT_IN_PROGRESS,
                    message: NO_REASSIGNMENT_MESSAGE.into(),
                },
            },
        ];
        let image = img_with().call();
        for RejectCase {
            label,
            topic,
            partition,
            target,
            allow_rf,
            expected,
        } in cases
        {
            let planned = process_one_partition(
                &image,
                topic,
                partition.0,
                target
                    .map(|ids| ids.iter().map(|id| id.0).collect::<Vec<_>>())
                    .as_deref(),
                matches!(allow_rf, ReplicationFactorPolicy::PermitChange),
                true,
            );
            check!(planned == Err(expected), "case {label}");
        }
    }

    #[test]
    fn the_replication_factor_check_counts_the_set_the_partition_is_headed_for() {
        // replicas [1,2,3,4], adding [4], removing [2]: headed for [1,3,4].
        let image = img_with()
            .assignment(ReassignmentSetup {
                replicas: vec![
                    krabka_ids::NodeId(1),
                    krabka_ids::NodeId(2),
                    krabka_ids::NodeId(3),
                    krabka_ids::NodeId(4),
                ],
                isr: vec![
                    krabka_ids::NodeId(1),
                    krabka_ids::NodeId(3),
                    krabka_ids::NodeId(4),
                ],
                adding: vec![krabka_ids::NodeId(4)],
                removing: vec![krabka_ids::NodeId(2)],
                ..Default::default()
            })
            .call();

        let error = process_one_partition(&image, "foo", 0, Some(&[1, 3]), false, true)
            .expect_err("a two-replica target changes the factor");

        assert!(
            error
                == RowError {
                    code: INVALID_REPLICATION_FACTOR,
                    message: "The replication factor is changed from 3 to 2".to_string()
                }
        );
    }

    #[test]
    fn start_rejects_an_exhausted_partition_epoch() {
        let image = img_with().partition_epoch(i32::MAX).call();
        let error = process_one_partition(&image, "foo", 0, Some(&[1, 4]), true, true)
            .expect_err("exhausted epoch must fail closed");
        assert!(error.code == INVALID_REQUEST);
    }

    #[test]
    fn an_exhausted_leader_epoch_fails_every_leader_changing_row_without_mutation() {
        for (label, state, target) in [
            (
                "a completion that moves the leader",
                ReassignmentSetup {
                    ..Default::default()
                },
                Some(&[2, 3][..]),
            ),
            (
                "a cancel that moves the leader",
                added_leader_assignment(),
                None,
            ),
        ] {
            let leader = state.leader;
            let assignment = state;
            let mut image = img_with().assignment(assignment).call();
            let mut seeded = image.partition("foo", 0).expect("seeded partition").clone();
            seeded.leader_epoch = LeaderEpoch(i32::MAX);
            image.apply(&MetadataRecord::V1Partition(seeded));

            for _ in 0..2 {
                let error = process_one_partition(&image, "foo", 0, target, true, true)
                    .expect_err("exhausted leader epoch must fail closed");
                check!(error.code == INVALID_REQUEST, "case {label}");
                check!(
                    image.partition("foo", 0).unwrap().leader == leader,
                    "case {label}"
                );
            }
        }
    }

    // ── KFC-9: the break-glass gate over a cancel ───────────────────

    #[test]
    fn an_unapproved_cancel_is_refused_before_any_question_about_the_partition() {
        let cases = [
            (
                "a reassignment is in progress",
                pending_reassignment_image(),
            ),
            ("nothing to cancel", img_with().call()),
        ];
        for (label, img) in cases {
            let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
            check!(err.code == POLICY_VIOLATION, "case {label}");
            check!(err.message == CANCEL_NEEDS_APPROVAL, "case {label}");
        }
    }

    #[test]
    fn an_unknown_partition_still_answers_that_it_is_unknown() {
        // The gate never masks a request that names nothing.
        let img = MetadataImage::new(Uuid::nil());
        let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
        check!(err.code == UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_start_is_never_gated() {
        let img = img_with().call();
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 4]), true, false)
            .expect("a start needs no approval")
            .expect("Some");
        check!(res.adding_replicas == vec![NodeId(4)]);
    }
}
