//! The pure reassignment decision, with no I/O and no controller. Given a
//! partition record and the set of alive brokers it returns the next
//! `PartitionRecord`, which is either a leader handoff or a completion.
//!
//! The logic lives apart from the background task in [`super`] so that a unit
//! test and the `stateright` model checker can drive the policy on its own.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use krabka_verified::{
    FreezeMutationKind,
    reassignment::{ReassignmentAction, reassignment_action},
};

use crate::{
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
    heartbeat::controller_state::ControllerLivenessState,
};

/// Remaps a partition's `directories` vector onto a new `replicas` order. A
/// KIP-455 reassignment changes both the replica membership and the order.
///
/// `directories` runs index-parallel to `replicas`. A verbatim clone after the
/// replica set changed would misalign the slots and break KIP-112 offline-dir
/// failover. A surviving replica keeps its dir UUID. A newly added replica
/// gets `Uuid::nil()`, which means UNASSIGNED, until it reports through
/// `AssignReplicasToDirs`.
pub(crate) fn remap_directories(
    old_replicas: &[NodeId],
    old_directories: &[uuid::Uuid],
    new_replicas: &[NodeId],
) -> Vec<uuid::Uuid> {
    let old: std::collections::HashMap<NodeId, uuid::Uuid> = old_replicas
        .iter()
        .copied()
        .zip(old_directories.iter().copied())
        .collect();
    new_replicas
        .iter()
        .map(|n| old.get(n).copied().unwrap_or_else(uuid::Uuid::nil))
        .collect()
}

/// The replicas and ISR a reassignment completes to.
pub(crate) struct CompletedReassignment {
    pub(crate) replicas: Vec<NodeId>,
    pub(crate) isr: Vec<NodeId>,
}

/// Kafka's `PartitionReassignmentReplicas.maybeCompleteReassignment`: the
/// replicas and ISR a reassignment completes to, or `None` while it must wait.
///
/// It waits when the ISR would keep no member outside `removing`, when no
/// replica would remain, and until every adding replica is in the ISR, as new
/// brokers may be unhealthy. It also waits when the replication factor
/// shrinks (`adding` is smaller than `removing`) until every target replica
/// is in the ISR: completing earlier would shrink the ISR needlessly, so that
/// `[0,1,2,3,4] -> [2,3,4,5]` with only 0, 1 and 5 in sync would leave an ISR
/// of `[5]`.
pub(crate) fn maybe_complete_reassignment(
    replicas: &[NodeId],
    isr: &[NodeId],
    adding: &[NodeId],
    removing: &[NodeId],
) -> Option<CompletedReassignment> {
    if adding.is_empty() && removing.is_empty() {
        return None;
    }
    let keep = |nodes: &[NodeId]| -> Vec<NodeId> {
        nodes
            .iter()
            .filter(|n| !removing.contains(n))
            .copied()
            .collect()
    };
    let (replicas, isr) = (keep(replicas), keep(isr));
    if isr.is_empty() || replicas.is_empty() {
        return None;
    }
    if !adding.iter().all(|n| isr.contains(n)) {
        return None;
    }
    if adding.len() < removing.len() && !replicas.iter().all(|n| isr.contains(n)) {
        return None;
    }
    Some(CompletedReassignment { replicas, isr })
}

/// The pure per-partition reassignment decision. From a partition's current
/// record and the alive set, it returns the next `PartitionRecord`, which is
/// either a leader handoff or a completion. It returns `None` to wait.
///
/// Neither step runs before [`maybe_complete_reassignment`] holds, as in
/// Kafka, where the same change that elects the new leader completes the
/// reassignment.
///
/// The function does no I/O. It is separate from
/// `compute_reassignment_progress` so that a unit test and a model checker can
/// drive the policy on its own.
pub(crate) fn reassign_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
) -> Option<PartitionRecord> {
    let target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|r| !pr.removing_replicas.contains(r))
        .copied()
        .collect();
    let eligible_handoffs: Vec<bool> = target
        .iter()
        .map(|n| pr.isr.contains(n) && alive.contains(n))
        .collect();
    let completed = maybe_complete_reassignment(
        &pr.replicas,
        &pr.isr,
        &pr.adding_replicas,
        &pr.removing_replicas,
    );
    let action = reassignment_action(
        completed.is_some(),
        pr.removing_replicas.contains(&pr.leader),
        &eligible_handoffs,
    );
    if let ReassignmentAction::Handoff(index) = action {
        // The verified index maps to target ∩ ISR ∩ alive.
        let new_leader = target[index];
        let leader_epoch = crate::metadata_epoch::next_leader(pr.leader_epoch)?;
        let partition_epoch = crate::metadata_epoch::next_i32(pr.partition_epoch)?;
        return Some(PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: new_leader,
            leader_epoch,
            replicas: pr.replicas.clone(),
            isr: pr.isr.clone(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch,
        });
    }
    if action != ReassignmentAction::Complete {
        return None;
    }
    // Completion phase: switch to the target replica set.
    let CompletedReassignment {
        replicas: target,
        isr: new_isr,
    } = completed?;
    let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
    let partition_epoch = crate::metadata_epoch::next_i32(pr.partition_epoch)?;
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        leader_epoch: pr.leader_epoch, // unchanged: leader stays, only replica set changes
        replicas: target,
        isr: new_isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch,
    })
}

/// Pure logic. It scans every in-flight reassignment, and produces a
/// completion record or a leader-handoff record for each one that is ready to
/// advance, followed by the KIP-966 eligible-leader state those records imply.
///
/// A completion narrows the ISR to the target replicas and drops the rest of
/// the replica set, so it is an ISR shrink and a replica-set change at once.
/// [`ElrPublisher`](crate::elr::ElrPublisher) rides the same batch, the way it
/// does on every other path that moves an ISR: without it a replica the
/// partition no longer has could stay in the ELR that
/// `DescribeTopicPartitions` reports.
pub(crate) async fn compute_reassignment_progress(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> Vec<MetadataRecord> {
    let mut updates = Vec::new();
    // Snapshot the alive set once (single lock) instead of taking the
    // liveness lock per target replica in the leader-handoff branch.
    let alive = liveness.alive_node_ids().await;
    for pr in image.reassignments_in_flight() {
        if matches!(
            resolve_freeze_mutation(
                image,
                &pr.topic,
                true,
                FreezeMutationKind::ReassignmentCompletion,
            ),
            FreezeMutationResolution::Frozen(_)
        ) {
            continue;
        }
        if let Some(next) = reassign_one(pr, &alive) {
            updates.push(MetadataRecord::V1Partition(next));
        }
    }
    crate::elr::ElrPublisher::new(image).extend(&mut updates);
    updates
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicFreezeRecord, TopicRecord,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        reassignment::test_support::{first_partition, img, liveness},
        test_support::{FreezeSetup, ReassignmentSetup},
    };

    #[test]
    fn remap_directories_preserves_slot_alignment_on_replica_removal() {
        let da = uuid::Uuid::from_u128(0xA);
        let db = uuid::Uuid::from_u128(0xB);
        let dc = uuid::Uuid::from_u128(0xC);
        // replicas [1,2,3] dirs [dA,dB,dC]; reassignment removes broker 2.
        let new = remap_directories(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[da, db, dc],
            &[NodeId(1), NodeId(3)],
        );
        // broker 1 keeps dA at slot 0; broker 3 keeps dC at slot 1 (NOT dB).
        assert!(new == vec![da, dc]);
    }

    #[test]
    fn remap_directories_assigns_nil_to_new_replica() {
        let da = uuid::Uuid::from_u128(0xA);
        // replicas [1] dirs [dA]; add broker 2 (no dir yet).
        let new = remap_directories(&[NodeId(1)], &[da], &[NodeId(1), NodeId(2)]);
        assert!(new == vec![da, uuid::Uuid::nil()]);
    }

    /// One row per branch of Kafka's `maybeCompleteReassignment`. The
    /// replicas are the union a reassignment in flight carries: the target
    /// followed by the removing replicas.
    #[test]
    fn maybe_complete_reassignment_follows_kafka() {
        struct Case {
            name: &'static str,
            replicas: &'static [u64],
            isr: &'static [u64],
            adding: &'static [u64],
            removing: &'static [u64],
            /// The replicas and ISR the reassignment completes to.
            expected: Option<(&'static [u64], &'static [u64])>,
        }
        let case = |name, replicas, isr, adding, removing, expected| Case {
            name,
            replicas,
            isr,
            adding,
            removing,
            expected,
        };
        let cases = [
            case("no reassignment", &[1, 2, 3], &[1, 2, 3], &[], &[], None),
            case(
                "the additions are in the ISR",
                &[1, 2, 3],
                &[1, 2, 3],
                &[3],
                &[2],
                Some((&[1, 3], &[1, 3])),
            ),
            case(
                "an addition is not in the ISR",
                &[1, 2, 3],
                &[1, 2],
                &[3],
                &[2],
                None,
            ),
            case(
                "the ISR would keep no member",
                &[1, 2, 3],
                &[3],
                &[],
                &[3],
                None,
            ),
            case(
                "a decrease with a target replica outside the ISR",
                &[1, 2, 3],
                &[1, 3],
                &[],
                &[3],
                None,
            ),
            case(
                "a decrease with every target replica in the ISR",
                &[1, 2, 3],
                &[1, 2, 3],
                &[],
                &[3],
                Some((&[1, 2], &[1, 2])),
            ),
            case(
                // [0,1,2,3,4] -> [2,3,4,5] with only 0, 1 and 5 in sync would
                // complete to an ISR of [5].
                "Kafka's example of a decrease that would shrink the ISR",
                &[2, 3, 4, 5, 0, 1],
                &[0, 1, 5],
                &[5],
                &[0, 1],
                None,
            ),
            case(
                "the same factor does not wait for the rest of the target",
                &[2, 3, 4, 1],
                &[1, 2, 3],
                &[3],
                &[1],
                Some((&[2, 3, 4], &[2, 3])),
            ),
            case(
                "an increase does not wait for the rest of the target",
                &[1, 2, 3, 4],
                &[1, 2, 3],
                &[3],
                &[],
                Some((&[1, 2, 3, 4], &[1, 2, 3])),
            ),
        ];
        for Case {
            name,
            replicas,
            isr,
            adding,
            removing,
            expected,
        } in cases
        {
            let ids = |nodes: &[u64]| nodes.iter().copied().map(NodeId).collect::<Vec<_>>();
            let got = maybe_complete_reassignment(
                &ids(replicas),
                &ids(isr),
                &ids(adding),
                &ids(removing),
            )
            .map(|done| (done.replicas, done.isr));
            let expected = expected.map(|(replicas, isr)| (ids(replicas), ids(isr)));
            assert!(got == expected, "{name}");
        }
    }

    /// A replication factor decrease completes only once every target replica
    /// is in the ISR, as in Kafka, where the change that completes it is the
    /// change that elects the new leader.
    #[tokio::test]
    async fn a_replication_factor_decrease_waits_for_the_target_replicas() {
        // Every case is one tick over an image with all brokers alive.
        struct Case {
            name: &'static str,
            assignment: ReassignmentSetup<'static>,
            /// The leader and ISR of the record the tick submits, if any.
            expected: Option<(u64, &'static [u64])>,
        }
        let cases = [
            Case {
                name: "broker 2 is a target replica outside the ISR",
                assignment: ReassignmentSetup {
                    isr: vec![NodeId(1), NodeId(3)],
                    removing: vec![NodeId(3)],
                    ..Default::default()
                },
                expected: None,
            },
            Case {
                name: "every target replica is in the ISR",
                assignment: ReassignmentSetup {
                    removing: vec![NodeId(3)],
                    ..Default::default()
                },
                expected: Some((1, &[1, 2])),
            },
            Case {
                name: "the leader is removed and broker 3 is not in the ISR",
                assignment: ReassignmentSetup {
                    replicas: vec![NodeId(3), NodeId(4), NodeId(1), NodeId(2)],
                    isr: vec![NodeId(1), NodeId(2), NodeId(4)],
                    adding: vec![NodeId(4)],
                    removing: vec![NodeId(1), NodeId(2)],
                    ..Default::default()
                },
                expected: None,
            },
            Case {
                name: "the leader is removed and every target replica is in the ISR",
                assignment: ReassignmentSetup {
                    replicas: vec![NodeId(3), NodeId(4), NodeId(1), NodeId(2)],
                    isr: vec![NodeId(1), NodeId(3), NodeId(4)],
                    adding: vec![NodeId(4)],
                    removing: vec![NodeId(1), NodeId(2)],
                    ..Default::default()
                },
                // The handoff goes first; completion follows on the next tick.
                expected: Some((3, &[1, 3, 4])),
            },
        ];
        for Case {
            name: case,
            assignment,
            expected,
        } in cases
        {
            let image = img(assignment);
            let l = liveness(&[1, 2, 3, 4]).await;

            let updates = compute_reassignment_progress(&image, &l).await;

            let got = match updates.as_slice() {
                [] => None,
                [update] => {
                    let pr = first_partition(update);
                    let isr: Vec<u64> = pr.isr.iter().map(|n| n.0).collect();
                    Some((pr.leader.0, isr))
                }
                more => panic!("{case}: {more:?}"),
            };
            let expected = expected.map(|(leader, isr)| (leader, isr.to_vec()));
            assert!(got == expected, "{case}: {updates:?}");
        }
    }

    async fn completed_target(image: &MetadataImage) -> PartitionRecord {
        let alive = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(image, &alive).await;
        assert!(updates.len() == 1);
        let partition = first_partition(&updates[0]);
        check!(partition.replicas == vec![NodeId(1), NodeId(3)]);
        partition.clone()
    }

    #[tokio::test]
    async fn completion_preserves_directory_slot_alignment() {
        // replicas=[1,2,3], adding=[3], removing=[2], all in ISR.
        // directories=[dA, dB, dC] — slot 0→broker1, 1→broker2, 2→broker3.
        // After completion target=[1,3]; expected dirs=[dA, dC].
        let da = Uuid::from_u128(0xA);
        let db = Uuid::from_u128(0xB);
        let dc = Uuid::from_u128(0xC);
        let image = img(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            directories: &[da, db, dc],
            ..Default::default()
        });
        let pr = completed_target(&image).await;
        // Slot 0 → broker 1 → dA; slot 1 → broker 3 → dC (NOT dB).
        check!(pr.directories == vec![da, dc]);
        check!(pr.partition_epoch == 1);
    }

    #[tokio::test]
    async fn complete_when_adding_in_isr_writes_target() {
        let img = img(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            ..Default::default()
        });
        let pr = completed_target(&img).await;
        // leader and leader_epoch are unchanged (leader didn't change).
        check!(pr.adding_replicas == Vec::<NodeId>::new());
        check!(pr.removing_replicas == Vec::<NodeId>::new());
        check!(pr.isr == vec![NodeId(1), NodeId(3)]);
        check!(pr.leader == 1);
        check!(pr.leader_epoch == krabka_metadata::LeaderEpoch(5));
        check!(pr.partition_epoch == 1);
    }

    /// KIP-966: a completion is an ISR shrink and a replica-set change at
    /// once, so the eligible-leader state moves with it in the same batch.
    /// Without the publisher on this path a replica the partition no longer
    /// has would stay in the ELR `DescribeTopicPartitions` reports.
    #[tokio::test]
    async fn a_completion_republishes_the_eligible_leader_state() {
        for (label, isr, adding, published, want) in [
            (
                "a completion that reaches min ISR tombstones the key",
                &[1u64, 2u64][..],
                &[][..],
                "0:3:",
                None,
            ),
            // The partition can no longer elect broker 3, so it leaves the ELR.
            // Kafka's last-known set holds the last leader of a partition
            // without one, so it does not go there: this partition has a
            // leader.
            (
                // Broker 1 replaces broker 3, so the replication factor holds
                // and the completion does not wait for broker 2.
                "a dropped replica leaves the ELR",
                &[1u64][..],
                &[1u64][..],
                "0:3:",
                None,
            ),
        ] {
            // replicas=[1,2,3], removing=[3]: the completion drops broker 3
            // from the replica set, and the published ELR still names it.
            let mut image = std::sync::Arc::try_unwrap(img(ReassignmentSetup {
                isr: isr.iter().copied().map(krabka_ids::NodeId).collect(),
                adding: adding.iter().copied().map(krabka_ids::NodeId).collect(),
                removing: vec![krabka_ids::NodeId(3)],
                ..Default::default()
            }))
            .expect("the fixture holds the only reference");
            crate::test_support::finalize_elr_version(&mut image);
            image.apply(&MetadataRecord::V1TopicConfig(
                krabka_metadata::TopicConfigRecord {
                    topic: "foo".into(),
                    overrides: [(
                        crate::config_keys::MIN_INSYNC_REPLICAS.to_string(),
                        "3".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                },
            ));
            for record in crate::elr::state::test_records("foo", published) {
                image.apply(&record);
            }
            let l = liveness(&[1, 2, 3]).await;

            let updates = compute_reassignment_progress(&image, &l).await;

            let expected = want.map_or_else(
                || {
                    MetadataRecord::V1PartitionElr(krabka_metadata::PartitionElrRecord {
                        topic: "foo".into(),
                        partition: 0,
                        eligible_leader_replicas: vec![],
                        last_known_elr: vec![],
                    })
                },
                |value| crate::elr::state::test_records("foo", value).remove(0),
            );
            let MetadataRecord::V1PartitionElr(expected) = expected else {
                unreachable!()
            };
            let [MetadataRecord::V1PartitionUpdate(update)] = updates.as_slice() else {
                panic!("{label}: {updates:?}")
            };
            check!(
                update.partition.replicas == [NodeId(1), NodeId(2)],
                "{label}"
            );
            check!(update.partition.removing_replicas.is_empty(), "{label}");
            check!(
                update.eligible_leader_replicas.as_ref()
                    == Some(&expected.eligible_leader_replicas),
                "{label}"
            );
            check!(
                update.last_known_elr.as_ref() == Some(&expected.last_known_elr),
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn freeze_allows_completion_of_an_already_accepted_reassignment() {
        let mut image = img(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            ..Default::default()
        });
        Arc::make_mut(&mut image).apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            set_at_ms: 10,
            ..crate::test_support::topic_freeze_record(FreezeSetup {
                scope: "foo",
                ..Default::default()
            })
        }));
        let l = liveness(&[1, 2, 3]).await;

        let completed = compute_reassignment_progress(&image, &l).await;
        assert!(completed.len() == 1);
        check!(first_partition(&completed[0]).replicas == vec![NodeId(1), NodeId(3)]);
    }

    #[tokio::test]
    async fn no_update_emitted_when_waiting_idle_or_no_alive_target() {
        // (case, assignment, alive) — every case
        // should wait / stay idle: compute_reassignment_progress emits nothing.
        let cases = [
            // Adding replica 3 not yet in ISR → wait.
            (
                "adding_not_in_isr",
                ReassignmentSetup {
                    isr: vec![NodeId(1), NodeId(2)],
                    adding: vec![NodeId(3)],
                    removing: vec![NodeId(2)],
                    ..Default::default()
                },
                vec![1, 2, 3],
            ),
            // leader=2, removing=[2]; only target replicas {1,3} in isr but
            // none alive (only 2 alive) — wait.
            (
                "leader_handoff_no_alive_target_replica",
                ReassignmentSetup {
                    adding: vec![NodeId(3)],
                    removing: vec![NodeId(2)],
                    leader: NodeId(2),
                    ..Default::default()
                },
                vec![2],
            ),
            // No reassignment in flight → idle partition emits no update.
            (
                "idle_partition",
                ReassignmentSetup {
                    ..Default::default()
                },
                vec![1, 2, 3],
            ),
        ];
        for (case, assignment, alive) in cases {
            let img = img(assignment);
            let l = liveness(&alive).await;
            let updates = compute_reassignment_progress(&img, &l).await;
            assert!(
                updates.is_empty(),
                "case {case}: should wait; got {updates:?}"
            );
        }
    }

    #[tokio::test]
    async fn leader_handoff_when_leader_in_removing() {
        // leader=2, removing=[2]; new leader must come from target ∩ isr = {1,3} ∩ {1,2,3} = {1,3}.
        let img = img(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            leader: krabka_ids::NodeId(2),
            ..Default::default()
        });
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(
            pr.leader == 1 || pr.leader == 3,
            "leader was {}",
            pr.leader.0
        );
        // leader_epoch bumped; replica set unchanged — completion happens
        // next tick.
        check!(pr.leader_epoch == krabka_metadata::LeaderEpoch(6));
        check!(pr.partition_epoch == 1);
        check!(pr.adding_replicas == vec![NodeId(3)]);
        check!(pr.removing_replicas == vec![NodeId(2)]);
    }

    #[test]
    fn exhausted_epochs_block_reassignment_transitions() {
        let image = img(ReassignmentSetup {
            adding: vec![krabka_ids::NodeId(3)],
            removing: vec![krabka_ids::NodeId(2)],
            leader: krabka_ids::NodeId(2),
            ..Default::default()
        });
        let mut record = image.partition("foo", 0).expect("seeded partition").clone();
        let alive = std::collections::HashSet::from([NodeId(1), NodeId(2), NodeId(3)]);

        record.partition_epoch = i32::MAX;
        assert!(reassign_one(&record, &alive).is_none());

        record.partition_epoch = 0;
        record.leader_epoch = krabka_metadata::LeaderEpoch(i32::MAX);
        assert!(reassign_one(&record, &alive).is_none());
    }

    #[tokio::test]
    async fn multiple_partitions_handled_independently() {
        let mut img_inner = MetadataImage::new(Uuid::nil());
        for n in 1..=6u64 {
            img_inner.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    host: String::new(),
                    port: 0,
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(n))
                },
            ));
        }
        for name in ["foo", "bar"] {
            img_inner.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 3,
            }));
            img_inner.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
                isr: vec![NodeId(1), NodeId(2), NodeId(3)],
                leader_epoch: krabka_metadata::LeaderEpoch(5),
                adding_replicas: vec![NodeId(3)],
                removing_replicas: vec![NodeId(2)],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        let img = Arc::new(img_inner);
        let l = liveness(&[1, 2, 3]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 2);
    }

    #[tokio::test]
    async fn target_includes_only_replicas_minus_removing() {
        // adding=[4,5], removing=[1,2], replicas=[1,2,3,4,5].
        // target = [3,4,5]. isr ⊇ adding required; isr=[1,2,3,4,5].
        let img = img(ReassignmentSetup {
            replicas: vec![
                krabka_ids::NodeId(1),
                krabka_ids::NodeId(2),
                krabka_ids::NodeId(3),
                krabka_ids::NodeId(4),
                krabka_ids::NodeId(5),
            ],
            isr: vec![
                krabka_ids::NodeId(1),
                krabka_ids::NodeId(2),
                krabka_ids::NodeId(3),
                krabka_ids::NodeId(4),
                krabka_ids::NodeId(5),
            ],
            adding: vec![krabka_ids::NodeId(4), krabka_ids::NodeId(5)],
            removing: vec![krabka_ids::NodeId(1), krabka_ids::NodeId(2)],
            leader: krabka_ids::NodeId(3),
            ..Default::default()
        });
        let l = liveness(&[1, 2, 3, 4, 5]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.replicas == vec![NodeId(3), NodeId(4), NodeId(5)]);
        assert!(pr.isr == vec![NodeId(3), NodeId(4), NodeId(5)]);
    }

    /// A start writes the target replicas in the operator's order followed by
    /// the removing ones, as Kafka's `PartitionReassignmentReplicas` does, so
    /// the completion lands on the target in that order and `replicas[0]`, the
    /// preferred leader, is the one the operator named first.
    #[test]
    fn completion_keeps_the_operators_target_order() {
        // Target [3,4,1] over current [1,2,3]: the start wrote [3,4,1,2].
        let image = img(ReassignmentSetup {
            replicas: vec![
                krabka_ids::NodeId(3),
                krabka_ids::NodeId(4),
                krabka_ids::NodeId(1),
                krabka_ids::NodeId(2),
            ],
            ..ReassignmentSetup::replacing_replica(NodeId(2), NodeId(4))
        });
        let record = image.partition("foo", 0).expect("seeded partition");
        let alive = std::collections::HashSet::from([NodeId(1), NodeId(2), NodeId(3), NodeId(4)]);

        let completed = reassign_one(record, &alive);

        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(3), NodeId(4), NodeId(1)],
            isr: vec![NodeId(1), NodeId(3), NodeId(4)],
            leader_epoch: krabka_metadata::LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 1,
        };
        assert!(completed == Some(expected));
    }

    #[tokio::test]
    async fn isr_intersection_when_some_targets_not_in_isr() {
        // adding=[4], removing=[2]; isr=[1,2,3,4]; target=[1,3,4].
        // new_isr = isr ∩ target = [1,3,4].
        let img = img(ReassignmentSetup {
            ..ReassignmentSetup::replacing_replica(NodeId(2), NodeId(4))
        });
        let l = liveness(&[1, 2, 3, 4]).await;
        let updates = compute_reassignment_progress(&img, &l).await;
        assert!(updates.len() == 1);
        let pr = first_partition(&updates[0]);
        assert!(pr.isr == vec![NodeId(1), NodeId(3), NodeId(4)]);
    }
}
