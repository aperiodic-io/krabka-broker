//! Validation and application of one partition's ISR proposal.
//!
//! Each partition row of an `AlterPartition` request is decided on its own,
//! in the order of Kafka's
//! `ReplicationControlManager.validateAlterPartitionData`: epochs the
//! controller has not seen, leader-epoch fencing, the requester being the
//! leader, partition-epoch staleness, the shape of the proposed ISR, the
//! leader recovery state, and the KIP-903 eligibility of every proposed
//! member. The first failure decides the row's error code. The row either
//! contributes one `PartitionRecord` change or an error response, so the whole
//! per-row decision belongs in one module.

#[cfg(test)]
use crate::test_support::WireBrokerId;
#[cfg(test)]
use krabka_metadata::LeaderEpoch;
use {
    crate::{codes, metadata_epoch::next_i32},
    krabka_metadata::{
        LeaderRecoveryState, MetadataRecord, NodeId, PartitionRecord, PartitionRecoveryRecord,
    },
    krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            alter_partition_request::PartitionData as ReqPartitionData,
            alter_partition_response::PartitionData as RespPartitionData,
        },
    },
    krabka_verified::isr::{AlterPartitionFacts, IsrAdmission, ProposedIsr, isr_admission},
    std::collections::HashSet,
};

/// A broker the request proposes for the ISR, with the broker epoch it claims.
struct ProposedReplica {
    broker_id: i32,
    broker_epoch: i64,
}

/// Validates and applies the ISR proposal of one partition of the known topic
/// `topic_name`, sent by broker `requester`. It returns the per-partition
/// response data, and on success it appends to `changes`.
///
/// The request row carries the v2 `new_isr` field or the v3
/// `new_isr_with_epochs` field. A v3 request leaves `new_isr` empty and fills
/// `new_isr_with_epochs`. When `new_isr` is empty, this function therefore
/// takes the broker IDs from `new_isr_with_epochs`.
pub(super) fn handle_partition_with_recovery(
    image: &krabka_metadata::MetadataImage,
    active: &HashSet<u64>,
    requester: i32,
    topic_name: &str,
    request: &ReqPartitionData,
    changes: &mut Vec<MetadataRecord>,
) -> RespPartitionData {
    let partition_index = request.partition_index;
    let new_isr_i32 = request.new_isr.as_slice();
    let new_isr_with_epochs = request.new_isr_with_epochs.as_slice();
    let Some(part_rec) = image.partition(topic_name, partition_index) else {
        return error_part(partition_index, codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    let current_recovery_state = image.leader_recovery_state(topic_name, partition_index);

    // Resolve the effective ISR from the request. Protocol v2 sends
    // `new_isr: Vec<i32>`; v3 sends `new_isr_with_epochs` instead and
    // leaves `new_isr` empty. Fall back to extracting broker_ids from
    // `new_isr_with_epochs` when the v2 field is absent.
    let fallback_isr_i32: Vec<i32>;
    let effective_isr_i32: &[i32] = if new_isr_i32.is_empty() && !new_isr_with_epochs.is_empty() {
        fallback_isr_i32 = new_isr_with_epochs.iter().map(|bs| bs.broker_id).collect();
        &fallback_isr_i32
    } else {
        new_isr_i32
    };

    // Kafka's `ineligibleReplicasForIsr`: a broker in the proposed ISR is
    // ineligible if it is not registered, is in controlled shutdown, is
    // fenced, or carries a broker epoch other than -1 that disagrees with its
    // registration (KIP-903). The registration carries the fence and the
    // controlled shutdown, and `active`, the controller's heartbeat registry,
    // also refuses a broker whose change has not committed yet. A request
    // older than v3 carries no epochs, so Kafka checks its `new_isr` with -1
    // for each. Any ineligible replica fails the whole partition.
    let proposed_states: Vec<ProposedReplica> =
        if !new_isr_i32.is_empty() || new_isr_with_epochs.is_empty() {
            effective_isr_i32
                .iter()
                .map(|&broker_id| ProposedReplica {
                    broker_id,
                    broker_epoch: -1,
                })
                .collect()
        } else {
            new_isr_with_epochs
                .iter()
                .map(|state| ProposedReplica {
                    broker_id: state.broker_id,
                    broker_epoch: state.broker_epoch,
                })
                .collect()
        };
    let replicas_eligible = proposed_states.iter().all(
        |&ProposedReplica {
             broker_id,
             broker_epoch,
         }| {
            let Ok(id) = u64::try_from(broker_id) else {
                return false;
            };
            image.broker(NodeId(id)).is_some_and(|registration| {
                !registration.fenced
                    && !registration.in_controlled_shutdown
                    && active.contains(&id)
                    && (broker_epoch == -1 || registration.broker_epoch == broker_epoch)
            })
        },
    );

    let requested_recovery_state = match request.leader_recovery_state {
        0 => Some(LeaderRecoveryState::Recovered),
        1 => Some(LeaderRecoveryState::Recovering),
        _ => None,
    };
    let recovery_state_valid = requested_recovery_state.is_some_and(|requested| {
        requested == LeaderRecoveryState::Recovered
            || (effective_isr_i32.len() <= 1
                && current_recovery_state == LeaderRecoveryState::Recovering)
    });

    let admission = isr_admission(AlterPartitionFacts {
        request_leader_epoch: request.leader_epoch,
        current_leader_epoch: part_rec.leader_epoch.0,
        request_partition_epoch: request.partition_epoch,
        current_partition_epoch: part_rec.partition_epoch,
        requester_is_leader: u64::try_from(requester).is_ok_and(|id| part_rec.leader.0 == id),
        proposed_isr: proposed_isr_shape(effective_isr_i32, part_rec),
        recovery_state_valid,
        replicas_eligible,
    });
    let error_code = match admission {
        IsrAdmission::NotController => codes::NOT_CONTROLLER,
        IsrAdmission::FencedLeaderEpoch => codes::FENCED_LEADER_EPOCH,
        IsrAdmission::InvalidUpdateVersion => codes::INVALID_UPDATE_VERSION,
        IsrAdmission::InvalidRequest => codes::INVALID_REQUEST,
        IsrAdmission::IneligibleReplica => codes::INELIGIBLE_REPLICA,
        IsrAdmission::Admit => codes::NONE,
    };
    // An admitted row had a valid, hence known, recovery state.
    let Some(requested_recovery_state) =
        requested_recovery_state.filter(|_| admission == IsrAdmission::Admit)
    else {
        return error_part(partition_index, error_code);
    };
    // An admitted ISR is a valid replica subset, so every member converts.
    let proposed_isr: Vec<NodeId> = effective_isr_i32
        .iter()
        .filter_map(|&n| u64::try_from(n).ok().map(NodeId))
        .collect();

    // Success: submit the ISR change.
    let Some(new_partition_epoch) = next_i32(part_rec.partition_epoch) else {
        return error_part(partition_index, codes::INVALID_REQUEST);
    };
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: topic_name.to_string(),
        partition: partition_index,
        leader: part_rec.leader,
        replicas: part_rec.replicas.clone(),
        isr: proposed_isr,
        leader_epoch: part_rec.leader_epoch,
        adding_replicas: part_rec.adding_replicas.clone(),
        removing_replicas: part_rec.removing_replicas.clone(),
        directories: part_rec.directories.clone(),
        partition_epoch: new_partition_epoch,
    }));
    if requested_recovery_state != current_recovery_state {
        changes.push(MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: topic_name.to_string(),
                partition: partition_index,
                state: requested_recovery_state,
            },
        ));
    }

    RespPartitionData {
        partition_index,
        error_code: codes::NONE,
        leader_id: i32::try_from(part_rec.leader.0).unwrap_or(0),
        leader_epoch: part_rec.leader_epoch.0,
        isr: effective_isr_i32.to_vec(),
        leader_recovery_state: requested_recovery_state as i8,
        partition_epoch: new_partition_epoch,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

/// Kafka's `Replicas.validateIsr` and its leader-membership check: every
/// member must be a non-negative, distinct, assigned replica, and the current
/// leader must be one of them.
fn proposed_isr_shape(proposed: &[i32], partition: &PartitionRecord) -> ProposedIsr {
    let assigned: HashSet<NodeId> = partition.replicas.iter().copied().collect();
    let mut seen = HashSet::with_capacity(proposed.len());
    let valid = proposed.iter().all(|&member| {
        u64::try_from(member)
            .is_ok_and(|id| assigned.contains(&NodeId(id)) && seen.insert(NodeId(id)))
    });
    if !valid {
        ProposedIsr::Invalid
    } else if seen.contains(&partition.leader) {
        ProposedIsr::Valid
    } else {
        ProposedIsr::WithoutLeader
    }
}

#[cfg(test)]
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct PartitionRequestSetup<'a> {
    #[default("t")]
    topic: &'a str,
    partition: krabka_ids::PartitionIndex,
    #[default(LeaderEpoch(5))]
    leader_epoch: LeaderEpoch,
    #[default(&[crate::test_support::WireBrokerId(1)])]
    new_isr: &'a [WireBrokerId],
    #[default(&[])]
    new_isr_with_epochs: &'a [krabka_protocol::owned::alter_partition_request::BrokerState],
}

/// Runs [`handle_partition_with_recovery`] for one row from the partition's
/// leader at its current partition epoch that asks for the `Recovered` state,
/// with every broker the image registers active.
#[cfg(test)]
#[bon::builder]
fn handle_partition(
    image: &krabka_metadata::MetadataImage,
    changes: &mut Vec<MetadataRecord>,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
    #[builder(into, default = LeaderEpoch (5))] leader_epoch: LeaderEpoch,
    #[builder(default = & [crate :: test_support :: WireBrokerId (1)])]
    new_isr: &'_ [WireBrokerId],
    #[builder(default = & [])]
    new_isr_with_epochs: &'_ [krabka_protocol::owned::alter_partition_request::BrokerState],
) -> RespPartitionData {
    let setup = PartitionRequestSetup {
        topic,
        partition,
        leader_epoch,
        new_isr,
        new_isr_with_epochs,
    };
    let active = image.brokers().map(|broker| broker.node_id.0).collect();
    let current = image.partition(setup.topic, setup.partition.0);
    handle_partition_with_recovery(
        image,
        &active,
        current.map_or(1, |partition| {
            i32::try_from(partition.leader.0).expect("test leader fits i32")
        }),
        setup.topic,
        &ReqPartitionData {
            partition_index: setup.partition.0,
            leader_epoch: setup.leader_epoch.0,
            partition_epoch: current.map_or(0, |partition| partition.partition_epoch),
            new_isr: setup.new_isr.iter().map(|id| id.0).collect(),
            new_isr_with_epochs: setup.new_isr_with_epochs.to_vec(),
            leader_recovery_state: LeaderRecoveryState::Recovered as i8,
            ..Default::default()
        },
        changes,
    )
}

/// A refused row: Kafka answers only the partition index and the error code,
/// and leaves every other field at its default.
fn error_part(partition_index: i32, error_code: i16) -> RespPartitionData {
    RespPartitionData {
        partition_index,
        error_code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::alter_partition::test_support::{
                ONE_REGISTERED_BROKER, PartitionFixture, RegisteredBroker,
                THREE_REGISTERED_BROKERS, TWO_REGISTERED_BROKERS, bs, image_with,
                image_with_partition, three_broker_states,
            },
            test_support::{BrokerEpoch, PartitionEpoch, WireBrokerId},
        },
        assert2::assert,
    };

    /// Kafka's `ReplicationControlManager.validateAlterPartitionData`, in its
    /// order. The partition has leader 2, replicas `[2, 4, 6]`, leader epoch 9
    /// and partition epoch 11. Each row changes one input of an otherwise
    /// valid request, or two where the order between two checks is the point.
    #[test]
    fn a_row_is_validated_in_kafkas_order_with_kafkas_codes() {
        struct Case {
            name: &'static str,
            broker_id: i32,
            leader_epoch: i32,
            partition_epoch: i32,
            isr: &'static [i32],
            error_code: i16,
        }
        let valid = Case {
            name: "valid",
            broker_id: 2,
            leader_epoch: 9,
            partition_epoch: 11,
            isr: &[2, 4],
            error_code: codes::NONE,
        };
        let cases = [
            Case {
                name: "leader epoch above the controller's",
                leader_epoch: 10,
                error_code: codes::NOT_CONTROLLER,
                ..valid
            },
            Case {
                name: "partition epoch above the controller's",
                partition_epoch: 12,
                error_code: codes::NOT_CONTROLLER,
                ..valid
            },
            Case {
                name: "higher partition epoch wins over a lower leader epoch",
                leader_epoch: 8,
                partition_epoch: 12,
                error_code: codes::NOT_CONTROLLER,
                ..valid
            },
            Case {
                name: "leader epoch below the controller's",
                leader_epoch: 8,
                error_code: codes::FENCED_LEADER_EPOCH,
                ..valid
            },
            Case {
                name: "fenced epoch wins over a sender that does not lead",
                broker_id: 4,
                leader_epoch: 8,
                error_code: codes::FENCED_LEADER_EPOCH,
                ..valid
            },
            Case {
                name: "sender is not the leader",
                broker_id: 4,
                error_code: codes::INVALID_REQUEST,
                ..valid
            },
            Case {
                name: "partition epoch below the controller's",
                partition_epoch: 10,
                error_code: codes::INVALID_UPDATE_VERSION,
                ..valid
            },
            Case {
                name: "stale partition epoch wins over an invalid ISR",
                partition_epoch: 10,
                isr: &[4, 4],
                error_code: codes::INVALID_UPDATE_VERSION,
                ..valid
            },
            Case {
                name: "duplicate ISR member",
                isr: &[2, 4, 4],
                error_code: codes::INVALID_REQUEST,
                ..valid
            },
            Case {
                name: "ISR member outside the replicas",
                isr: &[2, 5],
                error_code: codes::INVALID_REQUEST,
                ..valid
            },
            Case {
                name: "ISR without the leader",
                isr: &[4, 6],
                error_code: codes::INVALID_REQUEST,
                ..valid
            },
            Case {
                name: "empty ISR",
                isr: &[],
                error_code: codes::INVALID_REQUEST,
                ..valid
            },
            Case { ..valid },
        ];
        let image = non_default_partition_image();
        let active = image.brokers().map(|broker| broker.node_id.0).collect();
        for case in cases {
            let mut changes = Vec::new();
            let response = handle_partition_with_recovery(
                &image,
                &active,
                case.broker_id,
                "t",
                &ReqPartitionData {
                    partition_index: 7,
                    leader_epoch: case.leader_epoch,
                    partition_epoch: case.partition_epoch,
                    new_isr: case.isr.to_vec(),
                    ..Default::default()
                },
                &mut changes,
            );
            let expected = if case.error_code == codes::NONE {
                RespPartitionData {
                    partition_index: 7,
                    leader_id: 2,
                    leader_epoch: 9,
                    isr: case.isr.to_vec(),
                    partition_epoch: 12,
                    ..Default::default()
                }
            } else {
                RespPartitionData {
                    partition_index: 7,
                    error_code: case.error_code,
                    ..Default::default()
                }
            };
            assert2::check!(response == expected, "{}", case.name);
            assert2::check!(
                changes.len() == usize::from(case.error_code == codes::NONE),
                "{}",
                case.name
            );
        }
    }

    #[derive(Clone, Copy)]
    enum EpochStateAdmission {
        Admitted,
        Refused,
    }

    /// Epoch-state cases use the same current partition and admission checks.
    fn epoch_state_case(
        registrations: &[RegisteredBroker],
        states: impl FnOnce() -> Vec<BrokerState>,
        error: i16,
        admission: EpochStateAdmission,
    ) {
        let image = image_with(registrations);
        let mut changes = Vec::new();
        let isr = states();
        let response = handle_partition()
            .image(&image)
            .changes(&mut changes)
            .new_isr(&[])
            .new_isr_with_epochs(&isr)
            .call();
        assert!(response.error_code == error, "got {}", response.error_code);
        match admission {
            EpochStateAdmission::Admitted => assert!(changes.len() == 1),
            EpochStateAdmission::Refused => assert!(changes.is_empty()),
        }
    }
    use krabka_protocol::owned::alter_partition_request::BrokerState;

    #[test]
    fn matching_epochs_succeed() {
        epoch_state_case(
            THREE_REGISTERED_BROKERS,
            || three_broker_states().call(),
            codes::NONE,
            EpochStateAdmission::Admitted,
        );
    }

    fn only_partition_change(changes: &[MetadataRecord]) -> &krabka_metadata::PartitionRecord {
        assert!(changes.len() == 1);
        let MetadataRecord::V1Partition(record) = &changes[0] else {
            panic!("wrong change variant");
        };
        record
    }

    fn non_default_partition_image() -> krabka_metadata::MetadataImage {
        image_with_partition(
            &PartitionFixture {
                partition: krabka_ids::PartitionIndex(7),
                leader: krabka_metadata::NodeId(2),
                replicas: &[
                    krabka_metadata::NodeId(2),
                    krabka_metadata::NodeId(4),
                    krabka_metadata::NodeId(6),
                ],
                isr: &[krabka_metadata::NodeId(2), krabka_metadata::NodeId(4)],
                leader_epoch: krabka_metadata::LeaderEpoch(9),
                partition_epoch: PartitionEpoch(11),
            },
            &[
                RegisteredBroker {
                    node_id: krabka_metadata::NodeId(2),
                    epoch: BrokerEpoch(20),
                },
                RegisteredBroker {
                    node_id: krabka_metadata::NodeId(4),
                    epoch: BrokerEpoch(40),
                },
                RegisteredBroker {
                    node_id: krabka_metadata::NodeId(6),
                    epoch: BrokerEpoch(60),
                },
            ],
        )
    }

    fn response_for_non_default_partition(
        image: &krabka_metadata::MetadataImage,
        changes: &mut Vec<MetadataRecord>,
        leader_epoch: i32,
    ) -> RespPartitionData {
        handle_partition()
            .image(image)
            .changes(changes)
            .partition(7)
            .leader_epoch(leader_epoch)
            .new_isr(&[WireBrokerId(2), WireBrokerId(4)])
            .call()
    }

    #[test]
    fn success_response_preserves_non_default_partition_fields() {
        let image = non_default_partition_image();
        let mut changes = Vec::new();
        let resp = response_for_non_default_partition(&image, &mut changes, 9);

        let expected = krabka_macros::tagged_wire!(RespPartitionData {
            partition_index: 7,
            error_code: codes::NONE,
            leader_id: 2,
            leader_epoch: 9,
            isr: vec![2, 4],
            leader_recovery_state: 0,
            partition_epoch: 12,
        });
        assert!(resp == expected);
        let record = only_partition_change(&changes);
        assert!(record.partition == 7);
        assert!(record.partition_epoch == 12);
    }

    /// Kafka's `ReplicationControlManager.validateAlterPartitionData` over a
    /// partition led by broker 1 at leader epoch 5 and partition epoch 10,
    /// with replicas `[1, 2, 3]` and ISR `[1, 2]`. Each row is one request and
    /// the error code a Kafka controller answers it with.
    /// A validation row: its label, then the request's broker id, leader
    /// epoch and partition epoch, the proposed ISR, and the expected error.
    struct ValidationCase<'a> {
        label: &'a str,
        requester: i32,
        leader_epoch: i32,
        partition_epoch: i32,
        new_isr: &'a [i32],
        expected: i16,
    }

    #[test]
    fn rows_are_validated_in_kafkas_order() {
        let fixture = PartitionFixture {
            partition_epoch: PartitionEpoch(10),
            ..Default::default()
        };
        let image = image_with_partition(&fixture, THREE_REGISTERED_BROKERS);
        let active = image.brokers().map(|broker| broker.node_id.0).collect();
        let cases: &[ValidationCase<'_>] = &[
            ValidationCase {
                label: "current epochs",
                requester: 1,
                leader_epoch: 5,
                partition_epoch: 10,
                new_isr: &[1],
                expected: codes::NONE,
            },
            ValidationCase {
                label: "a leader epoch the controller has not seen",
                requester: 1,
                leader_epoch: 6,
                partition_epoch: 10,
                new_isr: &[1],
                expected: codes::NOT_CONTROLLER,
            },
            ValidationCase {
                label: "a partition epoch the controller has not seen",
                requester: 1,
                leader_epoch: 4,
                partition_epoch: 11,
                new_isr: &[1],
                expected: codes::NOT_CONTROLLER,
            },
            ValidationCase {
                label: "an older leader epoch",
                requester: 2,
                leader_epoch: 4,
                partition_epoch: 9,
                new_isr: &[1],
                expected: codes::FENCED_LEADER_EPOCH,
            },
            ValidationCase {
                label: "a requester that is not the leader",
                requester: 2,
                leader_epoch: 5,
                partition_epoch: 9,
                new_isr: &[1],
                expected: codes::INVALID_REQUEST,
            },
            ValidationCase {
                label: "an older partition epoch",
                requester: 1,
                leader_epoch: 5,
                partition_epoch: 9,
                new_isr: &[1, 4],
                expected: codes::INVALID_UPDATE_VERSION,
            },
            ValidationCase {
                label: "an ISR without the leader",
                requester: 1,
                leader_epoch: 5,
                partition_epoch: 10,
                new_isr: &[2],
                expected: codes::INVALID_REQUEST,
            },
            ValidationCase {
                label: "an ISR naming a replica twice",
                requester: 1,
                leader_epoch: 5,
                partition_epoch: 10,
                new_isr: &[1, 2, 2],
                expected: codes::INVALID_REQUEST,
            },
        ];
        for &ValidationCase {
            label,
            requester,
            leader_epoch,
            partition_epoch,
            new_isr,
            expected,
        } in cases
        {
            let mut changes = Vec::new();
            let response = handle_partition_with_recovery(
                &image,
                &active,
                requester,
                "t",
                &ReqPartitionData {
                    partition_index: 0,
                    leader_epoch,
                    partition_epoch,
                    new_isr: new_isr.to_vec(),
                    ..Default::default()
                },
                &mut changes,
            );
            assert2::check!(response.error_code == expected, "{label}");
            assert2::check!(changes.is_empty() == (expected != codes::NONE), "{label}");
        }
    }

    /// Two proposals built from the same ISR: the leader shrinks to `[1]`,
    /// then a stale expand to `[1, 2, 3]` built at the same leader epoch
    /// arrives. The controller has moved to partition epoch 11, so Kafka
    /// answers `INVALID_UPDATE_VERSION` and the newer ISR stands.
    #[test]
    fn a_stale_partition_epoch_cannot_overwrite_a_newer_isr() {
        let mut image = image_with_partition(
            &PartitionFixture {
                partition_epoch: PartitionEpoch(10),
                ..Default::default()
            },
            THREE_REGISTERED_BROKERS,
        );
        let active = image.brokers().map(|broker| broker.node_id.0).collect();
        let row = |new_isr: &[i32]| ReqPartitionData {
            partition_index: 0,
            leader_epoch: 5,
            partition_epoch: 10,
            new_isr: new_isr.to_vec(),
            ..Default::default()
        };

        let mut changes = Vec::new();
        let shrink =
            handle_partition_with_recovery(&image, &active, 1, "t", &row(&[1]), &mut changes);
        assert!(shrink.error_code == codes::NONE);
        for change in &changes {
            image.apply(change);
        }

        let mut stale_changes = Vec::new();
        let stale = handle_partition_with_recovery(
            &image,
            &active,
            1,
            "t",
            &row(&[1, 2, 3]),
            &mut stale_changes,
        );

        let expected = RespPartitionData {
            partition_index: 0,
            error_code: codes::INVALID_UPDATE_VERSION,
            ..Default::default()
        };
        assert!(stale == expected);
        assert!(stale_changes.is_empty());
        let committed = image.partition("t", 0).expect("partition");
        assert!(committed.isr == vec![krabka_metadata::NodeId(1)]);
        assert!(committed.partition_epoch == 11);
    }

    #[test]
    fn exhausted_partition_epoch_rejects_the_isr_change() {
        let image = image_with_partition(
            &PartitionFixture {
                replicas: &[krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                partition_epoch: PartitionEpoch(i32::MAX),
                ..Default::default()
            },
            TWO_REGISTERED_BROKERS,
        );
        let mut changes = Vec::new();

        let response = handle_partition()
            .image(&image)
            .changes(&mut changes)
            .new_isr(&[WireBrokerId(1), WireBrokerId(2)])
            .call();

        assert!(response.error_code == codes::INVALID_REQUEST);
        assert!(changes.is_empty());
    }

    #[test]
    fn error_response_matches_kafkas_default_fields() {
        let image = non_default_partition_image();
        let mut changes = Vec::new();
        let resp = response_for_non_default_partition(&image, &mut changes, 8);

        let expected = RespPartitionData {
            partition_index: 7,
            error_code: codes::FENCED_LEADER_EPOCH,
            ..Default::default()
        };
        assert!(resp == expected);
        assert!(changes.is_empty());
    }

    #[test]
    fn explicit_v2_isr_wins_when_epoch_states_are_also_present() {
        let image = image_with(THREE_REGISTERED_BROKERS);
        let mut changes = Vec::new();
        let resp = handle_partition()
            .image(&image)
            .changes(&mut changes)
            .new_isr(&[WireBrokerId(1), WireBrokerId(2)])
            .new_isr_with_epochs(&[bs(WireBrokerId(3), BrokerEpoch(30))])
            .call();
        let expected = krabka_macros::tagged_wire!(RespPartitionData {
            partition_index: 0,
            error_code: codes::NONE,
            leader_id: 1,
            leader_epoch: 5,
            isr: vec![1, 2],
            leader_recovery_state: 0,
            partition_epoch: 1,
        });
        assert!(resp == expected);
        let record = only_partition_change(&changes);
        assert!(record.isr == vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)]);
    }

    #[test]
    fn stale_epoch_is_ineligible() {
        // 29 disagrees with broker 3's registered epoch 30.
        epoch_state_case(
            THREE_REGISTERED_BROKERS,
            || three_broker_states().third(29).call(),
            codes::INELIGIBLE_REPLICA,
            EpochStateAdmission::Refused,
        );
    }

    #[test]
    fn unregistered_replica_is_ineligible() {
        // Broker 3 was never registered.
        epoch_state_case(
            TWO_REGISTERED_BROKERS,
            || three_broker_states().third(-1).call(),
            codes::INELIGIBLE_REPLICA,
            EpochStateAdmission::Refused,
        );
    }

    #[test]
    fn sentinel_epoch_skips_epoch_check() {
        // -1 skips the epoch comparison.
        epoch_state_case(
            THREE_REGISTERED_BROKERS,
            || three_broker_states().first(-1).second(-1).third(-1).call(),
            codes::NONE,
            EpochStateAdmission::Admitted,
        );
    }

    /// A v2 request carries no broker epochs, so none is compared, but every
    /// replica in its ISR must still be registered: Kafka checks `new_isr`
    /// with -1 for each epoch.
    #[yare::parameterized(
        registered_replicas = { &[1, 2][..], codes::NONE },
        unregistered_replica = { &[1, 2, 3][..], codes::INELIGIBLE_REPLICA },
    )]
    fn a_v2_request_checks_registration_without_epochs(new_isr: &[i32], expected: i16) {
        let image = image_with(TWO_REGISTERED_BROKERS);
        let mut changes = Vec::new();
        let resp = handle_partition()
            .image(&image)
            .changes(&mut changes)
            .new_isr(
                &new_isr
                    .iter()
                    .copied()
                    .map(WireBrokerId)
                    .collect::<Vec<_>>(),
            )
            .call();
        assert2::check!(resp.error_code == expected, "new_isr {new_isr:?}");
    }

    #[test]
    fn negative_replica_id_is_invalid_even_when_node_zero_is_a_replica() {
        let image = image_with_partition(
            &PartitionFixture {
                replicas: &[krabka_metadata::NodeId(0), krabka_metadata::NodeId(1)],
                isr: &[krabka_metadata::NodeId(1)],
                ..Default::default()
            },
            &[
                RegisteredBroker {
                    node_id: krabka_metadata::NodeId(0),
                    epoch: BrokerEpoch(0),
                },
                RegisteredBroker {
                    node_id: krabka_metadata::NodeId(1),
                    epoch: BrokerEpoch(10),
                },
            ],
        );
        let mut changes = Vec::new();
        let resp = handle_partition()
            .image(&image)
            .changes(&mut changes)
            .new_isr(&[WireBrokerId(-1)])
            .call();

        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(changes.is_empty());
    }

    #[test]
    fn recovering_partition_cannot_expand_until_leader_reports_recovered() {
        let mut image = image_with(TWO_REGISTERED_BROKERS);
        image.apply(&MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: "t".into(),
                partition: 0,
                state: LeaderRecoveryState::Recovering,
            },
        ));
        let mut changes = Vec::new();

        let active = image.brokers().map(|broker| broker.node_id.0).collect();
        let rejected = handle_partition_with_recovery(
            &image,
            &active,
            1,
            "t",
            &ReqPartitionData {
                partition_index: 0,
                leader_epoch: 5,
                new_isr: vec![1, 2],
                leader_recovery_state: LeaderRecoveryState::Recovering as i8,
                ..Default::default()
            },
            &mut changes,
        );
        assert!(rejected.error_code == codes::INVALID_REQUEST);
        assert!(rejected.leader_recovery_state == LeaderRecoveryState::Recovered as i8);
        assert!(changes.is_empty());

        let recovered = handle_partition_with_recovery(
            &image,
            &active,
            1,
            "t",
            &ReqPartitionData {
                partition_index: 0,
                leader_epoch: 5,
                new_isr: vec![1],
                leader_recovery_state: LeaderRecoveryState::Recovered as i8,
                ..Default::default()
            },
            &mut changes,
        );
        assert!(recovered.error_code == codes::NONE);
        assert!(matches!(changes.as_slice(), [
            MetadataRecord::V1Partition(_),
            MetadataRecord::V1PartitionRecovery(PartitionRecoveryRecord {
                state: LeaderRecoveryState::Recovered,
                ..
            })
        ]));
    }

    /// krabka-io/krabka-broker#825: Kafka's `ineligibleReplicasForIsr`. The
    /// leader, broker 1, proposes the ISR `[1, 2]`. Broker 2 is in each row's
    /// state, as the controller's heartbeat registry holds it, or as its
    /// registration in the image holds it while the registry says active.
    #[tokio::test]
    async fn a_replica_that_is_not_active_is_ineligible_for_the_isr() {
        use crate::heartbeat::{
            controller_state::{BrokerControlState, ControllerLivenessState},
            fencing::{RegistrationChange, registration_change as registration_change_record},
        };

        /// A broker 2 situation and the error code the ISR check answers with.
        struct Broker2Case {
            broker_2: Broker2,
            expected: i16,
        }

        /// Where broker 2 stands.
        #[derive(Debug, Clone, Copy)]
        enum Broker2 {
            Unfenced,
            Fenced,
            InControlledShutdown,
            NeverHeartbeated,
            NotRegistered,
            RegistrationFenced,
            RegistrationInControlledShutdown,
        }

        let cases: &[Broker2Case] = &[
            Broker2Case {
                broker_2: Broker2::Unfenced,
                expected: codes::NONE,
            },
            Broker2Case {
                broker_2: Broker2::Fenced,
                expected: codes::INELIGIBLE_REPLICA,
            },
            Broker2Case {
                broker_2: Broker2::InControlledShutdown,
                expected: codes::INELIGIBLE_REPLICA,
            },
            Broker2Case {
                broker_2: Broker2::NeverHeartbeated,
                expected: codes::INELIGIBLE_REPLICA,
            },
            Broker2Case {
                broker_2: Broker2::NotRegistered,
                expected: codes::INELIGIBLE_REPLICA,
            },
            Broker2Case {
                broker_2: Broker2::RegistrationFenced,
                expected: codes::INELIGIBLE_REPLICA,
            },
            Broker2Case {
                broker_2: Broker2::RegistrationInControlledShutdown,
                expected: codes::INELIGIBLE_REPLICA,
            },
        ];
        for Broker2Case { broker_2, expected } in cases {
            let registered: &[RegisteredBroker] = match broker_2 {
                Broker2::NotRegistered => ONE_REGISTERED_BROKER,
                _ => TWO_REGISTERED_BROKERS,
            };
            let mut image = image_with(registered);
            let registration_change = match broker_2 {
                Broker2::RegistrationFenced => Some(RegistrationChange::FENCE),
                Broker2::RegistrationInControlledShutdown => {
                    Some(RegistrationChange::CONTROLLED_SHUTDOWN)
                }
                _ => None,
            };
            if let Some(change) = registration_change {
                let record = registration_change_record(&image, NodeId(2), change)
                    .expect("broker 2 is registered unfenced");
                image.apply(&record);
            }
            let liveness = ControllerLivenessState::new(krabka_units::secs(10));
            for RegisteredBroker { node_id: node, .. } in registered {
                if *node == 2 && matches!(broker_2, Broker2::NeverHeartbeated) {
                    liveness.track_registered([2]).await;
                    continue;
                }
                liveness.record_fenced_heartbeat(node.0).await;
                liveness
                    .touch(node.0, BrokerControlState::Unfenced, 0)
                    .await;
            }
            match broker_2 {
                Broker2::Fenced => liveness.touch(2, BrokerControlState::Fenced, 0).await,
                Broker2::InControlledShutdown => {
                    liveness
                        .touch(2, BrokerControlState::ControlledShutdown, 0)
                        .await;
                    liveness.enter_controlled_shutdown(2, 5).await;
                }
                _ => {}
            }
            let active = liveness.alive_snapshot().await;
            let mut changes = Vec::new();

            for (form, request) in [
                ("v2 new_isr", ReqPartitionData {
                    partition_index: 0,
                    leader_epoch: 5,
                    new_isr: vec![1, 2],
                    ..Default::default()
                }),
                ("v3 new_isr_with_epochs", ReqPartitionData {
                    partition_index: 0,
                    leader_epoch: 5,
                    new_isr_with_epochs: vec![
                        bs(
                            crate::test_support::WireBrokerId(1),
                            crate::test_support::BrokerEpoch(10),
                        ),
                        bs(
                            crate::test_support::WireBrokerId(2),
                            crate::test_support::BrokerEpoch(-1),
                        ),
                    ],
                    ..Default::default()
                }),
            ] {
                let response =
                    handle_partition_with_recovery(&image, &active, 1, "t", &request, &mut changes);
                assert2::check!(
                    response.error_code == *expected,
                    "broker 2 {broker_2:?}, {form}"
                );
            }
        }
    }
}
