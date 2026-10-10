//! Unit tests for the KIP-848 heartbeat path.

use std::{collections::HashMap, sync::Arc};

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_protocol::primitives::uuid::Uuid;

use super::*;
use crate::coordinator::unified::{
    actor::{
        member_state::build_member,
        test_support::{
            StaticMetadata, empty_metadata, make_coordinator, make_coordinator_with_topic,
            make_coordinator_with_topic_policy, rpc, seed_classic_member, subscription_blob,
        },
    },
    offsets_log::fake::InMemoryOffsetsLog,
    persistence_next_gen::GroupMetadataValue,
    reconciler::ReconcileInput,
    test_support::MemberEpoch,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_emits_one_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(resp.error_code == 0);
    let batches = log.batches().await;
    assert!(
        batches.len() == 1,
        "first join should write exactly one batch"
    );
    // Minimum: k3 (group metadata) + k5 (member metadata) + k8 (current).
    assert!(batches[0].records.len() >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_join_adopts_client_member_id() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let resp = rpc::consumer_request(
        &handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "client-uuid-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
    )
    .await;
    // The join must succeed, echo the client-supplied member id, and
    // advance the epoch off 0. The client-id first-join takes the same
    // flush path as the empty-id case and persists exactly one batch.
    check!(resp.error_code == 0);
    check!(resp.member_id.as_deref() == Some("client-uuid-1"));
    check!(resp.member_epoch >= 1);
    check!(log.batches().await.len() == 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_limit_rejects_only_new_members() {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = crate::coordinator::unified::actor::test_support::coordinator_with_log(
        NextGenConfig {
            max_size: 1,
            ..NextGenConfig::assigning_at_once()
        },
        empty_metadata(),
        log,
    );
    let handle = coord.get_or_create_consumer("g");

    let joined = rpc::consumer_heartbeat(&handle, "m1", 0, Some("t")).await;
    check!(joined.error_code == codes::NONE);

    let rejected = rpc::consumer_heartbeat(&handle, "m2", 0, Some("t")).await;
    check!(rejected.error_code == codes::GROUP_MAX_SIZE_REACHED);

    let existing = rpc::consumer_heartbeat(&handle, "m1", joined.member_epoch, Some("t")).await;
    check!(existing.error_code == codes::NONE);
    check!(existing.member_epoch == joined.member_epoch);
}

/// A consumer actor exists from its first heartbeat, but the group only from
/// its first join. Kafka's `getOrMaybeCreateConsumerGroup` and
/// `consumerGroupLeave` answer `GROUP_ID_NOT_FOUND` to any other epoch while
/// the group is missing, and `UNKNOWN_MEMBER_ID` once it exists and lacks the
/// member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_actor_holds_no_consumer_group_before_the_first_join() {
    let (coord, _log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");

    let mut answers = Vec::new();
    for (member_id, epoch) in [("m1", 3), ("m1", -1), ("m1", 0), ("m2", 3), ("m1", -1)] {
        let answer = rpc::consumer_heartbeat(&handle, member_id, epoch, Some("t")).await;
        answers.push((answer.error_code, answer.error_message));
    }

    check!(
        answers
            == vec![
                (
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Consumer group g not found.".to_string())
                ),
                (
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Group g not found.".to_string())
                ),
                (codes::NONE, None),
                // The group exists now.
                (
                    codes::UNKNOWN_MEMBER_ID,
                    Some("Member m2 is not a member of group g.".to_string())
                ),
                (codes::NONE, None),
            ]
    );
}

const IDENTITY_TOPIC: Uuid = Uuid([9; 16]);

/// The one topic `t`, with two partitions, of the handoff test.
fn handoff_metadata() -> StaticMetadata {
    StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".into(), IDENTITY_TOPIC)].into(),
            partitions_per_topic: [(IDENTITY_TOPIC, 2)].into(),
            ..Default::default()
        },
    }
}

fn handoff_heartbeat(
    state: &mut GroupState,
    request: ConsumerGroupHeartbeatRequest,
) -> HeartbeatStep {
    step_heartbeat(
        state,
        &NextGenConfig::assigning_at_once(),
        &handoff_metadata(),
        &ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            ..request
        },
        ClientIdentity {
            id: "client",
            host: "host",
        },
        Instant::now(),
        &RegexResolution::none(),
    )
}

/// The heartbeat with which a consumer joins `handoff_metadata`'s topic.
fn handoff_join(state: &mut GroupState, member_id: &str) -> HeartbeatStep {
    handoff_heartbeat(
        state,
        crate::coordinator::unified::test_support::consumer_join_request(
            crate::coordinator::unified::test_support::ConsumerJoinSetup {
                group_id: "",
                member_id,
                ..Default::default()
            },
        ),
    )
}

/// What the Java client sends in steady state
/// (`ConsumerHeartbeatRequestManager.HeartbeatState.buildRequestData`): no
/// subscription and no rebalance timeout, and `TopicPartitions` only when its
/// assignment changed, so `owned` is `None` while it did not.
fn handoff_keepalive(
    state: &mut GroupState,
    member_id: &str,
    member_epoch: i32,
    owned: Option<Vec<i32>>,
) -> HeartbeatStep {
    handoff_heartbeat(
        state,
        ConsumerGroupHeartbeatRequest {
            member_id: member_id.into(),
            member_epoch,
            rebalance_timeout_ms: -1,
            topic_partitions: owned.map(|partitions| {
                vec![
                    krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions {
                        topic_id: IDENTITY_TOPIC,
                        partitions,
                        ..Default::default()
                    },
                ]
            }),
            ..Default::default()
        },
    )
}

/// The partitions of `t` that a response assigns, or `None` when it carries no
/// assignment.
fn assigned_partitions(response: &ConsumerGroupHeartbeatResponse) -> Option<Vec<i32>> {
    response.assignment.as_ref().map(|assignment| {
        assignment
            .topic_partitions
            .iter()
            .flat_map(|topic| topic.partitions.iter().copied())
            .collect()
    })
}

/// Kafka reconciles a member only inside its own heartbeat, and reads a
/// heartbeat without `TopicPartitions` as "the owned set is unchanged"
/// (`CurrentAssignmentBuilder.ownsRevokedPartitions(null)`). So with two
/// members, the joiner is granted a partition only after the incumbent, told to
/// revoke it in its own heartbeat, reports an owned set without it. Stock
/// clients send that steady-state heartbeat with a null owned set, which used
/// to wipe the pending revocation and hand the partition to both members.
#[test]
fn a_joiner_gets_a_partition_only_after_the_incumbent_reports_it_revoked() {
    use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState::{
        Stable, UnreleasedPartitions, UnrevokedPartitions,
    };

    let mut state = GroupState::new("g");
    let incumbent = handoff_join(&mut state, "a");
    check!(
        incumbent.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "a",
                epoch: MemberEpoch(2),
                partitions: Some(vec![PartitionIndex(0), PartitionIndex(1)])
            })
    );

    // `b` joins: the group moves to epoch 3 and `a` keeps both partitions
    // until its own heartbeat.
    let joiner = handoff_join(&mut state, "b");
    check!(joiner.response.member_epoch == 3);
    check!(assigned_partitions(&joiner.response) == Some(vec![]));
    check!(state.members["b"].assignment_state == UnreleasedPartitions);
    check!(state.members["a"].assigned_partitions == [(IDENTITY_TOPIC, vec![0, 1])].into());

    // The incumbent's heartbeat carries no owned set. It is told its smaller
    // assignment, and it stays at epoch 2 with the other partition pending
    // revocation.
    let told = handoff_keepalive(&mut state, "a", 2, None);
    let kept = assigned_partitions(&told.response).expect("a is told its assignment shrank");
    check!(kept.len() == 1);
    check!(
        told.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "a",
                epoch: MemberEpoch(2),
                partitions: Some(kept.iter().copied().map(PartitionIndex).collect())
            })
    );
    let revoked = 1 - kept[0];
    check!(state.members["a"].assignment_state == UnrevokedPartitions);
    check!(
        state.members["a"].partitions_pending_revocation
            == [(IDENTITY_TOPIC, vec![revoked])].into()
    );

    // Neither `a`'s next null heartbeat nor `b`'s moves anything: `a` still
    // owns the partition, so `b` does not get it.
    let again = handoff_keepalive(&mut state, "a", 2, None);
    check!(
        again.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "a",
                epoch: MemberEpoch(2),
                ..Default::default()
            })
    );
    let waiting = handoff_keepalive(&mut state, "b", 3, None);
    check!(
        waiting.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "b",
                epoch: MemberEpoch(3),
                ..Default::default()
            })
    );
    check!(state.members["b"].assigned_partitions.is_empty());
    check!(state.members["a"].assignment_state == UnrevokedPartitions);

    // `a` reports what it owns now, without the revoked partition. It moves to
    // the target epoch, and `b` is granted the partition at its next heartbeat.
    let acknowledged = handoff_keepalive(&mut state, "a", 2, Some(kept.clone()));
    check!(
        acknowledged.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "a",
                epoch: MemberEpoch(3),
                ..Default::default()
            })
    );
    check!(state.members["a"].assignment_state == Stable);
    let granted = handoff_keepalive(&mut state, "b", 3, None);
    check!(
        granted.response
            == identity_ok(IdentityExpectationSetup {
                member_id: "b",
                epoch: MemberEpoch(3),
                partitions: Some(vec![PartitionIndex(revoked)])
            })
    );
    check!(state.members["b"].assignment_state == Stable);
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum StaticMemberPresence {
    #[default]
    Present,
    Released,
}

/// One row of [`heartbeat_identity_rules_follow_kafka`].
#[derive(krabka_macros::FieldDefaults)]
struct IdentityRow {
    #[default("identity row")]
    name: &'static str,
    /// Whether to send a static leave for `s1` before the row's request.
    static_member: StaticMemberPresence,
    #[default("m1")]
    member_id: &'static str,
    instance_id: Option<&'static str>,
    #[default(MemberEpoch(5))]
    member_epoch: MemberEpoch,
    owned: Option<Vec<PartitionIndex>>,
    expected: ConsumerGroupHeartbeatResponse,
    /// The sorted member identities and epochs after the request.
    #[default(vec![("m1", MemberEpoch(5)), ("s1", MemberEpoch(5))])]
    members_after: Vec<(&'static str, MemberEpoch)>,
    #[default(Some("s1"))]
    i1_after: Option<&'static str>,
}

fn heartbeat_interval_ms() -> i32 {
    i32::try_from(
        NextGenConfig::assigning_at_once()
            .heartbeat_interval
            .as_millis(),
    )
    .unwrap()
}

#[derive(krabka_macros::FieldDefaults)]
struct IdentityExpectationSetup<'a> {
    #[default("m1")]
    member_id: &'a str,
    #[default(MemberEpoch(5))]
    epoch: MemberEpoch,
    partitions: Option<Vec<PartitionIndex>>,
}

fn identity_ok(setup: IdentityExpectationSetup<'_>) -> ConsumerGroupHeartbeatResponse {
    use krabka_protocol::owned::common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions;

    let IdentityExpectationSetup {
        member_id,
        epoch,
        partitions,
    } = setup;
    ConsumerGroupHeartbeatResponse {
        member_id: Some(member_id.into()),
        member_epoch: epoch.0,
        // Kafka's leave responses carry only the member id and epoch.
        heartbeat_interval_ms: if epoch.0 < 0 {
            0
        } else {
            heartbeat_interval_ms()
        },
        assignment: partitions.map(|partitions| RespAssignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions: partitions.into_iter().map(|index| index.0).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn identity_error(
    error_code: crate::test_support::KafkaErrorCode,
    message: &str,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: error_code.0,
        error_message: Some(message.into()),
        ..Default::default()
    }
}

fn fenced_epoch(direction: &str, received: MemberEpoch) -> ConsumerGroupHeartbeatResponse {
    let received = received.0;
    identity_error(
        crate::test_support::KafkaErrorCode(codes::FENCED_MEMBER_EPOCH),
        &format!(
            "The consumer group member has a {direction} member epoch ({received}) than the one \
             known by the group coordinator (5). The member must abandon all its partitions and \
             rejoin."
        ),
    )
}

fn identity_rows() -> Vec<IdentityRow> {
    let fenced_instance = || {
        identity_error(
            crate::test_support::KafkaErrorCode(codes::FENCED_INSTANCE_ID),
            "Static member m9 with instance id i1 was fenced by member s1.",
        )
    };
    vec![
        IdentityRow {
            members_after: vec![("m1", MemberEpoch(5)), ("s1", MemberEpoch(-2))],
            name: "static member leaves with epoch -2",
            member_id: "s1",
            instance_id: Some("i1"),
            member_epoch: MemberEpoch(-2),
            expected: identity_ok(IdentityExpectationSetup {
                member_id: "s1",
                epoch: MemberEpoch(-2),
                ..Default::default()
            }),
            ..Default::default()
        },
        IdentityRow {
            static_member: StaticMemberPresence::Released,
            members_after: vec![("m1", MemberEpoch(5)), ("s2", MemberEpoch(5))],
            i1_after: Some("s2"),
            name: "new member takes a released instance id",
            member_id: "s2",
            instance_id: Some("i1"),
            member_epoch: MemberEpoch(0),
            owned: Some(vec![]),
            expected: identity_ok(IdentityExpectationSetup {
                member_id: "s2",
                partitions: Some(vec![PartitionIndex(2), PartitionIndex(3)]),
                ..Default::default()
            }),
        },
        IdentityRow {
            name: "new member with an unreleased instance id",
            member_id: "s2",
            instance_id: Some("i1"),
            member_epoch: MemberEpoch(0),
            owned: Some(vec![]),
            expected: identity_error(
                crate::test_support::KafkaErrorCode(codes::UNRELEASED_INSTANCE_ID),
                "Static member s2 with instance id i1 cannot join the group because the instance \
                 id is owned by s1 member.",
            ),
            ..Default::default()
        },
        IdentityRow {
            name: "known member rejoins with epoch 0",
            member_epoch: MemberEpoch(0),
            owned: Some(vec![]),
            expected: identity_ok_two_partitions(),
            ..Default::default()
        },
        IdentityRow {
            name: "previous epoch with a subset of the assignment",
            member_epoch: MemberEpoch(4),
            owned: Some(vec![krabka_ids::PartitionIndex(0)]),
            expected: identity_ok_two_partitions(),
            ..Default::default()
        },
        IdentityRow {
            name: "previous epoch with a superset of the assignment",
            member_epoch: MemberEpoch(4),
            owned: Some(vec![
                krabka_ids::PartitionIndex(0),
                krabka_ids::PartitionIndex(1),
                krabka_ids::PartitionIndex(2),
            ]),
            expected: fenced_epoch("smaller", MemberEpoch(4)),
            ..Default::default()
        },
        IdentityRow {
            name: "epoch older than the previous epoch",
            member_epoch: MemberEpoch(3),
            owned: Some(vec![krabka_ids::PartitionIndex(0)]),
            expected: fenced_epoch("smaller", MemberEpoch(3)),
            ..Default::default()
        },
        IdentityRow {
            name: "greater epoch",
            member_epoch: MemberEpoch(6),
            owned: Some(vec![
                krabka_ids::PartitionIndex(0),
                krabka_ids::PartitionIndex(1),
            ]),
            expected: fenced_epoch("greater", MemberEpoch(6)),
            ..Default::default()
        },
        IdentityRow {
            name: "leave of an unknown member",
            member_id: "ghost",
            member_epoch: MemberEpoch(-1),
            expected: identity_error(
                crate::test_support::KafkaErrorCode(codes::UNKNOWN_MEMBER_ID),
                "Member ghost is not a member of group g.",
            ),
            ..Default::default()
        },
        IdentityRow {
            members_after: vec![("s1", MemberEpoch(5))],
            name: "dynamic member leaves with epoch -1",
            member_epoch: MemberEpoch(-1),
            expected: identity_ok(IdentityExpectationSetup {
                epoch: MemberEpoch(-1),
                ..Default::default()
            }),
            ..Default::default()
        },
        IdentityRow {
            name: "static heartbeat from another member id",
            member_id: "m9",
            instance_id: Some("i1"),
            owned: Some(vec![
                krabka_ids::PartitionIndex(2),
                krabka_ids::PartitionIndex(3),
            ]),
            expected: fenced_instance(),
            ..Default::default()
        },
        IdentityRow {
            name: "static leave from another member id",
            member_id: "m9",
            instance_id: Some("i1"),
            member_epoch: MemberEpoch(-2),
            expected: fenced_instance(),
            ..Default::default()
        },
        IdentityRow {
            name: "static heartbeat with an unknown instance id",
            member_id: "s1",
            instance_id: Some("i9"),
            owned: Some(vec![
                krabka_ids::PartitionIndex(2),
                krabka_ids::PartitionIndex(3),
            ]),
            expected: identity_error(
                crate::test_support::KafkaErrorCode(codes::UNKNOWN_MEMBER_ID),
                "Instance id i9 is unknown.",
            ),
            ..Default::default()
        },
    ]
}

#[derive(krabka_macros::FieldDefaults)]
struct IdentityRequestSetup<'a> {
    #[default("m1")]
    member_id: &'a str,
    instance_id: Option<&'a str>,
    #[default(MemberEpoch(5))]
    member_epoch: MemberEpoch,
    owned: Option<Vec<PartitionIndex>>,
}

fn identity_request(setup: IdentityRequestSetup<'_>) -> ConsumerGroupHeartbeatRequest {
    use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

    let IdentityRequestSetup {
        member_id,
        instance_id,
        member_epoch,
        owned,
    } = setup;
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        instance_id: instance_id.map(str::to_string),
        member_epoch: member_epoch.0,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: owned.map(|partitions| {
            vec![TopicPartitions {
                topic_id: IDENTITY_TOPIC,
                partitions: partitions.into_iter().map(|index| index.0).collect(),
                ..Default::default()
            }]
        }),
        ..Default::default()
    }
}

/// A group with a dynamic member `m1` on partitions 0 and 1 and a static
/// member `s1` (instance `i1`) on partitions 2 and 3, both at epoch 5 with
/// previous epoch 4, and a settled target.
fn identity_group() -> GroupState {
    let mut state = GroupState::new("g");
    state.group_epoch = 5;
    for (member_id, instance_id, partitions) in
        [("m1", None, vec![0, 1]), ("s1", Some("i1"), vec![2, 3])]
    {
        let mut member = build_member(
            member_id,
            &identity_request(IdentityRequestSetup {
                member_id,
                instance_id,
                member_epoch: MemberEpoch(0),
                ..Default::default()
            }),
            crate::coordinator::unified::ClientIdentity { id: "c", host: "h" },
            Instant::now(),
        );
        member.member_epoch = 5;
        member.previous_member_epoch = 4;
        member.assigned_partitions = HashMap::from([(IDENTITY_TOPIC, partitions.clone())]);
        state.add_or_update_member(member);
        state.target.per_member.insert(
            member_id.to_string(),
            HashMap::from([(IDENTITY_TOPIC, partitions)]),
        );
    }
    state.target.epoch = 5;
    state
}

/// Kafka's KIP-848 identity rules for `ConsumerGroupHeartbeat`
/// (`GroupMetadataManager.consumerGroupHeartbeat`, `consumerGroupLeave`,
/// `throwIfConsumerGroupMemberEpochIsInvalid`, the instance checks and
/// `getOrMaybeSubscribeStaticConsumerGroupMember`). Each row starts from
/// [`identity_group`] and compares the whole response and the members after.
#[test]
fn heartbeat_identity_rules_follow_kafka() {
    let config = NextGenConfig::assigning_at_once();
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: HashMap::from([("t".to_string(), IDENTITY_TOPIC)]),
            partitions_per_topic: HashMap::from([(IDENTITY_TOPIC, 4)]),
            ..Default::default()
        },
    };
    let client = crate::coordinator::unified::ClientIdentity { id: "c", host: "h" };

    for row in identity_rows() {
        let mut state = identity_group();
        // The group recorded the hash of its topics, so its first heartbeat's
        // refresh finds nothing new.
        state.record_metadata_hash(crate::coordinator::unified::reconciler::metadata_hash(
            &state,
            &metadata.input,
        ));
        if row.static_member == StaticMemberPresence::Released {
            let released = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &identity_request(IdentityRequestSetup {
                    member_id: "s1",
                    instance_id: Some("i1"),
                    member_epoch: MemberEpoch(-2),
                    ..Default::default()
                }),
                client,
                Instant::now(),
                &RegexResolution::none(),
            );
            check!(released.response.error_code == codes::NONE, "{}", row.name);
        }

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &identity_request(IdentityRequestSetup {
                member_id: row.member_id,
                instance_id: row.instance_id,
                member_epoch: row.member_epoch,
                owned: row.owned,
            }),
            client,
            Instant::now(),
            &RegexResolution::none(),
        );

        let mut members: Vec<(&str, MemberEpoch)> = state
            .members
            .values()
            .map(|member| (member.member_id.as_str(), MemberEpoch(member.member_epoch)))
            .collect();
        members.sort_unstable();
        check!(step.response == row.expected, "{}", row.name);
        check!(members == row.members_after, "{}", row.name);
        check!(
            state.current_member_for_instance("i1") == row.i1_after,
            "{}",
            row.name
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MemberRecordKind {
    Tombstone,
    Value,
}

/// The member identities and record kinds, sorted.
fn written<T>(records: &[(String, Option<T>)]) -> Vec<(String, MemberRecordKind)> {
    let mut out: Vec<_> = records
        .iter()
        .map(|(id, value)| {
            (
                id.clone(),
                if value.is_some() {
                    MemberRecordKind::Value
                } else {
                    MemberRecordKind::Tombstone
                },
            )
        })
        .collect();
    out.sort_unstable();
    out
}

/// A static member that leaves with -2 writes only its current assignment at
/// epoch -2. A new member with the same instance id then writes its own
/// records and the released member's tombstones in one batch, as Kafka's
/// `replaceMember` does.
#[test]
fn static_replacement_writes_new_records_and_tombstones_the_released_member() {
    let config = NextGenConfig::assigning_at_once();
    let metadata = empty_metadata();
    let client = crate::coordinator::unified::ClientIdentity { id: "c", host: "h" };
    let join = |member_id: &str, member_epoch: i32| {
        identity_request(IdentityRequestSetup {
            member_id,
            instance_id: Some("i1"),
            member_epoch: MemberEpoch(member_epoch),
            owned: Some(vec![]),
        })
    };
    let mut state = GroupState::new("g");
    let joined = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", 0),
        client,
        Instant::now(),
        &RegexResolution::none(),
    );
    check!(joined.response.error_code == codes::NONE);

    let left = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &join("s1", -2),
        client,
        Instant::now(),
        &RegexResolution::none(),
    );
    let current: Vec<(&str, Option<i32>)> = left
        .pending
        .current_per_member
        .iter()
        .map(|(id, value)| (id.as_str(), value.as_ref().map(|value| value.member_epoch)))
        .collect();
    check!(current == vec![("s1", Some(-2))]);
    check!(left.pending.member_metadata.is_empty());
    check!(left.pending.group_metadata.is_none());

    let replaced = step_heartbeat(
        &mut state,
        &config,
        &*metadata,
        &ConsumerGroupHeartbeatRequest {
            rack_id: Some("rack-b".into()),
            rebalance_timeout_ms: 12_345,
            ..join("s2", 0)
        },
        client,
        Instant::now(),
        &RegexResolution::none(),
    );
    check!(replaced.response.error_code == codes::NONE);
    check!(replaced.response.member_epoch == joined.response.member_epoch);
    // Kafka's `replaceMember` records come first: the released member's
    // tombstones and the copy under the new id.
    let expected = vec![
        ("s1".to_string(), MemberRecordKind::Tombstone),
        ("s2".to_string(), MemberRecordKind::Value),
    ];
    check!(written(&replaced.pending.member_metadata) == expected);
    check!(written(&replaced.pending.target_per_member) == expected);
    check!(written(&replaced.pending.current_per_member) == expected);
    // Then the heartbeat's own: the restarted process's join fields replace
    // the released member's, and the copy reconciles from epoch 0.
    let own = replaced
        .pending
        .then
        .as_deref()
        .expect("the heartbeat's records");
    check!(written(&own.member_metadata) == vec![("s2".to_string(), MemberRecordKind::Value)]);
    check!(written(&own.current_per_member) == vec![("s2".to_string(), MemberRecordKind::Value)]);
    let metadata_of_s2 = own
        .member_metadata
        .iter()
        .find_map(|(id, value)| (id == "s2").then_some(value.as_ref()).flatten())
        .expect("s2 member metadata");
    check!(metadata_of_s2.rack_id.as_deref() == Some("rack-b"));
    check!(metadata_of_s2.rebalance_timeout_ms == 12_345);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_heartbeat_emits_no_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let resp1 = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    let mid = resp1.member_id.clone().unwrap();
    let batches_after_join = log.batches().await.len();

    let _ = rpc::consumer_heartbeat(&handle, &mid, resp1.member_epoch, Some("t")).await;
    let batches_after_steady = log.batches().await.len();
    assert!(
        batches_after_steady == batches_after_join,
        "steady-state heartbeat should not write"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_emits_tombstone_batch() {
    let (coord, log) = make_coordinator();
    let handle = coord.get_or_create_consumer("g");
    let response = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    let mid = response.member_id.unwrap();
    let pre_leave = log.batches().await.len();

    let _ = rpc::consumer_request(
        &handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: mid,
            member_epoch: -1,
            ..Default::default()
        },
    )
    .await;
    crate::coordinator::unified::test_support::assert_next_tombstone_batch(&log, pre_leave).await;
}

/// Kafka's `consumerGroupFenceMembers`: a leave writes the leaver's
/// tombstones and the group epoch, and computes no target. The next
/// heartbeat of a survivor computes the target, writes the targets that
/// changed and the target metadata, and reconciles the survivor.
#[test]
fn a_leave_bumps_the_epoch_and_the_next_heartbeat_assigns() {
    let config = NextGenConfig::assigning_at_once();
    let topic_id = Uuid([8; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".into(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let client = crate::coordinator::unified::ClientIdentity {
        id: "client",
        host: "host",
    };
    let join = |member_id: &str| {
        crate::coordinator::unified::test_support::consumer_join_request(
            crate::coordinator::unified::test_support::ConsumerJoinSetup {
                member_id,
                ..Default::default()
            },
        )
    };
    let mut state = GroupState::new("g");
    let step = |state: &mut GroupState, req: &ConsumerGroupHeartbeatRequest| {
        step_heartbeat(
            state,
            &config,
            &metadata,
            req,
            client,
            Instant::now(),
            &RegexResolution::none(),
        )
    };
    step(&mut state, &join("m1"));
    step(&mut state, &join("m2"));
    let (epoch, target_epoch) = (state.group_epoch, state.target.epoch);
    check!((epoch, target_epoch) == (3, 3));

    let leave = step(
        &mut state,
        &ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m2".into(),
            member_epoch: -1,
            ..Default::default()
        },
    );
    check!(
        leave.pending
            == PendingRecords {
                member_metadata: vec![("m2".into(), None)],
                target_per_member: vec![("m2".into(), None)],
                current_per_member: vec![("m2".into(), None)],
                group_metadata: Some(GroupMetadataValue {
                    epoch: 4,
                    metadata_hash: state.metadata_hash(),
                }),
                ..PendingRecords::default()
            }
    );
    check!((state.group_epoch, state.target.epoch) == (4, 3));

    let m1_epoch = state.members["m1"].member_epoch;
    let next = step(
        &mut state,
        &ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: m1_epoch,
            rebalance_timeout_ms: -1,
            ..Default::default()
        },
    );
    check!(state.target.epoch == 4);
    check!(state.target.per_member["m1"][&topic_id] == vec![0, 1]);
    check!(next.pending.group_metadata == None);
    check!(
        next.pending
            .target_metadata
            .map(|value| value.assignment_epoch)
            == Some(4)
    );
    check!(
        next.pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect::<Vec<_>>()
            == vec!["m1"]
    );
}

/// KIP-848 upgrade trigger: a `ConsumerGroupHeartbeat` for a *classic*
/// group under the default `bidirectional` policy converts that group in
/// place to a next-gen consumer group that hosts the classic member. The
/// conversion atomically tombstones the classic k2 `GroupMetadata` and
/// writes the full next-gen record set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_heartbeat_upgrades_a_classic_group() {
    let (coord, log) = make_coordinator_with_topic("t", 2);

    // Seed a classic group with one classic consumer member subscribed to
    // "t". Seeding (vs a JoinGroup round-trip) keeps the test deterministic
    // and timing-free; `classic_is_convertible` only inspects protocol_type
    // and each member's protocol_metadata, both set here.
    let handle = seed_classic_member(
        &coord,
        crate::coordinator::unified::actor::test_support::ClassicMemberSetup::default(),
    );

    // A native consumer-protocol heartbeat for the same group → upgrade.
    let resp = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
    assert!(resp.error_code == codes::NONE);

    // Describe now reports 2 members: the hosted classic member and the new
    // native consumer member.
    let describe = rpc::describe(&handle).await;
    // The hosted classic member must survive the upgrade, the new native
    // consumer member must be present, and the upgrade batch tombstoned
    // the classic k2 GroupMetadata record.
    check!(describe.members.len() == 2);
    check!(describe.members.iter().any(|m| m.is_classic));
    check!(describe.members.iter().any(|m| !m.is_classic));
    check!(log.has_classic_group_metadata_tombstone("g").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_upgrade_append_keeps_the_atomic_batch_unpublished() {
    let (coord, log) = make_coordinator_with_topic("t", 1);
    let handle = seed_classic_member(
        &coord,
        crate::coordinator::unified::actor::test_support::ClassicMemberSetup::default(),
    );
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let response = crate::coordinator::unified::actor::test_support::rpc::consumer_request(
        &handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "native".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
    )
    .await;
    check!(response.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
    assert!(log.batches().await.is_empty());
}

#[test]
fn step_heartbeat_first_join_targets_all_partitions() {
    use crate::coordinator::unified::consumer_state::GroupState;
    let topic_id = Uuid([7; 16]);
    let metadata = StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_string(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    };
    let config = NextGenConfig::assigning_at_once();
    let mut group = GroupState::new("g");
    let req = ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: "m1".into(),
        member_epoch: 0,
        subscribed_topic_names: Some(vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    let step = step_heartbeat(
        &mut group,
        &config,
        &metadata,
        &req,
        crate::coordinator::unified::ClientIdentity {
            id: "client-a",
            host: "",
        },
        Instant::now(),
        &RegexResolution::none(),
    );
    // First join succeeds, advances the new group from epoch 1 to 2, targets
    // all partitions of "t", and must persist records.
    check!(step.response.error_code == 0);
    check!(step.response.member_epoch == 2);
    check!(group.target.per_member["m1"][&topic_id].clone() == vec![0, 1]);
    // Kafka's first `ConsumerGroupMetadataValue` and target assignment
    // metadata of a new group carry epoch 2.
    check!(
        (
            step.pending.group_metadata.map(|value| value.epoch),
            step.pending
                .target_metadata
                .map(|value| value.assignment_epoch)
        ) == (Some(2), Some(2))
    );
}

/// Kafka's `getOrMaybeCreateConsumerGroup` for a heartbeat that meets a
/// classic group: (label, policy, classic group members, protocol type,
/// heartbeat epoch) to (error code, error message, classic k2 tombstone
/// written). An empty classic group, such as one that only holds committed
/// offsets, is replaced whatever the policy and protocol type; a non-empty
/// one goes through `validateOnlineUpgrade`; only a joining heartbeat may
/// create a consumer group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_replaces_or_upgrades_a_classic_group_as_kafka_does() {
    use crate::coordinator::unified::{
        classic_state::{ClassicGroup as ClassicState, Member},
        config::ConsumerGroupMigrationPolicy as Policy,
        group::{CoordinatorGroup, GroupKind},
    };

    let disabled = "Cannot upgrade classic group g to consumer group because online upgrade is \
                    disabled.";
    let rows = [
        (
            "offset-only group, policy disabled",
            Policy::Disabled,
            0,
            None,
            0,
            (codes::NONE, None, true),
        ),
        (
            "empty connect group",
            Policy::Bidirectional,
            0,
            Some("connect"),
            0,
            (codes::NONE, None, true),
        ),
        (
            "live group, policy disabled",
            Policy::Disabled,
            1,
            Some("consumer"),
            0,
            (codes::GROUP_ID_NOT_FOUND, Some(disabled), false),
        ),
        (
            "live group, upgrade allowed",
            Policy::Upgrade,
            1,
            Some("consumer"),
            0,
            (codes::NONE, None, true),
        ),
        (
            "offset-only group, steady heartbeat",
            Policy::Bidirectional,
            0,
            None,
            3,
            (
                codes::GROUP_ID_NOT_FOUND,
                Some("Group g is not a consumer group."),
                false,
            ),
        ),
    ];
    for (label, policy, members, protocol_type, epoch, want) in rows {
        let (coord, log) = make_coordinator_with_topic_policy("t", 1, policy);
        let mut classic = ClassicState::new("g");
        classic.protocol_type = protocol_type.map(String::from);
        for index in 0..members {
            classic.add_member(Member::new(
                format!("classic-{index}"),
                "client",
                "127.0.0.1",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_mins(1),
                vec![("range".into(), subscription_blob(&["t"]))],
            ));
        }
        classic.generation_id = 1;
        coord.seed_classic(
            "g",
            Box::new(CoordinatorGroup::seeded(
                "g",
                GroupKind::Classic(classic),
                HashMap::new(),
            )),
        );
        let handle = coord.find("g").expect("seeded classic actor");
        let response = crate::coordinator::unified::actor::test_support::rpc::consumer_request(
            &handle,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "native".into(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                topic_partitions: Some(vec![]),
                ..Default::default()
            },
        )
        .await;
        let got = (
            response.error_code,
            response.error_message.as_deref(),
            log.has_classic_group_metadata_tombstone("g").await,
        );
        check!(got == want, "{label}");
    }
}

/// Independent success expectation for both partitions of the fixture topic.
fn identity_ok_two_partitions() -> ConsumerGroupHeartbeatResponse {
    identity_ok(IdentityExpectationSetup {
        partitions: Some(vec![PartitionIndex(0), PartitionIndex(1)]),
        ..Default::default()
    })
}
