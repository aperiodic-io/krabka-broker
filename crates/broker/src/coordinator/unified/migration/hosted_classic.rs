//! Serving hosted classic members from the consumer-group reconciler.
//!
//! A classic member hosted in a consumer group keeps speaking the classic
//! `Heartbeat`, `JoinGroup`, and `SyncGroup` RPCs. This module maps those onto
//! the consumer-group machinery as Kafka's `GroupMetadataManager` does: the
//! `JoinGroup` reconciles the member like a heartbeat, the `SyncGroup` returns
//! the partitions the member is assigned and writes nothing, and the
//! `Heartbeat` asks the member to rejoin while it has not reconciled to the
//! group's target assignment.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use bytes::Bytes;
use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest, sync_group_request::SyncGroupRequest,
};

use super::assignment::{consumer_assignment_blob, embedded_protocol_version};
use crate::{
    codes,
    coordinator::unified::{
        actor::{JoinResult, SyncResult},
        consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
        persistence_next_gen::MemberAssignmentState,
        reconciler::ReconcileInput,
    },
};

/// The classic protocol type that a consumer group serves.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// Kafka's `validateConsumerGroupMember`: the member a classic `Heartbeat` or
/// `SyncGroup` names. A static member is looked up by its instance id, and a
/// request whose member id is not the one that holds the instance id is
/// fenced.
fn validate_member<'a>(
    state: &'a ConsumerState,
    member_id: &str,
    instance_id: Option<&str>,
) -> Result<&'a MemberState, i16> {
    let Some(instance_id) = instance_id else {
        return state.members.get(member_id).ok_or(codes::UNKNOWN_MEMBER_ID);
    };
    let member = state
        .current_member_for_instance(instance_id)
        .and_then(|id| state.members.get(id))
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if member.member_id != member_id {
        return Err(codes::FENCED_INSTANCE_ID);
    }
    Ok(member)
}

/// The checks that Kafka's `classicGroupHeartbeatToConsumerGroup` and
/// `classicGroupSyncToConsumerGroup` share: the member exists and is not
/// fenced (`validateConsumerGroupMember`), it uses the classic protocol
/// (`throwIfMemberDoesNotUseClassicProtocol`, `UNKNOWN_MEMBER_ID`), and the
/// request's generation is its member epoch (`throwIfGenerationIdUnmatched`,
/// `ILLEGAL_GENERATION`).
fn validate_classic_member<'a>(
    state: &'a ConsumerState,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
) -> Result<(&'a MemberState, &'a ClassicMemberFacade), i16> {
    let member = validate_member(state, member_id, instance_id)?;
    let facade = member.classic.as_ref().ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if member.member_epoch != generation_id {
        return Err(codes::ILLEGAL_GENERATION);
    }
    Ok((member, facade))
}

/// Kafka's `classicGroupHeartbeatToConsumerGroup`.
///
/// After the member checks, the heartbeat refreshes the member's session and
/// answers `REBALANCE_IN_PROGRESS`, which asks the client to send `JoinGroup`
/// again, when the member must rejoin to reconcile:
///
/// 1. the target assignment epoch moved past the member epoch;
/// 2. the member has partitions to revoke; or
/// 3. the member waits on partitions that their previous owners released.
///
/// Otherwise it answers `NONE`. It writes nothing.
pub(crate) fn serve_classic_heartbeat(
    state: &mut ConsumerState,
    request: &HeartbeatRequest,
) -> i16 {
    let (member_id, rejoin) = match validate_classic_member(
        state,
        &request.member_id,
        request.group_instance_id.as_deref(),
        request.generation_id,
    ) {
        Ok((member, _)) => (
            member.member_id.clone(),
            member.member_epoch < state.target.epoch
                || member.assignment_state == MemberAssignmentState::UnrevokedPartitions
                || (member.assignment_state == MemberAssignmentState::UnreleasedPartitions
                    && !state.waiting_on_unreleased_partition(&member.member_id)),
        ),
        Err(code) => return code,
    };
    if let Some(member) = state.members.get_mut(&member_id) {
        member.last_seen = Instant::now();
    }
    if rejoin {
        codes::REBALANCE_IN_PROGRESS
    } else {
        codes::NONE
    }
}

/// Kafka's `classicGroupSyncToConsumerGroup`.
///
/// The member's `JoinGroup` already reconciled it, so the sync only answers:
/// after the member checks, the protocol type and name must be the ones its
/// `JoinGroup` was answered with (`throwIfClassicProtocolUnmatched`,
/// `INCONSISTENT_GROUP_PROTOCOL`), and a member whose epoch the target
/// assignment moved past must rejoin first (`throwIfRebalanceInProgress`,
/// `REBALANCE_IN_PROGRESS`), unless it is revoking partitions.
///
/// The answer echoes the request's protocol type and name and carries the
/// member's assigned partitions (`prepareAssignment`) as a
/// `ConsumerProtocolAssignment` blob, at the version of the metadata of the
/// member's first protocol. The sync writes nothing and changes nothing.
pub(crate) fn serve_classic_sync(
    state: &ConsumerState,
    request: &SyncGroupRequest,
    image: &ReconcileInput,
) -> SyncResult {
    let refused = |error_code| SyncResult {
        error_code,
        ..SyncResult::default()
    };
    let (member, facade) = match validate_classic_member(
        state,
        &request.member_id,
        request.group_instance_id.as_deref(),
        request.generation_id,
    ) {
        Ok(found) => found,
        Err(code) => return refused(code),
    };
    let first_protocol = facade.supported_protocols.first();
    if request
        .protocol_type
        .as_deref()
        .is_some_and(|protocol_type| protocol_type != CONSUMER_PROTOCOL_TYPE)
        || request
            .protocol_name
            .as_deref()
            .is_some_and(|name| first_protocol.is_none_or(|(first, _)| first.as_str() != name))
    {
        return refused(codes::INCONSISTENT_GROUP_PROTOCOL);
    }
    if state.target.epoch > member.member_epoch
        && member.assignment_state != MemberAssignmentState::UnrevokedPartitions
    {
        return refused(codes::REBALANCE_IN_PROGRESS);
    }
    // `prepareAssignment` reads the version off the first protocol's
    // metadata, and a blob without a version is an `IllegalStateException`.
    let Some(version) = first_protocol
        .and_then(|(_, metadata)| embedded_protocol_version(metadata))
        .filter(|version| *version >= 0)
    else {
        return refused(codes::UNKNOWN_SERVER_ERROR);
    };
    SyncResult {
        error_code: codes::NONE,
        assignment: consumer_assignment_blob(&member.assigned_partitions, image, version),
        protocol_type: request.protocol_type.clone(),
        protocol_name: request.protocol_name.clone(),
    }
}

/// Kafka's `ConsumerGroup.supportsClassicProtocols` for a group with members:
/// the protocol type is `consumer`, and one of `protocols` is supported by
/// every member that uses the classic protocol.
pub(crate) fn supports_classic_protocols(
    state: &ConsumerState,
    protocol_type: &str,
    protocols: &HashSet<&str>,
) -> bool {
    if protocol_type != CONSUMER_PROTOCOL_TYPE {
        return false;
    }
    if state.members.is_empty() {
        return !protocols.is_empty();
    }
    protocols.iter().any(|name| {
        state
            .members
            .values()
            .filter_map(|member| member.classic.as_ref())
            .all(|facade| {
                facade
                    .supported_protocols
                    .iter()
                    .any(|(supported, _)| supported == name)
            })
    })
}

/// A classic `JoinGroup` for a member of a consumer group, as Kafka's
/// `classicGroupJoinToConsumerGroup` builds its `updatedMember` from it.
pub(crate) struct ClassicMemberRegistration {
    pub member_id: String,
    pub subscription_topics: HashSet<String>,
    /// The rack of the member's `ConsumerProtocolSubscription`, if any.
    pub rack_id: Option<String>,
    pub protocols: Vec<(String, Bytes)>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Duration,
    /// The member's rebalance timeout. The caller resolves Kafka's `-1`
    /// sentinel (`maybeUpdateRebalanceTimeoutMs(ofSentinel(..))`) to the
    /// stored timeout.
    pub rebalance_timeout: Duration,
    pub instance_id: Option<String>,
}

/// Creates or updates a classic member of a consumer group, as Kafka's
/// `classicGroupJoinToConsumerGroup` builds its `updatedMember`.
///
/// The member keeps its epochs, its assignment, its server assignor
/// (`maybeUpdateServerAssignorName` of nothing), and its pattern, which the
/// caller drops through Kafka's regex update. An absent instance id or rack
/// keeps the stored one (`maybeUpdate*`). The topic names,
/// client and classic metadata are replaced. A new member starts at epoch 0,
/// as Kafka's `getOrMaybeCreateMember` creates it.
pub(crate) fn upsert_classic_member(
    state: &mut ConsumerState,
    registration: ClassicMemberRegistration,
) {
    let ClassicMemberRegistration {
        member_id,
        subscription_topics,
        rack_id,
        protocols,
        client_id,
        client_host,
        session_timeout,
        rebalance_timeout,
        instance_id,
    } = registration;
    let existing = state.members.get(&member_id);
    let member = MemberState {
        member_id: member_id.clone(),
        instance_id: instance_id.or_else(|| existing.and_then(|m| m.instance_id.clone())),
        rack_id: rack_id.or_else(|| existing.and_then(|m| m.rack_id.clone())),
        client_id,
        client_host,
        subscribed_topic_names: subscription_topics,
        subscribed_topic_regex: existing.and_then(|m| m.subscribed_topic_regex.clone()),
        server_assignor: existing.and_then(|m| m.server_assignor.clone()),
        rebalance_timeout,
        member_epoch: existing.map_or(0, |m| m.member_epoch),
        previous_member_epoch: existing.map_or(0, |m| m.previous_member_epoch),
        assignment_state: existing.map_or(MemberAssignmentState::Stable, |m| m.assignment_state),
        assigned_partitions: existing
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default(),
        partitions_pending_revocation: existing
            .map(|m| m.partitions_pending_revocation.clone())
            .unwrap_or_default(),
        assignment_epochs: existing
            .map(|m| m.assignment_epochs.clone())
            .unwrap_or_default(),
        last_seen: Instant::now(),
        classic: Some(ClassicMemberFacade {
            supported_protocols: protocols,
            session_timeout,
        }),
    };
    state.add_or_update_member(member);
}

/// Builds the `JoinGroup` result for a hosted classic member, as Kafka's
/// `classicGroupJoinToConsumerGroup` does.
///
/// The coordinator computes the assignment, so the result names no leader and
/// lists no members. A classic client then joins as a follower and sends an
/// empty `SyncGroup`, which returns its assignment. A client that finds its own
/// id in `leader` runs its assignor over the member list. An entry with no
/// subscription metadata makes the Java client fail to parse it.
///
/// The generation is the member epoch, and the protocol name is the member's
/// first protocol. The client sends both back in `SyncGroup`, and the
/// generation in `Heartbeat` and `OffsetCommit`, whose fence compares it with
/// the member epoch.
pub(crate) fn build_hosted_classic_join_result(member: &MemberState) -> JoinResult {
    JoinResult {
        error_code: codes::NONE,
        generation_id: member.member_epoch,
        protocol_type: Some(CONSUMER_PROTOCOL_TYPE.into()),
        protocol_name: member
            .classic
            .as_ref()
            .and_then(|facade| facade.supported_protocols.first())
            .map(|(name, _)| name.clone()),
        member_id: member.member_id.clone(),
        ..JoinResult::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::check;
    use krabka_protocol::primitives::uuid::Uuid;

    use super::*;
    use crate::coordinator::unified::actor::test_support::topic_reconcile_input;

    const TOPIC: Uuid = Uuid([7; 16]);
    const TARGET_EPOCH: i32 = 5;

    /// A member at the target epoch that holds partition 0 of `t`, with a
    /// classic facade whose first protocol, `range`, carries a version-1
    /// subscription prefix, or no facade when `classic` is unset.
    fn member(id: &str, classic: bool) -> MemberState {
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: ["t".to_string()].into(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_secs(60),
            member_epoch: TARGET_EPOCH,
            previous_member_epoch: TARGET_EPOCH - 1,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: [(TOPIC, vec![0])].into(),
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs: HashMap::new(),
            last_seen: Instant::now(),
            classic: classic.then(|| ClassicMemberFacade {
                supported_protocols: vec![
                    ("range".into(), Bytes::from_static(&[0, 1, 0, 0])),
                    ("roundrobin".into(), Bytes::from_static(&[0, 1, 0, 0])),
                ],
                session_timeout: Duration::from_secs(45),
            }),
        }
    }

    /// A group at target epoch 5 with `members`, each targeted at what it
    /// holds plus partition 1 for `m`.
    fn group(members: Vec<MemberState>) -> ConsumerState {
        let mut state = ConsumerState::new("g");
        state.group_epoch = TARGET_EPOCH;
        state.target.epoch = TARGET_EPOCH;
        for member in members {
            let target = if member.member_id == "m" {
                [(TOPIC, vec![0, 1])].into()
            } else {
                member.assigned_partitions.clone()
            };
            state
                .target
                .per_member
                .insert(member.member_id.clone(), target);
            state.add_or_update_member(member);
        }
        state
    }

    fn image() -> ReconcileInput {
        topic_reconcile_input("t", TOPIC, 2)
    }

    fn sync_request(member_id: &str, generation_id: i32) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            generation_id,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            ..SyncGroupRequest::default()
        }
    }

    fn refused(error_code: i16) -> SyncResult {
        SyncResult {
            error_code,
            ..SyncResult::default()
        }
    }

    /// Kafka's `classicGroupSyncToConsumerGroup`: (label, the group's
    /// members, the request) to the whole answer. A served sync returns the
    /// partitions the member is ASSIGNED, not its target, at the version of
    /// its first protocol, and echoes the request's protocol type and name.
    #[test]
    fn classic_sync_follows_kafka() {
        let assigned_v1 = consumer_assignment_blob(&[(TOPIC, vec![0])].into(), &image(), 1);
        let behind = |mut m: MemberState, state| {
            m.member_epoch = TARGET_EPOCH - 1;
            m.assignment_state = state;
            m
        };
        let static_member = |mut m: MemberState| {
            m.instance_id = Some("instance-1".into());
            m
        };
        let rows: Vec<(&str, Vec<MemberState>, SyncGroupRequest, SyncResult)> = vec![
            (
                "a member at the target epoch",
                vec![member("m", true)],
                sync_request("m", TARGET_EPOCH),
                SyncResult {
                    error_code: codes::NONE,
                    assignment: assigned_v1.clone(),
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                },
            ),
            (
                "a request without protocol type and name",
                vec![member("m", true)],
                SyncGroupRequest {
                    protocol_type: None,
                    protocol_name: None,
                    ..sync_request("m", TARGET_EPOCH)
                },
                SyncResult {
                    error_code: codes::NONE,
                    assignment: assigned_v1.clone(),
                    protocol_type: None,
                    protocol_name: None,
                },
            ),
            (
                "a member revoking partitions after the target moved",
                vec![behind(
                    member("m", true),
                    MemberAssignmentState::UnrevokedPartitions,
                )],
                sync_request("m", TARGET_EPOCH - 1),
                SyncResult {
                    error_code: codes::NONE,
                    assignment: assigned_v1,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                },
            ),
            (
                "an absent member",
                vec![member("m", true)],
                sync_request("nobody", TARGET_EPOCH),
                refused(codes::UNKNOWN_MEMBER_ID),
            ),
            (
                "a member of the consumer protocol",
                vec![member("m", false)],
                sync_request("m", TARGET_EPOCH),
                refused(codes::UNKNOWN_MEMBER_ID),
            ),
            (
                "an unknown instance id",
                vec![member("m", true)],
                SyncGroupRequest {
                    group_instance_id: Some("instance-1".into()),
                    ..sync_request("m", TARGET_EPOCH)
                },
                refused(codes::UNKNOWN_MEMBER_ID),
            ),
            (
                "an instance id another member holds",
                vec![static_member(member("m", true))],
                SyncGroupRequest {
                    group_instance_id: Some("instance-1".into()),
                    ..sync_request("other", TARGET_EPOCH)
                },
                refused(codes::FENCED_INSTANCE_ID),
            ),
            (
                "a generation that is not the member epoch",
                vec![member("m", true)],
                sync_request("m", TARGET_EPOCH + 1),
                refused(codes::ILLEGAL_GENERATION),
            ),
            (
                "another protocol type",
                vec![member("m", true)],
                SyncGroupRequest {
                    protocol_type: Some("connect".into()),
                    ..sync_request("m", TARGET_EPOCH)
                },
                refused(codes::INCONSISTENT_GROUP_PROTOCOL),
            ),
            (
                "a protocol other than the member's first",
                vec![member("m", true)],
                SyncGroupRequest {
                    protocol_name: Some("roundrobin".into()),
                    ..sync_request("m", TARGET_EPOCH)
                },
                refused(codes::INCONSISTENT_GROUP_PROTOCOL),
            ),
            (
                "a stable member that the target moved past",
                vec![behind(member("m", true), MemberAssignmentState::Stable)],
                sync_request("m", TARGET_EPOCH - 1),
                refused(codes::REBALANCE_IN_PROGRESS),
            ),
        ];
        for (label, members, request, want) in rows {
            let state = group(members);

            check!(
                serve_classic_sync(&state, &request, &image()) == want,
                "{label}"
            );
        }
    }

    /// Kafka's `checkAssignmentVersion`: the assignment is written at the
    /// version of the member's first protocol, a version above the highest
    /// the schema knows as the highest, and a negative version is an error.
    #[test]
    fn classic_sync_writes_the_assignment_at_the_members_protocol_version() {
        for (prefix, want) in [(0_i16, Some(0_i16)), (3, Some(3)), (7, Some(3)), (-1, None)] {
            let mut m = member("m", true);
            let mut metadata = prefix.to_be_bytes().to_vec();
            metadata.extend([0, 0, 0, 0]);
            m.classic = Some(ClassicMemberFacade {
                supported_protocols: vec![("range".into(), Bytes::from(metadata))],
                session_timeout: Duration::from_secs(45),
            });
            let state = group(vec![m]);

            let got = serve_classic_sync(&state, &sync_request("m", TARGET_EPOCH), &image());

            let want = want.map_or_else(
                || refused(codes::UNKNOWN_SERVER_ERROR),
                |version| SyncResult {
                    error_code: codes::NONE,
                    assignment: consumer_assignment_blob(
                        &[(TOPIC, vec![0])].into(),
                        &image(),
                        version,
                    ),
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                },
            );
            check!(got == want, "version prefix {prefix}");
        }
    }

    /// Kafka's `classicGroupHeartbeatToConsumerGroup`: (label, the member's
    /// epoch and state, whether another member still holds the partition the
    /// member waits for) to the answer. The member rejoins when the target
    /// moved past it, when it revokes, and when the partition it waits for
    /// is free.
    #[test]
    fn classic_heartbeat_asks_for_a_rejoin_as_kafka_does() {
        let rows = [
            (
                "reconciled",
                TARGET_EPOCH,
                MemberAssignmentState::Stable,
                false,
                codes::NONE,
            ),
            (
                "the target moved past the member",
                TARGET_EPOCH - 1,
                MemberAssignmentState::Stable,
                false,
                codes::REBALANCE_IN_PROGRESS,
            ),
            (
                "partitions to revoke",
                TARGET_EPOCH,
                MemberAssignmentState::UnrevokedPartitions,
                false,
                codes::REBALANCE_IN_PROGRESS,
            ),
            (
                "waiting on a partition another member holds",
                TARGET_EPOCH,
                MemberAssignmentState::UnreleasedPartitions,
                true,
                codes::NONE,
            ),
            (
                "the partition it waited on is free",
                TARGET_EPOCH,
                MemberAssignmentState::UnreleasedPartitions,
                false,
                codes::REBALANCE_IN_PROGRESS,
            ),
        ];
        for (label, epoch, assignment_state, held_elsewhere, want) in rows {
            let mut m = member("m", true);
            m.member_epoch = epoch;
            m.assignment_state = assignment_state;
            let mut other = member("other", false);
            other.assigned_partitions = if held_elsewhere {
                [(TOPIC, vec![1])].into()
            } else {
                HashMap::new()
            };
            let mut state = group(vec![m, other]);
            let request = HeartbeatRequest {
                group_id: "g".into(),
                member_id: "m".into(),
                generation_id: epoch,
                ..HeartbeatRequest::default()
            };

            check!(
                serve_classic_heartbeat(&mut state, &request) == want,
                "{label}"
            );
        }
    }

    /// The member checks of the classic `Heartbeat` are the sync's: (label,
    /// request) to the error.
    #[test]
    fn classic_heartbeat_refuses_what_kafka_refuses() {
        let mut static_m = member("m", true);
        static_m.instance_id = Some("instance-1".into());
        let request = |member_id: &str, instance: Option<&str>, generation_id| HeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            generation_id,
            group_instance_id: instance.map(Into::into),
            ..HeartbeatRequest::default()
        };
        let rows = [
            (
                "an absent member",
                request("nobody", None, TARGET_EPOCH),
                codes::UNKNOWN_MEMBER_ID,
            ),
            (
                "a member of the consumer protocol",
                request("native", None, TARGET_EPOCH),
                codes::UNKNOWN_MEMBER_ID,
            ),
            (
                "an unknown instance id",
                request("m", Some("instance-2"), TARGET_EPOCH),
                codes::UNKNOWN_MEMBER_ID,
            ),
            (
                "an instance id another member holds",
                request("other", Some("instance-1"), TARGET_EPOCH),
                codes::FENCED_INSTANCE_ID,
            ),
            (
                "a generation that is not the member epoch",
                request("m", Some("instance-1"), 0),
                codes::ILLEGAL_GENERATION,
            ),
        ];
        for (label, request, want) in rows {
            let mut state = group(vec![static_m.clone(), member("native", false)]);

            check!(
                serve_classic_heartbeat(&mut state, &request) == want,
                "{label}"
            );
        }
    }

    /// Kafka's `supportsClassicProtocols`: (label, protocol type, protocols)
    /// to whether a group whose classic members both support `range`, and one
    /// of them `roundrobin`, accepts them.
    #[test]
    fn classic_protocols_must_be_supported_by_every_classic_member() {
        let mut narrow = member("narrow", true);
        narrow.classic = Some(ClassicMemberFacade {
            supported_protocols: vec![("range".into(), Bytes::from_static(&[0, 1]))],
            session_timeout: Duration::from_secs(45),
        });
        let state = group(vec![member("m", true), narrow, member("native", false)]);
        for (label, protocol_type, protocols, want) in [
            ("a shared protocol", "consumer", vec!["range"], true),
            (
                "one shared among others",
                "consumer",
                vec!["sticky", "range"],
                true,
            ),
            (
                "a protocol one member lacks",
                "consumer",
                vec!["roundrobin"],
                false,
            ),
            ("another protocol type", "connect", vec!["range"], false),
        ] {
            let protocols: HashSet<&str> = protocols.into_iter().collect();

            check!(
                supports_classic_protocols(&state, protocol_type, &protocols) == want,
                "{label}"
            );
        }
    }
}
