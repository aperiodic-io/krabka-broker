//! Hydration of a next-gen consumer group from the records replayed off
//! `__consumer_offsets`.
//!
//! A coordinator failover rebuilds a group from its persisted k3/k5/k7/k8
//! records, and this is where that seed becomes live [`GroupState`]: members,
//! their epochs, their target and current assignments, and the
//! `ClassicMemberFacade` of any classic member the group hosts after a KIP-848
//! upgrade.
//!
//! A hosted classic member needs nothing beyond Kafka's own schema: its k5
//! record carries the classic metadata, and its k7 target and k8 current
//! assignment are everything its `SyncGroup` and `Heartbeat` read.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use krabka_protocol::primitives::uuid::Uuid;

use super::{FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS};
use crate::coordinator::unified::{
    GroupSeed,
    consumer_state::{ClassicMemberFacade, GroupState, MemberState},
    persistence_next_gen::AssignedTopicPartitions,
};

fn topic_partition_map(partitions: Vec<AssignedTopicPartitions>) -> HashMap<Uuid, Vec<i32>> {
    partitions
        .into_iter()
        .map(|tp| (tp.topic_id, tp.partitions))
        .collect()
}

pub(super) fn apply_seed(state: &mut GroupState, seed: GroupSeed) {
    state.mark_persisted();
    state.group_epoch = seed.group_epoch;
    state.record_metadata_hash(seed.metadata_hash);
    state.target.epoch = seed.target_epoch;
    state.record_assignment(seed.assignment_timestamp_ms);
    state.set_has_subscription_metadata_record(seed.has_subscription_metadata_record);
    // What each regular expression resolved to, as the group last recorded it:
    // the members keep the topics of their regex subscriptions across the
    // failover, without a heartbeat that carries the pattern.
    for (regex, resolved) in seed.resolved_regexes {
        state.set_resolved_regex(regex, resolved.into());
    }
    for (mid, meta) in seed.members {
        let mut sub = std::collections::HashSet::new();
        for n in meta.subscribed_topic_names {
            sub.insert(n);
        }
        // KIP-848 migration: a k5 record carrying a `classic` block describes a
        // classic-protocol member hosted in an upgraded group. Rebuild its
        // `ClassicMemberFacade` so the member keeps speaking
        // `JoinGroup`/`SyncGroup`/`Heartbeat` after a coordinator failover; a
        // native consumer-protocol member has `classic == None`.
        let classic = meta.classic.as_ref().map(|c| ClassicMemberFacade {
            supported_protocols: c.supported_protocols.clone(),
            session_timeout: Duration::from_millis(
                u64::try_from(c.session_timeout_ms.max(0)).unwrap_or(FALLBACK_SESSION_TIMEOUT_MS),
            ),
        });
        state.add_or_update_member(MemberState {
            instance_id: meta.instance_id,
            rack_id: meta.rack_id,
            client_id: meta.client_id,
            client_host: meta.client_host,
            subscribed_topic_names: sub,
            // Kafka's `isNotEmpty` gate: an empty pattern is no regex.
            subscribed_topic_regex: meta
                .subscribed_topic_regex
                .filter(|regex| !regex.is_empty()),
            server_assignor: meta.server_assignor,
            rebalance_timeout: Duration::from_millis(
                u64::try_from(meta.rebalance_timeout_ms.max(0))
                    .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
            ),
            classic,
            ..MemberState::empty(mid.clone(), Instant::now())
        });
    }
    crate::coordinator::unified::seeds::hydrate_member_epochs!(state, seed; m, cur {
            m.assignment_state = cur.state;
            // Kafka's `ConsumerGroupMember.Builder.updateWith` reads each
            // partition's assignment epoch through
            // `Utils.assignmentFromTopicPartitions`, with the member epoch as
            // the default.
            for tp in cur
                .assigned_partitions
                .iter()
                .chain(&cur.partitions_pending_revocation)
            {
                m.assignment_epochs
                    .entry(tp.topic_id)
                    .or_default()
                    .extend(tp.epochs(cur.member_epoch));
            }
            for tp in cur.assigned_partitions {
                m.assigned_partitions.insert(tp.topic_id, tp.partitions);
            }
            for tp in cur.partitions_pending_revocation {
                m.partitions_pending_revocation
                    .insert(tp.topic_id, tp.partitions);
            }
    });
    // The k7 target the group last installed. Without it the group would come
    // back with `target.epoch` set but no per-member target at all, so the
    // first RPC after the failover would hand a member an empty assignment.
    for (mid, target) in seed.target_per_member {
        state
            .target
            .per_member
            .insert(mid, topic_partition_map(target.topic_partitions));
    }
    // Kafka arms the rebalance timeout of every loaded member that still has
    // partitions to revoke (`GroupMetadataManager.onLoaded`).
    let now = Instant::now();
    let member_ids: Vec<String> = state.members.keys().cloned().collect();
    for member_id in member_ids {
        state.track_rebalance_timeout(&member_id, now);
    }
    // Kafka replays the `MetadataHash` of `ConsumerGroupMetadataValue` into
    // the group, and a loaded group refreshes its metadata at the first
    // heartbeat, since its refresh deadline starts expired
    // (`DeadlineAndEpoch.EMPTY`). That heartbeat computes the hash from the
    // current image and bumps the epoch only when it differs from the stored
    // one, so a subscribed topic that changed while no coordinator held the
    // group reaches its next target, and an unchanged one keeps the stored
    // target.
    state.request_metadata_refresh();
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_protocol::owned::heartbeat_request::HeartbeatRequest;

    use super::*;
    use crate::{
        codes,
        coordinator::unified::{
            consumer_state::ResolvedRegularExpression,
            migration::serve_classic_heartbeat,
            persistence_next_gen::{
                ClassicMemberMetadata, CurrentMemberAssignmentValue, CurrentTopicPartitions,
                MemberAssignmentState, MemberMetadataValue, RegularExpressionValue,
                TargetAssignmentMemberValue,
            },
            reconciler::ReconcileInput,
            test_support::MemberEpoch,
        },
    };

    const TOPIC: Uuid = Uuid([7; 16]);

    fn image() -> ReconcileInput {
        crate::coordinator::unified::actor::test_support::topic_reconcile_input("t", TOPIC, 2)
    }

    /// The replayed client identity and timeout shared by the member seed fixtures.
    fn seeded_member_metadata(
        topics: &[&str],
        regex: Option<&str>,
        classic: Option<ClassicMemberMetadata>,
    ) -> MemberMetadataValue {
        MemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".to_string(),
            client_host: "/127.0.0.1".to_string(),
            subscribed_topic_names: topics.iter().map(|topic| (*topic).to_string()).collect(),
            subscribed_topic_regex: regex.map(str::to_string),
            server_assignor: None,
            rebalance_timeout_ms: 60_000,
            classic,
        }
    }

    /// The records a coordinator failover replays for a group at epoch 5 that
    /// hosts one classic member: a k5 with the classic sub-state, a k7 target
    /// of `target`, and a k8 current assignment at `member_epoch` in `state`
    /// of `assigned` with `pending` awaiting revocation.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct HostedCurrentSetup {
        #[default(MemberEpoch(5))]
        epoch: MemberEpoch,
        #[default(MemberAssignmentState::Stable)]
        state: MemberAssignmentState,
    }

    #[derive(krabka_macros::FieldDefaults)]
    struct HostedClassicSetup {
        #[default(vec![PartitionIndex(0), PartitionIndex(1)])]
        target: Vec<PartitionIndex>,
        current: HostedCurrentSetup,
        #[default(vec![PartitionIndex(0), PartitionIndex(1)])]
        assigned: Vec<PartitionIndex>,
        pending: Vec<PartitionIndex>,
    }

    fn hosted_classic_seed(setup: HostedClassicSetup) -> GroupSeed {
        let HostedClassicSetup {
            target,
            current:
                HostedCurrentSetup {
                    epoch: member_epoch,
                    state,
                },
            assigned,
            pending,
        } = setup;
        GroupSeed {
            group_epoch: 5,
            target_epoch: 5,
            assignment_timestamp_ms: 0,
            members: [(
                "m".to_string(),
                seeded_member_metadata(
                    &["t"],
                    None,
                    Some(ClassicMemberMetadata {
                        session_timeout_ms: 30_000,
                        supported_protocols: vec![(
                            "range".to_string(),
                            Bytes::from_static(b"meta"),
                        )],
                    }),
                ),
            )]
            .into(),
            target_per_member: [(
                "m".to_string(),
                TargetAssignmentMemberValue {
                    topic_partitions: vec![AssignedTopicPartitions {
                        topic_id: TOPIC,
                        partitions: target.into_iter().map(|index| index.0).collect(),
                    }],
                },
            )]
            .into(),
            current_per_member: [(
                "m".to_string(),
                CurrentMemberAssignmentValue {
                    member_epoch: member_epoch.0,
                    previous_member_epoch: member_epoch.0 - 1,
                    state,
                    assigned_partitions: vec![CurrentTopicPartitions {
                        topic_id: TOPIC,
                        partitions: assigned.into_iter().map(|index| index.0).collect(),
                        assignment_epochs: None,
                    }],
                    partitions_pending_revocation: vec![CurrentTopicPartitions {
                        topic_id: TOPIC,
                        partitions: pending.into_iter().map(|index| index.0).collect(),
                        assignment_epochs: None,
                    }],
                },
            )]
            .into(),
            ..GroupSeed::default()
        }
    }

    #[test]
    fn seed_restores_the_per_member_target() {
        let mut state = GroupState::new("g");
        apply_seed(
            &mut state,
            hosted_classic_seed(HostedClassicSetup::default()),
        );

        let restored: HashMap<Uuid, Vec<i32>> = [(TOPIC, vec![0, 1])].into();
        check!(state.target.epoch == 5);
        check!(state.target.per_member.get("m") == Some(&restored));
    }

    /// A hosted classic member's `Heartbeat` reads only what the k7 and k8
    /// records restore: (k8 epoch and state, k8 assigned, k8 pending
    /// revocation) to the answer of Kafka's
    /// `classicGroupHeartbeatToConsumerGroup` after a failover.
    #[test]
    fn seeded_hosted_classic_heartbeat_follows_the_restored_assignment() {
        for (current, assigned, pending, want) in [
            (
                (5, MemberAssignmentState::Stable),
                vec![0, 1],
                vec![],
                codes::NONE,
            ),
            (
                (4, MemberAssignmentState::Stable),
                vec![0],
                vec![],
                codes::REBALANCE_IN_PROGRESS,
            ),
            (
                (5, MemberAssignmentState::UnrevokedPartitions),
                vec![0],
                vec![1],
                codes::REBALANCE_IN_PROGRESS,
            ),
            (
                (5, MemberAssignmentState::UnreleasedPartitions),
                vec![0],
                vec![],
                codes::REBALANCE_IN_PROGRESS,
            ),
        ] {
            let mut state = GroupState::new("g");
            apply_seed(
                &mut state,
                hosted_classic_seed(HostedClassicSetup {
                    current: HostedCurrentSetup {
                        epoch: MemberEpoch(current.0),
                        state: current.1,
                    },
                    assigned: assigned.iter().copied().map(PartitionIndex).collect(),
                    pending: pending.iter().copied().map(PartitionIndex).collect(),
                    ..Default::default()
                }),
            );
            let request = HeartbeatRequest {
                group_id: "g".into(),
                member_id: "m".into(),
                generation_id: current.0,
                ..HeartbeatRequest::default()
            };

            check!(
                serve_classic_heartbeat(&mut state, &request) == want,
                "current = {current:?}, assigned = {assigned:?}, pending = {pending:?}"
            );
        }
    }

    /// The seed of a group whose member `m` subscribes to `orders` by name and
    /// to `pay.*` by regex, with the resolution of `pay.*` when `resolved` has
    /// one. That is what a coordinator failover replays.
    fn regex_seed(resolved: Option<&[&str]>) -> GroupSeed {
        GroupSeed {
            group_epoch: 5,
            target_epoch: 5,
            assignment_timestamp_ms: 0,
            members: [(
                "m".to_string(),
                seeded_member_metadata(&["orders"], Some("pay.*"), None),
            )]
            .into(),
            resolved_regexes: resolved
                .into_iter()
                .map(|topics| {
                    (
                        "pay.*".to_string(),
                        RegularExpressionValue {
                            topics: topics.iter().map(|topic| (*topic).to_string()).collect(),
                            version: 7,
                            timestamp_ms: 1_000,
                        },
                    )
                })
                .collect(),
            ..GroupSeed::default()
        }
    }

    /// Kafka replays the topics a regex resolved to
    /// (`ConsumerGroupRegularExpression`), so a group that a failover loads
    /// still subscribes to them: `OffsetDelete` and offset expiration see
    /// them, and no heartbeat that carries the pattern is needed. A regex with
    /// no record is not resolved, which leaves the group subscribed to every
    /// topic until it is. A heartbeat that drops the regex takes its topics
    /// away.
    #[test]
    fn a_replayed_regex_keeps_the_topics_it_resolved_to() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

        use crate::coordinator::unified::{
            ClientIdentity,
            actor::{
                member_state::update_member_state,
                offset_delete::{SubscribedTopics, offset_delete_guard},
                regex_resolution::RegexResolution,
                test_support::StaticMetadata,
            },
            config::NextGenConfig,
            group::{CoordinatorGroup, GroupKind},
        };

        // (label, the replayed resolution, the pattern of a heartbeat after
        // the replay, the group's subscribed topics)
        type Row<'a> = (
            &'a str,
            Option<&'a [&'a str]>,
            Option<&'a str>,
            SubscribedTopics,
        );
        let named = |topics: &[&str]| {
            SubscribedTopics::Named(topics.iter().map(|topic| (*topic).to_string()).collect())
        };
        let rows: [Row<'_>; 4] = [
            (
                "replayed with its resolution",
                Some(&["payments"]),
                None,
                named(&["orders", "payments"]),
            ),
            (
                "the resolution stands when a heartbeat does not carry the pattern",
                Some(&["payments", "payouts"]),
                None,
                named(&["orders", "payments", "payouts"]),
            ),
            (
                "replayed without a resolution",
                None,
                None,
                SubscribedTopics::All,
            ),
            (
                "a heartbeat with the empty pattern drops the regex",
                Some(&["payments"]),
                Some(""),
                named(&["orders"]),
            ),
        ];
        for (label, resolved, heartbeat_pattern, want) in rows {
            let mut state = GroupState::new("g");
            apply_seed(&mut state, regex_seed(resolved));
            if heartbeat_pattern.is_some() || resolved.is_some() {
                update_member_state(
                    &mut state,
                    &NextGenConfig::assigning_at_once(),
                    &StaticMetadata { input: image() },
                    &ConsumerGroupHeartbeatRequest {
                        group_id: "g".into(),
                        member_id: "m".into(),
                        member_epoch: 5,
                        subscribed_topic_regex: heartbeat_pattern.map(str::to_owned),
                        rebalance_timeout_ms: 60_000,
                        ..Default::default()
                    },
                    ClientIdentity {
                        id: "c",
                        host: "/127.0.0.1",
                    },
                    Instant::now(),
                    &RegexResolution::none(),
                )
                .unwrap();
            }
            let group = CoordinatorGroup::seeded("g", GroupKind::Consumer(state), HashMap::new());

            check!(offset_delete_guard(&group) == Ok(want), "{label}");
        }
    }

    /// The seed hands the group the resolution of a regex as it was recorded,
    /// and the member the pattern.
    #[test]
    fn the_seed_restores_the_resolution_and_the_pattern() {
        let mut state = GroupState::new("g");
        apply_seed(&mut state, regex_seed(Some(&["payments"])));

        check!(
            state.resolved_regex("pay.*")
                == Some(&ResolvedRegularExpression {
                    topics: ["payments".to_string()].into(),
                    version: 7,
                    timestamp_ms: 1_000,
                })
        );
        check!(state.members["m"].subscribed_topic_regex.as_deref() == Some("pay.*"));
        let topics: Vec<&String> = state.regex_topics(&state.members["m"]).collect();
        check!(topics == ["payments"]);
    }
}
