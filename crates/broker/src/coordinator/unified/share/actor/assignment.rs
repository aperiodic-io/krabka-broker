//! Recomputation of a share group's target assignment. It runs the share-group
//! assignor over the current membership and the latest metadata snapshot, and
//! it sits apart from the actor loop because it is pure, synchronous
//! state-machine work with no log or persister access.

use {
    crate::coordinator::unified::{
        actor::MetadataProvider,
        assignor::{GroupSpec, MemberSubscription, SubscriptionShape, subscription_type},
        reconciler::ReconcileInput,
        share::{
            assignor::ShareGroupAssignor,
            state::{ShareGroupState, ShareMemberState},
        },
        wall_clock_ms,
    },
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{HashMap, HashSet},
        time::Duration,
    },
};

/// The group epoch cannot move past `i32::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EpochExhausted;

/// Steps 1 and 2 of Kafka's `GroupMetadataManager.shareGroupHeartbeat` after
/// the member update: the epoch bump and the target assignment.
///
/// The epoch bumps when the heartbeat changed the member's subscription
/// (`subscription_changed`), when the subscribed topics changed in the
/// metadata image (Kafka's metadata hash), or, for a group whose target is
/// current, when initialized partitions of a subscribed topic are not
/// assigned yet (`initializedAssignmentPending`). The target is recomputed
/// whenever the group epoch is ahead of the target epoch, from the previous
/// target and over the initialized partitions only
/// (`withTopicAssignablePartitionsMap`), unless the group's
/// `assignment_interval` since the last target has not elapsed
/// (`canComputeNextTargetAssignment`).
///
/// It returns the members whose target changed when it computed a target.
///
/// # Errors
///
/// Returns [`EpochExhausted`] when the epoch must move and cannot.
pub(super) fn reconcile(
    state: &mut ShareGroupState,
    metadata: &dyn MetadataProvider,
    assignment_interval: Duration,
    subscription_changed: bool,
) -> Result<Option<Vec<String>>, EpochExhausted> {
    let input = metadata.snapshot();
    let metadata_hash = metadata_hash(state, &input);
    let metadata_changed = metadata_hash != state.metadata_hash;
    let pending = state.target.epoch >= state.group_epoch && initialized_assignment_pending(state);
    if (subscription_changed || metadata_changed || pending) && !state.bump_epoch() {
        return Err(EpochExhausted);
    }
    state.metadata_hash = metadata_hash;
    if state.target.epoch >= state.group_epoch
        || state.assignment_delayed(assignment_interval, wall_clock_ms())
    {
        return Ok(None);
    }

    let members: Vec<MemberSubscription> = state
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(m, &input),
            assigned_partitions: state
                .target
                .per_member
                .get(&m.member_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    // Kafka's `ModernGroup.subscriptionType`: share groups subscribe by name
    // only.
    let shapes: Vec<SubscriptionShape<'_>> = state
        .members
        .values()
        .map(|m| SubscriptionShape {
            topic_names: &m.subscribed_topic_names,
            topic_regex: None,
            regex_topic_names: Vec::new(),
        })
        .collect();
    let group = GroupSpec {
        subscription_type: subscription_type(&shapes),
        members,
    };
    let topics = input.topic_metadata();
    let mut assignable: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for tp in &state.initialized {
        assignable
            .entry(tp.topic_id)
            .or_default()
            .insert(tp.partition);
    }
    let assignment = ShareGroupAssignor.assign(&group, &topics, Some(&assignable));
    Ok(Some(state.install_target(assignment, wall_clock_ms())))
}

/// Kafka's `ModernGroup.computeMetadataHash` over the topics that the
/// members of `state` subscribe to.
pub(super) fn metadata_hash(state: &ShareGroupState, input: &ReconcileInput) -> i64 {
    input.metadata_hash(
        state
            .members
            .values()
            .flat_map(|m| m.subscribed_topic_names.iter().map(String::as_str)),
    )
}

/// Kafka's `GroupMetadataManager.initializedAssignmentPending`: whether a
/// subscribed topic has initialized partitions that differ from the ones the
/// target assigns.
fn initialized_assignment_pending(state: &ShareGroupState) -> bool {
    if state.members.is_empty() || state.initialized.is_empty() {
        return false;
    }
    let subscribed: HashSet<&str> = state
        .members
        .values()
        .flat_map(|m| m.subscribed_topic_names.iter().map(String::as_str))
        .collect();
    if subscribed.is_empty() {
        return false;
    }
    let mut assigned: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for member in state.target.per_member.values() {
        for (topic_id, partitions) in member {
            assigned
                .entry(*topic_id)
                .or_default()
                .extend(partitions.iter().copied());
        }
    }
    let mut initialized: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for tp in &state.initialized {
        initialized
            .entry(tp.topic_id)
            .or_default()
            .insert(tp.partition);
    }
    initialized.iter().any(|(topic_id, partitions)| {
        state
            .topic_names
            .get(topic_id)
            .is_some_and(|name| subscribed.contains(name.as_str()))
            && assigned.get(topic_id) != Some(partitions)
    })
}

/// Resolve a share member's effective topic-id subscription. Share groups
/// support exact-name subscriptions only, with no regex, so this is a simple
/// name → id lookup against the current metadata.
fn resolve_subscribed_topic_ids(member: &ShareMemberState, input: &ReconcileInput) -> Vec<Uuid> {
    member
        .subscribed_topic_names
        .iter()
        .filter_map(|n| input.topic_id_by_name.get(n).copied())
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::{
            EpochExhausted, MetadataProvider, ReconcileInput, ShareGroupState, ShareMemberState,
            Uuid, reconcile,
        },
        crate::coordinator::unified::share::state::share_partition,
        assert2::{assert, check},
        std::{collections::HashSet, time::Duration},
    };

    #[derive(Debug)]
    struct Metadata {
        topic: Uuid,
        partitions: i32,
    }

    impl MetadataProvider for Metadata {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput {
                topic_id_by_name: [("t".to_owned(), self.topic)].into(),
                partitions_per_topic: [(self.topic, self.partitions)].into(),
                ..Default::default()
            }
        }
    }

    struct Step {
        name: &'static str,
        /// The partitions in the image.
        partitions: i32,
        /// The partition initialized before the step.
        initialized: Option<i32>,
        /// The expected group epoch.
        epoch: i32,
        /// The expected target of `m`.
        target: Vec<i32>,
    }

    /// Kafka's share heartbeat: the epoch bumps for a join, a metadata change
    /// and newly initialized partitions, only initialized partitions are
    /// assigned, and a retry with nothing new keeps the epoch.
    #[test]
    fn epoch_bumps_and_target_follow_initialized_partitions() {
        let topic = Uuid([5; 16]);
        let mut state = ShareGroupState::new("g");
        state.add_or_update_member(ShareMemberState::joining(
            "m",
            "client",
            "host",
            HashSet::from(["t".to_owned()]),
        ));
        let steps: [Step; 6] = [
            Step {
                name: "join with nothing initialized",
                partitions: 1,
                initialized: None,
                epoch: 2,
                target: vec![],
            },
            Step {
                name: "partition 0 initialized",
                partitions: 1,
                initialized: Some(0),
                epoch: 3,
                target: vec![0],
            },
            Step {
                name: "retry",
                partitions: 1,
                initialized: None,
                epoch: 3,
                target: vec![0],
            },
            Step {
                name: "topic grows",
                partitions: 2,
                initialized: None,
                epoch: 4,
                target: vec![0],
            },
            Step {
                name: "partition 1 initialized",
                partitions: 2,
                initialized: Some(1),
                epoch: 5,
                target: vec![0, 1],
            },
            Step {
                name: "retry after growth",
                partitions: 2,
                initialized: None,
                epoch: 5,
                target: vec![0, 1],
            },
        ];
        for (
            index,
            Step {
                name,
                partitions,
                initialized,
                epoch,
                target,
            },
        ) in steps.into_iter().enumerate()
        {
            if let Some(partition) = initialized {
                state.mark_initialized(share_partition(topic, partition));
                state.topic_names.insert(topic, "t".to_owned());
            }
            // Only the join changes the member's subscription.
            check!(
                reconcile(
                    &mut state,
                    &Metadata { topic, partitions },
                    Duration::ZERO,
                    index == 0,
                )
                .is_ok(),
                "{name}"
            );
            check!(state.group_epoch == epoch, "{name}");
            let got = state
                .target
                .per_member
                .get("m")
                .and_then(|m| m.get(&topic))
                .cloned()
                .unwrap_or_default();
            check!(got == target, "{name}");
        }
    }

    /// Kafka replays `ShareGroupMetadataValue.MetadataHash` into the group,
    /// and the first heartbeat after the load bumps the epoch only when the
    /// hash of the current image differs. The hash it writes is Kafka's:
    /// the golden values are hash4j 0.22.0's for topic `t`, id
    /// `0505..05-0505..05`, with one and with two partitions and no racks.
    #[test]
    fn a_replayed_hash_decides_the_first_epoch_bump() {
        use {
            super::super::seed::apply_seed,
            crate::coordinator::unified::{
                ShareGroupSeed,
                share::persistence::{
                    ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
                },
            },
        };

        const ONE_PARTITION: i64 = -1_770_207_100_006_454_364;
        const TWO_PARTITIONS: i64 = -6_073_397_787_647_429_838;
        let topic = Uuid([5; 16]);
        // (case, the stored hash, partitions in the image at the load,
        // (group epoch, hash) after the first heartbeat)
        let rows = [
            (
                "the stored hash matches",
                ONE_PARTITION,
                1,
                (3, ONE_PARTITION),
            ),
            ("the topic grew", ONE_PARTITION, 2, (4, TWO_PARTITIONS)),
            ("no hash was stored", 0, 1, (4, ONE_PARTITION)),
        ];
        for (case, stored, partitions, expected) in rows {
            let mut state = ShareGroupState::new("g");
            apply_seed(&mut state, ShareGroupSeed {
                group_epoch: 3,
                metadata_hash: stored,
                target_epoch: 3,
                assignment_timestamp_ms: 0,
                members: [("m".to_owned(), ShareGroupMemberMetadataValue {
                    rack_id: None,
                    client_id: "client".into(),
                    client_host: "host".into(),
                    subscribed_topic_names: vec!["t".into()],
                })]
                .into(),
                current_per_member: [("m".to_owned(), ShareGroupCurrentMemberAssignmentValue {
                    member_epoch: 3,
                    previous_member_epoch: 2,
                    assigned_partitions: vec![],
                })]
                .into(),
                ..ShareGroupSeed::default()
            });
            check!(
                reconcile(
                    &mut state,
                    &Metadata { topic, partitions },
                    Duration::ZERO,
                    false
                )
                .is_ok(),
                "{case}"
            );
            check!(
                (state.group_epoch, state.metadata_hash) == expected,
                "{case}"
            );
        }
    }

    #[test]
    fn metadata_change_fails_closed_at_epoch_limit() {
        let topic = Uuid([6; 16]);
        let mut state = ShareGroupState::new("g");
        state.group_epoch = i32::MAX;
        state.target.epoch = i32::MAX;
        state.members.insert(
            "m".to_owned(),
            ShareMemberState::joining("m", "client", "host", HashSet::from(["t".to_owned()])),
        );

        assert!(
            reconcile(
                &mut state,
                &Metadata {
                    topic,
                    partitions: 1,
                },
                Duration::ZERO,
                false,
            ) == Err(EpochExhausted)
        );
        check!(state.group_epoch == i32::MAX);
        assert!(state.target.per_member.is_empty());
    }
}
