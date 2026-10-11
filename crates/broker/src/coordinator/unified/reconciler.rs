//! The epoch and target assignment steps of Kafka's consumer group
//! heartbeat: the subscription metadata update that bumps the group epoch,
//! the epoch bump of a fence, and the target assignment that a group whose
//! epoch is ahead of its target computes.

use std::collections::{BTreeSet, HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::assignor::{
    Assignor, GroupSpec, MemberSubscription, SubscriptionShape, TopicMetadata, subscription_type,
};
use crate::coordinator::unified::{
    consumer_state::{GroupState, MemberState},
    topic_hash,
};

#[derive(Debug, Clone, Default)]
pub struct ReconcileInput {
    pub topic_id_by_name: HashMap<String, Uuid>,
    pub partitions_per_topic: HashMap<Uuid, i32>,
    /// Per-`(topic_id, partition_index)` rack of each replica whose broker
    /// has one, repeats included: Kafka's
    /// `CoordinatorMetadataImage.TopicMetadata.partitionRacks`. An empty or
    /// missing entry means there is no rack data for that partition.
    /// [`Self::topic_metadata`] gives the assignors the set of these racks,
    /// which the built-in assignors do not read, as in Kafka.
    pub partition_racks: HashMap<(Uuid, i32), Vec<String>>,
}

impl ReconcileInput {
    /// Kafka's `Utils.computeTopicHash` for the topic named `name`, or `None`
    /// when the snapshot does not hold it.
    #[must_use]
    pub fn topic_hash(&self, name: &str) -> Option<i64> {
        let topic_id = self.topic_id_by_name.get(name)?;
        let partitions = self
            .partitions_per_topic
            .get(topic_id)
            .copied()
            .unwrap_or(0)
            .max(0);
        let racks: Vec<Vec<&str>> = (0..partitions)
            .map(|partition| {
                self.partition_racks
                    .get(&(*topic_id, partition))
                    .map(|racks| racks.iter().map(String::as_str).collect())
                    .unwrap_or_default()
            })
            .collect();
        Some(topic_hash::topic_hash(topic_id.0, name, racks.into_iter()))
    }

    /// Kafka's `ModernGroup.computeMetadataHash`: the group hash over the
    /// topic hash of every topic in `topics` that the snapshot holds.
    #[must_use]
    pub fn metadata_hash<'a>(&self, topics: impl IntoIterator<Item = &'a str>) -> i64 {
        let topics: BTreeSet<&str> = topics.into_iter().collect();
        topic_hash::group_hash(
            topics
                .into_iter()
                .filter_map(|topic| self.topic_hash(topic).map(|hash| (topic, hash))),
        )
    }

    /// The assignors' view of the snapshot: the partition counts, and for each
    /// partition the set of racks it has a replica on, as Kafka's
    /// `SubscribedTopicDescriber.racksForPartition` gives it.
    #[must_use]
    pub fn topic_metadata(&self) -> TopicMetadata {
        TopicMetadata {
            partitions_per_topic: self.partitions_per_topic.clone(),
            partition_racks: self
                .partition_racks
                .iter()
                .map(|(partition, racks)| {
                    let racks: BTreeSet<&String> = racks.iter().collect();
                    (*partition, racks.into_iter().cloned().collect())
                })
                .collect(),
        }
    }
}

/// The group epoch cannot move past `i32::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochExhausted;

/// Kafka's `GroupMetadataManager.updateSubscriptionMetadata`, which
/// `consumerGroupHeartbeat` and `classicGroupJoinToConsumerGroup` run when the
/// heartbeat changed the subscription (`bump`) or the group's metadata
/// expired.
///
/// It computes the metadata hash of the subscribed topics, and bumps the group
/// epoch when `bump` is set or the hash differs from the stored one. It then
/// records the hash and ends a requested refresh, as Kafka's
/// `setMetadataRefreshDeadline` does. It returns whether it bumped the epoch.
///
/// # Errors
///
/// Returns [`EpochExhausted`] when the epoch must move and cannot; the group
/// is then unchanged.
pub fn update_subscription_metadata(
    group: &mut GroupState,
    input: &ReconcileInput,
    bump: bool,
) -> Result<bool, EpochExhausted> {
    let hash = metadata_hash(group, input);
    let bump = bump || hash != group.metadata_hash();
    if bump && !group.bump_epoch() {
        return Err(EpochExhausted);
    }
    group.record_metadata_hash(hash);
    Ok(bump)
}

/// The epoch bump of Kafka's `consumerGroupFenceMembers` and of
/// `handleRegularExpressionsResult`: the group epoch moves by one and the
/// group records the metadata hash of the topics that the remaining
/// subscriptions name.
///
/// # Errors
///
/// Returns [`EpochExhausted`] when the epoch cannot move; the group is then
/// unchanged.
pub fn bump_with_metadata_hash(
    group: &mut GroupState,
    input: &ReconcileInput,
) -> Result<(), EpochExhausted> {
    let hash = metadata_hash(group, input);
    if !group.bump_epoch() {
        return Err(EpochExhausted);
    }
    group.set_metadata_hash(hash);
    Ok(())
}

/// Kafka's `TargetAssignmentBuilder.build` for a consumer group whose epoch is
/// ahead of its target (`maybeUpdateTargetAssignment`): runs `assignor` over
/// every member and installs the result as the target at the group epoch.
///
/// It returns the members whose target changed, sorted: the members for which
/// Kafka writes a `ConsumerGroupTargetAssignmentMember` record.
pub fn compute_target(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor: &dyn Assignor,
) -> Vec<String> {
    let subscriptions: Vec<MemberSubscription> = group
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(group, m, &input.topic_id_by_name),
            assigned_partitions: group
                .target
                .per_member
                .get(&m.member_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let shapes: Vec<SubscriptionShape<'_>> = group
        .members
        .values()
        .map(|m| SubscriptionShape {
            topic_names: &m.subscribed_topic_names,
            topic_regex: m.subscribed_topic_regex.as_deref(),
            regex_topic_names: regex_topic_names(group, m, &input.topic_id_by_name),
        })
        .collect();
    let spec = GroupSpec {
        members: subscriptions,
        subscription_type: subscription_type(&shapes),
    };
    let assignment = assignor.assign(&spec, &input.topic_metadata());
    group.install_target(assignment)
}

/// Kafka's `ModernGroup.computeMetadataHash` for a consumer group: the
/// `topic_hash` group hash over every topic that the group subscribes to and
/// `input` holds.
///
/// A member subscribes to its topic names and to the topics that its regex
/// resolved to, which is what Kafka's `ConsumerGroup.subscribedTopicNames`
/// counts.
#[must_use]
pub fn metadata_hash(group: &GroupState, input: &ReconcileInput) -> i64 {
    let mut topics: BTreeSet<&str> = BTreeSet::new();
    for member in group.members.values() {
        topics.extend(member.subscribed_topic_names.iter().map(String::as_str));
        topics.extend(group.regex_topics(member).map(String::as_str));
    }
    input.metadata_hash(topics)
}

/// Insert into `out` every topic-id that a member subscribes to, both by exact
/// name and through the topics that its regex resolved to in the group. This
/// is the single source of truth for what a member subscribes to, and both
/// `compute_target` and `membership_topic_ids` use it.
///
/// The group resolves a pattern against the metadata image, as Kafka's
/// `TopicRegexResolver` does, and keeps the topics that the requesting
/// principal may `Describe` (see `GroupState::resolved_regex`). A resolved
/// topic that the snapshot no longer has, or that it does not have yet, is
/// skipped. A pattern the group has not resolved yet subscribes the member to
/// no topic.
fn collect_subscribed_topic_ids(
    group: &GroupState,
    member: &MemberState,
    topic_id_by_name: &HashMap<String, Uuid>,
    out: &mut HashSet<Uuid>,
) {
    for name in member
        .subscribed_topic_names
        .iter()
        .chain(group.regex_topics(member))
    {
        if let Some(id) = topic_id_by_name.get(name) {
            out.insert(*id);
        }
    }
}

/// The existing topic names that a member's regex subscription resolved to,
/// for Kafka's subscription-type rule.
fn regex_topic_names<'a>(
    group: &GroupState,
    member: &MemberState,
    topic_id_by_name: &'a HashMap<String, Uuid>,
) -> Vec<&'a str> {
    group
        .regex_topics(member)
        .filter_map(|name| topic_id_by_name.get_key_value(name))
        .map(|(name, _)| name.as_str())
        .collect()
}

/// Resolve a member's effective topic-id subscription as a vector.
fn resolve_subscribed_topic_ids(
    group: &GroupState,
    member: &MemberState,
    topic_id_by_name: &HashMap<String, Uuid>,
) -> Vec<Uuid> {
    let mut out = HashSet::new();
    collect_subscribed_topic_ids(group, member, topic_id_by_name, &mut out);
    out.into_iter().collect()
}

#[must_use]
pub fn membership_topic_ids(group: &GroupState, input: &ReconcileInput) -> HashSet<Uuid> {
    let mut out = HashSet::new();
    for m in group.members.values() {
        collect_subscribed_topic_ids(group, m, &input.topic_id_by_name, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{super::assignor::UniformAssignor, *};
    use crate::coordinator::unified::consumer_state::{
        MemberState, ResolvedRegularExpression, test_support::subscribed_member,
    };

    fn fresh_member(id: &str, topic: &str) -> MemberState {
        subscribed_member(id, &[topic])
    }

    fn input(topic_name: &str, partitions: i32) -> (ReconcileInput, Uuid) {
        let t = Uuid([1; 16]);
        (
            ReconcileInput {
                topic_id_by_name: [(topic_name.into(), t)].into(),
                partitions_per_topic: [(t, partitions)].into(),
                ..Default::default()
            },
            t,
        )
    }

    /// Kafka's `updateSubscriptionMetadata`: (case, bump asked, the topic's
    /// partitions when the group recorded its hash, the partitions now) to
    /// (epoch bumped, group epoch after). A new hash bumps the epoch even when
    /// the heartbeat did not ask for it, and an unchanged hash with no bump
    /// asked keeps it.
    #[test]
    fn the_subscription_metadata_update_bumps_as_kafka_does() {
        let rows = [
            ("nothing changed", false, 2, 2, (false, 1)),
            ("the subscription changed", true, 2, 2, (true, 2)),
            ("the topic grew", false, 2, 4, (true, 2)),
            ("both", true, 2, 4, (true, 2)),
        ];
        for (case, bump, recorded, now, expected) in rows {
            let mut g = GroupState::new("g");
            g.add_or_update_member(fresh_member("m1", "t"));
            let (at_record, _) = input("t", recorded);
            g.record_metadata_hash(metadata_hash(&g, &at_record));
            let (current, _) = input("t", now);
            let bumped = update_subscription_metadata(&mut g, &current, bump).unwrap();
            check!((bumped, g.group_epoch) == expected, "{case}");
            check!(g.metadata_hash() == metadata_hash(&g, &current), "{case}");
            check!(!g.metadata_refresh_requested(), "{case}");
        }
    }

    #[test]
    fn an_exhausted_epoch_bumps_nothing() {
        let mut group = GroupState::new("g");
        group.group_epoch = i32::MAX;
        let (input, _) = input("t", 1);
        assert!(update_subscription_metadata(&mut group, &input, true) == Err(EpochExhausted));
        assert!(bump_with_metadata_hash(&mut group, &input) == Err(EpochExhausted));
        assert!(group.group_epoch == i32::MAX);
    }

    /// Kafka's `TargetAssignmentBuilder` writes a target record for each member
    /// whose assignment changed, a member with no previous target included,
    /// and installs the target at the group epoch.
    #[test]
    fn compute_target_reports_the_members_whose_target_changed() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 4);
        g.bump_epoch();
        let first = compute_target(&mut g, &inp, &UniformAssignor);
        check!(first == vec!["m1".to_string()]);
        check!(g.target.epoch == 2);
        check!(g.target.per_member["m1"][&t] == vec![0, 1, 2, 3]);

        // A member that subscribes to nothing joins: it gets an empty target
        // and m1 keeps its own, so only the new member has a record.
        g.add_or_update_member(fresh_member("m2", "unknown"));
        g.bump_epoch();
        let second = compute_target(&mut g, &inp, &UniformAssignor);
        check!(second == vec!["m2".to_string()]);
        check!(g.target.per_member["m2"].is_empty());
        check!(g.target.epoch == 3);
    }

    #[test]
    fn subscription_topic_ids_resolved() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 2);
        let ids = membership_topic_ids(&g, &inp);
        assert!(ids.contains(&t));
    }

    // ── subscribed_topic_regex resolution ───────────────────

    fn input_with_topics(topics: &[(&str, i32)]) -> ReconcileInput {
        let mut topic_id_by_name = HashMap::new();
        let mut partitions_per_topic = HashMap::new();
        for (i, (name, parts)) in topics.iter().enumerate() {
            let id = Uuid([u8::try_from(i + 1).unwrap_or(255); 16]);
            topic_id_by_name.insert((*name).to_string(), id);
            partitions_per_topic.insert(id, *parts);
        }
        ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            ..Default::default()
        }
    }

    fn member_with_regex(id: &str, names: &[&str], regex: Option<&str>) -> MemberState {
        let mut member = fresh_member(id, "unused");
        member.subscribed_topic_names = names.iter().map(|name| (*name).to_string()).collect();
        member.subscribed_topic_regex = regex.map(String::from);
        member
    }

    /// Records that `regex` resolved to `topics` in `group`, as a heartbeat
    /// that resolved it does.
    fn resolve(group: &mut GroupState, regex: &str, topics: &[&str]) {
        group.set_resolved_regex(
            regex.to_owned(),
            ResolvedRegularExpression {
                topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                version: 1,
                timestamp_ms: 1,
            },
        );
    }

    /// The topics that `member_id` is assigned, by name.
    fn assigned_names(group: &GroupState, input: &ReconcileInput, member_id: &str) -> Vec<String> {
        let mut names: Vec<String> = group
            .target
            .per_member
            .get(member_id)
            .map(|topics| {
                input
                    .topic_id_by_name
                    .iter()
                    .filter(|(_, id)| topics.contains_key(id))
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    /// (regex, names, what the group resolved the regex to, the topics that
    /// the member is assigned). The reconciler subscribes a member to its
    /// names and to the topics its regex resolved to in the group, whatever
    /// else the pattern would match: a topic that the resolution leaves out,
    /// because the requesting principal may not `Describe` it or because it is
    /// too new, is not assigned, and a regex the group has not resolved yet
    /// subscribes to nothing.
    #[test]
    fn a_member_is_assigned_the_names_and_the_topics_its_regex_resolved_to() {
        type Row<'a> = (&'a str, &'a [&'a str], Option<&'a [&'a str]>, &'a [&'a str]);
        let inp = input_with_topics(&[
            ("orders-eu", 1),
            ("orders-us", 1),
            ("audit", 1),
            ("shipments", 1),
        ]);
        let rows: [Row<'_>; 5] = [
            (
                "^orders-.*",
                &[],
                Some(&["orders-eu", "orders-us"]),
                &["orders-eu", "orders-us"],
            ),
            ("^orders-.*", &[], Some(&["orders-eu"]), &["orders-eu"]),
            (
                "^orders-.*",
                &["audit"],
                Some(&["orders-eu"]),
                &["audit", "orders-eu"],
            ),
            // The resolution may name a topic that has been deleted since.
            (
                "^orders-.*",
                &[],
                Some(&["orders-eu", "orders-gone"]),
                &["orders-eu"],
            ),
            // No resolution yet: the names still apply.
            ("^orders-.*", &["audit"], None, &["audit"]),
        ];
        for (regex, names, resolved, expected) in rows {
            let mut g = GroupState::new("g");
            g.add_or_update_member(member_with_regex("m1", names, Some(regex)));
            if let Some(topics) = resolved {
                resolve(&mut g, regex, topics);
            }
            g.bump_epoch();
            compute_target(&mut g, &inp, &UniformAssignor);
            assert!(
                assigned_names(&g, &inp, "m1") == expected,
                "{regex} {names:?} {resolved:?}"
            );
        }
    }

    /// Every member of a group that subscribes to the same regex gets the
    /// resolution that the group holds for it, and a member with a different
    /// regex gets that regex's.
    #[test]
    fn members_share_the_resolution_of_the_same_regex() {
        let inp = input_with_topics(&[("a1", 2), ("a2", 2), ("b1", 2)]);
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("a.*")));
        g.add_or_update_member(member_with_regex("m2", &[], Some("a.*")));
        g.add_or_update_member(member_with_regex("m3", &[], Some("b.*")));
        resolve(&mut g, "a.*", &["a1", "a2"]);
        resolve(&mut g, "b.*", &["b1"]);
        g.bump_epoch();
        compute_target(&mut g, &inp, &UniformAssignor);
        let assigned: Vec<Vec<String>> = ["m1", "m2", "m3"]
            .iter()
            .map(|member_id| assigned_names(&g, &inp, member_id))
            .collect();
        assert!(assigned == [vec!["a1", "a2"], vec!["a1", "a2"], vec!["b1"]]);
    }

    /// Kafka's metadata hash covers the id, the name, the partition count and
    /// the partition racks of every topic that the group subscribes to, by
    /// name or through a resolved regex, and nothing else.
    #[test]
    fn the_metadata_hash_moves_with_the_subscribed_topics_only() {
        let snapshot = |topics: &[(&str, u8, i32)], racks: &[(u8, i32, &str)]| ReconcileInput {
            topic_id_by_name: topics
                .iter()
                .map(|(name, id, _)| ((*name).to_string(), Uuid([*id; 16])))
                .collect(),
            partitions_per_topic: topics
                .iter()
                .map(|(_, id, partitions)| (Uuid([*id; 16]), *partitions))
                .collect(),
            partition_racks: racks
                .iter()
                .map(|(id, partition, rack)| ((Uuid([*id; 16]), *partition), vec![(*rack).into()]))
                .collect(),
        };
        let base: &[(&str, u8, i32)] = &[
            ("orders", 1, 2),
            ("payments", 2, 1),
            ("payouts", 3, 1),
            ("audit", 4, 1),
        ];
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "orders"));
        g.add_or_update_member(member_with_regex("m2", &[], Some("^pay.*")));
        resolve(&mut g, "^pay.*", &["payments"]);
        let before = metadata_hash(&g, &snapshot(base, &[(1, 0, "rack-a")]));

        // (name, the snapshot after, whether the hash moves)
        let rows = [
            (
                "nothing changes",
                snapshot(base, &[(1, 0, "rack-a")]),
                false,
            ),
            (
                "an unsubscribed topic is created",
                snapshot(
                    &[base, &[("refunds", 5, 1)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "an unsubscribed topic grows",
                snapshot(
                    &[&base[..3], &[("audit", 4, 2)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "a topic that the regex matches without authorization grows",
                snapshot(
                    &[&base[..2], &[("payouts", 3, 2), ("audit", 4, 1)][..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                false,
            ),
            (
                "a subscribed topic grows",
                snapshot(
                    &[&[("orders", 1, 3)][..], &base[1..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                true,
            ),
            (
                "a subscribed topic is deleted",
                snapshot(&base[1..], &[]),
                true,
            ),
            (
                "a subscribed topic is created again with a new id",
                snapshot(
                    &[&[("orders", 9, 2)][..], &base[1..]].concat(),
                    &[(9, 0, "rack-a")],
                ),
                true,
            ),
            (
                "a partition of a subscribed topic moves to another rack",
                snapshot(base, &[(1, 0, "rack-b")]),
                true,
            ),
            (
                "a topic that the regex resolved to grows",
                snapshot(
                    &[&base[..1], &[("payments", 2, 2)][..], &base[2..]].concat(),
                    &[(1, 0, "rack-a")],
                ),
                true,
            ),
        ];
        let mut found = Vec::new();
        let mut expected = Vec::new();
        for (name, after, moves) in rows {
            found.push((name, metadata_hash(&g, &after) != before));
            expected.push((name, moves));
        }
        check!(found == expected);
        check!(metadata_hash(&g, &ReconcileInput::default()) == 0);
    }

    #[test]
    fn membership_topic_ids_includes_regex_matches() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("^orders-.*")));
        resolve(&mut g, "^orders-.*", &["orders-eu"]);
        let inp = input_with_topics(&[("orders-eu", 1), ("shipments", 1)]);
        let orders = inp.topic_id_by_name["orders-eu"];
        let ids = membership_topic_ids(&g, &inp);
        assert!(
            ids.contains(&orders),
            "regex match flows into membership set"
        );
    }
}
