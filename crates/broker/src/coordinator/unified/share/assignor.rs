//! KIP-932 share-group assignor, a port of Kafka's `SimpleAssignor`.
//!
//! Share-group assignment is **non-exclusive**: when a group has more members
//! than partitions, several members share a partition. The assignor balances
//! first and keeps as much of the previous target assignment as balance
//! allows. A homogeneous group, where every member subscribes to the same
//! topics, balances all of its partitions together
//! (`SimpleHomogeneousAssignmentBuilder`). A heterogeneous group balances each
//! topic among the members that subscribe to it
//! (`SimpleHeterogeneousAssignmentBuilder`). Rack ids are ignored, as in
//! Kafka.
//!
//! Kafka walks Java hash maps and hash sets, whose order follows the hash of
//! each member id and partition. This port walks members in member-id order
//! and partitions in `(topic id, partition)` order, so it is deterministic.
//! Where Kafka's walk order decides between equally balanced results, the
//! member that gets a partition can differ.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Bound,
};

use krabka_protocol::primitives::uuid::Uuid;

use crate::coordinator::unified::assignor::{
    Assignment, GroupSpec, SubscriptionType, TopicMetadata,
};

/// A topic id as an ordered key.
type TopicKey = [u8; 16];
/// A `(topic id, partition)` pair as an ordered key.
type TopicPartition = (TopicKey, i32);
/// One member's assignment, in key order.
type MemberAssignment = BTreeMap<TopicKey, BTreeSet<i32>>;

/// Kafka's `SimpleAssignor`, which share groups report as `simple`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShareGroupAssignor;

impl ShareGroupAssignor {
    #[must_use]
    pub fn name(&self) -> &'static str {
        "simple"
    }

    /// The target assignment of `group` over `topics`.
    ///
    /// Each member's `assigned_partitions` is its current target assignment.
    /// `assignable` is Kafka's `GroupSpec.isPartitionAssignable` map: with
    /// `Some`, only the listed partitions are assigned; with `None`, every
    /// partition is.
    #[must_use]
    pub fn assign(
        &self,
        group: &GroupSpec,
        topics: &TopicMetadata,
        assignable: Option<&HashMap<Uuid, HashSet<i32>>>,
    ) -> Assignment {
        let mut members: Vec<_> = group.members.iter().collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        if members.is_empty() {
            return Assignment::new();
        }
        let subscriptions: Vec<BTreeSet<TopicKey>> = members
            .iter()
            .map(|m| m.subscribed_topic_ids.iter().map(|t| t.0).collect())
            .collect();
        let old: Vec<MemberAssignment> = members
            .iter()
            .map(|m| {
                m.assigned_partitions
                    .iter()
                    .map(|(t, parts)| (t.0, parts.iter().copied().collect()))
                    .collect()
            })
            .collect();
        let target_partitions = |topic: TopicKey| -> Vec<i32> {
            let count = topics
                .partitions_per_topic
                .get(&Uuid(topic))
                .copied()
                .unwrap_or(0);
            (0..count)
                .filter(|p| {
                    assignable.is_none_or(|a| a.get(&Uuid(topic)).is_some_and(|s| s.contains(p)))
                })
                .collect()
        };
        // A previous assignment keeps only partitions that are still target
        // partitions. Kafka's builders keep a partition that stopped being
        // assignable; that cannot happen in Kafka, where a topic never shrinks
        // and an initialized partition stays initialized, but here it would
        // hand out a partition the image no longer holds.
        let old: Vec<MemberAssignment> = old
            .into_iter()
            .map(|mut assignment| {
                for (topic, parts) in &mut assignment {
                    let targets = target_partitions(*topic);
                    parts.retain(|p| targets.contains(p));
                }
                assignment
            })
            .collect();
        let assignments = match group.subscription_type {
            SubscriptionType::Homogeneous => homogeneous(&subscriptions[0], old, target_partitions),
            SubscriptionType::Heterogeneous => {
                heterogeneous(&subscriptions, old, target_partitions)
            }
        };
        members
            .iter()
            .zip(assignments)
            .map(|(m, assignment)| {
                let assignment = assignment
                    .into_iter()
                    .filter(|(_, parts)| !parts.is_empty())
                    .map(|(t, parts)| (Uuid(t), parts.into_iter().collect()))
                    .collect();
                (m.member_id.clone(), assignment)
            })
            .collect()
    }
}

/// Kafka's `desiredSharing`: how many members each partition should have.
fn desired_sharing(members: usize, partitions: usize) -> usize {
    if partitions == 0 {
        0
    } else {
        members.div_ceil(partitions)
    }
}

/// Kafka's `desiredAssignmentCount` of the member at `index` of `members`:
/// the members split `sharing * partitions` assignments as evenly as the
/// ceiling steps allow. Kafka computes the steps in `double`; this computes
/// them exactly.
fn desired_count(index: usize, members: usize, sharing: usize, partitions: usize) -> usize {
    let total = sharing * partitions;
    (total * (index + 1)).div_ceil(members) - (total * index).div_ceil(members)
}

/// The next member after `cursor` in `set`, which is Java's
/// `Iterator.next` over a set that the loop removes from as it goes.
fn next_after(set: &BTreeSet<usize>, cursor: Option<usize>) -> Option<usize> {
    match cursor {
        None => set.iter().next().copied(),
        Some(c) => set
            .range((Bound::Excluded(c), Bound::Unbounded))
            .next()
            .copied(),
    }
}

/// The next unfilled member for `assignRemainingPartitions`: the one after
/// `cursor`, or, when the walk ran off the end and assigned something on this
/// pass, the first one again.
fn next_member(
    unfilled: &BTreeSet<usize>,
    cursor: Option<usize>,
    assigned_this_pass: &mut bool,
) -> Option<usize> {
    if let Some(member) = next_after(unfilled, cursor) {
        return Some(member);
    }
    if !*assigned_this_pass {
        return None;
    }
    *assigned_this_pass = false;
    next_after(unfilled, None)
}

/// Revoke excess holders in their original member order, then update the caller's indices.
macro_rules! revoke_overshared_partitions {
    ($by_partition:ident, $sharing:ident;
        |$member:ident, $partition:ident| $remove:expr; { $($revoked:tt)* }) => {
        for (&$partition, holders) in &mut $by_partition {
            let mut count = holders.len();
            if count <= $sharing {
                continue;
            }
            for $member in holders.clone() {
                if $remove {
                    count -= 1;
                    holders.remove(&$member);
                    $($revoked)*
                }
                if count <= $sharing {
                    break;
                }
            }
        }
    };
}

/// Kafka's `SimpleHomogeneousAssignmentBuilder.build`.
fn homogeneous(
    subscribed: &BTreeSet<TopicKey>,
    old: Vec<MemberAssignment>,
    target_partitions: impl Fn(TopicKey) -> Vec<i32>,
) -> Vec<MemberAssignment> {
    let member_count = old.len();
    if subscribed.is_empty() {
        return vec![MemberAssignment::new(); member_count];
    }
    let targets: Vec<TopicPartition> = subscribed
        .iter()
        .flat_map(|&t| target_partitions(t).into_iter().map(move |p| (t, p)))
        .collect();
    let sharing = desired_sharing(member_count, targets.len());
    let desired: Vec<usize> = (0..member_count)
        .map(|i| desired_count(i, member_count, sharing, targets.len()))
        .collect();

    // revokeUnassignablePartitions: drop the topics no longer subscribed and
    // count what each member keeps.
    let mut assignment = old;
    let mut by_partition: BTreeMap<TopicPartition, BTreeSet<usize>> = BTreeMap::new();
    let mut by_member: Vec<BTreeSet<TopicPartition>> = vec![BTreeSet::new(); member_count];
    let mut unfilled: BTreeSet<usize> = BTreeSet::new();
    let mut overfilled: BTreeSet<usize> = BTreeSet::new();
    for (member, member_assignment) in assignment.iter_mut().enumerate() {
        member_assignment.retain(|topic, _| subscribed.contains(topic));
        for (&topic, parts) in member_assignment.iter() {
            for &p in parts {
                by_partition.entry((topic, p)).or_default().insert(member);
                by_member[member].insert((topic, p));
            }
        }
        let count = by_member[member].len();
        if count < desired[member] {
            unfilled.insert(member);
        } else if count > desired[member] {
            overfilled.insert(member);
        }
    }

    // revokeOverfilledMembers.
    for &member in &overfilled {
        while by_member[member].len() > desired[member] {
            let Some(tp) = by_member[member].pop_first() else {
                break;
            };
            remove_partition(&mut assignment[member], tp);
            if let Some(holders) = by_partition.get_mut(&tp) {
                holders.remove(&member);
            }
        }
    }

    // revokeOversharedPartitions.
    revoke_overshared_partitions! {
        by_partition, sharing; |member, tp| remove_partition(&mut assignment[member], tp); {
            by_member[member].remove(&tp);
            unfilled.insert(member);
        }
    }

    // Add the target partitions nobody holds, then assignRemainingPartitions.
    for &tp in &targets {
        by_partition.entry(tp).or_default();
    }
    assign_remaining(&by_partition, sharing, &mut unfilled, |member, tp| {
        assignment[member].entry(tp.0).or_default().insert(tp.1);
        by_member[member].insert(tp);
        by_member[member].len() >= desired[member]
    });
    assignment
}

/// Kafka's `SimpleHeterogeneousAssignmentBuilder.build`: each topic is
/// balanced among its own subscribers.
///
/// Kafka keeps a member's partitions of a topic that no member subscribes to
/// any more, because it only walks subscribed topics. This port drops them,
/// so a deleted or abandoned topic leaves the assignment.
fn heterogeneous(
    subscriptions: &[BTreeSet<TopicKey>],
    old: Vec<MemberAssignment>,
    target_partitions: impl Fn(TopicKey) -> Vec<i32>,
) -> Vec<MemberAssignment> {
    let member_count = old.len();
    let subscribed: BTreeSet<TopicKey> = subscriptions.iter().flatten().copied().collect();
    let mut assignment = old;
    for member_assignment in &mut assignment {
        member_assignment.retain(|topic, _| subscribed.contains(topic));
    }
    for &topic in &subscribed {
        let targets = target_partitions(topic);
        let subscribers: Vec<usize> = (0..member_count)
            .filter(|&m| subscriptions[m].contains(&topic))
            .collect();
        let sharing = desired_sharing(subscribers.len(), targets.len());
        let mut desired = vec![0; member_count];
        for (i, &member) in subscribers.iter().enumerate() {
            desired[member] = desired_count(i, subscribers.len(), sharing, targets.len());
        }

        // revokeUnassignablePartitions.
        let mut by_partition: BTreeMap<i32, BTreeSet<usize>> = BTreeMap::new();
        let mut by_member: BTreeMap<usize, BTreeSet<i32>> = BTreeMap::new();
        for (member, member_assignment) in assignment.iter().enumerate() {
            for &p in member_assignment.get(&topic).into_iter().flatten() {
                by_partition.entry(p).or_default().insert(member);
                by_member.entry(member).or_default().insert(p);
            }
        }

        // revokeOverfilledMembers.
        for (&member, held) in &mut by_member {
            while held.len() > desired[member] {
                let Some(p) = held.pop_first() else {
                    break;
                };
                if let Some(holders) = by_partition.get_mut(&p) {
                    holders.remove(&member);
                }
                remove_partition(&mut assignment[member], (topic, p));
            }
        }

        // revokeOversharedPartitions.
        revoke_overshared_partitions! {
            by_partition, sharing; |member, p| remove_partition(&mut assignment[member], (topic, p)); {
                if let Some(held) = by_member.get_mut(&member) {
                    held.remove(&p);
                }
            }
        }

        for &p in &targets {
            by_partition.entry(p).or_default();
        }

        // assignRemainingPartitions.
        let held = |by_member: &BTreeMap<usize, BTreeSet<i32>>, member: usize| {
            by_member.get(&member).map_or(0, BTreeSet::len)
        };
        let mut unfilled: BTreeSet<usize> = subscribers
            .iter()
            .copied()
            .filter(|&m| held(&by_member, m) < desired[m])
            .collect();
        assign_remaining(&by_partition, sharing, &mut unfilled, |member, p| {
            assignment[member].entry(topic).or_default().insert(p);
            by_member.entry(member).or_default().insert(p);
            held(&by_member, member) >= desired[member]
        });
    }
    assignment
}

/// Fill undershared partitions in Kafka's member order. `assign` returns whether
/// the member reached its desired count.
fn assign_remaining<P: Copy>(
    by_partition: &BTreeMap<P, BTreeSet<usize>>,
    sharing: usize,
    unfilled: &mut BTreeSet<usize>,
    mut assign: impl FnMut(usize, P) -> bool,
) {
    let mut cursor = None;
    let mut assigned_this_pass = false;
    for (&partition, holders) in by_partition {
        if unfilled.is_empty() {
            break;
        }
        let mut to_make = sharing.saturating_sub(holders.len());
        while to_make > 0 {
            let Some(member) = next_member(unfilled, cursor, &mut assigned_this_pass) else {
                break;
            };
            cursor = Some(member);
            // Kafka checks the original holders and never adds the member it
            // just assigned. A repeat insertion is a no-op, but the count
            // still drops, so fewer members can hold it than `sharing`.
            if holders.contains(&member) {
                continue;
            }
            let filled = assign(member, partition);
            to_make -= 1;
            assigned_this_pass = true;
            if filled {
                unfilled.remove(&member);
            }
        }
    }
}

/// Removes `tp` from `assignment`, and says whether it was there.
fn remove_partition(assignment: &mut MemberAssignment, (topic, partition): TopicPartition) -> bool {
    assignment
        .get_mut(&topic)
        .is_some_and(|parts| parts.remove(&partition))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::assignor::MemberSubscription;

    const T1: Uuid = Uuid([1; 16]);
    const T2: Uuid = Uuid([2; 16]);
    const T3: Uuid = Uuid([3; 16]);

    fn topics(counts: &[(Uuid, i32)]) -> TopicMetadata {
        TopicMetadata {
            partitions_per_topic: counts.iter().copied().collect(),
            ..TopicMetadata::default()
        }
    }

    fn member(id: &str, subs: &[Uuid], assigned: &[(Uuid, &[i32])]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: subs.to_vec(),
            assigned_partitions: assigned.iter().map(|(t, p)| (*t, p.to_vec())).collect(),
        }
    }

    fn spec(members: Vec<MemberSubscription>, subscription_type: SubscriptionType) -> GroupSpec {
        GroupSpec {
            members,
            subscription_type,
        }
    }

    /// One member's expected topics and partitions.
    type ExpectedMember<'a> = (&'a str, &'a [(Uuid, &'a [i32])]);

    fn expected(rows: &[ExpectedMember<'_>]) -> Assignment {
        rows.iter()
            .map(|(m, parts)| {
                (
                    (*m).to_owned(),
                    parts.iter().map(|(t, p)| (*t, p.to_vec())).collect(),
                )
            })
            .collect()
    }

    fn overshared_first_partition(c_subscriptions: &[Uuid]) -> Vec<MemberSubscription> {
        vec![
            member("A", &[T1], &[(T1, &[0])]),
            member("B", &[T1], &[(T1, &[0])]),
            member("C", c_subscriptions, &[(T1, &[0])]),
        ]
    }

    fn repeated_member_expected(c_partitions: &[(Uuid, &[i32])]) -> Assignment {
        expected(&[
            ("A", &[(T1, &[1])]),
            ("B", &[(T1, &[0])]),
            ("C", c_partitions),
        ])
    }

    /// The whole assignment for inputs from Kafka's `SimpleAssignorTest` and
    /// from the issue: balance over the group, sharing when members outnumber
    /// partitions, and stickiness.
    #[test]
    fn assignments_match_kafka_simple_assignor() {
        use SubscriptionType::{Heterogeneous, Homogeneous};
        type Row = (
            &'static str,
            GroupSpec,
            TopicMetadata,
            Option<HashMap<Uuid, HashSet<i32>>>,
            Assignment,
        );
        let rows: Vec<Row> = vec![
            (
                "no members",
                spec(vec![], Homogeneous),
                topics(&[(T1, 3)]),
                None,
                Assignment::new(),
            ),
            (
                "no subscribed topic",
                spec(vec![member("A", &[], &[])], Homogeneous),
                topics(&[(T1, 3)]),
                None,
                expected(&[("A", &[])]),
            ),
            (
                "three single-partition topics balance over three members",
                spec(
                    vec![
                        member("A", &[T1, T2, T3], &[]),
                        member("B", &[T1, T2, T3], &[]),
                        member("C", &[T1, T2, T3], &[]),
                    ],
                    Homogeneous,
                ),
                topics(&[(T1, 1), (T2, 1), (T3, 1)]),
                None,
                expected(&[
                    ("A", &[(T1, &[0])]),
                    ("B", &[(T2, &[0])]),
                    ("C", &[(T3, &[0])]),
                ]),
            ),
            (
                "more members than partitions share",
                spec(
                    vec![
                        member("A", &[T1], &[]),
                        member("B", &[T1], &[]),
                        member("C", &[T1], &[]),
                    ],
                    Homogeneous,
                ),
                topics(&[(T1, 1)]),
                None,
                expected(&[
                    ("A", &[(T1, &[0])]),
                    ("B", &[(T1, &[0])]),
                    ("C", &[(T1, &[0])]),
                ]),
            ),
            (
                "a joining member takes partitions and the rest stay",
                spec(
                    vec![
                        member("A", &[T1], &[(T1, &[0, 1, 2])]),
                        member("B", &[T1], &[(T1, &[3, 4, 5])]),
                        member("C", &[T1], &[]),
                    ],
                    Homogeneous,
                ),
                topics(&[(T1, 6)]),
                None,
                expected(&[
                    ("A", &[(T1, &[1, 2])]),
                    ("B", &[(T1, &[4, 5])]),
                    ("C", &[(T1, &[0, 3])]),
                ]),
            ),
            (
                "an unsubscribed topic leaves the assignment",
                spec(
                    vec![member("A", &[T2], &[(T1, &[0]), (T2, &[0])])],
                    Homogeneous,
                ),
                topics(&[(T1, 1), (T2, 1)]),
                None,
                expected(&[("A", &[(T2, &[0])])]),
            ),
            (
                "only assignable partitions are assigned",
                spec(
                    vec![member("A", &[T1, T3], &[]), member("B", &[T1, T3], &[])],
                    Homogeneous,
                ),
                topics(&[(T1, 3), (T3, 2)]),
                Some(HashMap::from([(T1, HashSet::from([0, 2]))])),
                expected(&[("A", &[(T1, &[0])]), ("B", &[(T1, &[2])])]),
            ),
            (
                "Kafka's heterogeneous case with a non-assignable topic",
                spec(
                    vec![
                        member("A", &[T1, T2], &[]),
                        member("B", &[T3], &[]),
                        member("C", &[T2, T3], &[]),
                    ],
                    Heterogeneous,
                ),
                topics(&[(T1, 3), (T2, 3), (T3, 2)]),
                Some(HashMap::from([
                    (T1, HashSet::from([0, 1, 2])),
                    (T2, HashSet::from([0, 1, 2])),
                ])),
                expected(&[
                    ("A", &[(T1, &[0, 1, 2]), (T2, &[0, 2])]),
                    ("B", &[]),
                    ("C", &[(T2, &[1])]),
                ]),
            ),
            (
                "a heterogeneous group balances each topic by itself",
                spec(
                    vec![
                        member("A", &[T1], &[]),
                        member("B", &[T2], &[]),
                        member("C", &[T1, T2], &[]),
                    ],
                    Heterogeneous,
                ),
                topics(&[(T1, 2), (T2, 2)]),
                None,
                expected(&[
                    ("A", &[(T1, &[0])]),
                    ("B", &[(T2, &[0])]),
                    ("C", &[(T1, &[1]), (T2, &[1])]),
                ]),
            ),
            // Kafka's assignRemainingPartitions does not add a member it just
            // assigned to the partition's holders. After the overshare on
            // partition 0 is revoked from A, A is the only unfilled member and
            // comes round twice for partition 1, which Kafka counts as two
            // assignments: partition 1 ends with one member, not two.
            (
                "a repeated member counts twice, as in Kafka (homogeneous)",
                spec(overshared_first_partition(&[T1]), Homogeneous),
                topics(&[(T1, 2)]),
                None,
                repeated_member_expected(&[(T1, &[0])]),
            ),
            (
                "a repeated member counts twice, as in Kafka (heterogeneous)",
                spec(overshared_first_partition(&[T1, T2]), Heterogeneous),
                topics(&[(T1, 2), (T2, 1)]),
                None,
                repeated_member_expected(&[(T1, &[0]), (T2, &[0])]),
            ),
        ];
        for (name, group, topics, assignable, want) in rows {
            let got = ShareGroupAssignor.assign(&group, &topics, assignable.as_ref());
            check!(got == want, "{name}");
        }
    }

    /// Kafka's `testIncrementalAssignment*MembersHomogeneous`: members join one
    /// at a time and then leave one at a time, every partition stays assigned,
    /// and the member counts stay balanced.
    #[test]
    fn incremental_membership_keeps_every_partition_assigned() {
        let partitions = 24;
        let topics = topics(&[(T1, partitions)]);
        let mut current: Assignment = Assignment::new();
        let run = |ids: &[String], current: &Assignment| {
            let members = ids
                .iter()
                .map(|id| MemberSubscription {
                    member_id: id.clone(),
                    rack_id: None,
                    subscribed_topic_ids: vec![T1],
                    assigned_partitions: current.get(id).cloned().unwrap_or_default(),
                })
                .collect();
            ShareGroupAssignor.assign(&spec(members, SubscriptionType::Homogeneous), &topics, None)
        };
        let covered = |a: &Assignment| {
            a.values()
                .flat_map(|m| m.get(&T1).into_iter().flatten().copied())
                .collect::<HashSet<i32>>()
                .len()
        };
        let mut ids: Vec<String> = Vec::new();
        for i in 0..30 {
            ids.push(format!("M{i:02}"));
            let next = run(&ids, &current);
            check!(covered(&next) == 24, "{} members", ids.len());
            // Balance: member counts differ by at most one.
            let counts: Vec<usize> = next
                .values()
                .map(|m| m.get(&T1).map_or(0, Vec::len))
                .collect();
            check!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
            current = next;
        }
        while ids.len() > 1 {
            ids.pop();
            current = run(&ids, &current);
            check!(covered(&current) == 24, "{} members", ids.len());
        }
        assert!(current["M00"][&T1] == (0..24).collect::<Vec<_>>());
    }
}
