use std::collections::{HashMap, HashSet};

use assert2::assert;
use krabka_protocol::primitives::uuid::Uuid;
use krabka_verified::uniform_assignor::{SubscriberLoad, select_least_loaded};
use proptest::prelude::*;

use super::{Balancer, UniformAssignor};
use crate::coordinator::unified::assignor::{
    Assignment, Assignor, GroupSpec, MemberSubscription, SubscriptionType, TopicMetadata,
};

const HOMOGENEOUS: SubscriptionType = SubscriptionType::Homogeneous;
const HETEROGENEOUS: SubscriptionType = SubscriptionType::Heterogeneous;

/// A topic ID whose rank in Kafka's `Uuid` order is `rank`.
fn tid(rank: u8) -> Uuid {
    Uuid([rank; 16])
}

type Partitions<'a> = &'a [(Uuid, &'a [i32])];

fn owned(partitions: Partitions<'_>) -> HashMap<Uuid, Vec<i32>> {
    partitions
        .iter()
        .map(|(topic, list)| (*topic, list.to_vec()))
        .collect()
}

fn member(id: &str, topics: &[Uuid], current: Partitions<'_>) -> MemberSubscription {
    MemberSubscription {
        member_id: id.into(),
        rack_id: None,
        subscribed_topic_ids: topics.to_vec(),
        assigned_partitions: owned(current),
    }
}

fn group(subscription_type: SubscriptionType, members: Vec<MemberSubscription>) -> GroupSpec {
    GroupSpec {
        members,
        subscription_type,
    }
}

fn metadata(topics: &[(Uuid, i32)]) -> TopicMetadata {
    TopicMetadata {
        partitions_per_topic: topics.iter().copied().collect(),
        ..TopicMetadata::default()
    }
}

fn assignment(members: &[(&str, Partitions<'_>)]) -> Assignment {
    members
        .iter()
        .map(|(id, partitions)| ((*id).to_owned(), owned(partitions)))
        .collect()
}

struct Scenario<'a> {
    name: &'a str,
    group: GroupSpec,
    topics: TopicMetadata,
    expected: Assignment,
}

fn run(scenarios: Vec<Scenario<'_>>) {
    for scenario in scenarios {
        let actual = UniformAssignor.assign(&scenario.group, &scenario.topics);
        assert!(actual == scenario.expected, "{}", scenario.name);
        check_validity(&scenario.group, &scenario.topics, &actual);
    }
}

/// Kafka's `checkValidityAndBalance` validity half, plus completeness: each
/// partition of a subscribed topic goes to exactly one subscriber.
fn check_validity(group: &GroupSpec, topics: &TopicMetadata, actual: &Assignment) {
    let mut seen: HashSet<(Uuid, i32)> = HashSet::new();
    for member in &group.members {
        for (topic, partitions) in &actual[&member.member_id] {
            assert!(member.subscribed_topic_ids.contains(topic));
            assert!(!partitions.is_empty());
            for partition in partitions {
                assert!(
                    seen.insert((*topic, *partition)),
                    "{partition} assigned twice"
                );
            }
        }
    }
    let expected: HashSet<(Uuid, i32)> = topics
        .partitions_per_topic
        .iter()
        .filter(|(topic, _)| {
            group
                .members
                .iter()
                .any(|m| m.subscribed_topic_ids.contains(topic))
        })
        .flat_map(|(topic, count)| (0..*count).map(move |p| (*topic, p)))
        .collect();
    assert!(seen == expected);
}

fn sizes(actual: &Assignment) -> Vec<usize> {
    let mut sizes: Vec<usize> = actual
        .values()
        .map(|topics| topics.values().map(Vec::len).sum())
        .collect();
    sizes.sort_unstable();
    sizes
}

/// `UniformHomogeneousAssignmentBuilderTest`, with exact expected
/// assignments. Kafka's homogeneous builder walks topics in `HashSet` order.
/// Here topics go in ascending ID order, so each row numbers its topics to
/// reproduce the order Kafka's expectation was computed in.
#[test]
fn homogeneous_builder_matches_kafka_scenarios() {
    let (first, second) = (tid(1), tid(2));
    let run_rows = vec![
        Scenario {
            name: "testOneMemberNoTopicSubscription",
            group: group(HOMOGENEOUS, vec![member("A", &[], &[])]),
            topics: metadata(&[(first, 3)]),
            expected: assignment(&[("A", &[])]),
        },
        Scenario {
            // Kafka throws PartitionAssignorException. The reconciler
            // resolves subscriptions from the same snapshot, so a missing
            // topic is skipped instead.
            name: "testOneMemberSubscribedToNonexistentTopic",
            group: group(HOMOGENEOUS, vec![member("A", &[second], &[])]),
            topics: metadata(&[(first, 3)]),
            expected: assignment(&[("A", &[])]),
        },
        Scenario {
            // Kafka's HashSet visits topic3 before topic1.
            name: "testFirstAssignmentTwoMembersTwoTopicsNoMemberRacks",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[second, first], &[]),
                    member("B", &[second, first], &[]),
                ],
            ),
            topics: metadata(&[(second, 3), (first, 2)]),
            expected: assignment(&[("A", &[(first, &[0, 1])]), ("B", &[(second, &[0, 1, 2])])]),
        },
        Scenario {
            name: "testFirstAssignmentNumMembersGreaterThanTotalNumPartitions",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[first], &[]),
                    member("B", &[first], &[]),
                    member("C", &[first], &[]),
                ],
            ),
            topics: metadata(&[(first, 2)]),
            expected: assignment(&[("A", &[]), ("B", &[(first, &[0])]), ("C", &[(first, &[1])])]),
        },
        Scenario {
            name: "testReassignmentForTwoMembersTwoTopicsGivenUnbalancedPrevAssignment",
            group: group(
                HOMOGENEOUS,
                vec![
                    member(
                        "A",
                        &[first, second],
                        &[(first, &[0, 1]), (second, &[0, 1])],
                    ),
                    member("B", &[first, second], &[(first, &[2]), (second, &[2])]),
                ],
            ),
            topics: metadata(&[(first, 3), (second, 3)]),
            expected: assignment(&[
                ("A", &[(first, &[0, 1]), (second, &[0])]),
                ("B", &[(first, &[2]), (second, &[1, 2])]),
            ]),
        },
        Scenario {
            // Kafka's HashSet visits topic2 before topic1.
            name: "testReassignmentWhenPartitionsAreAddedForTwoMembersTwoTopics",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[second, first], &[(second, &[0, 2]), (first, &[0])]),
                    member("B", &[second, first], &[(second, &[1]), (first, &[1, 2])]),
                ],
            ),
            topics: metadata(&[(second, 6), (first, 5)]),
            expected: assignment(&[
                ("A", &[(second, &[0, 2]), (first, &[0, 3, 4])]),
                ("B", &[(second, &[1, 3, 4, 5]), (first, &[1, 2])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[first, second], &[(first, &[0, 2]), (second, &[0])]),
                    member("B", &[first, second], &[(first, &[1]), (second, &[1, 2])]),
                    member("C", &[first, second], &[]),
                ],
            ),
            topics: metadata(&[(first, 3), (second, 3)]),
            expected: assignment(&[
                ("A", &[(first, &[0, 2])]),
                ("B", &[(first, &[1]), (second, &[1])]),
                ("C", &[(second, &[0, 2])]),
            ]),
        },
        Scenario {
            // Kafka's HashSet visits topic2 before topic1.
            name: "testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithThreeMembersTwoTopics",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[second, first], &[(second, &[0]), (first, &[0])]),
                    member("B", &[second, first], &[(second, &[1]), (first, &[1])]),
                ],
            ),
            topics: metadata(&[(second, 3), (first, 3)]),
            expected: assignment(&[
                ("A", &[(second, &[0]), (first, &[0, 2])]),
                ("B", &[(second, &[1, 2]), (first, &[1])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoMembersTwoTopics",
            group: group(
                HOMOGENEOUS,
                vec![
                    member("A", &[second], &[(first, &[0]), (second, &[0])]),
                    member("B", &[second], &[(first, &[1]), (second, &[1])]),
                ],
            ),
            topics: metadata(&[(first, 2), (second, 2)]),
            expected: assignment(&[("A", &[(second, &[0])]), ("B", &[(second, &[1])])]),
        },
    ];
    run(run_rows);
}

/// `UniformHeterogeneousAssignmentBuilderTest`, with exact expected
/// assignments. Topic IDs rank topic1 < topic2 < topic3 < topic4, as Kafka's
/// test IDs do.
#[test]
fn heterogeneous_builder_matches_kafka_scenarios() {
    let (t1, t2, t3, t4) = (tid(1), tid(2), tid(3), tid(4));
    let rows = vec![
        Scenario {
            name: "testTwoMembersNoTopicSubscription",
            group: group(
                HETEROGENEOUS,
                vec![member("A", &[], &[]), member("B", &[], &[])],
            ),
            topics: metadata(&[(t1, 3)]),
            expected: empty_two_member_assignment(),
        },
        Scenario {
            // Kafka throws PartitionAssignorException; missing topics are
            // skipped here.
            name: "testTwoMembersSubscribedToNonexistentTopics",
            group: group(
                HETEROGENEOUS,
                vec![member("A", &[t3], &[]), member("B", &[t2], &[])],
            ),
            topics: metadata(&[(t1, 3)]),
            expected: empty_two_member_assignment(),
        },
        Scenario {
            name: "testFirstAssignmentTwoMembersTwoTopics",
            group: group(
                HETEROGENEOUS,
                vec![member("A", &[t1, t3], &[]), member("B", &[t3], &[])],
            ),
            topics: metadata(&[(t1, 3), (t3, 6)]),
            expected: assignment(&[
                ("A", &[(t1, &[0, 1, 2]), (t3, &[4])]),
                ("B", &[(t3, &[0, 1, 2, 3, 5])]),
            ]),
        },
        Scenario {
            name: "testFirstAssignmentNumMembersGreaterThanTotalNumPartitions",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t3], &[]),
                    member("B", &[t3], &[]),
                    member("C", &[t1], &[]),
                ],
            ),
            topics: metadata(&[(t1, 2), (t3, 1)]),
            expected: assignment(&[("A", &[(t3, &[0])]), ("B", &[]), ("C", &[(t1, &[0, 1])])]),
        },
        Scenario {
            name: "testReassignmentForTwoMembersThreeTopicsGivenUnbalancedPrevAssignment",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t1], &[(t1, &[0, 1, 2])]),
                    member("B", &[t1, t2], &[(t1, &[3]), (t2, &[0])]),
                    member(
                        "C",
                        &[t1, t2, t3],
                        &[(t1, &[4, 5]), (t2, &[1, 2, 3]), (t3, &[0, 1, 2, 3])],
                    ),
                ],
            ),
            topics: metadata(&[(t1, 6), (t2, 4), (t3, 4)]),
            expected: assignment(&[
                ("A", &[(t1, &[0, 1, 2, 5])]),
                ("B", &[(t1, &[3]), (t2, &[0, 1, 2, 3])]),
                ("C", &[(t1, &[4]), (t3, &[0, 1, 2, 3])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenPartitionsAreAddedForTwoMembers",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t1, t3], &[(t1, &[0, 1, 2, 3]), (t3, &[0, 1])]),
                    member(
                        "B",
                        &[t1, t2, t3, t4],
                        &[(t2, &[0, 1, 2]), (t4, &[0, 1, 2])],
                    ),
                ],
            ),
            topics: metadata(&[(t1, 6), (t2, 5), (t3, 3), (t4, 3)]),
            expected: assignment(&[
                ("A", &[(t1, &[0, 1, 2, 3, 4, 5]), (t3, &[0, 1, 2])]),
                ("B", &[(t2, &[0, 1, 2, 3, 4]), (t4, &[0, 1, 2])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenOneMemberAddedAndPartitionsAddedTwoMembersTwoTopics",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t1], &[(t1, &[0, 2]), (t2, &[0])]),
                    member("B", &[t1, t2], &[(t1, &[1]), (t2, &[1, 2])]),
                    member("C", &[t1, t2], &[]),
                ],
            ),
            topics: metadata(&[(t1, 6), (t2, 7)]),
            expected: assignment(&[
                ("A", &[(t1, &[0, 2, 3, 4, 5])]),
                ("B", &[(t1, &[1]), (t2, &[1, 2, 6])]),
                ("C", &[(t2, &[0, 3, 4, 5])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenOneMemberRemovedAfterInitialAssignmentWithThreeMembersThreeTopics",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t1, t3], &[(t1, &[0, 1, 2]), (t3, &[0, 1])]),
                    member("B", &[t2], &[(t2, &[3, 4, 5, 6])]),
                ],
            ),
            topics: metadata(&[(t1, 3), (t2, 8), (t3, 3)]),
            expected: assignment(&[
                ("A", &[(t1, &[0, 1, 2]), (t3, &[0, 1, 2])]),
                ("B", &[(t2, &[0, 1, 2, 3, 4, 5, 6, 7])]),
            ]),
        },
        Scenario {
            name: "testReassignmentWhenOneSubscriptionRemovedAfterInitialAssignmentWithTwoMembersTwoTopics",
            group: group(
                HETEROGENEOUS,
                vec![
                    member("A", &[t1], &[(t1, &[0, 2]), (t2, &[1, 3])]),
                    member("B", &[t1, t2], &[(t1, &[1]), (t2, &[0, 2, 4])]),
                ],
            ),
            topics: metadata(&[(t1, 3), (t2, 5)]),
            expected: assignment(&[("A", &[(t1, &[0, 1, 2])]), ("B", &[(t2, &[0, 1, 2, 3, 4])])]),
        },
        Scenario {
            name: "testReassignmentWhenTopicPartitionsRunOutAndMembersHaveNoPartitions",
            group: group(
                HETEROGENEOUS,
                vec![
                    member(
                        "A",
                        &[t1, t2, t3],
                        &[(t1, &[0, 1]), (t2, &[0, 1]), (t3, &[0, 1])],
                    ),
                    member("B", &[t1, t2, t3], &[]),
                ],
            ),
            topics: metadata(&[(t1, 2), (t2, 2), (t3, 2)]),
            expected: assignment(&[
                ("A", &[(t2, &[0]), (t3, &[0, 1])]),
                ("B", &[(t1, &[0, 1]), (t2, &[1])]),
            ]),
        },
        Scenario {
            name: "testFirstAssignmentWithTwoMembersIncludingOneWithoutSubscriptions",
            group: group(
                HETEROGENEOUS,
                vec![member("A", &[t1], &[]), member("B", &[], &[])],
            ),
            topics: metadata(&[(t1, 3)]),
            expected: assignment(&[("A", &[(t1, &[0, 1, 2])]), ("B", &[])]),
        },
    ];
    run(rows);
}

/// The review scenario: member A sits in rack r1, member B in r2, and every
/// replica of the topic's four partitions is in r1. Kafka's uniform
/// assignor does not read racks, so each member gets two partitions.
#[test]
fn replica_racks_do_not_outrank_balance() {
    let topic = tid(1);
    let members = vec![
        MemberSubscription {
            rack_id: Some("r1".into()),
            ..member("A", &[topic], &[])
        },
        MemberSubscription {
            rack_id: Some("r2".into()),
            ..member("B", &[topic], &[])
        },
    ];
    let topics = TopicMetadata {
        partitions_per_topic: [(topic, 4)].into(),
        partition_racks: (0..4).map(|p| ((topic, p), vec!["r1".into()])).collect(),
    };
    for subscription_type in [HOMOGENEOUS, HETEROGENEOUS] {
        let actual = UniformAssignor.assign(&group(subscription_type, members.clone()), &topics);
        let expected = match subscription_type {
            SubscriptionType::Homogeneous => {
                assignment(&[("A", &[(topic, &[0, 1])]), ("B", &[(topic, &[2, 3])])])
            }
            SubscriptionType::Heterogeneous => {
                assignment(&[("A", &[(topic, &[0, 2])]), ("B", &[(topic, &[1, 3])])])
            }
        };
        assert!(actual == expected, "{subscription_type:?}");
    }
}

/// A group whose type says homogeneous but whose resolved subscriptions
/// differ runs the heterogeneous builder, so no member receives a topic it
/// does not subscribe to.
#[test]
fn diverging_resolved_subscriptions_use_the_heterogeneous_builder() {
    let (t1, t2) = (tid(1), tid(2));
    let members = vec![member("A", &[t1, t2], &[]), member("B", &[t1], &[])];
    let topics = metadata(&[(t1, 2), (t2, 2)]);
    let as_homogeneous = UniformAssignor.assign(&group(HOMOGENEOUS, members.clone()), &topics);
    let as_heterogeneous = UniformAssignor.assign(&group(HETEROGENEOUS, members), &topics);
    assert!(as_homogeneous == as_heterogeneous);
    assert!(as_homogeneous == assignment(&[("A", &[(t2, &[0, 1])]), ("B", &[(t1, &[0, 1])])]));
}

/// Kafka's `testValidityAndBalanceForLargeSampleSet`: 100 topics of three
/// partitions over 49 members.
#[test]
fn homogeneous_large_sample_is_valid_and_balanced() {
    let topic_ids: Vec<Uuid> = (1..=100).map(tid).collect();
    let members: Vec<MemberSubscription> = (1..50)
        .map(|i| member(&format!("member{i}"), &topic_ids, &[]))
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: topic_ids.iter().map(|id| (*id, 3)).collect(),
        ..TopicMetadata::default()
    };
    let spec = group(HOMOGENEOUS, members);
    let actual = UniformAssignor.assign(&spec, &topics);
    check_validity(&spec, &topics, &actual);
    let sizes = sizes(&actual);
    assert!(sizes[sizes.len() - 1] - sizes[0] <= 1);
}

/// The three-topic group of Kafka's `CommonAssignorTests`.
fn common_group(subscription_type: SubscriptionType) -> (GroupSpec, TopicMetadata) {
    let topic_ids = [tid(1), tid(2), tid(3)];
    let members = ["A", "B", "C"]
        .iter()
        .map(|id| member(id, &topic_ids, &[]))
        .collect();
    let topics = metadata(&[(tid(1), 2), (tid(2), 5), (tid(3), 7)]);
    (group(subscription_type, members), topics)
}

fn with_current(spec: &GroupSpec, current: &Assignment, order: &[&str]) -> GroupSpec {
    let members = order
        .iter()
        .map(|id| {
            let member = spec
                .members
                .iter()
                .find(|member| member.member_id == *id)
                .expect("member exists");
            MemberSubscription {
                assigned_partitions: current[*id].clone(),
                ..member.clone()
            }
        })
        .collect();
    group(spec.subscription_type, members)
}

/// Kafka's `testAssignmentReuse` and `testReassignmentStickiness`: feeding an
/// assignment back in returns it unchanged, whatever order the members come
/// in.
#[test]
fn assignment_is_a_fixed_point_in_every_member_order() {
    let orders: [[&str; 3]; 6] = [
        ["A", "B", "C"],
        ["A", "C", "B"],
        ["B", "A", "C"],
        ["B", "C", "A"],
        ["C", "A", "B"],
        ["C", "B", "A"],
    ];
    for subscription_type in [HOMOGENEOUS, HETEROGENEOUS] {
        let (spec, topics) = common_group(subscription_type);
        let first = UniformAssignor.assign(&spec, &topics);
        check_validity(&spec, &topics, &first);
        for order in orders {
            let again = UniformAssignor.assign(&with_current(&spec, &first, &order), &topics);
            assert!(again == first, "{subscription_type:?} {order:?}");
        }
    }
}

/// How many partitions of `previous` each member still holds in `next`.
fn retained(previous: &Assignment, next: &Assignment, member: &str) -> usize {
    previous[member]
        .iter()
        .map(|(topic, partitions)| {
            partitions
                .iter()
                .filter(|p| next[member].get(topic).is_some_and(|now| now.contains(p)))
                .count()
        })
        .sum()
}

prop_compose! {
    /// A homogeneous group with a random but valid current assignment: up
    /// to four topics of up to eight partitions, up to five members, and
    /// each partition owned by a random member or by nobody.
    fn homogeneous_case()(
        counts in prop::collection::vec(0i32..8, 1..4),
        member_count in 1usize..6,
    )(
        owners in prop::collection::vec(
            prop::option::of(0..member_count),
            usize::try_from(counts.iter().sum::<i32>()).expect("nonnegative"),
        ),
        counts in Just(counts),
        member_count in Just(member_count),
    ) -> (GroupSpec, TopicMetadata) {
        let topic_ids: Vec<Uuid> = (1..=counts.len())
            .map(|rank| tid(u8::try_from(rank).expect("few topics")))
            .collect();
        let mut current: Vec<HashMap<Uuid, Vec<i32>>> = vec![HashMap::new(); member_count];
        let mut owner = owners.into_iter();
        for (topic, count) in topic_ids.iter().zip(&counts) {
            for partition in 0..*count {
                if let Some(Some(member)) = owner.next() {
                    current[member].entry(*topic).or_default().push(partition);
                }
            }
        }
        let members = current
            .into_iter()
            .enumerate()
            .map(|(index, assigned_partitions)| MemberSubscription {
                member_id: format!("m{index}"),
                rack_id: None,
                subscribed_topic_ids: topic_ids.clone(),
                assigned_partitions,
            })
            .collect();
        let topics = metadata(
            &topic_ids.iter().copied().zip(counts.iter().copied()).collect::<Vec<_>>(),
        );
        (group(HOMOGENEOUS, members), topics)
    }
}

proptest! {
    /// The homogeneous builder is valid, balanced within one partition, and
    /// sticky: each member keeps as many of its current partitions as its
    /// quota allows, and a second run changes nothing.
    #[test]
    fn homogeneous_is_valid_balanced_and_sticky((spec, topics) in homogeneous_case()) {
        let actual = UniformAssignor.assign(&spec, &topics);
        check_validity(&spec, &topics, &actual);
        let sizes = sizes(&actual);
        prop_assert!(sizes[sizes.len() - 1] - sizes[0] <= 1);

        let previous: Assignment = spec
            .members
            .iter()
            .map(|m| (m.member_id.clone(), m.assigned_partitions.clone()))
            .collect();
        for member in &spec.members {
            let held: usize = member.assigned_partitions.values().map(Vec::len).sum();
            let quota: usize = actual[&member.member_id].values().map(Vec::len).sum();
            prop_assert!(retained(&previous, &actual, &member.member_id) == held.min(quota));
        }

        let order: Vec<&str> = spec.members.iter().rev().map(|m| m.member_id.as_str()).collect();
        let again = UniformAssignor.assign(&with_current(&spec, &actual, &order), &topics);
        prop_assert!(again == actual);
    }

    /// With no partition moved away, the balancer's least-loaded sequence
    /// is the proved kernel's lexicographic minimum of current load,
    /// starting load and member index. The heterogeneous builder relies on
    /// this when it assigns unassigned partitions through the kernel.
    #[test]
    fn kernel_selection_matches_the_balancer(
        start in prop::collection::vec(0usize..6, 1..6),
        picks in 1usize..30,
    ) {
        let members: Vec<usize> = (0..start.len()).collect();
        let mut balancer_sizes = start.clone();
        let mut kernel_sizes = start.clone();
        let mut balancer = Balancer::new(&members, &balancer_sizes);
        for _ in 0..picks {
            let from_balancer = balancer.next_least_loaded(&balancer_sizes);
            balancer_sizes[from_balancer] += 1;
            let loads: Vec<SubscriberLoad> = kernel_sizes
                .iter()
                .zip(&start)
                .map(|(&assigned, &assigned_at_topic_start)| SubscriberLoad {
                    assigned,
                    assigned_at_topic_start,
                })
                .collect();
            let from_kernel = select_least_loaded(&loads).expect("members are non-empty");
            kernel_sizes[from_kernel] += 1;
            prop_assert!(from_kernel == from_balancer);
        }
    }
}

/// Kafka's independent expected mapping when neither fixture member gets a partition.
fn empty_two_member_assignment() -> Assignment {
    assignment(&[("A", &[]), ("B", &[])])
}
