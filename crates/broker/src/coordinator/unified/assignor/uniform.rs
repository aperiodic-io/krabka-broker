//! `UniformAssignor`, the default of KIP-848. It ports Kafka's server-side
//! `org.apache.kafka.coordinator.group.assignor.UniformAssignor`.
//!
//! The assignor ranks balance ahead of stickiness. Every member ends with a
//! partition count within one of every other member's where the
//! subscriptions allow it, and a member keeps as much of its current target
//! assignment as that balance leaves room for. It does not consult replica
//! racks, as in Kafka.
//!
//! Two builders do the work, as in Kafka:
//!
//! - `homogeneous` ports `UniformHomogeneousAssignmentBuilder`. It runs when
//!   the group's [`SubscriptionType`] is homogeneous and every member's
//!   resolved topic IDs agree. Regex authorization is per member here, so a
//!   group that Kafka's rule calls homogeneous can still resolve to different
//!   topics, and then the heterogeneous builder keeps the result valid. The
//!   group-wide quota split and each member's quota come from the proved
//!   kernels [`uniform_quota_split`] and [`homogeneous_member_quotas`].
//! - `heterogeneous` ports `UniformHeterogeneousAssignmentBuilder`. The
//!   subscriber that receives each unassigned partition comes from the proved
//!   kernel [`select_least_loaded`]. The final balancing pass ports
//!   `MemberAssignmentBalancer` step for step.
//!
//! Kafka iterates members and homogeneous topics in hash order, so its tests
//! pin them with sorted maps. This port fixes both orders: members in
//! ascending member-ID order, and topics in ascending order of Kafka's
//! `Uuid.compareTo`.
//!
//! Kafka rejects a subscription to a topic that is missing from the metadata.
//! Here the reconciler resolves subscriptions from the same metadata
//! snapshot, so a subscribed topic that is missing, or that has a negative
//! partition count, is skipped instead. A current assignment entry outside a
//! member's subscribed topics or outside the partition range is dropped, and
//! a partition that two members claim stays with the first of them.

use {
    super::{Assignment, Assignor, GroupSpec, SubscriptionType, TopicMetadata},
    krabka_protocol::primitives::uuid::Uuid,
    krabka_verified::uniform_assignor::{
        SubscriberLoad, homogeneous_member_quotas, select_least_loaded, uniform_quota_split,
    },
    std::{cmp::Ordering, collections::HashMap},
};

#[derive(Debug)]
pub struct UniformAssignor;

impl Assignor for UniformAssignor {
    fn name(&self) -> &'static str { "uniform" }

    fn assign(&self, group: &GroupSpec, topics: &TopicMetadata) -> Assignment {
        let mut spec = Spec::new(group, topics);
        let homogeneous_group = group.subscription_type == SubscriptionType::Homogeneous
            && spec.subscriptions.windows(2).all(|pair| pair[0] == pair[1]);
        if homogeneous_group {
            homogeneous(&mut spec);
        } else {
            heterogeneous(&mut spec);
        }
        spec.into_assignment()
    }
}

/// Kafka's `Uuid.compareTo`: the most significant 64 bits as a signed long,
/// then the least significant 64 bits as a signed long.
fn kafka_uuid_order(a: &Uuid, b: &Uuid) -> Ordering {
    let halves = |uuid: &Uuid| {
        let (most, least) = uuid.0.split_at(8);
        (
            i64::from_be_bytes(most.try_into().expect("8 bytes")),
            i64::from_be_bytes(least.try_into().expect("8 bytes")),
        )
    };
    halves(a).cmp(&halves(b))
}

/// An existing topic and its partition count.
#[derive(Debug, Clone, Copy)]
struct TopicSize {
    topic_id: Uuid,
    partitions: usize,
}

/// One partition, named by its topic's number in [`Spec::topics`] and its
/// index within that topic.
#[derive(Debug, Clone, Copy)]
struct PartitionSlot {
    topic: usize,
    partition: usize,
}

/// The assignor's working state. Members and topics are numbered by their
/// position in `member_ids` and `topics`.
#[derive(Debug)]
struct Spec<'a> {
    /// Member IDs in ascending order.
    member_ids: Vec<&'a str>,
    /// Existing topics with their partition counts, in Kafka `Uuid` order.
    topics: Vec<TopicSize>,
    /// Each member's subscribed topics that exist, as ascending topic numbers.
    subscriptions: Vec<Vec<usize>>,
    /// The owner of each partition of each topic, or `None` while unassigned.
    owners: Vec<Vec<Option<usize>>>,
}

impl<'a> Spec<'a> {
    fn new(group: &'a GroupSpec, metadata: &TopicMetadata) -> Self {
        let mut members: Vec<_> = group.members.iter().collect();
        members.sort_unstable_by(|a, b| a.member_id.cmp(&b.member_id));

        let mut topics: Vec<TopicSize> = metadata
            .partitions_per_topic
            .iter()
            .filter_map(|(id, count)| {
                Some(TopicSize {
                    topic_id: *id,
                    partitions: usize::try_from(*count).ok()?,
                })
            })
            .collect();
        topics.sort_unstable_by(|a, b| kafka_uuid_order(&a.topic_id, &b.topic_id));
        let topic_numbers: HashMap<Uuid, usize> = topics
            .iter()
            .enumerate()
            .map(|(number, topic)| (topic.topic_id, number))
            .collect();

        let subscriptions: Vec<Vec<usize>> = members
            .iter()
            .map(|member| {
                let mut subscribed: Vec<usize> = member
                    .subscribed_topic_ids
                    .iter()
                    .filter_map(|id| topic_numbers.get(id).copied())
                    .collect();
                subscribed.sort_unstable();
                subscribed.dedup();
                subscribed
            })
            .collect();

        let mut owners: Vec<Vec<Option<usize>>> = topics
            .iter()
            .map(|topic| vec![None; topic.partitions])
            .collect();
        for (member, (subscription, spec)) in subscriptions.iter().zip(&members).enumerate() {
            for (topic_id, partitions) in &spec.assigned_partitions {
                let Some(&topic) = topic_numbers.get(topic_id) else {
                    continue;
                };
                if subscription.binary_search(&topic).is_err() {
                    continue;
                }
                for &partition in partitions {
                    let owner = usize::try_from(partition)
                        .ok()
                        .and_then(|index| owners[topic].get_mut(index));
                    if let Some(owner @ None) = owner {
                        *owner = Some(member);
                    }
                }
            }
        }

        Spec {
            member_ids: members
                .iter()
                .map(|member| member.member_id.as_str())
                .collect(),
            topics,
            subscriptions,
            owners,
        }
    }

    /// The partitions `member` owns, in topic order and then partition order.
    fn owned_by(&self, member: usize) -> Vec<PartitionSlot> {
        self.subscriptions[member]
            .iter()
            .flat_map(|&topic| {
                self.owners[topic]
                    .iter()
                    .enumerate()
                    .filter(move |(_, owner)| **owner == Some(member))
                    .map(move |(partition, _)| PartitionSlot { topic, partition })
            })
            .collect()
    }

    fn into_assignment(self) -> Assignment {
        let mut out: Assignment = self
            .member_ids
            .iter()
            .map(|id| ((*id).to_owned(), HashMap::new()))
            .collect();
        for (topic, owners) in self.topics.iter().zip(&self.owners) {
            for (partition, owner) in owners.iter().enumerate() {
                if let Some(member) = owner {
                    out.get_mut(self.member_ids[*member])
                        .expect("every member has an entry")
                        .entry(topic.topic_id)
                        .or_default()
                        .push(i32::try_from(partition).expect("partition counts are i32"));
                }
            }
        }
        out
    }
}

/// `UniformHomogeneousAssignmentBuilder.build`: every member subscribes to
/// the same topics.
///
/// Each member's quota is the floor or the ceiling of the partition total
/// over the members. A member keeps its current partitions up to its quota
/// and gives the rest back. The unassigned partitions, in topic order and
/// then partition order, followed by the given-back ones, go to the members
/// with room, in member order.
fn homogeneous(spec: &mut Spec<'_>) {
    let Some(subscribed) = spec.subscriptions.first().cloned() else {
        return;
    };
    let total: usize = subscribed
        .iter()
        .map(|&topic| spec.topics[topic].partitions)
        .sum();
    let split = uniform_quota_split(total, spec.member_ids.len());

    let mut unassigned: Vec<PartitionSlot> = subscribed
        .iter()
        .flat_map(|&topic| {
            spec.owners[topic]
                .iter()
                .enumerate()
                .filter(|(_, owner)| owner.is_none())
                .map(move |(partition, _)| PartitionSlot { topic, partition })
        })
        .collect();
    let owned: Vec<Vec<PartitionSlot>> = (0..spec.member_ids.len())
        .map(|member| spec.owned_by(member))
        .collect();
    let counts: Vec<usize> = owned.iter().map(Vec::len).collect();
    let quotas = homogeneous_member_quotas(split.minimum_quota, split.extra_quotas, &counts);

    for (partitions, quota) in owned.iter().zip(&quotas) {
        for &slot in &partitions[quota.retain..] {
            spec.owners[slot.topic][slot.partition] = None;
            unassigned.push(slot);
        }
    }
    // The proved quotas add up to the partition total, and every partition
    // is either retained or unassigned here, so the fills use up the list.
    let mut next = unassigned.into_iter();
    for (member, quota) in quotas.iter().enumerate() {
        for slot in next.by_ref().take(quota.fill) {
            spec.owners[slot.topic][slot.partition] = Some(member);
        }
    }
}

/// The maximum number of passes of the final balancing phase, as in Kafka.
const MAX_ITERATION_COUNT: usize = 16;

/// `UniformHeterogeneousAssignmentBuilder.build`: members subscribe to
/// different topics.
///
/// Members keep every current partition of the topics they still subscribe
/// to. The unassigned partitions go, topic by topic, to the least-loaded
/// subscriber. A final pass then moves partitions from the most-loaded to the
/// least-loaded subscribers of each topic until no move improves balance.
fn heterogeneous(spec: &mut Spec<'_>) {
    let mut builder = Heterogeneous::new(spec);
    builder.assign_remaining_partitions();
    builder.balance();
}

#[derive(Debug)]
struct Heterogeneous<'s, 'a> {
    spec: &'s mut Spec<'a>,
    /// The subscribers of each topic, in ascending member order.
    topic_subscribers: Vec<Vec<usize>>,
    /// Each member's partition count across every topic.
    sizes: Vec<usize>,
}

impl<'s, 'a> Heterogeneous<'s, 'a> {
    fn new(spec: &'s mut Spec<'a>) -> Self {
        let mut topic_subscribers = vec![Vec::new(); spec.topics.len()];
        for (member, subscription) in spec.subscriptions.iter().enumerate() {
            for &topic in subscription {
                topic_subscribers[topic].push(member);
            }
        }
        let mut sizes = vec![0; spec.member_ids.len()];
        for owner in spec.owners.iter().flatten().flatten() {
            sizes[*owner] += 1;
        }
        Heterogeneous {
            spec,
            topic_subscribers,
            sizes,
        }
    }

    /// `sortTopicIds`: partitions per subscriber descending, then subscriber
    /// count ascending, then topic ID ascending.
    fn sort_topics(&self, topics: &mut [usize]) {
        let per_subscriber = |topic: usize| {
            let partitions = u32::try_from(self.spec.topics[topic].partitions).expect("i32 count");
            let subscribers =
                u32::try_from(self.topic_subscribers[topic].len()).expect("member count fits u32");
            f64::from(partitions) / f64::from(subscribers)
        };
        topics.sort_by(|&a, &b| {
            per_subscriber(b)
                .total_cmp(&per_subscriber(a))
                .then_with(|| {
                    self.topic_subscribers[a]
                        .len()
                        .cmp(&self.topic_subscribers[b].len())
                })
                .then_with(|| {
                    kafka_uuid_order(&self.spec.topics[a].topic_id, &self.spec.topics[b].topic_id)
                })
        });
    }

    fn assign_partition(&mut self, topic: usize, partition: usize, member: usize) {
        if let Some(previous) = self.spec.owners[topic][partition] {
            self.sizes[previous] -= 1;
        }
        self.spec.owners[topic][partition] = Some(member);
        self.sizes[member] += 1;
    }

    /// `assignRemainingPartitions`.
    fn assign_remaining_partitions(&mut self) {
        let mut topics: Vec<usize> = (0..self.spec.topics.len())
            .filter(|&topic| {
                !self.topic_subscribers[topic].is_empty()
                    && self.spec.owners[topic].iter().any(Option::is_none)
            })
            .collect();
        self.sort_topics(&mut topics);
        let mut loads = Vec::new();
        for topic in topics {
            let subscribers = self.topic_subscribers[topic].clone();
            let start: Vec<usize> = subscribers.iter().map(|&m| self.sizes[m]).collect();
            for partition in 0..self.spec.owners[topic].len() {
                if self.spec.owners[topic][partition].is_some() {
                    continue;
                }
                loads.clear();
                loads.extend(subscribers.iter().zip(&start).map(|(&member, &at_start)| {
                    SubscriberLoad {
                        assigned: self.sizes[member],
                        assigned_at_topic_start: at_start,
                    }
                }));
                let chosen = select_least_loaded(&loads).expect("the topic has subscribers");
                self.assign_partition(topic, partition, subscribers[chosen]);
            }
        }
    }

    /// `balance` and `balanceTopics`: pass over the topics with two or more
    /// subscribers until a full pass moves nothing, or the pass limit.
    fn balance(&mut self) {
        let mut topics: Vec<usize> = (0..self.spec.topics.len())
            .filter(|&topic| self.topic_subscribers[topic].len() >= 2)
            .collect();
        if topics.is_empty() {
            return;
        }
        self.sort_topics(&mut topics);
        let mut last_rebalanced: Option<usize> = None;
        for _ in 0..MAX_ITERATION_COUNT {
            for (index, &topic) in topics.iter().enumerate() {
                if last_rebalanced == Some(index) {
                    return;
                }
                if self.balance_topic(topic) > 0 || last_rebalanced.is_none() {
                    last_rebalanced = Some(index);
                }
            }
        }
    }

    /// `balanceTopic`: move the topic's partitions from its most-loaded
    /// subscribers to its least-loaded ones. Returns how many moved.
    fn balance_topic(&mut self, topic: usize) -> usize {
        let mut balancer = Balancer::new(&self.topic_subscribers[topic], &self.sizes);
        if balancer.imbalance(&self.sizes) <= 1 {
            return 0;
        }

        // The topic's partitions grouped by owner, and each owner's range.
        let owners = &self.spec.owners[topic];
        let mut partitions: Vec<usize> = (0..owners.len()).collect();
        partitions.sort_by_key(|&partition| (owners[partition], partition));
        let mut start: HashMap<usize, usize> = HashMap::new();
        let mut end: HashMap<usize, usize> = HashMap::new();
        for (index, &partition) in partitions.iter().enumerate() {
            if let Some(owner) = owners[partition] {
                start.entry(owner).or_insert(index);
                end.insert(owner, index + 1);
            }
        }

        let mut moved = 0;
        while !balancer.is_balanced() {
            let most_loaded = loop {
                let Some(member) = balancer.next_most_loaded(&self.sizes) else {
                    break None;
                };
                match (start.get(&member), end.get(&member)) {
                    (Some(first), Some(last)) if last > first => break Some(member),
                    _ => balancer.exclude_most_loaded(),
                }
            };
            let Some(most_loaded) = most_loaded else {
                break;
            };
            let last = end.get_mut(&most_loaded).expect("checked above");
            *last -= 1;
            let partition = partitions[*last];
            let least_loaded = balancer.next_least_loaded(&self.sizes);
            if balancer.is_balanced() {
                break;
            }
            self.assign_partition(topic, partition, least_loaded);
            moved += 1;
        }
        moved
    }
}

/// Kafka's `MemberAssignmentBalancer`, over one topic's subscribers.
///
/// It keeps two ranges of the subscribers sorted by load: the least-loaded
/// range grows to the right one level at a time, and the most-loaded range
/// grows to the left. Balancing ends when the two levels are within one.
#[derive(Debug)]
struct Balancer {
    sorted_members: Vec<usize>,
    /// `[0, least_loaded_range_end)` is the least-loaded range.
    least_loaded_range_end: usize,
    least_loaded_range_partition_count: i64,
    next_least_loaded_member: usize,
    /// `[most_loaded_range_start, most_loaded_range_end)` is the most-loaded
    /// range.
    most_loaded_range_start: usize,
    most_loaded_range_end: usize,
    most_loaded_range_partition_count: i64,
    /// The next position to take from, counting down. `None` is Kafka's -1.
    next_most_loaded_member: Option<usize>,
}

fn level(size: usize) -> i64 { i64::try_from(size).expect("partition counts fit i64") }

impl Balancer {
    /// `MemberAssignmentBalancer.initialize`.
    fn new(members: &[usize], sizes: &[usize]) -> Self {
        let mut sorted_members = members.to_vec();
        sorted_members.sort_by_key(|&member| (sizes[member], member));
        let len = sorted_members.len();
        Balancer {
            least_loaded_range_end: 0,
            least_loaded_range_partition_count: level(sizes[sorted_members[0]]) - 1,
            next_least_loaded_member: 0,
            most_loaded_range_start: len,
            most_loaded_range_end: len,
            most_loaded_range_partition_count: level(sizes[sorted_members[len - 1]]) + 1,
            next_most_loaded_member: Some(len - 1),
            sorted_members,
        }
    }

    /// The load gap between the most and the least loaded member, which
    /// `initialize` returns.
    fn imbalance(&self, sizes: &[usize]) -> usize {
        let first = self.sorted_members[0];
        let last = self.sorted_members[self.sorted_members.len() - 1];
        sizes[last] - sizes[first]
    }

    /// `nextLeastLoadedMember`.
    fn next_least_loaded(&mut self, sizes: &[usize]) -> usize {
        if self.next_least_loaded_member >= self.least_loaded_range_end {
            self.least_loaded_range_partition_count += 1;
            while self.least_loaded_range_end < self.sorted_members.len()
                && level(sizes[self.sorted_members[self.least_loaded_range_end]])
                    == self.least_loaded_range_partition_count
            {
                self.least_loaded_range_end += 1;
            }
            self.next_least_loaded_member = 0;
        }
        let member = self.sorted_members[self.next_least_loaded_member];
        self.next_least_loaded_member += 1;
        member
    }

    /// `nextMostLoadedMember`.
    fn next_most_loaded(&mut self, sizes: &[usize]) -> Option<usize> {
        if self
            .next_most_loaded_member
            .is_none_or(|next| next < self.most_loaded_range_start)
        {
            if self.most_loaded_range_end <= self.most_loaded_range_start
                && self.most_loaded_range_start > 0
            {
                self.most_loaded_range_partition_count =
                    level(sizes[self.sorted_members[self.most_loaded_range_start - 1]]);
            } else {
                self.most_loaded_range_partition_count -= 1;
            }
            while self.most_loaded_range_start > 0
                && level(sizes[self.sorted_members[self.most_loaded_range_start - 1]])
                    == self.most_loaded_range_partition_count
            {
                self.most_loaded_range_start -= 1;
            }
            self.next_most_loaded_member = self.most_loaded_range_end.checked_sub(1);
        }
        let next = self.next_most_loaded_member?;
        self.next_most_loaded_member = next.checked_sub(1);
        Some(self.sorted_members[next])
    }

    /// `excludeMostLoadedMember`: swap the member last returned by
    /// `next_most_loaded` to the end of the most-loaded range, and shrink it.
    fn exclude_most_loaded(&mut self) {
        let returned = self.next_most_loaded_member.map_or(0, |next| next + 1);
        self.sorted_members
            .swap(returned, self.most_loaded_range_end - 1);
        self.most_loaded_range_end -= 1;
    }

    /// `isBalanced`.
    fn is_balanced(&self) -> bool {
        self.most_loaded_range_partition_count - self.least_loaded_range_partition_count <= 1
    }
}

#[cfg(test)]
mod tests;
