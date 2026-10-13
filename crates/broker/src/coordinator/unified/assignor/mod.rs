//! Server-side assignors (KIP-848). Each implementation maps a group spec
//! (members, their subscriptions and their current target assignments) and
//! the topic metadata to per-member partition assignments.

pub mod range;
pub mod uniform;

use {
    krabka_protocol::primitives::uuid::Uuid,
    std::collections::{HashMap, HashSet},
};
pub use {range::RangeAssignor, uniform::UniformAssignor};

#[derive(Debug, Clone)]
pub struct MemberSubscription {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub subscribed_topic_ids: Vec<Uuid>,
    /// The member's current target assignment, which is Kafka's
    /// `GroupSpec.memberAssignment`. A new member has none. The sticky
    /// assignors keep as much of it as balance allows.
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

/// Kafka's `SubscriptionType`. It says whether every member subscribes to
/// the same topics, and it selects the builder the uniform assignor runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionType {
    Homogeneous,
    Heterogeneous,
}

/// Kafka's `GroupSpec`: the members and the group's subscription type.
#[derive(Debug, Clone)]
pub struct GroupSpec {
    pub members: Vec<MemberSubscription>,
    pub subscription_type: SubscriptionType,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub partitions_per_topic: HashMap<Uuid, i32>,
    /// Per-`(topic_id, partition_index)` set of racks on which the partition
    /// has at least one replica. The value is empty, or the key is missing,
    /// for partitions whose replicas have no rack info. This is Kafka's
    /// `SubscribedTopicDescriber.racksForPartition`. The built-in uniform and
    /// range assignors do not read it, as in Kafka.
    pub partition_racks: HashMap<TopicIdPartition, Vec<String>>,
}

/// A partition named by its topic id and partition index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicIdPartition {
    pub topic_id: Uuid,
    pub partition: i32,
}

pub type Assignment = HashMap<String, HashMap<Uuid, Vec<i32>>>;

pub trait Assignor: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn assign(&self, group: &GroupSpec, topics: &TopicMetadata) -> Assignment;
}

/// One member's subscription, in the terms that Kafka's
/// `ConsumerGroup.subscriptionType` counts.
#[derive(Debug, Clone)]
pub struct SubscriptionShape<'a> {
    /// The topic names the member subscribes to by name, whether or not the
    /// topics exist.
    pub topic_names: &'a HashSet<String>,
    /// The member's subscribed regular expression, if it has one.
    pub topic_regex: Option<&'a str>,
    /// The topic names that `topic_regex` resolves to for this member.
    pub regex_topic_names: Vec<&'a str>,
}

/// The group's subscription type, as Kafka's `ConsumerGroup.subscriptionType`
/// computes it.
///
/// Without regular expressions, the group is homogeneous when every
/// subscribed topic name has every member as a by-name subscriber. With
/// regular expressions, it is homogeneous only when all members share one
/// regular expression and every subscribed topic comes from that expression
/// alone. A group with no subscribed topic is homogeneous.
#[must_use]
pub fn subscription_type(members: &[SubscriptionShape<'_>]) -> SubscriptionType {
    // Kafka's `subscribedRegularExpressions`: each regex with its member count.
    let mut regex_members: HashMap<&str, usize> = HashMap::new();
    // Kafka's `SubscriptionCount.byNameCount`, per topic name.
    let mut by_name: HashMap<&str, usize> = HashMap::new();
    // The topic names each distinct regex resolves to, for
    // `SubscriptionCount.byRegexCount`.
    let mut regex_topics: HashMap<&str, HashSet<&str>> = HashMap::new();
    for member in members {
        for name in member.topic_names {
            *by_name.entry(name.as_str()).or_default() += 1;
        }
        if let Some(regex) = member.topic_regex {
            *regex_members.entry(regex).or_default() += 1;
            regex_topics
                .entry(regex)
                .or_default()
                .extend(member.regex_topic_names.iter().copied());
        }
    }
    let mut by_regex: HashMap<&str, usize> = HashMap::new();
    for names in regex_topics.values() {
        for name in names {
            *by_regex.entry(name).or_default() += 1;
        }
    }

    let member_count = members.len();
    let homogeneous = match regex_members.len() {
        0 => by_name.values().all(|&count| count == member_count),
        // A member holds at most one regex, so a regex that every member
        // holds is the only one.
        1 if regex_members.values().all(|&count| count == member_count) => {
            by_name.is_empty() && by_regex.values().all(|&count| count == 1)
        }
        _ => false,
    };
    if homogeneous {
        SubscriptionType::Homogeneous
    } else {
        SubscriptionType::Heterogeneous
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{SubscriptionShape, SubscriptionType, subscription_type},
        assert2::assert,
        std::collections::HashSet,
    };

    struct Row<'a> {
        name: &'static str,
        members: Vec<SubscriptionShape<'a>>,
        expected: SubscriptionType,
    }

    fn names(list: &[&str]) -> HashSet<String> {
        list.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn subscription_type_matches_kafka_consumer_group_rule() {
        let ab = names(&["a", "b"]);
        let a = names(&["a"]);
        let missing = names(&["a", "b", "missing"]);
        let none = names(&[]);
        let by_names = |topic_names| SubscriptionShape {
            topic_names,
            topic_regex: None,
            regex_topic_names: Vec::new(),
        };
        let by_regex = |topic_names, regex, resolved: &[&'static str]| SubscriptionShape {
            topic_names,
            topic_regex: Some(regex),
            regex_topic_names: resolved.to_vec(),
        };
        let rows: Vec<Row<'_>> = vec![
            Row {
                name: "no members",
                members: vec![],
                expected: SubscriptionType::Homogeneous,
            },
            Row {
                name: "no subscriptions",
                members: vec![by_names(&none), by_names(&none)],
                expected: SubscriptionType::Homogeneous,
            },
            Row {
                name: "same names",
                members: vec![by_names(&ab), by_names(&ab)],
                expected: SubscriptionType::Homogeneous,
            },
            Row {
                name: "different names",
                members: vec![by_names(&ab), by_names(&a)],
                expected: SubscriptionType::Heterogeneous,
            },
            Row {
                // Kafka counts names, so a missing topic still splits the group.
                name: "one member also names a missing topic",
                members: vec![by_names(&ab), by_names(&missing)],
                expected: SubscriptionType::Heterogeneous,
            },
            Row {
                name: "one shared regex",
                members: vec![
                    by_regex(&none, "t.*", &["a", "b"]),
                    by_regex(&none, "t.*", &["a", "b"]),
                ],
                expected: SubscriptionType::Homogeneous,
            },
            Row {
                name: "unresolved shared regex",
                members: vec![by_regex(&none, "t.*", &[]), by_regex(&none, "t.*", &[])],
                expected: SubscriptionType::Homogeneous,
            },
            Row {
                name: "two regexes that resolve alike",
                members: vec![
                    by_regex(&none, "t.*", &["a", "b"]),
                    by_regex(&none, "[ab]", &["a", "b"]),
                ],
                expected: SubscriptionType::Heterogeneous,
            },
            Row {
                name: "regex and names that resolve alike",
                members: vec![by_regex(&none, "t.*", &["a", "b"]), by_names(&ab)],
                expected: SubscriptionType::Heterogeneous,
            },
            Row {
                name: "shared regex plus a name",
                members: vec![
                    by_regex(&a, "t.*", &["a", "b"]),
                    by_regex(&none, "t.*", &["a", "b"]),
                ],
                expected: SubscriptionType::Heterogeneous,
            },
        ];
        for Row {
            name,
            members,
            expected,
        } in rows
        {
            assert!(subscription_type(&members) == expected, "{name}");
        }
    }
}
