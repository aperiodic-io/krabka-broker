//! KIP-73 throttled replication: the value types and the parser.

pub use krabka_throttle::{MICROS_PER_TOKEN, ThrottleState, TokenBucket};
use std::collections::BTreeSet;

mod refresh;
pub(crate) use refresh::apply_image;
pub use refresh::run;
use {
    krabka_metadata::{MetadataImage, NodeId},
    krabka_units::{Time, secs},
};

/// How long a throttled-replication quota remembers what it measured: Kafka's
/// `replication.quota.window.num` (11) times
/// `replication.quota.window.size.seconds` (1), after which a sample leaves
/// the window (`ReplicationQuotaManagerConfig`, whose defaults are
/// `QuotaConfig.NUM_QUOTA_SAMPLES_DEFAULT` and
/// `QuotaConfig.QUOTA_WINDOW_SIZE_SECONDS_DEFAULT`).
///
/// The bytes of an in-sync follower count against the bucket, so the debt they
/// leave stops an out-of-sync follower drawing from it. That debt is kept for
/// no longer than this window, so a burst of in-sync traffic starves a lagging
/// follower for at most the window and it cannot fall out of the ISR on a
/// debt that Kafka would already have forgotten.
pub(crate) const REPLICATION_QUOTA_WINDOW: Time = secs(11);

/// Topic-level `*.throttled.replicas` config value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledReplicas {
    /// Empty string: no replicas are throttled.
    None,
    /// `"*"` wildcard: all replicas of this topic are throttled.
    All,
    /// `"partition:broker,partition:broker,..."`: specific pairs.
    List(Vec<(i32, NodeId)>),
}

impl ThrottledReplicas {
    /// Parse a `*.throttled.replicas` value with Kafka's
    /// `ThrottledReplicaListValidator` rule. The value is split on commas and
    /// each element is trimmed. The list is the literal `*`, or a list of
    /// `[partitionId]:[brokerId]` pairs of unsigned decimal digits. An empty
    /// element matches Kafka's `([0-9]+:[0-9]+)?` and names no pair.
    ///
    /// # Errors
    /// Returns an error when an element is not an unsigned `partition:broker`
    /// pair, or when a number overflows its type.
    pub fn parse(value: &str) -> Result<Self, String> {
        let elements: Vec<&str> = value
            .split(',')
            .map(|element| element.trim_matches(|c: char| c <= ' '))
            .collect();
        if elements.concat() == "*" {
            return Ok(Self::All);
        }
        let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        let mut out = Vec::new();
        for element in elements.into_iter().filter(|element| !element.is_empty()) {
            let (p_str, n_str) = element
                .split_once(':')
                .filter(|(p, n)| digits(p) && digits(n))
                .ok_or_else(|| format!("invalid pair {element:?}"))?;
            let p: i32 = p_str.parse().map_err(|e| format!("partition: {e}"))?;
            let n: u64 = n_str.parse().map_err(|e| format!("broker: {e}"))?;
            out.push((p, NodeId(n)));
        }
        if out.is_empty() {
            return Ok(Self::None);
        }
        Ok(Self::List(out))
    }

    /// The partitions this list throttles on `broker`, as Kafka's
    /// `ConfigHandler.parseThrottledPartitions` reduces it: only the entries
    /// that name `broker` count, and `*` throttles every partition.
    ///
    /// The broker id in an entry is the replica the throttle is for, on the
    /// broker that holds it. `kafka-reassign-partitions --throttle` writes the
    /// leader list as `partition:sourceReplica` and the follower list as
    /// `partition:destinationReplica`, so a leader throttles a partition when
    /// its own id is listed for it, whichever follower fetches (#1210).
    #[must_use]
    pub fn partitions_of(&self, broker: NodeId) -> ThrottledPartitions {
        match self {
            Self::None => ThrottledPartitions::None,
            Self::All => ThrottledPartitions::All,
            Self::List(entries) => ThrottledPartitions::Listed(
                entries
                    .iter()
                    .filter(|&&(_, node)| node == broker)
                    .map(|&(partition, _)| partition)
                    .collect(),
            ),
        }
    }
}

/// The partitions of one topic that a `*.replication.throttled.replicas` list
/// throttles on one broker, which is Kafka's
/// `ReplicationQuotaManager.isThrottled(TopicPartition)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottledPartitions {
    /// No partition is throttled.
    None,
    /// Every partition is throttled: the list is `*`.
    All,
    /// The partitions the list names for this broker.
    Listed(BTreeSet<i32>),
}

impl ThrottledPartitions {
    #[must_use]
    pub fn contains(&self, partition: i32) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Listed(partitions) => partitions.contains(&partition),
        }
    }
}

/// The partitions of a topic that the leader-side and the follower-side
/// throttled-replicas lists throttle on one broker.
#[derive(Debug, Clone)]
pub struct TopicThrottle {
    pub leader: ThrottledPartitions,
    pub follower: ThrottledPartitions,
}

impl TopicThrottle {
    /// The throttles of `topic` on the broker `broker`, which is the broker
    /// that reads the lists: Kafka applies them per broker
    /// (`ConfigHandler.processConfigChanges`).
    #[must_use]
    pub fn for_topic(image: &MetadataImage, topic: &str, broker: NodeId) -> Self {
        let configs = image.topic_config(topic);
        let read = |key: &str| -> ThrottledPartitions {
            configs
                .and_then(|c| c.get(key))
                .and_then(|v| ThrottledReplicas::parse(v).ok())
                .unwrap_or(ThrottledReplicas::None)
                .partitions_of(broker)
        };
        Self {
            leader: read(LEADER_THROTTLED_REPLICAS_KEY),
            follower: read(FOLLOWER_THROTTLED_REPLICAS_KEY),
        }
    }
}

pub const LEADER_THROTTLED_REPLICAS_KEY: &str = "leader.replication.throttled.replicas";
pub const FOLLOWER_THROTTLED_REPLICAS_KEY: &str = "follower.replication.throttled.replicas";
pub const LEADER_THROTTLED_RATE_KEY: &str = "leader.replication.throttled.rate";
pub const FOLLOWER_THROTTLED_RATE_KEY: &str = "follower.replication.throttled.rate";
pub const ALTER_LOG_DIRS_THROTTLED_RATE_KEY: &str =
    "replica.alter.log.dirs.io.max.bytes.per.second";

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn empty_string_parses_as_none() {
        assert!(ThrottledReplicas::parse("").unwrap() == ThrottledReplicas::None);
    }

    #[test]
    fn wildcard_parses_as_all() {
        assert!(ThrottledReplicas::parse("*").unwrap() == ThrottledReplicas::All);
    }

    #[test]
    fn pairs_parse_in_order() {
        for (input, want) in [
            ("0:1", vec![(0, NodeId(1))]),
            ("0:1,0:2,1:3", vec![
                (0, NodeId(1)),
                (0, NodeId(2)),
                (1, NodeId(3)),
            ]),
        ] {
            assert!(
                ThrottledReplicas::parse(input) == Ok(ThrottledReplicas::List(want)),
                "{input}"
            );
        }
    }

    /// Each broker reads only the entries that name it, as Kafka's
    /// `ConfigHandler.parseThrottledPartitions` filters on its own broker id,
    /// and `*` throttles every partition (#1210).
    #[test]
    fn a_broker_keeps_only_the_entries_that_name_it() {
        let listed =
            |partitions: &[i32]| ThrottledPartitions::Listed(partitions.iter().copied().collect());
        // (list, broker, partitions it throttles there)
        let cases = [
            ("", 1, ThrottledPartitions::None),
            ("*", 1, ThrottledPartitions::All),
            ("*", 9, ThrottledPartitions::All),
            ("0:1,0:2,1:3", 1, listed(&[0])),
            ("0:1,0:2,1:3", 2, listed(&[0])),
            ("0:1,0:2,1:3", 3, listed(&[1])),
            ("0:1,0:2,1:3", 4, listed(&[])),
            ("0:1,2:1,5:1,3:2", 1, listed(&[0, 2, 5])),
        ];
        for (list, broker, want) in cases {
            let parsed = ThrottledReplicas::parse(list).unwrap();
            assert!(
                parsed.partitions_of(NodeId(broker)) == want,
                "{list:?} on broker {broker}"
            );
        }
    }

    #[test]
    fn a_listed_partition_is_throttled_and_no_other() {
        let listed = ThrottledPartitions::Listed([0, 3].into_iter().collect());
        for (partition, want) in [(0, true), (1, false), (3, true), (4, false)] {
            assert!(listed.contains(partition) == want, "partition {partition}");
        }
        assert!(ThrottledPartitions::All.contains(7));
        assert!(!ThrottledPartitions::None.contains(0));
    }

    /// `kafka-reassign-partitions --throttle` writes the leader list as
    /// `partition:sourceReplica` and the follower list as
    /// `partition:destinationReplica`. Moving partition 0 from broker 1 to
    /// broker 3 throttles the source broker's outbound replication and the
    /// destination broker's inbound one, and no other broker's (#1210).
    #[test]
    fn a_reassignment_throttle_lands_on_the_source_and_the_destination() {
        use krabka_metadata::{MetadataRecord, TopicConfigRecord};

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "moved".into(),
            overrides: [
                (LEADER_THROTTLED_REPLICAS_KEY.to_owned(), "0:1".to_owned()),
                (FOLLOWER_THROTTLED_REPLICAS_KEY.to_owned(), "0:3".to_owned()),
            ]
            .into_iter()
            .collect(),
        }));

        // (broker, throttles its outbound replication, its inbound replication)
        for (broker, leader, follower) in [(1, true, false), (2, false, false), (3, false, true)] {
            let throttle = TopicThrottle::for_topic(&image, "moved", NodeId(broker));
            assert!(
                (throttle.leader.contains(0), throttle.follower.contains(0)) == (leader, follower),
                "broker {broker}"
            );
        }
    }

    #[test]
    fn malformed_pair_rejected() {
        // Kafka's `([0-9]+:[0-9]+)?` has no sign and no space around the colon.
        for input in ["not-a-pair", "0:x", "x:1", "-1:1", "+0:1", "0 : 1"] {
            assert!(ThrottledReplicas::parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn kafka_list_spellings_parse_as_kafka_reads_them() {
        let cases = [
            (" ", ThrottledReplicas::None),
            (" * ", ThrottledReplicas::All),
            (
                " 0:1 , 2:3 ",
                ThrottledReplicas::List(vec![(0, NodeId(1)), (2, NodeId(3))]),
            ),
            (
                "0:1,,1:2",
                ThrottledReplicas::List(vec![(0, NodeId(1)), (1, NodeId(2))]),
            ),
        ];
        for (input, want) in cases {
            assert!(ThrottledReplicas::parse(input) == Ok(want), "{input:?}");
        }
    }
}
