//! The topics a chaos run writes to, what each one is for, and the entry
//! limits the brokers run with.
//!
//! Every topic has three replicas and `min.insync.replicas=2`, and unclean
//! leader election is off, so an acknowledgement with `acks=all` is a promise
//! that the record survives any one broker. Each partition prefers a
//! different broker, so a preferred election after a failover moves
//! leadership somewhere.

use std::fmt;

/// What makes two records duplicates on a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    /// The record key.
    Key,
    /// The value of the `dedup.key.header` header.
    Header,
}

/// Where the identities of a topic's records come from.
#[derive(Clone, Copy, Debug)]
pub enum Identities {
    /// Keys drawn from `keys` names and, when `headers` is not 0, dedup
    /// header values drawn from `headers` names. A small pool makes most
    /// sends duplicates of something sent earlier, by any producer.
    Pool { keys: u64, headers: u64 },
    /// A key no record had before, from each producer's own counter.
    Fresh,
}

/// One topic of the run.
#[derive(Debug)]
pub struct TopicSpec {
    pub name: &'static str,
    /// The replicas of each partition, the preferred leader first.
    pub replicas: &'static [[i32; 3]],
    pub window_ms: i64,
    pub rule: Rule,
    pub identities: Identities,
    /// Whether the first-wins check covers the topic. It does not cover a
    /// topic whose window expires during the run, nor one that holds more
    /// identities than a partition index may keep, since the index then
    /// evicts some and admits their duplicates by design.
    pub unique: bool,
    /// Whether the config faults change this topic's rule, window and
    /// enabled state. A change takes the topic out of the first-wins check.
    pub mutable: bool,
}

/// The window of every topic that is not about expiry: an hour, longer than
/// any run that `DEDUP_CHAOS_SECS` asks for in reason, so nothing expires by
/// accident.
pub const LONG_WINDOW_MS: i64 = 3_600_000;

/// The window of the topic whose entries expire while the run goes on.
pub const SHORT_WINDOW_MS: i64 = 3_000;

/// `dedup_max_entries_per_partition` on every broker. The pools of the
/// unique topics stay below it, and the `chaos-capacity` topic goes past it.
pub const PER_PARTITION_LIMIT: u64 = 1_500;

/// `dedup_max_entries` on every broker. Every partition of the run at its own
/// limit still fits, so the broker-wide limit never evicts an entry of a
/// unique topic: 2 x 1200 + 1200 + 600 + 150 + 1500 = 5850 entries at most.
pub const BROKER_LIMIT: u64 = 9_000;

/// The topic the config faults change.
pub const MUTABLE_TOPIC: usize = 2;

pub const TOPICS: [TopicSpec; 5] = [
    TopicSpec {
        name: "chaos-key",
        replicas: &[[1, 2, 3], [2, 3, 1]],
        window_ms: LONG_WINDOW_MS,
        rule: Rule::Key,
        identities: Identities::Pool {
            keys: 1_200,
            headers: 0,
        },
        unique: true,
        mutable: false,
    },
    TopicSpec {
        name: "chaos-header",
        replicas: &[[3, 1, 2]],
        window_ms: LONG_WINDOW_MS,
        rule: Rule::Header,
        // Fifty keys under 1200 header values: the key repeats all the time
        // and must not make a record a duplicate.
        identities: Identities::Pool {
            keys: 50,
            headers: 1_200,
        },
        unique: true,
        mutable: false,
    },
    TopicSpec {
        name: "chaos-flip",
        replicas: &[[1, 3, 2]],
        window_ms: LONG_WINDOW_MS,
        rule: Rule::Key,
        identities: Identities::Pool {
            keys: 600,
            headers: 600,
        },
        unique: true,
        mutable: true,
    },
    TopicSpec {
        name: "chaos-short",
        replicas: &[[2, 1, 3]],
        window_ms: SHORT_WINDOW_MS,
        rule: Rule::Key,
        identities: Identities::Pool {
            keys: 150,
            headers: 0,
        },
        unique: false,
        mutable: false,
    },
    TopicSpec {
        name: "chaos-capacity",
        replicas: &[[3, 2, 1]],
        window_ms: LONG_WINDOW_MS,
        rule: Rule::Key,
        identities: Identities::Fresh,
        unique: false,
        mutable: false,
    },
];

/// One partition of one of [`TOPICS`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Target {
    pub topic: usize,
    pub partition: i32,
}

impl Target {
    pub fn spec(self) -> &'static TopicSpec { &TOPICS[self.topic] }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.spec().name, self.partition)
    }
}

/// Every partition of the run, in topic order.
pub fn targets() -> Vec<Target> {
    TOPICS
        .iter()
        .enumerate()
        .flat_map(|(topic, spec)| {
            (0..spec.replicas.len()).map(move |partition| Target {
                topic,
                partition: i32::try_from(partition).expect("a partition index"),
            })
        })
        .collect()
}
