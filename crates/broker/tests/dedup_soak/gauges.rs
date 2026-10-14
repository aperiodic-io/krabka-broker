//! The KFC-10 series of one broker, read from its metrics registry.
//!
//! The registry is rendered as `/metrics` renders it, and every
//! `krabka_broker_dedup_*` sample line is folded into one [`DedupGauges`]:
//! the broker-wide entry gauge and limit, the entry and heap gauges of each
//! partition, and the sums of the counters that show which paths ran.

use {
    krabka_broker::BrokerHandle,
    std::collections::{BTreeMap, BTreeSet},
};

krabka_macros::metric_registry_fixture!(render_registry);

/// The prefix every KFC-10 series carries.
const DEDUP_PREFIX: &str = "krabka_broker_dedup_";

/// One partition, as its series' labels name it.
pub type PartitionKey = (String, i32);

/// The sums of the per-partition KFC-10 counters of one broker.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CounterSums {
    pub dropped: u64,
    pub unindexed: u64,
    pub evicted: u64,
    pub rebuilds: u64,
}

impl CounterSums {
    /// What each counter added since `earlier`. A broker that restarted
    /// counts from zero again, and a deleted partition takes its series with
    /// it, so a counter that went down is taken to have restarted from zero.
    pub fn since(self, earlier: Self) -> Self {
        let delta = |now: u64, then: u64| if now >= then { now - then } else { now };
        Self {
            dropped: delta(self.dropped, earlier.dropped),
            unindexed: delta(self.unindexed, earlier.unindexed),
            evicted: delta(self.evicted, earlier.evicted),
            rebuilds: delta(self.rebuilds, earlier.rebuilds),
        }
    }
}

/// Every KFC-10 series of one broker.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DedupGauges {
    /// `dedup_broker_index_entries`.
    pub broker_entries: u64,
    /// `dedup_broker_index_max_entries`.
    pub broker_max_entries: u64,
    /// `dedup_index_entries` of each partition.
    pub entries: BTreeMap<PartitionKey, u64>,
    /// `dedup_index_heap_bytes` of each partition.
    pub heap_bytes: BTreeMap<PartitionKey, u64>,
    pub counters: CounterSums,
    /// Every topic that any per-partition KFC-10 series names.
    pub topics: BTreeSet<String>,
    /// The sample lines of the whole registry, KFC-10 or not: a family that
    /// keeps the series of deleted partitions grows this without bound.
    pub registry_series: u64,
}

impl DedupGauges {
    /// Fold a rendered registry into the KFC-10 series it holds.
    pub fn parse(rendered: &str) -> Self {
        let mut gauges = Self::default();
        for line in rendered.lines().filter(|line| !line.starts_with('#')) {
            gauges.registry_series += 1;
            let Some(series) = line.strip_prefix(DEDUP_PREFIX) else {
                continue;
            };
            let (series, value) = series
                .rsplit_once(' ')
                .unwrap_or_else(|| panic!("a sample line without a value: {line}"));
            let value: u64 = value
                .parse()
                .unwrap_or_else(|_| panic!("a KFC-10 sample that is not a whole number: {line}"));
            match series.split_once('{') {
                None => gauges.broker_series(series, value),
                Some((name, labels)) => {
                    let partition = partition_of(labels)
                        .unwrap_or_else(|| panic!("a KFC-10 series without a partition: {line}"));
                    gauges.partition_series(name, partition, value);
                }
            }
        }
        gauges
    }

    fn broker_series(&mut self, name: &str, value: u64) {
        match name {
            "broker_index_entries" => self.broker_entries = value,
            "broker_index_max_entries" => self.broker_max_entries = value,
            _ => {}
        }
    }

    fn partition_series(&mut self, name: &str, partition: PartitionKey, value: u64) {
        self.topics.insert(partition.0.clone());
        let counters = &mut self.counters;
        match name {
            "index_entries" => {
                self.entries.insert(partition, value);
            }
            "index_heap_bytes" => {
                self.heap_bytes.insert(partition, value);
            }
            "records_dropped_total" => counters.dropped += value,
            "records_unindexed_total" => counters.unindexed += value,
            "capacity_evicted_entries_total" => counters.evicted += value,
            "index_rebuilds_total" => counters.rebuilds += value,
            _ => {}
        }
    }

    /// The entries of every partition index, summed.
    pub fn entries_sum(&self) -> u64 { self.entries.values().sum() }

    /// The estimated heap of every partition index, summed.
    pub fn heap_sum(&self) -> u64 { self.heap_bytes.values().sum() }

    /// The entries of the fullest partition index.
    pub fn fullest_partition(&self) -> u64 { self.entries.values().copied().max().unwrap_or(0) }

    /// Whether every index is empty, by the broker gauge and by every
    /// partition gauge.
    pub fn is_empty(&self) -> bool { self.broker_entries == 0 && self.entries_sum() == 0 }

    /// The partitions whose index is not empty.
    pub fn occupied(&self) -> Vec<(&PartitionKey, u64)> {
        self.entries
            .iter()
            .filter(|(_, entries)| **entries > 0)
            .map(|(partition, entries)| (partition, *entries))
            .collect()
    }
}

/// The `topic` and `partition` labels of a series, from the text between its
/// braces.
fn partition_of(labels: &str) -> Option<PartitionKey> {
    let labels = labels.strip_suffix('}')?;
    let mut topic = None;
    let mut partition = None;
    for pair in labels.split(',') {
        let (key, value) = pair.split_once('=')?;
        let value = value.strip_prefix('"')?.strip_suffix('"')?;
        match key {
            "topic" => topic = Some(value.to_owned()),
            "partition" => partition = value.parse().ok(),
            _ => {}
        }
    }
    Some((topic?, partition?))
}

/// The KFC-10 series of `broker` now.
pub async fn scrape(broker: &BrokerHandle) -> DedupGauges {
    render_registry!(broker.metrics(), rendered, registry; expect("encode the registry"));
    drop(registry);
    DedupGauges::parse(&rendered)
}

mod tests {
    use {super::*, assert2::assert};

    const RENDERED: &str = "\
# HELP krabka_broker_dedup_index_entries Identities held.
# TYPE krabka_broker_dedup_index_entries gauge
krabka_broker_dedup_index_entries{topic=\"a\",partition=\"0\"} 7
krabka_broker_dedup_index_entries{topic=\"a\",partition=\"1\"} 0
krabka_broker_dedup_index_entries{topic=\"b\",partition=\"0\"} 5
krabka_broker_dedup_index_heap_bytes{topic=\"a\",partition=\"0\"} 4096
krabka_broker_dedup_index_heap_bytes{topic=\"b\",partition=\"0\"} 1024
krabka_broker_dedup_records_dropped_total{topic=\"a\",partition=\"0\"} 3
krabka_broker_dedup_records_dropped_total{topic=\"b\",partition=\"0\"} 4
krabka_broker_dedup_records_unindexed_total{topic=\"b\",partition=\"0\"} 9
krabka_broker_dedup_capacity_evicted_entries_total{topic=\"b\",partition=\"0\"} 2
krabka_broker_dedup_index_rebuilds_total{topic=\"c\",partition=\"2\"} 1
krabka_broker_dedup_broker_index_entries 12
krabka_broker_dedup_broker_index_max_entries 32768
krabka_broker_produce_requests_total{topic=\"a\",partition=\"0\"} 99
";

    #[test]
    fn a_rendered_registry_folds_into_one_broker_s_series() {
        let key = |topic: &str, partition| (topic.to_owned(), partition);
        assert!(
            DedupGauges::parse(RENDERED)
                == DedupGauges {
                    broker_entries: 12,
                    broker_max_entries: 32_768,
                    entries: BTreeMap::from([(key("a", 0), 7), (key("a", 1), 0), (key("b", 0), 5)]),
                    heap_bytes: BTreeMap::from([(key("a", 0), 4_096), (key("b", 0), 1_024)]),
                    counters: CounterSums {
                        dropped: 7,
                        unindexed: 9,
                        evicted: 2,
                        rebuilds: 1,
                    },
                    topics: BTreeSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned()]),
                    registry_series: 13,
                }
        );
    }

    #[test]
    fn the_summaries_read_the_partition_gauges() {
        let gauges = DedupGauges::parse(RENDERED);
        assert!(gauges.entries_sum() == 12);
        assert!(gauges.heap_sum() == 5_120);
        assert!(gauges.fullest_partition() == 7);
        assert!(!gauges.is_empty());
        assert!(gauges.occupied() == [(&("a".to_owned(), 0), 7), (&("b".to_owned(), 0), 5)]);
        assert!(DedupGauges::default().is_empty());
    }

    #[test]
    fn a_counter_that_went_down_restarted_from_zero() {
        let earlier = CounterSums {
            dropped: 10,
            unindexed: 5,
            evicted: 0,
            rebuilds: 3,
        };
        let now = CounterSums {
            dropped: 15,
            unindexed: 2,
            evicted: 4,
            rebuilds: 3,
        };
        assert!(
            now.since(earlier)
                == CounterSums {
                    dropped: 5,
                    unindexed: 2,
                    evicted: 4,
                    rebuilds: 0,
                }
        );
    }
}
