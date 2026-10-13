//! KFC-10 produce-time deduplication accounting: the per-partition counters
//! and index gauge each partition's dedup state reports, and the broker-wide
//! index budget gauges.
//!
//! The dedup state accumulates its counts and hands them over as one
//! [`DedupCounterDelta`] per report, so the produce path takes the metric
//! label lookups once per report rather than once per record.

use {
    super::{BrokerMetrics, PartitionLabel},
    std::sync::Arc,
};

/// Increments a partition's dedup counters since the last report.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DedupCounterDelta {
    /// Records dropped as duplicates.
    pub records_dropped: u64,
    /// Records admitted without being indexed because an index limit was full.
    pub records_unindexed: u64,
    /// Records whose future timestamp was clamped to the broker clock.
    pub records_skew_clamped: u64,
    /// Still-live identities evicted to make room at an entry limit.
    pub capacity_evicted_entries: u64,
    /// Batches re-encoded because only some of their records were duplicates.
    pub batches_rebuilt: u64,
    /// Rebuilds of the partition's index from the log.
    pub index_rebuilds: u64,
}

impl BrokerMetrics {
    /// KFC-10: add one partition's dedup counter increments and publish the
    /// size of its index.
    ///
    /// A zero increment creates no counter series, so a partition that has
    /// never dropped a duplicate carries no `dedup_records_dropped` series.
    /// The index gauge is always set, because a partition with dedup enabled
    /// and an empty index is worth seeing as zero.
    pub fn record_dedup_partition(
        &self,
        topic: &Arc<str>,
        partition: i32,
        delta: DedupCounterDelta,
        index_entries: usize,
    ) {
        let DedupCounterDelta {
            records_dropped,
            records_unindexed,
            records_skew_clamped,
            capacity_evicted_entries,
            batches_rebuilt,
            index_rebuilds,
        } = delta;
        for (family, amount) in [
            (&self.dedup_records_dropped, records_dropped),
            (&self.dedup_records_unindexed, records_unindexed),
            (&self.dedup_records_skew_clamped, records_skew_clamped),
            (
                &self.dedup_capacity_evicted_entries,
                capacity_evicted_entries,
            ),
            (&self.dedup_batches_rebuilt, batches_rebuilt),
            (&self.dedup_index_rebuilds, index_rebuilds),
        ] {
            self.count_partition(family, topic, partition, amount);
        }
        let label = PartitionLabel {
            topic: Arc::clone(topic),
            partition,
        };
        self.dedup_index_entries
            .get_or_create(&label)
            .set(saturating_i64(index_entries));
        self.track_partition_series(&label);
    }

    /// KFC-10: publish the estimated heap of one partition's dedup index.
    pub fn set_dedup_index_heap_bytes(&self, topic: &Arc<str>, partition: i32, heap_bytes: usize) {
        let label = PartitionLabel {
            topic: Arc::clone(topic),
            partition,
        };
        self.dedup_index_heap_bytes
            .get_or_create(&label)
            .set(saturating_i64(heap_bytes));
        self.track_partition_series(&label);
    }

    /// KFC-10: publish the identities held across every dedup index on this
    /// broker and the broker-wide limit on them.
    pub fn set_dedup_broker_index(&self, entries: usize, max_entries: usize) {
        self.dedup_broker_index_entries.set(saturating_i64(entries));
        self.dedup_broker_index_max_entries
            .set(saturating_i64(max_entries));
    }
}

/// `value` as a gauge value, saturated at `i64::MAX`: no index can hold that
/// many entries, so the bound needs no failure path of its own.
fn saturating_i64(value: usize) -> i64 { i64::try_from(value).unwrap_or(i64::MAX) }

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        prometheus_client::metrics::{counter::Counter, family::Family},
    };

    const TOPIC: &str = "orders";
    const PARTITION: i32 = 3;

    fn topic() -> Arc<str> { Arc::from(TOPIC) }

    fn label() -> PartitionLabel {
        PartitionLabel {
            topic: topic(),
            partition: PARTITION,
        }
    }

    /// Every dedup counter family, named the way the cases below name it.
    fn counter_families(
        m: &BrokerMetrics,
    ) -> [(&'static str, &Family<PartitionLabel, Counter>); 6] {
        [
            ("records_dropped", &m.dedup_records_dropped),
            ("records_unindexed", &m.dedup_records_unindexed),
            ("records_skew_clamped", &m.dedup_records_skew_clamped),
            (
                "capacity_evicted_entries",
                &m.dedup_capacity_evicted_entries,
            ),
            ("batches_rebuilt", &m.dedup_batches_rebuilt),
            ("index_rebuilds", &m.dedup_index_rebuilds),
        ]
    }

    /// Each counter family's value for the test partition, `None` where the
    /// family carries no series for it. `Family::get` reads without creating,
    /// so a missing series stays missing.
    fn counter_values(m: &BrokerMetrics) -> Vec<(&'static str, Option<u64>)> {
        counter_families(m)
            .into_iter()
            .map(|(name, family)| (name, family.get(&label()).map(|counter| counter.get())))
            .collect()
    }

    fn index_entries(m: &BrokerMetrics) -> Option<i64> {
        m.dedup_index_entries.get(&label()).map(|gauge| gauge.get())
    }

    fn heap_bytes(m: &BrokerMetrics) -> Option<i64> {
        m.dedup_index_heap_bytes
            .get(&label())
            .map(|gauge| gauge.get())
    }

    #[test]
    fn the_heap_gauge_takes_the_latest_estimate_and_saturates() {
        let m = BrokerMetrics::new();
        let mut seen = Vec::new();
        for bytes in [8_192, 4_096, usize::MAX] {
            m.set_dedup_index_heap_bytes(&topic(), PARTITION, bytes);
            seen.push(heap_bytes(&m));
        }
        assert!(seen == [Some(8_192), Some(4_096), Some(i64::MAX)]);
    }

    #[test]
    fn each_delta_field_increments_only_its_own_family() {
        let cases = [
            ("records_dropped", DedupCounterDelta {
                records_dropped: 3,
                ..DedupCounterDelta::default()
            }),
            ("records_unindexed", DedupCounterDelta {
                records_unindexed: 3,
                ..DedupCounterDelta::default()
            }),
            ("records_skew_clamped", DedupCounterDelta {
                records_skew_clamped: 3,
                ..DedupCounterDelta::default()
            }),
            ("capacity_evicted_entries", DedupCounterDelta {
                capacity_evicted_entries: 3,
                ..DedupCounterDelta::default()
            }),
            ("batches_rebuilt", DedupCounterDelta {
                batches_rebuilt: 3,
                ..DedupCounterDelta::default()
            }),
            ("index_rebuilds", DedupCounterDelta {
                index_rebuilds: 3,
                ..DedupCounterDelta::default()
            }),
        ];
        for (field, delta) in cases {
            let m = BrokerMetrics::new();
            m.record_dedup_partition(&topic(), PARTITION, delta, 0);

            let want: Vec<(&str, Option<u64>)> = counter_families(&m)
                .into_iter()
                .map(|(name, _)| (name, (name == field).then_some(3)))
                .collect();
            assert!(counter_values(&m) == want, "delta field {field}");
        }
    }

    #[test]
    fn a_zero_delta_creates_no_counter_series_but_sets_the_gauge() {
        let m = BrokerMetrics::new();
        m.record_dedup_partition(&topic(), PARTITION, DedupCounterDelta::default(), 0);

        let want: Vec<(&str, Option<u64>)> = counter_families(&m)
            .into_iter()
            .map(|(name, _)| (name, None))
            .collect();
        assert!(counter_values(&m) == want);
        assert!(index_entries(&m) == Some(0));
    }

    #[test]
    fn counters_accumulate_and_the_index_gauge_takes_the_latest_size() {
        let m = BrokerMetrics::new();
        let delta = DedupCounterDelta {
            records_dropped: 2,
            records_unindexed: 1,
            records_skew_clamped: 1,
            capacity_evicted_entries: 4,
            batches_rebuilt: 1,
            index_rebuilds: 1,
        };
        m.record_dedup_partition(&topic(), PARTITION, delta, 7);
        m.record_dedup_partition(&topic(), PARTITION, delta, 4);

        let want = vec![
            ("records_dropped", Some(4)),
            ("records_unindexed", Some(2)),
            ("records_skew_clamped", Some(2)),
            ("capacity_evicted_entries", Some(8)),
            ("batches_rebuilt", Some(2)),
            ("index_rebuilds", Some(2)),
        ];
        assert!(counter_values(&m) == want);
        assert!(index_entries(&m) == Some(4));
    }

    #[test]
    fn broker_index_gauges_take_the_reported_values_and_saturate() {
        for (entries, max_entries, want) in [
            (0, 1_000, (0, 1_000)),
            (750, 1_000, (750, 1_000)),
            (usize::MAX, usize::MAX, (i64::MAX, i64::MAX)),
        ] {
            let m = BrokerMetrics::new();
            m.set_dedup_broker_index(entries, max_entries);
            let got = (
                m.dedup_broker_index_entries.get(),
                m.dedup_broker_index_max_entries.get(),
            );
            assert!(got == want, "entries {entries}, max {max_entries}");
        }
    }

    #[test]
    fn evicting_the_partition_removes_every_dedup_series() {
        let m = BrokerMetrics::new();
        let delta = DedupCounterDelta {
            records_dropped: 1,
            records_unindexed: 1,
            records_skew_clamped: 1,
            capacity_evicted_entries: 1,
            batches_rebuilt: 1,
            index_rebuilds: 1,
        };
        m.record_dedup_partition(&topic(), PARTITION, delta, 5);
        m.set_dedup_index_heap_bytes(&topic(), PARTITION, 8_192);
        assert!(index_entries(&m) == Some(5));
        assert!(heap_bytes(&m) == Some(8_192));

        m.evict_partition_series(&label());

        let want: Vec<(&str, Option<u64>)> = counter_families(&m)
            .into_iter()
            .map(|(name, _)| (name, None))
            .collect();
        assert!(counter_values(&m) == want);
        assert!(index_entries(&m).is_none());
        assert!(heap_bytes(&m).is_none());
    }
}
