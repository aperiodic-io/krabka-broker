//! Produce and fetch accounting: the per-topic and per-partition byte,
//! request, failure and message-conversion counters that the data path bumps
//! once per request slice.
//!
//! Every helper here takes the topic as `&Arc<str>` rather than `&str`. A
//! Produce request with a hundred partitions runs eight of these calls per
//! partition, and each one builds a throwaway label set only to hash it, so
//! an owned `String` per call put an O(partitions) burst of allocations back
//! on the path `PartitionRegistry` avoids them on. `PartitionRegistry::shared_topic_name`
//! is where a caller gets the `Arc` without allocating one.

#[cfg(test)]
use super::PartitionLabel;
use {
    super::{BrokerMetrics, TopicLabel},
    std::sync::Arc,
};

impl BrokerMetrics {
    /// Convenience: record a Produce hit on `topic` with the given
    /// payload size. No-op on the error path — callers shouldn't call
    /// this if the request was rejected.
    pub fn record_produce(&self, topic: &Arc<str>, bytes: u64) {
        let lbl = TopicLabel {
            topic: Arc::clone(topic),
        };
        self.topic_produce_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_in.get_or_create(&lbl).inc_by(bytes);
        }
        self.track_topic_series(&lbl);
    }

    /// Account `messages` records received on the Produce
    /// path for `topic`. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`. Called once per
    /// `RecordBatch` with the batch's record count. Zero is a
    /// legitimate value (legacy batches whose record count we can't
    /// cheaply derive without a full conversion) and is a no-op.
    pub fn record_produce_messages(&self, topic: &Arc<str>, messages: u64) {
        if messages == 0 {
            return;
        }
        self.count_topic(&self.topic_messages_in, Arc::clone(topic), messages);
    }

    /// Convenience: record a Fetch hit on `topic` with the bytes
    /// delivered. The `bytes` arg may legitimately be zero (empty
    /// fetch); the request counter still increments.
    pub fn record_fetch(&self, topic: &Arc<str>, bytes: u64) {
        let lbl = TopicLabel {
            topic: Arc::clone(topic),
        };
        self.topic_fetch_requests.get_or_create(&lbl).inc();
        if bytes > 0 {
            self.topic_bytes_out.get_or_create(&lbl).inc_by(bytes);
        }
        self.track_topic_series(&lbl);
    }

    /// Record a single failed Produce partition response
    /// for `topic`. Callers bump once per partition whose response
    /// carries a non-zero error code — mirrors the JVM's per-row
    /// `failedProduceRequestRate.mark()`.
    pub fn record_failed_produce(&self, topic: &Arc<str>) {
        self.count_topic(&self.topic_failed_produce_requests, Arc::clone(topic), 1);
    }

    /// Record a single failed Fetch partition response
    /// for `topic`. Same per-partition semantics as
    /// `record_failed_produce`.
    pub fn record_failed_fetch(&self, topic: &Arc<str>) {
        self.count_topic(&self.topic_failed_fetch_requests, Arc::clone(topic), 1);
    }

    /// Convenience: account a partition's slice of a Produce request.
    /// Called once per partition by the request handler (alongside the
    /// existing topic-level `record_produce`).
    pub fn record_partition_produce(&self, topic: &Arc<str>, partition: i32, bytes: u64) {
        self.count_partition(&self.partition_bytes_in, topic, partition, bytes);
    }

    /// Convenience: account a partition's slice of a Fetch response.
    pub fn record_partition_fetch(&self, topic: &Arc<str>, partition: i32, bytes: u64) {
        self.count_partition(&self.partition_bytes_out, topic, partition, bytes);
    }

    /// Account one v0/v1 → v2 up-conversion on the Produce
    /// path (the partition's `records` field arrived as a legacy
    /// `MessageSet` and was decoded into a v2 `RecordBatch`).
    pub fn record_produce_message_conversion(&self, topic: &Arc<str>) {
        self.count_topic(&self.produce_message_conversions, Arc::clone(topic), 1);
    }

    /// Account one v2 → v0/v1 down-conversion on the Fetch
    /// path (a legacy client's Fetch v < 4 response is being assembled
    /// from a v2 record batch).
    pub fn record_fetch_message_conversion(&self, topic: &Arc<str>) {
        self.count_topic(&self.fetch_message_conversions, Arc::clone(topic), 1);
    }

    /// Convenience: account handler-thread microseconds spent on a
    /// partition. Called from the produce / fetch hot paths around the
    /// per-partition work. No-ops on zero so we don't allocate a label
    /// entry for trivial measurements.
    pub fn record_partition_cpu_micros(&self, topic: &Arc<str>, partition: i32, micros: u64) {
        self.count_partition(&self.partition_cpu_micros, topic, partition, micros);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        prometheus_client::metrics::{counter::Counter, family::Family},
    };

    /// The topic name every case below records under.
    fn topic(name: &str) -> Arc<str> { Arc::from(name) }

    /// One counter family, the topic label to read in it and the count that
    /// label must hold.
    struct TopicCounterCase<'a> {
        family_name: &'a str,
        family: &'a Family<TopicLabel, Counter>,
        label: &'a TopicLabel,
        want: u64,
    }

    fn assert_topic_counters(cases: &[TopicCounterCase<'_>]) {
        for TopicCounterCase {
            family_name,
            family,
            label,
            want,
        } in cases
        {
            let got = family.get_or_create(label).get();
            assert!(got == *want, "{family_name} for {:?}", label.topic);
        }
    }

    #[test]
    fn record_fetch_zero_bytes_still_bumps_request_count() {
        let m = BrokerMetrics::new();
        let t = topic("t");
        let lbl = TopicLabel {
            topic: Arc::clone(&t),
        };
        // Pre-condition: no entry for the label yet.
        m.record_fetch(&t, 0);
        assert!(m.topic_fetch_requests.get_or_create(&lbl).get() == 1);
        assert!(m.topic_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn record_produce_increments_both_counters() {
        let m = BrokerMetrics::new();
        let t = topic("t");
        let lbl = TopicLabel {
            topic: Arc::clone(&t),
        };
        m.record_produce(&t, 1024);
        m.record_produce(&t, 2048);
        assert!(m.topic_produce_requests.get_or_create(&lbl).get() == 2);
        assert!(m.topic_bytes_in.get_or_create(&lbl).get() == 3072);
    }

    #[test]
    fn record_produce_messages_sums_across_calls_and_skips_zero() {
        let m = BrokerMetrics::new();
        let t = topic("t");
        let lbl = TopicLabel {
            topic: Arc::clone(&t),
        };
        // Zero is a no-op (legacy batches; the v2-conversion-time
        // counter tracks those arrivals separately).
        m.record_produce_messages(&t, 0);
        // The label entry is intentionally NOT eagerly created on a
        // zero-bump; rate(...) over a never-seen topic should yield
        // 0, not a phantom series.
        m.record_produce_messages(&t, 3);
        m.record_produce_messages(&t, 7);
        assert!(m.topic_messages_in.get_or_create(&lbl).get() == 10);
    }

    #[test]
    fn partition_helpers_increment_the_right_family() {
        let m = BrokerMetrics::new();
        let t = topic("t");
        m.record_partition_produce(&t, 0, 1024);
        m.record_partition_produce(&t, 1, 512);
        m.record_partition_fetch(&t, 0, 2048);
        m.record_partition_cpu_micros(&t, 0, 500);
        m.partition_disk_bytes
            .get_or_create(&PartitionLabel {
                topic: "t".into(),
                partition: 0,
            })
            .set(1_000_000);

        let lbl_p0 = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        let lbl_p1 = PartitionLabel {
            topic: "t".into(),
            partition: 1,
        };
        let cases = [
            ("bytes_in", &m.partition_bytes_in, &lbl_p0, 1024),
            ("bytes_in", &m.partition_bytes_in, &lbl_p1, 512),
            ("bytes_out", &m.partition_bytes_out, &lbl_p0, 2048),
            ("cpu_micros", &m.partition_cpu_micros, &lbl_p0, 500),
        ];
        crate::metrics::assert_metric_counter_cases!(cases);
        // `partition_disk_bytes` is a Gauge family (i64), so it stays
        // out of the Counter table above.
        let disk_p0 = m.partition_disk_bytes.get_or_create(&lbl_p0).get();
        assert!(disk_p0 == 1_000_000);
    }

    #[test]
    fn failed_request_counters_track_per_topic_and_per_call() {
        // `record_failed_produce` / `record_failed_fetch`
        // are bumped once per failed partition row. Two calls on
        // `t-good` and one on `t-bad` must land on the right labels
        // and yield independent series.
        let m = BrokerMetrics::new();
        let t_good = topic("t-good");
        let t_bad = topic("t-bad");
        m.record_failed_produce(&t_good);
        m.record_failed_produce(&t_good);
        m.record_failed_produce(&t_bad);
        m.record_failed_fetch(&t_good);

        let good = TopicLabel {
            topic: "t-good".into(),
        };
        let bad = TopicLabel {
            topic: "t-bad".into(),
        };
        // t-bad never saw a failed fetch — series is materialized by
        // `get_or_create` at read time but its value is 0, which is
        // what `rate(failed_fetch{topic="t-bad"}[1m])` should compute.
        let cases = [
            TopicCounterCase {
                family_name: "failed_produce",
                family: &m.topic_failed_produce_requests,
                label: &good,
                want: 2,
            },
            TopicCounterCase {
                family_name: "failed_produce",
                family: &m.topic_failed_produce_requests,
                label: &bad,
                want: 1,
            },
            TopicCounterCase {
                family_name: "failed_fetch",
                family: &m.topic_failed_fetch_requests,
                label: &good,
                want: 1,
            },
            TopicCounterCase {
                family_name: "failed_fetch",
                family: &m.topic_failed_fetch_requests,
                label: &bad,
                want: 0,
            },
        ];
        assert_topic_counters(&cases);
    }

    #[test]
    fn zero_bytes_no_op_on_partition_helpers() {
        let m = BrokerMetrics::new();
        let t = topic("t");
        m.record_partition_produce(&t, 0, 0);
        m.record_partition_fetch(&t, 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Counters still exist (get_or_create creates them) but at 0.
        assert!(m.partition_bytes_in.get_or_create(&lbl).get() == 0);
        assert!(m.partition_bytes_out.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn zero_micros_no_op() {
        let m = BrokerMetrics::new();
        m.record_partition_cpu_micros(&topic("t"), 0, 0);
        let lbl = PartitionLabel {
            topic: "t".into(),
            partition: 0,
        };
        // Helper short-circuits at 0; the label entry isn't created.
        assert!(m.partition_cpu_micros.get_or_create(&lbl).get() == 0);
    }

    #[test]
    fn message_conversion_helpers_accumulate_per_topic() {
        let m = BrokerMetrics::new();
        let orders_name = topic("orders");
        let payments_name = topic("payments");
        m.record_produce_message_conversion(&orders_name);
        m.record_produce_message_conversion(&orders_name);
        m.record_produce_message_conversion(&payments_name);
        m.record_fetch_message_conversion(&orders_name);
        m.record_fetch_message_conversion(&payments_name);
        m.record_fetch_message_conversion(&payments_name);

        let orders = TopicLabel {
            topic: "orders".into(),
        };
        let payments = TopicLabel {
            topic: "payments".into(),
        };
        let cases = [
            TopicCounterCase {
                family_name: "produce_conversions",
                family: &m.produce_message_conversions,
                label: &orders,
                want: 2,
            },
            TopicCounterCase {
                family_name: "produce_conversions",
                family: &m.produce_message_conversions,
                label: &payments,
                want: 1,
            },
            TopicCounterCase {
                family_name: "fetch_conversions",
                family: &m.fetch_message_conversions,
                label: &orders,
                want: 1,
            },
            TopicCounterCase {
                family_name: "fetch_conversions",
                family: &m.fetch_message_conversions,
                label: &payments,
                want: 2,
            },
        ];
        assert_topic_counters(&cases);
    }
}
