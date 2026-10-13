//! Inter-broker replication accounting: the follower-side and leader-side
//! byte counters of a replica fetch, and the unclean-election counter that
//! records when an out-of-ISR replica took leadership.

#[cfg(test)]
use super::PartitionLabel;
use {super::BrokerMetrics, std::sync::Arc};

impl BrokerMetrics {
    /// Account bytes this broker received from the partition
    /// leader as a follower (inter-broker `Fetch` round-trip, follower
    /// side). Called from the replicator after a successful append, once per
    /// record batch, so `topic` is the replicator task's own `Arc<str>` and
    /// the label costs a refcount bump rather than an allocation.
    pub fn record_replication_in(&self, topic: &Arc<str>, partition: i32, bytes: u64) {
        self.count_partition(&self.replication_bytes_in, topic, partition, bytes);
    }

    /// KIP-841: account one unclean leader election (an
    /// election that picked an out-of-ISR replica because the ISR was
    /// empty and the topic had `unclean.leader.election.enable=true`).
    pub fn record_unclean_leader_election(&self) { self.unclean_leader_elections_total.inc(); }

    /// Account bytes this broker served to a follower as the
    /// partition leader (inter-broker `Fetch` round-trip, leader side).
    /// Called from the `Fetch` handler when `replica_id >= 0`, once per
    /// partition row of the response.
    pub fn record_replication_out(&self, topic: &Arc<str>, partition: i32, bytes: u64) {
        self.count_partition(&self.replication_bytes_out, topic, partition, bytes);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::metrics::{MetricCounterCase, assert_metric_counter_cases},
    };

    #[test]
    fn replication_helpers_accumulate_per_partition() {
        let m = BrokerMetrics::new();
        let orders: Arc<str> = Arc::from("orders");
        // Two appends from the same leader partition.
        m.record_replication_in(&orders, 3, 1_500);
        m.record_replication_in(&orders, 3, 2_500);
        // Different partition stays independent.
        m.record_replication_in(&orders, 4, 100);
        // Outbound side: bytes this broker served to its followers.
        m.record_replication_out(&orders, 3, 4_000);
        m.record_replication_out(&orders, 4, 0); // no-op

        let lbl3 = PartitionLabel {
            topic: "orders".into(),
            partition: 3,
        };
        let lbl4 = PartitionLabel {
            topic: "orders".into(),
            partition: 4,
        };
        let cases = [
            MetricCounterCase {
                name: "replication_in",
                family: &m.replication_bytes_in,
                label: &lbl3,
                expected: 4_000,
            },
            MetricCounterCase {
                name: "replication_in",
                family: &m.replication_bytes_in,
                label: &lbl4,
                expected: 100,
            },
            MetricCounterCase {
                name: "replication_out",
                family: &m.replication_bytes_out,
                label: &lbl3,
                expected: 4_000,
            },
            MetricCounterCase {
                name: "replication_out",
                family: &m.replication_bytes_out,
                label: &lbl4,
                expected: 0,
            },
        ];
        assert_metric_counter_cases(&cases);
    }
}
