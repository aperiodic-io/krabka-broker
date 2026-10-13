//! Diskless WAL quorum, object-flush, trim, and cold-read accounting.

use {
    super::{BrokerMetrics, WalShardLabel, WalVoterLabel},
    krabka_ids::PartitionIndex,
    krabka_raft::NodeId,
    uuid::Uuid,
};

impl BrokerMetrics {
    fn wal_shard_label(topic_id: Uuid, partition: PartitionIndex) -> WalShardLabel {
        WalShardLabel {
            topic_id: topic_id.to_string(),
            partition: partition.0,
        }
    }

    pub(crate) fn record_diskless_wal_watermark(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        offset: i64,
    ) {
        self.diskless_wal_durable_watermark
            .get_or_create(&Self::wal_shard_label(topic_id, partition))
            .set(offset);
    }

    pub(crate) fn record_diskless_wal_voter_lag(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        voter: NodeId,
        lag: i64,
    ) {
        self.diskless_wal_voter_lag
            .get_or_create(&WalVoterLabel {
                topic_id: topic_id.to_string(),
                partition: partition.0,
                voter: voter.0,
            })
            .set(lag.max(0));
    }

    pub(crate) fn record_diskless_wal_projection_lag(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        lag: i64,
    ) {
        self.diskless_wal_index_projection_lag
            .get(&Self::wal_shard_label(topic_id, partition))
            .map(|gauge| gauge.set(lag.max(0)));
    }

    pub(crate) fn record_diskless_wal_trim_frontier(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        frontier: i64,
    ) {
        self.diskless_wal_trim_frontier
            .get(&Self::wal_shard_label(topic_id, partition))
            .map(|gauge| gauge.set(frontier));
    }

    pub(crate) fn initialize_diskless_wal_flusher_metrics(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        projection_lag: i64,
        trim_frontier: i64,
    ) {
        let label = Self::wal_shard_label(topic_id, partition);
        if self.diskless_wal_index_projection_lag.get(&label).is_none() {
            self.diskless_wal_index_projection_lag
                .get_or_create(&label)
                .set(projection_lag.max(0));
        }
        if self.diskless_wal_trim_frontier.get(&label).is_none() {
            self.diskless_wal_trim_frontier
                .get_or_create(&label)
                .set(trim_frontier);
        }
    }

    pub(crate) fn remove_diskless_wal_voters(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        voters: &[NodeId],
    ) {
        for voter in voters {
            self.diskless_wal_voter_lag.remove(&WalVoterLabel {
                topic_id: topic_id.to_string(),
                partition: partition.0,
                voter: voter.0,
            });
        }
    }

    pub(crate) fn remove_diskless_wal_shard(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        voters: &[NodeId],
    ) {
        let label = Self::wal_shard_label(topic_id, partition);
        self.diskless_wal_durable_watermark.remove(&label);
        self.diskless_wal_index_projection_lag.remove(&label);
        self.diskless_wal_trim_frontier.remove(&label);
        self.remove_diskless_wal_voters(topic_id, partition, voters);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[tokio::test]
    async fn diskless_metrics_encode_with_bounded_shard_labels() {
        let metrics = BrokerMetrics::new();
        let topic_id = Uuid::from_u128(42);
        let partition = PartitionIndex(3);
        metrics.initialize_diskless_wal_flusher_metrics(topic_id, partition, 2, 11);
        metrics.record_diskless_wal_watermark(topic_id, partition, 17);
        metrics.record_diskless_wal_voter_lag(topic_id, partition, NodeId(9), 4);
        metrics.record_diskless_wal_projection_lag(topic_id, partition, 2);
        metrics.record_diskless_wal_trim_frontier(topic_id, partition, 11);
        metrics.initialize_diskless_wal_flusher_metrics(topic_id, partition, 99, 99);
        metrics.diskless_wal_quorum_loss_events_total.inc();
        metrics.diskless_wal_flush_attempts_total.inc();
        metrics.diskless_wal_flush_bytes_total.inc_by(128);
        metrics.diskless_wal_flush_failures_total.inc();
        metrics.diskless_wal_cold_read_hits_total.inc();
        metrics.diskless_wal_cold_read_misses_total.inc();
        metrics.diskless_wal_cold_read_errors_total.inc();

        krabka_macros::metric_registry_fixture!(metrics, body, registry; unwrap());

        for sample in [
            "krabka_broker_diskless_wal_durable_watermark{topic_id=\"00000000-0000-0000-0000-00000000002a\",partition=\"3\"} 17",
            "krabka_broker_diskless_wal_voter_lag{topic_id=\"00000000-0000-0000-0000-00000000002a\",partition=\"3\",voter=\"9\"} 4",
            "krabka_broker_diskless_wal_quorum_loss_events_total 1",
            "krabka_broker_diskless_wal_flush_attempts_total 1",
            "krabka_broker_diskless_wal_flush_bytes_total 128",
            "krabka_broker_diskless_wal_flush_failures_total 1",
            "krabka_broker_diskless_wal_index_projection_lag{topic_id=\"00000000-0000-0000-0000-00000000002a\",partition=\"3\"} 2",
            "krabka_broker_diskless_wal_trim_frontier{topic_id=\"00000000-0000-0000-0000-00000000002a\",partition=\"3\"} 11",
            "krabka_broker_diskless_wal_cold_read_hits_total 1",
            "krabka_broker_diskless_wal_cold_read_misses_total 1",
            "krabka_broker_diskless_wal_cold_read_errors_total 1",
        ] {
            assert!(body.contains(sample), "missing {sample} in:\n{body}");
        }

        drop(registry);
        metrics.remove_diskless_wal_shard(topic_id, partition, &[NodeId(9)]);
        body.clear();
        let registry = metrics.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut body, &registry).unwrap();
        assert!(!body.contains("topic_id=\"00000000-0000-0000-0000-00000000002a\""));
    }
}
