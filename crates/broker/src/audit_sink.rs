//! Kafka-native audit sink.
//!
//! The sink appends OCSF records to this broker's partition of the internal
//! audit topic. This is the broker-affinity write path.

use {
    crate::{
        metrics::BrokerMetrics, partition::ProduceBatchError, partition_registry::PartitionRegistry,
    },
    async_trait::async_trait,
    bytes::Bytes,
    krabka_audit::{AuditError, AuditRecord, AuditSink},
    krabka_ids::PartitionIndex,
    krabka_protocol::records::{Record, RecordBatch, RecordHeader},
    std::sync::Arc,
};

/// Writes audit records to a single partition of the audit topic that this
/// broker leads.
///
/// Audit sink construction: the sink resolves the partition index once at construction.
#[derive(derive_more::Debug)]
pub struct KafkaTopicAuditSink {
    #[debug(skip)]
    partitions: Arc<PartitionRegistry>,
    topic: String,
    partition_index: PartitionIndex,
    #[debug(skip)]
    node_id: krabka_raft::NodeId,
    #[debug(skip)]
    metrics: BrokerMetrics,
}

impl KafkaTopicAuditSink {
    #[must_use]
    pub(crate) fn new(
        partitions: Arc<PartitionRegistry>,
        topic: String,
        partition_index: PartitionIndex,
        node_id: krabka_raft::NodeId,
        metrics: BrokerMetrics,
    ) -> Self {
        Self {
            partitions,
            topic,
            partition_index,
            node_id,
            metrics,
        }
    }
}

#[async_trait]
impl AuditSink for KafkaTopicAuditSink {
    async fn write(&self, record: AuditRecord, durable: bool) -> Result<(), AuditError> {
        let Some(partition) = self.partitions.get(&self.topic, self.partition_index) else {
            self.metrics.audit_write_failures.inc();
            return Err(AuditError::Sink(format!(
                "audit partition {}-{} not local",
                self.topic, self.partition_index
            )));
        };
        let leadership = partition.lock_produce_transition().await;
        if leadership.leader_node_id != self.node_id {
            self.metrics.audit_write_failures.inc();
            return Err(AuditError::Sink(format!(
                "audit partition {}-{} is led by node {}, not this node {}",
                self.topic, self.partition_index, leadership.leader_node_id.0, self.node_id.0
            )));
        }

        let headers = record
            .headers
            .into_iter()
            .map(|(k, v)| RecordHeader {
                key: k,
                value: Some(Bytes::from(v)),
            })
            .collect();
        let mut batch = RecordBatch::default();
        // `offset_delta`/`key` are left at their `Record::default()` values (0 /
        // None) — a single audit record with no key. Spelling them out would only
        // create equivalent "delete field" mutants.
        batch.records.push(Record {
            value: Some(Bytes::from(record.value)),
            headers,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        let result = if durable {
            partition.produce_batch_durable_outcome(batch).await
        } else {
            partition.produce_batch_outcome(batch).await
        };
        if let Err(error) = result {
            self.metrics.audit_write_failures.inc();
            return Err(match error {
                ProduceBatchError::Rejected(error) => AuditError::Sink(error.to_string()),
                ProduceBatchError::Indeterminate(error) => AuditError::Indeterminate(error),
            });
        }
        self.metrics.audit_events.inc();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{partition::Partition, test_support::open_partition as fixture_partition},
        assert2::assert,
        krabka_log::Offset,
        krabka_units::mebibytes,
        std::sync::Arc,
    };

    /// The `__audit` partition and the registry that holds it.
    struct AuditPartition {
        partition: Arc<Partition>,
        partitions: Arc<PartitionRegistry>,
    }

    fn audit_partition(path: &std::path::Path) -> AuditPartition {
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = fixture_partition().log_dir(path).topic("__audit").call();
        partitions.insert("__audit".into(), PartitionIndex(0), Arc::clone(&partition));
        AuditPartition {
            partition,
            partitions,
        }
    }

    fn audit_sink(partitions: Arc<PartitionRegistry>) -> KafkaTopicAuditSink {
        KafkaTopicAuditSink::new(
            partitions,
            "__audit".to_string(),
            PartitionIndex(0),
            krabka_raft::NodeId(0),
            BrokerMetrics::new(),
        )
    }

    #[tokio::test]
    async fn write_appends_record_value_and_headers_to_local_partition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AuditPartition {
            partition,
            partitions,
        } = audit_partition(dir.path());
        let sink = audit_sink(partitions);

        sink.write(
            AuditRecord {
                class: krabka_audit::AuditEventClass::ApiActivity,
                value: b"{\"ok\":true}".to_vec(),
                headers: vec![("event_class".to_string(), b"admin".to_vec())],
            },
            true,
        )
        .await
        .expect("write audit record");

        let out = partition
            .read_log(Offset(0), mebibytes(1))
            .expect("read audit partition");
        let records: Vec<_> = out.batches.iter().flat_map(|b| &b.records).collect();

        assert!(
            (
                records.len(),
                records[0].value.as_deref(),
                records[0].headers.len(),
                records[0].headers[0].key.as_str(),
                records[0].headers[0].value.as_deref(),
            ) == (
                1,
                Some(&b"{\"ok\":true}"[..]),
                1,
                "event_class",
                Some(&b"admin"[..]),
            )
        );
    }

    #[tokio::test]
    async fn write_refuses_a_partition_after_leadership_moves_away() {
        let dir = tempfile::tempdir().expect("tempdir");
        let AuditPartition {
            partition,
            partitions,
        } = audit_partition(dir.path());
        partition.install_leader_change(1, 1).await;
        let sink = audit_sink(partitions);

        let error = sink
            .write(
                AuditRecord {
                    class: krabka_audit::AuditEventClass::ApiActivity,
                    value: b"{}".to_vec(),
                    headers: Vec::new(),
                },
                true,
            )
            .await
            .expect_err("former leader must reject audit write");

        assert!(error.to_string().contains("led by node 1"));
        assert!(partition.log_end_offset() == Offset(0));
    }
}
