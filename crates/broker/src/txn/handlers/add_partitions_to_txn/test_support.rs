//! Fixtures shared by the `AddPartitionsToTxn` unit tests: a request topic
//! entry, and the fully pinned response row that a whole-value comparison
//! checks the handler's output against.
use {
    crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        coordinator::test_support::single_replica_partition,
        handlers::test_support::StartedBroker,
        test_support::{configure_single_partition_transactions, start_broker_no_audit_with},
        txn::{
            state::{TopicPartition, TxnEntry},
            version::resolve_txn_version,
        },
    },
    krabka_ids::PartitionIndex,
    krabka_protocol::owned::common::{
        add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic as RequestTopic,
        add_partitions_to_txn_response::{
            add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
            add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
        },
    },
    std::collections::HashSet,
};

/// One partition row of a response, in a static test table.
#[derive(Debug, Clone, Copy)]
pub(super) struct PartitionCode {
    pub(super) partition: i32,
    pub(super) code: i16,
}

/// A topic and the partitions a request names, in a static test table.
#[derive(Debug, Clone, Copy)]
pub(super) struct TopicSpec {
    pub(super) name: &'static str,
    pub(super) partitions: &'static [i32],
}

/// A topic and the partition rows a response answers for it, in a static test
/// table.
#[derive(Debug, Clone, Copy)]
pub(super) struct TopicRows {
    pub(super) name: &'static str,
    pub(super) rows: &'static [PartitionCode],
}

/// A topic partition, in a static test table.
#[derive(Debug, Clone, Copy)]
pub(super) struct PartitionSpec {
    pub(super) topic: &'static str,
    pub(super) partition: i32,
}

impl PartitionSpec {
    /// The transaction-state form of this partition.
    pub(super) fn to_topic_partition(self) -> TopicPartition {
        TopicPartition {
            topic: self.topic.to_owned(),
            partition: PartitionIndex(self.partition),
        }
    }
}

pub(super) fn topic(name: &str, partitions: &[i32]) -> RequestTopic {
    RequestTopic {
        name: name.into(),
        partitions: partitions.to_vec(),
        ..Default::default()
    }
}

/// Builds a fully pinned expected topic-result row. Every field is
/// explicit, so that whole-value comparisons kill field-drop mutants.
pub(super) fn topic_result(name: &str, rows: &[PartitionCode]) -> AddPartitionsToTxnTopicResult {
    AddPartitionsToTxnTopicResult {
        name: name.into(),
        results_by_partition: rows
            .iter()
            .map(
                |&PartitionCode { partition, code }| AddPartitionsToTxnPartitionResult {
                    partition_index: partition,
                    partition_error_code: code,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            )
            .collect(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
    }
}

/// Adds `topic` with `partitions` partitions, each led by this broker, to the
/// metadata image, so the existence check of `AddPartitionsToTxn` finds it.
pub(in crate::txn::handlers) async fn seed_topic(broker: &Broker, topic: &str, partitions: i32) {
    let mut records = vec![krabka_metadata::MetadataRecord::V1Topic(
        krabka_metadata::TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::new_v4(),
            partitions,
            replication_factor: 1,
        },
    )];
    records.extend((0..partitions).map(|partition| {
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            directories: vec![uuid::Uuid::nil()],
            ..single_replica_partition(
                topic,
                krabka_ids::PartitionIndex(partition),
                broker.config.node_id,
            )
        })
    }));
    broker
        .controller
        .submit_change(records)
        .await
        .unwrap_or_else(|error| panic!("seed topic {topic}: {error}"));
}

/// A started broker and the data directory that must outlive it.
pub(in crate::txn::handlers) struct CoordinatorBroker {
    pub(in crate::txn::handlers) handle: BrokerHandle,
    pub(in crate::txn::handlers) dir: tempfile::TempDir,
}

/// A broker that coordinates every transactional id, with topics `a` and `b`
/// of one partition each, both led by this broker.
///
/// `transaction_state_num_partitions = 1` puts every transactional id on
/// `__transaction_state-0`, and this returns once that partition is loaded,
/// so [`seed_transaction`] and the handler both see a coordinator.
pub(in crate::txn::handlers) async fn start_coordinator(
    authorizer: std::sync::Arc<dyn Authorizer>,
) -> CoordinatorBroker {
    let StartedBroker {
        broker: handle,
        dir,
    } = start_broker_no_audit_with(|cfg| {
        configure_single_partition_transactions(cfg, authorizer);
    })
    .await;
    let broker = handle.broker_arc_for_test();
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    handle.wait_until_transaction_coordinator_ready().await;
    seed_topic(&broker, "a", 1).await;
    seed_topic(&broker, "b", 1).await;
    CoordinatorBroker { handle, dir }
}

/// Opens an empty transaction for `tid` under `producer_id` at producer epoch
/// 2 on a broker [`start_coordinator`] started.
pub(in crate::txn::handlers) async fn seed_transaction(
    broker: &Broker,
    tid: &str,
    producer_id: i64,
) {
    let txnv = resolve_txn_version(&broker.controller.current_image());
    broker
        .txn_coordinator
        .put(
            TxnEntry::new_empty(
                tid.to_owned(),
                krabka_log::ProducerId(producer_id),
                2,
                30_000,
                0,
            ),
            txnv,
        )
        .await
        .expect("seed the open transaction");
}

/// The partitions `tid`'s transaction holds.
pub(super) async fn enlisted(broker: &Broker, tid: &str) -> HashSet<TopicPartition> {
    broker
        .txn_coordinator
        .get(tid)
        .expect("open transaction")
        .lock()
        .await
        .partitions
        .clone()
}

/// The set of partitions a static test table names.
pub(super) fn partition_set(specs: &[PartitionSpec]) -> HashSet<TopicPartition> {
    specs
        .iter()
        .map(|&spec| spec.to_topic_partition())
        .collect()
}
