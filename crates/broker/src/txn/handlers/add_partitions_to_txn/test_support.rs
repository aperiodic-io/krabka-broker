//! Fixtures shared by the `AddPartitionsToTxn` unit tests: a request topic
//! entry, and the fully pinned response row that a whole-value comparison
//! checks the handler's output against.

use krabka_protocol::owned::common::{
    add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
        add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};

pub(super) fn topic(name: &str, partitions: &[i32]) -> AddPartitionsToTxnTopic {
    AddPartitionsToTxnTopic {
        name: name.into(),
        partitions: partitions.to_vec(),
        ..Default::default()
    }
}

/// Builds a fully pinned expected topic-result row. Every field is
/// explicit, so that whole-value comparisons kill field-drop mutants.
pub(super) fn topic_result(name: &str, rows: &[(i32, i16)]) -> AddPartitionsToTxnTopicResult {
    AddPartitionsToTxnTopicResult {
        name: name.into(),
        results_by_partition: rows
            .iter()
            .map(
                |&(partition_index, partition_error_code)| AddPartitionsToTxnPartitionResult {
                    partition_index,
                    partition_error_code,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            )
            .collect(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
    }
}

/// Adds `topic` with `partitions` partitions, each led by this broker, to the
/// metadata image, so the existence check of `AddPartitionsToTxn` finds it.
pub(in crate::txn::handlers) async fn seed_topic(
    broker: &crate::broker::Broker,
    topic: &str,
    partitions: i32,
) {
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
            ..crate::coordinator::test_support::single_replica_partition(
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

/// A broker that coordinates every transactional id, with topics `a` and `b`
/// of one partition each, both led by this broker.
///
/// `transaction_state_num_partitions = 1` puts every transactional id on
/// `__transaction_state-0`, and this returns once that partition is loaded,
/// so [`seed_transaction`] and the handler both see a coordinator.
pub(in crate::txn::handlers) async fn start_coordinator(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = crate::test_support::start_broker_no_audit_with(|cfg| {
        crate::test_support::configure_single_partition_transactions(cfg, authorizer);
    })
    .await;
    let broker = handle.broker_arc_for_test();
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    handle.wait_until_transaction_coordinator_ready().await;
    seed_topic(&broker, "a", 1).await;
    seed_topic(&broker, "b", 1).await;
    (handle, dir)
}

/// Opens an empty transaction for `tid` under `producer_id` at producer epoch
/// 2 on a broker [`start_coordinator`] started.
pub(in crate::txn::handlers) async fn seed_transaction(
    broker: &crate::broker::Broker,
    tid: &str,
    producer_id: i64,
) {
    let txnv = crate::txn::version::resolve_txn_version(&broker.controller.current_image());
    broker
        .txn_coordinator
        .put(
            crate::txn::state::TxnEntry::new_empty(
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

/// The `(topic, partition)` pairs `tid`'s transaction holds.
pub(super) async fn enlisted(
    broker: &crate::broker::Broker,
    tid: &str,
) -> std::collections::BTreeSet<(String, i32)> {
    broker
        .txn_coordinator
        .get(tid)
        .expect("open transaction")
        .lock()
        .await
        .partitions
        .iter()
        .map(|tp| (tp.topic.clone(), tp.partition.get()))
        .collect()
}
