//! Fixtures shared by the `WriteTxnMarkers` unit tests: a running broker with
//! auditing switched off, and a locally-led partition opened beneath it so
//! that the handler finds it in `broker.partitions`.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use crate::broker::{Broker, BrokerHandle};

/// Open `topic-partition` under `log_dir` and register it with the broker.
/// The partition starts with no local leader role; the caller installs one.
pub(crate) fn open_partition(
    broker: &Broker,
    log_dir: &Path,
    setup: crate::test_support::StandalonePartitionSetup<'_>,
) -> Arc<crate::partition::Partition> {
    let part = crate::test_support::open_partition(log_dir, setup);
    broker
        .partitions
        .insert(setup.topic.into(), setup.partition, Arc::clone(&part));
    part
}

/// Start a broker with auditing switched off, and wait until its group
/// coordinator serves `__consumer_offsets`: the offsets-partition marker tests
/// append to that topic, and no broker creates it when it starts.
pub(super) async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_group_broker_with(|cfg| cfg.audit_enabled = false).await
}

/// Find the group's local offsets replica from one current metadata snapshot.
pub(super) fn local_offsets_partition(
    broker: &Broker,
    group_id: &str,
) -> Arc<crate::partition::Partition> {
    local_offsets_partition_with_index(broker, group_id).1
}

pub(super) fn local_offsets_partition_with_index(
    broker: &Broker,
    group_id: &str,
) -> (i32, Arc<crate::partition::Partition>) {
    let partition = crate::coordinator::partitioner::partition_for_group(
        &broker.controller.current_image(),
        group_id,
    );
    let part = broker
        .partitions
        .get(
            crate::coordinator::bootstrap::OFFSETS_TOPIC,
            PartitionIndex(partition),
        )
        .expect("local offsets partition");
    (partition, part)
}

/// One transaction record with sequence zero and unchanged protocol header defaults.
pub(super) fn single_transactional_batch(
    (producer_id, producer_epoch): (krabka_log::ProducerId, i16),
    record: Record,
) -> RecordBatch {
    RecordBatch {
        producer_id: producer_id.get(),
        producer_epoch,
        base_sequence: 0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![record],
        ..RecordBatch::default()
    }
}

/// A coordinator-written transaction containing one offset record. Batch
/// timestamps and the record's remaining fields retain their protocol defaults.
pub(super) fn transactional_offset_batch(
    (producer_id, producer_epoch): (krabka_log::ProducerId, i16),
    (group, topic, partition): (&str, &str, i32),
    value: &crate::coordinator::persistence::OffsetCommitValue,
) -> krabka_protocol::records::RecordBatch {
    single_transactional_batch(
        (producer_id, producer_epoch),
        Record {
            key: Some(
                crate::coordinator::persistence::OffsetCommitValue::encode_key(
                    group, topic, partition,
                )
                .unwrap(),
            ),
            value: Some(value.encode_value()),
            ..Record::default()
        },
    )
}
