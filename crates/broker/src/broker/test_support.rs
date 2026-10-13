//! Fixtures shared by the unit tests of several `broker` submodules: the
//! [`crate::metadata_source::MetadataSource`] they read from, a locally
//! spawned partition seeded with records, and the metadata records that tests
//! submit. They live in one module so no submodule owns a helper its siblings
//! also need.

use {
    crate::{
        broker::BrokerHandle,
        log_dir::partition_dir,
        partition::Partition,
        test_support::{FakeMetadataSource, single_partition_topic, spawn_standalone_partition},
    },
    assert2::assert,
    krabka_ids::{LeaderEpoch, NodeId, PartitionIndex},
    std::sync::Arc,
};

/// A metadata source over `image`, with `leader` as the controller leader and
/// a loopback controller listener for the gauges and adapter paths to report.
pub(super) fn fake_source(
    image: krabka_metadata::MetadataImage,
    leader: Option<krabka_raft::NodeId>,
) -> FakeMetadataSource {
    FakeMetadataSource::builder()
        .image(image)
        .leader(leader)
        .controller_bound_addr("127.0.0.1:9093".parse().expect("loopback controller addr"))
        .build()
}

#[derive(Clone, Copy)]
pub(super) struct LocalPartitionSetup<'a> {
    pub topic: &'a str,
    pub partition: PartitionIndex,
    pub values: &'a [&'static [u8]],
}

impl Default for LocalPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            partition: PartitionIndex::default(),
            values: &[],
        }
    }
}

#[bon::builder]
pub(super) fn local_partition_with_records(
    log_dir: &std::path::Path,
    #[builder(default = LocalPartitionSetup::default().topic)] topic: &'_ str,
    #[builder(into, default = LocalPartitionSetup::default().partition)] partition: PartitionIndex,
    #[builder(default = LocalPartitionSetup::default().values)] values: &'_ [&'static [u8]],
) -> Arc<Partition> {
    let setup = LocalPartitionSetup {
        topic,
        partition,
        values,
    };
    let LocalPartitionSetup {
        topic,
        partition,
        values,
    } = setup;
    let part_dir = partition_dir(log_dir, topic, partition.0);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default())
        .expect("open partition log");
    let part = spawn_standalone_partition()
        .log_dir(log_dir)
        .log(log)
        .topic(topic)
        .partition(partition)
        .call();
    if !values.is_empty() {
        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
            records: values
                .iter()
                .enumerate()
                .map(|(idx, value)| krabka_protocol::records::Record {
                    offset_delta: i32::try_from(idx).expect("offset delta fits"),
                    value: Some(bytes::Bytes::from_static(value)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        part.log
            .lock()
            .expect("partition log lock")
            .append(&mut batch)
            .expect("append records");
    }
    part
}

pub(super) fn metadata_topic_record(
    topic: &str,
    topic_id: uuid::Uuid,
) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(single_partition_topic(topic, topic_id))
}

#[derive(Clone)]
pub(super) struct MetadataPartitionSetup<'a> {
    pub topic: &'a str,
    pub partition: PartitionIndex,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    pub leader_epoch: LeaderEpoch,
}

impl Default for MetadataPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            partition: PartitionIndex::default(),
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(3),
        }
    }
}

#[bon::builder]
pub(super) fn metadata_partition_record(
    #[builder(default = MetadataPartitionSetup::default().topic)] topic: &'_ str,
    #[builder(into, default = MetadataPartitionSetup::default().partition)]
    partition: PartitionIndex,
    #[builder(into, default = MetadataPartitionSetup::default().leader)] leader: NodeId,
    #[builder(default = MetadataPartitionSetup::default().replicas)] replicas: Vec<NodeId>,
    #[builder(default = MetadataPartitionSetup::default().isr)] isr: Vec<NodeId>,
    #[builder(into, default = MetadataPartitionSetup::default().leader_epoch)]
    leader_epoch: LeaderEpoch,
) -> krabka_metadata::PartitionRecord {
    let setup = MetadataPartitionSetup {
        topic,
        partition,
        leader,
        replicas,
        isr,
        leader_epoch,
    };
    let MetadataPartitionSetup {
        topic,
        partition,
        leader,
        replicas,
        isr,
        leader_epoch,
        ..
    } = setup;
    let directories = vec![uuid::Uuid::nil(); replicas.len()];
    krabka_metadata::PartitionRecord {
        topic: topic.to_string(),
        partition: partition.0,
        leader,
        replicas,
        isr,
        leader_epoch,
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories,
        partition_epoch: 0,
    }
}

#[bon::builder]
pub(super) async fn submit_metadata_topic_partition(
    handle: &BrokerHandle,
    topic_id: uuid::Uuid,
    partition_record: krabka_metadata::PartitionRecord,
) {
    let topic = partition_record.topic.as_str();
    handle
        .submit_metadata_record_for_test(metadata_topic_record(topic, topic_id))
        .await
        .expect("submit topic record");
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
            partition_record.clone(),
        ))
        .await
        .expect("submit partition record");

    let image = handle.controller_image_for_test();
    assert!(image.topic(topic).is_some());
    assert!(image.partition(topic, partition_record.partition) == Some(&partition_record));
}
