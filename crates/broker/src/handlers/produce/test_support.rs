//! Fixture builders that more than one of the produce handler's unit-test
//! modules needs, kept in one place so each of them builds the same image,
//! `min.insync.replicas` override, and record batch.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        config_keys::MIN_INSYNC_REPLICAS,
        freeze::resolve::FreezeMutationResolution,
        handlers::test_support::{
            PartitionSpawnSetup, ReplicatedPartitionSetup, replicated_partition, spawn_partition,
        },
        log_dir_status::LogDirRegistry,
        metrics::{BrokerMetrics, RequestPhases},
        partition::Partition,
        partition_registry::PartitionRegistry,
        producer_id_manager::ProducerIdManager,
        producer_state::ProducerState,
        test_support::{replica_nodes, string_pairs, topic_partition_image},
        txn::coordinator::TxnCoordinator,
    },
    bytes::{Bytes, BytesMut},
    krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord},
    krabka_protocol::records::{Attributes, Record, RecordBatch, RecordsPayload},
    std::{collections::BTreeMap, sync::Arc},
};

/// The default services of a standalone produce pipeline, shared by tests
/// that vary one admission gate or one writer outcome.
pub(super) struct PipelineFixture {
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) txn_coordinator: Arc<TxnCoordinator>,
    pub(super) producer_state: Arc<ProducerState>,
    pub(super) log_dir_status: LogDirRegistry,
    pub(super) metrics: BrokerMetrics,
    phases: RequestPhases,
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct PipelinePartitionSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    pub log_config: krabka_log::LogConfig,
}

impl PipelineFixture {
    pub(super) async fn register_partition(
        &self,
        root: &std::path::Path,
        topic: &str,
        image: &MetadataImage,
    ) -> Arc<Partition> {
        let partition = self.partition(root, topic, image).await;
        self.partitions.insert(
            topic.into(),
            krabka_ids::PartitionIndex(0),
            Arc::clone(&partition),
        );
        partition
    }

    pub(super) async fn partition(
        &self,
        root: &std::path::Path,
        topic: &str,
        image: &MetadataImage,
    ) -> Arc<Partition> {
        self.partition_with_config(root, image, PipelinePartitionSetup {
            topic,
            ..Default::default()
        })
        .await
    }

    pub(super) async fn partition_with_config(
        &self,
        root: &std::path::Path,
        image: &MetadataImage,
        setup: PipelinePartitionSetup<'_>,
    ) -> Arc<Partition> {
        let PipelinePartitionSetup { topic, log_config } = setup;
        let partition = spawn_partition(root, PartitionSpawnSetup {
            topic,
            log_dir_status: self.log_dir_status.clone(),
            producer_state: Arc::clone(&self.producer_state),
            log_config,
            ..Default::default()
        });
        let record = image.partition(topic, 0).expect("partition");
        let topic_id = image.topic(topic).expect("topic").topic_id;
        partition
            .install_replication_target(Some(topic_id), record.leader.0, record.leader_epoch.0)
            .await;
        partition
            .install_isr(&record.isr, &record.replicas, record.leader)
            .await;
        partition
    }

    pub(super) fn new(node_id: krabka_ids::NodeId) -> Self {
        let partitions = Arc::new(PartitionRegistry::new());
        let txn_coordinator = Arc::new(TxnCoordinator::new(
            node_id,
            Arc::clone(&partitions),
            Arc::new(ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        ));
        Self {
            partitions,
            txn_coordinator,
            producer_state: Arc::new(ProducerState::new()),
            log_dir_status: LogDirRegistry::default(),
            metrics: BrokerMetrics::new(),
            phases: RequestPhases::default(),
        }
    }

    pub(super) fn services<'a>(
        &'a self,
        image: &'a Arc<MetadataImage>,
    ) -> super::pipeline::PartitionServices<'a> {
        super::pipeline::PartitionServices {
            partitions: &self.partitions,
            txn_coordinator: &self.txn_coordinator,
            producer_state: &self.producer_state,
            log_dir_status: &self.log_dir_status,
            image,
            broker_policy: super::leadership::BrokerProducePolicy {
                node_id: krabka_audit::NodeId(1),
                default_min_insync_replicas: 1,
                is_witness: false,
            },
            record_decompression_policy: krabka_compression::RecordDecompressionPolicy::default(),
            metrics: &self.metrics,
            phases: &self.phases,
            schema_validator: None,
            unstable_api_versions: UnstableApiVersions::Disabled,
        }
    }
}

pub(super) fn pipeline_input(
    topic: &str,
    payload: super::framing::PartitionPayload,
) -> super::pipeline::PartitionInput<'static> {
    super::pipeline::PartitionInput {
        part_data: super::framing::FramedPartition { index: 0, payload },
        topic_compression: None,
        timestamps: super::topic_settings::TimestampPolicy::default(),
        compacted_topic: false,
        max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
        delivery: None,
        schema: None,
        topic_name: topic.into(),
        freeze: FreezeMutationResolution::Admit,
        internal_topic_denied: false,
        transaction: super::producer_checks::TransactionRequest {
            transactional_id: None,
            version: 9,
            producer_id_expiration_ms: 86_400_000,
            verification_enabled: true,
        },
        acks: 1,
    }
}

pub(crate) fn image_with_topic(topic: &str, isr: &[u64]) -> MetadataImage {
    topic_partition_image(topic, i16::try_from(isr.len().max(1)).unwrap(), || {
        replicated_partition(ReplicatedPartitionSetup {
            topic,
            leader: krabka_audit::NodeId(*isr.first().unwrap_or(&1)),
            replicas: &replica_nodes(isr),
            ..Default::default()
        })
    })
}

#[bon::builder]
pub(super) fn one_transactional_batch(producer_id: i64, producer_epoch: i16) -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        producer_id,
        producer_epoch,
        base_sequence: 0,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }])
}

pub(super) fn set_min_isr(img: &mut MetadataImage, topic: &str, n: i32) {
    let mut o = BTreeMap::new();
    o.insert(MIN_INSYNC_REPLICAS.into(), n.to_string());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: o,
    }));
}

pub(super) fn image_with_overrides(topic: &str, overrides: &[(&str, &str)]) -> MetadataImage {
    let mut image = image_with_topic(topic, &[1]);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: string_pairs(overrides),
    }));
    image
}

pub(super) fn encode_batch(batch: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    batch.encode(&mut buf).expect("encode record batch");
    buf.freeze()
}
