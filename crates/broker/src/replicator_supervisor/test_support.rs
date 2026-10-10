//! Fixtures shared by the supervisor's unit tests: metadata-record builders, a
//! static `MetadataSource`, a counting `AssignDirsReporter`, and a supervisor
//! built over a temporary log dir.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use assert2::assert;
use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, LeaderEpoch, MetadataImage, MetadataRecord,
    PartitionRecord, TopicRecord,
};
use krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest;
use krabka_raft::NodeId;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ReplicatorSupervisor, ReplicatorSupervisorConfig, dir_assignments::AssignDirsReporter,
    materialize::MaterializePartitionConfig,
};
pub(super) use crate::test_support::await_until;
use crate::{
    config::ReplicationRuntimeConfig,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    test_support::{FakeMetadataSource, PartitionCount, ReplicationFactor},
    throttle::ThrottleState,
};

pub(super) fn image_with(records: &[MetadataRecord]) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for r in records {
        img.apply(r);
    }
    img
}

#[derive(Clone, Copy, Default)]
pub(super) enum SupervisorTopicIdentity {
    #[default]
    Fresh,
    Specified(Uuid),
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SupervisorTopicSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub identity: SupervisorTopicIdentity,
    pub partitions: PartitionCount,
    pub replication_factor: ReplicationFactor,
}

pub(super) fn topic_record(setup: SupervisorTopicSetup<'_>) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: setup.topic.into(),
        topic_id: match setup.identity {
            SupervisorTopicIdentity::Fresh => Uuid::new_v4(),
            SupervisorTopicIdentity::Specified(id) => id,
        },
        partitions: setup.partitions.0,
        replication_factor: setup.replication_factor.0,
    })
}

pub(super) fn single_partition_image(topic: &str, topic_id: Uuid, leader: NodeId) -> MetadataImage {
    image_with(&[
        topic_record(
            crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                topic,
                identity:
                    crate::replicator_supervisor::test_support::SupervisorTopicIdentity::Specified(
                        topic_id,
                    ),
                replication_factor: crate::test_support::ReplicationFactor(1),
                ..Default::default()
            },
        ),
        partition_record(
            crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                topic,
                leader,
                replicas: vec![leader],
                ..Default::default()
            },
        ),
    ])
}

/// One rf=3 topic and its partition, with the caller's exact replica list and epoch.
pub(super) fn topic_partition_records(setup: SupervisorPartitionSetup<'_>) -> [MetadataRecord; 2] {
    [
        topic_record(SupervisorTopicSetup {
            topic: setup.topic,
            ..Default::default()
        }),
        partition_record(setup),
    ]
}

pub(super) fn replicated_partition_image(setup: SupervisorPartitionSetup<'_>) -> MetadataImage {
    image_with(&topic_partition_records(setup))
}

/// The supervisor tests' rf=3 topic, with all three brokers as replicas.
pub(super) fn three_replica_image(leader: NodeId, epoch: LeaderEpoch) -> MetadataImage {
    replicated_partition_image(SupervisorPartitionSetup {
        leader,
        epoch,
        ..Default::default()
    })
}

pub(super) fn follower_promotion_images() -> (MetadataImage, MetadataImage) {
    let replicas = vec![NodeId(1), NodeId(2)];
    let topic =
        topic_record(crate::replicator_supervisor::test_support::SupervisorTopicSetup::default());
    (
        image_with(&[
            topic.clone(),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    replicas: replicas.clone(),
                    epoch: krabka_metadata::LeaderEpoch(3),
                    ..Default::default()
                },
            ),
        ]),
        image_with(&[
            topic,
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    leader: NodeId(2),
                    replicas,
                    epoch: krabka_metadata::LeaderEpoch(7),
                    ..Default::default()
                },
            ),
        ]),
    )
}

#[derive(Default)]
pub(super) struct MaterializeFixture {
    log_dir_status: crate::log_dir_status::LogDirRegistry,
    producer_state: Arc<crate::producer_state::ProducerState>,
}

#[derive(Clone, Copy)]
pub(super) struct MaterializeSetup<'a> {
    pub topic: &'a str,
    pub log_dirs: &'a [PathBuf],
    pub log_config: &'a LogConfig,
    pub context: &'a str,
}

impl Default for MaterializeSetup<'_> {
    fn default() -> Self {
        static LOG_CONFIG: std::sync::OnceLock<LogConfig> = std::sync::OnceLock::new();
        Self {
            topic: "t",
            log_dirs: &[],
            log_config: LOG_CONFIG.get_or_init(LogConfig::default),
            context: "materialize",
        }
    }
}

impl MaterializeFixture {
    pub(super) fn config<'a>(
        &'a self,
        partitions: &'a PartitionRegistry,
        setup: MaterializeSetup<'a>,
    ) -> MaterializePartitionConfig<'a> {
        MaterializePartitionConfig {
            partitions,
            topic: setup.topic,
            topic_id: None,
            partition: 0,
            log_dirs: setup.log_dirs,
            log_config: setup.log_config,
            log_dir_status: &self.log_dir_status,
            producer_state: &self.producer_state,
            runtime: crate::partition::PartitionRuntimeConfig::new(
                (1_024, 64, 3),
                false,
                (None, None, None),
            ),
        }
    }

    pub(super) fn materialize(&self, partitions: &PartitionRegistry, setup: MaterializeSetup<'_>) {
        super::materialize::materialize_partition(self.config(partitions, setup))
            .expect(setup.context);
    }
}

/// Materialize `t-0` with the usual config, retaining its directory and registry.
pub(super) fn materialized_partition() -> (tempfile::TempDir, Arc<PartitionRegistry>) {
    materialized_partition_with_config(MaterializeSetup::default().log_config)
}

/// Materialize `t-0` using the caller's log policy and retain its directory guard.
pub(super) fn materialized_partition_with_config(
    log_config: &LogConfig,
) -> (tempfile::TempDir, Arc<PartitionRegistry>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    MaterializeFixture::default().materialize(
        &partitions,
        crate::replicator_supervisor::test_support::MaterializeSetup {
            log_dirs: &[dir.path().to_path_buf()],
            log_config,
            ..Default::default()
        },
    );
    (dir, partitions)
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct SupervisorPartitionSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub partition: PartitionIndex,
    #[default(NodeId(1))]
    pub leader: NodeId,
    #[default(vec![NodeId(1), NodeId(2), NodeId(3)])]
    pub replicas: Vec<NodeId>,
    pub epoch: LeaderEpoch,
}

impl<'a> SupervisorPartitionSetup<'a> {
    pub(super) fn single_replica(topic: &'a str, leader: NodeId) -> Self {
        Self {
            topic,
            leader,
            replicas: vec![leader],
            ..Default::default()
        }
    }
}

pub(super) fn partition_record(setup: SupervisorPartitionSetup<'_>) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: setup.topic.into(),
        partition: setup.partition.0,
        leader: setup.leader,
        replicas: setup.replicas.clone(),
        isr: setup.replicas,
        leader_epoch: setup.epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

pub(super) fn broker_record(node_id: NodeId) -> BrokerRegistrationRecord {
    BrokerRegistrationRecord {
        incarnation_id: Uuid::new_v4(),
        host: "legacy-host".into(),
        endpoints: vec![BrokerEndpoint {
            name: "INTERNAL".into(),
            host: "internal-host".into(),
            port: 19092,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }],
        ..crate::test_support::broker_registration(node_id)
    }
}

/// A metadata source over `image` with no controller leader elected, and a
/// loopback controller listener for the assign-dirs reporter to resolve
/// against.
pub(super) fn static_source(image: MetadataImage) -> FakeMetadataSource {
    FakeMetadataSource::static_image(image)
}

#[derive(Default)]
pub(super) struct CountingAssignDirsReporter {
    pub(super) calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AssignDirsReporter for CountingAssignDirsReporter {
    async fn send(
        &self,
        _controller: &Arc<dyn MetadataSource>,
        _client_id: &str,
        req: AssignReplicasToDirsRequest,
    ) -> Result<(), String> {
        assert!(!req.directories.is_empty());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

pub(super) type SupervisorFixture = (
    ReplicatorSupervisor,
    Arc<PartitionRegistry>,
    Arc<CountingAssignDirsReporter>,
    tempfile::TempDir,
);

/// Reconcile once and retrieve `t-0`, retaining every supervisor fixture guard.
pub(super) async fn reconciled_partition(
    image: &MetadataImage,
    context: &str,
) -> (SupervisorFixture, Arc<crate::partition::Partition>) {
    let fixture = reconciled_supervisor(image).await;
    let partition = fixture
        .1
        .get("t", krabka_ids::PartitionIndex(0))
        .expect(context);
    (fixture, partition)
}

/// A supervisor fixture after the first metadata reconciliation, with every guard retained.
pub(super) async fn reconciled_supervisor(image: &MetadataImage) -> SupervisorFixture {
    let fixture = supervisor_fixture(image.clone());
    fixture.0.reconcile(image).await;
    fixture
}

pub(super) fn supervisor_fixture(image: MetadataImage) -> SupervisorFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let partitions = Arc::new(PartitionRegistry::new());
    let reporter = Arc::new(CountingAssignDirsReporter::default());
    let mut supervisor = ReplicatorSupervisor::new(ReplicatorSupervisorConfig {
        node_id: NodeId(2),
        broker_id: 2,
        controller: Arc::new(static_source(image)),
        partitions: partitions.clone(),
        log_dirs: vec![dir.path().to_path_buf()],
        log_config: LogConfig::default(),
        unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
        client_id: "supervisor-test".into(),
        shutdown: CancellationToken::new(),
        txn_coordinator: None,
        share_coordinator: None,
        inter_broker_client: Arc::new(crate::network::client::InterBrokerClient::new(None, None)),
        inter_broker_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        inter_broker_server_name: "localhost".into(),
        inter_broker_listener_name: "INTERNAL".into(),
        controller_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
        controller_server_name: "localhost".into(),
        controller_quorum_voters: Vec::new(),
        replication: ReplicationRuntimeConfig::default(),
        throttle_state: Arc::new(ThrottleState::new()),
        log_dir_status: crate::log_dir_status::LogDirRegistry::default(),
        producer_state: Arc::new(crate::producer_state::ProducerState::new()),
        max_produce_group: 1_024,
        partition_writer_queue_depth: 64,
        diskless_wal_local_replica_count: 3,
        metrics: crate::metrics::BrokerMetrics::default(),
        log_dir_ids: crate::log_dir_id::LogDirIds::resolve(&[dir.path().to_path_buf()]),
        hot_tail: Arc::new(crate::diskless::hot_tail::HotTailCache::default()),
        wal_shards: Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
            krabka_raft::NodeId(2),
        )),
    });
    supervisor.assign_dirs_reporter = reporter.clone();
    (supervisor, partitions, reporter, dir)
}
