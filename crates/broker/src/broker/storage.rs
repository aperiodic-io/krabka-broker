//! Log-directory recovery and group/coordinator construction. This phase scans
//! every configured `log.dir`, reopens and recovers each partition, and builds
//! the group coordinator and producer-id manager on top of them. It is its own
//! module because it is the one startup step that touches on-disk state.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_units::convert::TimeExt as _;

use crate::{
    broker::{
        DisklessRuntime,
        partition_spawn::{PartitionSpawnConfig, try_spawn_partition_with_replication_target},
    },
    config::BrokerConfig,
    config_keys::resolve_diskless,
    coordinator::{
        GroupCoordinator,
        bootstrap::{bootstrap, bootstrap_audit_topic},
        unified::{ImageMetadataProvider, offsets_log::ProductionOffsetsLog},
    },
    cordoned_log_dirs::CordonedLogDirs,
    diskless::recovery::{open_config, recover_open_log},
    error::BrokerError,
    log_dir,
    log_dir_id::LogDirIds,
    log_dir_status::LogDirRegistry,
    metadata_source::MetadataSource,
    partition::{PartitionRuntimeConfig, ReplicationTarget},
    partition_registry::PartitionRegistry,
    producer_id_manager::ProducerIdManager,
    producer_state::ProducerState,
    replicator_supervisor::desired_wal_placements,
    wal::{
        ControllerSequencer, OffsetSequencer,
        quorum::{follower::hydrate_on_promotion, registry::ShardId},
    },
};

pub(super) struct StorageStartup {
    pub(super) log_dir_status: LogDirRegistry,
    pub(super) log_dir_ids: LogDirIds,
    pub(super) partitions: Arc<PartitionRegistry>,
    pub(super) producer_state: Arc<ProducerState>,
    pub(super) group_coordinator: Arc<GroupCoordinator>,
    pub(super) producer_ids: Arc<ProducerIdManager>,
}

pub(super) async fn recover_storage_and_groups(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    diskless_runtime: &DisklessRuntime,
) -> Result<StorageStartup, BrokerError> {
    let log_dirs = config.all_log_dirs();
    let log_dir_status = LogDirRegistry::probe(&log_dirs).with_metadata_dir(config.metadata_dir());
    let log_dir_ids = LogDirIds::provision(
        &log_dirs,
        config.cluster_id.unwrap_or_else(uuid::Uuid::nil),
        config.node_id,
    );
    let partitions = Arc::new(
        PartitionRegistry::with_stamp_source(config.stamp_source.as_ref().map(Arc::clone))
            .with_cordoned_log_dirs(CordonedLogDirs::new(
                log_dirs.clone(),
                config.cordoned_log_dirs.clone(),
            )),
    );
    let producer_state = Arc::new(ProducerState::new());
    if config.is_broker() {
        let startup_image = controller.current_image();
        let scan_dirs = log_dir_status.online_subset(&log_dirs);
        let wal_placements =
            desired_wal_placements(&startup_image, config.diskless_wal_local_replica_count);
        for (topic, partition_id, owning_dir) in log_dir::scan_all(&scan_dirs)? {
            let directory = log_dir::partition_dir(&owning_dir, &topic, partition_id);
            let diskless = resolve_diskless(startup_image.topic_config(&topic));
            let open_config = open_config(&config.log_config, diskless);
            let mut log = krabka_log::Log::open(&directory, open_config)?;
            if let Some(stamp_source) = partitions.stamp_source() {
                log.set_stamp_source(stamp_source)?;
            }
            if diskless {
                if let Some(topic_id) = startup_image.topic(&topic).map(|topic| topic.topic_id) {
                    let shard = ShardId {
                        topic_id,
                        partition: PartitionIndex(partition_id),
                    };
                    if wal_placements
                        .get(&shard)
                        .and_then(|placement| placement.voters.first())
                        == Some(&config.node_id)
                    {
                        // Promotion hydration is deliberately repeated before
                        // the partition writer starts. If the preceding process
                        // crashed after adopting only part of the checkpointed
                        // follower prefix, the exact-overlap check makes this a
                        // safe retry and closes the remaining durable tail
                        // before any request can observe the partition.
                        hydrate_on_promotion(
                            &scan_dirs,
                            &topic,
                            shard,
                            config.node_id,
                            &config.log_config,
                            &mut log,
                        )?;
                    }
                }
                recover_open_log(
                    &topic,
                    PartitionIndex(partition_id),
                    &mut log,
                    &producer_state,
                    startup_image.partition_next_offset(&topic, partition_id),
                )
                .await?;
            } else {
                producer_state
                    .rebuild_from_log(&topic, PartitionIndex(partition_id), &log)
                    .await?;
            }
            let topic_id = startup_image.topic(&topic).map(|topic| topic.topic_id);
            let initial_target = if diskless {
                ReplicationTarget {
                    topic_id,
                    leader_node_id: krabka_raft::NodeId(0),
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                }
            } else {
                startup_image.partition(&topic, partition_id).map_or(
                    ReplicationTarget {
                        topic_id,
                        leader_node_id: krabka_raft::NodeId(0),
                        leader_epoch: krabka_metadata::LeaderEpoch(0),
                    },
                    |record| ReplicationTarget {
                        topic_id,
                        leader_node_id: record.leader,
                        leader_epoch: record.leader_epoch,
                    },
                )
            };
            let partition = try_spawn_partition_with_replication_target(
                PartitionSpawnConfig {
                    topic: topic.clone(),
                    topic_id,
                    partition_id: PartitionIndex(partition_id),
                    log_dir: owning_dir,
                    log,
                    log_dir_status: log_dir_status.clone(),
                    producer_state: Arc::clone(&producer_state),
                    runtime: PartitionRuntimeConfig::from_broker(
                        config,
                        diskless,
                        (
                            Some(Arc::clone(&diskless_runtime.hot_tail)),
                            Some(Arc::clone(&diskless_runtime.wal_shards)),
                            diskless.then(|| {
                                Arc::new(ControllerSequencer::new(Arc::clone(controller)))
                                    as Arc<dyn OffsetSequencer>
                            }),
                        ),
                    ),
                },
                initial_target,
            )?;
            partitions.insert(Arc::from(topic), PartitionIndex(partition_id), partition);
        }
    }
    let offsets_log = Arc::new(ProductionOffsetsLog::new(
        Arc::clone(&partitions),
        Arc::clone(controller),
        config.node_id,
    ));
    let mut consumer_group = config.next_gen_consumer_group.as_ref().clone();
    consumer_group.session_expiry_tick = config.coordinator_session_expiry_tick.to_std();
    consumer_group.actor_mailbox_capacity = config.coordinator_actor_mailbox_capacity;
    consumer_group.shutdown_ack_timeout = config.coordinator_shutdown_ack_timeout.to_std();
    consumer_group.classic_initial_rebalance_delay =
        config.classic_group_initial_rebalance_delay.to_std();
    let mut share_group = config.share_group.as_ref().clone();
    share_group.actor_mailbox_capacity = config.coordinator_actor_mailbox_capacity;
    let mut streams_group = config.streams_group.as_ref().clone();
    streams_group.actor_mailbox_capacity = config.coordinator_actor_mailbox_capacity;
    streams_group.unstable_api_versions = config.features.unstable_api_versions;
    let group_coordinator = Arc::new(GroupCoordinator::new(
        consumer_group,
        share_group,
        Arc::new(ImageMetadataProvider {
            controller: Arc::clone(controller),
        }),
        offsets_log,
        streams_group,
    ));
    let producer_ids = Arc::new(ProducerIdManager::clustered(
        config.node_id,
        config.broker_epoch,
        Arc::clone(controller),
    ));
    bootstrap(
        config,
        controller,
        &partitions,
        &group_coordinator,
        &log_dir_status,
        &producer_state,
    )
    .await?;
    bootstrap_audit_topic(config, controller).await?;
    Ok(StorageStartup {
        log_dir_status,
        log_dir_ids,
        partitions,
        producer_state,
        group_coordinator,
        producer_ids,
    })
}
