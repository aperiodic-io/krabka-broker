//! Construction of a partition's runtime: the diskless WAL wiring, the writer
//! task, and the notify/watermark plumbing that a `Partition` owns. Every
//! caller in the crate that materialises a partition goes through here, so the
//! spawn paths live apart from the broker's startup sequence.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicI32, AtomicU64},
};

use krabka_ids::PartitionIndex;

use crate::{
    config::BrokerConfig,
    delivery::DeliveryHandles,
    diskless::hot_tail::HotTailCache,
    error::BrokerError,
    log_dir_status::LogDirRegistry,
    partition::{
        Partition, PartitionRuntimeConfig, ReplicationTarget, WriterMessage,
        empty_marker_materialization, initial_replication_target,
    },
    partition_writer::run_with_sequencer,
    producer_state::ProducerState,
    replica_state::ReplicaState,
    wal::{
        SharedWal,
        quorum::{
            QuorumWalStore,
            engine::WalShardEngine,
            registry::{ShardId, WalShardRegistry},
        },
    },
};

type PartitionWal = (
    Option<SharedWal>,
    Option<krabka_log::Offset>,
    Option<Arc<WalShardEngine>>,
);

fn partition_wal(
    identity: (&str, Option<uuid::Uuid>, PartitionIndex),
    log: Arc<Mutex<krabka_log::Log>>,
    diskless: bool,
    hot_tail: Option<Arc<HotTailCache>>,
    wal_shards: Option<Arc<WalShardRegistry>>,
    replica_count: usize,
) -> Result<PartitionWal, BrokerError> {
    let (topic, topic_id, partition_id) = identity;
    if !diskless {
        return Ok((None, None, None));
    }
    let topic_id = topic_id.ok_or_else(|| {
        BrokerError::Replication(format!(
            "diskless WAL topic id is not available for {topic}-{}",
            partition_id.0
        ))
    })?;
    let registry = wal_shards.ok_or_else(|| {
        BrokerError::Replication(format!(
            "diskless WAL shard registry is not available for {topic}-{}",
            partition_id.0
        ))
    })?;
    let wal = QuorumWalStore::for_distributed_partition(
        topic_id,
        partition_id,
        log,
        hot_tail,
        replica_count,
    )?;
    registry.insert(
        ShardId {
            topic_id,
            partition: partition_id,
        },
        wal.engine(),
    );
    let durable_watermark = wal.engine().durable_watermark();
    let engine = wal.engine();
    Ok((
        Some(Arc::new(wal) as SharedWal),
        Some(durable_watermark),
        Some(engine),
    ))
}

/// Create the partition runtime (mpsc channel + writer task + notify).
///
/// `log_dir` is the parent `log.dir` that owns the partition (i.e. the
/// configured directory, not the `<topic>-<partition>` subdirectory).
/// Stored on the `Partition` so KIP-113 (`AlterReplicaLogDirs`) can
/// reject moves whose target is the partition's current dir without
/// access to the `Log` mutex on the hot path, and so
/// `DescribeLogDirs` can attribute the partition to a dir even when
/// the path is not stable across canonicalisation.
#[krabka_macros::partition_spawn_parameters]
pub(crate) fn spawn_partition() -> Arc<Partition> {
    #[cfg(test)]
    let topic_id = diskless.then(uuid::Uuid::new_v4);
    #[cfg(not(test))]
    let topic_id = None;
    spawn_partition_with_replication_target(
        topic,
        ReplicationTarget {
            topic_id,
            leader_node_id: krabka_raft::NodeId(0),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
        },
        partition_id,
        log_dir,
        log,
        log_dir_status,
        producer_state,
        diskless,
    )
}

#[allow(clippy::too_many_arguments)] // Mirrors spawn_partition plus the replication target.
#[krabka_macros::partition_spawn_parameters(target)]
pub(crate) fn spawn_partition_with_replication_target() -> Arc<Partition> {
    let broker_config = BrokerConfig::default();
    #[cfg(test)]
    let wal_shards = replication_target.topic_id.map(|topic_id| {
        use crate::wal::quorum::registry::{ShardId, WalPlacement};

        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(0)));
        registry.replace_placements(&std::collections::HashMap::from([(
            ShardId {
                topic_id,
                partition: partition_id,
            },
            WalPlacement {
                voters: vec![krabka_raft::NodeId(0)],
                leader_epoch: 0,
            },
        )]));
        registry
    });
    #[cfg(not(test))]
    let wal_shards = None;
    #[cfg(test)]
    let diskless_wal_local_replica_count = if diskless {
        1
    } else {
        broker_config.diskless_wal_local_replica_count
    };
    #[cfg(not(test))]
    let diskless_wal_local_replica_count = broker_config.diskless_wal_local_replica_count;
    try_spawn_partition_with_replication_target(
        PartitionSpawnConfig {
            topic,
            topic_id: replication_target.topic_id,
            partition_id,
            log_dir,
            log,
            log_dir_status,
            producer_state,
            runtime: PartitionRuntimeConfig::new(
                (
                    broker_config.max_produce_group,
                    broker_config.partition_writer_queue_depth,
                    diskless_wal_local_replica_count,
                ),
                diskless,
                (None, wal_shards, None),
            ),
        },
        replication_target,
    )
    .expect("spawn partition")
}

pub(crate) struct PartitionSpawnConfig {
    pub topic: String,
    pub topic_id: Option<uuid::Uuid>,
    pub partition_id: PartitionIndex,
    pub log_dir: std::path::PathBuf,
    pub log: krabka_log::Log,
    pub log_dir_status: LogDirRegistry,
    pub producer_state: Arc<ProducerState>,
    pub runtime: PartitionRuntimeConfig,
}

pub(crate) fn try_spawn_partition_with_sequencer(
    config: PartitionSpawnConfig,
) -> Result<Arc<Partition>, BrokerError> {
    let initial_target = ReplicationTarget {
        topic_id: config.topic_id,
        leader_node_id: krabka_raft::NodeId(0),
        leader_epoch: krabka_metadata::LeaderEpoch(0),
    };
    try_spawn_partition_with_replication_target(config, initial_target)
}

pub(crate) fn try_spawn_partition_with_replication_target(
    config: PartitionSpawnConfig,
    initial_target: ReplicationTarget,
) -> Result<Arc<Partition>, BrokerError> {
    assert2::assert!((config.topic_id) == (initial_target.topic_id));
    let PartitionSpawnConfig {
        topic,
        topic_id,
        partition_id,
        log_dir,
        log,
        log_dir_status,
        producer_state,
        runtime,
    } = config;
    let PartitionRuntimeConfig {
        max_produce_group,
        partition_writer_queue_depth,
        diskless_wal_local_replica_count,
        diskless,
        hot_tail,
        wal_shards,
        sequencer,
    } = runtime;
    let log = Arc::new(Mutex::new(log));
    let (wal, recovered_durable_watermark, wal_engine) = partition_wal(
        (&topic, topic_id, partition_id),
        Arc::clone(&log),
        diskless,
        hot_tail,
        wal_shards,
        diskless_wal_local_replica_count,
    )?;
    let (tx, rx) = tokio::sync::mpsc::channel::<WriterMessage>(partition_writer_queue_depth);
    let notify = Arc::new(tokio::sync::Notify::new());
    let delivery = DeliveryHandles::new();
    // Seed the watermark mirror from the log the recovery walk just opened, so
    // a reader that takes no lock never sees the placeholder zero.
    delivery.publish_now(&log);
    let mut initial_replica_state = ReplicaState::new();
    initial_replica_state.current_leader_epoch =
        krabka_ids::LeaderEpoch(initial_target.leader_epoch.0);
    if let Some(durable_watermark) = recovered_durable_watermark {
        initial_replica_state.recompute_hw_for_wal_durable(durable_watermark);
    }
    let initial_wal_watermark = initial_replica_state.hw;
    let replica_state = Arc::new(tokio::sync::Mutex::new(initial_replica_state));
    let hw_advance_notify = Arc::new(tokio::sync::Notify::new());
    let current_leader = Arc::new(AtomicU64::new(initial_target.leader_node_id.0));
    let current_leader_epoch = Arc::new(AtomicI32::new(initial_target.leader_epoch.0));
    let replication_target = initial_replication_target(topic_id);
    *replication_target
        .try_write()
        .expect("new partition target is uncontended") = initial_target;
    let log_dir = Arc::new(arc_swap::ArcSwap::from_pointee(log_dir));
    let writer_future = run_with_sequencer(
        (topic.clone(), partition_id),
        (log.clone(), log_dir.clone()),
        rx,
        (
            notify.clone(),
            replica_state.clone(),
            hw_advance_notify.clone(),
            delivery.clone(),
        ),
        (log_dir_status, producer_state, wal),
        max_produce_group,
        sequencer,
    );
    let writer = if let Some(engine) = wal_engine {
        let replica_state = replica_state.clone();
        let hw_advance_notify = hw_advance_notify.clone();
        tokio::spawn(async move {
            let watermark_updates = async move {
                let mut observed = initial_wal_watermark;
                loop {
                    let durable = engine.wait_for_durable_advance(observed).await;
                    observed = durable;
                    let mut state = replica_state.lock().await;
                    let previous = state.hw;
                    state.recompute_hw_for_wal_durable(durable);
                    if state.hw != previous {
                        hw_advance_notify.notify_waiters();
                    }
                }
            };
            tokio::select! {
                () = writer_future => {}
                () = watermark_updates => {}
            }
        })
    } else {
        tokio::spawn(writer_future)
    };
    Ok(Arc::new(Partition {
        topic,
        index: partition_id,
        log_dir,
        log,
        writer_tx: tx,
        marker_materialization: empty_marker_materialization(),
        append_notify: notify,
        replica_state,
        hw_advance_notify,
        delivery,
        current_leader,
        current_leader_epoch,
        replication_target,
        diskless,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    }))
}

#[cfg(test)]
mod tests;
