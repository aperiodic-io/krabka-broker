//! `__consumer_offsets` topic lifecycle.
//!
//! At startup the module opens the local partitions of the topic and replays
//! every record synchronously into the in-memory `GroupCoordinator`. The first
//! `FindCoordinator(GROUP)` creates the topic, not the startup.

use {
    crate::{
        broker::spawn_partition,
        config::{BrokerConfig, DEFAULT_AUDIT_TOPIC},
        coordinator::GroupCoordinator,
        error::BrokerError,
        log_dir,
        log_dir_status::LogDirRegistry,
        metadata_source::MetadataSource,
        partition_registry::PartitionRegistry,
        producer_state::ProducerState,
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::PartitionRecord,
    krabka_units::convert::ByteSizeExt as _,
    std::{collections::BTreeMap, sync::Arc},
};

mod apply;
mod audit;
mod replay;

#[cfg(test)]
mod classic_state_tests;
#[cfg(test)]
mod delete_groups_replay_tests;
#[cfg(test)]
mod group_type_replay_tests;
#[cfg(test)]
mod log_walk_tests;
#[cfg(test)]
mod partition_metadata_replay_tests;
#[cfg(test)]
mod share_streams_replay_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod topic_bootstrap_tests;
#[cfg(test)]
mod unknown_record_tests;
#[cfg(test)]
mod upgrade_downgrade_tests;

pub use self::audit::bootstrap_audit_topic;
pub(crate) use self::replay::replay_partition;
use self::replay::{Replayed, finalize, replay_records};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
/// First offsets partition, used by focused compatibility tests.
#[cfg(test)]
pub const OFFSETS_PARTITION: i32 = 0;
/// Kafka's default offsets-topic partition count. Existing clusters retain the
/// partition count recorded in metadata; this value is only used at creation.
pub const OFFSETS_NUM_PARTITIONS: i32 = 50;

/// Internal topic that carries tamper-evident OCSF audit records for the
/// `FedRAMP` MLA, under its default name.
///
/// This is an alias for [`crate::config::DEFAULT_AUDIT_TOPIC`] rather than a
/// second spelling of the name: `krabka.audit.topic` renames the audit log, so
/// every code path that has to reach the live one reads
/// `BrokerConfig::audit_topic` instead. Tests that boot a default broker use
/// this.
pub const AUDIT_TOPIC: &str = DEFAULT_AUDIT_TOPIC;

/// The topic configs Kafka writes when it creates `__consumer_offsets`
/// (`GroupCoordinatorService.groupMetadataTopicConfigs`). Offsets and group
/// metadata are keyed records, so the topic is compacted, never deleted by
/// time.
pub(crate) fn offsets_topic_configs(config: &BrokerConfig) -> BTreeMap<String, String> {
    use crate::config_keys::{CLEANUP_POLICY, COMPRESSION_TYPE, SEGMENT_BYTES};
    BTreeMap::from([
        (CLEANUP_POLICY.to_owned(), "compact".to_owned()),
        (COMPRESSION_TYPE.to_owned(), "producer".to_owned()),
        (
            SEGMENT_BYTES.to_owned(),
            config.offsets_topic_segment_bytes.bytes_u64().to_string(),
        ),
    ])
}

/// Open every `__consumer_offsets` partition assigned to this broker, spawn
/// its writer task, and replay each local log that this broker leads into the
/// supplied `GroupCoordinator`.
///
/// The function does not create the topic. The first
/// `FindCoordinator(GROUP)` creates it, with its configured partition count
/// and replication factor
/// ([`crate::auto_topic_creation::AutoTopicCreation`]), as Kafka's
/// `KafkaApis.getCoordinator` does. On a restart the topic is in the image
/// already, and this function reloads the group state before the listener
/// binds.
///
/// `Broker::start` calls this exactly once, BEFORE the TCP listener binds and
/// AFTER the controller has elected a leader. See `Broker::start`.
pub async fn bootstrap(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
    coordinator: &Arc<GroupCoordinator>,
    log_dir_status: &LogDirRegistry,
    producer_state: &Arc<ProducerState>,
) -> Result<(), BrokerError> {
    // KIP-113 offline-dir handling: exclude dirs flagged offline by the
    // startup probe; placing `__consumer_offsets-N` on a known-bad dir
    // would fail immediately at `Log::open` below and leave the broker
    // unable to bootstrap the group coordinator.
    let placement_dirs = log_dir_status.online_subset(&config.all_log_dirs());
    if placement_dirs.is_empty() {
        return Err(BrokerError::Io(std::io::Error::other(
            "every configured log.dir failed the startup writability probe; \
             cannot bootstrap the group-coordinator partition",
        )));
    }

    let local_records: Vec<PartitionRecord> = controller
        .current_image()
        .partitions_of(OFFSETS_TOPIC)
        .filter(|record| record.replicas.contains(&config.node_id))
        .cloned()
        .collect();
    let mut replayed = Replayed::default();
    for record in local_records {
        let partition_id = PartitionIndex(record.partition);
        if let Some(partition) = partitions.get(OFFSETS_TOPIC, partition_id) {
            if record.leader == config.node_id {
                let log = partition.log.lock().map_err(|_| {
                    BrokerError::Startup(format!(
                        "{OFFSETS_TOPIC}-{} log lock poisoned during replay",
                        record.partition
                    ))
                })?;
                replayed.merge(replay_records(&log, coordinator)?);
            }
            continue;
        }

        let topic_dir = log_dir::place_partition_dir_avoiding(
            &placement_dirs,
            partitions.cordoned_log_dirs(),
            OFFSETS_TOPIC,
            record.partition,
        );
        std::fs::create_dir_all(&topic_dir)?;
        let log = krabka_log::Log::open(&topic_dir, config.log_config.clone())?;
        if record.leader == config.node_id {
            replayed.merge(replay_records(&log, coordinator)?);
        }
        let owning_dir = topic_dir
            .parent()
            .expect("placed partition dir always has a parent log.dir")
            .to_path_buf();
        let partition = spawn_partition(
            OFFSETS_TOPIC.to_string(),
            partition_id,
            owning_dir,
            log,
            log_dir_status.clone(),
            producer_state.clone(),
            false,
        );
        partitions.insert(OFFSETS_TOPIC.into(), partition_id, partition);
    }
    finalize(coordinator, replayed).await;
    Ok(())
}
