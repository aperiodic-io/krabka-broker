//! Race-free materialization of a partition's on-disk log and writer task.
//! Both the supervisor reconcile pass and the first-touch handler path open a
//! partition through this one helper, so a key can never be opened twice.

use std::{path::PathBuf, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};

use crate::partition_registry::PartitionRegistry;

/// Open (or recover) the on-disk `Partition` for `(topic, partition)` and
/// insert it into `partitions` with
/// `PartitionRegistry::materialize_if_vacant`.
///
/// This is the canonical, race-free materialization helper. Both the
/// `ReplicatorSupervisor` reconcile loop and the `InitProducerId` handler
/// (first-touch path) call this function. `materialize_if_vacant` runs the
/// build closure under the per-key lock, so two concurrent callers for the
/// same key can never both spawn independent writer tasks.
///
/// Returns `Ok(())` if the partition is already present, which is a no-op, or
/// if the function opened it. Returns `Err(String)` on I/O failure.
pub(crate) struct MaterializePartitionConfig<'a> {
    pub partitions: &'a PartitionRegistry,
    pub topic: &'a str,
    pub topic_id: Option<uuid::Uuid>,
    pub partition: i32,
    pub log_dirs: &'a [PathBuf],
    pub log_config: &'a LogConfig,
    pub log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub runtime: crate::partition::PartitionRuntimeConfig,
}

pub(crate) fn materialize_partition(config: MaterializePartitionConfig<'_>) -> Result<(), String> {
    materialize_partition_with_replication_target(config, None)
}

pub(super) fn materialize_partition_with_replication_target(
    config: MaterializePartitionConfig<'_>,
    initial_target: Option<crate::partition::ReplicationTarget>,
) -> Result<(), String> {
    let MaterializePartitionConfig {
        partitions,
        topic,
        topic_id,
        partition,
        log_dirs,
        log_config,
        log_dir_status,
        producer_state,
        runtime,
    } = config;
    // `materialize_if_vacant` runs `build` under the per-key write lock —
    // only one thread can be inside it for a given key at a time,
    // eliminating the TOCTOU race that existed with the old
    // `contains_key` + `insert` pattern. JBOD placement (KIP-113) happens
    // under this lock too, so two concurrent materializations of the same
    // partition can never pick two different log dirs.
    partitions.materialize_if_vacant(topic, PartitionIndex(partition), || {
        // The name comes from the metadata log. Refuse a name that would put
        // the partition directory outside the log directory, whatever path
        // wrote it there.
        krabka_log::topic_name::validate_topic_name(topic).map_err(|e| e.to_string())?;
        let partition_index = PartitionIndex(partition);
        let preferred = topic_id.and_then(|id| partitions.preferred_log_dir(id, partition_index));
        if preferred
            .as_ref()
            .is_some_and(|dir| log_dir_status.is_offline(dir))
        {
            return Err("preferred log directory is offline".to_owned());
        }
        let dir = preferred.as_ref().map_or_else(
            || {
                crate::log_dir::place_partition_dir_avoiding(
                    log_dirs,
                    partitions.cordoned_log_dirs(),
                    topic,
                    partition,
                )
            },
            |log_dir| crate::log_dir::partition_dir(log_dir, topic, partition),
        );
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        let open_config = crate::diskless::recovery::open_config(log_config, runtime.diskless);
        let mut log = Log::open(&dir, open_config).map_err(|e| format!("Log::open: {e}"))?;
        if let Some(stamp_source) = partitions.stamp_source() {
            log.set_stamp_source(stamp_source)
                .map_err(|e| format!("set stamp source: {e}"))?;
        }
        if runtime.diskless
            && let (Some(topic_id), Some(registry)) = (topic_id, runtime.wal_shards.as_ref())
        {
            let shard = crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: PartitionIndex(partition),
            };
            if registry.local_is_leader(shard) {
                crate::wal::quorum::follower::hydrate_on_promotion(
                    log_dirs,
                    topic,
                    shard,
                    registry.local_node_id(),
                    log_config,
                    &mut log,
                )
                .map_err(|e| format!("hydrate promoted WAL follower: {e}"))?;
            }
        }
        // Kafka loads the producer state of a log when it opens the log. A
        // disk-backed partition that this broker already leads in the image
        // gets that target below, and a later promotion to the same target
        // copies nothing, so the tracker takes the log's state here.
        producer_state.install_snapshot_before_materialization(
            topic,
            PartitionIndex(partition),
            log.recovered_producers(),
        );
        let owning_dir = dir
            .parent()
            .expect("placed partition dir always has a parent log.dir")
            .to_path_buf();
        let spawn = crate::broker::PartitionSpawnConfig {
            topic: topic.to_string(),
            topic_id,
            partition_id: PartitionIndex(partition),
            log_dir: owning_dir,
            log,
            log_dir_status: log_dir_status.clone(),
            producer_state: producer_state.clone(),
            runtime,
        };
        let partition = match initial_target {
            Some(target) => {
                crate::broker::try_spawn_partition_with_replication_target(spawn, target)
            }
            None => crate::broker::try_spawn_partition_with_sequencer(spawn),
        }
        .map_err(|e| format!("spawn partition: {e}"))?;
        if preferred.is_some() {
            partitions.clear_preferred_log_dir(
                topic_id.expect("a preference is keyed by topic id"),
                partition_index,
            );
        }
        Ok(partition)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use assert2::assert;

    use super::*;
    use crate::replicator_supervisor::test_support::MaterializeFixture;

    #[derive(Debug)]
    struct TestStampSource(AtomicU64);

    impl krabka_log::StampSource for TestStampSource {
        fn next_stamp(&self) -> u64 {
            self.0.fetch_add(1, Ordering::Relaxed)
        }
    }

    fn materialize_test_partition(
        partitions: &Arc<PartitionRegistry>,
        log_dir: &std::path::Path,
        topic: &str,
    ) {
        MaterializeFixture::default().materialize(
            partitions,
            crate::replicator_supervisor::test_support::MaterializeSetup {
                topic,
                log_dirs: &[log_dir.to_path_buf()],
                context: "materialize partition",
                ..Default::default()
            },
        );
    }

    fn append_one(partition: &crate::partition::Partition) -> krabka_log::Offset {
        use krabka_protocol::records::{Record, RecordBatch};

        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        partition
            .log
            .lock()
            .expect("partition log")
            .append(&mut batch)
            .expect("append")
            .0
    }

    #[tokio::test]
    async fn materialize_partition_helper_supports_isr_install() {
        let (_dir, partitions) =
            crate::replicator_supervisor::test_support::materialized_partition();
        let part = partitions.get("t", PartitionIndex(0)).expect("part");
        // Mirror what reconcile does for leader partitions.
        crate::partition::test_support::install_three_replica_isr(&part).await;
        let st = part.replica_state.lock().await;
        assert!(st.isr.len() == 3);
    }

    /// A topic name reaches this function from the metadata log. A name that
    /// would put the partition directory outside the log directory is
    /// refused, and no directory appears anywhere.
    #[tokio::test]
    async fn materialization_refuses_a_topic_name_that_escapes_the_log_dir() {
        let root = tempfile::tempdir().expect("root");
        let log_dir = root.path().join("logs");
        std::fs::create_dir_all(&log_dir).expect("log dir");
        let escaping = "../outside".to_owned();
        let absolute = format!("{}/abs", root.path().display());
        for topic in [escaping.as_str(), absolute.as_str(), "..", ""] {
            let partitions = Arc::new(PartitionRegistry::new());
            let result = materialize_partition(MaterializeFixture::default().config(
                &partitions,
                crate::replicator_supervisor::test_support::MaterializeSetup {
                    topic,
                    log_dirs: std::slice::from_ref(&log_dir),
                    ..Default::default()
                },
            ));
            assert!(
                result
                    == Err(krabka_log::topic_name::validate_topic_name(topic)
                        .expect_err("invalid name")
                        .to_string()),
                "{topic:?}"
            );
            assert!(partitions.get(topic, PartitionIndex(0)).is_none());
        }
        let mut entries: Vec<_> = std::fs::read_dir(root.path())
            .expect("read root")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        entries.sort();
        assert!(entries == vec![std::ffi::OsString::from("logs")]);
        assert!(
            std::fs::read_dir(&log_dir)
                .expect("read log dir")
                .next()
                .is_none()
        );
    }

    #[tokio::test]
    async fn materialization_consumes_precreation_log_dir_preference() {
        let first = tempfile::tempdir().expect("first log dir");
        let preferred = tempfile::tempdir().expect("preferred log dir");
        let partitions = Arc::new(PartitionRegistry::new());
        let topic_id = uuid::Uuid::from_u128(1);
        partitions.set_preferred_log_dir(
            topic_id,
            PartitionIndex(0),
            preferred.path().to_path_buf(),
        );

        materialize_partition(MaterializePartitionConfig {
            topic_id: Some(topic_id),
            ..MaterializeFixture::default().config(
                &partitions,
                crate::replicator_supervisor::test_support::MaterializeSetup {
                    log_dirs: &[first.path().to_path_buf(), preferred.path().to_path_buf()],
                    ..Default::default()
                },
            )
        })
        .expect("materialize");

        let partition = partitions.get("t", PartitionIndex(0)).expect("partition");
        assert!(partition.log_dir.load_full().as_path() == preferred.path());
        assert!(
            partitions
                .preferred_log_dir(topic_id, PartitionIndex(0))
                .is_none()
        );
    }

    #[tokio::test]
    async fn materialized_partition_stamps_only_when_source_is_configured() {
        let disabled_dir = tempfile::tempdir().expect("disabled tempdir");
        let disabled = Arc::new(PartitionRegistry::new());
        materialize_test_partition(&disabled, disabled_dir.path(), "disabled");
        let disabled_part = disabled
            .get("disabled", PartitionIndex(0))
            .expect("disabled partition");
        let disabled_offset = append_one(&disabled_part);
        assert!(disabled_offset == krabka_log::Offset(0));
        assert!(
            disabled_part.stamp_for_offset(disabled_offset).is_none(),
            "Kafka-only partitions must not create internal stamps"
        );

        let enabled_dir = tempfile::tempdir().expect("enabled tempdir");
        let source: Arc<dyn krabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(100)));
        let enabled = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&enabled, enabled_dir.path(), "enabled");
        let enabled_part = enabled
            .get("enabled", PartitionIndex(0))
            .expect("enabled partition");
        let enabled_offset = append_one(&enabled_part);
        assert!(enabled_offset == krabka_log::Offset(0));
        assert!(enabled_part.stamp_for_offset(enabled_offset) == Some(100));
    }

    #[tokio::test]
    async fn recovered_partition_installs_source_before_new_appends() {
        use krabka_protocol::records::{Record, RecordBatch};

        let dir = tempfile::tempdir().expect("tempdir");
        let partition_dir = crate::log_dir::partition_dir(dir.path(), "recovered", 0);
        std::fs::create_dir_all(&partition_dir).expect("partition dir");
        let mut existing = Log::open(&partition_dir, LogConfig::default()).expect("open existing");
        existing
            .append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .expect("append existing");
        drop(existing);

        let source: Arc<dyn krabka_log::StampSource> =
            Arc::new(TestStampSource(AtomicU64::new(500)));
        let partitions = Arc::new(PartitionRegistry::with_stamp_source(Some(source)));
        materialize_test_partition(&partitions, dir.path(), "recovered");
        let partition = partitions
            .get("recovered", PartitionIndex(0))
            .expect("recovered partition");

        let new_offset = append_one(&partition);
        assert!(new_offset == krabka_log::Offset(1));
        assert!(partition.stamp_for_offset(krabka_log::Offset(0)).is_none());
        assert!(partition.stamp_for_offset(new_offset) == Some(500));
    }

    #[tokio::test]
    async fn materialize_diskless_partition_registers_wal_shard() {
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let topic_id = uuid::Uuid::from_u128(0xD15C);
        let hot_tail = Arc::new(crate::diskless::hot_tail::HotTailCache::default());
        let wal_shards = Arc::new(crate::wal::quorum::registry::WalShardRegistry::new(
            krabka_raft::NodeId(0),
        ));

        materialize_partition(MaterializePartitionConfig {
            topic_id: Some(topic_id),
            runtime: crate::partition::PartitionRuntimeConfig::new(
                (1_024, 64, 3),
                true,
                (Some(hot_tail), Some(wal_shards.clone()), None),
            ),
            ..MaterializeFixture::default().config(
                &partitions,
                crate::replicator_supervisor::test_support::MaterializeSetup {
                    topic: "diskless",
                    log_dirs: &[dir.path().to_path_buf()],
                    ..Default::default()
                },
            )
        })
        .expect("materialize");

        assert!(
            wal_shards
                .get(crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(0),
                })
                .is_some()
        );
    }
}
