//! Materialization of the partitions this broker hosts, and the per-reconcile
//! sync of each one's cached leader and leader epoch, plus the ISR where this
//! broker is the leader.

use std::{collections::HashSet, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::{
    ReplicatorSupervisor, TopicPartition,
    materialize::{MaterializePartitionConfig, materialize_partition_with_replication_target},
};

impl ReplicatorSupervisor {
    pub(super) async fn reconcile_local_partitions(
        &self,
        local_set: &HashSet<TopicPartition>,
        image: &MetadataImage,
    ) {
        for key in local_set {
            if let Err(e) = self.materialize_local_partition(image, &key.0, key.1) {
                warn!(
                    topic = %key.0, partition = key.1, error = %e,
                    "failed to materialize local partition"
                );
                continue;
            }
            let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
                continue;
            };
            let Some(part) = self.partitions.get(&key.0, PartitionIndex(key.1)) else {
                continue;
            };
            // Always sync the partition's cached leader + epoch.
            // `Partition::install_leader_change` is idempotent (atomic stores
            // no-op on equal writes).
            let topic_id = image.topic(&key.0).map(|topic| topic.topic_id);
            let promoting_diskless = part.diskless
                && part_record.leader == self.node_id
                && part
                    .current_leader
                    .load(std::sync::atomic::Ordering::Acquire)
                    != self.node_id.0;
            if promoting_diskless {
                let Some(topic_id) = topic_id else {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        "cannot prepare diskless promotion without topic identity"
                    );
                    continue;
                };
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(key.1),
                };
                let engine = self.wal_shards.get(shard);
                let result = part
                    .install_replication_target_after_log_prepare(
                        Some(topic_id),
                        part_record.leader.0,
                        part_record.leader_epoch.0,
                        |log| {
                            let durable = crate::wal::quorum::follower::hydrate_on_promotion(
                                &self.log_dirs,
                                &key.0,
                                shard,
                                self.node_id,
                                &self.log_config,
                                log,
                            )?;
                            if let (Some(durable), Some(engine)) = (durable, engine.as_ref()) {
                                engine.adopt_local_durable_prefix(
                                    durable,
                                    log.log_start_offset(),
                                    log.log_end_offset(),
                                );
                            }
                            Ok(log.recovered_producers())
                        },
                        |snapshot| {
                            self.producer_state.rebuild_from_snapshot(
                                &key.0,
                                PartitionIndex(key.1),
                                snapshot,
                            )
                        },
                    )
                    .await;
                if let Err(error) = result {
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        error = %error,
                        "failed to prepare diskless promotion"
                    );
                    continue;
                }
            } else if !part.diskless && part_record.leader == self.node_id {
                // Kafka's `Partition.makeLeader`: the new leader epoch is
                // recorded at the log end before the role is published. A
                // checkpoint write that fails takes the log directory offline,
                // as `LeaderEpochFileCache` does through `LogDirFailureChannel`.
                if let Err(error) = part
                    .install_local_leadership(
                        &self.producer_state,
                        topic_id,
                        part_record.leader.0,
                        part_record.leader_epoch.0,
                    )
                    .await
                {
                    crate::partition_writer::flag_storage_failure(
                        &error,
                        &part.log_dir,
                        &self.log_dir_status,
                    );
                    warn!(
                        topic = %key.0,
                        partition = key.1,
                        error = %error,
                        "failed to record the leader epoch start offset"
                    );
                    continue;
                }
            } else {
                let losing_leadership = part.diskless
                    && part_record.leader != self.node_id
                    && part
                        .current_leader
                        .load(std::sync::atomic::Ordering::Acquire)
                        == self.node_id.0;
                part.install_replication_target(
                    topic_id,
                    part_record.leader.0,
                    part_record.leader_epoch.0,
                )
                .await;
                if losing_leadership && let Some(topic_id) = topic_id {
                    self.hot_tail
                        .remove_partition(topic_id, PartitionIndex(key.1));
                }
            }
            if part_record.leader == self.node_id {
                // Install the *current* ISR from the metadata image (not the
                // full replica set) as ISR membership: using `replicas` would
                // undo any shrink applied via AlterPartition, so
                // isr_maintenance's shrink would never stick (and producers
                // with acks=-1 would stay blocked on lagging followers). The
                // replica set is passed separately so follower-progress
                // tracking survives across reconciles for replicas catching
                // up toward ISR re-admission.
                part.install_isr(&part_record.isr, &part_record.replicas, part_record.leader)
                    .await;
            }
        }
    }

    /// Open (or recover) the on-disk `Partition` for `(topic, partition)`
    /// and insert it into the broker's shared `partitions` map.
    /// Idempotent: a no-op if the partition is already present.
    pub(super) fn materialize_local_partition(
        &self,
        image: &MetadataImage,
        topic: &str,
        partition: i32,
    ) -> Result<(), String> {
        let diskless = crate::config_keys::resolve_diskless(image.topic_config(topic));
        let topic_id = image.topic(topic).map(|topic| topic.topic_id);
        let initial_target = if diskless {
            None
        } else {
            image
                .partition(topic, partition)
                .map(|record| crate::partition::ReplicationTarget {
                    topic_id,
                    leader_node_id: record.leader,
                    leader_epoch: record.leader_epoch,
                })
        };
        materialize_partition_with_replication_target(
            MaterializePartitionConfig {
                partitions: &self.partitions,
                topic,
                topic_id,
                partition,
                log_dirs: &self.log_dirs,
                log_config: &self.log_config,
                log_dir_status: &self.log_dir_status,
                producer_state: &self.producer_state,
                runtime: crate::partition::PartitionRuntimeConfig::new(
                    (
                        self.max_produce_group,
                        self.partition_writer_queue_depth,
                        self.diskless_wal_local_replica_count,
                    ),
                    diskless,
                    (
                        Some(self.hot_tail.clone()),
                        Some(self.wal_shards.clone()),
                        diskless.then(|| {
                            Arc::new(crate::wal::ControllerSequencer::new(
                                self.controller.clone(),
                            )) as Arc<dyn crate::wal::OffsetSequencer>
                        }),
                    ),
                ),
            },
            initial_target,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use assert2::assert;
    use krabka_metadata::MetadataRecord;
    use krabka_raft::NodeId;
    use uuid::Uuid;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        follower_promotion_images, image_with, partition_record, reconciled_partition,
        supervisor_fixture, three_replica_image, topic_record,
    };

    #[tokio::test]
    async fn reconcile_materializes_leader_partition_and_installs_isr() {
        let img = three_replica_image(NodeId(2), krabka_metadata::LeaderEpoch(7));
        let ((_supervisor, _partitions, _reporter, _dir), part) =
            reconciled_partition(&img, "local leader materialized").await;
        assert!(
            part.current_leader
                .load(std::sync::atomic::Ordering::Acquire)
                == 2,
            "leader cache updated"
        );
        assert!(
            part.current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire)
                == 7,
            "leader epoch cache updated"
        );
        let state = part.replica_state.lock().await;
        assert!(state.isr == [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect());
    }

    /// Kafka's `Partition.makeLeader` records the new leader epoch at the log
    /// end: here after two follower-era records at epoch 3, and again (a
    /// no-op) on a reconcile that changes nothing.
    #[tokio::test]
    async fn reconcile_records_a_promoted_leader_epoch_at_the_log_end() {
        let (as_follower, as_leader) = follower_promotion_images();
        let ((supervisor, _partitions, _reporter, _dir), part) =
            reconciled_partition(&as_follower, "local follower materialized").await;
        for _ in 0..2 {
            let mut batch = krabka_protocol::records::RecordBatch {
                partition_leader_epoch: 3,
                records: vec![krabka_protocol::records::Record::default()],
                ..Default::default()
            };
            part.log.lock().unwrap().append(&mut batch).unwrap();
        }
        let history = || part.epoch_history();
        assert!(
            history() == [(3, 0)],
            "a follower records only what it wrote"
        );

        for pass in ["promotion", "unchanged reconcile"] {
            supervisor.reconcile(&as_leader).await;
            assert!(history() == [(3, 0), (7, 2)], "{pass}");
            assert!(part.current_leader.load(Ordering::Acquire) == 2, "{pass}");
            assert!(
                part.current_leader_epoch.load(Ordering::Acquire) == 7,
                "{pass}"
            );
        }
    }

    // A disk that refuses every write to the leader-epoch checkpoint.
    krabka_macros::epoch_checkpoint_failure!(EpochCheckpointFull, PermissionDenied);

    /// Kafka's `LeaderEpochFileCache` reports a checkpoint write that fails to
    /// `LogDirFailureChannel`, so a promotion that cannot record its epoch
    /// takes the partition's log directory offline. The promotion is not
    /// published, and the heartbeat reports the directory to the controller.
    #[tokio::test]
    async fn a_promotion_that_cannot_record_its_epoch_takes_the_log_directory_offline() {
        let (as_follower, as_leader) = follower_promotion_images();
        let ((supervisor, _partitions, _reporter, dir), part) =
            reconciled_partition(&as_follower, "local follower materialized").await;
        part.log
            .lock()
            .unwrap()
            .test_set_io(Arc::new(EpochCheckpointFull));

        supervisor.reconcile(&as_leader).await;

        let offline: Vec<std::path::PathBuf> = supervisor
            .log_dir_status
            .offline()
            .into_iter()
            .map(|(log_dir, _reason)| log_dir)
            .collect();
        assert!(
            (offline, part.current_leader.load(Ordering::Acquire))
                == (vec![dir.path().to_path_buf()], 1)
        );
    }

    #[tokio::test]
    async fn reconcile_does_not_treat_reserved_diskless_offset_as_durable() {
        use std::collections::BTreeMap;

        use krabka_metadata::PartitionOffsetAdvanceRecord;

        let mut overrides = BTreeMap::new();
        overrides.insert("krabka.diskless".into(), "true".into());
        let img = image_with(&[
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "diskless",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "diskless",
                    leader: NodeId(2),
                    replicas: vec![NodeId(2)],
                    ..Default::default()
                },
            ),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: "diskless".into(),
                partition: 0,
                count: 7,
            }),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor.reconcile(&img).await;

        let partition = partitions
            .get("diskless", PartitionIndex(0))
            .expect("diskless leader materialized");
        assert!(partition.high_watermark().await == krabka_log::Offset(0));
    }

    #[tokio::test]
    async fn reconcile_materializes_follower_but_does_not_install_isr() {
        let img = three_replica_image(NodeId(1), krabka_metadata::LeaderEpoch(7));
        let ((_supervisor, _partitions, _reporter, _dir), part) =
            reconciled_partition(&img, "local follower materialized").await;
        let state = part.replica_state.lock().await;
        assert!(state.isr.is_empty());
    }

    #[tokio::test]
    async fn materialize_local_partition_inserts_partition() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "t", 0)
            .unwrap();

        assert!(partitions.contains("t", PartitionIndex(0)));
    }

    #[tokio::test]
    async fn non_diskless_materialization_installs_target_before_registry_visibility() {
        let topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(crate::test_support::single_partition_topic("t", topic_id)),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    leader: NodeId(2),
                    replicas: vec![NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(7),
                    ..Default::default()
                },
            ),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "t", 0)
            .expect("materialize");

        let partition = partitions
            .get("t", PartitionIndex(0))
            .expect("registry-visible partition");
        let expected = crate::partition::ReplicationTarget {
            topic_id: Some(topic_id),
            leader_node_id: NodeId(2),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
        };
        assert!(*partition.replication_target.read().await == expected);
        assert!(partition.current_leader.load(Ordering::Acquire) == 2);
        assert!(partition.current_leader_epoch.load(Ordering::Acquire) == 7);
        assert!(
            partition.replica_state.lock().await.current_leader_epoch == krabka_ids::LeaderEpoch(7)
        );
    }

    #[tokio::test]
    async fn diskless_materialization_keeps_leader_unpublished_until_hydration() {
        let topic_id = Uuid::new_v4();
        let img = image_with(&[
            MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
                "diskless", topic_id,
            )),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "diskless",
                    leader: NodeId(2),
                    replicas: vec![NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(7),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides: maplit::btreemap! {"krabka.diskless".into() => "true".into()},
            }),
        ]);
        let (supervisor, partitions, _reporter, _dir) = supervisor_fixture(img.clone());

        supervisor
            .materialize_local_partition(&img, "diskless", 0)
            .expect("materialize");

        let partition = partitions
            .get("diskless", PartitionIndex(0))
            .expect("registry-visible partition");
        assert!(partition.diskless);
        assert!(
            *partition.replication_target.read().await
                == crate::partition::ReplicationTarget {
                    topic_id: Some(topic_id),
                    leader_node_id: NodeId(0),
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                }
        );
        assert!(partition.current_leader.load(Ordering::Acquire) == 0);
        assert!(partition.current_leader_epoch.load(Ordering::Acquire) == 0);
    }
}
