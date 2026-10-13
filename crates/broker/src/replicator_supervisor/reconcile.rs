//! The reconcile pass the supervisor runs for one metadata image: it places
//! the WAL voters, prunes what left the image, materializes what joined it, and
//! retargets the follower replicator tasks.

use {
    super::{ReplicatorSupervisor, desired_local_set, desired_wal_placements, push_topic_configs},
    crate::{
        config_keys::resolve_diskless,
        wal::quorum::{prune_orphaned_shard_dirs, registry::ShardId, shard_dir},
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::MetadataImage,
    tracing::warn,
};

impl ReplicatorSupervisor {
    pub(crate) async fn reconcile(&self, image: &MetadataImage) {
        let wal_placements = desired_wal_placements(image, self.diskless_wal_local_replica_count);
        for (shard, placement) in &wal_placements {
            let shortfall = self
                .diskless_wal_local_replica_count
                .saturating_sub(placement.voters.len());
            if let Some(shortfall) = std::num::NonZeroUsize::new(shortfall) {
                warn!(
                    topic_id = %shard.topic_id,
                    partition = shard.partition.0,
                    available = placement.voters.len(),
                    required = self.diskless_wal_local_replica_count,
                    shortfall = shortfall.get(),
                    "diskless WAL placement lacks enough distinct-rack registered brokers"
                );
            }
        }
        self.wal_shards.replace_placements(&wal_placements);
        let desired_wal_followers = self.desired_wal_followers(image, &wal_placements);

        // A promoted WAL voter must have exclusive access to its follower log
        // before the canonical partition adopts that durable prefix.
        self.stop_obsolete_wal_followers(&desired_wal_followers)
            .await;
        let wal_dirs_to_keep = image
            .all_partitions()
            .filter(|partition| resolve_diskless(image.topic_config(&partition.topic)))
            .filter_map(|partition| {
                let topic_id = image.topic(&partition.topic)?.topic_id;
                let shard = ShardId {
                    topic_id,
                    partition: PartitionIndex(partition.partition),
                };
                wal_placements
                    .get(&shard)
                    .is_some_and(|placement| placement.voters.contains(&self.node_id))
                    .then_some((partition.topic.clone(), shard))
            })
            .flat_map(|(topic, shard)| {
                self.log_dirs.iter().map(move |log_dir| {
                    shard_dir(log_dir, &topic, Some(shard.topic_id), shard.partition)
                })
            })
            .collect();
        if let Err(error) = prune_orphaned_shard_dirs(&self.log_dirs, &wal_dirs_to_keep) {
            warn!(error = %error, "failed to prune orphaned WAL shard directories");
        }

        let local_set = desired_local_set(self.node_id, image);

        // A DeleteTopics handler tears down its local partition immediately
        // after the metadata commit. A reconcile that already captured the
        // preceding image can race that teardown and materialize the deleted
        // partition again. Re-prune from the authoritative new image before
        // materializing its desired set so that stale-image resurrection is
        // idempotently repaired on the next watch delivery.
        self.prune_deleted_topic_partitions(image);

        // A reassignment can remove this broker from a live partition's
        // replica set without deleting the topic. Stop and remove that local
        // runtime before materializing the new desired set. Partitions that
        // are absent from metadata are left alone here: startup recovery can
        // discover an on-disk partition before its topic record arrives, and
        // `prune_deleted_topic_partitions` handles known topic tombstones.
        self.prune_unassigned_partitions(&local_set, image);

        // 0. Materialize the on-disk partition for every assignment where
        //    self is in `replicas`, regardless of leader/follower role.
        //    Additionally: sync the partition's cached leader + epoch
        //    (idempotent), and for partitions where self is leader,
        //    install the ISR into ReplicaState for HW computation.
        self.reconcile_local_partitions(&local_set, image).await;

        // Start WAL followers only after reassignment pruning completes. A
        // broker that just stopped hosting the ordinary partition deletes its
        // old shard directory during pruning; starting first would race that
        // deletion against the new follower log.
        self.spawn_wal_followers(image, desired_wal_followers);

        // Push topic-config overrides onto every locally-hosted partition.
        // Pushes are idempotent — sending the same `LogConfig` is a cheap
        // noop write inside `Log::set_config`. The metadata-watch reconcile
        // loop fires on every image change, so AlterConfigs propagation is
        // bounded to one reconcile tick.
        push_topic_configs(
            &local_set,
            &self.partitions,
            image,
            &self.log_config,
            self.node_id,
            self.unstable_api_versions,
            &self.dedup,
        )
        .await;

        self.reconcile_fetchers(image);

        // 3. Refresh the txn coordinator's view of locally-led
        //    __transaction_state partitions. Cheap (Arc clone + lock).
        if let Some(coord) = &self.txn_coordinator {
            // The loads run in the background. The reconcile loop does not
            // wait for them.
            drop(coord.refresh_leader_partitions(image).await);
        }

        // 3b. Apply the leadership of __share_group_state partitions to the
        //     share coordinator (KIP-932). A newly led partition loads its
        //     log in a background task, and a lost one drops its state. The
        //     reconcile does not wait for the loads.
        if let Some(coord) = &self.share_coordinator {
            drop(coord.refresh_leader_partitions(image).await);
        }

        // 4. KIP-858: report any (topic, partition) whose owning log-dir UUID
        //    has changed since the last successful report (first materialization
        //    or after a KIP-113 dir swap). Only sends if there is at least one
        //    change; on error the tracker is NOT updated so we retry next tick.
        //    The report submits a non-clobbering V1PartitionDirAssignment delta
        //    (merges one replica's `directories` slot), so it can no longer
        //    revert a concurrent reassignment.
        self.report_dir_assignments(&local_set, image).await;
    }
}
