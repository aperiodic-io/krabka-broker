//! The background work of the share coordinator, as Kafka's
//! `ShareCoordinatorService` runs it.
//!
//! - **Topic deletion.** When the metadata image drops a topic, the
//!   coordinator writes a tombstone for every key of that topic id on every
//!   active state partition (`handleTopicsDeletion`, which schedules
//!   `ShareCoordinatorShard.maybeCleanupShareState` on every shard).
//! - **Cold-partition snapshot.** Every
//!   `share.coordinator.cold.partition.snapshot.interval.ms`, the coordinator
//!   writes a new `ShareSnapshot` for each key whose latest snapshot is at
//!   least that old (`snapshotColdPartitions`). An idle key then no longer
//!   holds the prune frontier of its state partition at an old offset.
//! - **Prune.** Every `share.coordinator.state.topic.prune.interval.ms`, the
//!   coordinator trims the redundant log prefix of each active state
//!   partition (`performRecordPruning`).
//!
//! Kafka runs the two timers only while share groups are enabled
//! (`isShareGroupsEnabled`: `share.version` 1 or more in the image).

use std::{collections::HashSet, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tokio::{sync::watch, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{LoadStatus, ShareCoordinator, Term, state_machine::StateRecord};

impl ShareCoordinator {
    /// The state partitions that this broker leads and has loaded.
    async fn active_partitions(&self) -> Vec<PartitionIndex> {
        let mut partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .filter(|(_, led)| led.status == LoadStatus::Active)
            .map(|(partition, _)| *partition)
            .collect();
        partitions.sort_unstable_by_key(|partition| partition.get());
        partitions
    }

    /// The keys that map to `state_partition` and that `filter` accepts.
    fn keys_of(
        &self,
        state_partition: PartitionIndex,
        filter: impl Fn(&uuid::Uuid) -> bool,
    ) -> Vec<(String, uuid::Uuid, i32)> {
        let mut keys: Vec<(String, uuid::Uuid, i32)> = self
            .state
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|(group, topic_id, partition)| {
                filter(topic_id)
                    && self.state_partition_for(group, topic_id, *partition) == state_partition
            })
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Waits until the records that `appended` counts on the partition of
    /// `term` are committed, and returns `appended`, or 0 when they do not
    /// commit. `last_written` is the log end offset after them.
    ///
    /// Kafka's runtime completes a job's write operation only when its
    /// records commit. A job runs over many keys under one read guard, so it
    /// waits once per partition, after the guard is released.
    async fn committed_count(
        &self,
        term: Term,
        last_written: Option<krabka_log::Offset>,
        appended: usize,
        job: &str,
    ) -> usize {
        let Some(last_written) = last_written.filter(|_| appended > 0) else {
            return 0;
        };
        match self.await_committed(term, last_written).await {
            Ok(()) => appended,
            Err(error) => {
                warn!(
                    partition = term.partition.get(),
                    %error,
                    job,
                    "share-state job records not committed"
                );
                0
            }
        }
    }

    /// Tombstones every key whose topic id is in `deleted`, on every active
    /// state partition, as Kafka's `maybeCleanupShareState` does. Returns the
    /// number of tombstones written and committed.
    pub(crate) async fn cleanup_deleted_topics(&self, deleted: &HashSet<uuid::Uuid>) -> usize {
        if deleted.is_empty() {
            return 0;
        }
        let mut written = 0;
        for state_partition in self.active_partitions().await {
            let Ok(active) = self.active(state_partition).await else {
                continue;
            };
            let mut appended = 0;
            for (group, topic_id, partition) in
                self.keys_of(state_partition, |topic_id| deleted.contains(topic_id))
            {
                let Some(entry) = self.entry(&group, topic_id, partition) else {
                    continue;
                };
                let _st = entry.lock().await;
                match self
                    .tombstone(active.term, &group, topic_id, partition)
                    .await
                {
                    Ok(()) => appended += 1,
                    Err(error) => warn!(
                        group,
                        %topic_id,
                        partition,
                        ?error,
                        "share state of a deleted topic not tombstoned"
                    ),
                }
            }
            let (term, last_written) = (active.term, self.last_written(active.term));
            drop(active);
            written += self
                .committed_count(term, last_written, appended, "deleted-topic cleanup")
                .await;
        }
        if written > 0 {
            info!(
                tombstones = written,
                "tombstoned the share state of deleted topics"
            );
        }
        written
    }

    /// Writes a new snapshot of each key whose latest snapshot is at least
    /// `cold_partition_snapshot_interval` old, on every active state
    /// partition, as Kafka's `snapshotColdPartitions` does. Returns the number
    /// of snapshots written and committed.
    ///
    /// A state partition whose every key already has a cold snapshot (its
    /// write timestamp differs from its create timestamp) is skipped.
    pub(crate) async fn snapshot_cold_partitions(&self) -> usize {
        let interval =
            crate::time_util::duration_millis(self.config.cold_partition_snapshot_interval);
        let mut written = 0;
        for state_partition in self.active_partitions().await {
            let Ok(active) = self.active(state_partition).await else {
                continue;
            };
            let keys = self.keys_of(state_partition, |_| true);
            let mut cells = Vec::with_capacity(keys.len());
            let mut all_cold = true;
            for key in keys {
                let Some(entry) = self.entry(&key.0, key.1, key.2) else {
                    continue;
                };
                {
                    let st = entry.lock().await;
                    all_cold &= st.create_timestamp != st.write_timestamp;
                }
                cells.push((key, entry));
            }
            if all_cold {
                continue;
            }
            let mut appended = 0;
            for ((group, topic_id, partition), entry) in cells {
                let mut st = entry.lock().await;
                let now = self.now_ms();
                if now.saturating_sub(st.write_timestamp) < interval {
                    continue;
                }
                let snapshot = st.to_snapshot(st.snapshot_epoch.wrapping_add(1), now);
                match self
                    .append_state_record(
                        active.term,
                        &mut st,
                        &group,
                        topic_id,
                        partition,
                        StateRecord::Snapshot(snapshot),
                    )
                    .await
                {
                    Ok(()) => appended += 1,
                    Err(error) => warn!(
                        group,
                        %topic_id,
                        partition,
                        ?error,
                        "cold share-state snapshot failed"
                    ),
                }
            }
            let (term, last_written) = (active.term, self.last_written(active.term));
            drop(active);
            written += self
                .committed_count(term, last_written, appended, "cold-partition snapshot")
                .await;
        }
        written
    }

    /// Trims the redundant prefix of every active state partition, as
    /// Kafka's `performRecordPruning` does.
    pub(crate) async fn prune_state_partitions(&self) {
        for state_partition in self.active_partitions().await {
            self.maybe_prune(state_partition).await;
        }
    }
}

/// Whether the timers run for `image`: a finalized `share.version` of 1 or
/// more.
pub(crate) fn periodic_jobs_enabled(image: &MetadataImage) -> bool {
    crate::features::share_groups_enabled(image)
}

/// The topic ids of `previous` that `next` does not hold.
fn deleted_topic_ids(previous: &MetadataImage, next: &MetadataImage) -> HashSet<uuid::Uuid> {
    crate::coordinator::topic_deletion::deleted_topics(previous, next)
        .into_iter()
        .map(|(_, topic_id)| topic_id)
        .collect()
}

/// Spawns the loop of [`run`].
///
/// The image that `images` holds now is the baseline: a topic that a later
/// image drops is a deleted topic.
pub(crate) fn spawn(
    coordinator: Arc<ShareCoordinator>,
    mut images: watch::Receiver<Arc<MetadataImage>>,
    shutdown: CancellationToken,
) {
    let baseline = images.borrow_and_update().clone();
    tokio::spawn(run(coordinator, (baseline, images), shutdown));
}

/// Watches `images`, a baseline image and the channel of the later ones, and
/// drives the background work until `shutdown` is cancelled or the channel
/// closes.
///
/// Each new image tombstones the keys of the topics it deleted. The two
/// timers start when share groups become enabled, first fire one interval
/// later, and fire again one interval after each run ends, as Kafka's
/// `TimerTask`s do. They stop when share groups become disabled.
async fn run(
    coordinator: Arc<ShareCoordinator>,
    images: (Arc<MetadataImage>, watch::Receiver<Arc<MetadataImage>>),
    shutdown: CancellationToken,
) {
    let (mut previous, mut images) = images;
    let prune_interval = coordinator.config.state_topic_prune_interval;
    let cold_interval = coordinator.config.cold_partition_snapshot_interval;
    let mut enabled = periodic_jobs_enabled(&previous);
    let schedule = |enabled: bool, interval| enabled.then(|| Instant::now() + interval);
    let mut next_prune = schedule(enabled, prune_interval);
    let mut next_cold = schedule(enabled, cold_interval);
    loop {
        tokio::select! {
            image = crate::metadata_source::next_image_until_shutdown(&mut images, &shutdown) => {
                    let Some(image) = image else { return; };
                let deleted = deleted_topic_ids(&previous, &image);
                coordinator.cleanup_deleted_topics(&deleted).await;
                let now_enabled = periodic_jobs_enabled(&image);
                if now_enabled != enabled {
                    enabled = now_enabled;
                    next_prune = schedule(enabled, prune_interval);
                    next_cold = schedule(enabled, cold_interval);
                }
                previous = image;
            }
            () = crate::time_util::sleep_until_opt(next_prune) => {
                coordinator.prune_state_partitions().await;
                next_prune = schedule(enabled, prune_interval);
            }
            () = crate::time_util::sleep_until_opt(next_cold) => {
                coordinator.snapshot_cold_partitions().await;
                next_cold = schedule(enabled, cold_interval);
            }
        }
    }
}

#[cfg(test)]
mod tests;
