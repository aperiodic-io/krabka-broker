//! Committed offsets of deleted topics.
//!
//! When the metadata image removes a topic, Kafka writes an `OffsetCommit`
//! tombstone for each offset of that topic in every group
//! (`GroupCoordinatorService.onMetadataUpdate`, which schedules
//! `OffsetMetadataManager.onTopicsDeleted` on every coordinator shard).
//! Without the tombstones a topic created again with the same name gets the
//! old offsets from `OffsetFetch`, and a consumer skips records or resets.
//!
//! This module watches the image and finds the deleted topics. Each group
//! actor decides which of its offsets to tombstone and appends the batch in
//! its own turn, so a concurrent commit cannot race the tombstone.
//!
//! The same watcher gives every created, changed and deleted topic to
//! `coordinator::metadata_update`, so that the groups that subscribe to one of
//! them refresh their metadata. `GroupCoordinatorService.onMetadataUpdate`
//! does both too, the refresh first. It also tells the coordinator when an
//! image can change what a regular expression of a consumer group resolves
//! to, with `metadata_update::regex_resolution_may_change`, so that the
//! groups resolve their patterns again at their next heartbeat.
//!
//! # Only the coordinator writes
//!
//! Every broker watches the image, but a broker tombstones only the groups
//! whose `__consumer_offsets` partition it leads, the same ownership rule as
//! the offset-retention sweep.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, NodeId};
use tokio_util::sync::CancellationToken;

use super::{
    GroupCoordinator,
    metadata_update::{changed_topics, on_metadata_update, regex_resolution_may_change},
    partitioner::local_partition_for_group,
    unified::actor::GroupActorMessage,
};
use crate::metadata_source::MetadataSource;

#[cfg(test)]
mod tests;

/// Spawn the watcher. It returns when `shutdown` is cancelled or the image
/// channel closes.
pub(crate) fn spawn(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    coordinator: Arc<GroupCoordinator>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut images = metadata.watch_image();
        let mut previous = images.borrow_and_update().clone();
        // Kafka's `onMetadataUpdate` starts `lastMetadataImageWithNewTopics` at
        // the version of the first image it reads, so a group that a failover
        // loaded refreshes the resolutions of its regular expressions once,
        // at its first heartbeat.
        coordinator.bump_regex_refresh_version(metadata.current_metadata_offset());
        while let Some(image) =
            crate::metadata_source::next_image_until_shutdown(&mut images, &shutdown).await
        {
            let owned =
                |group_id: &str| local_partition_for_group(&image, node_id, group_id).is_ok();
            let changed = changed_topics(&previous, &image);
            if !changed.is_empty() {
                on_metadata_update(&coordinator, owned, &changed).await;
            }
            if regex_resolution_may_change(&previous, &image) {
                coordinator.bump_regex_refresh_version(metadata.current_metadata_offset());
            }
            let deleted = deleted_topics(&previous, &image);
            if !deleted.is_empty() {
                remember_deletions(&coordinator, &deleted);
                on_topics_deleted(&coordinator, owned, &deleted).await;
            }
            previous = image;
        }
    });
}

/// The name and topic id of each topic of `previous` whose topic id `next`
/// does not hold, sorted.
///
/// The watch channel can skip images, so the comparison uses topic ids: a
/// topic that was deleted and created again with the same name between two
/// images the watcher reads is still a deleted topic. The id travels with the
/// name, so the actors keep the offsets committed to the new topic.
pub(crate) fn deleted_topics(
    previous: &MetadataImage,
    next: &MetadataImage,
) -> Vec<(String, uuid::Uuid)> {
    let mut deleted: Vec<(String, uuid::Uuid)> = previous
        .topics()
        .filter(|topic| next.topic_by_id(&topic.topic_id).is_none())
        .map(|topic| (topic.name.clone(), topic.topic_id))
        .collect();
    deleted.sort_unstable();
    deleted
}

/// How many recent deletions the coordinator remembers for partitions that
/// are still loading.
const REMEMBERED_DELETIONS: usize = 1024;

/// Records `deleted` for [`after_partition_load`], keeping the latest
/// [`REMEMBERED_DELETIONS`].
fn remember_deletions(coordinator: &GroupCoordinator, deleted: &[(String, uuid::Uuid)]) {
    let mut recent = coordinator
        .recent_topic_deletions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    recent.extend(deleted.iter().cloned());
    while recent.len() > REMEMBERED_DELETIONS {
        recent.pop_front();
    }
}

/// Applies the remembered deletions to the groups of a `__consumer_offsets`
/// partition that has just finished loading.
///
/// One image can delete a topic and move the partition to this broker. The
/// watcher then finds no group to tombstone, because the load has not run
/// yet, and the previous leader no longer owns the partition. This pass
/// closes that window. It applies only the deletions whose topic name the
/// current image does not hold: a replayed offset has no topic id, so for a
/// name that exists again it cannot tell an offset of the old topic from one
/// of the new topic, and it keeps them.
pub(crate) async fn after_partition_load(
    coordinator: &GroupCoordinator,
    image: &MetadataImage,
    owned: impl Fn(&str) -> bool,
) -> Vec<(String, Vec<(String, i32)>)> {
    let deletions: Vec<(String, uuid::Uuid)> = coordinator
        .recent_topic_deletions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|(name, _)| image.topic(name).is_none())
        .cloned()
        .collect();
    if deletions.is_empty() {
        return Vec::new();
    }
    on_topics_deleted(coordinator, owned, &deletions).await
}

/// Sends `topics` to every group actor that `owned` accepts, one group at a
/// time, and returns the offsets that each group tombstoned.
///
/// The watcher awaits each reply before it reads the next image. A later
/// deletion of a topic with the same name therefore cannot reach an actor
/// before this one.
pub(crate) async fn on_topics_deleted(
    coordinator: &GroupCoordinator,
    owned: impl Fn(&str) -> bool,
    topics: &[(String, uuid::Uuid)],
) -> Vec<(String, Vec<(String, i32)>)> {
    let group_ids = coordinator.group_ids();
    let mut changed = Vec::new();
    for group_id in group_ids {
        if !owned(&group_id) {
            continue;
        }
        let Some(handle) = coordinator.find(&group_id) else {
            continue;
        };
        let Ok(deleted) =
            crate::task_util::ask(&handle.tx, |reply| GroupActorMessage::DeleteTopicOffsets {
                topics: topics.to_vec(),
                reply,
            })
            .await
        else {
            continue;
        };
        if !deleted.is_empty() {
            tracing::info!(
                group_id = %group_id,
                offsets = deleted.len(),
                "tombstoned the committed offsets of deleted topics",
            );
            changed.push((group_id, deleted));
        }
    }
    changed.sort_unstable();
    changed
}
