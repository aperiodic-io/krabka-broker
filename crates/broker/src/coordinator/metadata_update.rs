//! Metadata refresh of the groups that subscribe to a changed topic.
//!
//! Kafka's `GroupMetadataManager.onMetadataUpdate` runs on every metadata
//! image that the group coordinator applies. It collects the topics that the
//! image created, changed or deleted, and calls `requestMetadataRefresh` on
//! every group that subscribes to one of them (`groupsSubscribedToTopic`). The
//! next heartbeat of such a group computes its metadata hash again. When the
//! hash moved, that heartbeat bumps the group epoch and computes a new target
//! assignment. A member that subscribed to a topic before the topic existed
//! therefore gets its partitions at its next heartbeat. Kafka 4.3.1 has no
//! periodic refresh: `GroupMetadataManager.METADATA_REFRESH_INTERVAL_MS` is
//! `Integer.MAX_VALUE`.
//!
//! The image watcher of `coordinator::topic_deletion` reads every image,
//! finds the changed topics with [`changed_topics`], and sends them to the
//! groups with [`on_metadata_update`]. Each group actor decides whether it
//! subscribes to one of them.
//!
//! # Group kinds
//!
//! Kafka refreshes consumer, share and streams groups this way. A classic
//! group has nothing to refresh. Here the update goes to the consumer-group
//! actors. A share group computes its target assignment against the current
//! metadata on every heartbeat, and a streams group compares its metadata hash
//! on every heartbeat. Both therefore see a changed topic at their next
//! heartbeat without the update.
//!
//! # Only the coordinator refreshes
//!
//! Every broker watches the image, but a broker sends the update only to the
//! groups whose `__consumer_offsets` partition it leads. Kafka also applies an
//! image only to its active coordinator shards.

use {
    super::{GroupCoordinator, unified::actor::GroupActorMessage},
    krabka_metadata::MetadataImage,
    std::{collections::BTreeSet, sync::Arc},
};

#[cfg(test)]
mod tests;

/// The names of the topics that `next` created, changed or deleted against
/// `previous`, sorted: Kafka's `CoordinatorMetadataDelta.changedTopicIds` and
/// `deletedTopicIds`.
///
/// A topic changed when its topic record or one of its partition records
/// differs, as a `TopicRecord`, a `PartitionRecord` or a
/// `PartitionChangeRecord` in a Kafka delta puts the topic in
/// `TopicsDelta.changedTopics`. The watch channel can skip images, so the
/// comparison is between whole images, by topic id. A topic that was deleted
/// and created again with the same name between two images is a changed topic.
#[must_use]
pub fn changed_topics(previous: &MetadataImage, next: &MetadataImage) -> Vec<String> {
    let mut changed: BTreeSet<&str> = next
        .topics()
        .filter(|topic| {
            previous.topic_by_id(&topic.topic_id).is_none_or(|before| {
                before != *topic
                    || !previous
                        .partitions_of(&topic.name)
                        .eq(next.partitions_of(&topic.name))
            })
        })
        .map(|topic| topic.name.as_str())
        .collect();
    changed.extend(
        previous
            .topics()
            .filter(|topic| next.topic_by_id(&topic.topic_id).is_none())
            .map(|topic| topic.name.as_str()),
    );
    changed.into_iter().map(str::to_owned).collect()
}

/// Whether `next` can resolve a regular expression of a consumer group to
/// other topics than `previous` did.
///
/// Kafka's `GroupMetadataManager.onMetadataUpdate` remembers the version of
/// the latest image that created a topic (`lastMetadataImageWithNewTopics`),
/// and a group refreshes the resolutions older than it at its next heartbeat.
/// It does not refresh when a topic is deleted, because the assignment drops a
/// deleted topic through the metadata hash, and the next resolution cleans the
/// resolved regular expression up. A resolution also depends on the `Describe`
/// grants of the principal that made it, so a change of the ACLs counts too,
/// which Kafka only sees at its refresh interval.
///
/// The watch channel can skip images, so the comparison is between whole
/// images: a topic is new when its topic id is not in `previous`.
#[must_use]
pub fn regex_resolution_may_change(previous: &MetadataImage, next: &MetadataImage) -> bool {
    next.topics()
        .any(|topic| previous.topic_by_id(&topic.topic_id).is_none())
        || acl_fingerprint(previous) != acl_fingerprint(next)
}

/// The number of ACLs of an image, and a hash that does not depend on their
/// order, so that two images hold the same ACLs when both agree.
#[derive(PartialEq, Eq)]
struct AclFingerprint {
    count: usize,
    hash_sum: u64,
}

fn acl_fingerprint(image: &MetadataImage) -> AclFingerprint {
    use std::hash::{DefaultHasher, Hash, Hasher};

    image.all_acls().fold(
        AclFingerprint {
            count: 0,
            hash_sum: 0,
        },
        |fingerprint, acl| {
            let mut hasher = DefaultHasher::new();
            acl.resource_type.hash(&mut hasher);
            acl.resource_name.hash(&mut hasher);
            acl.pattern_type.hash(&mut hasher);
            acl.principal.hash(&mut hasher);
            acl.host.hash(&mut hasher);
            acl.operation.hash(&mut hasher);
            acl.permission_type.hash(&mut hasher);
            AclFingerprint {
                count: fingerprint.count + 1,
                hash_sum: fingerprint.hash_sum.wrapping_add(hasher.finish()),
            }
        },
    )
}

/// Sends `topics` to every group actor that `owned` accepts.
///
/// The update has no reply: a group only notes that its next heartbeat must
/// refresh the metadata. An actor handles its mailbox in order, so a heartbeat
/// that the actor receives after the update sees the request.
pub async fn on_metadata_update(
    coordinator: &GroupCoordinator,
    owned: impl Fn(&str) -> bool,
    topics: &[String],
) {
    let topics: Arc<[String]> = topics.into();
    let group_ids: Vec<String> = coordinator
        .groups
        .iter()
        .map(|entry| entry.key().clone())
        .collect();
    for group_id in group_ids {
        if !owned(&group_id) {
            continue;
        }
        let Some(handle) = coordinator.find(&group_id) else {
            continue;
        };
        // An actor that has stopped has no group left to refresh.
        let _ = handle
            .tx
            .send(GroupActorMessage::MetadataUpdate {
                topics: Arc::clone(&topics),
            })
            .await;
    }
}
