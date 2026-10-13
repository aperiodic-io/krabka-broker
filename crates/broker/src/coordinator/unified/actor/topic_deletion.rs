//! The actor half of topic deletion: tombstone the offsets of deleted topics.
//!
//! `coordinator::topic_deletion` watches the metadata image and sends the names
//! of the deleted topics to every group this broker coordinates. Kafka does the
//! same in `GroupCoordinatorService.onMetadataUpdate`, which schedules
//! `OffsetMetadataManager.onTopicsDeleted` on every coordinator shard.

use {
    super::{chrono_now_ms, retention::append_tombstones},
    crate::coordinator::unified::{group::CoordinatorGroup, offsets_log::OffsetsLog},
    std::collections::BTreeSet,
    tokio::sync::oneshot,
};

#[cfg(test)]
mod tests;

/// The `DeleteTopicOffsets` mailbox arm: runs [`delete_topic_offsets`] and
/// replies with the tombstoned keys. Returns the actor's keep-running flag,
/// which is always `true`.
pub(super) async fn reply_delete_topic_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    topics: &[(String, uuid::Uuid)],
    reply: oneshot::Sender<Vec<(String, i32)>>,
) -> bool {
    let _ = reply.send(delete_topic_offsets(group, offsets_log, topics).await);
    true
}

/// Tombstones every committed offset and every open transactional offset of
/// the deleted topics in one batch, then removes them from the group.
///
/// `topics` holds each deleted topic's name and id. A committed offset goes
/// when its topic id is the deleted id or unknown, as in Kafka's
/// `OffsetMetadataManager.onTopicsDeleted`, so an offset committed to a topic
/// created again with the same name stays. An open transactional offset
/// carries no topic id, so it goes by name.
///
/// Returns the tombstoned keys, sorted. A group with no offset of those topics
/// writes nothing. A failed append changes nothing and returns no keys: Kafka
/// logs the failure of `onTopicsDeleted` and does not retry it either.
async fn delete_topic_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    topics: &[(String, uuid::Uuid)],
) -> Vec<(String, i32)> {
    let deleted_id = |name: &str| {
        topics
            .iter()
            .find(|(topic, _)| topic == name)
            .map(|(_, id)| *id)
    };
    let committed = group
        .committed_offsets
        .iter()
        .filter(|((topic, _), entry)| {
            deleted_id(topic).is_some_and(|id| entry.topic_id.is_none_or(|own| own == id))
        })
        .map(|(key, _)| key.clone());
    let pending = group
        .offsets()
        .pending_txn
        .into_iter()
        .filter(|(topic, _)| deleted_id(topic).is_some());
    let keys: Vec<(String, i32)> = committed
        .chain(pending)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if keys.is_empty() {
        return keys;
    }
    if let Err(error) =
        append_tombstones(offsets_log, &group.group_id, &keys, None, chrono_now_ms()).await
    {
        tracing::error!(
            group_id = %group.group_id,
            %error,
            "could not tombstone the offsets of deleted topics",
        );
        return Vec::new();
    }
    group.drop_offsets(&keys);
    keys
}
