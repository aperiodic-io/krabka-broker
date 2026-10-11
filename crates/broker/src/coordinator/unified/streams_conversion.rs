//! The KIP-1071 cold upgrade and downgrade that flip a drained group between
//! the classic and the streams protocol in place.
//!
//! The two directions mirror each other — inspect the live actor for members,
//! tombstone the records of the protocol being left, and force the type lock —
//! so they are easiest to keep correct side by side.

use std::sync::Arc;

use super::{
    actor::GroupActorMessage,
    group_coordinator::{GroupCoordinator, GroupType},
    streams,
};
use crate::error::BrokerError;

impl GroupCoordinator {
    /// The `error_message` of the `GROUP_ID_NOT_FOUND` answer to a
    /// `StreamsGroupHeartbeat` for `group_id`, or `None` when the group is a
    /// streams group or the heartbeat may create one.
    ///
    /// Kafka creates a streams group only on a join (`getOrCreateStreamsGroup`),
    /// in place of nothing or of an empty classic group, which this method
    /// converts. Any other heartbeat needs a streams group
    /// (`getStreamsGroupOrThrow`, and `streamsGroup` for a leave). A group of
    /// another type is never a streams group (`castToStreamsGroup`).
    ///
    /// # Errors
    ///
    /// Returns an error when the classic group tombstone cannot be appended.
    pub(crate) async fn streams_group_lookup_error(
        self: &Arc<Self>,
        group_id: &str,
        member_epoch: i32,
        now_ms: i64,
    ) -> Result<Option<String>, BrokerError> {
        let joining = member_epoch == 0;
        let not_streams = format!("Group {group_id} is not a streams group.");
        match self.group_type(group_id) {
            Some(GroupType::Streams) => return Ok(None),
            Some(GroupType::Share | GroupType::NextGen) => return Ok(Some(not_streams)),
            Some(GroupType::Classic) if !joining => return Ok(Some(not_streams)),
            Some(GroupType::Classic) => {
                return Ok(
                    match self
                        .try_convert_classic_to_streams(group_id, now_ms)
                        .await?
                    {
                        streams::migration::ConvertOutcome::RejectLiveMembers => Some(not_streams),
                        _ => None,
                    },
                );
            }
            None => {}
        }
        // A consumer group lives in the `groups` registry without a type lock.
        // `ClassicInspect` answers only for a classic group, and an empty
        // classic group is free for a streams group.
        if let Some(handle) = self.find(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            // An actor that has stopped holds no group.
            let classic_members = if handle
                .tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
            {
                rx.await.ok().map(|view| view.members.len())
            } else {
                Some(0)
            };
            if classic_members != Some(0) {
                return Ok(Some(not_streams));
            }
        }
        Ok((!joining).then(|| {
            if member_epoch < 0 {
                format!("Group {group_id} not found.")
            } else {
                format!("Streams group {group_id} not found.")
            }
        }))
    }

    /// KIP-1071 cold upgrade: convert a drained classic `group_id` to a
    /// streams group in place.
    ///
    /// The method tombstones the classic k2 `GroupMetadata` and forces the
    /// type lock to `Streams`. The committed offsets survive untouched. The
    /// classic actor stays in the `groups` map, so `OffsetFetch` requests can
    /// still read back the committed offset state.
    ///
    /// The method returns `NotClassic` for a non-classic group, and the caller
    /// then serves it as normal. It returns `Converted` after a successful
    /// flip. It returns `RejectLiveMembers` when live classic members remain,
    /// because Kafka does not support an online streams migration.
    pub(crate) async fn try_convert_classic_to_streams(
        self: &Arc<Self>,
        group_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::ConvertOutcome, BrokerError> {
        use streams::migration::{ConvertOutcome, classic_group_metadata_tombstone_batch};

        if self.group_type(group_id) != Some(GroupType::Classic) {
            return Ok(ConvertOutcome::NotClassic);
        }

        // Inspect the live classic actor (if any) for remaining members.
        if let Some(handle) = self.find(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(GroupActorMessage::ClassicInspect { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(ConvertOutcome::RejectLiveMembers);
            }
        }

        // Drained classic group → convert. Tombstone the classic k2 GroupMetadata
        // to clear any persisted classic metadata (defensive + matching the
        // KIP-848 upgrade flip; a no-op on replay when none was persisted). Flip
        // the type lock to Streams; the classic actor (if any) stays in
        // `self.groups` so its committed_offsets remain accessible to
        // `OffsetFetch` without a full replay cycle.
        let batch = classic_group_metadata_tombstone_batch(group_id, now_ms)?;
        self.offsets_log.append(group_id, batch).await?;
        self.mark_streams_after_upgrade(group_id);
        Ok(ConvertOutcome::Converted)
    }

    /// KIP-1071 cold downgrade: Kafka 4.3.1's `classicGroupJoin` for a
    /// `JoinGroup` from `member_id` to the streams group `group_id`.
    ///
    /// The method returns `NotStreams` for a non-streams group, and the caller
    /// then serves the classic `JoinGroup` as normal. It returns
    /// `RejectLiveMembers` when the streams group still has members, and the
    /// join answers `INCONSISTENT_GROUP_PROTOCOL`, because Kafka sends only an
    /// empty streams group to `classicGroupJoinToClassicGroup`.
    ///
    /// There, `maybeDeleteEmptyStreamsGroup` deletes the drained group. A
    /// join with no member id then creates a classic group, and Kafka writes
    /// the streams tombstones and `newEmptyGroupMetadataRecord` in one batch,
    /// [`streams::migration::streams_to_classic_batch`]. After that append
    /// the method forces the type lock to `Classic`, drops the streams actor
    /// and returns `Converted`; the committed offsets, k0 and k1, and the
    /// offset-home `groups` entry survive. A join with a member id finds no
    /// group to create, answers `UNKNOWN_MEMBER_ID` and writes nothing, but
    /// the in-memory deletion stands, as Kafka's runtime keeps the state of a
    /// write that returns no records: the method drops the streams group and
    /// its type lock and returns `Removed`. The log still holds the group, so
    /// a replay brings it back, as in Kafka.
    ///
    /// It is the mirror of [`Self::try_convert_classic_to_streams`].
    ///
    /// # Errors
    ///
    /// Returns the append error, and the streams group stays as it was, as
    /// Kafka reverts its snapshot when the write fails.
    pub(crate) async fn try_convert_streams_to_classic(
        self: &Arc<Self>,
        group_id: &str,
        member_id: &str,
        now_ms: i64,
    ) -> Result<streams::migration::DowngradeOutcome, BrokerError> {
        use streams::{
            actor::StreamsGroupActorMessage,
            migration::{DowngradeOutcome, streams_to_classic_batch},
        };

        if self.group_type(group_id) != Some(GroupType::Streams) {
            return Ok(DowngradeOutcome::NotStreams);
        }

        // Kafka's `StreamsGroup.isEmpty`: a group with a member is not sent
        // down the classic path.
        if let Some(handle) = self.find_streams(group_id) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(StreamsGroupActorMessage::Describe { reply: tx })
                .await
                .is_ok()
                && let Ok(view) = rx.await
                && !view.members.is_empty()
            {
                return Ok(DowngradeOutcome::RejectLiveMembers);
            }
        }

        if !member_id.is_empty() {
            self.streams_seeds.remove(group_id);
            self.streams_seeds_cache.remove(group_id);
            self.group_types.remove(group_id);
            self.streams_groups.remove(group_id);
            return Ok(DowngradeOutcome::Removed);
        }

        let batch = streams_to_classic_batch(group_id, now_ms)?;
        self.offsets_log.append(group_id, batch).await?;
        self.mark_classic_after_streams_downgrade(group_id);
        self.streams_groups.remove(group_id);
        Ok(DowngradeOutcome::Converted)
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::{DeleteGroupError, unified::test_support::make_coord_with_log};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn conversion_paths_update_type_locks_and_report_missing_streams() {
        let (coord, offsets_log) = make_coord_with_log();
        assert!(
            coord
                .try_convert_classic_to_streams("fresh", 100)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::NotClassic
        );

        coord.mark_classic("g");
        check!(
            coord
                .try_convert_classic_to_streams("g", 101)
                .await
                .unwrap()
                == streams::migration::ConvertOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Streams));
        check!(offsets_log.appended.lock().await.len() == 1);

        check!(
            coord
                .try_convert_streams_to_classic("fresh", "", 102)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::NotStreams
        );
        check!(
            coord
                .try_convert_streams_to_classic("g", "", 103)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::Converted
        );
        check!(coord.group_type("g") == Some(GroupType::Classic));
        check!(
            offsets_log.appended.lock().await.get(1)
                == Some(&streams::migration::streams_to_classic_batch("g", 103).unwrap())
        );

        // Kafka's `classicGroupJoinToClassicGroup` deletes the drained group
        // in memory for a join with a member id, and writes nothing.
        coord.mark_streams("h");
        check!(
            coord
                .try_convert_streams_to_classic("h", "m1", 104)
                .await
                .unwrap()
                == streams::migration::DowngradeOutcome::Removed
        );
        check!(coord.group_type("h").is_none());
        check!(offsets_log.appended.lock().await.len() == 2);

        coord.mark_streams("missing-streams-actor");
        assert!(
            coord.delete_group("missing-streams-actor").await == Err(DeleteGroupError::NotFound)
        );
    }
}
