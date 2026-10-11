//! The heartbeat-interval tick that expires members whose session timed out.
//! It is separate from the request path because it runs on a timer rather
//! than on a client request, and an eviction is the one membership change the
//! group makes on its own.

use std::time::Instant;

use super::{
    heartbeat::fence_member,
    records::{chrono_now_ms, flush_pending},
    share_state::cleanup_deleted_topics,
};
use crate::{
    coordinator::unified::{
        GroupCoordinator,
        actor::MetadataProvider,
        offsets_log::OffsetsLog,
        share::{config::ShareGroupConfig, state::ShareGroupState},
    },
    error::BrokerError,
};

/// Called on every heartbeat-interval tick. Each member whose session
/// expired is fenced on its own, with a batch of its own, as each of Kafka's
/// session timers runs `shareGroupFenceMember` for its member. Then the
/// topics that the metadata image dropped leave the group's share-state
/// partition metadata, so an empty group that no heartbeat reaches still
/// follows Kafka's `maybeCleanupShareGroupState`. Returns `Err` if a fence
/// write fails, and the actor must then exit.
pub(super) async fn handle_session_tick(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), BrokerError> {
    let expired = state.expired_members(Instant::now(), config.session_timeout);
    for member_id in expired {
        let Some(pending) = fence_member(state, metadata, &member_id) else {
            return Err(BrokerError::Share("group epoch is exhausted".to_owned()));
        };
        if let Err(e) =
            flush_pending(state, pending, offsets_log, coordinator, chrono_now_ms()).await
        {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "share-group actor exiting after tick log-write failure",
            );
            return Err(e);
        }
    }
    cleanup_deleted_topics(state, offsets_log, coordinator, chrono_now_ms()).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;

    use super::*;
    use crate::{
        coordinator::unified::{
            config::NextGenConfig,
            share::{
                actor::{
                    records::PendingShareRecords,
                    test_support::{
                        TopicMetadataSetup, make_coordinator_with_config, metadata_with_topic,
                    },
                },
                persistence::ShareGroupMetadataValue,
                state::ShareMemberState,
            },
            test_support::{check_two_members_expired, expired_session_last_seen},
        },
        test_support::PartitionCount,
    };

    /// Kafka runs a session timer per member, and each one that fires runs
    /// `shareGroupFenceMember` for its member alone: two members that expire
    /// together are fenced in two batches, each with the member's tombstones
    /// and an epoch bump, and no target is computed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn each_expired_member_is_fenced_in_a_batch_of_its_own() {
        let (metadata, _id) = metadata_with_topic(TopicMetadataSetup::four_partitions());
        let config = ShareGroupConfig {
            session_timeout: Duration::from_millis(1),
            ..ShareGroupConfig::default()
        };
        let (coord, log) = make_coordinator_with_config(
            metadata.clone(),
            NextGenConfig::default(),
            config.clone(),
        );
        let mut state = ShareGroupState::new("g");
        for member_id in ["m1", "m2"] {
            let mut m = ShareMemberState::joining(member_id, "c", "h", ["t".to_owned()].into());
            m.last_seen = expired_session_last_seen();
            state.add_or_update_member(m);
        }
        state.group_epoch = 2;
        state.target.epoch = 2;

        handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
            .await
            .expect("tick should succeed");

        // The first fence leaves the other member subscribed to `t`.
        let t_hash = metadata.snapshot().metadata_hash(["t"]);
        let fence = |member_id: &str, epoch, metadata_hash| {
            use crate::coordinator::unified::test_support::member_tombstones;
            PendingShareRecords {
                member_metadata: member_tombstones(member_id),
                target_per_member: member_tombstones(member_id),
                current_per_member: member_tombstones(member_id),
                group_metadata: Some(ShareGroupMetadataValue {
                    epoch,
                    metadata_hash,
                }),
                ..PendingShareRecords::default()
            }
            .into_batch("g", 0)
            .unwrap()
            .records
        };
        let written: Vec<_> = log.record_batches().await;
        let mut expected = vec![fence("m1", 3, t_hash), fence("m2", 4, 0)];
        if written.first().is_some_and(|batch| batch != &expected[0]) {
            // The members expire together: either may be fenced first.
            expected = vec![fence("m2", 3, t_hash), fence("m1", 4, 0)];
        }
        check!(written == expected);
        check_two_members_expired((state.members.len(), state.group_epoch, state.target.epoch));
    }

    /// Kafka's `maybeCleanupShareGroupState` reaches an empty group too: a
    /// tick takes a topic the image no longer holds out of every set of the
    /// group's share-state partition metadata and writes the record, and a
    /// tick with nothing to clean writes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tick_drops_the_share_state_of_a_deleted_topic() {
        use crate::coordinator::unified::share::persistence::{
            ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
        };

        let (metadata, topic_id) = metadata_with_topic(TopicMetadataSetup {
            partitions: PartitionCount(2),
            ..Default::default()
        });
        let config = ShareGroupConfig::default();
        let (coord, log) = make_coordinator_with_config(
            metadata.clone(),
            NextGenConfig::default(),
            config.clone(),
        );
        let gone = krabka_protocol::primitives::uuid::Uuid([9; 16]);
        let mut state = ShareGroupState::new("g");
        state.initialized.extend([(topic_id, 0), (gone, 0)]);
        state.initializing.insert((gone, 1), 1);
        state.deleting.insert(gone, "gone".to_owned());
        state.topic_names.insert(topic_id, "t".to_owned());
        state.topic_names.insert(gone, "gone".to_owned());

        for _ in 0..2 {
            handle_session_tick(&mut state, &config, &*metadata, &*log, &coord)
                .await
                .expect("tick should succeed");
        }

        let expected = PendingShareRecords {
            state_partition_metadata: Some(ShareGroupStatePartitionMetadataValue {
                initialized: vec![TopicPartitionsInfo {
                    topic_id: uuid::Uuid::from_bytes(topic_id.0),
                    topic_name: "t".to_owned(),
                    partitions: vec![0],
                }],
                ..ShareGroupStatePartitionMetadataValue::default()
            }),
            ..PendingShareRecords::default()
        }
        .into_batch("g", 0)
        .unwrap()
        .records;
        let written: Vec<_> = log.record_batches().await;
        check!(written == vec![expected]);
    }
}
