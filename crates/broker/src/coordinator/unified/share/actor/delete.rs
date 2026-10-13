//! `DeleteGroups` for a share group (KIP-932).
//!
//! Kafka's `GroupCoordinatorService.deleteGroups` runs the share group delete
//! first. A group with members answers `NON_EMPTY_GROUP`. For an empty group,
//! the group records every initialized and initializing topic as deleting,
//! and the persister deletes their share state
//! (`shareGroupBuildPartitionDeleteRequest`). A group whose state delete
//! failed is kept and answers the error. Only then does the general delete
//! write the group tombstones (`ShareGroup.createGroupTombstoneRecords`),
//! which tombstone the `ShareGroupStatePartitionMetadata` record too.

use {
    super::{
        records::{
            PendingShareRecords, chrono_now_ms, flush_pending, state_partition_metadata_from,
        },
        share_state::TopicImage,
    },
    crate::{
        codes,
        coordinator::{
            DeleteGroupError,
            unified::{
                GroupCoordinator, OffsetRecordBatchBuilder,
                offsets_log::OffsetsLog,
                share::{
                    persistence::{ShareGroupKey, encode_share_key},
                    state::{ShareGroupState, TopicPartitionList},
                },
            },
        },
        error::BrokerError,
    },
    krabka_protocol::records::RecordBatch,
};

/// Deletes the share group that `state` holds.
///
/// # Errors
///
/// Returns [`DeleteGroupError::NonEmpty`] when the group has members,
/// [`DeleteGroupError::ShareState`] when the delete of a share state fails
/// (the group is kept, without the partitions whose state is gone), and
/// [`DeleteGroupError::Internal`] when the tombstone append fails.
pub(super) async fn delete_group(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), DeleteGroupError> {
    if !state.members.is_empty() {
        return Err(DeleteGroupError::NonEmpty);
    }

    delete_share_state(state, offsets_log, coordinator).await?;

    let appended = match tombstone_batch(&state.group_id, chrono_now_ms()) {
        Ok(batch) => offsets_log.append(&state.group_id, batch).await,
        Err(error) => Err(error),
    };
    appended.map_err(|error| {
        tracing::warn!(
            group_id = %state.group_id,
            %error,
            "share group tombstone append failed",
        );
        DeleteGroupError::Internal
    })?;
    // The actor clears the seeds while it still owns the group, so a group
    // that a later heartbeat creates with the same id starts empty.
    coordinator.share_seeds.remove(&state.group_id);
    coordinator.share_seeds_cache.remove(&state.group_id);
    Ok(())
}

/// Kafka's `GroupMetadataManager.shareGroupBuildPartitionDeleteRequest`
/// followed by `GroupCoordinatorService.performShareGroupsDeletion`.
///
/// Every initialized and initializing partition, and every partition of a
/// topic that an earlier delete left deleting, moves to the deleting set, and
/// the group writes that `ShareGroupStatePartitionMetadata` record before the
/// persister deletes any state. When a delete fails, the group is kept with
/// the record as written, so a retry deletes every one of those topics again.
/// A group with no share-state partition metadata writes nothing here.
async fn delete_share_state(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), DeleteGroupError> {
    if !state.has_state_partition_metadata() {
        return Ok(());
    }
    let Some(persister) = coordinator.share_persister() else {
        return Err(DeleteGroupError::ShareState(
            codes::COORDINATOR_NOT_AVAILABLE,
        ));
    };

    let image = TopicImage::current(coordinator);
    let before = (
        state.initialized.clone(),
        state.initializing.clone(),
        state.deleting.clone(),
        state.topic_names.clone(),
    );
    let candidates = state.move_all_to_deleting(|topic_id| image.by_id(topic_id));
    let pending = PendingShareRecords {
        state_partition_metadata: Some(state_partition_metadata_from(state)),
        ..Default::default()
    };
    if let Err(error) =
        flush_pending(state, pending, offsets_log, coordinator, chrono_now_ms()).await
    {
        tracing::warn!(
            group_id = %state.group_id,
            %error,
            "persisting the deleting share partitions of a share group delete failed",
        );
        (
            state.initialized,
            state.initializing,
            state.deleting,
            state.topic_names,
        ) = before;
        return Err(DeleteGroupError::Internal);
    }

    let mut error_code = codes::NONE;
    for TopicPartitionList {
        topic_id,
        partitions,
    } in candidates
    {
        let topic_uuid = uuid::Uuid::from_bytes(topic_id.0);
        for partition in partitions {
            if let Err(error) = persister
                .delete(&state.group_id, topic_uuid, partition)
                .await
            {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %topic_uuid,
                    partition,
                    %error,
                    "share state delete failed; the share group is kept",
                );
                if error_code == codes::NONE {
                    error_code = match error {
                        BrokerError::SharePartitionState { code, .. } => code,
                        _ => codes::COORDINATOR_NOT_AVAILABLE,
                    };
                }
            }
        }
    }
    if error_code == codes::NONE {
        Ok(())
    } else {
        Err(DeleteGroupError::ShareState(error_code))
    }
}

/// The tombstones of an empty share group, in the order of Kafka's
/// `ShareGroup.createGroupTombstoneRecords`: the target assignment metadata,
/// the `ShareGroupStatePartitionMetadata`, and the group epoch. An empty
/// group has no member records left.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `group_id` is longer than 32767
/// bytes.
pub(super) fn tombstone_batch(group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
    let mut batch = OffsetRecordBatchBuilder::default();
    for key in [
        ShareGroupKey::TargetAssignmentMetadata {
            group_id: group_id.into(),
        },
        ShareGroupKey::StatePartitionMetadata {
            group_id: group_id.into(),
        },
        ShareGroupKey::GroupMetadata {
            group_id: group_id.into(),
        },
    ] {
        batch.push(encode_share_key(&key)?, None);
    }
    Ok(batch.finish(now_ms))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::{
                ShareGroupSeed,
                share::{
                    actor::ShareGroupActorMessage,
                    persistence::{
                        DeletingTopic, ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
                    },
                },
                test_support::{
                    AppendedRecords as Appended, appended_offsets_records as appended_records,
                    fixed_source, make_coord_with_log, make_share_persister, share_member,
                },
            },
            metrics::{BrokerMetrics, ShareGroupIdLabel},
        },
        assert2::check,
        std::collections::HashMap,
    };

    struct Row {
        /// The share group seed, `None` for no group.
        group_seed: Option<ShareGroupSeed>,
        /// Whether a share persister is wired.
        persister: bool,
        expected: Result<(), DeleteGroupError>,
        /// Whether the group is kept.
        kept: bool,
        /// The expected appended records.
        appended: Appended,
    }

    fn seed(members: usize, initialized_partitions: &[i32]) -> ShareGroupSeed {
        let initialized = if initialized_partitions.is_empty() {
            Vec::new()
        } else {
            vec![TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                topic_name: "orders".to_owned(),
                partitions: initialized_partitions.to_vec(),
            }]
        };
        ShareGroupSeed {
            group_epoch: 3,
            members: (0..members)
                .map(|index| (format!("member-{index}"), share_member("client")))
                .collect::<HashMap<_, _>>(),
            state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                initializing: Vec::new(),
                initialized,
                deleting: Vec::new(),
            },
            ..ShareGroupSeed::default()
        }
    }

    /// An empty group whose only partitions are still initializing.
    fn initializing_seed(partitions: &[i32]) -> ShareGroupSeed {
        let mut seed = seed(0, &[]);
        seed.state_partition_metadata.initializing = vec![TopicPartitionsInfo {
            topic_id: uuid::Uuid::from_bytes([9; 16]),
            topic_name: "orders".to_owned(),
            partitions: partitions.to_vec(),
        }];
        seed
    }

    /// An empty group whose only topic an earlier delete left deleting.
    fn deleting_seed() -> ShareGroupSeed {
        let mut seed = seed(0, &[]);
        seed.state_partition_metadata.deleting = vec![DeletingTopic {
            topic_id: uuid::Uuid::from_bytes([9; 16]),
            topic_name: "orders".to_owned(),
        }];
        seed
    }

    /// The `(key, value)` of a `ShareGroupStatePartitionMetadata` record of
    /// `sg` holding `value`.
    fn metadata_record(value: ShareGroupStatePartitionMetadataValue) -> Appended {
        PendingShareRecords {
            state_partition_metadata: Some(value),
            ..Default::default()
        }
        .into_batch("sg", 0)
        .unwrap()
        .records
        .into_iter()
        .map(|record| (record.key, record.value))
        .collect()
    }

    /// `DeleteGroups` on a share group, per row: whether the group exists,
    /// its members and initialized or initializing partitions, and whether a share persister
    /// is wired. The persister runs over a metadata image with no brokers, so
    /// it cannot reach a share coordinator and every state delete fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_share_group_answers_as_kafka() {
        let tombstones: Appended = tombstone_batch("sg", 0)
            .unwrap()
            .records
            .into_iter()
            .map(|record| (record.key, record.value))
            .collect();
        // Kafka's `shareGroupBuildPartitionDeleteRequest` records every
        // initialized topic as deleting before the persister runs.
        let orders_deleting = metadata_record(ShareGroupStatePartitionMetadataValue {
            deleting: vec![DeletingTopic {
                topic_id: uuid::Uuid::from_bytes([9; 16]),
                topic_name: "orders".to_owned(),
            }],
            ..Default::default()
        });
        let rows: [Row; 7] = [
            Row {
                group_seed: None,
                persister: true,
                expected: Err(DeleteGroupError::NotFound),
                kept: false,
                appended: Vec::new(),
            },
            Row {
                group_seed: Some(seed(1, &[])),
                persister: true,
                expected: Err(DeleteGroupError::NonEmpty),
                kept: true,
                appended: Vec::new(),
            },
            Row {
                group_seed: Some(seed(0, &[])),
                persister: false,
                expected: Ok(()),
                kept: false,
                appended: tombstones.clone(),
            },
            Row {
                group_seed: Some(seed(0, &[0, 1])),
                persister: true,
                expected: Err(DeleteGroupError::ShareState(
                    codes::COORDINATOR_NOT_AVAILABLE,
                )),
                kept: true,
                appended: orders_deleting,
            },
            // A deleting topic that the metadata image no longer holds is
            // dropped from the record, and with nothing left to delete the
            // group goes.
            Row {
                group_seed: Some(deleting_seed()),
                persister: true,
                expected: Ok(()),
                kept: false,
                appended: [
                    metadata_record(ShareGroupStatePartitionMetadataValue::default()),
                    tombstones.clone(),
                ]
                .concat(),
            },
            Row {
                group_seed: Some(seed(0, &[0])),
                persister: false,
                expected: Err(DeleteGroupError::ShareState(
                    codes::COORDINATOR_NOT_AVAILABLE,
                )),
                kept: true,
                appended: Vec::new(),
            },
            // Kafka's `shareGroupBuildPartitionDeleteRequest` deletes the
            // initializing partitions too, so a group that holds only those
            // still needs the persister.
            Row {
                group_seed: Some(initializing_seed(&[0])),
                persister: false,
                expected: Err(DeleteGroupError::ShareState(
                    codes::COORDINATOR_NOT_AVAILABLE,
                )),
                kept: true,
                appended: Vec::new(),
            },
        ];

        for (
            index,
            Row {
                group_seed,
                persister,
                expected,
                kept,
                appended,
            },
        ) in rows.into_iter().enumerate()
        {
            let (coordinator, log) = make_coord_with_log();
            if persister {
                coordinator.set_share_persister(make_share_persister(fixed_source(
                    krabka_metadata::MetadataImage::default(),
                )));
            }
            if let Some(group_seed) = group_seed {
                coordinator.mark_share("sg");
                let handle = coordinator.get_or_create_share("sg");
                handle
                    .tx
                    .send(ShareGroupActorMessage::Seed(group_seed))
                    .await
                    .expect("seed the share group");
            }

            let result = coordinator.delete_group("sg").await;

            check!(result == expected, "row {index}");
            check!(
                coordinator.share_group_ids().contains(&"sg".to_owned()) == kept,
                "row {index}"
            );
            check!(
                (coordinator.group_type("sg")
                    == Some(crate::coordinator::unified::group_coordinator::GroupType::Share))
                    == kept,
                "row {index}"
            );
            let written = appended_records(log.as_ref()).await;
            check!(written == appended, "row {index}");
        }
    }

    /// A share actor that stopped after a log-write failure leaves a closed
    /// handle and a cached seed. `DeleteGroups` respawns the actor from the
    /// seed and deletes the group, and the seeds are gone afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_respawns_a_stopped_share_actor() {
        let (coordinator, log) = make_coord_with_log();
        coordinator.mark_share("sg");
        coordinator.update_share_cache("sg", seed(0, &[]));
        let handle = coordinator.get_or_create_share("sg");
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(ShareGroupActorMessage::Shutdown(stopped_tx))
            .await
            .expect("stop the share actor");
        stopped_rx.await.expect("the share actor stopped");
        tokio::time::timeout(std::time::Duration::from_secs(5), handle.tx.closed())
            .await
            .expect("the stopped actor closes its mailbox");

        let result = coordinator.delete_group("sg").await;

        let written = appended_records(log.as_ref()).await;
        let tombstones: Appended = tombstone_batch("sg", 0)
            .unwrap()
            .records
            .into_iter()
            .map(|record| (record.key, record.value))
            .collect();
        check!(result == Ok(()));
        check!(written == tombstones);
        check!(!coordinator.share_group_ids().contains(&"sg".to_owned()));
        check!(coordinator.cached_share_seed("sg") == None);
        check!(coordinator.group_type("sg") == None);
    }

    /// A deleted share group takes its dead-letter counters with it. A delete
    /// that the group's members refuse does not, and another group's counters
    /// stay either way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_share_group_releases_its_dead_letter_counters() {
        let series = |metrics: &BrokerMetrics, group: &str| {
            let label = ShareGroupIdLabel {
                group_id: group.to_owned(),
            };
            [
                metrics.share_group_dlq_records.get(&label).is_some(),
                metrics
                    .share_group_dlq_produce_requests
                    .get(&label)
                    .is_some(),
                metrics
                    .share_group_dlq_failed_produce_requests
                    .get(&label)
                    .is_some(),
            ]
        };
        // (members, result, whether the group's series stay)
        let rows = [
            (1, Err(DeleteGroupError::NonEmpty), true),
            (0, Ok(()), false),
        ];

        for (members, expected, kept) in rows {
            let (coordinator, _log) = make_coord_with_log();
            let metrics = BrokerMetrics::new();
            coordinator.set_metrics(metrics.clone());
            for group in ["sg", "other"] {
                metrics.record_share_dlq_produce(group);
                metrics.record_share_dlq_records(group, 2);
                metrics.record_share_dlq_produce_failed(group);
            }
            coordinator.mark_share("sg");
            coordinator
                .get_or_create_share("sg")
                .tx
                .send(ShareGroupActorMessage::Seed(seed(members, &[])))
                .await
                .expect("seed the share group");

            let result = coordinator.delete_group("sg").await;

            check!(result == expected, "{members} members");
            check!(series(&metrics, "sg") == [kept; 3], "{members} members");
            check!(series(&metrics, "other") == [true; 3], "{members} members");
        }
    }
}
