//! Serialized administrative mutations of an empty share group's offsets.

use std::collections::HashMap;

use krabka_log::Offset;
use krabka_protocol::primitives::uuid::Uuid;

use super::{
    PendingShareRecords, chrono_now_ms, flush_pending, share_state::TopicImage,
    state_partition_metadata_from,
};
use crate::{
    codes,
    coordinator::unified::{GroupCoordinator, share::state::ShareGroupState},
    error::BrokerError,
};

/// One partition of an `AlterShareGroupOffsets` request whose topic and
/// partition the metadata image holds.
#[derive(Debug)]
pub struct ResetPartition {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
    pub partition: i32,
    pub start_offset: i64,
}

/// What `DeleteShareGroupOffsets` did for one requested topic, in the order
/// of Kafka's response rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTopicOutcome {
    /// The persister deleted the share state of `partitions`.
    Deleted {
        topic_id: uuid::Uuid,
        partitions: Vec<i32>,
    },
    /// The metadata image holds no topic of this name.
    UnknownTopic,
    /// The group has no initialized partition of the topic and is not
    /// deleting it, Kafka's "There is no offset information to delete." row.
    NoState,
    /// The persister failed to delete a partition's state with `error_code`.
    Failed {
        topic_id: uuid::Uuid,
        error_code: i16,
    },
}

/// The partition error code of a failed persister call: the code the share
/// coordinator answered, or `COORDINATOR_NOT_AVAILABLE` when the call did not
/// reach one.
fn persister_error_code(error: &BrokerError) -> i16 {
    match error {
        BrokerError::SharePartitionState { code, .. } => *code,
        _ => codes::COORDINATOR_NOT_AVAILABLE,
    }
}

/// Writes the group's `ShareGroupStatePartitionMetadata` record.
async fn write_state_partition_metadata(
    state: &ShareGroupState,
    coordinator: &GroupCoordinator,
) -> Result<(), BrokerError> {
    let pending = PendingShareRecords {
        state_partition_metadata: Some(state_partition_metadata_from(state)),
        ..Default::default()
    };
    flush_pending(
        state,
        pending,
        &*coordinator.offsets_log,
        coordinator,
        chrono_now_ms(),
    )
    .await
}

/// The three sets of the share-state partition metadata, kept to undo an
/// in-memory change whose record did not reach the log, as Kafka's timeline
/// snapshot reverts a failed write.
struct MetadataSnapshot {
    initialized: std::collections::HashSet<(Uuid, i32)>,
    initializing: HashMap<(Uuid, i32), i64>,
    deleting: HashMap<Uuid, String>,
    topic_names: HashMap<Uuid, String>,
}

/// Check group emptiness before resolving the installed persister.
fn empty_group_persister<'a>(
    state: &ShareGroupState,
    coordinator: &'a GroupCoordinator,
) -> Result<&'a std::sync::Arc<crate::share_coordinator::persister_client::SharePersister>, i16> {
    if !state.members.is_empty() {
        return Err(codes::NON_EMPTY_GROUP);
    }
    coordinator
        .share_persister()
        .ok_or(codes::COORDINATOR_NOT_AVAILABLE)
}

impl MetadataSnapshot {
    fn take(state: &ShareGroupState) -> Self {
        Self {
            initialized: state.initialized.clone(),
            initializing: state.initializing.clone(),
            deleting: state.deleting.clone(),
            topic_names: state.topic_names.clone(),
        }
    }

    fn restore(self, state: &mut ShareGroupState) {
        state.initialized = self.initialized;
        state.initializing = self.initializing;
        state.deleting = self.deleting;
        state.topic_names = self.topic_names;
    }
}

/// Applies an `AlterShareGroupOffsets` batch to an empty share group, as
/// Kafka's `GroupMetadataManager.alterShareGroupOffsets` and
/// `GroupCoordinatorService.persisterInitialize` do.
///
/// The group epoch does not move and no `ShareGroupMetadata` record is
/// written. The requested partitions become initializing in one
/// `ShareGroupStatePartitionMetadata` record (`addInitializingTopicsRecords`),
/// the persister initializes each at the group's current epoch and the
/// requested start offset, and when every call succeeded a second record
/// moves them to initialized (`initializeShareGroupState`). When a call
/// failed, the partitions stay initializing and each failed partition
/// answers its error. When the second record fails, the partitions leave
/// the initializing set (`uninitializeShareGroupState`) and the response
/// still answers success, as Kafka's does.
///
/// # Errors
///
/// Returns `NON_EMPTY_GROUP` for a group with members, and
/// `COORDINATOR_NOT_AVAILABLE` without a persister or when the first record
/// does not reach the log.
pub(crate) async fn reset_offsets(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
    requests: Vec<ResetPartition>,
) -> Result<Vec<i16>, i16> {
    let persister = empty_group_persister(state, coordinator)?;
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let partitions: Vec<(Uuid, i32)> = requests
        .iter()
        .map(|request| (Uuid(*request.topic_id.as_bytes()), request.partition))
        .collect();
    let names: HashMap<Uuid, String> = requests
        .iter()
        .map(|request| {
            (
                Uuid(*request.topic_id.as_bytes()),
                request.topic_name.clone(),
            )
        })
        .collect();
    let before = MetadataSnapshot::take(state);
    state.add_initializing(&partitions, &names, chrono_now_ms());
    if write_state_partition_metadata(state, coordinator)
        .await
        .is_err()
    {
        before.restore(state);
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }

    let state_epoch = state.group_epoch;
    let mut results = Vec::with_capacity(requests.len());
    for request in &requests {
        let result = persister
            .initialize(
                &state.group_id,
                request.topic_id,
                request.partition,
                state_epoch,
                Offset(request.start_offset),
            )
            .await;
        results.push(match result {
            Ok(()) => codes::NONE,
            Err(error) => {
                tracing::warn!(
                    group_id = %state.group_id,
                    topic_id = %request.topic_id,
                    partition = request.partition,
                    %error,
                    "AlterShareGroupOffsets initialize failed",
                );
                persister_error_code(&error)
            }
        });
    }
    if results.iter().any(|code| *code != codes::NONE) {
        return Ok(results);
    }

    let initializing = MetadataSnapshot::take(state);
    for tp in &partitions {
        state.mark_initialized(*tp);
    }
    if let Err(error) = write_state_partition_metadata(state, coordinator).await {
        tracing::warn!(
            group_id = %state.group_id,
            %error,
            "persisting the initialized share partitions after AlterShareGroupOffsets failed",
        );
        initializing.restore(state);
        let initialized = MetadataSnapshot::take(state);
        state.uninitialize(&partitions);
        if write_state_partition_metadata(state, coordinator)
            .await
            .is_err()
        {
            initialized.restore(state);
        }
    }
    Ok(results)
}

/// Applies a `DeleteShareGroupOffsets` request to an empty share group, as
/// Kafka's `GroupCoordinatorService.deleteShareGroupOffsets` does in three
/// steps.
///
/// 1. `sharePartitionsEligibleForOffsetDeletion`: each topic with initialized
///    partitions moves to the deleting set, a topic that is already deleting
///    is deleted again, and the group writes its
///    `ShareGroupStatePartitionMetadata` record.
/// 2. The persister deletes the share state of those partitions.
/// 3. `completeDeleteShareGroupOffsets`: the topics whose state is gone leave
///    the deleting set in a second record.
///
/// A group with no share-state partition metadata answers no row at all, as
/// Kafka's does. Otherwise the outcomes come in Kafka's row order: the
/// deleted topics, then the refused topics in request order, then the topics
/// whose delete failed.
///
/// # Errors
///
/// Returns `NON_EMPTY_GROUP` for a group with members, and
/// `COORDINATOR_NOT_AVAILABLE` without a persister or when a record does not
/// reach the log.
pub(crate) async fn delete_offsets(
    state: &mut ShareGroupState,
    coordinator: &GroupCoordinator,
    topic_names: Vec<String>,
) -> Result<Vec<(String, DeleteTopicOutcome)>, i16> {
    let persister = empty_group_persister(state, coordinator)?;
    if !state.has_state_partition_metadata() {
        return Ok(Vec::new());
    }

    let image = TopicImage::current(coordinator);
    let before = MetadataSnapshot::take(state);
    let mut refused = Vec::new();
    let mut to_delete: Vec<(String, uuid::Uuid, Vec<i32>)> = Vec::new();
    for topic_name in topic_names {
        let Some((topic_id, partition_count)) = image.by_name(&topic_name) else {
            refused.push((topic_name, DeleteTopicOutcome::UnknownTopic));
            continue;
        };
        match state.mark_topic_deleting(Uuid(*topic_id.as_bytes()), &topic_name, partition_count) {
            Some(partitions) => to_delete.push((topic_name, topic_id, partitions)),
            None => refused.push((topic_name, DeleteTopicOutcome::NoState)),
        }
    }
    if write_state_partition_metadata(state, coordinator)
        .await
        .is_err()
    {
        before.restore(state);
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }

    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    for (topic_name, topic_id, partitions) in to_delete {
        let mut error_code = codes::NONE;
        for partition in &partitions {
            if let Err(error) = persister
                .delete(&state.group_id, topic_id, *partition)
                .await
            {
                tracing::warn!(
                    group_id = %state.group_id,
                    %topic_id,
                    partition,
                    %error,
                    "DeleteShareGroupOffsets state delete failed",
                );
                if error_code == codes::NONE {
                    error_code = persister_error_code(&error);
                }
            }
        }
        if error_code == codes::NONE {
            deleted.push((
                topic_name,
                DeleteTopicOutcome::Deleted {
                    topic_id,
                    partitions,
                },
            ));
        } else {
            failed.push((
                topic_name,
                DeleteTopicOutcome::Failed {
                    topic_id,
                    error_code,
                },
            ));
        }
    }

    if !deleted.is_empty() {
        let completed: Vec<Uuid> = deleted
            .iter()
            .filter_map(|(_, outcome)| match outcome {
                DeleteTopicOutcome::Deleted { topic_id, .. } => Some(Uuid(*topic_id.as_bytes())),
                _ => None,
            })
            .collect();
        let deleting = MetadataSnapshot::take(state);
        state.complete_deleting(&completed);
        if write_state_partition_metadata(state, coordinator)
            .await
            .is_err()
        {
            deleting.restore(state);
            return Err(codes::COORDINATOR_NOT_AVAILABLE);
        }
    }
    deleted.extend(refused);
    deleted.extend(failed);
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::check;
    use bytes::Bytes;

    use super::{DeleteTopicOutcome, ResetPartition, delete_offsets, reset_offsets};
    use crate::{
        codes,
        coordinator::unified::{
            offsets_log::fake::InMemoryOffsetsLog,
            share::{
                actor::{
                    records::PendingShareRecords,
                    test_support::{
                        TopicMetadataSetup, make_coordinator, metadata_with_topic,
                        unavailable_persister_coordinator,
                    },
                },
                persistence::{
                    DeletingTopic, ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
                },
                state::{ShareGroupState, ShareMemberState},
            },
            test_support::{fixed_source, make_share_persister},
        },
    };

    /// `(key, value)` of every record appended to `__consumer_offsets`.
    type Appended = Vec<(Option<Bytes>, Option<Bytes>)>;

    async fn appended(log: &InMemoryOffsetsLog) -> Appended {
        log.batches()
            .await
            .into_iter()
            .flat_map(|batch| batch.records)
            .map(|record| (record.key, record.value))
            .collect()
    }

    /// The records of one `ShareGroupStatePartitionMetadata` write of `g`.
    fn metadata_record(value: ShareGroupStatePartitionMetadataValue) -> Appended {
        PendingShareRecords {
            state_partition_metadata: Some(value),
            ..Default::default()
        }
        .into_batch("g", 0)
        .unwrap()
        .records
        .into_iter()
        .map(|record| (record.key, record.value))
        .collect()
    }

    fn partitions(topic_id: uuid::Uuid, partitions: Vec<i32>) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id,
            topic_name: "t".to_owned(),
            partitions,
        }
    }

    #[tokio::test]
    async fn missing_persister_fails_without_mutation() {
        let (metadata, _topic_id) = metadata_with_topic(TopicMetadataSetup::default());
        let (coordinator, _log) = make_coordinator(metadata);
        let mut state = ShareGroupState::new("g");

        check!(
            reset_offsets(&mut state, &coordinator, Vec::new()).await
                == Err(codes::COORDINATOR_NOT_AVAILABLE)
        );
    }

    #[tokio::test]
    async fn nonempty_gate_precedes_persister_access() {
        let (metadata, _topic_id) = metadata_with_topic(TopicMetadataSetup::default());
        let (coordinator, _log) = make_coordinator(metadata);
        let mut state = ShareGroupState::new("g");
        state.members.insert(
            "m".into(),
            ShareMemberState::joining("m", "client", "host", HashSet::default()),
        );

        check!(
            reset_offsets(&mut state, &coordinator, Vec::new()).await
                == Err(codes::NON_EMPTY_GROUP)
        );
        check!(
            delete_offsets(&mut state, &coordinator, Vec::new()).await
                == Err(codes::NON_EMPTY_GROUP)
        );
    }

    /// Kafka's `alterShareGroupOffsets` writes only the initializing record,
    /// at no new group epoch. When the persister fails, the partition answers
    /// the error and stays initializing: Kafka's `persisterInitialize`
    /// writes no further record for a partial failure. The persister here
    /// reaches no share coordinator, so every call fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alter_records_initializing_and_keeps_it_when_the_persister_fails() {
        let (coordinator, log, topic_id) = unavailable_persister_coordinator(TopicMetadataSetup {
            partitions: crate::test_support::PartitionCount(2),
            ..Default::default()
        });
        let topic_uuid = uuid::Uuid::from_bytes(topic_id.0);
        let mut state = ShareGroupState::new("g");
        state.group_epoch = 4;
        state.deleting.insert(topic_id, "t".to_owned());

        let results = reset_offsets(
            &mut state,
            &coordinator,
            vec![ResetPartition {
                topic_id: topic_uuid,
                topic_name: "t".to_owned(),
                partition: 1,
                start_offset: 42,
            }],
        )
        .await;

        check!(results == Ok(vec![codes::COORDINATOR_NOT_AVAILABLE]));
        check!(state.group_epoch == 4);
        check!(state.initializing.keys().copied().collect::<Vec<_>>() == vec![(topic_id, 1)]);
        // `addInitializingTopicsRecords` takes the topic out of the deleting
        // set too.
        check!(
            appended(&log).await
                == metadata_record(ShareGroupStatePartitionMetadataValue {
                    initializing: vec![partitions(topic_uuid, vec![1])],
                    ..Default::default()
                })
        );
    }

    /// A group with no share-state partition metadata answers no row and
    /// writes nothing, as Kafka's `sharePartitionsEligibleForOffsetDeletion`
    /// returns before it looks at the request.
    #[tokio::test]
    async fn delete_without_state_answers_no_row() {
        let (metadata, _topic_id) = metadata_with_topic(TopicMetadataSetup {
            partitions: crate::test_support::PartitionCount(2),
            ..Default::default()
        });
        let (coordinator, log) = make_coordinator(metadata);
        coordinator.set_share_persister(make_share_persister(fixed_source(
            krabka_metadata::MetadataImage::default(),
        )));
        let mut state = ShareGroupState::new("g");

        let outcomes = delete_offsets(
            &mut state,
            &coordinator,
            vec!["t".to_owned(), "missing".to_owned()],
        )
        .await;

        check!(outcomes == Ok(Vec::new()));
        check!(appended(&log).await == Appended::new());
    }

    /// Kafka's `deleteShareGroupOffsets`: an initialized topic moves to the
    /// deleting set in a record written before the persister runs, and a
    /// failed delete leaves it there. A retry deletes every partition of the
    /// deleting topic again. The rows come refused-before-failed, the refused
    /// ones in request order.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_delete_leaves_the_topic_deleting_for_a_retry() {
        let (coordinator, log, topic_id) =
            unavailable_persister_coordinator(TopicMetadataSetup::default());
        let topic_uuid = uuid::Uuid::from_bytes(topic_id.0);
        let other = krabka_protocol::primitives::uuid::Uuid([8; 16]);
        let mut state = ShareGroupState::new("g");
        state.initialized.insert((topic_id, 0));
        state.initializing.insert((other, 0), 1);
        state.topic_names.insert(topic_id, "t".to_owned());
        state.topic_names.insert(other, "other".to_owned());
        let request = || vec!["missing".to_owned(), "t".to_owned()];
        let expected_outcomes = vec![
            ("missing".to_owned(), DeleteTopicOutcome::UnknownTopic),
            (
                "t".to_owned(),
                DeleteTopicOutcome::Failed {
                    topic_id: topic_uuid,
                    error_code: codes::COORDINATOR_NOT_AVAILABLE,
                },
            ),
        ];
        let deleting = metadata_record(ShareGroupStatePartitionMetadataValue {
            initializing: vec![TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes([8; 16]),
                topic_name: "other".to_owned(),
                partitions: vec![0],
            }],
            initialized: Vec::new(),
            deleting: vec![DeletingTopic {
                topic_id: topic_uuid,
                topic_name: "t".to_owned(),
            }],
        });

        let first = delete_offsets(&mut state, &coordinator, request()).await;
        let retry = delete_offsets(&mut state, &coordinator, request()).await;

        check!(first == Ok(expected_outcomes.clone()));
        check!(retry == Ok(expected_outcomes));
        check!(appended(&log).await == [deleting.clone(), deleting].concat());
    }
}
