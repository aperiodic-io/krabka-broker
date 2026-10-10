//! The KIP-932 share-state lifecycle hook. It drives `Initialize` on the share
//! persister for the partitions the group gained, and it stands apart from the
//! membership state machine because it is best-effort work that runs after
//! reconciliation rather than inside it.
//!
//! The hook also takes a topic that the metadata image no longer holds out of
//! the group's `ShareGroupStatePartitionMetadata`, as Kafka's
//! `GroupMetadataManager.maybeCleanupShareGroupState` does for a deleted
//! topic. It does not call the persister for it: the share coordinator removes
//! the share state of a deleted topic on its own, as Kafka's
//! `ShareCoordinatorService.onTopicsDeleted` does. Kafka deletes share state
//! otherwise only for `DeleteShareGroupOffsets` and `DeleteGroups`
//! (`sharePartitionsEligibleForOffsetDeletion`,
//! `shareGroupBuildPartitionDeleteRequest`). A partition of a topic that no
//! member subscribes to any more keeps its share state and its share-partition
//! start offset, so consumers that subscribe again continue from it.

use std::collections::{HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::records::{PendingShareRecords, flush_pending, state_partition_metadata_from};
use crate::{
    coordinator::unified::{
        GroupCoordinator,
        offsets_log::OffsetsLog,
        reconciler::ReconcileInput,
        share::{
            config::ShareGroupConfig, persistence::ShareGroupStatePartitionMetadataValue,
            state::ShareGroupState,
        },
    },
    share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
};

/// The share partitions that a heartbeat initializes, and the group epoch
/// it initializes them at.
pub(super) struct Initialize {
    calls: Vec<InitializeCall>,
    state_epoch: i32,
}

/// Kafka's `GroupMetadataManager.maybeCreateInitializeShareGroupStateRequest`:
/// every partition of a subscribed topic that the group has not initialized
/// yet, and that is not initializing since less than
/// `initialize_retry_interval`, becomes initializing, as Kafka's
/// `addInitializingTopicsRecords` records it. It returns the group's
/// `ShareGroupStatePartitionMetadata` value, which the heartbeat writes last
/// in its own batch, and the persister calls that [`start_initialize`] makes
/// once that batch is written, or `None` when there is nothing to initialize.
///
/// Every partition is initialized at the group epoch and at start offset -1,
/// Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`, as Kafka's
/// `buildInitializeShareGroupStateRequest` asks; the share partition resolves
/// `share.auto.offset.reset` when it first loads.
pub(super) fn prepare_initialize(
    state: &mut ShareGroupState,
    config: &ShareGroupConfig,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> Option<(ShareGroupStatePartitionMetadataValue, Initialize)> {
    coordinator.share_persister()?;
    let input = coordinator.metadata.snapshot();
    let retry_ms = crate::time_util::duration_millis(config.initialize_retry_interval);
    let to_init = partitions_to_initialize(state, &input, now_ms, retry_ms);
    if to_init.is_empty() {
        return None;
    }
    let topic_names: HashMap<Uuid, String> = input
        .topic_id_by_name
        .iter()
        .map(|(name, topic_id)| (*topic_id, name.clone()))
        .collect();
    state.add_initializing(&to_init, &topic_names, now_ms);
    state.forget_unused_topic_names();
    let calls = to_init
        .iter()
        .map(|&(topic_id, partition)| InitializeCall {
            topic_id,
            partition,
            start_offset: UNINITIALIZED_START_OFFSET,
        })
        .collect();
    Some((
        state_partition_metadata_from(state),
        Initialize {
            calls,
            state_epoch: state.group_epoch,
        },
    ))
}

/// Starts the persister calls of `initialize` after the heartbeat's batch
/// recorded the partitions as initializing, in a task that reports back to
/// the group's actor: Kafka's `shareGroupHeartbeat` puts
/// `persisterInitialize` on a timer task, "async with respect to the
/// heartbeat". [`apply_initialized`] applies the outcome.
pub(super) fn start_initialize(
    state: &ShareGroupState,
    coordinator: &GroupCoordinator,
    initialize: Initialize,
) {
    let Some(persister) = coordinator.share_persister() else {
        return;
    };
    spawn_initialize(
        std::sync::Arc::clone(persister),
        std::sync::Arc::clone(&coordinator.share_groups),
        state.group_id.clone(),
        initialize.state_epoch,
        initialize.calls,
    );
}

/// Kafka's `GroupMetadataManager.maybeCleanupShareGroupState` for this
/// group: every topic of its `ShareGroupStatePartitionMetadata` that the
/// metadata image no longer holds leaves the initializing, initialized and
/// deleting sets, and the group writes the new record when a set changed.
///
/// Kafka runs it for every group when a metadata delta deletes topics. The
/// group runs it at each heartbeat and each session tick, against the topics
/// its record names.
pub(super) async fn cleanup_deleted_topics(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    let topic_ids = state.state_topic_ids();
    if topic_ids.is_empty() {
        return;
    }
    let deleted = deleted_topics(&topic_ids, &TopicImage::current(coordinator));
    if state.cleanup_deleted_topics(&deleted) {
        write_state_partition_metadata(state, offsets_log, coordinator, now_ms).await;
    }
}

/// The metadata image that the group checks its share-state partition
/// metadata against: the broker's current image, or the provider's snapshot
/// for a coordinator that runs without a metadata source.
pub(super) enum TopicImage {
    Image(std::sync::Arc<krabka_metadata::MetadataImage>),
    Snapshot(ReconcileInput),
}

impl TopicImage {
    pub(super) fn current(coordinator: &GroupCoordinator) -> Self {
        match coordinator.metadata_source() {
            Some(source) => Self::Image(source.current_image()),
            None => Self::Snapshot(coordinator.metadata.snapshot()),
        }
    }

    /// Whether the image holds any topic. Before the broker loads an image it
    /// holds none, and no topic reads as deleted.
    pub(super) fn is_loaded(&self) -> bool {
        match self {
            Self::Image(image) => image.topics().next().is_some(),
            Self::Snapshot(input) => !input.topic_id_by_name.is_empty(),
        }
    }

    /// The name and partition count of the topic with `topic_id`.
    pub(super) fn by_id(&self, topic_id: &Uuid) -> Option<(String, i32)> {
        match self {
            Self::Image(image) => image
                .topic_by_id(&uuid::Uuid::from_bytes(topic_id.0))
                .map(|topic| (topic.name.clone(), image.topic_partition_count(&topic.name))),
            Self::Snapshot(input) => input
                .topic_id_by_name
                .iter()
                .find(|(_, id)| *id == topic_id)
                .map(|(name, id)| {
                    (
                        name.clone(),
                        input.partitions_per_topic.get(id).copied().unwrap_or(0),
                    )
                }),
        }
    }

    /// The topic id and partition count of the topic named `topic_name`.
    pub(super) fn by_name(&self, topic_name: &str) -> Option<(uuid::Uuid, i32)> {
        match self {
            Self::Image(image) => image
                .topic(topic_name)
                .map(|topic| (topic.topic_id, image.topic_partition_count(topic_name))),
            Self::Snapshot(input) => input.topic_id_by_name.get(topic_name).map(|id| {
                (
                    uuid::Uuid::from_bytes(id.0),
                    input.partitions_per_topic.get(id).copied().unwrap_or(0),
                )
            }),
        }
    }
}

/// The topics of `topic_ids` that `image` does not hold. An image that is not
/// loaded yet deletes nothing.
fn deleted_topics(topic_ids: &HashSet<Uuid>, image: &TopicImage) -> HashSet<Uuid> {
    if !image.is_loaded() {
        return HashSet::new();
    }
    topic_ids
        .iter()
        .filter(|topic_id| image.by_id(topic_id).is_none())
        .copied()
        .collect()
}

/// One `Initialize` call of the persister: the share partition and the start
/// offset it is initialized at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InitializeCall {
    topic_id: Uuid,
    partition: i32,
    start_offset: i64,
}

/// Run `calls` against the persister together, at `state_epoch`, and send the
/// outcome of each to the actor of `group_id` that `actors` holds then.
///
/// The calls run concurrently, as Kafka's persister batches one group's
/// partitions into one request per share coordinator, so a slow coordinator
/// costs one wait and not one wait per partition. An actor that is gone by
/// then, because this broker unloaded the group, gets nothing. The actor that
/// loads the group next retries the partitions that are still initializing,
/// once `initialize_retry_interval` has passed.
fn spawn_initialize(
    persister: std::sync::Arc<crate::share_coordinator::persister_client::SharePersister>,
    actors: std::sync::Arc<dashmap::DashMap<String, std::sync::Arc<super::ShareGroupActorHandle>>>,
    group_id: String,
    state_epoch: i32,
    calls: Vec<InitializeCall>,
) {
    tokio::spawn(async move {
        let outcomes = futures_util::future::join_all(calls.into_iter().map(|call| {
            let (persister, group_id) = (&persister, &group_id);
            async move {
                let topic_uuid = uuid::Uuid::from_bytes(call.topic_id.0);
                let result = persister
                    .initialize(
                        group_id,
                        topic_uuid,
                        call.partition,
                        state_epoch,
                        krabka_log::Offset(call.start_offset),
                    )
                    .await;
                if let Err(e) = &result {
                    tracing::warn!(
                        %group_id,
                        topic_id = %topic_uuid,
                        partition = call.partition,
                        error = %e,
                        "share-state Initialize failed; will retry next heartbeat",
                    );
                }
                ((call.topic_id, call.partition), result.is_ok())
            }
        }))
        .await;
        let actor = actors
            .get(&group_id)
            .map(|entry| std::sync::Arc::clone(entry.value()));
        if let Some(actor) = actor {
            let _ = actor
                .tx
                .send(super::ShareGroupActorMessage::ShareStateInitialized(
                    outcomes,
                ))
                .await;
        }
    });
}

/// Apply the outcome of the `Initialize` calls that [`initialize`] started:
/// each `(partition, initialized)` pair moves the partition to the
/// initialized set, or takes it out of the initializing set so a later
/// heartbeat retries it. Then write the group's
/// `ShareGroupStatePartitionMetadata`, as Kafka's `initializeShareGroupState`
/// and `uninitializeShareGroupState` write it.
pub(super) async fn apply_initialized(
    state: &mut ShareGroupState,
    outcomes: &[((Uuid, i32), bool)],
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) {
    if outcomes.is_empty() {
        return;
    }
    for &(partition, initialized) in outcomes {
        if initialized {
            state.mark_initialized(partition);
        } else {
            state.uninitialize(&[partition]);
        }
    }
    write_state_partition_metadata(state, offsets_log, coordinator, now_ms).await;
}

/// Writes the group's `ShareGroupStatePartitionMetadata` record, and says
/// whether the write succeeded.
async fn write_state_partition_metadata(
    state: &mut ShareGroupState,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> bool {
    state.forget_unused_topic_names();
    let pending = PendingShareRecords {
        state_partition_metadata: Some(state_partition_metadata_from(state)),
        ..Default::default()
    };
    match flush_pending(state, pending, offsets_log, coordinator, now_ms).await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                group_id = %state.group_id,
                error = %e,
                "persisting ShareGroupStatePartitionMetadata failed; in-memory set retained",
            );
            false
        }
    }
}

/// Kafka's `GroupMetadataManager.subscribedTopicsChangeMap`: every partition
/// of a subscribed topic in the image that is neither initialized nor
/// initializing for less than `retry_ms`, sorted. A topic whose partitions
/// are all covered contributes nothing.
fn partitions_to_initialize(
    state: &ShareGroupState,
    input: &ReconcileInput,
    now_ms: i64,
    retry_ms: i64,
) -> Vec<(Uuid, i32)> {
    let subscribed: HashSet<&String> = state
        .members
        .values()
        .flat_map(|m| m.subscribed_topic_names.iter())
        .collect();
    let covered = |tp: &(Uuid, i32)| {
        state.initialized.contains(tp)
            || state
                .initializing
                .get(tp)
                .is_some_and(|&at| now_ms.saturating_sub(at) < retry_ms)
    };
    let mut out: Vec<(Uuid, i32)> = subscribed
        .into_iter()
        .filter_map(|name| input.topic_id_by_name.get(name))
        .flat_map(|topic_id| {
            let count = input
                .partitions_per_topic
                .get(topic_id)
                .copied()
                .unwrap_or(0);
            (0..count).map(move |p| (*topic_id, p))
        })
        .filter(|tp| !covered(tp))
        .collect();
    out.sort_unstable_by_key(|(topic_id, partition)| (topic_id.0, *partition));
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{
        super::{
            ShareGroupActorMessage,
            test_support::{heartbeat, make_coordinator, metadata_with_topic},
        },
        *,
    };
    use crate::{
        codes,
        coordinator::unified::{
            ShareGroupSeed,
            share::{
                actor::test_support::{
                    ShareSubscriptionSetup, TopicMetadataSetup, subscribed_request,
                    unavailable_persister_coordinator,
                },
                persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
                state::ShareMemberState,
            },
            test_support::{fixed_source, make_share_persister},
        },
    };

    /// Kafka's `subscribedTopicsChangeMap`: every partition of a subscribed
    /// topic in the image that is neither initialized nor freshly
    /// initializing.
    #[test]
    fn partitions_to_initialize_skip_initialized_and_fresh_initializing() {
        let orders = Uuid([1; 16]);
        let input = ReconcileInput {
            topic_id_by_name: HashMap::from([("orders".to_owned(), orders)]),
            partitions_per_topic: HashMap::from([(orders, 4)]),
            ..ReconcileInput::default()
        };
        // (row, initializing partition 1 recorded at, expected partitions)
        let rows = [
            ("fresh initializing entry is covered", 950, vec![2, 3]),
            ("stale initializing entry is retried", 800, vec![1, 2, 3]),
        ];
        for (row, recorded_at, expected) in rows {
            let mut state = ShareGroupState::new("g");
            state.members.insert(
                "m".to_owned(),
                ShareMemberState::joining(
                    "m",
                    "c",
                    "h",
                    HashSet::from(["orders".to_owned(), "missing".to_owned()]),
                ),
            );
            state.initialized.insert((orders, 0));
            state.initializing.insert((orders, 1), recorded_at);
            let expected: Vec<(Uuid, i32)> = expected.into_iter().map(|p| (orders, p)).collect();
            assert!(
                partitions_to_initialize(&state, &input, 1_000, 100) == expected,
                "{row}"
            );
        }
    }

    /// The share-partition metadata that the last committed write of `group`
    /// holds.
    fn committed_metadata(
        coordinator: &GroupCoordinator,
        group: &str,
    ) -> ShareGroupStatePartitionMetadataValue {
        coordinator
            .cached_share_seed(group)
            .expect("the group wrote a record")
            .state_partition_metadata
    }

    fn topic_partitions(topic_id: Uuid, partitions: Vec<i32>) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id: uuid::Uuid::from_bytes(topic_id.0),
            topic_name: "t".to_owned(),
            partitions,
        }
    }

    /// Kafka's `shareGroupHeartbeat` runs `persisterInitialize` "async with
    /// respect to the heartbeat". A share coordinator that cannot answer,
    /// here because `__share_group_state` does not exist, holds the
    /// persister for its whole five-second wait, and the heartbeat must not
    /// wait with it. The partitions are initializing when it answers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_heartbeat_does_not_wait_for_the_persister() {
        let (coordinator, log, topic_id) = unavailable_persister_coordinator(TopicMetadataSetup {
            partitions: crate::test_support::PartitionCount(2),
            ..Default::default()
        });
        let handle = coordinator.get_or_create_share("g");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            heartbeat(
                &handle,
                subscribed_request(ShareSubscriptionSetup::default()),
            ),
        )
        .await
        .expect("the heartbeat answers before the persister does");

        assert!(response.error_code == codes::NONE);
        assert!(
            committed_metadata(&coordinator, "g")
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![0, 1])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
        // Kafka's `shareGroupHeartbeat` writes one batch: the member (k10),
        // the group epoch (k11), the member's target (k13) and the target
        // metadata (k12), its current assignment (k14), and last the
        // partitions it initializes (k15).
        let key_versions: Vec<Vec<i16>> = log
            .batches()
            .await
            .iter()
            .map(|batch| {
                batch
                    .records
                    .iter()
                    .map(|record| {
                        let key = record.key.as_ref().expect("a record key");
                        i16::from_be_bytes([key[0], key[1]])
                    })
                    .collect()
            })
            .collect();
        assert!(key_versions == vec![vec![10, 11, 13, 12, 14, 15]]);
    }

    /// The outcome of the `Initialize` calls reaches the actor as a message.
    /// A partition the persister initialized is initialized, one it failed
    /// leaves the initializing set for a later heartbeat to retry, and one
    /// with no outcome yet stays initializing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_initialize_outcome_moves_each_partition() {
        let (metadata, topic_id) = metadata_with_topic(TopicMetadataSetup {
            partitions: crate::test_support::PartitionCount(3),
            ..Default::default()
        });
        let (coordinator, _log) = make_coordinator(metadata);
        let handle = coordinator.get_or_create_share("g");
        handle
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![0, 1, 2])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                },
                ..ShareGroupSeed::default()
            }))
            .await
            .expect("seed the group");

        handle
            .tx
            .send(ShareGroupActorMessage::ShareStateInitialized(vec![
                ((topic_id, 0), true),
                ((topic_id, 1), false),
            ]))
            .await
            .expect("send the outcome");
        let (reply, described) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(ShareGroupActorMessage::Describe { reply })
            .await
            .expect("describe after the outcome");
        described.await.expect("the actor answers in order");

        assert!(
            committed_metadata(&coordinator, "g")
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![2])],
                    initialized: vec![topic_partitions(topic_id, vec![0])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
    }

    /// Kafka's `buildInitializeShareGroupStateRequest` initializes every
    /// partition at start offset -1, a new partition of a topic that the
    /// group already initialized included.
    #[test]
    fn every_partition_initializes_at_the_uninitialized_start_offset() {
        let (metadata, topic_id) = metadata_with_topic(TopicMetadataSetup {
            partitions: crate::test_support::PartitionCount(2),
            ..Default::default()
        });
        let (coordinator, _log) = make_coordinator(metadata);
        coordinator.set_share_persister(make_share_persister(fixed_source(
            krabka_metadata::MetadataImage::default(),
        )));
        let mut state = ShareGroupState::new("g");
        state.members.insert(
            "m".into(),
            ShareMemberState::joining("m", "c", "h", ["t".to_owned()].into()),
        );
        state.mark_initialized((topic_id, 0));
        state.topic_names.insert(topic_id, "t".to_owned());

        let (value, initialize) = prepare_initialize(
            &mut state,
            &ShareGroupConfig::default(),
            &coordinator,
            1_000,
        )
        .expect("partition 1 initializes");

        assert!(
            initialize.calls
                == vec![InitializeCall {
                    topic_id,
                    partition: 1,
                    start_offset: UNINITIALIZED_START_OFFSET,
                }]
        );
        assert!(
            value
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![topic_partitions(topic_id, vec![1])],
                    initialized: vec![topic_partitions(topic_id, vec![0])],
                    ..ShareGroupStatePartitionMetadataValue::default()
                }
        );
    }

    /// Kafka's `maybeCleanupShareGroupState`: a topic the image no longer
    /// holds leaves all three sets, the rest stay, and an image that is not
    /// loaded yet deletes nothing.
    #[test]
    fn a_topic_missing_from_the_image_leaves_every_set() {
        let kept = Uuid([5; 16]);
        let gone = Uuid([6; 16]);
        let image = || {
            TopicImage::Snapshot(ReconcileInput {
                topic_id_by_name: HashMap::from([("kept".to_owned(), kept)]),
                partitions_per_topic: HashMap::from([(kept, 2)]),
                ..ReconcileInput::default()
            })
        };
        // (row, image, expected initialized, initializing, deleting, changed)
        let rows = [
            (
                "a deleted topic goes",
                image(),
                HashSet::from([(kept, 0)]),
                vec![(kept, 1)],
                vec![],
                true,
            ),
            (
                "an unloaded image deletes nothing",
                TopicImage::Snapshot(ReconcileInput::default()),
                HashSet::from([(kept, 0), (gone, 0)]),
                vec![(kept, 1), (gone, 1)],
                vec![gone],
                false,
            ),
        ];
        for (row, image, initialized, initializing, deleting, changed) in rows {
            let mut state = ShareGroupState::new("g");
            state.initialized.extend([(kept, 0), (gone, 0)]);
            state.initializing.extend([((kept, 1), 7), ((gone, 1), 7)]);
            state.deleting.insert(gone, "gone".to_owned());
            state.topic_names.insert(kept, "kept".to_owned());
            state.topic_names.insert(gone, "gone".to_owned());

            let deleted = deleted_topics(&state.state_topic_ids(), &image);

            assert!(state.cleanup_deleted_topics(&deleted) == changed, "{row}");
            assert!(state.initialized == initialized, "{row}");
            let mut held: Vec<(Uuid, i32)> = state.initializing.keys().copied().collect();
            held.sort_unstable_by_key(|(topic_id, partition)| (topic_id.0, *partition));
            assert!(held == initializing, "{row}");
            assert!(
                state.deleting.keys().copied().collect::<Vec<_>>() == deleting,
                "{row}"
            );
        }
    }
}
