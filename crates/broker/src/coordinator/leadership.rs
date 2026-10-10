//! Group-coordinator actor ownership across offsets-partition leader changes.
//!
//! A partition this broker starts to lead is replayed in the background. Until
//! that replay has seeded every group of the partition, the partition is
//! loading: Kafka's `CoordinatorRuntime.withActiveContextOrThrow` answers
//! `COORDINATOR_LOAD_IN_PROGRESS` for a shard in the `LOADING` state, and
//! [`GroupCoordinator::is_loading`] is what the group RPC routing check reads
//! to do the same. Without it a request would find no actor, create an empty
//! one, and have its answer overwritten when the replay seeds the group.
//!
//! A request can read a new metadata image before the image watcher below has
//! taken the new leadership up. A leadership term the watcher has not taken up
//! yet is loading too, as Kafka's runtime has no `ACTIVE` shard for an
//! election it has not processed, so no request slips in ahead of the load.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use krabka_ids::{LeaderEpoch, PartitionIndex};
use krabka_metadata::{MetadataImage, NodeId};
use tokio_util::sync::CancellationToken;

use super::{
    GroupCoordinator,
    bootstrap::{OFFSETS_TOPIC, replay_partition},
    partitioner::partition_for_group,
    unified::{
        actor::GroupActorMessage, share::actor::ShareGroupActorMessage,
        streams::actor::StreamsGroupActorMessage,
    },
};
use crate::{
    metadata_source::MetadataSource, partition_registry::PartitionRegistry,
    task_util::shutdown_actor,
};

pub(crate) fn spawn(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    coordinator: Arc<GroupCoordinator>,
    shutdown: CancellationToken,
) {
    // The partitions led at start were replayed by the storage recovery, so
    // their terms are taken up as served before any request is routed.
    let mut images = metadata.watch_image();
    let mut previous_image = images.borrow_and_update().clone();
    let mut led = led_partitions(&previous_image, node_id);
    for (&partition, &epoch) in &led {
        coordinator.take_up(partition, epoch);
    }
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                image = crate::metadata_source::next_published_image(&mut images) => {
                    let Some(image) = image else { break; };
                    let next = led_partitions(&image, node_id);
                    let lost: Vec<PartitionIndex> =
                        led.keys().filter(|p| !next.contains_key(p)).copied().collect();
                    for partition in lost {
                        coordinator.end_any_load(partition);
                        unload_partition(&coordinator, &previous_image, partition).await;
                    }
                    for (&partition, &epoch) in &next {
                        if led.contains_key(&partition) {
                            // Still led: a newer epoch keeps the shard as it
                            // is, as Kafka's `scheduleLoadOperation` only
                            // bumps the epoch of a loaded or loading shard.
                            coordinator.take_up(partition, epoch);
                            continue;
                        }
                        // Marked before the task starts, so no request routed
                        // after this image lands can slip in ahead of it.
                        let load = LoadGuard::begin(Arc::clone(&coordinator), partition, epoch);
                        spawn_partition_load(
                            node_id,
                            Arc::clone(&metadata),
                            Arc::clone(&partitions),
                            load,
                            partition,
                            shutdown.child_token(),
                        );
                    }
                    led = next;
                    previous_image = image;
                }
            }
        }
    });
}

/// Source of the ids that tell one load of a partition from the next.
static NEXT_LOAD_ID: AtomicU64 = AtomicU64::new(0);

/// The `__consumer_offsets` leadership terms the image watcher has taken up,
/// and which of them are still replaying. One lock guards both, so a routing
/// check never sees a term taken up without the load that came with it.
#[derive(Debug, Default)]
pub(crate) struct ShardTerms {
    /// The leader epoch of every offsets partition this broker leads, as the
    /// image watcher last took it up.
    led: HashMap<i32, LeaderEpoch>,
    /// The led partitions still replaying, each with the id of the load that
    /// owns the entry.
    loading: HashMap<i32, u64>,
}

impl GroupCoordinator {
    fn shard_terms(&self) -> std::sync::MutexGuard<'_, ShardTerms> {
        self.shard_terms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `true` while `partition` of `__consumer_offsets`, which the image
    /// names this broker's to lead at `leader_epoch`, is not yet served here:
    /// the image watcher has not taken that term up, or its replay has not
    /// finished.
    pub(crate) fn is_loading(&self, partition: i32, leader_epoch: LeaderEpoch) -> bool {
        let terms = self.shard_terms();
        terms.led.get(&partition) != Some(&leader_epoch) || terms.loading.contains_key(&partition)
    }

    /// `true` while any offsets partition this broker leads is replaying, the
    /// check of Kafka's read-all-shards operations such as `ListGroups`.
    pub(crate) fn is_any_loading(&self) -> bool {
        !self.shard_terms().loading.is_empty()
    }

    /// Record `epoch` as the term this broker leads `partition` under,
    /// without a load.
    fn take_up(&self, partition: PartitionIndex, epoch: LeaderEpoch) {
        self.shard_terms().led.insert(partition.get(), epoch);
    }

    fn begin_load(&self, partition: PartitionIndex, epoch: LeaderEpoch) -> u64 {
        let load_id = NEXT_LOAD_ID.fetch_add(1, Ordering::Relaxed);
        let mut terms = self.shard_terms();
        terms.led.insert(partition.get(), epoch);
        terms.loading.insert(partition.get(), load_id);
        load_id
    }

    /// Clears the mark only if the load `load_id` still owns it, so a load
    /// that outlives a lost and regained leadership cannot clear the newer
    /// load's mark.
    fn end_load(&self, partition: i32, load_id: u64) {
        let mut terms = self.shard_terms();
        if terms.loading.get(&partition) == Some(&load_id) {
            terms.loading.remove(&partition);
        }
    }

    /// Forget the term and any load of a partition this broker no longer
    /// leads.
    fn end_any_load(&self, partition: PartitionIndex) {
        let mut terms = self.shard_terms();
        terms.led.remove(&partition.get());
        terms.loading.remove(&partition.get());
    }
}

/// The loading mark of one partition load. Dropping it clears the mark,
/// whether the load finished, failed, lost the leadership or was cancelled.
struct LoadGuard {
    coordinator: Arc<GroupCoordinator>,
    partition: i32,
    load_id: u64,
}

impl LoadGuard {
    fn begin(
        coordinator: Arc<GroupCoordinator>,
        partition: PartitionIndex,
        epoch: LeaderEpoch,
    ) -> Self {
        let load_id = coordinator.begin_load(partition, epoch);
        Self {
            coordinator,
            partition: partition.get(),
            load_id,
        }
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.coordinator.end_load(self.partition, self.load_id);
    }
}

fn spawn_partition_load(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    load: LoadGuard,
    partition: PartitionIndex,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let coordinator = Arc::clone(&load.coordinator);
        let _load = load;
        loop {
            let still_leader = metadata
                .current_image()
                .partition(OFFSETS_TOPIC, partition.get())
                .is_some_and(|record| record.leader == node_id);
            if !still_leader {
                return;
            }
            if partitions.contains(OFFSETS_TOPIC, partition) {
                if let Err(error) = replay_partition(&partitions, &coordinator, partition).await {
                    tracing::error!(
                        partition = partition.get(),
                        %error,
                        "could not load newly-led group coordinator partition"
                    );
                    return;
                }
                let image = metadata.current_image();
                super::topic_deletion::after_partition_load(&coordinator, &image, |group_id| {
                    partition_for_group(&image, group_id) == partition.get()
                })
                .await;
                return;
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }
    });
}

fn led_partitions(image: &MetadataImage, node_id: NodeId) -> HashMap<PartitionIndex, LeaderEpoch> {
    image
        .partitions_of(OFFSETS_TOPIC)
        .filter(|partition| partition.leader == node_id)
        .map(|partition| (PartitionIndex(partition.partition), partition.leader_epoch))
        .collect()
}

async fn unload_partition(
    coordinator: &GroupCoordinator,
    image: &MetadataImage,
    partition: PartitionIndex,
) {
    let timeout = coordinator
        .config
        .shutdown_ack_timeout
        .max(Duration::from_millis(1));
    // Offset-only groups have a classic actor but no protocol-type record.
    // Include every live actor and seed map so losing an offsets partition
    // cannot leave any stale coordinator state reachable on the old leader.
    let mut known_group_ids = HashSet::new();
    known_group_ids.extend(
        coordinator
            .group_types
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.groups.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .share_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_groups
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(coordinator.seeds.iter().map(|entry| entry.key().clone()));
    known_group_ids.extend(
        coordinator
            .seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .share_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds
            .iter()
            .map(|entry| entry.key().clone()),
    );
    known_group_ids.extend(
        coordinator
            .streams_seeds_cache
            .iter()
            .map(|entry| entry.key().clone()),
    );
    let group_ids: Vec<String> = known_group_ids
        .into_iter()
        .filter(|group_id| partition_for_group(image, group_id) == partition.get())
        .collect();
    for group_id in group_ids {
        if let Some((_, handle)) = coordinator.groups.remove(&group_id) {
            shutdown_actor(&handle.tx, GroupActorMessage::Shutdown, timeout).await;
        }
        if let Some((_, handle)) = coordinator.share_groups.remove(&group_id) {
            shutdown_actor(&handle.tx, ShareGroupActorMessage::Shutdown, timeout).await;
        }
        if let Some((_, handle)) = coordinator.streams_groups.remove(&group_id) {
            shutdown_actor(&handle.tx, StreamsGroupActorMessage::Shutdown, timeout).await;
        }
        coordinator.seeds.remove(&group_id);
        coordinator.seeds_cache.remove(&group_id);
        coordinator.share_seeds.remove(&group_id);
        coordinator.share_seeds_cache.remove(&group_id);
        coordinator.streams_seeds.remove(&group_id);
        coordinator.streams_seeds_cache.remove(&group_id);
        coordinator.group_types.remove(&group_id);
        // The group's offsets partition moved to another broker, so its lag is
        // that broker's to report from here on. Nothing else would release the
        // series: this broker's sampler will never name the group again.
        coordinator.forget_group_metrics(&group_id);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{MetadataRecord, PartitionRecord};

    use super::*;

    #[test]
    fn led_partitions_track_every_local_offsets_leader_and_its_epoch() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (partition, leader, epoch) in [(0, NodeId(1), 4), (1, NodeId(2), 5), (2, NodeId(1), 6)]
        {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.into(),
                partition,
                leader,
                leader_epoch: LeaderEpoch(epoch),
                replicas: vec![leader],
                isr: vec![leader],
                ..PartitionRecord::default()
            }));
        }
        check!(
            led_partitions(&image, NodeId(1))
                == maplit::hashmap! {
                    PartitionIndex(0) => LeaderEpoch(4),
                    PartitionIndex(2) => LeaderEpoch(6),
                }
        );
    }

    /// A coordinator that leads the one partition of the offsets topic, its
    /// image, and the metrics it reports to.
    fn coordinator_of_the_offsets_partition() -> (
        Arc<GroupCoordinator>,
        MetadataImage,
        crate::metrics::BrokerMetrics,
    ) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: OFFSETS_TOPIC.into(),
            topic_id: uuid::Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: OFFSETS_TOPIC.into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            ..PartitionRecord::default()
        }));
        let metadata: Arc<dyn MetadataSource> = Arc::new(
            crate::test_support::FakeMetadataSource::builder()
                .image(image.clone())
                .leader(Some(NodeId(1)))
                .build(),
        );
        let coordinator = crate::coordinator::test_support::default_coordinator(
            Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: Arc::clone(&metadata),
            }),
            Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
        );
        let metrics = crate::metrics::BrokerMetrics::new();
        coordinator.set_metrics(metrics.clone());
        (coordinator, image, metrics)
    }

    /// Losing the offsets partition that hosts a group ends this broker's
    /// claim to report that group's lag. Nothing else would release the
    /// series: no metadata image records a group's coordinator moving, and
    /// this broker's lag sampler will never name the group again.
    #[tokio::test]
    async fn unloading_a_partition_releases_its_groups_lag_series() {
        let (coordinator, image, metrics) = coordinator_of_the_offsets_partition();
        // The group has to be live for the unload to find it.
        let _actor = coordinator.get_or_create_classic("billing");
        let label = crate::metrics::ConsumerGroupLabel {
            group_id: "billing".into(),
            topic: "orders".into(),
            partition: 0,
        };
        metrics.publish_consumer_group_lag(&std::collections::HashMap::from([(label.clone(), 33)]));

        unload_partition(&coordinator, &image, PartitionIndex(0)).await;

        check!(metrics.consumer_group_lag.get(&label).is_none());
    }

    /// Losing the offsets partition of a share group makes this broker stop
    /// coordinating it, not stop leading its share partitions, which go on
    /// writing dead-letter records. The counters of those writes are not the
    /// coordinator's to release, and would start again from zero if they were.
    #[tokio::test]
    async fn unloading_a_partition_keeps_its_share_groups_dead_letter_counters() {
        let (coordinator, image, metrics) = coordinator_of_the_offsets_partition();
        coordinator.mark_share("workers");
        let _actor = coordinator.get_or_create_share("workers");
        metrics.record_share_dlq_produce("workers");
        metrics.record_share_dlq_records("workers", 3);
        metrics.record_share_dlq_produce_failed("workers");

        unload_partition(&coordinator, &image, PartitionIndex(0)).await;

        let label = crate::metrics::ShareGroupIdLabel {
            group_id: "workers".into(),
        };
        check!(
            coordinator.share_group_ids().is_empty(),
            "the group unloaded"
        );
        check!(metrics.share_group_dlq_records.get_or_create(&label).get() == 3);
        check!(
            metrics
                .share_group_dlq_produce_requests
                .get_or_create(&label)
                .get()
                == 1
        );
        check!(
            metrics
                .share_group_dlq_failed_produce_requests
                .get_or_create(&label)
                .get()
                == 1
        );
    }

    /// The group RPCs this broker serves, each reduced to the error code a
    /// client reads first.
    #[derive(Debug, Clone, Copy)]
    enum GroupRpc {
        JoinGroup,
        Heartbeat,
        LeaveGroup,
        OffsetCommit,
        TxnOffsetCommit,
        OffsetFetch,
        DeleteGroups,
        ListGroups,
    }

    async fn call(broker: &crate::broker::Broker, rpc: GroupRpc) -> i16 {
        use krabka_protocol::owned::{
            delete_groups_request::DeleteGroupsRequest,
            heartbeat_request::HeartbeatRequest,
            join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
            leave_group_request::{LeaveGroupRequest, MemberIdentity},
            list_groups_request::ListGroupsRequest,
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup},
            txn_offset_commit_request::{
                TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition,
                TxnOffsetCommitRequestTopic,
            },
        };

        let principal = crate::test_support::principal("alice");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "load-client");
        match rpc {
            GroupRpc::JoinGroup => {
                let request = JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 30_000,
                    rebalance_timeout_ms: 30_000,
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                crate::handlers::join_group::handle(broker, request, 9, &ctx)
                    .await
                    .unwrap()
                    .error_code
            }
            GroupRpc::Heartbeat => {
                let request = HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    generation_id: 1,
                    ..Default::default()
                };
                crate::handlers::heartbeat::handle(broker, request, 4, &ctx)
                    .await
                    .unwrap()
                    .error_code
            }
            GroupRpc::LeaveGroup => {
                let request = LeaveGroupRequest {
                    group_id: "g".into(),
                    members: vec![MemberIdentity {
                        member_id: "m".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                crate::handlers::leave_group::handle(broker, request, 5, &ctx)
                    .await
                    .unwrap()
                    .error_code
            }
            GroupRpc::OffsetCommit => {
                let request = OffsetCommitRequest {
                    group_id: "g".into(),
                    generation_id_or_member_epoch: -1,
                    topics: vec![OffsetCommitRequestTopic {
                        // A topic the image knows, so the commit reaches the coordinator.
                        name: OFFSETS_TOPIC.into(),
                        partitions: vec![OffsetCommitRequestPartition {
                            partition_index: 0,
                            committed_offset: 5,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                crate::handlers::offset_commit::handle(broker, request, 8, &ctx)
                    .await
                    .unwrap()
                    .topics[0]
                    .partitions[0]
                    .error_code
            }
            GroupRpc::TxnOffsetCommit => {
                let request = TxnOffsetCommitRequest {
                    transactional_id: "tid".into(),
                    group_id: "g".into(),
                    producer_id: 7,
                    producer_epoch: 0,
                    generation_id_or_member_epoch: -1,
                    topics: vec![TxnOffsetCommitRequestTopic {
                        // A topic the image knows, so the commit reaches the coordinator.
                        name: OFFSETS_TOPIC.into(),
                        partitions: vec![TxnOffsetCommitRequestPartition {
                            partition_index: 0,
                            committed_offset: 5,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                crate::txn::handlers::txn_offset_commit::handle(broker, request, 4, &ctx)
                    .await
                    .unwrap()
                    .topics[0]
                    .partitions[0]
                    .error_code
            }
            GroupRpc::OffsetFetch => {
                let request = OffsetFetchRequest {
                    groups: vec![OffsetFetchRequestGroup {
                        group_id: "g".into(),
                        topics: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                crate::handlers::offset_fetch::handle(broker, request, 8, &ctx)
                    .await
                    .unwrap()
                    .groups[0]
                    .error_code
            }
            GroupRpc::ListGroups => {
                crate::handlers::list_groups::handle(broker, ListGroupsRequest::default(), 4, &ctx)
                    .await
                    .unwrap()
                    .error_code
            }
            GroupRpc::DeleteGroups => {
                let request = DeleteGroupsRequest {
                    groups_names: vec!["g".into()],
                    ..Default::default()
                };
                crate::handlers::delete_groups::handle(broker, request, 2, &ctx)
                    .await
                    .unwrap()
                    .results[0]
                    .error_code
            }
        }
    }

    /// How this broker stands with a group's offsets partition that the image
    /// says it leads, short of serving it.
    #[derive(Debug, Clone, Copy)]
    enum Unserved {
        /// The image watcher took the term up and the replay runs.
        Loading,
        /// The image is published but the watcher has not taken the term up.
        NotTakenUp,
        /// The watcher last took up an older term of the same partition.
        OlderTerm,
    }

    /// While the group's offsets partition is not served, every group RPC
    /// answers `COORDINATOR_LOAD_IN_PROGRESS` (the classic `Heartbeat` `NONE`,
    /// as `GroupCoordinatorService.heartbeat` maps it) and creates no actor.
    /// Once the partition is served the same group is served again.
    #[tokio::test]
    async fn group_rpcs_answer_load_in_progress_until_the_partition_is_served() {
        let (broker_handle, _dir) = crate::test_support::start_broker_no_audit_with(|config| {
            config.offsets_topic_replication_factor = 1;
        })
        .await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        let image = broker.controller.current_image();
        let partition = PartitionIndex(partition_for_group(&image, "g"));
        let epoch = image
            .partition(OFFSETS_TOPIC, partition.get())
            .unwrap()
            .leader_epoch;
        check!(!broker.group_coordinator.is_loading(partition.get(), epoch));

        let rows = [
            (
                GroupRpc::JoinGroup,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (GroupRpc::Heartbeat, crate::codes::NONE),
            (
                GroupRpc::LeaveGroup,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::OffsetCommit,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::TxnOffsetCommit,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::OffsetFetch,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                GroupRpc::DeleteGroups,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            // Kafka's `listGroups` reads every local shard, so one loading
            // shard fails the whole answer.
            (
                GroupRpc::ListGroups,
                crate::codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
        ];
        for unserved in [Unserved::Loading, Unserved::NotTakenUp, Unserved::OlderTerm] {
            let coordinator = &broker.group_coordinator;
            let load = match unserved {
                Unserved::Loading => {
                    Some(LoadGuard::begin(Arc::clone(coordinator), partition, epoch))
                }
                Unserved::NotTakenUp => {
                    coordinator.end_any_load(partition);
                    None
                }
                Unserved::OlderTerm => {
                    coordinator.take_up(partition, LeaderEpoch(epoch.0 - 1));
                    None
                }
            };
            for (rpc, want) in rows {
                check!(call(&broker, rpc).await == want, "{unserved:?} {rpc:?}");
                check!(coordinator.find("g").is_none(), "{unserved:?} {rpc:?}");
            }

            drop(load);
            coordinator.take_up(partition, epoch);

            check!(
                !coordinator.is_loading(partition.get(), epoch),
                "{unserved:?}"
            );
        }
        check!(call(&broker, GroupRpc::JoinGroup).await == crate::codes::MEMBER_ID_REQUIRED);
        check!(call(&broker, GroupRpc::ListGroups).await == crate::codes::NONE);
        broker_handle.shutdown().await;
    }

    /// A load that outlives a lost and regained leadership cannot clear the
    /// mark of the load that replaced it.
    #[test]
    fn a_stale_load_does_not_clear_the_newer_mark() {
        let coordinator = crate::coordinator::test_support::default_coordinator(
            Arc::new(crate::coordinator::unified::ImageMetadataProvider {
                controller: Arc::new(crate::test_support::FakeMetadataSource::builder().build()),
            }),
            Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
        );
        let stale = LoadGuard::begin(Arc::clone(&coordinator), PartitionIndex(3), LeaderEpoch(1));
        coordinator.end_any_load(PartitionIndex(3));
        let current = LoadGuard::begin(Arc::clone(&coordinator), PartitionIndex(3), LeaderEpoch(2));

        drop(stale);
        check!(coordinator.is_loading(3, LeaderEpoch(2)));
        drop(current);
        check!(!coordinator.is_loading(3, LeaderEpoch(2)));
    }
}
