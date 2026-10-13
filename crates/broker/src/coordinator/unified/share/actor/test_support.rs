//! Shared unit-test fixtures for the share-group actor modules: a static
//! metadata provider, a coordinator wired to an in-memory offsets log, and a
//! heartbeat round-trip helper.

use {
    super::{ShareGroupActorHandle, ShareGroupActorMessage},
    crate::{
        coordinator::unified::test_support::{
            GroupCoordinator, InMemoryOffsetsLog, MemberEpoch, MetadataProvider, NextGenConfig,
            ReconcileInput, ShareGroupConfig, StreamsGroupConfig, fixed_source,
            make_share_persister,
        },
        test_support::PartitionCount,
    },
    krabka_ids::PartitionIndex,
    krabka_protocol::{
        owned::{
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
            share_group_heartbeat_response::ShareGroupHeartbeatResponse,
        },
        primitives::uuid::Uuid,
    },
    std::sync::Arc,
    tokio::sync::oneshot,
};

#[derive(Debug)]
struct StaticMetadata {
    input: ReconcileInput,
}
impl MetadataProvider for StaticMetadata {
    fn snapshot(&self) -> ReconcileInput { self.input.clone() }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TopicMetadataSetup<'a> {
    #[default("t")]
    pub name: &'a str,
    pub partitions: PartitionCount,
}

impl TopicMetadataSetup<'_> {
    pub(super) fn four_partitions() -> Self {
        Self {
            partitions: PartitionCount(4),
            ..Default::default()
        }
    }
}

/// A share coordinator for a topic, with the requested group configuration.
pub(super) fn topic_coordinator(
    topic: TopicMetadataSetup<'_>,
    share: ShareGroupConfig,
) -> (Arc<GroupCoordinator>, Uuid) {
    let (metadata, id) = metadata_with_topic(topic);
    let (coordinator, _) =
        make_coordinator_with_config(metadata, NextGenConfig::assigning_at_once(), share);
    (coordinator, id)
}

/// Metadata snapshot with one named topic and the requested partition count.
pub(super) fn metadata_with_topic(
    setup: TopicMetadataSetup<'_>,
) -> (Arc<dyn MetadataProvider>, Uuid) {
    let id = Uuid([7; 16]);
    let input = ReconcileInput {
        topic_id_by_name: [(setup.name.to_string(), id)].into(),
        partitions_per_topic: [(id, setup.partitions.0)].into(),
        ..Default::default()
    };
    (Arc::new(StaticMetadata { input }), id)
}

/// A persister with no share-state topic cannot reach a share coordinator.
pub(super) fn unavailable_persister_coordinator(
    setup: TopicMetadataSetup<'_>,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>, Uuid) {
    let (metadata, topic_id) = metadata_with_topic(setup);
    let (coordinator, log) = make_coordinator(metadata);
    coordinator.set_share_persister(make_share_persister(fixed_source(
        krabka_metadata::MetadataImage::default(),
    )));
    (coordinator, log, topic_id)
}

/// Create the metadata, coordinator, log and first share actor in their usual order.
pub(super) fn subscribed_group(
    setup: TopicMetadataSetup<'_>,
) -> (
    Arc<GroupCoordinator>,
    Arc<InMemoryOffsetsLog>,
    Arc<ShareGroupActorHandle>,
) {
    let (metadata, _) = metadata_with_topic(setup);
    let (coordinator, log) = make_coordinator(metadata);
    let handle = coordinator.get_or_create_share("g");
    (coordinator, log, handle)
}

pub(super) fn make_coordinator(
    metadata: Arc<dyn MetadataProvider>,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    make_coordinator_with_config(
        metadata,
        NextGenConfig::assigning_at_once(),
        ShareGroupConfig::assigning_at_once(),
    )
}

pub(super) fn make_coordinator_with_config(
    metadata: Arc<dyn MetadataProvider>,
    next_gen: NextGenConfig,
    share: ShareGroupConfig,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>) {
    let log = Arc::new(InMemoryOffsetsLog::default());
    let coord = Arc::new(GroupCoordinator::new(
        next_gen,
        share,
        metadata,
        log.clone(),
        StreamsGroupConfig::default(),
    ));
    (coord, log)
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ShareSubscriptionSetup<'a> {
    #[default("m1")]
    pub member_id: &'a str,
    pub epoch: MemberEpoch,
}

/// Ordinary share heartbeat for the test topic, with every other wire field defaulted.
pub(super) fn subscribed_request(setup: ShareSubscriptionSetup<'_>) -> ShareGroupHeartbeatRequest {
    ShareGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: setup.member_id.into(),
        member_epoch: setup.epoch.0,
        subscribed_topic_names: Some(vec!["t".into()]),
        ..Default::default()
    }
}

pub(super) async fn heartbeat(
    handle: &ShareGroupActorHandle,
    req: ShareGroupHeartbeatRequest,
) -> ShareGroupHeartbeatResponse {
    let (tx, rx) = oneshot::channel();
    handle
        .tx
        .send(ShareGroupActorMessage::Heartbeat {
            request: req,
            client_id: "client-a".into(),
            client_host: "/127.0.0.1".into(),
            reply: tx,
        })
        .await
        .unwrap();
    rx.await.unwrap()
}

/// Seeds the group behind `handle` with `partitions` of `topic` already
/// initialized, as bootstrap replay of a `ShareGroupStatePartitionMetadata`
/// record would.
#[derive(krabka_macros::FieldDefaults)]
pub(super) struct InitializedTopicSetup<'a> {
    pub topic_id: Uuid,
    #[default("t")]
    pub topic_name: &'a str,
    #[default(vec![PartitionIndex(0)])]
    pub partitions: Vec<PartitionIndex>,
}

pub(super) async fn seed_initialized(
    handle: &ShareGroupActorHandle,
    setup: InitializedTopicSetup<'_>,
) {
    use crate::coordinator::unified::{
        ShareGroupSeed,
        share::persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
    };
    let InitializedTopicSetup {
        topic_id,
        topic_name,
        partitions,
    } = setup;
    handle
        .tx
        .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
            state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                initialized: vec![TopicPartitionsInfo {
                    topic_id: uuid::Uuid::from_bytes(topic_id.0),
                    topic_name: topic_name.to_owned(),
                    partitions: partitions.into_iter().map(|index| index.0).collect(),
                }],
                ..ShareGroupStatePartitionMetadataValue::default()
            },
            ..ShareGroupSeed::new_group()
        }))
        .await
        .unwrap();
}
