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
#[bon::builder]
pub(super) fn topic_coordinator(
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default)] partitions: PartitionCount,
    share: ShareGroupConfig,
) -> (Arc<GroupCoordinator>, Uuid) {
    let topic = TopicMetadataSetup { name, partitions };
    let (metadata, id) = metadata_with_topic()
        .name(topic.name)
        .partitions(topic.partitions)
        .call();
    let (coordinator, _) =
        make_coordinator_with_config(metadata, NextGenConfig::assigning_at_once(), share);
    (coordinator, id)
}

/// Metadata snapshot with one named topic and the requested partition count.
#[bon::builder]
pub(super) fn metadata_with_topic(
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default)] partitions: PartitionCount,
) -> (Arc<dyn MetadataProvider>, Uuid) {
    let setup = TopicMetadataSetup { name, partitions };
    let id = Uuid([7; 16]);
    let input = ReconcileInput {
        topic_id_by_name: [(setup.name.to_string(), id)].into(),
        partitions_per_topic: [(id, setup.partitions.0)].into(),
        ..Default::default()
    };
    (Arc::new(StaticMetadata { input }), id)
}

/// A persister with no share-state topic cannot reach a share coordinator.
#[bon::builder]
pub(super) fn unavailable_persister_coordinator(
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default)] partitions: PartitionCount,
) -> (Arc<GroupCoordinator>, Arc<InMemoryOffsetsLog>, Uuid) {
    let setup = TopicMetadataSetup { name, partitions };
    let (metadata, topic_id) = metadata_with_topic()
        .name(setup.name)
        .partitions(setup.partitions)
        .call();
    let (coordinator, log) = make_coordinator(metadata);
    coordinator.set_share_persister(make_share_persister(fixed_source(
        krabka_metadata::MetadataImage::default(),
    )));
    (coordinator, log, topic_id)
}

/// Create the metadata, coordinator, log and first share actor in their usual order.
#[bon::builder]
pub(super) fn subscribed_group(
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default)] partitions: PartitionCount,
) -> (
    Arc<GroupCoordinator>,
    Arc<InMemoryOffsetsLog>,
    Arc<ShareGroupActorHandle>,
) {
    let setup = TopicMetadataSetup { name, partitions };
    let (metadata, _) = metadata_with_topic()
        .name(setup.name)
        .partitions(setup.partitions)
        .call();
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
#[bon::builder]
pub(super) fn subscribed_request(
    #[builder(default = "m1")] member_id: &'_ str,
    #[builder(into, default)] epoch: MemberEpoch,
) -> ShareGroupHeartbeatRequest {
    let setup = ShareSubscriptionSetup { member_id, epoch };
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

#[bon::builder]
pub(super) async fn seed_initialized(
    handle: &ShareGroupActorHandle,
    #[builder(default)] topic_id: Uuid,
    #[builder(default = "t")] topic_name: &'_ str,
    #[builder(default = vec ! [PartitionIndex (0)])] partitions: Vec<PartitionIndex>,
) {
    use crate::coordinator::unified::{
        ShareGroupSeed,
        share::persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
    };

    let setup = InitializedTopicSetup {
        topic_id,
        topic_name,
        partitions,
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
