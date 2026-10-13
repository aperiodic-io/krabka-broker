//! KIP-932 share-state persister RPC handlers (api keys 83–87). Each handler
//! decodes the typed request and delegates every `(topic, partition)` to the
//! matching coordinator method. The method answers per-partition
//! `NOT_COORDINATOR` when this broker does not lead the state partition, and
//! `COORDINATOR_LOAD_IN_PROGRESS` while the state partition loads. The handler
//! maps the result to a per-partition `error_code`.
//!
//! These are inter-broker RPCs. As in Kafka, every handler first checks
//! `ClusterAction` on `Cluster("kafka-cluster")` for the principal of the
//! connection. A broker that calls them needs that grant, or super-user
//! status.

pub(crate) mod delete;
pub(crate) mod initialize;
pub(crate) mod read;
pub(crate) mod read_summary;
pub(crate) mod write;

#[cfg(test)]
mod authorization_tests;

use crate::{
    broker::Broker,
    codes::NONE,
    handlers::{RequestContext, cluster_action_denied as request_cluster_action_denied},
};

/// Whether a request has no topic rows or contains an empty partition row.
fn empty_partition_data<T>(topics: &[T], empty: impl Fn(&T) -> bool) -> bool {
    topics.is_empty() || topics.iter().any(empty)
}

struct OperationVerdict {
    code: i16,
    message: Option<String>,
}

/// The code and Kafka message for a completed share-state operation.
fn operation_result(
    result: Result<(), super::coordinator::ShareStateError>,
    operation: &str,
) -> OperationVerdict {
    match result {
        Ok(()) => OperationVerdict {
            code: NONE,
            message: None,
        },
        Err(error) => OperationVerdict {
            code: error.code(),
            message: Some(error.row_message(operation)),
        },
    }
}

/// Kafka's message for `CLUSTER_AUTHORIZATION_FAILED`, which
/// `toGlobalErrorResponse` puts on every partition row.
const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

/// Whether the authorizer denies `ClusterAction` on the cluster to the
/// principal of `ctx`.
fn cluster_action_denied(broker: &Broker, ctx: &RequestContext<'_>) -> bool {
    request_cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    )
}

#[cfg(test)]
pub(crate) mod test_support {
    use {
        crate::{
            broker::Broker,
            partition_registry::PartitionRegistry,
            share_coordinator::{
                bootstrap,
                config::ShareCoordinatorConfig,
                coordinator::{
                    LoadStatus, ShareCoordinator,
                    test_support::{
                        DeliveryAttemptCount, FixtureDeliveryState, image_with_topic, manual_clock,
                        open_state_partition, share_write, state_batch,
                    },
                },
                persistence::StateBatch,
            },
            test_support::KafkaErrorCode,
            topic_creator::TopicCreator,
        },
        krabka_ids::{NodeId, PartitionIndex},
        krabka_log::Offset,
        krabka_metadata::MetadataImage,
        krabka_protocol::{
            UnknownTaggedFields,
            owned::{
                delete_share_group_state_response, initialize_share_group_state_response,
                read_share_group_state_response, read_share_group_state_summary_response,
                write_share_group_state_response,
            },
            primitives::uuid::Uuid as ProtoUuid,
        },
        std::{fmt::Debug, path::Path, sync::Arc},
    };

    pub(crate) fn batch(bounds: std::ops::RangeInclusive<Offset>) -> StateBatch {
        state_batch()
            .bounds(bounds)
            .delivery(FixtureDeliveryState::Acknowledged)
            .attempts(DeliveryAttemptCount(3))
            .call()
    }

    pub(crate) fn open_all_state_partitions(
        registry: &PartitionRegistry,
        log_dir: &Path,
        num_partitions: i32,
    ) {
        for partition in 0..num_partitions {
            open_state_partition(registry, log_dir, partition);
        }
    }

    pub(crate) fn coordinator(log_dir: &Path) -> Arc<ShareCoordinator> {
        coordinator_with(log_dir, ShareCoordinatorConfig::default())
    }

    /// [`coordinator`] with `config`, for a test of a rule that Kafka trunk
    /// added to the share coordinator.
    pub(crate) fn coordinator_with(
        log_dir: &Path,
        config: ShareCoordinatorConfig,
    ) -> Arc<ShareCoordinator> {
        let registry = Arc::new(PartitionRegistry::new());
        open_all_state_partitions(&registry, log_dir, config.state_topic_num_partitions);
        Arc::new(ShareCoordinator::with_wall_clock(
            NodeId(1),
            registry,
            config,
            manual_clock(),
        ))
    }

    use crate::test_support::PartitionCount;

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    pub(crate) struct StateFixtureSetup {
        #[default(uuid::Uuid::from_bytes([33; 16]))]
        pub topic: uuid::Uuid,
        #[default(PartitionCount(1))]
        pub partitions: PartitionCount,
        pub partition: PartitionIndex,
        #[default(false)]
        pub stored_data: bool,
    }

    /// A coordinator and the metadata image it runs over.
    pub(crate) struct StateFixture {
        pub coordinator: Arc<ShareCoordinator>,
        pub image: krabka_metadata::MetadataImage,
    }

    /// A led coordinator with partition state at epoch 17 and start offset 90.
    #[bon::builder]
    pub(crate) async fn initialized_state(
        log_dir: &Path,
        #[builder(default = uuid :: Uuid :: from_bytes ([33 ; 16]))] topic: uuid::Uuid,
        #[builder(into, default = PartitionCount (1))] partitions: PartitionCount,
        #[builder(into, default)] partition: PartitionIndex,
        #[builder(default)] stored_data: bool,
    ) -> StateFixture {
        let setup = StateFixtureSetup {
            topic,
            partitions,
            partition,
            stored_data,
        };
        let coordinator = coordinator(log_dir);
        let image = image_with_topic(setup.topic, setup.partitions.0);
        coordinator.lead_all_partitions_for_test().await;
        coordinator
            .initialize(
                &image,
                "share-group",
                setup.topic,
                setup.partition.0,
                17,
                Offset(90),
            )
            .await
            .expect("initialize state");
        if setup.stored_data {
            coordinator
                .read(&image, "share-group", setup.topic, setup.partition.0, 3)
                .await
                .expect("raise the stored leader epoch");
            coordinator
                .write(
                    &image,
                    "share-group",
                    setup.topic,
                    setup.partition.0,
                    share_write((17, 3), (101, 9), vec![batch(Offset(101)..=Offset(105))]),
                )
                .await
                .expect("write state");
        }
        StateFixture { coordinator, image }
    }

    pub(crate) async fn retain_leadership(coordinator: &Arc<ShareCoordinator>, led: bool) {
        if !led {
            coordinator
                .refresh_leader_partitions(&krabka_metadata::MetadataImage::default())
                .await
                .finished()
                .await;
        }
    }

    /// One external response's associated topic and partition row schemas.
    pub(crate) trait StateReply: Sized {
        type Partition;
        type Topic;
        fn topic(topic: ProtoUuid, partitions: Vec<Self::Partition>) -> Self::Topic;
        fn from_results(results: Vec<Self::Topic>) -> Self;
        fn error_row(partition: i32, code: i16, message: Option<&str>) -> Self::Partition;
    }

    krabka_macros::share_fixture_schemas! {
        initialize_share_group_state_response::InitializeShareGroupStateResponse => initialize_share_group_state_response::InitializeStateResult, initialize_share_group_state_response::PartitionResult;
        write_share_group_state_response::WriteShareGroupStateResponse => write_share_group_state_response::WriteStateResult, write_share_group_state_response::PartitionResult;
        delete_share_group_state_response::DeleteShareGroupStateResponse => delete_share_group_state_response::DeleteStateResult, delete_share_group_state_response::PartitionResult;
        read_share_group_state_response::ReadShareGroupStateResponse => read_share_group_state_response::ReadStateResult, read_share_group_state_response::PartitionResult;
        read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse => read_share_group_state_summary_response::ReadStateSummaryResult, read_share_group_state_summary_response::PartitionResult;
    }

    pub(crate) fn state_response<R: StateReply>(
        topic: ProtoUuid,
        partitions: Vec<R::Partition>,
    ) -> R {
        R::from_results(vec![R::topic(topic, partitions)])
    }

    /// The keyed write-RPC response fields, with explicit caller-selected default topic.
    #[derive(Clone, Copy, Default)]
    pub(crate) struct StateResponseSetup<'a> {
        pub(crate) topic: uuid::Uuid,
        pub(crate) partition: PartitionIndex,
        pub(crate) code: KafkaErrorCode,
        pub(crate) message: Option<&'a str>,
    }

    impl StateResponseSetup<'_> {
        pub(crate) fn for_topic(topic: uuid::Uuid) -> Self {
            Self {
                topic,
                ..Self::default()
            }
        }

        pub(crate) fn response<R: StateReply>(self) -> R {
            state_response::<R>(ProtoUuid(*self.topic.as_bytes()), vec![R::error_row(
                self.partition.0,
                self.code.0,
                self.message,
            )])
        }
    }

    pub(crate) struct StoredStateRow<Q, R> {
        pub(crate) led: bool,
        pub(crate) request: Q,
        pub(crate) expected: R,
    }

    /// The three empty request shapes that every read RPC answers with no rows.
    pub(crate) struct EmptyStateRequests<Q> {
        pub(crate) group: Q,
        pub(crate) partitions: Q,
        pub(crate) topics: Q,
    }

    impl<Q> EmptyStateRequests<Q> {
        pub(crate) fn into_rows<R: Clone>(self, empty: R) -> [StoredStateRow<Q, R>; 3] {
            [
                StoredStateRow {
                    led: true,
                    request: self.group,
                    expected: empty.clone(),
                },
                StoredStateRow {
                    led: true,
                    request: self.partitions,
                    expected: empty.clone(),
                },
                StoredStateRow {
                    led: true,
                    request: self.topics,
                    expected: empty,
                },
            ]
        }
    }

    /// Check read operations sequentially against each independently supplied complete response.
    pub(crate) async fn check_stored_rows<Q, R: PartialEq + Debug>(
        rows: impl IntoIterator<Item = StoredStateRow<Q, R>>,
        topic: uuid::Uuid,
        partitions: i32,
        partition: i32,
        serve: impl AsyncFn(&ShareCoordinator, &MetadataImage, Q) -> R,
    ) {
        for (index, row) in rows.into_iter().enumerate() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let StateFixture { coordinator, image } = initialized_state()
                .log_dir(dir.path())
                .topic(topic)
                .partitions(partitions)
                .partition(partition)
                .stored_data(true)
                .call()
                .await;
            retain_leadership(&coordinator, row.led).await;
            let response = serve(&coordinator, &image, row.request).await;
            assert2::check!(response == row.expected, "row {index}");
        }
    }
    /// Creates the real `__share_group_state` topic through the active
    /// controller, with its configured shape, and waits until the share
    /// coordinator of `broker` has loaded every partition of it. A topic that
    /// exists already is kept.
    ///
    /// A test must not seed the leadership by hand on a live broker: the
    /// metadata reconcile loop applies the image again at any time, and an
    /// image without the topic drops every led partition.
    pub(crate) async fn lead_share_state_partitions(broker: &Broker) {
        use crate::{
            auto_topic_creation::creatable_topic,
            codes::{NONE, TOPIC_ALREADY_EXISTS},
        };

        let partitions = broker.config.share_coordinator.state_topic_num_partitions;
        let request = krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
            topics: vec![creatable_topic(&broker.config, bootstrap::TOPIC)],
            ..Default::default()
        };
        let response = TopicCreator::new(broker)
            .create_topic_without_principal(request)
            .await
            .expect("the controller answers CreateTopics");
        for row in &response.topics {
            assert2::assert!(
                [NONE, TOPIC_ALREADY_EXISTS].contains(&row.error_code),
                "create __share_group_state: {row:?}"
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                broker
                    .share_coordinator
                    .refresh_leader_partitions(&broker.controller.current_image())
                    .await
                    .finished()
                    .await;
                let mut active = true;
                for partition in 0..partitions {
                    active &= broker
                        .share_coordinator
                        .load_status(PartitionIndex(partition))
                        .await
                        == Some(LoadStatus::Active);
                }
                if active {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every __share_group_state partition loads on this broker");
    }
}
