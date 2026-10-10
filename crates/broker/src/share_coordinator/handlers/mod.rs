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

use crate::{broker::Broker, handlers::RequestContext};

/// Whether a request has no topic rows or contains an empty partition row.
macro_rules! empty_partition_data {
    ($request:ident) => {
        $request.topics.is_empty()
            || $request
                .topics
                .iter()
                .any(|topic| topic.partitions.is_empty())
    };
}
use empty_partition_data;

/// The code and Kafka message for a completed share-state operation.
fn operation_result(
    result: Result<(), super::coordinator::ShareStateError>,
    operation: &str,
) -> (i16, Option<String>) {
    match result {
        Ok(()) => (crate::codes::NONE, None),
        Err(error) => (error.code(), Some(error.row_message(operation))),
    }
}

/// Kafka's message for `CLUSTER_AUTHORIZATION_FAILED`, which
/// `toGlobalErrorResponse` puts on every partition row.
const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

/// Whether the authorizer denies `ClusterAction` on the cluster to the
/// principal of `ctx`.
fn cluster_action_denied(broker: &Broker, ctx: &RequestContext<'_>) -> bool {
    crate::handlers::cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    )
}

/// Builds Kafka's `toGlobalErrorResponse` for a share-state request: one
/// result row for each requested topic, and one partition row with
/// `CLUSTER_AUTHORIZATION_FAILED` and Kafka's message for each requested
/// partition.
macro_rules! cluster_authorization_failed {
    ($request:expr, $response:ident, $result:ident, $partition:ident) => {
        $response {
            results: $request
                .topics
                .iter()
                .map(|topic| $result {
                    topic_id: topic.topic_id,
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| $partition {
                            partition: partition.partition,
                            error_code: $crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                            error_message: Some(
                                super::CLUSTER_AUTHORIZATION_FAILED_MESSAGE.to_string(),
                            ),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    };
}
use cluster_authorization_failed;

/// The common ACL gate and metadata snapshot of the share-state RPCs.
macro_rules! share_state_handler {
    ($request:ident, $response:ident, $result:ident, $partition:ident, $serve:ident) => {
        /// Checks `ClusterAction` on the cluster, then serves the request.
        ///
        #[doc = concat!("Kafka's `KafkaApis` answers a denied principal with `", stringify!($response), ".toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on every requested partition, and the share coordinator does not run.")]
        pub(crate) async fn handle(
            broker: &$crate::broker::Broker,
            req: $request,
            _version: i16,
            ctx: &$crate::handlers::RequestContext<'_>,
        ) -> Result<$response, $crate::error::BrokerError> {
            Ok(if super::cluster_action_denied(broker, ctx) {
                super::cluster_authorization_failed!(req, $response, $result, $partition)
            } else {
                $serve(
                    &broker.share_coordinator,
                    &broker.controller.current_image(),
                    req,
                )
                .await
            })
        }
    };
}
use share_state_handler;

/// Runs each topic's partition operations together and keeps request order in the results.
/// Each operation waits for its own records to commit, as Kafka's `ShareCoordinatorService` does.
macro_rules! state_results {
    ($request:expr, $result:ident, |$group:ident, $topic:ident, $partition:ident| $body:block) => {{
        let request = $request;
        let $group = request.group_id.as_str();
        futures_util::future::join_all(request.topics.into_iter().map(|topic| async move {
            let $topic = uuid::Uuid::from_bytes(topic.topic_id.0);
            let partitions = futures_util::future::join_all(
                topic.partitions.into_iter().map(|$partition| async move $body),
            )
            .await;
            $result {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            }
        }))
        .await
    }};
}
use state_results;

#[cfg(test)]
pub(crate) mod test_support {
    use std::{path::Path, sync::Arc};

    use krabka_ids::{NodeId, PartitionIndex};
    use krabka_log::Offset;

    use crate::{
        broker::Broker,
        partition_registry::PartitionRegistry,
        share_coordinator::{
            bootstrap,
            config::ShareCoordinatorConfig,
            coordinator::{
                ShareCoordinator,
                test_support::{
                    DeliveryAttemptCount, FixtureDeliveryState, StateBatchSetup, state_batch,
                },
            },
            persistence::StateBatch,
        },
    };

    pub(crate) fn batch(bounds: std::ops::RangeInclusive<Offset>) -> StateBatch {
        state_batch(StateBatchSetup {
            bounds,
            delivery: FixtureDeliveryState::Acknowledged,
            attempts: DeliveryAttemptCount(3),
        })
    }

    pub(crate) fn open_all_state_partitions(
        registry: &PartitionRegistry,
        log_dir: &Path,
        num_partitions: i32,
    ) {
        for partition in 0..num_partitions {
            crate::share_coordinator::coordinator::test_support::open_state_partition(
                registry, log_dir, partition,
            );
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
            crate::share_coordinator::coordinator::test_support::manual_clock(),
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
    }

    /// A led coordinator with partition state at epoch 17 and start offset 90.
    pub(crate) async fn initialized_state(
        log_dir: &Path,
        setup: StateFixtureSetup,
    ) -> (Arc<ShareCoordinator>, krabka_metadata::MetadataImage) {
        let StateFixtureSetup {
            topic,
            partitions,
            partition,
        } = setup;
        let coordinator = coordinator(log_dir);
        let image = crate::share_coordinator::coordinator::test_support::image_with_topic(
            topic,
            partitions.0,
        );
        coordinator.lead_all_partitions_for_test().await;
        coordinator
            .initialize(&image, "share-group", topic, partition.0, 17, Offset(90))
            .await
            .expect("initialize state");
        (coordinator, image)
    }

    /// Stored read fixtures: leader epoch 3, start offset 101 and one terminal batch.
    pub(crate) async fn stored_state(
        log_dir: &Path,
        setup: StateFixtureSetup,
    ) -> (Arc<ShareCoordinator>, krabka_metadata::MetadataImage) {
        let StateFixtureSetup {
            topic, partition, ..
        } = setup;
        let (coordinator, image) = initialized_state(log_dir, setup).await;
        coordinator
            .read(&image, "share-group", topic, partition.0, 3)
            .await
            .expect("raise the stored leader epoch");
        coordinator
            .write(
                &image,
                "share-group",
                topic,
                partition.0,
                crate::share_coordinator::coordinator::test_support::share_write(
                    (17, 3),
                    (101, 9),
                    vec![batch(Offset(101)..=Offset(105))],
                ),
            )
            .await
            .expect("write state");
        (coordinator, image)
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

    macro_rules! response_fixture {
        ($response:ident, $result:ident, $partition:ident, $topic:expr) => {
            fn response(partitions: Vec<$partition>) -> $response {
                super::super::test_support::response_fixture!(@value $response, $result, $topic, partitions)
            }
        };
        ($response:ident, $result:ident, $partition:ident; keyed) => {
            #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
            struct StateResponseSetup<'a> {
                #[default(TOPIC)]
                topic: uuid::Uuid,
                partition: krabka_ids::PartitionIndex,
                #[default($crate::test_support::KafkaErrorCode($crate::codes::NONE))]
                code: $crate::test_support::KafkaErrorCode,
                message: Option<&'a str>,
            }

            fn response(setup: StateResponseSetup<'_>) -> $response {
                super::super::test_support::response_fixture!(@value $response, $result,
                    krabka_protocol::primitives::uuid::Uuid(*setup.topic.as_bytes()),
                    vec![$partition {
                        partition: setup.partition.0,
                        error_code: setup.code.0,
                        error_message: setup.message.map(str::to_owned),
                        unknown_tagged_fields: krabka_protocol::tagged_fields::UnknownTaggedFields(vec![]),
                    }]
                )
            }
        };
        (@value $response:ident, $result:ident, $topic:expr, $partitions:expr) => {
            $response {
                results: vec![$result {
                    topic_id: $topic,
                    partitions: $partitions,
                    unknown_tagged_fields: krabka_protocol::tagged_fields::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: krabka_protocol::tagged_fields::UnknownTaggedFields(vec![]),
            }
        };
    }
    pub(crate) use response_fixture;

    /// Request fixtures share a singleton topic while keeping their partition fields explicit.
    macro_rules! request_fixture {
        ($request:ident, $topic:ident, $partition:ident;
            fn request($group:ident: &str, [$($topic_id:ident: uuid::Uuid)?], $parts:ident: &[$row:ty]);
            topic $wire_topic:expr; |$pattern:pat_param| $fields:block) => {
            fn request($group: &str $(, $topic_id: uuid::Uuid)?, $parts: &[$row]) -> $request {
                $request {
                    group_id: $group.into(),
                    topics: vec![$topic {
                        topic_id: $wire_topic,
                        partitions: $parts.iter().map(|&$pattern| $fields).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            }
        };
    }
    pub(crate) use request_fixture;

    /// A refused row with its independently supplied code and message.
    macro_rules! error_row_fixture {
        ($partition:ident) => {
            fn error_row(partition: i32, error_code: i16, message: &str) -> $partition {
                $partition {
                    partition,
                    error_code,
                    error_message: Some(message.to_owned()),
                    ..Default::default()
                }
            }
        };
    }
    pub(crate) use error_row_fixture;

    /// A request with a group id but no topics, distinct from an empty partition list.
    macro_rules! no_topics {
        ($request:ident, $group:expr) => {
            $request {
                group_id: $group.into(),
                ..Default::default()
            }
        };
    }
    pub(crate) use no_topics;

    /// Check each read operation against the whole independently supplied expected response.
    macro_rules! stored_response_rows {
        ($rows:expr, $topic:expr, ($partitions:expr, $partition:expr), $serve:ident) => {
            for (index, (led, req, expected)) in $rows.into_iter().enumerate() {
                let dir = tempfile::TempDir::new().expect("tempdir");
                let (coordinator, image) = super::super::test_support::stored_state(
                    dir.path(),
                    $crate::share_coordinator::handlers::test_support::StateFixtureSetup {
                        topic: $topic,
                        partitions: $crate::test_support::PartitionCount($partitions),
                        partition: krabka_ids::PartitionIndex($partition),
                    },
                )
                .await;
                super::super::test_support::retain_leadership(&coordinator, led).await;
                let resp = $serve(&coordinator, &image, req).await;
                assert2::check!(resp == expected, "row {index}");
            }
        };
    }
    pub(crate) use stored_response_rows;

    /// Creates the real `__share_group_state` topic through the active
    /// controller, with its configured shape, and waits until the share
    /// coordinator of `broker` has loaded every partition of it. A topic that
    /// exists already is kept.
    ///
    /// A test must not seed the leadership by hand on a live broker: the
    /// metadata reconcile loop applies the image again at any time, and an
    /// image without the topic drops every led partition.
    pub(crate) async fn lead_share_state_partitions(broker: &Broker) {
        let partitions = broker.config.share_coordinator.state_topic_num_partitions;
        let request = krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
            topics: vec![crate::auto_topic_creation::creatable_topic(
                &broker.config,
                bootstrap::TOPIC,
            )],
            ..Default::default()
        };
        let response = crate::topic_creator::TopicCreator::new(broker)
            .create_topic_without_principal(request)
            .await
            .expect("the controller answers CreateTopics");
        for row in &response.topics {
            assert2::assert!(
                [crate::codes::NONE, crate::codes::TOPIC_ALREADY_EXISTS].contains(&row.error_code),
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
                        == Some(crate::share_coordinator::coordinator::LoadStatus::Active);
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
