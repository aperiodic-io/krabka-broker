//! `InitializeShareGroupState` (`api_key=83`). It seeds the durable share state
//! for each `(group, topic, partition)` at the requested `state_epoch` and
//! `start_offset`. It gates every partition on local leadership of that
//! partition's `__share_group_state` partition.

use {
    crate::share_coordinator::coordinator::ShareCoordinator,
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        initialize_share_group_state_request::InitializeShareGroupStateRequest,
        initialize_share_group_state_response::{
            InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult,
        },
    },
};

krabka_macros::share_state_handler!(
    InitializeShareGroupStateRequest,
    InitializeShareGroupStateResponse,
    InitializeStateResult,
    PartitionResult,
    initialize_state
);

/// Initializes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.initializeState` does.
///
/// An empty group id or an empty topic list gets a response with no results.
async fn initialize_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: InitializeShareGroupStateRequest,
) -> InitializeShareGroupStateResponse {
    if req.group_id.is_empty() || req.topics.is_empty() {
        return InitializeShareGroupStateResponse::default();
    }
    let results = krabka_macros::share_state_results!(
        req,
        InitializeStateResult,
        |group_id, topic_id, pd| {
            let result = coordinator
                .initialize(
                    image,
                    group_id,
                    topic_id,
                    pd.partition,
                    pd.state_epoch,
                    Offset(pd.start_offset),
                )
                .await;
            let verdict = super::operation_result(result, "initialize");
            PartitionResult {
                partition: pd.partition,
                error_code: verdict.code,
                error_message: verdict.message,
                ..Default::default()
            }
        }
    );

    InitializeShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            share_coordinator::{
                config::ShareCoordinatorConfig,
                coordinator::{
                    ShareStateSummary,
                    test_support::{
                        Logged, NOW_MS, image_with_topic, logged_records, logged_since,
                    },
                },
                persistence::ShareSnapshotValue,
            },
            test_support::KafkaErrorCode,
        },
        assert2::check,
        krabka_protocol::{
            owned::initialize_share_group_state_request::{InitializeStateData, PartitionData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([32; 16]);
    const UNKNOWN_TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);

    use krabka_ids::PartitionIndex;

    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct InitializationEpoch(i32);
    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct SnapshotEpoch(i32);
    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct DeliveryCompleteCount(i32);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct InitializationSetup<'a> {
        #[default("g")]
        group: &'a str,
        #[default(TOPIC)]
        topic: uuid::Uuid,
        partition: PartitionIndex,
        #[default(InitializationEpoch(1))]
        epoch: InitializationEpoch,
        offset: Offset,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct SnapshotSetup {
        snapshot_epoch: SnapshotEpoch,
        #[default(InitializationEpoch(1))]
        state_epoch: InitializationEpoch,
        offset: Offset,
        delivery_complete: DeliveryCompleteCount,
    }

    #[derive(Clone, Copy)]
    enum StateLeadership {
        Led,
        Follower,
    }
    #[derive(Clone, Copy)]
    enum InitializationRules {
        Kafka431,
        Trunk,
    }

    #[bon::builder]
    fn request(
        #[builder(default = "g")] group: &'_ str,
        #[builder(default = TOPIC)] topic: uuid::Uuid,
        #[builder(into, default)] partition: PartitionIndex,
        #[builder(into, default = InitializationEpoch (1))] epoch: InitializationEpoch,
        #[builder(into, default)] offset: Offset,
    ) -> InitializeShareGroupStateRequest {
        let setup = InitializationSetup {
            group,
            topic,
            partition,
            epoch,
            offset,
        };
        InitializeShareGroupStateRequest {
            group_id: setup.group.into(),
            topics: vec![InitializeStateData {
                topic_id: ProtoUuid(*setup.topic.as_bytes()),
                partitions: vec![PartitionData {
                    partition: setup.partition.0,
                    state_epoch: setup.epoch.0,
                    start_offset: setup.offset.0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    use crate::share_coordinator::handlers::test_support::StateResponseSetup;

    fn response(setup: StateResponseSetup<'_>) -> InitializeShareGroupStateResponse {
        setup.response()
    }

    #[bon::builder]
    fn unknown_topic_response(
        #[builder(into, default)] partition: PartitionIndex,
        #[builder(default = TOPIC)] topic: uuid::Uuid,
    ) -> InitializeShareGroupStateResponse {
        response(StateResponseSetup {
            topic,
            partition,
            code: KafkaErrorCode(codes::UNKNOWN_TOPIC_OR_PARTITION),
            message: Some("This server does not host this topic-partition."),
        })
    }

    #[bon::builder]
    fn snapshot(
        #[builder(into, default)] snapshot_epoch: SnapshotEpoch,
        #[builder(into, default = InitializationEpoch (1))] state_epoch: InitializationEpoch,
        #[builder(into, default)] offset: Offset,
        #[builder(into, default)] delivery_complete: DeliveryCompleteCount,
    ) -> Logged {
        let setup = SnapshotSetup {
            snapshot_epoch,
            state_epoch,
            offset,
            delivery_complete,
        };
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: setup.snapshot_epoch.0,
            state_epoch: setup.state_epoch.0,
            leader_epoch: 0,
            start_offset: setup.offset,
            delivery_complete_count: setup.delivery_complete.0,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![],
        })
    }

    /// One `InitializeShareGroupState` row, as Kafka's
    /// `ShareCoordinatorShard.initializeState` answers it.
    struct Row {
        name: &'static str,
        leadership: StateLeadership,
        request: InitializeShareGroupStateRequest,
        response: InitializeShareGroupStateResponse,
        /// The summary of the requested key after the request.
        summary: Option<ShareStateSummary>,
        /// The records the request appended to the state partition of the key.
        appended: Vec<Logged>,
    }

    /// The rows under Kafka 4.3.1's rules, or under Kafka trunk's when using trunk rules.
    fn rows(rules: InitializationRules) -> Vec<Row> {
        let trunk = matches!(rules, InitializationRules::Trunk);
        let fenced = "The coordinator rejected the request because the state epoch did not match.";
        vec![
            // Trunk takes a repeat as a no-op; 4.3.1 writes a new snapshot.
            Row {
                name: "an equal epoch and start offset",
                leadership: StateLeadership::Led,
                request: request().epoch(5).offset(10).call(),
                response: response(StateResponseSetup::for_topic(TOPIC)),
                summary: Some((5, 0, Offset(10), 0)),
                appended: if trunk {
                    vec![]
                } else {
                    vec![
                        snapshot()
                            .snapshot_epoch(1)
                            .state_epoch(5)
                            .offset(10)
                            .call(),
                    ]
                },
            },
            Row {
                name: "an equal epoch with a new start offset writes the next snapshot",
                leadership: StateLeadership::Led,
                request: request().epoch(5).offset(20).call(),
                response: response(StateResponseSetup::for_topic(TOPIC)),
                summary: Some((5, 0, Offset(20), 0)),
                appended: vec![
                    snapshot()
                        .snapshot_epoch(1)
                        .state_epoch(5)
                        .offset(20)
                        .call(),
                ],
            },
            Row {
                name: "an older epoch is fenced",
                leadership: StateLeadership::Led,
                request: request().epoch(4).offset(10).call(),
                response: response(StateResponseSetup {
                    code: KafkaErrorCode(codes::FENCED_STATE_EPOCH),
                    message: Some(fenced),
                    ..StateResponseSetup::for_topic(TOPIC)
                }),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "a new key at an uninitialized start offset",
                leadership: StateLeadership::Led,
                request: request().partition(1).offset(-1).call(),
                response: response(StateResponseSetup {
                    partition: PartitionIndex(1),
                    ..StateResponseSetup::for_topic(TOPIC)
                }),
                summary: Some((1, 0, Offset(-1), -1)),
                appended: vec![snapshot().offset(-1).delivery_complete(-1).call()],
            },
            Row {
                name: "a negative partition",
                leadership: StateLeadership::Led,
                request: request().partition(-1).call(),
                response: response(StateResponseSetup {
                    partition: PartitionIndex(-1),
                    code: KafkaErrorCode(codes::INVALID_REQUEST),
                    message: Some("The partition id cannot be a negative number."),
                    ..StateResponseSetup::for_topic(TOPIC)
                }),
                summary: None,
                appended: vec![],
            },
            // Trunk refuses a negative state epoch; 4.3.1 reads -1 as "not
            // supplied", skips the fence and writes the snapshot.
            if trunk {
                Row {
                    name: "a negative state epoch",
                    leadership: StateLeadership::Led,
                    request: request().epoch(-1).call(),
                    response: response(StateResponseSetup {
                        code: KafkaErrorCode(codes::INVALID_REQUEST),
                        message: Some("The state epoch cannot be a negative number."),
                        ..StateResponseSetup::for_topic(TOPIC)
                    }),
                    summary: Some((5, 0, Offset(10), 0)),
                    appended: vec![],
                }
            } else {
                Row {
                    name: "a state epoch of -1",
                    leadership: StateLeadership::Led,
                    request: request().epoch(-1).call(),
                    response: response(StateResponseSetup::for_topic(TOPIC)),
                    summary: Some((-1, 0, Offset(0), 0)),
                    appended: vec![snapshot().snapshot_epoch(1).state_epoch(-1).call()],
                }
            },
            Row {
                name: "a partition past the partition count",
                leadership: StateLeadership::Led,
                request: request().partition(3).call(),
                response: unknown_topic_response().partition(3).call(),
                summary: None,
                appended: vec![],
            },
            Row {
                name: "a topic id the image does not hold",
                leadership: StateLeadership::Led,
                request: request().topic(UNKNOWN_TOPIC).call(),
                response: unknown_topic_response().topic(UNKNOWN_TOPIC).call(),
                summary: None,
                appended: vec![],
            },
            Row {
                name: "an empty group id",
                leadership: StateLeadership::Led,
                request: request().group("").epoch(6).call(),
                response: InitializeShareGroupStateResponse::default(),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "a state partition this broker does not lead",
                leadership: StateLeadership::Follower,
                request: request().epoch(6).call(),
                response: response(StateResponseSetup {
                    code: KafkaErrorCode(codes::NOT_COORDINATOR),
                    message: Some(
                        "Unable to initialize share group state: This is not the correct coordinator.",
                    ),
                    ..StateResponseSetup::for_topic(TOPIC)
                }),
                summary: None,
                appended: vec![],
            },
        ]
    }

    /// The whole response, the stored summary, and the appended records for
    /// each request, over a stored state of epoch 5 and start offset 10 on
    /// partition 0 of a three-partition topic.
    #[tokio::test]
    async fn initialize_state_answers_as_kafka() {
        for (rules, row) in [InitializationRules::Kafka431, InitializationRules::Trunk]
            .into_iter()
            .flat_map(|rules| rows(rules).into_iter().map(move |row| (rules, row)))
        {
            let trunk = matches!(rules, InitializationRules::Trunk);
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator =
                super::super::test_support::coordinator_with(dir.path(), ShareCoordinatorConfig {
                    trunk_rules: trunk,
                    ..ShareCoordinatorConfig::default()
                });
            let image = image_with_topic(TOPIC, 3);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize(&image, "g", TOPIC, 0, 5, Offset(10))
                .await
                .expect("seed state");
            let topic = row.request.topics[0].topic_id;
            let partition = row.request.topics[0].partitions[0].partition;
            // An empty group id touches no key; watch the seeded key's group.
            let key_group = if row.request.group_id.is_empty() {
                "g".to_owned()
            } else {
                row.request.group_id.clone()
            };
            let topic_id = uuid::Uuid::from_bytes(topic.0);
            let state_partition = coordinator.state_partition_for(&key_group, &topic_id, partition);
            let before = logged_records(&coordinator, state_partition).len();
            if matches!(row.leadership, StateLeadership::Follower) {
                coordinator
                    .refresh_leader_partitions(&MetadataImage::default())
                    .await
                    .finished()
                    .await;
            }

            let resp = initialize_state(&coordinator, &image, row.request).await;
            check!(resp == row.response, "{}, trunk {trunk}", row.name);
            let appended = logged_since(&coordinator, state_partition, before);
            check!(appended == row.appended, "{}, trunk {trunk}", row.name);
            let summary = coordinator
                .read_summary(&key_group, topic_id, partition)
                .await
                .ok()
                .flatten();
            check!(summary == row.summary, "{}, trunk {trunk}", row.name);
        }
    }

    /// An empty topic list gets a response with no results.
    #[tokio::test]
    async fn empty_topics_get_no_results() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::coordinator(dir.path());
        let req = InitializeShareGroupStateRequest {
            group_id: "g".into(),
            ..Default::default()
        };
        let resp = initialize_state(&coordinator, &image_with_topic(TOPIC, 3), req).await;
        check!(resp == InitializeShareGroupStateResponse::default());
    }
}
