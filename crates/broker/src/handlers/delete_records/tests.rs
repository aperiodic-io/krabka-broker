//! End-to-end tests for the `DeleteRecords` handler: the ACL denial rows, the
//! unknown-partition row, and the KFC-1 trim bound on a scheduled topic.
//!
//! These drive `handle` against a live broker, so they live apart from the
//! unit tests that each sibling module keeps beside its own helpers.

use {
    super::*,
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::{Broker, BrokerHandle},
        codes,
        config_keys::{
            CLEANUP_POLICY, DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, parse_cleanup_policy,
        },
        coordinator::bootstrap::OFFSETS_TOPIC,
        delivery::test_support::batch_at as delivery_batch_at,
        fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
        freeze::resolve::resolve_topic_freeze,
        handlers::{
            RequestContext, create_topics::handle as create_topics_handle,
            delete_records::test_support::gated_config, fetch::handle as fetch_handle,
            test_support::configured_topic_request,
        },
        heartbeat::fencing::{RegistrationChange, registration_change},
        partition::Partition,
        replica_state::ReplicaState,
        share_coordinator::bootstrap::TOPIC as SHARE_COORDINATOR_TOPIC,
        test_support::{
            DenyAll, KafkaErrorCode, append_partition_batch, broker_registration, encode_request,
            peer, principal, start_broker_no_audit_with,
        },
        txn::bootstrap::TOPIC,
    },
    assert2::{assert, check},
    krabka_ids::{Offset, PartitionIndex},
    krabka_protocol::owned::{
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        delete_records_response::{
            DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
        },
    },
    std::sync::Arc,
};

const VERSION: i16 = 2;

/// A partition to trim and the offset to trim it to.
#[derive(Clone, Copy)]
struct TrimTarget {
    index: PartitionIndex,
    offset: Offset,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TrimRequestSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(&[TrimTarget { index: PartitionIndex(0), offset: Offset(-1) }])]
    partitions: &'a [TrimTarget],
    #[default(RequestTimeoutMillis(5_000))]
    timeout: RequestTimeoutMillis,
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct RequestTimeoutMillis(i32);

#[bon::builder]
fn request(
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = & [TrimTarget { index : PartitionIndex (0) , offset : Offset (- 1) }])]
    partitions: &'_ [TrimTarget],
    #[builder(into, default = RequestTimeoutMillis (5_000))] timeout: RequestTimeoutMillis,
) -> DeleteRecordsRequest {
    let setup = TrimRequestSetup {
        topic,
        partitions,
        timeout,
    };
    DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: setup.topic.into(),
            partitions: setup
                .partitions
                .iter()
                .map(|&TrimTarget { index, offset }| DeleteRecordsPartition {
                    partition_index: index.0,
                    offset: offset.0,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: setup.timeout.0,
        ..Default::default()
    }
}

krabka_macros::wire_codec_helpers! {
    test_context ["admin-client"];
}

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &DeleteRecordsRequest,
    ctx: &RequestContext<'_>,
) -> DeleteRecordsResponse {
    handle(broker, req.clone(), VERSION, ctx)
        .await
        .expect("handle")
}

async fn drive_trim(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    request: DeleteRecordsRequest,
) -> DeleteRecordsResponse {
    drive(broker, &request, ctx).await
}

/// Compare the entire response with independent caller-specified expected rows.
#[bon::builder]
async fn check_trim_response(
    broker: &Broker,
    context: &RequestContext<'_>,
    request: &DeleteRecordsRequest,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = & [PartitionIndex (0)])] indices: &'_ [PartitionIndex],
    #[builder(into, default = Offset (- 1))] low_watermark: Offset,
    #[builder(into, default)] error: KafkaErrorCode,
) {
    let expected = ExpectedTrimRowsSetup {
        topic,
        indices,
        low_watermark,
        error,
    };
    let response = drive(broker, request, context).await;
    assert!(
        response
            == krabka_macros::unthrottled_wire!(DeleteRecordsResponse {
                topics: expected_trim_rows()
                    .topic(expected.topic)
                    .indices(expected.indices)
                    .low_watermark(expected.low_watermark)
                    .error(expected.error)
                    .call(),
            })
    );
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RefusedTrimSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(KafkaErrorCode(codes::POLICY_VIOLATION))]
    error: KafkaErrorCode,
}

#[bon::builder]
async fn check_refused_trim(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default = KafkaErrorCode (codes :: POLICY_VIOLATION))] error: KafkaErrorCode,
) {
    let setup = RefusedTrimSetup { topic, error };
    let response = drive_trim(broker, ctx, request().topic(setup.topic).call()).await;
    assert!(
        response.topics
            == expected_trim_rows()
                .topic(setup.topic)
                .error(setup.error)
                .call(),
        "{response:?}"
    );
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TrimFrontierSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    offset: Offset,
    low_watermark: Offset,
    error: KafkaErrorCode,
    frontier: Offset,
}

#[bon::builder]
async fn check_trim_frontier(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    partition: &Partition,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default)] offset: Offset,
    #[builder(into, default)] low_watermark: Offset,
    #[builder(into, default)] error: KafkaErrorCode,
    #[builder(into, default)] frontier: Offset,
) {
    let setup = TrimFrontierSetup {
        topic,
        offset,
        low_watermark,
        error,
        frontier,
    };
    let response = drive_trim(
        broker,
        ctx,
        request()
            .topic(setup.topic)
            .partitions(&[TrimTarget {
                index: PartitionIndex(0),
                offset: setup.offset,
            }])
            .call(),
    )
    .await;
    check!(
        response.topics
            == expected_trim_rows()
                .topic(setup.topic)
                .low_watermark(setup.low_watermark)
                .error(setup.error)
                .call(),
        "{}",
        setup.topic
    );
    check!(
        partition.log_start_offset() == setup.frontier,
        "{}",
        setup.topic
    );
}

macro_rules! pending_partition_fixture {
    (($part:ident, $before:ident), $handle:expr, $broker:expr, $topic:expr, $context:expr) => {
        topic_holding_a_pending_batch()
            .broker_handle($handle)
            .ctx($context)
            .topic($topic)
            .call()
            .await;
        let $part = $broker
            .partitions
            .get($topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        let $before = $part.log_start_offset();
    };
}

#[tokio::test]
async fn handle_denied_topic_returns_topic_auth_rows() {
    broker_fixture!((broker_handle, _dir, broker), deny_all);
    request_identity!((p, peer), principal("alice"));
    let req = request()
        .topic("secret")
        .partitions(&[
            TrimTarget {
                index: PartitionIndex(0),
                offset: Offset(3),
            },
            TrimTarget {
                index: PartitionIndex(2),
                offset: Offset(-1),
            },
        ])
        .call();

    check_trim_response()
        .broker(&broker)
        .context(&test_context(&p, &peer))
        .request(&req)
        .topic("secret")
        .indices(&[PartitionIndex(0), PartitionIndex(2)])
        .error(codes::TOPIC_AUTHORIZATION_FAILED)
        .call()
        .await;
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unknown_partition_preserves_requested_index() {
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((p, peer), principal("admin"));
    let req = request()
        .topic("missing")
        .partitions(&[TrimTarget {
            index: PartitionIndex(4),
            offset: Offset(0),
        }])
        .call();

    check_trim_response()
        .broker(&broker)
        .context(&test_context(&p, &peer))
        .request(&req)
        .topic("missing")
        .indices(&[PartitionIndex(4)])
        .error(codes::UNKNOWN_TOPIC_OR_PARTITION)
        .call()
        .await;
    broker_handle.shutdown().await;
}

// Activation time of a batch that has long since come due.
const DELIVERED_MS: i64 = 1_700_000_000_000;
// Activation time of a batch that still waits. It sits far enough ahead
// that every clock this test can read calls it pending, so the schedule
// holds without a mock timeline.
const PENDING_MS: i64 = 4_100_000_000_000;

// A two-record batch that activates at `activation_ms`, stamped with the
// epoch the partition writer expects from a leader append.
fn batch_at(activation_ms: i64, leader_epoch: i32) -> krabka_protocol::records::RecordBatch {
    krabka_protocol::records::RecordBatch {
        partition_leader_epoch: leader_epoch,
        ..delivery_batch_at(activation_ms)
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct PendingBatchTopicSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    delivery_mode: Option<&'a str>,
}

/// A topic config key and the value it is set to.
#[derive(Clone, Copy)]
struct ConfigPair<'a> {
    name: &'a str,
    value: &'a str,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ConfiguredPendingBatchSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    configs: &'a [ConfigPair<'a>],
}

// Create `topic` with the given `delivery.mode`, then append one batch
// that has come due and one that has not. Two records per batch puts the
// log end offset at 4, and a scheduled topic's delivery watermark at 2.
#[bon::builder]
async fn topic_holding_a_pending_batch(
    broker_handle: &BrokerHandle,
    ctx: &RequestContext<'_>,
    #[builder(default = "orders")] topic: &'_ str,
    delivery_mode: Option<&'_ str>,
) {
    let setup = PendingBatchTopicSetup {
        topic,
        delivery_mode,
    };
    let PendingBatchTopicSetup {
        topic,
        delivery_mode,
    } = setup;
    let configs: Vec<ConfigPair<'_>> = delivery_mode
        .map(|mode| ConfigPair {
            name: DELIVERY_MODE,
            value: mode,
        })
        .into_iter()
        .collect();
    topic_with_configs_holding_a_pending_batch()
        .broker_handle(broker_handle)
        .ctx(ctx)
        .topic(topic)
        .configs(&configs)
        .call()
        .await;
}

// `topic_holding_a_pending_batch` for a topic created with `configs`. It
// waits until the delivery mode and the cleanup policy reach the partition
// log before it appends.
#[bon::builder]
async fn topic_with_configs_holding_a_pending_batch(
    broker_handle: &BrokerHandle,
    ctx: &RequestContext<'_>,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default)] configs: &'_ [ConfigPair<'_>],
) {
    use krabka_protocol::owned::create_topics_response;

    let setup = ConfiguredPendingBatchSetup { topic, configs };

    let ConfiguredPendingBatchSetup { topic, configs } = setup;
    let broker = broker_handle.broker_arc_for_test();

    let config = |key: &str| {
        configs
            .iter()
            .find(|pair| pair.name == key)
            .map(|pair| pair.value)
    };
    let delivery_mode = config(DELIVERY_MODE);
    let expected_cleanup = config(CLEANUP_POLICY)
        .map_or_else(krabka_log::CleanupPolicy::default, |policy| {
            parse_cleanup_policy(policy).expect("a valid cleanup.policy")
        });
    let version = create_topics_response::MAX_VERSION;
    let pairs: Vec<_> = configs.iter().map(|pair| (pair.name, pair.value)).collect();
    let create = configured_topic_request()
        .topic(topic)
        .configs(&pairs)
        .call();
    let created = create_topics_handle(&broker, create, version, ctx)
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
    broker_handle.wait_until_partition_present(topic, 0).await;

    let expected_policy = if delivery_mode == Some(DELIVERY_MODE_SCHEDULED) {
        krabka_log::DeliveryPolicy::Scheduled
    } else {
        krabka_log::DeliveryPolicy::Immediate
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker_handle
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| {
                    config.delivery_policy == expected_policy
                        && config.cleanup_policy == expected_cleanup
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delivery mode reaches the partition log");

    let part = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    for activation_ms in [DELIVERED_MS, PENDING_MS] {
        part.produce_batch(batch_at(activation_ms, leader_epoch))
            .await
            .expect("append a batch");
    }
}

#[tokio::test]
async fn a_trim_stops_at_the_delivery_watermark_of_a_scheduled_topic() {
    // The `-1` sentinel resolves to the high watermark, which is 4 on both
    // topics: replication is never gated on delivery. The scheduled topic
    // keeps the batch that has not come due.
    let cases = [
        ("delete-records-immediate-delivery", None, 4),
        (
            "delete-records-scheduled-delivery",
            Some(DELIVERY_MODE_SCHEDULED),
            2,
        ),
    ];

    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);

    for (topic, delivery_mode, expected_low_watermark) in cases {
        topic_holding_a_pending_batch()
            .broker_handle(&broker_handle)
            .ctx(&ctx)
            .topic(topic)
            .maybe_delivery_mode(delivery_mode)
            .call()
            .await;

        let resp = drive_trim(
            &broker,
            &test_context(&admin, &peer),
            request().topic(topic).call(),
        )
        .await;

        let expected = krabka_macros::unthrottled_wire!(DeleteRecordsResponse {
            topics: vec![krabka_macros::tagged_wire!(DeleteRecordsTopicResult {
                name: topic.into(),
                partitions: vec![krabka_macros::tagged_wire!(DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: expected_low_watermark,
                    error_code: codes::NONE,
                })],
            })],
        });
        check!(resp == expected, "{topic}");
    }

    broker_handle.shutdown().await;
}

// ── KFC-9: the break-glass gate over a trim ─────────────────────────

#[tokio::test]
async fn a_refused_trim_deletes_nothing() {
    broker_fixture!((broker_handle, _dir, broker), break_glass(gated_config()));
    request_identity!((principal, peer, ctx), principal("admin"), test_context);
    topic_holding_a_pending_batch()
        .broker_handle(&broker_handle)
        .ctx(&ctx)
        .call()
        .await;
    let part = broker
        .partitions
        .get("orders", krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let before = part.log_start_offset();

    let resp = drive_trim(&broker, &test_context(&principal, &peer), request().call()).await;

    let expected = vec![krabka_macros::tagged_wire!(DeleteRecordsTopicResult {
        name: "orders".to_owned(),
        partitions: vec![krabka_macros::tagged_wire!(DeleteRecordsPartitionResult {
            partition_index: 0,
            low_watermark: -1,
            error_code: codes::POLICY_VIOLATION,
        })],
    })];
    assert!(resp.topics == expected, "{resp:?}");
    // The refusal refused: the log start offset did not move.
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

// ── The trims that must not move the deletion frontier ───────────────

/// Independent expected partition rows, preserving the caller's requested index order.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ExpectedTrimRowsSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(&[PartitionIndex(0)])]
    indices: &'a [PartitionIndex],
    #[default(Offset(-1))]
    low_watermark: Offset,
    error: KafkaErrorCode,
}

#[bon::builder]
fn expected_trim_rows(
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = & [PartitionIndex (0)])] indices: &'_ [PartitionIndex],
    #[builder(into, default = Offset (- 1))] low_watermark: Offset,
    #[builder(into, default)] error: KafkaErrorCode,
) -> Vec<DeleteRecordsTopicResult> {
    let setup = ExpectedTrimRowsSetup {
        topic,
        indices,
        low_watermark,
        error,
    };
    vec![krabka_macros::tagged_wire!(DeleteRecordsTopicResult {
        name: setup.topic.to_owned(),
        partitions: setup
            .indices
            .iter()
            .map(
                |index| krabka_macros::tagged_wire!(DeleteRecordsPartitionResult {
                    partition_index: index.0,
                    low_watermark: setup.low_watermark.0,
                    error_code: setup.error.0,
                })
            )
            .collect(),
    })]
}

/// A trim the partition has already satisfied answers success and deletes
/// nothing, and one past the log end is refused outright.
///
/// Both rows are load-bearing. A stale request that answered
/// `OFFSET_OUT_OF_RANGE` would make an ordinary retry look like an error, and
/// a request past the log end that answered success would claim records were
/// deleted that the partition never held.
#[tokio::test]
async fn a_trim_that_deletes_nothing_leaves_the_log_start_alone() {
    // `topic_holding_a_pending_batch` leaves the log start at 0 and the log
    // end at 4, so offset 0 is the retry case and offset 100 is past the end.
    let cases = [
        ("delete-records-stale", 0_i64, 0_i64, codes::NONE),
        (
            "delete-records-beyond-end",
            100,
            -1,
            codes::OFFSET_OUT_OF_RANGE,
        ),
    ];

    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);

    for (topic, offset, low_watermark, error_code) in cases {
        pending_partition_fixture!((part, before), &broker_handle, &broker, topic, &ctx);

        check_trim_frontier()
            .broker(&broker)
            .ctx(&test_context(&admin, &peer))
            .partition(&part)
            .topic(topic)
            .offset(offset)
            .low_watermark(low_watermark)
            .error(error_code)
            .frontier(before)
            .call()
            .await;
    }

    broker_handle.shutdown().await;
}

/// KFC-9: a write freeze over the topic refuses the trim, and the records
/// stay. The freeze answers ahead of every other check the trim makes, so a
/// caller who could otherwise trim still cannot.
#[tokio::test]
async fn a_frozen_topic_refuses_a_trim() {
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);
    let topic = "delete-records-frozen";
    pending_partition_fixture!((part, before), &broker_handle, &broker, topic, &ctx);

    broker_handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1TopicFreeze(
            krabka_metadata::TopicFreezeRecord {
                scope: topic.to_owned(),
                pattern_type: krabka_metadata::PatternType::Literal,
                frozen: true,
                reason: "a compliance hold".to_owned(),
                set_by: "User:carol".to_owned(),
                set_at_ms: 1_770_000_000_000,
                proposal_id: uuid::Uuid::nil(),
                key_id: String::new(),
                signature: Vec::new(),
            },
        ))
        .await
        .expect("the freeze record commits");
    broker_handle
        .wait_for_image(|image| resolve_topic_freeze(image, topic).is_some())
        .await;

    check_refused_trim()
        .broker(&broker)
        .ctx(&test_context(&admin, &peer))
        .topic(topic)
        .call()
        .await;
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

/// Only the leader trims its local segments. A replica that has lost the
/// leadership answers `NOT_LEADER_OR_FOLLOWER` and keeps its records, so the
/// client re-resolves the leader rather than deleting on a stale replica.
#[tokio::test]
async fn a_replica_that_is_not_the_leader_refuses_a_trim() {
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);
    let topic = "delete-records-follower";
    pending_partition_fixture!((part, before), &broker_handle, &broker, topic, &ctx);
    part.current_leader.store(
        broker.config.node_id.0 + 1,
        std::sync::atomic::Ordering::Release,
    );

    check_refused_trim()
        .broker(&broker)
        .ctx(&test_context(&admin, &peer))
        .topic(topic)
        .error(codes::NOT_LEADER_OR_FOLLOWER)
        .call()
        .await;
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

/// Kafka's `Partition.deleteRecordsOnLeader` refuses a trim with
/// `POLICY_VIOLATION` and a low watermark of -1 when the topic compacts and
/// does not delete, and trims a `compact,delete` topic like a `delete` one.
/// Each topic holds four records, so a trim to offset 2 moves the log start.
#[tokio::test]
async fn a_compact_only_topic_refuses_a_trim() {
    let cases = [
        ("delete-records-policy-delete", "delete", 2, codes::NONE, 2),
        (
            "delete-records-policy-compact",
            "compact",
            -1,
            codes::POLICY_VIOLATION,
            0,
        ),
        (
            "delete-records-policy-compact-delete",
            "compact,delete",
            2,
            codes::NONE,
            2,
        ),
    ];
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);

    for (topic, policy, low_watermark, error_code, log_start) in cases {
        topic_with_configs_holding_a_pending_batch()
            .broker_handle(&broker_handle)
            .ctx(&ctx)
            .topic(topic)
            .configs(&[ConfigPair {
                name: CLEANUP_POLICY,
                value: policy,
            }])
            .call()
            .await;
        let part = broker
            .partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");

        let resp = drive_trim(
            &broker,
            &test_context(&admin, &peer),
            request()
                .topic(topic)
                .partitions(&[TrimTarget {
                    index: PartitionIndex(0),
                    offset: Offset(2),
                }])
                .call(),
        )
        .await;

        check!(
            resp.topics
                == expected_trim_rows()
                    .topic(topic)
                    .low_watermark(low_watermark)
                    .error(error_code)
                    .call(),
            "{policy}"
        );
        check!(
            part.log_start_offset() == krabka_log::Offset(log_start),
            "{policy}"
        );
    }
    broker_handle.shutdown().await;
}

/// A partition the metadata holds and this broker does not host answers
/// `NOT_LEADER_OR_FOLLOWER` (Kafka's `ReplicaManager.getPartitionOrError`), so
/// the client refreshes its metadata and retries on the leader. Only a
/// partition the metadata does not hold answers `UNKNOWN_TOPIC_OR_PARTITION`.
#[tokio::test]
async fn a_partition_hosted_elsewhere_answers_not_leader_or_follower() {
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer), principal("admin"));
    let topic = "delete-records-elsewhere";
    let elsewhere = krabka_metadata::NodeId(broker.config.node_id.0 + 1);
    broker
        .controller
        .submit_change(vec![
            krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
                name: topic.to_owned(),
                topic_id: uuid::Uuid::from_u128(0x749),
                partitions: 1,
                replication_factor: 1,
            }),
            krabka_metadata::MetadataRecord::V1Partition(
                crate::handlers::test_support::single_replica_partition(
                    topic,
                    krabka_ids::PartitionIndex(0),
                    elsewhere,
                ),
            ),
        ])
        .await
        .expect("the partition commits");
    broker_handle
        .wait_for_image(|image| image.partition(topic, 0).is_some())
        .await;

    let resp = drive_trim(
        &broker,
        &test_context(&admin, &peer),
        request()
            .topic(topic)
            .partitions(&[
                TrimTarget {
                    index: PartitionIndex(0),
                    offset: Offset(1),
                },
                TrimTarget {
                    index: PartitionIndex(1),
                    offset: Offset(1),
                },
            ])
            .call(),
    )
    .await;

    let expected = vec![krabka_macros::tagged_wire!(DeleteRecordsTopicResult {
        name: topic.to_owned(),
        partitions: vec![
            krabka_macros::tagged_wire!(DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: -1,
                error_code: codes::NOT_LEADER_OR_FOLLOWER,
            }),
            krabka_macros::tagged_wire!(DeleteRecordsPartitionResult {
                partition_index: 1,
                low_watermark: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            }),
        ],
    })];
    assert!(resp.topics == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// Kafka refuses `DeleteRecords` on every partition of an internal topic with
/// `INVALID_TOPIC_EXCEPTION` and a low watermark of -1, and trims an ordinary
/// topic. Each topic holds two records first, so a trim to the high watermark
/// would move the log start.
#[tokio::test]
async fn an_internal_topic_refuses_a_trim() {
    broker_fixture!((broker_handle, _dir, broker), allow_all);
    request_identity!((admin, peer, ctx), principal("admin"), test_context);

    broker_handle.wait_until_group_coordinator_ready().await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    broker_handle.wait_until_share_coordinator_ready().await;
    topic_holding_a_pending_batch()
        .broker_handle(&broker_handle)
        .ctx(&ctx)
        .call()
        .await;

    let cases = [
        (OFFSETS_TOPIC, -1_i64, codes::INVALID_TOPIC_EXCEPTION, 0_i64),
        (TOPIC, -1, codes::INVALID_TOPIC_EXCEPTION, 0),
        (
            SHARE_COORDINATOR_TOPIC,
            -1,
            codes::INVALID_TOPIC_EXCEPTION,
            0,
        ),
        ("orders", 4, codes::NONE, 4),
    ];
    for (topic, low_watermark, error_code, log_start) in cases {
        broker_handle
            .wait_until_local_partition_leader(topic, 0, broker.config.node_id)
            .await;
        let part = broker
            .partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        if topic != "orders" {
            let leader_epoch = part
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire);
            part.produce_batch(batch_at(DELIVERED_MS, leader_epoch))
                .await
                .expect("append a batch");
        }

        check_trim_frontier()
            .broker(&broker)
            .ctx(&test_context(&admin, &peer))
            .partition(&part)
            .topic(topic)
            .offset(-1)
            .low_watermark(low_watermark)
            .error(error_code)
            .frontier(log_start)
            .call()
            .await;
    }

    // A partition of an internal topic that the metadata does not hold is
    // unknown, as `KafkaApis` answers it before the internal-topic check.
    let missing = 10_000;
    let resp = drive_trim(
        &broker,
        &test_context(&admin, &peer),
        request()
            .topic(OFFSETS_TOPIC)
            .partitions(&[TrimTarget {
                index: PartitionIndex(missing),
                offset: Offset(-1),
            }])
            .call(),
    )
    .await;
    check!(
        resp.topics[0].partitions
            == vec![error_partition_result(
                missing,
                codes::UNKNOWN_TOPIC_OR_PARTITION
            )]
    );

    broker_handle.shutdown().await;
}

// ── KIP-107: the trim reaches the followers ─────────────────────────

/// The follower that [`replicated_topic`] assigns beside this broker.
const FOLLOWER: u64 = 2;

/// Records [`replicated_topic`] appends to the leader log.
const APPENDED: i64 = 10;

/// Create `topic` with one partition that this broker leads and that the live,
/// registered broker [`FOLLOWER`] follows, append [`APPENDED`] records, and
/// fetch them as the follower so the high watermark reaches the log end.
async fn replicated_topic(broker_handle: &BrokerHandle, topic: &str) -> Arc<Partition> {
    use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};

    let broker = broker_handle.broker_arc_for_test();
    register_follower(broker_handle).await;
    broker.liveness.record_heartbeat(FOLLOWER).await;
    let replicas = vec![krabka_audit::NodeId(1), krabka_audit::NodeId(FOLLOWER)];
    for record in [
        MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::new_v4(),
            partitions: 1,
            replication_factor: 2,
        }),
        MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); 2],
            partition_epoch: 0,
        }),
    ] {
        broker_handle
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit topic metadata");
    }
    let partition = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(partition) = broker.partitions.get(topic, krabka_ids::PartitionIndex(0))
                && partition
                    .replica_state
                    .lock()
                    .await
                    .isr
                    .contains(&krabka_raft::NodeId(FOLLOWER))
            {
                return partition;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker leads the partition with the follower in the ISR");
    let mut batch = krabka_protocol::records::RecordBatch {
        last_offset_delta: i32::try_from(APPENDED - 1).expect("delta"),
        records: (0..APPENDED)
            .map(|offset| krabka_protocol::records::Record {
                offset_delta: i32::try_from(offset).expect("delta"),
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    append_partition_batch(&partition, &mut batch);
    follower_fetch(&broker, topic, APPENDED, 0).await;
    assert!(partition.high_watermark().await == krabka_log::Offset(APPENDED));
    partition
}

/// Register [`FOLLOWER`] as a broker in the controller's image.
async fn register_follower(broker_handle: &BrokerHandle) {
    broker_handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: -1,
                ..broker_registration(krabka_raft::NodeId(FOLLOWER))
            },
        ))
        .await
        .expect("register the follower");
}

/// Fence [`FOLLOWER`] the way the controller does: its heartbeat session is
/// fenced, and its registration says so.
async fn fence_follower(broker_handle: &BrokerHandle) {
    let broker = broker_handle.broker_arc_for_test();
    broker.liveness.apply_fencing(FOLLOWER, true, true).await;
    let fence = registration_change(
        &broker.controller.current_image(),
        krabka_raft::NodeId(FOLLOWER),
        RegistrationChange::FENCE,
    )
    .expect("the follower is registered and unfenced");
    broker_handle
        .submit_metadata_record_for_test(fence)
        .await
        .expect("fence the follower");
}

/// One Fetch from [`FOLLOWER`] at `fetch_offset`, reporting `log_start_offset`
/// as the follower's own log start.
async fn follower_fetch(broker: &Broker, topic: &str, fetch_offset: i64, log_start_offset: i64) {
    use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

    const VERSION: i16 = 12;
    let request = FetchRequest {
        replica_id: i32::try_from(FOLLOWER).expect("replica id"),
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1_048_576,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                log_start_offset,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    request_identity!(
        (replicator, peer, ctx),
        principal("replicator"),
        client_id = "replica-2"
    );
    let (response, _) = fetch_handle(broker, VERSION, 1, &encode_request(&request, VERSION), &ctx)
        .await
        .expect("follower fetch");
    assert!(
        response.responses[0].partitions[0].error_code == codes::NONE,
        "{response:?}"
    );
}

/// The row a `DeleteRecords` of `topic-0` to `offset` answers, with
/// `timeout_ms`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DeleteToSetup<'a> {
    #[default("delete-records-followers")]
    topic: &'a str,
    offset: Offset,
    #[default(RequestTimeoutMillis(200))]
    timeout: RequestTimeoutMillis,
}

#[bon::builder]
async fn delete_to(
    broker: &Broker,
    #[builder(default = "delete-records-followers")] topic: &'_ str,
    #[builder(into, default)] offset: Offset,
    #[builder(into, default = RequestTimeoutMillis (200))] timeout: RequestTimeoutMillis,
) -> Vec<DeleteRecordsTopicResult> {
    let setup = DeleteToSetup {
        topic,
        offset,
        timeout,
    };
    request_identity!((admin, peer, ctx), principal("admin"), test_context);
    let request = request()
        .topic(setup.topic)
        .partitions(&[TrimTarget {
            index: PartitionIndex(0),
            offset: setup.offset,
        }])
        .timeout(setup.timeout)
        .call();
    drive(broker, &request, &ctx).await.topics
}

/// Kafka's `DeleteRecords` purgatory (#746): a row answers once the lowest log
/// start of the leader and every live follower reaches the trim point. A
/// follower that has not reported it holds the row until `timeout_ms`, which
/// answers `REQUEST_TIMED_OUT` with the low watermark the trim saw. A follower
/// on a fenced broker is not live and does not hold the row.
#[tokio::test]
async fn a_trim_waits_for_every_live_follower_to_reach_the_trim_point() {
    let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        // The follower never fetches on its own. Keep it in the ISR and alive
        // for the whole test, so only the fetches below move its state.
        cfg.replica_lag_time_max = krabka_units::secs(600);
        cfg.heartbeat_timeout = krabka_units::secs(600);
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let topic = "delete-records-followers";
    let partition = replicated_topic(&broker_handle, topic).await;

    // The follower still reports log start 0, so the row times out with that
    // low watermark, and the leader log is trimmed all the same.
    let timed_out = delete_to()
        .broker(&broker)
        .topic(topic)
        .offset(4)
        .call()
        .await;
    check!(
        timed_out
            == expected_trim_rows()
                .topic(topic)
                .low_watermark(0)
                .error(codes::REQUEST_TIMED_OUT)
                .call()
    );
    check!(partition.log_start_offset() == krabka_log::Offset(4));

    // The follower catches up while a second request waits.
    let waiting = {
        let broker = broker.clone();
        tokio::spawn(async move {
            delete_to()
                .broker(&broker)
                .topic(topic)
                .offset(6)
                .timeout(10_000)
                .call()
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    check!(!waiting.is_finished(), "the row waits for the follower");
    follower_fetch(&broker, topic, APPENDED, 6).await;
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("the row completes once the follower reports")
        .expect("delete task");
    check!(completed == expected_trim_rows().topic(topic).low_watermark(6).call());

    // A retry at a point every replica already passed answers at once.
    let retry = delete_to()
        .broker(&broker)
        .topic(topic)
        .offset(5)
        .timeout(0)
        .call()
        .await;
    check!(retry == expected_trim_rows().topic(topic).low_watermark(6).call());

    // A leadership this broker has published but not installed yet (no
    // replica set, no follower progress) still waits for the follower the
    // metadata image assigns, whose log start it does not know.
    {
        let mut state = partition.replica_state.lock().await;
        let high_watermark = state.hw;
        *state = ReplicaState::new();
        state.hw = high_watermark;
    }
    let not_installed = delete_to()
        .broker(&broker)
        .topic(topic)
        .offset(7)
        .call()
        .await;
    check!(
        not_installed
            == expected_trim_rows()
                .topic(topic)
                .error(codes::REQUEST_TIMED_OUT)
                .call()
    );
    // The follower's next fetch reports its progress to the new leadership.
    follower_fetch(&broker, topic, APPENDED, 7).await;

    // A fenced follower is not live, so its old log start does not count.
    fence_follower(&broker_handle).await;
    let fenced = delete_to()
        .broker(&broker)
        .topic(topic)
        .offset(8)
        .timeout(10_000)
        .call()
        .await;
    check!(fenced == expected_trim_rows().topic(topic).low_watermark(8).call());

    broker_handle.shutdown().await;
}
