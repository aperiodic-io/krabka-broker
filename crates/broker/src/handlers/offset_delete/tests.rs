//! Handler tests for `OffsetDelete`: the group-level refusals, Kafka's check
//! order and the subscription guard, each as a whole response.

use std::{collections::HashMap, sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use krabka_protocol::owned::offset_delete_response::{self, OffsetDeleteResponse};

use super::{
    handle,
    test_support::{expected_row, expected_topic, req_with_topics},
};
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    coordinator::unified::{
        classic_state::{ClassicGroup, Member},
        consumer_state::{
            GroupState as ConsumerGroup, ResolvedRegularExpression, test_support::member,
        },
        group::{CoordinatorGroup, GroupKind},
    },
    test_support::{DenyAll, peer},
};

async fn create_topic(broker: &BrokerHandle, name: &str) {
    crate::handlers::test_support::create_topic(
        broker,
        crate::handlers::test_support::ClientTopicSetup {
            client_id: "offset-delete-test",
            name,
            ..Default::default()
        },
    )
    .await;
}

use crate::coordinator::unified::actor::test_support::subscription_blob as subscription;

/// A `Stable` classic group with one member whose selected protocol carries
/// `metadata`.
fn classic_group(group_id: &str, protocol_type: &str, metadata: Bytes) -> CoordinatorGroup {
    let mut state = ClassicGroup::new(group_id);
    state.protocol_type = Some(protocol_type.into());
    state.add_member(Member::new(
        "m1",
        "client",
        "host",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), metadata)],
    ));
    state.resolve_selected_protocol_metadata("range");
    state.complete_rebalance("range");
    state.install_assignments(HashMap::from([("m1".to_string(), Bytes::new())]));
    CoordinatorGroup::seeded(group_id, GroupKind::Classic(state), HashMap::new())
}

/// A KIP-848 consumer group with one member that subscribes by regex.
fn consumer_group(group_id: &str, regex: &str, resolved: &[&str]) -> CoordinatorGroup {
    let mut state = ConsumerGroup::new(group_id);
    let mut m = member("m1");
    m.subscribed_topic_regex = Some(regex.into());
    state.add_or_update_member(m);
    state.set_resolved_regex(
        regex.into(),
        ResolvedRegularExpression {
            topics: resolved.iter().map(|s| (*s).to_string()).collect(),
            version: 1,
            timestamp_ms: 1,
        },
    );
    CoordinatorGroup::seeded(group_id, GroupKind::Consumer(state), HashMap::new())
}

fn whole(code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        ..Default::default()
    }
}

/// #734 and #813, row by row. Topics `t` and `u` exist with one partition
/// each; `ghost` does not.
/// An `OffsetDelete` case: name, broker, group id, request topics, and the
/// whole expected response.
type Case<'a> = (
    &'a str,
    &'a BrokerHandle,
    &'a str,
    &'a [(&'a str, &'a [i32])],
    OffsetDeleteResponse,
);

#[tokio::test]
async fn offset_delete_matches_kafka_whole_responses() {
    let (allowed, _allowed_dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let (denied, _denied_dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(DenyAll)).await;
    create_topic(&allowed, "t").await;
    create_topic(&allowed, "u").await;
    let coordinator = allowed.broker_arc_for_test().group_coordinator.clone();
    for group in [
        classic_group("connect-g", "connect", Bytes::from_static(b"opaque")),
        classic_group("classic-g", "consumer", subscription(&["t", "ghost"])),
        classic_group("garbled-g", "consumer", Bytes::from_static(b"\x00")),
        consumer_group("kip848-g", "t.*", &["t"]),
    ] {
        let group_id = group.group_id.clone();
        coordinator.seed_classic(&group_id, Box::new(group));
    }

    let rows: Vec<Case<'_>> = vec![
        (
            "group Delete denied: top-level only",
            &denied,
            "classic-g",
            &[("t", &[0])],
            whole(codes::GROUP_AUTHORIZATION_FAILED),
        ),
        (
            "empty group id",
            &allowed,
            "",
            &[("t", &[0])],
            whole(codes::INVALID_GROUP_ID),
        ),
        (
            "missing group: top-level only",
            &allowed,
            "ghost-g",
            &[("t", &[0])],
            whole(codes::GROUP_ID_NOT_FOUND),
        ),
        (
            "non-empty group without the consumer protocol",
            &allowed,
            "connect-g",
            &[("t", &[0])],
            whole(codes::NON_EMPTY_GROUP),
        ),
        (
            "undecodable metadata subscribes to every topic",
            &allowed,
            "garbled-g",
            &[("u", &[0])],
            OffsetDeleteResponse {
                topics: vec![expected_topic(
                    "u",
                    vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)],
                )],
                ..whole(codes::NONE)
            },
        ),
        (
            "KIP-848 group subscribed by regex",
            &allowed,
            "kip848-g",
            &[("t", &[0])],
            OffsetDeleteResponse {
                topics: vec![expected_topic(
                    "t",
                    vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)],
                )],
                ..whole(codes::NONE)
            },
        ),
        (
            "existence before subscription, broker rows before the coordinator's",
            &allowed,
            "classic-g",
            &[("t", &[0]), ("ghost", &[0]), ("u", &[0, 5])],
            OffsetDeleteResponse {
                topics: vec![
                    expected_topic(
                        "ghost",
                        vec![expected_row(0, codes::UNKNOWN_TOPIC_OR_PARTITION)],
                    ),
                    expected_topic(
                        "u",
                        vec![
                            expected_row(5, codes::UNKNOWN_TOPIC_OR_PARTITION),
                            expected_row(0, codes::NONE),
                        ],
                    ),
                    expected_topic("t", vec![expected_row(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)]),
                ],
                ..whole(codes::NONE)
            },
        ),
    ];

    request_identity!(
        (principal, peer, ctx),
        crate::test_support::principal("alice"),
        client_id = "offset-delete-client"
    );
    let version = offset_delete_response::MAX_VERSION;
    let before_ms = crate::time_util::now_ms();
    for (name, broker, group_id, topics, want) in rows {
        let req = krabka_protocol::owned::offset_delete_request::OffsetDeleteRequest {
            group_id: group_id.into(),
            ..req_with_topics(topics)
        };
        let resp = handle(&broker.broker_arc_for_test(), req, version, &ctx)
            .await
            .expect("OffsetDelete");
        check!(resp == want, "{name}");
    }

    // The successful delete writes a tombstone whose record timestamp must
    // be the broker's clock, so later commits do not age this segment from 1970.
    {
        let broker = allowed.broker_arc_for_test();
        let partition = crate::coordinator::partitioner::local_partition_for_group(
            &broker.controller.current_image(),
            broker.config.node_id,
            "classic-g",
        )
        .unwrap();
        let part = broker
            .partitions
            .get(
                crate::coordinator::bootstrap::OFFSETS_TOPIC,
                krabka_ids::PartitionIndex(partition),
            )
            .unwrap();
        let log = part.log.lock().unwrap();
        let (offset, timestamp) = log
            .offset_for_timestamp(before_ms)
            .expect("dated tombstone");
        check!(offset == log.log_end_offset() - 1);
        check!(timestamp <= crate::time_util::now_ms());
    }

    allowed.shutdown().await;
    denied.shutdown().await;
}
