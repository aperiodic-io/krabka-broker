//! End-to-end tests of the `OffsetFetch` handler against a running broker,
//! driven over the wire encoding.
//!
//! Both request shapes are covered, because KIP-447's `require_stable` is one
//! top-level field that has to reach two different response shapes: the
//! pre-KIP-516 `topics[]` of v0–v7 and the `groups[]` of v8 and above.

#[cfg(test)]
use crate::handlers::test_support::CreatedTopicResponse;
use {
    super::*,
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer},
        broker::{Broker, BrokerHandle},
        codes,
        coordinator::unified::{
            actor::{GroupActorMessage, GroupKindTag},
            classic_state::OffsetEntry,
            regex_resolver::no_topic_regex_resolver,
        },
        handlers::test_support::{
            LiveBrokerFixture, StartedBroker as HandlerStartedBroker, allow_all_authorizer,
            configured_topic_request, created_topic_response,
        },
        test_support::{
            self as broker_fixture_support, dispatch_wire, start_group_broker_no_audit,
        },
        time_util::now_ms,
    },
    assert2::assert,
    krabka_log::Offset,
    krabka_protocol::{
        owned::{
            offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics},
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
    tokio::sync::oneshot,
};

// Seed a committed offset for (group, topic, partition) directly on the
// group actor via UpdateCommitted.
//
// The commit is stamped now, not at the epoch. These groups are memberless
// and carry no protocol type, so KIP-211 retention measures each offset from
// its own commit; a 1970 stamp is already past `offsets.retention.minutes`
// and the sweep that runs at broker start would tombstone the offsets — and
// the group with them — out from under a test that is asking about
// `require_stable`.
#[bon::builder]
async fn seed_committed_offset(
    broker: &Broker,
    #[builder(default = "grp")] group: &'_ str,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default = krabka_ids :: PartitionIndex (0))]
    partition: krabka_ids::PartitionIndex,
    #[builder(into, default = Offset (42))] offset: Offset,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::UpdateCommitted {
        entries: vec![((topic.to_string(), partition.0), OffsetEntry {
            offset,
            leader_epoch: 5,
            metadata: String::new(),
            commit_timestamp_ms: crate::time_util::now_ms(),
            expire_timestamp_ms: None,
            topic_id: None,
        })],
        reply: tx,
    })
    .await
    .expect("send UpdateCommitted");
    rx.await.expect("UpdateCommitted ack");
}

/// Retain broker resources after the group actor acknowledges its default offset.
async fn start_seeded_offset_broker() -> LiveBrokerFixture {
    let HandlerStartedBroker {
        broker: handle,
        dir: directory,
    } = start_group_broker_no_audit(allow_all_authorizer()).await;
    let broker = handle.broker_arc_for_test();
    seed_committed_offset().broker(&broker).call().await;
    LiveBrokerFixture {
        handle,
        directory,
        broker,
    }
}

fn named_topic_request(group: &str, topic: &str, partitions: Vec<i32>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        group_id: group.into(),
        topics: Some(vec![
            krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                name: topic.into(),
                partition_indexes: partitions,
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

// A named-topic OffsetFetch (v0–v7 path) returns the group's committed
// offset for the requested partition. A non-zero committed offset pins
// the committed_offset field against the struct-field-deletion mutant,
// which would default it to 0.
#[tokio::test]
async fn named_topic_fetch_returns_committed_offset() {
    const VERSION: i16 = 7; // legacy single-group path (< 8)
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_seeded_offset_broker().await;

    krabka_macros::request_test_identity! {
        bind(p, peer, ctx);
        user ["admin"];
        client ["consumer"];
    };
    let req = named_topic_request("grp", "orders", vec![0]);

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name == "orders")
        .expect("orders topic row");
    let part = topic
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .expect("partition 0 row");
    assert!(
        part.committed_offset == 42,
        "committed_offset must echo the seeded value (42), got {}",
        part.committed_offset
    );
    broker_handle.shutdown().await;
}

// The offsets-log positions the two halves of a transaction occupy in these
// tests: its offset-commit records, and the marker that lands above them.
const TXN_RECORDS_AT: i64 = 10;
const TXN_MARKER_AT: i64 = 11;

/// A topic-partition that an unresolved transaction wrote an offset for.
struct PendingKey {
    topic: String,
    partition: krabka_ids::PartitionIndex,
}

/// An offset that a committed transaction publishes.
struct CommittedTxnOffset {
    topic: String,
    partition: i32,
    entry: OffsetEntry,
}

#[derive(derive_more::From, derive_more::Into, Default)]
struct TxnOffsetCommits(Vec<CommittedTxnOffset>);

// Mark (topic, partition) keys as written by an unresolved transaction, the
// way `TxnOffsetCommit` does once its records are durable.
#[bon::builder]
async fn seed_pending_txn_offsets(
    broker: &Broker,
    #[builder(default = "grp")] group: &'_ str,
    #[builder(into, default = krabka_ids :: ProducerId (91))] producer: krabka_ids::ProducerId,
    #[builder(into, default = Offset (TXN_RECORDS_AT))] written_at: Offset,
    #[builder(default = vec ! [PendingKey { topic : "orders" . into () , partition : krabka_ids :: PartitionIndex (0) }])]
    keys: Vec<PendingKey>,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::AddPendingTxnOffsets {
        producer_id: producer.0,
        written_at: written_at.0,
        keys: keys
            .into_iter()
            .map(|PendingKey { topic, partition }| (topic, partition.0))
            .collect(),
        reply: tx,
    })
    .await
    .expect("send AddPendingTxnOffsets");
    rx.await.expect("AddPendingTxnOffsets ack");
}

async fn seed_default_pending_txn_offsets(broker: &Broker) {
    seed_pending_txn_offsets().broker(broker).call().await;
}

// Resolve the transaction the way its marker does: publish the offsets a
// commit carries (an abort carries none) and drop the producer's pending
// marks, stamped with the marker's own position in the offsets log.
#[bon::builder]
async fn resolve_pending_txn_offsets(
    broker: &Broker,
    #[builder(default = "grp")] group: &'_ str,
    #[builder(into, default = krabka_ids :: ProducerId (91))] producer: krabka_ids::ProducerId,
    #[builder(into, default = Offset (TXN_MARKER_AT))] resolved_through: Offset,
    #[builder(into, default)] committed: TxnOffsetCommits,
) {
    let h = broker
        .group_coordinator
        .get_or_create_group(group, GroupKindTag::Classic);
    let (tx, rx) = oneshot::channel();
    h.tx.send(GroupActorMessage::ResolveTxnOffsets {
        producer_id: producer.0,
        resolved_through: resolved_through.0,
        committed: committed
            .0
            .into_iter()
            .map(|offset| ((offset.topic, offset.partition), offset.entry))
            .collect(),
        reply: tx,
    })
    .await
    .expect("send ResolveTxnOffsets");
    rx.await.expect("ResolveTxnOffsets ack");
}

async fn fetch(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
) -> krabka_protocol::owned::offset_fetch_response::OffsetFetchResponse {
    krabka_macros::request_test_identity! {
        bind(p, peer, ctx);
        user ["admin"];
        client ["consumer"];
    };
    // The response shape changes with the version (KIP-516 moves it into
    // `groups[]` at v8), so this reads it back off the wire.
    dispatch_wire(
        broker,
        krabka_protocol::owned::offset_fetch_request::API_KEY,
        version,
        req,
        &ctx,
    )
    .await
}

// KIP-447 on the pre-KIP-516 shape. `orders-0` carries a stable offset that an
// open transaction is about to replace; `orders-1` is stable and untouched.
//
// `require_stable = false` keeps the pre-KIP-447 answer, so the consumer sees
// the offset the transaction is replacing. `require_stable = true` turns
// `orders-0` into the UNSTABLE_OFFSET_COMMIT row Kafka sends — the invalid
// offset sentinels with an empty, not null, metadata string — while
// `orders-1` still answers normally. Both wire shapes must behave identically.
#[derive(Clone, Copy)]
enum RequireStableShape {
    Legacy,
    Groups,
}

#[yare::parameterized(
    legacy = { 7, RequireStableShape::Legacy },
    groups = { 9, RequireStableShape::Groups },
)]
#[test_macro(tokio::test)]
async fn require_stable_reports_unstable_offsets_on_each_wire_shape(
    version: i16,
    shape: RequireStableShape,
) {
    require_stable(version, shape).await;
}

// A `TxnOffsetCommit` records its KIP-447 mark only after its records are
// durable, so the transaction's own marker can be resolved on the group actor
// in the window in between — the idle-transaction reaper aborts without any
// client involved at all. The late mark then describes a transaction that is
// already over, and taking it would leave `orders-0` answering
// UNSTABLE_OFFSET_COMMIT with nothing left to clear it: an EOS consumer would
// retry that fetch for ever.
//
// The offsets log settles which came first. A mark for records below an
// applied marker is dropped; a mark for records above it is a new transaction
// and is honoured.
#[tokio::test]
async fn a_mark_for_records_below_an_applied_marker_does_not_strand_the_partition() {
    const VERSION: i16 = 7;
    const PRODUCER_ID: i64 = 91;
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_seeded_offset_broker().await;

    let request = OffsetFetchRequest {
        require_stable: true,
        ..named_topic_request("grp", "orders", vec![0])
    };
    let expect = |partition| OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: vec![OffsetFetchResponseTopic {
            name: "orders".into(),
            partitions: vec![partition],
            ..Default::default()
        }],
        error_code: codes::NONE,
        groups: Vec::new(),
        ..Default::default()
    };

    // The abort marker for the records at TXN_RECORDS_AT lands first and
    // publishes nothing; the commit's own mark arrives after it.
    resolve_pending_txn_offsets()
        .broker(&broker)
        .producer(PRODUCER_ID)
        .call()
        .await;
    seed_default_pending_txn_offsets(&broker).await;

    let after_late_mark = fetch(&broker, VERSION, &request).await;
    assert!(
        after_late_mark
            == expect(OffsetFetchResponsePartition {
                partition_index: 0,
                committed_offset: 42,
                committed_leader_epoch: 5,
                metadata: Some(String::new()),
                error_code: codes::NONE,
                ..Default::default()
            })
    );

    // The producer's next transaction writes above that marker, and is
    // reported unstable as usual.
    seed_pending_txn_offsets()
        .broker(&broker)
        .producer(PRODUCER_ID)
        .written_at(TXN_MARKER_AT + 1)
        .call()
        .await;
    let next_transaction = fetch(&broker, VERSION, &request).await;
    assert!(
        next_transaction
            == expect(OffsetFetchResponsePartition {
                partition_index: 0,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: Some(String::new()),
                error_code: codes::UNSTABLE_OFFSET_COMMIT,
                ..Default::default()
            })
    );
    broker_handle.shutdown().await;
}

/// The name of the topic that exists in the topic-reference tables.
const KNOWN_NAME: &str = "orders";

/// A name that no topic in the topic-reference tables has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// An id that no topic in the topic-reference tables has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

use crate::handlers::test_support::{DenyTopics, TopicRef, WireTopic};

/// Create the known topic and return its id. Seed a committed offset for
/// `(KNOWN_NAME, 0)`, and one for the empty topic name, which a zero-id row
/// must not read.
async fn seed_topic_reference_group(broker_handle: &BrokerHandle) -> WireUuid {
    let CreatedTopicResponse {
        client: _client,
        response: _response,
    } = created_topic_response()
        .broker(broker_handle)
        .client_id("offset-fetch-resolution-test")
        .request(|| configured_topic_request().topic(KNOWN_NAME).call())
        .call()
        .await;
    broker_handle
        .wait_until_partition_present(KNOWN_NAME, 0)
        .await;
    let broker = broker_handle.broker_arc_for_test();
    seed_committed_offset()
        .broker(&broker)
        .topic(KNOWN_NAME)
        .call()
        .await;
    seed_committed_offset()
        .broker(&broker)
        .topic("")
        .offset(7)
        .call()
        .await;
    let image = broker_handle.controller_image_for_test();
    let topic = image.topic(KNOWN_NAME).expect("known topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

#[derive(derive_more::From, derive_more::Into)]
/// The request row for `topic`, and the topic row that the response carries
/// for it at `version` with `partition` as its only partition row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OffsetFetchVersion(i16);

#[derive(krabka_macros::FieldDefaults)]
struct TopicReferenceSetup {
    #[default(OffsetFetchVersion(9))]
    version: OffsetFetchVersion,
    #[default(TopicRef::KnownName)]
    topic: TopicRef,
    known_id: WireUuid,
    #[default(no_offset_row(codes::NONE))]
    partition: OffsetFetchResponsePartitions,
}

/// The request row for a topic reference and the response row it expects.
struct TopicReferenceRows {
    request: OffsetFetchRequestTopics,
    response: OffsetFetchResponseTopics,
}

#[bon::builder]
fn topic_reference_rows(
    #[builder(into, default = OffsetFetchVersion (9))] version: OffsetFetchVersion,
    #[builder(default = TopicRef :: KnownName)] topic: TopicRef,
    #[builder(default)] known_id: WireUuid,
    #[builder(default = no_offset_row (codes :: NONE))] partition: OffsetFetchResponsePartitions,
) -> TopicReferenceRows {
    let setup = TopicReferenceSetup {
        version,
        topic,
        known_id,
        partition,
    };
    let WireTopic { name, id: topic_id } = setup.topic.wire_reference(
        WireTopic {
            name: KNOWN_NAME,
            id: setup.known_id,
        },
        WireTopic {
            name: UNKNOWN_NAME,
            id: UNKNOWN_ID,
        },
    );
    let request = OffsetFetchRequestTopics {
        name: name.to_string(),
        topic_id,
        partition_indexes: vec![0],
        ..Default::default()
    };
    // The wire carries the name at v8 and v9, and the id at v10.
    let id_only = setup.version.0 >= 10;
    let response = OffsetFetchResponseTopics {
        name: if id_only {
            String::new()
        } else {
            name.to_string()
        },
        topic_id: if id_only { topic_id } else { WireUuid::ZERO },
        partitions: vec![setup.partition],
        ..Default::default()
    };
    TopicReferenceRows { request, response }
}

fn groups_request(topics: Vec<OffsetFetchRequestTopics>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: "grp".into(),
            topics: Some(topics),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn groups_response(topics: Vec<OffsetFetchResponseTopics>) -> OffsetFetchResponse {
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: Vec::new(),
        error_code: codes::NONE,
        groups: vec![OffsetFetchResponseGroup {
            group_id: "grp".into(),
            topics,
            error_code: codes::NONE,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The partition row of a partition with no committed offset, or of a
/// refused topic.
fn no_offset_row(error_code: i16) -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code,
        ..Default::default()
    }
}

/// The partition row of the seeded offset of the known topic.
fn seeded_row() -> OffsetFetchResponsePartitions {
    OffsetFetchResponsePartitions {
        partition_index: 0,
        committed_offset: 42,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

struct TopicReferenceCase {
    version: OffsetFetchVersion,
    topic: TopicRef,
    partition: OffsetFetchResponsePartitions,
}

fn unresolved_topic_reference_cases() -> [TopicReferenceCase; 2] {
    [
        TopicReferenceCase {
            version: OffsetFetchVersion(10),
            topic: TopicRef::UnknownId,
            partition: no_offset_row(codes::UNKNOWN_TOPIC_ID),
        },
        TopicReferenceCase {
            version: OffsetFetchVersion(10),
            topic: TopicRef::ZeroId,
            partition: no_offset_row(codes::UNKNOWN_TOPIC_ID),
        },
    ]
}

/// A started broker seeded with the known topic, and the id of that topic.
struct TopicReferenceBroker {
    handle: BrokerHandle,
    dir: tempfile::TempDir,
    topic_id: WireUuid,
}

async fn topic_reference_broker(authorizer: Arc<dyn Authorizer>) -> TopicReferenceBroker {
    let HandlerStartedBroker {
        broker: handle,
        dir: directory,
    } = start_group_broker_no_audit(authorizer).await;
    let topic_id = seed_topic_reference_group(&handle).await;
    TopicReferenceBroker {
        handle,
        dir: directory,
        topic_id,
    }
}

async fn seed_unresolved_name_offset(broker: &Broker) {
    seed_committed_offset()
        .broker(broker)
        .topic(UNKNOWN_NAME)
        .offset(9)
        .call()
        .await;
}

/// Send one single-topic request per case, and compare every whole response
/// with the one that the case expects.
async fn run_topic_reference_table(
    authorizer: Arc<dyn Authorizer>,
    cases: Vec<TopicReferenceCase>,
) {
    let TopicReferenceBroker {
        handle: broker_handle,
        dir: _dir,
        topic_id: known_id,
    } = topic_reference_broker(authorizer).await;
    let broker = broker_handle.broker_arc_for_test();
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for TopicReferenceCase {
        version,
        topic,
        partition,
    } in cases
    {
        let TopicReferenceRows {
            request: request_row,
            response: response_row,
        } = topic_reference_rows()
            .version(version)
            .topic(topic)
            .known_id(known_id)
            .partition(partition)
            .call();
        let response = fetch(&broker, version.0, &groups_request(vec![request_row])).await;
        actual.push((version, topic, response));
        expected.push((version, topic, groups_response(vec![response_row])));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.fetchOffsetsForGroup` answers `UNKNOWN_TOPIC_ID` at v10
/// for every row whose name is empty after id resolution. The zero id is such
/// a row, and it must not read the offset stored under the empty name.
#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    run_topic_reference_table(
        Arc::new(AllowAllAuthorizer),
        vec![
            TopicReferenceCase {
                version: OffsetFetchVersion(9),
                topic: TopicRef::KnownName,
                partition: seeded_row(),
            },
            TopicReferenceCase {
                version: OffsetFetchVersion(9),
                topic: TopicRef::UnknownName,
                partition: no_offset_row(codes::NONE),
            },
            TopicReferenceCase {
                version: OffsetFetchVersion(10),
                topic: TopicRef::KnownId,
                partition: seeded_row(),
            },
        ]
        .into_iter()
        .chain(unresolved_topic_reference_cases())
        .collect(),
    )
    .await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    run_topic_reference_table(
        Arc::new(DenyTopics),
        vec![
            TopicReferenceCase {
                version: OffsetFetchVersion(9),
                topic: TopicRef::KnownName,
                partition: no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
            },
            TopicReferenceCase {
                version: OffsetFetchVersion(10),
                topic: TopicRef::KnownId,
                partition: no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
            },
        ]
        .into_iter()
        .chain(unresolved_topic_reference_cases())
        .collect(),
    )
    .await;
}

/// Kafka appends the refused topics after the topics that the coordinator
/// answers, so a zero-id row that comes first in the request comes last in
/// the response.
#[tokio::test]
async fn refused_topics_follow_the_answered_topics() {
    const VERSION: i16 = 10;
    let TopicReferenceBroker {
        handle: broker_handle,
        dir: _dir,
        topic_id: known_id,
    } = topic_reference_broker(Arc::new(AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let TopicReferenceRows {
        request: zero_request,
        response: zero_response,
    } = topic_reference_rows()
        .version(VERSION)
        .topic(TopicRef::ZeroId)
        .known_id(known_id)
        .partition(no_offset_row(codes::UNKNOWN_TOPIC_ID))
        .call();
    let TopicReferenceRows {
        request: known_request,
        response: known_response,
    } = topic_reference_rows()
        .version(VERSION)
        .topic(TopicRef::KnownId)
        .known_id(known_id)
        .partition(seeded_row())
        .call();

    let actual = fetch(
        &broker,
        VERSION,
        &groups_request(vec![zero_request, known_request]),
    )
    .await;

    assert!(actual == groups_response(vec![known_response, zero_response]));
    broker_handle.shutdown().await;
}

/// Allows every group operation, denies `Read` on every topic, and allows
/// `Describe` on the known topic only.
#[derive(Debug)]
struct DescribeKnownTopic;

krabka_macros::test_authorizer!(DescribeKnownTopic, req; {
    let allowed = req.resource_type != krabka_metadata::ResourceType::Topic
        || (req.operation == krabka_metadata::AclOperation::Describe
            && req.resource_name == KNOWN_NAME);
    if allowed {
        crate::authorizer::AuthorizationResult::Allow
    } else {
        crate::authorizer::AuthorizationResult::Deny
    }
});

/// The legacy (v0 to v7) partition row of the seeded offset of the known
/// topic.
fn legacy_seeded_row() -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index: 0,
        committed_offset: 42,
        committed_leader_epoch: 5,
        metadata: Some(String::new()),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// The legacy (v0 to v7) partition row of a refused topic.
fn legacy_refused_row() -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition {
        partition_index: 0,
        committed_offset: -1,
        committed_leader_epoch: -1,
        metadata: Some(String::new()),
        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        ..Default::default()
    }
}

/// A topic name and its one legacy partition row.
struct LegacyTopic<'a> {
    name: &'a str,
    partition: OffsetFetchResponsePartition,
}

/// A legacy (v0 to v7) response that carries one partition row per topic.
fn legacy_response(topics: Vec<LegacyTopic<'_>>) -> OffsetFetchResponse {
    OffsetFetchResponse {
        topics: topics
            .into_iter()
            .map(|LegacyTopic { name, partition }| OffsetFetchResponseTopic {
                name: name.to_string(),
                partitions: vec![partition],
                ..Default::default()
            })
            .collect(),
        error_code: codes::NONE,
        ..Default::default()
    }
}

/// A legacy (v0 to v7) request for partition 0 of `topics`, or for every
/// topic when `None`.
fn legacy_request(topics: Option<&[&str]>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        group_id: "grp".into(),
        topics: topics.map(|names| {
            names
                .iter()
                .map(
                    |name| krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                        name: (*name).to_string(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    },
                )
                .collect()
        }),
        ..Default::default()
    }
}

/// A KIP-516 request for partition 0 of `topics` by name, or for every topic
/// when `None`.
fn named_groups_request(topics: Option<&[&str]>) -> OffsetFetchRequest {
    OffsetFetchRequest {
        groups: vec![OffsetFetchRequestGroup {
            group_id: "grp".into(),
            topics: topics.map(|names| {
                names
                    .iter()
                    .map(|name| OffsetFetchRequestTopics {
                        name: (*name).to_string(),
                        partition_indexes: vec![0],
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// A KIP-516 (v8 and v9) topic row by name, with one partition row.
fn named_topic(name: &str, partition: OffsetFetchResponsePartitions) -> OffsetFetchResponseTopics {
    OffsetFetchResponseTopics {
        name: name.to_string(),
        partitions: vec![partition],
        ..Default::default()
    }
}

/// One request, its version, and the whole response it expects.
struct ResponseCase<'a> {
    case: &'a str,
    version: i16,
    request: OffsetFetchRequest,
    response: OffsetFetchResponse,
}

/// Sends each case's request and compares every whole response with the one
/// that the case expects.
async fn run_response_table(broker: &Broker, cases: Vec<ResponseCase<'_>>) {
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for ResponseCase {
        case,
        version,
        request,
        response,
    } in cases
    {
        actual.push((case, fetch(broker, version, &request).await));
        expected.push((case, response));
    }
    assert!(actual == expected);
}

/// Kafka authorizes each topic with `Describe`, not `Read`, and appends the
/// refused topics after the answered ones on both shapes. A fetch-all leaves
/// out every topic that the principal may not describe instead of answering
/// it with `TOPIC_AUTHORIZATION_FAILED`.
#[tokio::test]
async fn topics_are_authorized_with_describe_and_fetch_all_hides_refused_topics() {
    let TopicReferenceBroker {
        handle: broker_handle,
        dir: _dir,
        topic_id: _known_id,
    } = topic_reference_broker(Arc::new(DescribeKnownTopic)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_unresolved_name_offset(&broker).await;
    let both: &[&str] = &[UNKNOWN_NAME, KNOWN_NAME];
    run_response_table(&broker, vec![
        ResponseCase {
            case: "v7 explicit list",
            version: 7,
            request: legacy_request(Some(both)),
            response: legacy_response(vec![
                LegacyTopic {
                    name: KNOWN_NAME,
                    partition: legacy_seeded_row(),
                },
                LegacyTopic {
                    name: UNKNOWN_NAME,
                    partition: legacy_refused_row(),
                },
            ]),
        },
        ResponseCase {
            case: "v7 fetch-all",
            version: 7,
            request: legacy_request(None),
            response: legacy_response(vec![LegacyTopic {
                name: KNOWN_NAME,
                partition: legacy_seeded_row(),
            }]),
        },
        ResponseCase {
            case: "v9 explicit list",
            version: 9,
            request: named_groups_request(Some(both)),
            response: groups_response(vec![
                named_topic(KNOWN_NAME, seeded_row()),
                named_topic(
                    UNKNOWN_NAME,
                    no_offset_row(codes::TOPIC_AUTHORIZATION_FAILED),
                ),
            ]),
        },
        ResponseCase {
            case: "v9 fetch-all",
            version: 9,
            request: named_groups_request(None),
            response: groups_response(vec![named_topic(KNOWN_NAME, seeded_row())]),
        },
    ])
    .await;
    broker_handle.shutdown().await;
}

/// A fetch-all names each topic by id at v10, so Kafka leaves out a topic that
/// the metadata image does not hold. Before v10 the name carries the row. The
/// topics come in name order.
#[tokio::test]
async fn fetch_all_leaves_out_topics_without_an_id_at_v10() {
    let TopicReferenceBroker {
        handle: broker_handle,
        dir: _dir,
        topic_id: known_id,
    } = topic_reference_broker(Arc::new(AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_unresolved_name_offset(&broker).await;
    let offset_row = |committed_offset| OffsetFetchResponsePartitions {
        committed_offset,
        ..seeded_row()
    };
    run_response_table(&broker, vec![
        ResponseCase {
            case: "v9 fetch-all",
            version: 9,
            request: named_groups_request(None),
            response: groups_response(vec![
                named_topic("", offset_row(7)),
                named_topic(UNKNOWN_NAME, offset_row(9)),
                named_topic(KNOWN_NAME, seeded_row()),
            ]),
        },
        ResponseCase {
            case: "v10 fetch-all",
            version: 10,
            request: named_groups_request(None),
            response: groups_response(vec![OffsetFetchResponseTopics {
                topic_id: known_id,
                partitions: vec![seeded_row()],
                ..Default::default()
            }]),
        },
    ])
    .await;
    broker_handle.shutdown().await;
}

/// Joins `member_id` to the KIP-848 consumer group `group` and returns its
/// member epoch.
async fn join_consumer_group(broker: &Broker, group: &str, member_id: &str) -> i32 {
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    let handle = broker.group_coordinator.get_or_create_consumer(group);
    let (reply, joined) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: ConsumerGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.into(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["orders".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client_id: "client".into(),
            client_host: "host".into(),
            reply,
            regex_resolver: no_topic_regex_resolver(),
        })
        .await
        .expect("send Heartbeat");
    let joined = joined.await.expect("Heartbeat reply");
    assert!(joined.error_code == codes::NONE);
    joined.member_epoch
}

/// Kafka's `OffsetMetadataManager.fetchOffsets` reads offsets without creating
/// a group, and `ConsumerGroup.validateOffsetFetch` checks the v9 member id
/// and epoch of a consumer group. A refused fetch is the group row with the
/// error code and no topics (`OffsetFetchResponse.groupError`).
#[tokio::test]
async fn offset_fetch_creates_no_group_and_checks_the_member_epoch() {
    struct Row {
        name: &'static str,
        version: i16,
        group_id: &'static str,
        member_id: Option<&'static str>,
        /// The epoch relative to the member's epoch, or an absolute -1.
        member_epoch: EpochRef,
        expected: OffsetFetchResponseGroup,
        group_exists_after: bool,
    }
    #[derive(Clone, Copy)]
    enum EpochRef {
        Current,
        Older,
        NoEpoch,
    }

    let offsets_row = |partition: OffsetFetchResponsePartitions| {
        vec![OffsetFetchResponseTopics {
            name: "orders".into(),
            partitions: vec![partition],
            ..Default::default()
        }]
    };
    let group_row = |group_id: &str, topics, error_code| OffsetFetchResponseGroup {
        group_id: group_id.into(),
        topics,
        error_code,
        ..Default::default()
    };
    let rows = [
        Row {
            name: "unknown group at v8",
            version: 8,
            group_id: "typo",
            member_id: None,
            member_epoch: EpochRef::NoEpoch,
            expected: group_row("typo", offsets_row(no_offset_row(codes::NONE)), codes::NONE),
            group_exists_after: false,
        },
        Row {
            name: "unknown group at v9 with a member",
            version: 9,
            group_id: "typo",
            member_id: Some("m1"),
            member_epoch: EpochRef::Current,
            expected: group_row("typo", offsets_row(no_offset_row(codes::NONE)), codes::NONE),
            group_exists_after: false,
        },
        Row {
            name: "consumer member with its epoch",
            version: 9,
            group_id: "grp",
            member_id: Some("m1"),
            member_epoch: EpochRef::Current,
            expected: group_row("grp", offsets_row(seeded_row()), codes::NONE),
            group_exists_after: true,
        },
        Row {
            name: "consumer member with an older epoch",
            version: 9,
            group_id: "grp",
            member_id: Some("m1"),
            member_epoch: EpochRef::Older,
            expected: group_row("grp", vec![], codes::STALE_MEMBER_EPOCH),
            group_exists_after: true,
        },
        Row {
            name: "unknown member of a consumer group",
            version: 9,
            group_id: "grp",
            member_id: Some("ghost"),
            member_epoch: EpochRef::Current,
            expected: group_row("grp", vec![], codes::UNKNOWN_MEMBER_ID),
            group_exists_after: true,
        },
        Row {
            name: "no member id and epoch -1 (admin client)",
            version: 9,
            group_id: "grp",
            member_id: None,
            member_epoch: EpochRef::NoEpoch,
            expected: group_row("grp", offsets_row(seeded_row()), codes::NONE),
            group_exists_after: true,
        },
        Row {
            name: "no member id with an epoch",
            version: 9,
            group_id: "grp",
            member_id: None,
            member_epoch: EpochRef::Current,
            expected: group_row("grp", vec![], codes::UNKNOWN_MEMBER_ID),
            group_exists_after: true,
        },
    ];

    for row in rows {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [broker_fixture_support::start_group_broker_no_audit] allow_all;
        };
        let epoch = join_consumer_group(&broker, "grp", "m1").await;
        seed_committed_offset().broker(&broker).call().await;

        let request = OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: row.group_id.into(),
                member_id: row.member_id.map(str::to_string),
                member_epoch: match row.member_epoch {
                    EpochRef::Current => epoch,
                    EpochRef::Older => epoch - 1,
                    EpochRef::NoEpoch => -1,
                },
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "orders".into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = fetch(&broker, row.version, &request).await;

        let expected = OffsetFetchResponse {
            groups: vec![row.expected],
            ..Default::default()
        };
        assert2::check!(response == expected, "{}", row.name);
        assert2::check!(
            broker.group_coordinator.find(row.group_id).is_some() == row.group_exists_after,
            "{}",
            row.name
        );
        broker_handle.shutdown().await;
    }
}

/// The legacy single-group shape (v0-v7) creates no group for an unknown id.
#[tokio::test]
async fn legacy_offset_fetch_creates_no_group() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker);
        boot [broker_fixture_support::start_group_broker_no_audit] allow_all;
    };
    let request = named_topic_request("typo", "orders", vec![0]);
    let response = fetch(&broker, 7, &request).await;
    assert!(response.topics[0].partitions[0].committed_offset == -1);
    assert!(broker.group_coordinator.find("typo").is_none());
    broker_handle.shutdown().await;
}

/// Exercise the same transaction phases against both KIP-447 wire shapes.
async fn require_stable(version: i16, shape: RequireStableShape) {
    const PRODUCER_ID: i64 = 91;
    let LiveBrokerFixture {
        handle: broker_handle,
        directory: _dir,
        broker,
    } = start_seeded_offset_broker().await;
    seed_committed_offset()
        .broker(&broker)
        .partition(1)
        .offset(11)
        .call()
        .await;
    seed_default_pending_txn_offsets(&broker).await;
    let relaxed_request = request(shape, false);
    let relaxed = fetch(&broker, version, &relaxed_request).await;
    assert!(
        relaxed
            == expected_response(shape, vec![
                (0, 42, 5, codes::NONE),
                (1, 11, 5, codes::NONE)
            ])
    );
    let strict_request = request(shape, true);
    let strict = fetch(&broker, version, &strict_request).await;
    let expected_strict = expected_response(shape, vec![
        (0, -1, -1, codes::UNSTABLE_OFFSET_COMMIT),
        (1, 11, 5, codes::NONE),
    ]);
    assert!(strict == expected_strict);
    resolve_pending_txn_offsets()
        .broker(&broker)
        .producer(PRODUCER_ID)
        .committed(vec![CommittedTxnOffset {
            topic: "orders".to_string(),
            partition: 0,
            entry: committed_entry(77),
        }])
        .call()
        .await;
    let resolved = fetch(&broker, version, &strict_request).await;
    assert!(
        resolved
            == expected_response(shape, vec![
                (0, 77, 5, codes::NONE),
                (1, 11, 5, codes::NONE)
            ])
    );
    broker_handle.shutdown().await;
}

fn request(shape: RequireStableShape, require_stable: bool) -> OffsetFetchRequest {
    match shape {
        RequireStableShape::Legacy => OffsetFetchRequest {
            group_id: "grp".into(),
            topics: Some(vec![
                krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopic {
                    name: "orders".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                },
            ]),
            require_stable,
            ..Default::default()
        },
        RequireStableShape::Groups => OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "grp".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: "orders".into(),
                    partition_indexes: vec![0, 1],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            require_stable,
            ..Default::default()
        },
    }
}

fn expected_response(
    shape: RequireStableShape,
    rows: Vec<(i32, i64, i32, i16)>,
) -> OffsetFetchResponse {
    match shape {
        RequireStableShape::Legacy => OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchResponseTopic {
                name: "orders".into(),
                partitions: rows
                    .into_iter()
                    .map(
                        |(
                            partition_index,
                            committed_offset,
                            committed_leader_epoch,
                            error_code,
                        )| {
                            OffsetFetchResponsePartition {
                                partition_index,
                                committed_offset,
                                committed_leader_epoch,
                                metadata: Some(String::new()),
                                error_code,
                                ..Default::default()
                            }
                        },
                    )
                    .collect(),
                ..Default::default()
            }],
            error_code: codes::NONE,
            groups: Vec::new(),
            ..Default::default()
        },
        RequireStableShape::Groups => OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: Vec::new(),
            error_code: codes::NONE,
            groups: vec![OffsetFetchResponseGroup {
                group_id: "grp".into(),
                topics: vec![OffsetFetchResponseTopics {
                    name: "orders".into(),
                    topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
                    partitions: rows
                        .into_iter()
                        .map(
                            |(
                                partition_index,
                                committed_offset,
                                committed_leader_epoch,
                                error_code,
                            )| OffsetFetchResponsePartitions {
                                partition_index,
                                committed_offset,
                                committed_leader_epoch,
                                metadata: Some(String::new()),
                                error_code,
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                }],
                error_code: codes::NONE,
                ..Default::default()
            }],
            ..Default::default()
        },
    }
}

fn committed_entry(offset: i64) -> OffsetEntry {
    OffsetEntry {
        offset: Offset(offset),
        leader_epoch: 5,
        metadata: String::new(),
        commit_timestamp_ms: now_ms(),
        expire_timestamp_ms: None,
        topic_id: None,
    }
}
