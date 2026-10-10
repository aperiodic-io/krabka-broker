//! Table-driven coverage for #694: the `TransactionalId` `Write` ACL gate on
//! `Produce`.
//!
//! Kafka's `KafkaApis.handleProduceRequest` authorizes `Write` on
//! `TransactionalId(transactional_id)` when, and only when, the request
//! carries a transactional batch (`RequestUtils.hasTransactionalRecords`),
//! and it answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) on every
//! partition row of the whole request, before any topic is resolved, when
//! that check does not pass. This suite drives `handle()` (the same
//! `Produce` entry point the dispatcher calls) directly, against a broker
//! whose authorizer grants ACLs by the principal's name, so each case names
//! exactly the resource-type/operation pairs it holds.

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::{
    owned::{
        create_topics_request::{self},
        init_producer_id_request::InitProducerIdRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};

use super::{FIRST_TOPIC_ID_VERSION, handle};
use crate::{
    broker::Broker,
    codes,
    handlers::test_support::CreateTopicSetup,
    test_support::{
        KafkaErrorCode, decode_response, dispatch_context, encode_request, peer, principal,
    },
};

#[derive(Clone, Copy)]
struct ProduceApiVersion(i16);

#[derive(Clone, Copy)]
struct ProducerEpoch(i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TransactionalBatchSetup {
    #[default(ProducerId(7))]
    producer_id: ProducerId,
    #[default(ProducerEpoch(0))]
    producer_epoch: ProducerEpoch,
}

#[derive(krabka_macros::FieldDefaults)]
struct ProduceRequestSetup<'a> {
    #[default(WireUuid::ZERO)]
    topic_id: WireUuid,
    #[default(ProduceApiVersion(12))]
    version: ProduceApiVersion,
    transactional_id: Option<&'a str>,
    #[default(plain_batch())]
    records: RecordsPayload,
}

#[derive(Clone, Copy)]
enum BatchKind {
    Transactional,
    Ordinary,
}

const TOPIC: &str = "orders";
const TXN_ID: &str = "t1";

/// A grant string a `GrantsInPrincipalName` principal needs to create topics.
const ADMIN_GRANTS: &str = "Cluster:Create";

async fn boot() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let (handle, dir) = crate::test_support::start_transaction_grant_broker().await;
    handle.wait_until_controller_leader().await;
    handle.wait_until_brokers_registered(1).await;
    let broker = handle.broker_arc_for_test();
    // A client's first `FindCoordinator(TRANSACTION)` creates
    // `__transaction_state`. `open_transaction` below drives `InitProducerId`
    // directly, so the topic is created here instead.
    handle.wait_until_transaction_coordinator_ready().await;
    create_topic(&broker, TOPIC).await;
    (handle, dir)
}

async fn create_topic(broker: &Broker, name: &str) {
    request_identity!(
        (admin, address, ctx),
        principal(ADMIN_GRANTS),
        client_id = "produce-txn-authz-admin"
    );
    let request = crate::handlers::test_support::configured_topic_request(CreateTopicSetup {
        topic: name,
        ..Default::default()
    });
    dispatch_context(
        broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
}

/// Drives `InitProducerId` for `TXN_ID`, as a principal granted
/// `TransactionalId:Write`, and returns the producer id and epoch a
/// transactional batch must now carry. It does not also drive
/// `AddPartitionsToTxn`: every case below uses Produce v12+, which is
/// transaction version 2 (KIP-890) and enlists the partition itself as part
/// of the append (`FIRST_ADD_PARTITION_PRODUCE_VERSION` in
/// `producer_checks.rs`), the same way a real v12+ client would.
async fn open_transaction(broker: &Broker) -> TransactionalBatchSetup {
    request_identity!(
        (grantee, address, ctx),
        principal("TransactionalId:Write"),
        client_id = "produce-txn-authz-open"
    );

    let init_request = InitProducerIdRequest {
        transactional_id: Some(TXN_ID.to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let init = crate::handlers::init_producer_id::handle(broker, init_request, version, &ctx)
        .await
        .expect("InitProducerId");
    assert!(init.error_code == codes::NONE, "InitProducerId: {init:?}");
    TransactionalBatchSetup {
        producer_id: ProducerId(init.producer_id),
        producer_epoch: ProducerEpoch(init.producer_epoch),
    }
}

/// One v2 batch with one record, non-transactional.
fn plain_batch() -> RecordsPayload {
    RecordsPayload::V2(vec![crate::test_support::repeated_records_batch(
        crate::test_support::RepeatedRecordsSetup::default(),
    )])
}

/// One v2 batch with one record, marked transactional (KIP-98) under
/// `producer_id`/`producer_epoch`.
fn transactional_batch(setup: TransactionalBatchSetup) -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        producer_id: setup.producer_id.0,
        producer_epoch: setup.producer_epoch.0,
        base_sequence: 0,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }])
}

/// One request naming `TOPIC` by name (`version < FIRST_TOPIC_ID_VERSION`) or
/// by id (`version >= FIRST_TOPIC_ID_VERSION`), carrying `records` and
/// `transactional_id`.
fn produce_request(setup: ProduceRequestSetup<'_>) -> ProduceRequest {
    let ProduceRequestSetup {
        topic_id,
        version,
        transactional_id,
        records,
    } = setup;
    let id_only = version.0 >= FIRST_TOPIC_ID_VERSION;
    ProduceRequest {
        transactional_id: transactional_id.map(str::to_owned),
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: if id_only {
                String::new()
            } else {
                TOPIC.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The response row Kafka's per-partition `PartitionResponse(error)`
/// constructor builds for a row that failed before the append: `base_offset`,
/// `log_append_time_ms` and `log_start_offset` are all the -1 sentinel.
fn refused_row(index: PartitionIndex, error_code: KafkaErrorCode) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: index.0,
        error_code: error_code.0,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        ..Default::default()
    }
}

async fn drive(broker: &Broker, grants: &str, setup: ProduceRequestSetup<'_>) -> ProduceResponse {
    let version = setup.version.0;
    let request = produce_request(setup);
    request_identity!(
        (user, address, ctx),
        principal(grants),
        client_id = "produce-txn-authz"
    );
    let request_bytes = encode_request(&request, version);
    let response_bytes = handle(broker, version, &request_bytes, request_bytes.clone(), &ctx)
        .await
        .expect("handle produce");
    decode_response(&response_bytes, version)
}

/// One case of [`transactional_id_write_gates_exactly_on_the_batch_not_the_request_field`].
struct Case<'a> {
    name: &'a str,
    version: ProduceApiVersion,
    batch: BatchKind,
    transactional_id: Option<&'a str>,
    grants: &'a str,
    expected: KafkaErrorCode,
}

/// Every case names its batch, its request-level `transactional_id`, the
/// grants its principal holds, and the row Kafka answers.
///
/// - "a transactional batch with a null `transactional_id`" is the
///   vulnerable case from #694: krabka used to key the check on
///   `transactional_id.is_some()` alone, so this request skipped the
///   `TransactionalId` `Write` check entirely and the batch reached
///   `verify_transactional_produce`, which resolves *some* transactional id
///   from the batch's producer id -- a principal with only topic `Write`
///   could write into a transaction it was never authorized against.
/// - "a non-transactional batch with a denied `transactional_id`" is the
///   mirror bug: krabka used to run the check whenever `transactional_id` was
///   present, regardless of the batch, and refused a plain produce that
///   Kafka accepts.
#[tokio::test]
async fn transactional_id_write_gates_exactly_on_the_batch_not_the_request_field() {
    let (handle, _dir) = boot().await;
    let broker = handle.broker_arc_for_test();
    let topic_id = {
        let image = handle.controller_image_for_test();
        WireUuid(
            image
                .topic(TOPIC)
                .expect("topic exists")
                .topic_id
                .into_bytes(),
        )
    };
    let transaction = open_transaction(&broker).await;
    let refused = KafkaErrorCode(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

    let cases = [
        Case {
            name: "transactional batch, no transactional_id, no grant -> refused (the #694 hijack path)",
            version: ProduceApiVersion(12),
            batch: BatchKind::Transactional,
            transactional_id: None,
            grants: "Topic:Write",
            expected: refused,
        },
        Case {
            name: "transactional batch, transactional_id, denied grant -> refused",
            version: ProduceApiVersion(12),
            batch: BatchKind::Transactional,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write",
            expected: refused,
        },
        Case {
            name: "transactional batch, transactional_id, allowed grant -> appended",
            version: ProduceApiVersion(12),
            batch: BatchKind::Transactional,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write+TransactionalId:Write",
            expected: KafkaErrorCode(codes::NONE),
        },
        Case {
            name: "non-transactional batch, transactional_id, denied grant -> appended (not checked)",
            version: ProduceApiVersion(12),
            batch: BatchKind::Ordinary,
            transactional_id: Some(TXN_ID),
            grants: "Topic:Write",
            expected: KafkaErrorCode(codes::NONE),
        },
        Case {
            name: "non-transactional batch, no transactional_id, no grant -> appended",
            version: ProduceApiVersion(12),
            batch: BatchKind::Ordinary,
            transactional_id: None,
            grants: "Topic:Write",
            expected: KafkaErrorCode(codes::NONE),
        },
    ];

    // Every appending case writes one record to the same partition, so the
    // base offset it gets back advances by one each time.
    let mut next_offset = Offset(0);
    for case in cases {
        let records = if matches!(case.batch, BatchKind::Transactional) {
            transactional_batch(transaction)
        } else {
            plain_batch()
        };
        let actual = drive(
            &broker,
            case.grants,
            ProduceRequestSetup {
                version: case.version,
                topic_id,
                transactional_id: case.transactional_id,
                records,
            },
        )
        .await;
        let expected_row = if case.expected.0 == codes::NONE {
            let row = PartitionProduceResponse {
                index: 0,
                error_code: codes::NONE,
                base_offset: next_offset.0,
                log_append_time_ms: -1,
                log_start_offset: 0,
                ..Default::default()
            };
            next_offset = Offset(next_offset.0 + 1);
            row
        } else {
            refused_row(PartitionIndex(0), case.expected)
        };
        let expected = ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: TOPIC.to_string(),
                topic_id: WireUuid::ZERO,
                partition_responses: vec![expected_row],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(actual == expected, "{}", case.name);
    }
    handle.shutdown().await;
}

/// Kafka answers the 53 row before it resolves any topic: an unknown topic id
/// at v13+, which would otherwise answer `UNKNOWN_TOPIC_ID` (100), still
/// answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) when the request
/// carries a transactional batch and the `transactional_id` check fails.
#[tokio::test]
async fn transactional_denial_precedes_topic_resolution_at_v13() {
    let (handle, _dir) = boot().await;
    let broker = handle.broker_arc_for_test();

    let unknown_id = WireUuid([0x0b; 16]);
    let version = ProduceApiVersion(13);
    let actual = drive(
        &broker,
        "Topic:Write",
        ProduceRequestSetup {
            version,
            topic_id: unknown_id,
            transactional_id: Some(TXN_ID),
            records: transactional_batch(TransactionalBatchSetup {
                producer_id: ProducerId(999),
                ..Default::default()
            }),
        },
    )
    .await;
    let expected = ProduceResponse {
        responses: vec![TopicProduceResponse {
            name: String::new(),
            topic_id: unknown_id,
            partition_responses: vec![refused_row(
                PartitionIndex(0),
                KafkaErrorCode(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            )],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(actual == expected);
    handle.shutdown().await;
}
