//! Handler tests for the order of the per-partition acknowledge checks of
//! `ShareFetch` and `ShareAcknowledge`, and for the partition that the
//! metadata does not hold.
//!
//! Kafka's `KafkaApis.handleAcknowledgements` runs
//! `validateAcknowledgementBatches` first (`INVALID_REQUEST`), then the topic
//! `Read` check (`TOPIC_AUTHORIZATION_FAILED`), then the metadata check
//! (`UNKNOWN_TOPIC_OR_PARTITION`). Only a partition that passes all three
//! reaches the share partition. The fetch half of a `ShareFetch` row runs the
//! `Read` check and the metadata check on its own, so a row can carry a fetch
//! error and a different acknowledge error.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::AuthorizationResult,
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            AcknowledgementMode, AcknowledgementPartitionSetup, OffsetRun, ShareSessionEpoch,
            StartedBroker, WireAckBatch, WirePartitionAcknowledgements,
            acknowledge_batches_request, acquired_share_records,
            create_topic as create_topic_from_test_support, initialize_earliest_share,
            produce_records, share_acknowledge_wire, share_fetch_wire, share_record_states,
        },
        share_partition::state::RecordState::{self, Acquired},
        test_support::start_broker_with_authorizer_no_audit,
    },
    assert2::assert,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request::{
                AcknowledgementBatch as FetchAcknowledgeBatch, FetchPartition, FetchTopic,
                ShareFetchRequest,
            },
            share_fetch_response::ShareFetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

const TOPIC: &str = "ack-order";
const VERSION: i16 = 2;

/// The principal that the topic `Read` check refuses.
const DENIED: &str = "denied";
/// The principal that may read everything.
const READER: &str = "reader";

/// A partition that the one-partition topic does not have.
const MISSING_PARTITION: i32 = 3;

const ACCEPT: i8 = 1;
const RENEW: i8 = 4;

/// One acknowledgement batch: its offsets and types.
type Batch = WireAckBatch<'static>;

/// The row's `(error_code, acknowledge_error_code)` of a `ShareFetch`.
#[derive(Clone, Copy, PartialEq, Eq)]
struct RowCodes {
    error_code: i16,
    acknowledge_error_code: i16,
}

/// Denies topic `Read` to [`DENIED`] and allows everything else.
#[derive(Debug)]
struct DenyOnePrincipal;

krabka_macros::test_authorizer!(DenyOnePrincipal, request; {
    if request.principal.name == DENIED
        && request.resource_type == ResourceType::Topic
        && request.operation == AclOperation::Read
    {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

async fn start() -> StartedBroker {
    let HandlerStartedBroker { broker, dir } =
        start_broker_with_authorizer_no_audit(Arc::new(DenyOnePrincipal)).await;
    StartedBroker { broker, dir }
}

async fn create_topic(broker: &BrokerHandle) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("ack-order-test")
        .name(TOPIC)
        .call()
        .await
}

/// Appends one batch of three records to partition 0.
async fn produce(broker: &BrokerHandle) {
    produce_records().broker(broker).topic(TOPIC).call().await;
}

/// Starts `group` at the earliest offset and lets [`READER`] acquire offsets
/// 0 to 2 at epoch 0.
async fn acquire_all(broker: &BrokerHandle, group: &str, topic_id: WireUuid) {
    initialize_earliest_share(broker, group, topic_id, 0..1).await;
    let response = share_fetch()
        .broker(broker)
        .group(group)
        .epoch(0)
        .topic_id(topic_id)
        .call()
        .await;
    let acquired: Vec<_> = acquired_share_records(&response.responses[0].partitions[0]);
    assert!(acquired == vec![OffsetRun::new(0, 2)], "{response:?}");
}

/// A `ShareFetch` that names each `(partition, batches)` row of `rows`.
#[derive(krabka_macros::FieldDefaults)]
struct ValidationFetchSetup<'a> {
    #[default(READER)]
    user: &'a str,
    #[default("ack-order")]
    group: &'a str,
    epoch: ShareSessionEpoch,
    topic_id: WireUuid,
    #[default(vec![AcknowledgementPartitionSetup::default()])]
    rows: Vec<AcknowledgementPartitionSetup>,
    mode: AcknowledgementMode,
}

#[bon::builder]
async fn share_fetch(
    broker: &BrokerHandle,
    #[builder(default = READER)] user: &'_ str,
    #[builder(default = "ack-order")] group: &'_ str,
    #[builder(into, default)] epoch: ShareSessionEpoch,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default = vec ! [AcknowledgementPartitionSetup :: default ()])] rows: Vec<
        AcknowledgementPartitionSetup,
    >,
    #[builder(default)] mode: AcknowledgementMode,
) -> ShareFetchResponse {
    let setup = ValidationFetchSetup {
        user,
        group,
        epoch,
        topic_id,
        rows,
        mode,
    };
    let request = ShareFetchRequest {
        group_id: Some(setup.group.into()),
        member_id: Some("member".into()),
        share_session_epoch: setup.epoch.0,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        is_renew_ack: matches!(setup.mode, AcknowledgementMode::Renew),
        topics: vec![FetchTopic {
            topic_id: setup.topic_id,
            partitions: setup
                .rows
                .into_iter()
                .map(|row| FetchPartition {
                    partition_index: row.index.0,
                    acknowledgement_batches: krabka_macros::acknowledgement_batches!(FetchAcknowledgeBatch; [row.batches]),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    share_fetch_wire()
        .broker(broker)
        .request(&request)
        .version(VERSION)
        .user(setup.user)
        .call()
        .await
}

#[derive(krabka_macros::FieldDefaults)]
struct ValidationAcknowledgeSetup<'a> {
    #[default(READER)]
    user: &'a str,
    #[default("ack-order")]
    group: &'a str,
    topic_id: WireUuid,
    partition: AcknowledgementPartitionSetup,
}

#[bon::builder]
async fn share_acknowledge(
    broker: &BrokerHandle,
    #[builder(default = READER)] user: &'_ str,
    #[builder(default = "ack-order")] group: &'_ str,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default)] partition: AcknowledgementPartitionSetup,
) -> ShareAcknowledgeResponse {
    let setup = ValidationAcknowledgeSetup {
        user,
        group,
        topic_id,
        partition,
    };
    let request = acknowledge_batches_request()
        .group(setup.group)
        .topic_id(setup.topic_id)
        .partition(setup.partition)
        .call();
    share_acknowledge_wire()
        .broker(broker)
        .request(&request)
        .version(VERSION)
        .user(setup.user)
        .call()
        .await
}

async fn record_states(broker: &BrokerHandle, group: &str, topic_id: WireUuid) -> Vec<RecordState> {
    share_record_states(broker, group, topic_id).await
}

/// One scenario for both APIs.
struct Case {
    name: &'static str,
    user: &'static str,
    partition: i32,
    batches: &'static [Batch],
    /// `ShareFetch`: the row's error codes.
    fetch: RowCodes,
    /// `ShareAcknowledge`: the row's error code.
    acknowledge: i16,
    /// The state of each offset left in the window of partition 0 after the
    /// request.
    states: &'static [RecordState],
}

const HELD: &[RecordState] = &[Acquired, Acquired, Acquired];

fn cases() -> Vec<Case> {
    let invalid = |name, batches| Case {
        name,
        user: READER,
        partition: 0,
        batches,
        fetch: RowCodes {
            error_code: codes::NONE,
            acknowledge_error_code: codes::INVALID_REQUEST,
        },
        acknowledge: codes::INVALID_REQUEST,
        states: HELD,
    };
    vec![
        Case {
            name: "allowed and valid",
            user: READER,
            partition: 0,
            batches: &[WireAckBatch {
                first: 0,
                last: 2,
                types: &[ACCEPT],
            }],
            fetch: RowCodes {
                error_code: codes::NONE,
                acknowledge_error_code: codes::NONE,
            },
            acknowledge: codes::NONE,
            // The accepted prefix leaves the window with the SPSO.
            states: &[],
        },
        Case {
            name: "denied and valid",
            user: DENIED,
            partition: 0,
            batches: &[WireAckBatch {
                first: 0,
                last: 2,
                types: &[ACCEPT],
            }],
            fetch: RowCodes {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                acknowledge_error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            },
            acknowledge: codes::TOPIC_AUTHORIZATION_FAILED,
            states: HELD,
        },
        Case {
            name: "denied and invalid",
            user: DENIED,
            partition: 0,
            batches: &[WireAckBatch {
                first: 2,
                last: 0,
                types: &[ACCEPT],
            }],
            fetch: RowCodes {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                acknowledge_error_code: codes::INVALID_REQUEST,
            },
            acknowledge: codes::INVALID_REQUEST,
            states: HELD,
        },
        invalid("first offset past last", &[WireAckBatch {
            first: 2,
            last: 0,
            types: &[ACCEPT],
        }]),
        invalid("overlapping batches", &[
            WireAckBatch {
                first: 0,
                last: 1,
                types: &[ACCEPT],
            },
            WireAckBatch {
                first: 0,
                last: 2,
                types: &[ACCEPT],
            },
        ]),
        invalid("no acknowledge type", &[WireAckBatch {
            first: 0,
            last: 2,
            types: &[],
        }]),
        invalid("type count not the range", &[WireAckBatch {
            first: 0,
            last: 2,
            types: &[ACCEPT, ACCEPT],
        }]),
        invalid("type out of range", &[WireAckBatch {
            first: 0,
            last: 2,
            types: &[5],
        }]),
        invalid("renew without IsRenewAck", &[WireAckBatch {
            first: 0,
            last: 2,
            types: &[RENEW],
        }]),
        Case {
            name: "partition the metadata does not hold",
            user: READER,
            partition: MISSING_PARTITION,
            batches: &[WireAckBatch {
                first: 0,
                last: 0,
                types: &[ACCEPT],
            }],
            fetch: RowCodes {
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                acknowledge_error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            },
            acknowledge: codes::UNKNOWN_TOPIC_OR_PARTITION,
            states: HELD,
        },
    ]
}

#[tokio::test]
async fn acknowledge_checks_run_in_kafka_order() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let topic_id = create_topic(&broker).await;
    produce(&broker).await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (index, case) in cases().into_iter().enumerate() {
        let group = format!("fetch-{index}");
        acquire_all(&broker, &group, topic_id).await;
        let response = share_fetch()
            .broker(&broker)
            .user(case.user)
            .group(&group)
            .topic_id(topic_id)
            .rows(
                [WirePartitionAcknowledgements {
                    index: case.partition,
                    batches: case.batches,
                }]
                .iter()
                .copied()
                .map(AcknowledgementPartitionSetup::from_wire)
                .collect(),
            )
            .call()
            .await;
        let row = &response.responses[0].partitions[0];
        actual.push((
            "ShareFetch",
            case.name,
            (
                row.partition_index,
                row.error_code,
                row.acknowledge_error_code,
            ),
            record_states(&broker, &group, topic_id).await,
        ));
        expected.push((
            "ShareFetch",
            case.name,
            (
                case.partition,
                case.fetch.error_code,
                case.fetch.acknowledge_error_code,
            ),
            case.states.to_vec(),
        ));

        let group = format!("acknowledge-{index}");
        acquire_all(&broker, &group, topic_id).await;
        let response = share_acknowledge()
            .broker(&broker)
            .user(case.user)
            .group(&group)
            .topic_id(topic_id)
            .partition(AcknowledgementPartitionSetup::from_wire(
                WirePartitionAcknowledgements {
                    index: case.partition,
                    batches: case.batches,
                },
            ))
            .call()
            .await;
        let row = &response.responses[0].partitions[0];
        actual.push((
            "ShareAcknowledge",
            case.name,
            (row.partition_index, row.error_code, codes::NONE),
            record_states(&broker, &group, topic_id).await,
        ));
        expected.push((
            "ShareAcknowledge",
            case.name,
            (case.partition, case.acknowledge, codes::NONE),
            case.states.to_vec(),
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A denied topic row without acknowledgements in a request without any
/// acknowledgement answers only the fetch error.
#[tokio::test]
async fn a_denied_row_without_acknowledgements_has_no_acknowledge_error() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let topic_id = create_topic(&broker).await;
    produce(&broker).await;
    acquire_all(&broker, "no-acks", topic_id).await;

    let response = share_fetch()
        .broker(&broker)
        .user(DENIED)
        .group("no-acks")
        .topic_id(topic_id)
        .call()
        .await;
    let row = &response.responses[0].partitions[0];

    assert!(
        RowCodes {
            error_code: row.error_code,
            acknowledge_error_code: row.acknowledge_error_code
        } == RowCodes {
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            acknowledge_error_code: codes::NONE
        }
    );
    broker.shutdown().await;
}
