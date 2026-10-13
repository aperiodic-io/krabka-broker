//! Handler tests for KIP-1222 renew acknowledgements on `ShareFetch` and
//! `ShareAcknowledge`.
//!
//! The Java client sets `IsRenewAck` for a whole request when any batch holds
//! the type `Renew` (4). Kafka's `SharePartition` renews only the offsets of
//! type 4 and applies the other types as usual. `share.renew.acknowledge.enable`
//! set to `false` refuses a renewal with `INVALID_RECORD_STATE`.
//! `KafkaApis.handleShareFetchRequest` refuses a renew-ack fetch whose
//! `MaxBytes`, `MinBytes`, `MaxRecords` or `MaxWaitMs` is not 0 with a
//! top-level `INVALID_REQUEST`, and runs no fetch for a valid one.

use {
    crate::{
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            AcknowledgementBatchSetup, AcknowledgementMode, AcknowledgementPartitionSetup,
            AcknowledgementSetup, ClientTopicSetup, ProduceRecordsSetup, RecordCount,
            ShareSessionEpoch, ShareWireSetup, acknowledge_batches_request, acquired_share_records,
            create_topic as create_topic_from_test_support, produce_records,
            share_acknowledge_wire, share_fetch_wire,
        },
        share_partition::state::RecordState::{self, Acknowledged, Acquired, Available},
        test_support::{KafkaErrorCode, initialize_share_state},
    },
    assert2::assert,
    krabka_metadata::{GroupConfigRecord, MetadataRecord},
    krabka_protocol::{
        owned::{
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request::{
                AcknowledgementBatch as FetchAcknowledgeBatch, ShareFetchRequest,
            },
            share_fetch_response::ShareFetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

/// The request version that carries `IsRenewAck`.
const VERSION: i16 = 2;

const ACCEPT: i8 = 1;
const RELEASE: i8 = 2;
const RENEW: i8 = 4;

/// One acknowledgement batch: `(first_offset, last_offset, types)`.
type Batch = (i64, i64, &'static [i8]);

use crate::{
    handlers::test_support::start_allow_all_no_audit as start,
    test_support::start_broker_no_audit_with,
};

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_from_test_support(broker, ClientTopicSetup {
        client_id: "share-renew-test",
        name,
        ..Default::default()
    })
    .await
}

/// Appends one batch of `count` records to partition 0 of `topic`.
async fn produce(broker: &BrokerHandle, topic: &str, count: RecordCount) {
    produce_records(broker, ProduceRecordsSetup {
        topic,
        count,
        ..Default::default()
    })
    .await;
}

/// The fetch limits of a `ShareFetch`.
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_bytes: FetchByteLimit,
    max_records: RecordCount,
}

#[derive(Debug, Clone, Copy)]
struct FetchByteLimit(i32);

const FETCH: Limits = Limits {
    max_bytes: FetchByteLimit(1 << 20),
    max_records: RecordCount(500),
};

const NO_FETCH: Limits = Limits {
    max_bytes: FetchByteLimit(0),
    max_records: RecordCount(0),
};

#[derive(Clone, krabka_macros::FieldDefaults)]
struct RenewFetchSetup<'a> {
    #[default("renew-group")]
    group: &'a str,
    #[default(ShareSessionEpoch(0))]
    epoch: ShareSessionEpoch,
    topic_id: WireUuid,
    mode: AcknowledgementMode,
    #[default(FETCH)]
    limits: Limits,
    batches: Vec<AcknowledgementBatchSetup>,
}

async fn share_fetch(broker: &BrokerHandle, setup: RenewFetchSetup<'_>) -> ShareFetchResponse {
    let request = fetch_request(setup);
    share_fetch_as(broker, "share-consumer", &request).await
}

async fn share_fetch_as(
    broker: &BrokerHandle,
    user: &str,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    share_fetch_wire(broker, request, ShareWireSetup {
        version: krabka_ids::ApiVersion(VERSION),
        user,
    })
    .await
}

async fn share_acknowledge(
    broker: &BrokerHandle,
    setup: AcknowledgementSetup<'_>,
) -> ShareAcknowledgeResponse {
    let request = acknowledge_batches_request(setup);
    share_acknowledge_wire(broker, &request, ShareWireSetup {
        version: krabka_ids::ApiVersion(VERSION),
        ..Default::default()
    })
    .await
}

/// Sets `share.renew.acknowledge.enable=false` on `group`.
async fn disable_renew(broker: &BrokerHandle, group: &str) {
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group.to_string(),
            configs: maplit::btreemap! {
                "share.renew.acknowledge.enable".to_owned() => "false".to_owned()
            },
        })])
        .await
        .expect("set the group config");
}

/// The request under test.
#[derive(Debug, Clone, Copy, Default)]
enum Call {
    #[default]
    ShareAcknowledge,
    ShareFetch(Limits),
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum RenewAvailability {
    #[default]
    Enabled,
    Disabled,
}

/// One scenario, overriding only the request and expectations it exercises.
#[derive(krabka_macros::FieldDefaults)]
struct RenewCaseSetup {
    #[default("renew-only")]
    name: &'static str,
    call: Call,
    availability: RenewAvailability,
    #[default(ack_batches(&[(0, 2, &[RENEW])]))]
    batches: Vec<AcknowledgementBatchSetup>,
    #[default(Outcome::expected(KafkaErrorCode(codes::NONE), Some(KafkaErrorCode(codes::NONE)), [Acquired, Acquired, Acquired]))]
    expected: Outcome,
}

/// Decode the literal acknowledgement wire tables into typed fixture ranges.
fn ack_batches(batches: &[Batch]) -> Vec<AcknowledgementBatchSetup> {
    AcknowledgementPartitionSetup::from_wire((0, batches)).batches
}

/// What a scenario observes: the top-level error, the partition error
/// (`ShareAcknowledge`) or acknowledge error (`ShareFetch`), the offsets that
/// the request acquired, and the state of each offset afterwards.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    error: i16,
    acknowledge_error: Option<i16>,
    acquired: Vec<(i64, i64)>,
    states: Vec<(i64, RecordState)>,
}

impl Outcome {
    fn expected(
        error: KafkaErrorCode,
        acknowledge_error: Option<KafkaErrorCode>,
        states: [RecordState; 3],
    ) -> Self {
        Self {
            error: error.0,
            acknowledge_error: acknowledge_error.map(|code| code.0),
            acquired: Vec::new(),
            states: vec![(0, states[0]), (1, states[1]), (2, states[2])],
        }
    }
}

fn cases() -> Vec<RenewCaseSetup> {
    const MIXED_BATCHES: &[Batch] = &[(0, 0, &[RENEW]), (1, 2, &[ACCEPT])];
    let mixed_outcome = || {
        Outcome::expected(
            KafkaErrorCode(codes::NONE),
            Some(KafkaErrorCode(codes::NONE)),
            [Acquired, Acknowledged, Acknowledged],
        )
    };
    vec![
        RenewCaseSetup {
            expected: Outcome::expected(
                KafkaErrorCode(codes::NONE),
                Some(KafkaErrorCode(codes::NONE)),
                [Acquired, Acquired, Acquired],
            ),
            ..Default::default()
        },
        RenewCaseSetup {
            name: "renew-and-accept-in-another-batch",
            batches: ack_batches(MIXED_BATCHES),
            expected: mixed_outcome(),
            ..Default::default()
        },
        RenewCaseSetup {
            name: "per-offset-renew-accept-release",
            batches: ack_batches(&[(0, 2, &[RENEW, ACCEPT, RELEASE])]),
            expected: Outcome::expected(
                KafkaErrorCode(codes::NONE),
                Some(KafkaErrorCode(codes::NONE)),
                [Acquired, Acknowledged, Available],
            ),
            ..Default::default()
        },
        RenewCaseSetup {
            name: "renew-disabled",
            availability: RenewAvailability::Disabled,
            expected: Outcome::expected(
                KafkaErrorCode(codes::NONE),
                Some(KafkaErrorCode(codes::INVALID_RECORD_STATE)),
                [Acquired, Acquired, Acquired],
            ),
            ..Default::default()
        },
        RenewCaseSetup {
            name: "fetch-renew-with-max-records",
            call: Call::ShareFetch(Limits {
                max_bytes: FetchByteLimit(0),
                max_records: RecordCount(500),
            }),
            batches: ack_batches(MIXED_BATCHES),
            expected: Outcome::expected(KafkaErrorCode(codes::INVALID_REQUEST), None, [
                Acquired, Acquired, Acquired,
            ]),
            ..Default::default()
        },
        RenewCaseSetup {
            name: "fetch-renew-with-zero-limits",
            call: Call::ShareFetch(NO_FETCH),
            batches: ack_batches(MIXED_BATCHES),
            expected: mixed_outcome(),
            ..Default::default()
        },
    ]
}

/// Each row acquires offsets 0-2 as one member, produces offsets 3-4 so that
/// a fetch could acquire more, and then sends the request under test.
#[tokio::test]
async fn renew_acknowledgements_renew_only_the_renew_offsets() {
    let (broker, _dir) = start().await;
    let shared = broker.broker_arc_for_test();

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let group = format!("renew-{}", case.name);
        let topic_id = create_topic(&broker, &group).await;
        initialize_share_state(&broker, &group, uuid::Uuid::from_bytes(topic_id.0), 0).await;
        if case.availability == RenewAvailability::Disabled {
            disable_renew(&broker, &group).await;
        }
        let opened = share_fetch(&broker, RenewFetchSetup {
            group: &group,
            topic_id,
            ..Default::default()
        })
        .await;
        assert!(opened.error_code == codes::NONE, "{opened:?}");
        produce(&broker, &group, RecordCount(3)).await;
        let fetched = share_fetch(&broker, RenewFetchSetup {
            group: &group,
            epoch: ShareSessionEpoch(1),
            topic_id,
            ..Default::default()
        })
        .await;
        assert!(
            fetched.responses[0].partitions[0].acquired_records.len() == 1,
            "{fetched:?}"
        );
        produce(&broker, &group, RecordCount(2)).await;

        let outcome = match case.call {
            Call::ShareAcknowledge => {
                let response = share_acknowledge(&broker, AcknowledgementSetup {
                    mode: AcknowledgementMode::Renew,
                    partition: AcknowledgementPartitionSetup {
                        batches: case.batches.clone(),
                        ..Default::default()
                    },
                    ..AcknowledgementSetup::for_topic_session(
                        &group,
                        ShareSessionEpoch(2),
                        topic_id,
                    )
                })
                .await;
                Outcome {
                    error: response.error_code,
                    acknowledge_error: response
                        .responses
                        .first()
                        .map(|topic| topic.partitions[0].error_code),
                    acquired: Vec::new(),
                    states: Vec::new(),
                }
            }
            Call::ShareFetch(limits) => {
                let response = share_fetch(&broker, RenewFetchSetup {
                    group: &group,
                    epoch: ShareSessionEpoch(2),
                    topic_id,
                    mode: AcknowledgementMode::Renew,
                    limits,
                    batches: case.batches.clone(),
                })
                .await;
                let row = response.responses.first().map(|topic| &topic.partitions[0]);
                Outcome {
                    error: response.error_code,
                    acknowledge_error: row.map(|row| row.acknowledge_error_code),
                    acquired: row.map(acquired_share_records).unwrap_or_default(),
                    states: Vec::new(),
                }
            }
        };
        let states = shared
            .share_partition_leaders
            .peek_for_test(&group, uuid::Uuid::from_bytes(topic_id.0), 0)
            .expect("the fetch cached the share partition")
            .lock()
            .await
            .record_states();

        actual.push((case.name, Outcome { states, ..outcome }));
        expected.push((case.name, case.expected));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The principal that [`DenyTopicReadToOne`] refuses.
const NO_TOPIC_READ: &str = "no-topic-read";

/// Denies topic `Read` to [`NO_TOPIC_READ`] and allows everything else.
#[derive(Debug)]
struct DenyTopicReadToOne;

test_authorizer!(DenyTopicReadToOne, (self, _source, request), {
    let denied = request.principal.name == NO_TOPIC_READ
        && request.resource_type == krabka_metadata::ResourceType::Topic
        && request.operation == krabka_metadata::AclOperation::Read;
    if denied {
        crate::authorizer::AuthorizationResult::Deny
    } else {
        crate::authorizer::AuthorizationResult::Allow
    }
});

/// A renew-ack fetch runs only the acknowledgement path, so a denied topic
/// `Read` is the acknowledge error of the row, and the fetch error stays
/// `NONE`.
#[tokio::test]
async fn a_renew_fetch_answers_a_denied_topic_as_an_acknowledge_error() {
    let (broker, _dir) =
        start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(DenyTopicReadToOne)).await;
    let topic_id = create_topic(&broker, "renew-denied").await;
    initialize_share_state(
        &broker,
        "renew-denied",
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let request = |setup: RenewFetchSetup<'_>| {
        fetch_request(RenewFetchSetup {
            group: "renew-denied",
            topic_id,
            ..setup
        })
    };

    let opened = share_fetch_as(&broker, NO_TOPIC_READ, &request(RenewFetchSetup::default())).await;
    let renewed = share_fetch_as(
        &broker,
        NO_TOPIC_READ,
        &request(RenewFetchSetup {
            epoch: ShareSessionEpoch(1),
            mode: AcknowledgementMode::Renew,
            limits: NO_FETCH,
            batches: ack_batches(&[(0, 0, &[RENEW])]),
            ..Default::default()
        }),
    )
    .await;

    let row = |response: &ShareFetchResponse| {
        let partition = &response.responses[0].partitions[0];
        (partition.error_code, partition.acknowledge_error_code)
    };
    assert!(
        (row(&opened), row(&renewed))
            == (
                (codes::TOPIC_AUTHORIZATION_FAILED, codes::NONE),
                (codes::NONE, codes::TOPIC_AUTHORIZATION_FAILED)
            )
    );
    broker.shutdown().await;
}

fn fetch_request(setup: RenewFetchSetup<'_>) -> ShareFetchRequest {
    let RenewFetchSetup {
        group,
        epoch,
        topic_id,
        mode,
        limits,
        batches,
    } = setup;
    let acknowledgement_batches = batches
        .into_iter()
        .map(|batch| FetchAcknowledgeBatch {
            first_offset: batch.first_offset.0,
            last_offset: batch.last_offset.0,
            acknowledge_types: batch.types.into_iter().map(|code| code.0).collect(),
            ..Default::default()
        })
        .collect();
    super::test_fetch_request()
        .group_id(group.into())
        .member_id("member".into())
        .topic_id(topic_id.0)
        .share_session_epoch(epoch.0)
        .max_bytes(limits.max_bytes.0)
        .max_records(limits.max_records.0)
        .acknowledgement_batches(acknowledgement_batches)
        .is_renew_ack(matches!(mode, AcknowledgementMode::Renew))
        .call()
}
