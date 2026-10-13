//! Handler tests for Kafka's version rules for topic ids in `Metadata`.
//!
//! `KafkaApis.handleTopicMetadataRequest` refuses a null name or a non-zero id
//! at versions 10 and 11. From version 12 on, a request with any non-zero id
//! describes only its ids, and ignores every name and every zero-id row. A
//! request with only zero ids describes its names, and a null name among them
//! fails the whole request. Each table row is one request, and the tests
//! compare the whole response as a client decodes it.

use {
    crate::{
        authorizer::{AllowAllAuthorizer, AuthorizationResult, Authorizer},
        broker::BrokerHandle,
        codes,
        test_support::{dispatch_wire, peer, principal, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The two topics that every test creates.
const TOPIC_A: &str = "resolution-a";
const TOPIC_B: &str = "resolution-b";

/// Denies `Describe` on every topic and allows everything else, so that topic
/// creation still works and only the per-topic gate refuses.
#[derive(Debug)]
struct DenyTopicDescribe;

test_authorizer!(DenyTopicDescribe, (self, _source, request), {
    if request.resource_type == ResourceType::Topic && request.operation == AclOperation::Describe {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// A request row: the name, if any, and the kind of id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Name {
    A,
    Unknown,
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Id {
    A,
    B,
    Unknown,
    Zero,
}

/// What Kafka answers for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expect {
    /// The ordinary response with these topic rows.
    Rows(Vec<Row>),
    /// `MetadataRequest.getErrorResponse` with this error code.
    Failed(i16),
}

/// One expected topic row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Row {
    /// The full metadata of topic A or topic B.
    Described(Id),
    /// `UNKNOWN_TOPIC_ID` with a null name and the unknown id.
    UnknownId,
    /// `UNKNOWN_TOPIC_OR_PARTITION` with the unknown name and the zero id.
    UnknownName,
    /// `TOPIC_AUTHORIZATION_FAILED` with a null name and the id of topic A.
    DeniedId,
    /// `TOPIC_AUTHORIZATION_FAILED` with the name of topic A and the zero id.
    DeniedName,
}

/// A topic reference of a request: the name and the id it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestedTopic {
    name: Name,
    id: Id,
}

impl RequestedTopic {
    const fn new(name: Name, id: Id) -> Self { Self { name, id } }
}

/// One table row.
#[derive(Debug, Clone)]
struct Case {
    version: i16,
    rows: Vec<RequestedTopic>,
    expect: Expect,
}

struct Fixture {
    broker: BrokerHandle,
    _dir: tempfile::TempDir,
    id_a: WireUuid,
    id_b: WireUuid,
}

async fn start(authorizer: Arc<dyn Authorizer>) -> Fixture {
    let (broker, dir) = start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("metadata-resolution-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: [TOPIC_A, TOPIC_B]
                .into_iter()
                .map(|name| CreatableTopic {
                    name: name.to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    for name in [TOPIC_A, TOPIC_B] {
        broker.wait_until_partition_present(name, 0).await;
    }
    let image = broker.controller_image_for_test();
    let id = |name| WireUuid(image.topic(name).expect("topic").topic_id.into_bytes());
    let (id_a, id_b) = (id(TOPIC_A), id(TOPIC_B));
    Fixture {
        broker,
        _dir: dir,
        id_a,
        id_b,
    }
}

async fn metadata(
    broker: &BrokerHandle,
    version: i16,
    request: &MetadataRequest,
) -> MetadataResponse {
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("describer"),
        client_id = "metadata-client"
    );
    dispatch_wire(
        &shared,
        krabka_protocol::api_key::ApiKey::Metadata as i16,
        version,
        request,
        &ctx,
    )
    .await
}

impl Fixture {
    fn id(&self, id: Id) -> WireUuid {
        match id {
            Id::A => self.id_a,
            Id::B => self.id_b,
            Id::Unknown => UNKNOWN_ID,
            Id::Zero => WireUuid::ZERO,
        }
    }

    fn request(&self, rows: &[RequestedTopic]) -> MetadataRequest {
        MetadataRequest {
            topics: Some(
                rows.iter()
                    .map(|RequestedTopic { name, id }| MetadataRequestTopic {
                        name: match name {
                            Name::A => Some(TOPIC_A.to_string()),
                            Name::Unknown => Some(UNKNOWN_NAME.to_string()),
                            Name::Null => None,
                        },
                        topic_id: self.id(*id),
                        ..Default::default()
                    })
                    .collect(),
            ),
            allow_auto_topic_creation: false,
            ..Default::default()
        }
    }

    /// The expected response for `case`.
    ///
    /// The broker list, the cluster id, the controller and the rows of the
    /// described topics come from `baseline`, a v12 name-only request for both
    /// topics that an allow-all broker answered. The rules under test do not
    /// touch those parts.
    fn expected(&self, case: &Case, baseline: &MetadataResponse) -> MetadataResponse {
        match &case.expect {
            Expect::Failed(error_code) => {
                let request = self.request(&case.rows);
                MetadataResponse {
                    topics: request
                        .topics
                        .unwrap_or_default()
                        .into_iter()
                        .map(|topic| MetadataResponseTopic {
                            error_code: *error_code,
                            name: Some(topic.name.unwrap_or_default()),
                            topic_id: topic.topic_id,
                            ..Default::default()
                        })
                        .collect(),
                    // The wire carries the top-level error from v13 on.
                    error_code: if case.version >= 13 { *error_code } else { 0 },
                    ..Default::default()
                }
            }
            Expect::Rows(rows) => MetadataResponse {
                topics: rows
                    .iter()
                    .map(|row| match row {
                        Row::Described(id) => {
                            let name = if *id == Id::A { TOPIC_A } else { TOPIC_B };
                            baseline
                                .topics
                                .iter()
                                .find(|topic| topic.name.as_deref() == Some(name))
                                .expect("baseline row")
                                .clone()
                        }
                        Row::UnknownId => MetadataResponseTopic {
                            error_code: codes::UNKNOWN_TOPIC_ID,
                            name: None,
                            topic_id: UNKNOWN_ID,
                            ..Default::default()
                        },
                        Row::UnknownName => MetadataResponseTopic {
                            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                            name: Some(UNKNOWN_NAME.to_string()),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        },
                        Row::DeniedId => MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: None,
                            topic_id: self.id_a,
                            ..Default::default()
                        },
                        Row::DeniedName => MetadataResponseTopic {
                            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                            name: Some(TOPIC_A.to_string()),
                            topic_id: WireUuid::ZERO,
                            ..Default::default()
                        },
                    })
                    .collect(),
                ..baseline.clone()
            },
        }
    }

    async fn run(&self, cases: &[Case], baseline: &MetadataResponse) {
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for case in cases {
            let response = metadata(&self.broker, case.version, &self.request(&case.rows)).await;
            actual.push((case.version, case.rows.clone(), response));
            expected.push((
                case.version,
                case.rows.clone(),
                self.expected(case, baseline),
            ));
        }
        assert!(actual == expected);
    }
}

async fn baseline(fixture: &Fixture) -> MetadataResponse {
    let request = MetadataRequest {
        topics: Some(
            [TOPIC_A, TOPIC_B]
                .into_iter()
                .map(|name| MetadataRequestTopic {
                    name: Some(name.to_string()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    let response = metadata(&fixture.broker, 12, &request).await;
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    response
}

/// The response envelope for `fixture`'s own broker: `brokers`, `cluster_id`,
/// `controller_id` and `cluster_authorized_operations`, with no topic row.
///
/// An empty topic list asks for nothing, so no per-topic authorization runs,
/// and this works under any authorizer. `Fixture::expected` borrows these
/// fields for every case whose rows do not name `Row::Described`. Reading
/// them from `fixture` itself, rather than from a differently-authorized
/// sibling broker, matters because each `for_tests` broker binds its own
/// listener: two instances answer with two different ports, so a shared
/// baseline across instances no longer round-trips through `==`.
async fn envelope(fixture: &Fixture) -> MetadataResponse {
    let request = MetadataRequest {
        topics: Some(Vec::new()),
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    metadata(&fixture.broker, 12, &request).await
}

#[tokio::test]
async fn topic_ids_follow_kafka_version_rules() {
    use Expect::{Failed, Rows};
    let cases = vec![
        // Versions 10 and 11 refuse a non-zero id or a null name.
        Case {
            version: 11,
            rows: vec![RequestedTopic {
                name: Name::A,
                id: Id::Zero,
            }],
            expect: Rows(vec![Row::Described(Id::A)]),
        },
        Case {
            version: 11,
            rows: vec![RequestedTopic {
                name: Name::A,
                id: Id::A,
            }],
            expect: Failed(codes::INVALID_REQUEST),
        },
        Case {
            version: 10,
            rows: vec![RequestedTopic {
                name: Name::Null,
                id: Id::Zero,
            }],
            expect: Failed(codes::INVALID_REQUEST),
        },
        Case {
            version: 11,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Unknown,
                },
                RequestedTopic {
                    name: Name::A,
                    id: Id::Zero,
                },
            ],
            expect: Failed(codes::INVALID_REQUEST),
        },
        // From version 12 on, the ids win and the names are ignored.
        Case {
            version: 12,
            rows: vec![RequestedTopic::new(Name::A, Id::B)],
            expect: Rows(vec![Row::Described(Id::B)]),
        },
        Case {
            version: 12,
            rows: vec![
                RequestedTopic::new(Name::A, Id::Zero),
                RequestedTopic::new(Name::Null, Id::B),
            ],
            expect: Rows(vec![Row::Described(Id::B)]),
        },
        Case {
            version: 12,
            rows: vec![RequestedTopic {
                name: Name::A,
                id: Id::Unknown,
            }],
            expect: Rows(vec![Row::UnknownId]),
        },
        Case {
            version: 13,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Unknown,
                },
                RequestedTopic {
                    name: Name::Null,
                    id: Id::A,
                },
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Zero,
                },
            ],
            expect: Rows(vec![Row::UnknownId, Row::Described(Id::A)]),
        },
        Case {
            version: 13,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::A,
                },
                RequestedTopic {
                    name: Name::Null,
                    id: Id::A,
                },
            ],
            expect: Rows(vec![Row::Described(Id::A)]),
        },
        // A repeated name is described once (#640).
        Case {
            version: 12,
            rows: vec![
                RequestedTopic {
                    name: Name::A,
                    id: Id::Zero,
                },
                RequestedTopic {
                    name: Name::A,
                    id: Id::Zero,
                },
            ],
            expect: Rows(vec![Row::Described(Id::A)]),
        },
        // With only zero ids, the names are described, and a null name fails
        // the whole request.
        Case {
            version: 12,
            rows: vec![RequestedTopic {
                name: Name::Unknown,
                id: Id::Zero,
            }],
            expect: Rows(vec![Row::UnknownName]),
        },
        Case {
            version: 12,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Zero,
                },
                RequestedTopic {
                    name: Name::A,
                    id: Id::Zero,
                },
            ],
            expect: Failed(codes::UNKNOWN_SERVER_ERROR),
        },
        Case {
            version: 13,
            rows: vec![RequestedTopic {
                name: Name::Null,
                id: Id::Zero,
            }],
            expect: Failed(codes::UNKNOWN_SERVER_ERROR),
        },
    ];
    let fixture = start(Arc::new(AllowAllAuthorizer)).await;
    let baseline = baseline(&fixture).await;

    fixture.run(&cases, &baseline).await;
    fixture.broker.shutdown().await;
}

/// A denied id row carries a null name and the real id. A denied name row
/// carries the name and the zero id. An unknown id needs no authorization, and
/// Kafka lists it ahead of every other row.
#[tokio::test]
async fn a_denied_topic_row_follows_how_the_request_names_it() {
    use Expect::Rows;
    let cases = vec![
        Case {
            version: 13,
            rows: vec![
                RequestedTopic {
                    name: Name::Null,
                    id: Id::A,
                },
                RequestedTopic {
                    name: Name::Null,
                    id: Id::Unknown,
                },
            ],
            expect: Rows(vec![Row::UnknownId, Row::DeniedId]),
        },
        Case {
            version: 12,
            rows: vec![RequestedTopic {
                name: Name::A,
                id: Id::Zero,
            }],
            expect: Rows(vec![Row::DeniedName]),
        },
    ];
    let denied = start(Arc::new(DenyTopicDescribe)).await;
    let baseline = envelope(&denied).await;

    denied.run(&cases, &baseline).await;
    denied.broker.shutdown().await;
}
