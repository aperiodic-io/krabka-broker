//! Handler tests for how `Metadata` authorizes, auto-creates and orders its
//! topic rows, after Kafka's `KafkaApis.handleTopicMetadataRequest`.
//!
//! One broker serves every case that shares an `auto.create.topics.enable`
//! value. The principal under test holds exactly the grants a case lists, and
//! each case asks about topics of its own, so a topic one case auto-creates is
//! never missing in another.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use assert2::assert;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    owned::{
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::{MetadataResponse, MetadataResponseTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::{AuthorizationResult, Authorizer},
    broker::BrokerHandle,
    codes,
    handlers::{acl_wire::CLUSTER_RESOURCE_NAME, test_support::CreateTopicSetup},
    test_support::{peer, principal, start_broker_no_audit_with},
};

/// The principal whose grants each case sets.
const TESTER: &str = "tester";

/// The topic that exists before any case runs.
const EXISTING: &str = "authz-existing";

/// The wire bit of an operation in an authorized-operations field (KIP-430):
/// `1 << AclOperation.code`.
const DESCRIBE_BIT: i32 = 1 << 8;
const CREATE_BIT: i32 = 1 << 5;

/// One grant: a resource type, a resource name and an operation.
type Grant = (ResourceType, String, AclOperation);

/// Allows [`TESTER`] exactly the grants it holds, and every other principal
/// everything, so the broker's own setup is never refused.
#[derive(Debug, Default)]
struct Grants(Mutex<HashSet<Grant>>);

impl Grants {
    fn set(&self, grants: &[Grant]) {
        *self.0.lock().expect("grants") = grants.iter().cloned().collect();
    }
}

test_authorizer!(Grants, (self, _source, request), {
    if request.principal.name != TESTER
        || self.0.lock().expect("grants").contains(&(
            request.resource_type,
            request.resource_name.to_owned(),
            request.operation,
        ))
    {
        AuthorizationResult::Allow
    } else {
        AuthorizationResult::Deny
    }
});

fn topic(name: &str, operation: AclOperation) -> Grant {
    (ResourceType::Topic, name.to_owned(), operation)
}

fn cluster(operation: AclOperation) -> Grant {
    (
        ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME.to_owned(),
        operation,
    )
}

struct Fixture {
    broker: BrokerHandle,
    grants: Arc<Grants>,
    _dir: tempfile::TempDir,
}

/// A broker whose `auto.create.topics.enable` is `auto_create_topics_enable`,
/// holding [`EXISTING`].
async fn start(auto_create_topics_enable: bool) -> Fixture {
    let grants = Arc::new(Grants::default());
    let authorizer: Arc<dyn Authorizer> = Arc::clone(&grants) as Arc<dyn Authorizer>;
    let (broker, dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = authorizer;
        cfg.auto_create_topics_enable = auto_create_topics_enable;
    })
    .await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("metadata-authorization-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(crate::handlers::test_support::configured_topic_request(
            CreateTopicSetup {
                topic: EXISTING,
                ..Default::default()
            },
        ))
        .await
        .expect("CreateTopics");
    assert!(
        response
            .topics
            .iter()
            .all(|topic| topic.error_code == codes::NONE),
        "{response:?}"
    );
    broker.wait_until_partition_present(EXISTING, 0).await;
    Fixture {
        broker,
        grants,
        _dir: dir,
    }
}

impl Fixture {
    async fn metadata(&self, version: i16, request: &MetadataRequest) -> MetadataResponse {
        let shared = self.broker.broker_arc_for_test();
        request_identity!(
            (user, address, ctx),
            principal(TESTER),
            client_id = "metadata-client"
        );
        crate::test_support::dispatch_wire(
            &shared,
            krabka_protocol::api_key::ApiKey::Metadata as i16,
            version,
            request,
            &ctx,
        )
        .await
    }

    /// The row an allow-all principal gets for [`EXISTING`] at `version`.
    async fn existing_row(&self, version: i16) -> MetadataResponseTopic {
        self.grants.set(&[topic(EXISTING, AclOperation::Describe)]);
        let response = self
            .metadata(version, &named(&[EXISTING], false, false))
            .await;
        let [row] = <[MetadataResponseTopic; 1]>::try_from(response.topics).expect("one row");
        assert!(row.error_code == codes::NONE);
        row
    }
}

fn named(names: &[&str], allow_auto_topic_creation: bool, include_ops: bool) -> MetadataRequest {
    MetadataRequest {
        topics: Some(
            names
                .iter()
                .map(|name| MetadataRequestTopic {
                    name: Some((*name).to_owned()),
                    ..Default::default()
                })
                .collect(),
        ),
        allow_auto_topic_creation,
        include_topic_authorized_operations: include_ops,
        ..Default::default()
    }
}

/// A partitionless row for a topic that is not described in full.
fn row(error_code: i16, name: &str) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code,
        name: Some(name.to_owned()),
        topic_id: WireUuid::ZERO,
        ..Default::default()
    }
}

/// One expected row of a case.
#[derive(Debug, Clone)]
enum Expect {
    /// The full row of [`EXISTING`], with these authorized operations.
    Existing(i32),
    /// A partitionless row.
    Row(i16, &'static str, i32),
}

struct Case {
    name: &'static str,
    /// The broker's `auto.create.topics.enable`.
    auto_create_topics_enable: bool,
    version: i16,
    topics: &'static [&'static str],
    allow_auto_topic_creation: bool,
    include_ops: bool,
    grants: Vec<Grant>,
    rows: Vec<Expect>,
    /// The missing topics the case auto-creates.
    creates: &'static [&'static str],
}

/// The cases of [`topic_rows_follow_kafka_authorization_and_auto_creation`].
fn topic_row_cases() -> Vec<Case> {
    use AclOperation::{Create, Describe};
    use Expect::{Existing, Row};
    vec![
        Case {
            name: "cluster Create auto-creates a missing topic and answers 3",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz-cluster-create"],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![topic("authz-cluster-create", Describe), cluster(Create)],
            rows: vec![Row(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                "authz-cluster-create",
                i32::MIN,
            )],
            creates: &["authz-cluster-create"],
        },
        Case {
            name: "topic Create alone auto-creates a missing topic",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz-topic-create"],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![
                topic("authz-topic-create", Describe),
                topic("authz-topic-create", Create),
            ],
            rows: vec![Row(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                "authz-topic-create",
                i32::MIN,
            )],
            creates: &["authz-topic-create"],
        },
        Case {
            name: "no Create answers 29 with the zero id",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz-no-create"],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![topic("authz-no-create", Describe)],
            rows: vec![Row(
                codes::TOPIC_AUTHORIZATION_FAILED,
                "authz-no-create",
                i32::MIN,
            )],
            creates: &[],
        },
        Case {
            name: "no auto-creation asked needs no Create",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz-no-auto"],
            allow_auto_topic_creation: false,
            include_ops: false,
            grants: vec![topic("authz-no-auto", Describe)],
            rows: vec![Row(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                "authz-no-auto",
                i32::MIN,
            )],
            creates: &[],
        },
        Case {
            name: "an invalid name answers 17 and is not created",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz bad name"],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![topic("authz bad name", Describe), cluster(Create)],
            rows: vec![Row(
                codes::INVALID_TOPIC_EXCEPTION,
                "authz bad name",
                i32::MIN,
            )],
            creates: &[],
        },
        Case {
            name: "rows are described, then denied Create, then denied Describe",
            auto_create_topics_enable: true,
            version: 12,
            topics: &["authz-hidden", "authz-order-no-create", EXISTING],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![
                topic(EXISTING, Describe),
                topic("authz-order-no-create", Describe),
            ],
            rows: vec![
                Existing(i32::MIN),
                Row(
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    "authz-order-no-create",
                    i32::MIN,
                ),
                Row(codes::TOPIC_AUTHORIZATION_FAILED, "authz-hidden", i32::MIN),
            ],
            creates: &[],
        },
        Case {
            name: "a repeated name answers one row",
            auto_create_topics_enable: true,
            version: 12,
            topics: &[EXISTING, EXISTING, "authz-twice", "authz-twice"],
            allow_auto_topic_creation: false,
            include_ops: false,
            grants: vec![topic(EXISTING, Describe), topic("authz-twice", Describe)],
            rows: vec![
                Existing(i32::MIN),
                Row(codes::UNKNOWN_TOPIC_OR_PARTITION, "authz-twice", i32::MIN),
            ],
            creates: &[],
        },
        Case {
            name: "the missing topic row carries its authorized operations",
            auto_create_topics_enable: true,
            version: 10,
            topics: &["authz-ops-missing", EXISTING],
            allow_auto_topic_creation: false,
            include_ops: true,
            grants: vec![
                topic(EXISTING, Describe),
                topic("authz-ops-missing", Describe),
                topic("authz-ops-missing", Create),
            ],
            rows: vec![
                Existing(DESCRIBE_BIT),
                Row(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "authz-ops-missing",
                    DESCRIBE_BIT | CREATE_BIT,
                ),
            ],
            creates: &[],
        },
        Case {
            name: "auto.create.topics.enable = false answers 3 and creates nothing",
            auto_create_topics_enable: false,
            version: 12,
            topics: &["authz-auto-create-disabled"],
            allow_auto_topic_creation: true,
            include_ops: false,
            grants: vec![
                topic("authz-auto-create-disabled", Describe),
                cluster(Create),
            ],
            rows: vec![Row(
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                "authz-auto-create-disabled",
                i32::MIN,
            )],
            creates: &[],
        },
    ]
}

/// Kafka's `handleTopicMetadataRequest`: `Describe` first, then cluster
/// `Create`, then topic `Create`, for a missing topic that the request asks to
/// auto-create on a broker whose `auto.create.topics.enable` holds, and no
/// auto-creation at all on one where it does not; rows ordered described, denied `Create`, denied `Describe`;
/// one row per distinct name; and the topic authorized operations on every
/// described row, the rows for missing topics included.
#[tokio::test]
async fn topic_rows_follow_kafka_authorization_and_auto_creation() {
    use Expect::{Existing, Row};
    let enabled = start(true).await;
    let disabled = start(false).await;
    for case in topic_row_cases() {
        let fixture = if case.auto_create_topics_enable {
            &enabled
        } else {
            &disabled
        };
        let existing = fixture.existing_row(case.version).await;
        fixture.grants.set(&case.grants);
        let response = fixture
            .metadata(
                case.version,
                &named(
                    case.topics,
                    case.allow_auto_topic_creation,
                    case.include_ops,
                ),
            )
            .await;
        let expected: Vec<MetadataResponseTopic> = case
            .rows
            .iter()
            .map(|expect| match expect {
                Existing(ops) => MetadataResponseTopic {
                    topic_authorized_operations: *ops,
                    ..existing.clone()
                },
                Row(error_code, name, ops) => MetadataResponseTopic {
                    topic_authorized_operations: *ops,
                    ..row(*error_code, name)
                },
            })
            .collect();
        assert!(response.topics == expected, "{}", case.name);
        for name in case.creates {
            fixture.broker.wait_until_partition_present(name, 0).await;
        }
        let image = fixture.broker.controller_image_for_test();
        for name in case.topics {
            if !case.creates.contains(name) && *name != EXISTING {
                assert!(image.topic(name).is_none(), "{}: {name}", case.name);
            }
        }
    }
    enabled.broker.shutdown().await;
    disabled.broker.shutdown().await;
}

/// KIP-430 `cluster_authorized_operations`: Kafka answers 0 when `Describe`
/// on the cluster is denied, whatever else the principal holds, and the bit
/// field when it is allowed.
#[tokio::test]
async fn cluster_authorized_operations_need_cluster_describe() {
    let cases = [
        ("Create only", vec![cluster(AclOperation::Create)], 0),
        (
            "Describe and Create",
            vec![
                cluster(AclOperation::Describe),
                cluster(AclOperation::Create),
            ],
            DESCRIBE_BIT | CREATE_BIT,
        ),
    ];
    let fixture = start(true).await;
    for (name, grants, expected) in cases {
        fixture.grants.set(&grants);
        let request = MetadataRequest {
            topics: Some(Vec::new()),
            include_cluster_authorized_operations: true,
            ..Default::default()
        };
        let response = fixture.metadata(8, &request).await;
        assert!(response.cluster_authorized_operations == expected, "{name}");
    }
    fixture.broker.shutdown().await;
}

/// `MetadataRequest.isAllTopics`: at version 0 an empty topic list asks for
/// every topic, and like any all-topics request it leaves out the topics the
/// principal may not describe.
#[tokio::test]
async fn an_empty_version_0_request_asks_for_every_topic() {
    let fixture = start(true).await;
    let existing = fixture.existing_row(0).await;
    fixture
        .grants
        .set(&[topic(EXISTING, AclOperation::Describe)]);
    let response = fixture
        .metadata(
            0,
            &MetadataRequest {
                topics: Some(Vec::new()),
                ..Default::default()
            },
        )
        .await;
    assert!(response.topics == vec![existing]);

    fixture.grants.set(&[]);
    let response = fixture
        .metadata(
            0,
            &MetadataRequest {
                topics: Some(Vec::new()),
                ..Default::default()
            },
        )
        .await;
    assert!(response.topics == Vec::<MetadataResponseTopic>::new());
    fixture.broker.shutdown().await;
}

/// The checks that Kafka's `KafkaApis.handleTopicMetadataRequest` makes with
/// `logIfDenied = false` leave no Deny in `authorization_denied_total` (or in
/// the audit trail behind it): the `Describe` filter of an all-topics request,
/// the cluster `Create` probe for auto-creation, and every operation of an
/// authorized-operations field. A `Describe` denied on a topic the request
/// names is a refusal and is counted.
#[tokio::test]
async fn quiet_probes_leave_no_denial_behind_and_a_named_refusal_does() {
    use AclOperation::{Alter, Create, Describe};

    use crate::metrics::AuthorizationDeniedLabel;

    let fixture = start(true).await;
    let shared = fixture.broker.broker_arc_for_test();
    let denied = |operation: &str, resource_type: &str| {
        shared
            .metrics
            .authorization_denied
            .get_or_create(&AuthorizationDeniedLabel {
                operation: operation.into(),
                resource_type: resource_type.into(),
            })
            .get()
    };

    // An all-topics request includes an allowed topic and quietly hides one
    // the principal may not describe.
    let existing = fixture.existing_row(12).await;
    let all_topics = MetadataRequest {
        topics: None,
        ..Default::default()
    };
    for (grants, expected) in [
        (vec![topic(EXISTING, Describe)], vec![existing]),
        (vec![], vec![]),
    ] {
        fixture.grants.set(&grants);
        let response = fixture.metadata(12, &all_topics).await;
        assert!(response.topics == expected);
        assert!(denied("Describe", "Topic") == 0);
    }

    // Auto-creation with topic `Create` alone: the cluster `Create` probe is
    // denied, and the per-topic check allows the topic.
    fixture.grants.set(&[
        topic("authz-quiet-probe", Describe),
        topic("authz-quiet-probe", Create),
    ]);
    let response = fixture
        .metadata(12, &named(&["authz-quiet-probe"], true, false))
        .await;
    assert!(response.topics[0].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION);
    fixture
        .broker
        .wait_until_partition_present("authz-quiet-probe", 0)
        .await;
    assert!(denied("Create", "Cluster") == 0);
    assert!(denied("Create", "Topic") == 0);

    // The operations of a topic the principal may describe, all but `Describe`
    // denied: a probe, not a refusal.
    fixture.grants.set(&[topic(EXISTING, Describe)]);
    let response = fixture.metadata(12, &named(&[EXISTING], false, true)).await;
    assert!(response.topics[0].topic_authorized_operations == DESCRIBE_BIT);
    assert!(denied("Alter", "Topic") == 0);

    // The same topic named without the `Describe` grant is a refusal.
    fixture.grants.set(&[topic(EXISTING, Alter)]);
    let response = fixture
        .metadata(12, &named(&[EXISTING], false, false))
        .await;
    assert!(response.topics[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    assert!(denied("Describe", "Topic") == 1);
    fixture.broker.shutdown().await;
}
