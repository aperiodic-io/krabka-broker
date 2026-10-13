//! Tests for the `DeleteTopics` handler driven over the wire against a live
//! in-process broker.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{
        test_support::{DOOMED, gated_config, id_state, named_state, request},
        *,
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        codes,
        config::BreakGlassConfig,
        handlers::{
            RequestContext, acl_test_support as acl_fixtures,
            acl_wire::CLUSTER_RESOURCE_NAME,
            create_topics::handle as create_topics_handle,
            test_support::{
                self as handler_fixture_support, AclSetup, CreateTopicSetup, acl, cluster_acl,
                configured_topic_request_from,
            },
        },
        metrics::AuthorizationDeniedLabel,
        test_support::{
            allow_acl, dispatch_wire, start_broker_no_audit_with,
            start_broker_with_authorizer_no_audit as start_broker,
        },
    },
    assert2::{assert, check},
    krabka_metadata::{AclEntry, AclOperation, MetadataRecord, PatternType, ResourceType},
    krabka_protocol::{
        owned::{
            delete_topics_request::DeleteTopicsRequest,
            delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::sync::Arc,
};

/// Wire version the cluster-`Delete` shortcut test drives both `CreateTopics`
/// (to seed the fixture topics) and `DeleteTopics` at.
const CREATE_VERSION: i16 = 7;

const VERSION: i16 = 6;

krabka_macros::wire_codec_helpers! {
    test_context ["admin-client"];
}

async fn drive(
    broker: &Broker,
    req: &DeleteTopicsRequest,
    ctx: &RequestContext<'_>,
) -> DeleteTopicsResponse {
    sorted(
        handle(broker, req.clone(), VERSION, ctx)
            .await
            .expect("handle"),
    )
}

/// The response with its rows in a fixed order. The handler shuffles the rows
/// as Kafka does, so the tests compare them as a set.
fn sorted(mut resp: DeleteTopicsResponse) -> DeleteTopicsResponse {
    resp.responses.sort_by(|a, b| {
        (&a.name, a.topic_id.0, a.error_code).cmp(&(&b.name, b.topic_id.0, b.error_code))
    });
    resp
}

/// One response row.
fn row(name: Option<&str>, topic_id: WireUuid, error_code: i16) -> DeletableTopicResult {
    wire::delete_topic_result(name.map(str::to_string), topic_id, error_code)
}

fn response(rows: Vec<DeletableTopicResult>) -> DeleteTopicsResponse {
    krabka_macros::unthrottled_wire!(DeleteTopicsResponse { responses: rows })
}

#[tokio::test]
async fn handle_denied_topic_returns_authorization_failure() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, p, peer);
        boot [start_broker] deny_all;
        user ["alice"];
    };
    let req = request(vec![named_state("secret")]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let expected = response(vec![row(
        Some("secret"),
        WireUuid::ZERO,
        codes::TOPIC_AUTHORIZATION_FAILED,
    )]);
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unknown_name_and_id_preserve_error_rows() {
    krabka_macros::broker_test_setup! {
        bind(broker_handle, _dir, broker, p, peer);
        boot [start_broker] allow_all;
        user ["admin"];
    };
    let bogus_id = WireUuid([8; 16]);
    let req = request(vec![named_state("missing"), id_state(bogus_id)]);

    let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

    let expected = response(vec![
        row(None, bogus_id, codes::UNKNOWN_TOPIC_ID),
        row(
            Some("missing"),
            WireUuid::ZERO,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        ),
    ]);
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

// ── KFC-9: the break-glass gate over a topic deletion ───────────────

/// The topic row a deletion answers, the topic id, and whether the topic
/// still exists.
struct DoomedOutcome {
    row: DeletableTopicResult,
    topic_id: WireUuid,
    exists: bool,
}

/// Create [`DOOMED`] on a broker with this break-glass configuration, run one
/// `DeleteTopics` request for it, and answer the topic row, the topic id, and
/// whether the topic still exists.
async fn delete_doomed(break_glass: BreakGlassConfig) -> DoomedOutcome {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit_with(move |cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.break_glass = break_glass;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(principal, peer);
        user ["admin"];
    };
    let ctx = test_context(&principal, &peer);
    seed_topic(&broker, &ctx, CreateTopicSetup {
        topic: DOOMED,
        ..Default::default()
    })
    .await;
    let topic_id = WireUuid(
        broker
            .controller
            .current_image()
            .topic(DOOMED)
            .expect("seeded topic")
            .topic_id
            .into_bytes(),
    );
    let ctx = test_context(&principal, &peer);
    let req = DeleteTopicsRequest {
        topics: vec![named_state(DOOMED)],
        timeout_ms: 5_000,
        ..Default::default()
    };

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");
    let exists = broker.controller.current_image().topic(DOOMED).is_some();
    broker_handle.shutdown().await;
    let row = resp.responses.into_iter().next().expect("one topic row");
    DoomedOutcome {
        row,
        topic_id,
        exists,
    }
}

#[tokio::test]
async fn the_wire_handler_refuses_a_deletion_that_no_proposal_covers() {
    let DoomedOutcome {
        row: refused,
        topic_id,
        exists,
    } = delete_doomed(gated_config()).await;

    let expected = krabka_macros::tagged_wire!(DeletableTopicResult {
        name: Some(DOOMED.to_owned()),
        topic_id,
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(
            "break-glass refused delete_topic on doomed: no approved proposal covers the request"
                .to_owned(),
        ),
    });
    assert!((refused, exists) == (expected, true));
}

#[tokio::test]
async fn a_refused_deletion_never_reaches_the_metadata_quorum() {
    // An ungated broker deletes the topic. A gated one answers
    // `POLICY_VIOLATION` and leaves the topic in the metadata image.
    let DoomedOutcome {
        row: ungated,
        topic_id: ungated_id,
        exists: ungated_exists,
    } = delete_doomed(BreakGlassConfig::default()).await;
    let DoomedOutcome {
        row: gated,
        exists: gated_exists,
        ..
    } = delete_doomed(gated_config()).await;

    check!(ungated == row(Some(DOOMED), ungated_id, codes::NONE));
    check!(!ungated_exists);
    check!(gated.error_code == codes::POLICY_VIOLATION);
    check!(gated_exists);
}

/// Kafka's `ControllerApis.deleteTopics` answers `INVALID_REQUEST` for a v6
/// row with no name and the zero id, a row with a name and a non-zero id, a
/// duplicate name, a duplicate id, and a name whose id another row carries.
/// Each such request deletes nothing, and each response row carries the name,
/// the id and the message that Kafka puts on it.
#[tokio::test]
async fn invalid_topic_rows_answer_invalid_request_and_delete_nothing() {
    const TOPIC: &str = "kept";
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker(Arc::new(AllowAllAuthorizer)).await;
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker_handle.listen_addr().to_string())
        .client_id("delete-topics-validation-test")
        .build()
        .await
        .expect("client build");
    let created = client
        .send(
            krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                topics: vec![
                    krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: TOPIC.to_string(),
                        num_partitions: 1,
                        replication_factor: 1,
                        ..Default::default()
                    },
                ],
                timeout_ms: 5_000,
                ..Default::default()
            },
        )
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
    broker_handle.wait_until_partition_present(TOPIC, 0).await;
    let topic_id = WireUuid(
        broker_handle
            .controller_image_for_test()
            .topic(TOPIC)
            .expect("topic in the image")
            .topic_id
            .into_bytes(),
    );
    let broker = broker_handle.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(p, peer);
        user ["admin"];
    };

    let invalid = |name: Option<&str>, id, message: &str| {
        krabka_macros::tagged_wire!(DeletableTopicResult {
            name: name.map(str::to_string),
            topic_id: id,
            error_code: codes::INVALID_REQUEST,
            error_message: Some(message.to_string()),
        })
    };
    let both = krabka_protocol::owned::delete_topics_request::DeleteTopicState {
        name: Some(TOPIC.into()),
        topic_id,
        ..Default::default()
    };
    let cases = [
        (
            "no name and the zero id",
            vec![id_state(WireUuid::ZERO)],
            vec![invalid(
                None,
                WireUuid::ZERO,
                "Neither topic name nor id were specified.",
            )],
        ),
        ("a name and a non-zero id", vec![both], vec![invalid(
            Some(TOPIC),
            topic_id,
            "You may not specify both topic name and topic id.",
        )]),
        (
            "a duplicate name",
            vec![named_state(TOPIC), named_state(TOPIC)],
            vec![invalid(
                Some(TOPIC),
                WireUuid::ZERO,
                "Duplicate topic name.",
            )],
        ),
        (
            "a duplicate id",
            vec![id_state(topic_id), id_state(topic_id)],
            vec![invalid(None, topic_id, "Duplicate topic id.")],
        ),
        (
            "a name whose id another row carries",
            vec![id_state(topic_id), named_state(TOPIC)],
            vec![invalid(
                Some(TOPIC),
                topic_id,
                "The provided topic name maps to an ID that was already supplied.",
            )],
        ),
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, rows, responses) in cases {
        let resp = drive(&broker, &request(rows), &test_context(&p, &peer)).await;
        let still_there = broker.controller.current_image().topic(TOPIC).is_some();
        actual.push((label, resp, still_there));
        expected.push((
            label,
            krabka_macros::unthrottled_wire!(DeleteTopicsResponse { responses }),
            true,
        ));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

// ── #699: the cluster `Delete` shortcut ─────────────────────────────

/// Creates `name` with `principal`, under whatever authorizer `broker`
/// carries, and asserts the create itself was not refused. Setup for the
/// cluster-`Delete` shortcut test below, which needs topics that already
/// exist before it authorizes their deletion.
async fn seed_topic(broker: &Broker, ctx: &RequestContext<'_>, setup: CreateTopicSetup<'_>) {
    let name = setup.topic;
    let req = configured_topic_request_from(setup);
    let resp = create_topics_handle(broker, req, CREATE_VERSION, ctx)
        .await
        .expect("handle CreateTopics");
    assert!(
        resp.topics[0].error_code == codes::NONE,
        "seed create of {name}: {resp:?}"
    );
}

/// Seed the topic, then read its identity from the published controller image.
async fn seeded_topic_id(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    setup: CreateTopicSetup<'_>,
) -> WireUuid {
    let name = setup.topic;
    seed_topic(broker, ctx, setup).await;
    WireUuid(
        broker
            .controller
            .current_image()
            .topic(name)
            .expect("seeded topic")
            .topic_id
            .into_bytes(),
    )
}

/// #699: Kafka's `Delete` decision for a `DeleteTopics` request, table-driven
/// over which ACL `alice` holds. `ControllerApis.handleDeleteTopics`/
/// `deleteTopics` checks `Delete` on the `Cluster` resource once as a
/// shortcut -- an Allow there authorizes every candidate topic name without a
/// further lookup -- and falls back to `Delete` on each surviving
/// `Topic(name)` individually only when that shortcut is denied. This mirrors
/// the `Create` shortcut #698 fixed for `CreateTopics`.
#[tokio::test]
async fn handle_authorizes_delete_per_topic_when_cluster_delete_is_denied() {
    struct Case {
        acls: Vec<AclEntry>,
        // Which of "a" and "app-x" this ACL shape lets `alice` delete.
        deleted: &'static [&'static str],
    }

    let cluster_create = cluster_acl(AclOperation::Create);
    let cluster_delete = acl()
        .resource_type(AclSetup::cluster(AclOperation::Delete).resource_type)
        .resource_name(AclSetup::cluster(AclOperation::Delete).resource_name)
        .pattern_type(AclSetup::cluster(AclOperation::Delete).pattern_type)
        .operation(AclSetup::cluster(AclOperation::Delete).operation)
        .call();
    let literal_a_delete = acl()
        .resource_name("a")
        .operation(AclOperation::Delete)
        .call();
    let prefixed_app_delete = acl()
        .resource_name("app-")
        .pattern_type(PatternType::Prefixed)
        .operation(AclOperation::Delete)
        .call();

    let cases = [
        ("cluster Delete authorizes every survivor", Case {
            acls: vec![cluster_delete.clone()],
            deleted: &["a", "app-x"],
        }),
        ("a literal ACL authorizes only its exact name", Case {
            acls: vec![literal_a_delete.clone()],
            deleted: &["a"],
        }),
        ("an app- prefixed ACL authorizes only its prefix", Case {
            acls: vec![prefixed_app_delete.clone()],
            deleted: &["app-x"],
        }),
        ("no Delete ACL authorizes nothing", Case {
            acls: vec![],
            deleted: &[],
        }),
    ];

    for (label, case) in cases {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, p, peer);
            boot [start_broker] controller_peer;
            user ["alice"];
        };

        // Setup: grant alice cluster-wide Create so the fixture topics can
        // be seeded, then create them. The Create ACL plays no part in the
        // Delete decision under test.
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(
                cluster_create.clone(),
            )])
            .await
            .expect("seed create acl");
        let ctx = test_context(&p, &peer);
        seed_topic(&broker, &ctx, CreateTopicSetup {
            topic: "a",
            ..Default::default()
        })
        .await;
        seed_topic(&broker, &ctx, CreateTopicSetup {
            topic: "app-x",
            ..Default::default()
        })
        .await;

        // Grant this case's Delete ACL shape, if any.
        if !case.acls.is_empty() {
            broker
                .controller
                .submit_change(
                    case.acls
                        .into_iter()
                        .map(MetadataRecord::V1AccessControlEntry)
                        .collect(),
                )
                .await
                .expect("seed delete acls");
        }

        let before = broker.controller.current_image();
        let id_of = |name: &str| {
            WireUuid(
                before
                    .topic(name)
                    .expect("seeded topic")
                    .topic_id
                    .into_bytes(),
            )
        };
        let req = request(vec![named_state("a"), named_state("app-x")]);
        let resp = drive(&broker, &req, &test_context(&p, &peer)).await;

        // A deleted topic's row carries its id. A refused name row carries
        // the zero id: `Delete` implies `Describe`, so a principal with no
        // `Delete` ACL on the topic may not describe it either.
        let expected_row = |name: &str| -> DeletableTopicResult {
            if case.deleted.contains(&name) {
                row(Some(name), id_of(name), codes::NONE)
            } else {
                row(
                    Some(name),
                    WireUuid::ZERO,
                    codes::TOPIC_AUTHORIZATION_FAILED,
                )
            }
        };
        let expected = krabka_macros::unthrottled_wire!(DeleteTopicsResponse {
            responses: vec![expected_row("a"), expected_row("app-x")],
        });
        check!(resp == expected, "case: {label}");

        // Kafka checks cluster `Delete` with `logIfDenied = false`, since a
        // Deny there falls back to the per-topic checks: it is no refusal, so
        // it leaves no count behind.
        let cluster_delete_denials = broker
            .metrics
            .authorization_denied
            .get_or_create(&AuthorizationDeniedLabel {
                operation: "Delete".into(),
                resource_type: "Cluster".into(),
            })
            .get();
        check!(cluster_delete_denials == 0, "case: {label}, cluster Delete");

        let image = broker_handle.controller_image_for_test();
        check!(
            image.topic("a").is_none() == case.deleted.contains(&"a"),
            "case: {label}, topic a"
        );
        check!(
            image.topic("app-x").is_none() == case.deleted.contains(&"app-x"),
            "case: {label}, topic app-x"
        );

        broker_handle.shutdown().await;
    }
}

// ── #634: the Describe and Delete decisions and the row identities ──

/// One `alice` Allow ACL on a literal resource.
fn alice_acl(resource_type: ResourceType, name: &str, operation: AclOperation) -> AclEntry {
    allow_acl()
        .resource_type(resource_type)
        .resource_name(name)
        .operation(operation)
        .call()
}

/// Kafka's `ControllerApis.deleteTopics` checks `Describe` and `Delete`
/// separately. An id row the principal may not delete carries its id, and
/// the name only when the principal may describe the topic. A name row the
/// principal may describe but not delete answers `UNKNOWN_TOPIC_OR_PARTITION`
/// when the topic does not exist. A deleted topic's row carries its id.
#[tokio::test]
async fn rows_follow_kafkas_describe_and_delete_decisions() {
    const TOPIC: &str = "t";
    const MISSING: &str = "missing";

    struct ExpectedRow {
        name: Option<&'static str>,
        with_id: bool,
        code: i16,
    }

    struct Case {
        label: &'static str,
        acls: Vec<AclEntry>,
        by_id: bool,
        names: &'static [&'static str],
        // The name, whether it carries the topic id, and the code of each expected row.
        expected: &'static [ExpectedRow],
        deleted: bool,
    }

    let unknown_id = WireUuid([8; 16]);
    let topic = |operation| alice_acl(ResourceType::Topic, TOPIC, operation);
    let cases = [
        Case {
            label: "an id row with Delete deletes and carries the id",
            acls: vec![topic(AclOperation::Delete)],
            by_id: true,
            names: &[],
            expected: &[ExpectedRow {
                name: Some(TOPIC),
                with_id: true,
                code: codes::NONE,
            }],
            deleted: true,
        },
        Case {
            label: "an id row with Describe and without Delete",
            acls: vec![topic(AclOperation::Describe)],
            by_id: true,
            names: &[],
            expected: &[ExpectedRow {
                name: Some(TOPIC),
                with_id: true,
                code: codes::TOPIC_AUTHORIZATION_FAILED,
            }],
            deleted: false,
        },
        Case {
            label: "an id row without Describe and without Delete",
            acls: vec![],
            by_id: true,
            names: &[],
            expected: &[ExpectedRow {
                name: None,
                with_id: true,
                code: codes::TOPIC_AUTHORIZATION_FAILED,
            }],
            deleted: false,
        },
        Case {
            label: "a name row with Describe and without Delete for a missing topic",
            acls: vec![alice_acl(
                ResourceType::Topic,
                MISSING,
                AclOperation::Describe,
            )],
            by_id: false,
            names: &[MISSING],
            expected: &[ExpectedRow {
                name: Some(MISSING),
                with_id: false,
                code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            }],
            deleted: false,
        },
        Case {
            label: "a name row without Describe for a missing topic",
            acls: vec![],
            by_id: false,
            names: &[MISSING],
            expected: &[ExpectedRow {
                name: Some(MISSING),
                with_id: false,
                code: codes::TOPIC_AUTHORIZATION_FAILED,
            }],
            deleted: false,
        },
        Case {
            label: "a name row with Describe and without Delete for an existing topic",
            acls: vec![topic(AclOperation::Describe)],
            by_id: false,
            names: &[TOPIC],
            expected: &[ExpectedRow {
                name: Some(TOPIC),
                with_id: false,
                code: codes::TOPIC_AUTHORIZATION_FAILED,
            }],
            deleted: false,
        },
    ];

    handler_fixture_support::check_cases(cases, async |case| {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, p, peer);
            boot [start_broker] controller_peer;
            user ["alice"];
        };
        let cluster_create = alice_acl(
            ResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
            AclOperation::Create,
        );
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(cluster_create)])
            .await
            .expect("seed create acl");
        let ctx = test_context(&p, &peer);
        let topic_id = seeded_topic_id(&broker, &ctx, CreateTopicSetup {
            topic: TOPIC,
            ..Default::default()
        })
        .await;
        if !case.acls.is_empty() {
            acl_fixtures::seed_case_acls(&broker, case.acls).await;
        }

        let mut rows: Vec<_> = case.names.iter().map(|name| named_state(name)).collect();
        if case.by_id {
            rows.push(id_state(topic_id));
        }
        rows.push(id_state(unknown_id));
        let resp = drive(&broker, &request(rows), &test_context(&p, &peer)).await;
        let still_there = broker.controller.current_image().topic(TOPIC).is_some();

        let responses = case
            .expected
            .iter()
            .map(
                |ExpectedRow {
                     name,
                     with_id,
                     code,
                 }| {
                    let id = if *with_id { topic_id } else { WireUuid::ZERO };
                    row(*name, id, *code)
                },
            )
            .chain([row(None, unknown_id, codes::UNKNOWN_TOPIC_ID)])
            .collect();
        let actual = handler_fixture_support::LabeledCaseOutcome {
            label: case.label,
            response: resp,
            state: still_there,
        };
        let expected = handler_fixture_support::LabeledCaseOutcome {
            label: case.label,
            response: sorted(krabka_macros::unthrottled_wire!(DeleteTopicsResponse {
                responses
            })),
            state: !case.deleted,
        };
        broker_handle.shutdown().await;
        handler_fixture_support::CaseOutcomes { actual, expected }
    })
    .await;
}

// ── #743: delete.topic.enable ───────────────────────────────────────

/// Kafka's `ControllerApis.deleteTopics` refuses every row when
/// `delete.topic.enable` is false: `INVALID_REQUEST` below v3 and
/// `TOPIC_DELETION_DISABLED` from v3, with the name and the id the client sent
/// and no message. The topic stays.
#[tokio::test]
async fn delete_topic_enable_false_refuses_every_row() {
    const TOPIC: &str = "kept";
    let cases = [
        (true, 6, codes::NONE, false),
        (false, 2, codes::INVALID_REQUEST, true),
        (false, 3, codes::TOPIC_DELETION_DISABLED, true),
        (false, 6, codes::TOPIC_DELETION_DISABLED, true),
    ];

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (enabled, version, error_code, exists) in cases {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker_no_audit_with(move |cfg| {
            cfg.authorizer = Arc::new(AllowAllAuthorizer);
            cfg.delete_topic_enable = enabled;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(p, peer);
            user ["admin"];
        };
        let ctx = test_context(&p, &peer);
        let topic_id = seeded_topic_id(&broker, &ctx, CreateTopicSetup {
            topic: TOPIC,
            ..Default::default()
        })
        .await;
        let req = if version < 6 {
            DeleteTopicsRequest {
                topic_names: vec![TOPIC.into()],
                timeout_ms: 5_000,
                ..Default::default()
            }
        } else {
            request(vec![named_state(TOPIC)])
        };
        let ctx = test_context(&p, &peer);
        let resp: DeleteTopicsResponse = dispatch_wire(
            &broker,
            krabka_protocol::owned::delete_topics_request::API_KEY,
            version,
            &req,
            &ctx,
        )
        .await;
        let still_there = broker.controller.current_image().topic(TOPIC).is_some();
        actual.push(((enabled, version), resp, still_there));

        // A deleted topic's v6 row carries its id; a refused row carries the
        // zero id the name row was sent with, and v2 carries no id at all.
        let row = krabka_macros::tagged_wire!(DeletableTopicResult {
            name: Some(TOPIC.into()),
            topic_id: if error_code == codes::NONE {
                topic_id
            } else {
                WireUuid::ZERO
            },
            error_code,
            error_message: None,
        });
        expected.push((
            (enabled, version),
            krabka_macros::unthrottled_wire!(DeleteTopicsResponse {
                responses: vec![row],
            }),
            exists,
        ));
        broker_handle.shutdown().await;
    }
    assert!(actual == expected);
}
