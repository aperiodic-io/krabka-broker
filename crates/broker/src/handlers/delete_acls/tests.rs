//! End-to-end `DeleteAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial
//! that stamps every filter and leaves the ACLs in place, and the matching-ACL
//! echo a successful filter returns next to the entries it actually removed,
//! Kafka's `AclBindingFilter` semantics over a mixed ACL set, and the two ways
//! Kafka refuses a filter with an `UNKNOWN` element.

use {
    super::{Doomed, filter::build_filter, handle, match_filter},
    crate::{
        broker::BrokerHandle,
        codes,
        error::BrokerError,
        handlers::{
            acl_test_support::{all_acls, seed_acls},
            delete_acls::test_support::{
                OPERATION_ANY, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_MATCH,
                PATTERN_TYPE_PREFIXED, PERMISSION_ANY, RESOURCE_TYPE_TOPIC, VERSION, acl,
                configured_authorizer, filter, named_filters, request, test_context,
            },
        },
        test_support::{DenyAll, start_broker_with_authorizer_no_audit as start_broker, test_ctx},
    },
    assert2::{assert, check},
    krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType},
    krabka_protocol::{
        ProtocolError,
        owned::{
            delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
            delete_acls_response::{DeleteAclsFilterResult, DeleteAclsResponse},
        },
    },
    std::sync::Arc,
};

fn orders_and_payments_request() -> DeleteAclsRequest {
    named_filters(&[("orders", "User:alice"), ("payments", "User:bob")])
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_filter() {
    seeded_acl_fixture!(
        (broker_handle, _dir, broker, ctx),
        start_broker(Arc::new(DenyAll)),
        vec![crate::handlers::acl_test_support::alice_orders_acl()],
        "alice"
    );
    let req = orders_and_payments_request();

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let denied = tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Request DeleteAcls needs ALTER permission.".into()),
        matching_acls: Vec::new(),
    });
    let expected = unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![denied.clone(), denied],
    });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).len() == 1);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_matching_acl_fields_and_deletes_only_matches() {
    seeded_acl_fixture!(
        (broker_handle, _dir, broker, ctx),
        start_broker(configured_authorizer()),
        crate::handlers::acl_test_support::orders_payments_acls(),
        "admin"
    );
    let req = request(vec![filter(Some("orders"), Some("User:alice"))]);

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let expected = unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![tagged_wire!(DeleteAclsFilterResult {
            error_code: codes::NONE,
            error_message: None,
            matching_acls: vec![crate::handlers::acl_test_support::expected_deleted_orders_acl()],
        })],
    });
    assert!(resp == expected);

    let remaining = all_acls(&broker_handle);
    assert!(
        remaining
            == vec![
                acl()
                    .resource_name("payments")
                    .principal("User:bob")
                    .operation(AclOperation::Write)
                    .call()
            ]
    );
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleDeleteAcls` refuses every filter with
/// `SECURITY_DISABLED` when no authorizer is configured, and reports no
/// matching ACLs. The default `AllowAllAuthorizer` is that state, and the
/// seeded binding must survive the refusal.
#[tokio::test]
async fn handle_answers_security_disabled_for_each_filter_when_no_authorizer_is_configured() {
    security_disabled_acl_fixture!((broker_handle, _dir, broker, ctx));
    let req = orders_and_payments_request();

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let disabled = tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::SECURITY_DISABLED,
        error_message: Some("No Authorizer is configured.".into()),
        matching_acls: Vec::new(),
    });
    let expected = unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![disabled.clone(), disabled],
    });
    assert!(resp == expected);
    assert!(
        all_acls(&broker_handle) == vec![crate::handlers::acl_test_support::alice_orders_acl()]
    );
    broker_handle.shutdown().await;
}

/// An ACL on topic `name` with `pattern_type`, `principal` and
/// `permission_type`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TopicAclSetup<'a> {
    #[default("orders")]
    name: &'a str,
    #[default(PatternType::Literal)]
    pattern_type: PatternType,
    #[default("User:alice")]
    principal: &'a str,
    #[default(PermissionType::Allow)]
    permission_type: PermissionType,
}

#[bon::builder]
fn topic_acl(
    #[builder(default = "orders")] name: &'_ str,
    #[builder(default = PatternType :: Literal)] pattern_type: PatternType,
    #[builder(default = "User:alice")] principal: &'_ str,
    #[builder(default = PermissionType :: Allow)] permission_type: PermissionType,
) -> AclEntry {
    let setup = TopicAclSetup {
        name,
        pattern_type,
        principal,
        permission_type,
    };
    let TopicAclSetup {
        name,
        pattern_type,
        principal,
        permission_type,
    } = setup;
    AclEntry {
        pattern_type,
        permission_type,
        ..acl().resource_name(name).principal(principal).call()
    }
}

/// A filter over topics with ANY operation and permission and a null host.
fn topic_filter(
    pattern_type: i8,
    resource_name: Option<&str>,
    principal: Option<&str>,
) -> DeleteAclsFilter {
    DeleteAclsFilter {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: resource_name.map(Into::into),
        pattern_type_filter: pattern_type,
        principal_filter: principal.map(Into::into),
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

/// Orders ACLs by resource name and principal, the two fields the table
/// below varies, so a set compares as a list.
fn sorted(mut acls: Vec<AclEntry>) -> Vec<AclEntry> {
    acls.sort_by(|a, b| (&a.resource_name, &a.principal).cmp(&(&b.resource_name, &b.principal)));
    acls
}

/// The table from issue #771: Kafka's `AclControlManager.deleteAcls` lists and
/// removes exactly the ACLs `AclBindingFilter.matches` selects. `MATCH` takes
/// every binding that applies to the named resource, and an empty string is a
/// value, not a wildcard.
#[tokio::test]
async fn handle_deletes_exactly_what_kafka_matches() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker(configured_authorizer())
    );
    test_ctx!(ctx, "admin");

    let literal_foo = topic_acl().name("foo").call();
    let wildcard_deny = topic_acl()
        .name("*")
        .principal("User:bob")
        .permission_type(PermissionType::Deny)
        .call();
    let prefixed_fo = topic_acl()
        .name("fo")
        .pattern_type(PatternType::Prefixed)
        .permission_type(PermissionType::Deny)
        .call();
    let prefixed_bar = topic_acl()
        .name("bar")
        .pattern_type(PatternType::Prefixed)
        .principal("User:bob")
        .call();
    let literal_food = topic_acl().name("food").call();
    let seeded = vec![
        literal_foo.clone(),
        wildcard_deny.clone(),
        prefixed_fo.clone(),
        prefixed_bar.clone(),
        literal_food.clone(),
    ];

    let cases: Vec<(&str, DeleteAclsFilter, Vec<AclEntry>)> = vec![
        (
            "empty name and principal delete nothing",
            topic_filter(PATTERN_TYPE_ANY, Some(""), Some("")),
            Vec::new(),
        ),
        (
            "empty name deletes nothing",
            topic_filter(PATTERN_TYPE_ANY, Some(""), None),
            Vec::new(),
        ),
        (
            "empty principal deletes nothing",
            topic_filter(PATTERN_TYPE_ANY, None, Some("")),
            Vec::new(),
        ),
        (
            "null name and principal delete every topic ACL",
            topic_filter(PATTERN_TYPE_ANY, None, None),
            seeded.clone(),
        ),
        (
            "ANY with a name compares it exactly",
            topic_filter(PATTERN_TYPE_ANY, Some("foo"), None),
            vec![literal_foo.clone()],
        ),
        (
            "MATCH takes the literal, the wildcard and covering prefixes",
            topic_filter(PATTERN_TYPE_MATCH, Some("foo"), None),
            vec![
                literal_foo.clone(),
                wildcard_deny.clone(),
                prefixed_fo.clone(),
            ],
        ),
        (
            "MATCH narrows by principal",
            topic_filter(PATTERN_TYPE_MATCH, Some("foo"), Some("User:alice")),
            vec![literal_foo.clone(), prefixed_fo.clone()],
        ),
        (
            "LITERAL with a null name",
            topic_filter(PATTERN_TYPE_LITERAL, None, None),
            vec![
                literal_foo.clone(),
                wildcard_deny.clone(),
                literal_food.clone(),
            ],
        ),
        (
            "PREFIXED with a name compares it exactly",
            topic_filter(PATTERN_TYPE_PREFIXED, Some("foo"), None),
            Vec::new(),
        ),
    ];

    for (name, f, want_deleted) in cases {
        seed_acls(&broker_handle, seeded.clone()).await;

        let mut resp = handle(&broker, request(vec![f]), VERSION, &ctx)
            .await
            .expect("handle");
        resp.filter_results[0].matching_acls.sort_by(|a, b| {
            (&a.resource_name, &a.principal).cmp(&(&b.resource_name, &b.principal))
        });

        let want_deleted = sorted(want_deleted);
        let expected = unthrottled_wire!(DeleteAclsResponse {
            filter_results: vec![tagged_wire!(DeleteAclsFilterResult {
                error_code: codes::NONE,
                error_message: None,
                matching_acls: want_deleted
                    .iter()
                    .map(super::response::matching_acl_result)
                    .collect(),
            })],
        });
        check!(resp == expected, "{name}");

        let want_left = sorted(
            seeded
                .iter()
                .filter(|e| !want_deleted.contains(e))
                .cloned()
                .collect(),
        );
        check!(sorted(all_acls(&broker_handle)) == want_left, "{name}");
    }
    broker_handle.shutdown().await;
}

/// Kafka's `AclControlManager.deleteAcls` matches every filter against the
/// same ACL set, so both filters list the shared ACL, and it is removed.
#[tokio::test]
async fn handle_lists_an_acl_under_every_filter_that_matches_it() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let shared = acl().call();
    let other = acl().resource_name("payments").principal("User:bob").call();
    seed_acls(&broker_handle, vec![shared.clone(), other.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");
    let req = request(vec![
        topic_filter(PATTERN_TYPE_ANY, Some("orders"), None),
        topic_filter(PATTERN_TYPE_MATCH, Some("orders"), Some("User:alice")),
    ]);

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let row = tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&shared)],
    });
    let expected = unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![row.clone(), row],
    });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle) == vec![other]);
    broker_handle.shutdown().await;
}

/// A byte Kafka does not define parses as `UNKNOWN`, and
/// `AclControlManager.validateFilter` refuses only that filter. The KIP-373
/// values are valid and match nothing krabka can store. The other filters
/// still run.
#[tokio::test]
async fn handle_refuses_a_filter_with_an_undefined_byte_and_runs_the_rest() {
    type Edit = fn(&mut DeleteAclsFilter);

    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let doomed = acl().call();
    seed_acls(&broker_handle, vec![doomed.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");

    let cases: [(Edit, Option<&str>); 7] = [
        (
            |f| f.resource_type_filter = 99,
            Some("Unknown patternFilter."),
        ),
        (
            |f| f.pattern_type_filter = 99,
            Some("Unknown patternFilter."),
        ),
        (|f| f.operation = 99, Some("Unknown entryFilter.")),
        (|f| f.permission_type = 99, Some("Unknown entryFilter.")),
        (|f| f.resource_type_filter = 7, None),
        (|f| f.operation = 13, None),
        (|f| f.operation = 14, None),
    ];
    let mut filters: Vec<DeleteAclsFilter> = cases
        .iter()
        .map(|(edit, _)| {
            let mut f = topic_filter(PATTERN_TYPE_ANY, None, None);
            edit(&mut f);
            f
        })
        .collect();
    filters.push(topic_filter(PATTERN_TYPE_ANY, Some("orders"), None));

    let resp = handle(&broker, request(filters), VERSION, &ctx)
        .await
        .expect("handle");

    let mut filter_results: Vec<DeleteAclsFilterResult> = cases
        .iter()
        .map(|(_, message)| {
            tagged_wire!(DeleteAclsFilterResult {
                error_code: if message.is_some() {
                    codes::INVALID_REQUEST
                } else {
                    codes::NONE
                },
                error_message: message.map(Into::into),
                matching_acls: Vec::new(),
            })
        })
        .collect();
    filter_results.push(tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&doomed)],
    }));
    let expected = unthrottled_wire!(DeleteAclsResponse { filter_results });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// Kafka refuses an `UNKNOWN` (0) element in any filter while the request
/// parses, before the cluster-alter check, and closes the connection. No
/// filter runs.
#[tokio::test]
async fn handle_closes_the_connection_on_an_unknown_element() {
    seeded_acl_fixture!(
        (broker_handle, _dir, broker, ctx),
        start_broker(configured_authorizer()),
        vec![crate::handlers::acl_test_support::alice_orders_acl()],
        "admin"
    );
    let mut unknown = filter(Some("payments"), None);
    unknown.permission_type = 0;
    let req = request(vec![filter(Some("orders"), Some("User:alice")), unknown]);

    let result = handle(&broker, req, VERSION, &ctx).await;

    assert!(
        let Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "Filters contain UNKNOWN elements"
        ))) = result
    );
    assert!(
        all_acls(&broker_handle) == vec![crate::handlers::acl_test_support::alice_orders_acl()]
    );
    broker_handle.shutdown().await;
}

/// Seeds `count` distinct topic ACLs in one submit, as `CreateAcls` writes
/// them. Several submits against a growing ACL set are far slower.
async fn seed_many_acls(handle: &BrokerHandle, count: usize) {
    seed_acls(
        handle,
        (0..count)
            .map(|n| acl().resource_name(&format!("topic-{n}")).call())
            .collect(),
    )
    .await;
}

/// Kafka's `AclControlManager.deleteAcls` caps a request at 10,000 removals.
/// The 10,001st match fails the whole request with `INVALID_REQUEST` on every
/// filter and deletes nothing. The check runs before a match is added, so a
/// second filter that matches the 10,000 ACLs already collected fails too.
///
/// The boundary itself is `match_filter_bounds_the_removals_of_a_request`: a
/// request that removes exactly 10,000 ACLs is not run here, because the image
/// removes an ACL by scanning the whole set and 10,000 of them take minutes in
/// a debug build.
#[tokio::test]
async fn handle_bounds_a_request_to_ten_thousand_removals() {
    let match_all = || filter(None, None);
    // (acls seeded, request filters)
    let cases = [
        (10_001, vec![match_all()]),
        (10_000, vec![match_all(), match_all()]),
    ];
    for (seeded, filters) in cases {
        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        seed_many_acls(&broker_handle, seeded).await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "admin");
        let filter_count = filters.len();

        let resp = handle(&broker, request(filters), VERSION, &ctx)
            .await
            .expect("handle");

        let bound = tagged_wire!(DeleteAclsFilterResult {
            error_code: codes::INVALID_REQUEST,
            error_message: Some(
                "Cannot remove more than 10000 acls in a single delete operation.".into(),
            ),
            matching_acls: Vec::new(),
        });
        let expected = unthrottled_wire!(DeleteAclsResponse {
            filter_results: vec![bound; filter_count],
        });
        check!(resp == expected, "{seeded} acls, {filter_count} filters");
        check!(
            all_acls(&broker_handle).len() == seeded,
            "{seeded} acls, {filter_count} filters"
        );
        broker_handle.shutdown().await;
    }
}

/// The bound of [`super::match_filter`], at a size a test can afford: a
/// request may remove exactly `bound` ACLs, and the next match fails it.
#[test]
fn match_filter_bounds_the_removals_of_a_request() {
    let image_acls: Vec<AclEntry> = (0..5)
        .map(|n| acl().resource_name(&format!("topic-{n}")).call())
        .collect();
    let build = |name: Option<&str>| build_filter(&filter(name, None)).expect("filter");
    let every_acl = build(None);
    let [topic_0, topic_1, topic_2] =
        ["topic-0", "topic-1", "topic-2"].map(|name| build(Some(name)));
    let bound = 3;
    // (label, filters in order, how many ACLs each matched; `None` is refused)
    let cases = [
        ("under the bound", vec![&topic_0, &topic_1], vec![
            Some(1),
            Some(1),
        ]),
        // A repeat adds no distinct removal, so it is never past the bound.
        (
            "repeats",
            vec![&topic_0, &topic_0, &topic_1, &topic_1],
            vec![Some(1); 4],
        ),
        (
            "exactly the bound",
            vec![&topic_0, &topic_1, &topic_2],
            vec![Some(1); 3],
        ),
        // The fourth ACL is the first one past the bound.
        ("one past the bound", vec![&every_acl], vec![None]),
        (
            "a later filter past the bound",
            vec![&topic_0, &topic_1, &every_acl],
            vec![Some(1), Some(1), None],
        ),
    ];
    for (label, filters, expected) in cases {
        let mut doomed = Doomed::default();
        let mut got = Vec::new();
        for filter in filters {
            let matched = match_filter(image_acls.iter(), filter, &mut doomed, bound);
            got.push(matched.as_ref().map(Vec::len));
            if matched.is_none() {
                break;
            }
        }
        check!(got == expected, "{label}");
    }
}
