//! End-to-end tests for the `AlterClientQuotas` handler: the cluster
//! authorization preamble, the per-entry results a mixed request returns, and
//! the response the handler returns.
//!
//! Most of them drive a live broker, so they are kept out of the module root.

use {
    super::{
        test_support::{entry, request},
        *,
    },
    crate::{
        broker::{Broker, BrokerHandle},
        codes::INVALID_REQUEST,
        handlers::RequestContext,
        test_support::{
            DenyAll, grant_cluster_operation, peer, principal,
            start_broker_with_authorizer as start_broker, test_ctx,
        },
    },
    assert2::assert,
    krabka_metadata::AclOperation,
    krabka_protocol::owned::alter_client_quotas_response::{
        EntityData as RespEntity, EntryData as RespEntry,
    },
    std::sync::Arc,
};

crate::test_support::context_helper!(client_id = "admin-client");

fn expected_entry(
    entity: (&str, Option<&str>),
    error_code: i16,
    error_message: Option<String>,
) -> RespEntry {
    tagged_wire!(RespEntry {
        error_code,
        error_message,
        entity: vec![tagged_wire!(RespEntity {
            entity_type: entity.0.into(),
            entity_name: entity.1.map(str::to_owned),
        })],
    })
}

fn expected_response(entries: Vec<RespEntry>) -> AlterClientQuotasResponse {
    unthrottled_wire!(AlterClientQuotasResponse { entries })
}

#[bon::builder]
async fn assert_handled_response(
    broker: &Broker,
    request: AlterClientQuotasRequest,
    version: i16,
    context: &RequestContext<'_>,
    expected: AlterClientQuotasResponse,
) {
    let actual = handle(broker, request, version, context)
        .await
        .expect("handle");
    assert!(actual == expected);
}

fn quota_value(handle: &BrokerHandle, user: &str, quota_key: &str) -> Option<f64> {
    let key: krabka_metadata::EntityKey = vec![("user".into(), Some(user.into()))];
    handle
        .controller_image_for_test()
        .client_quotas()
        .get(&key)
        .and_then(|configs| configs.get(quota_key).copied())
}

#[test]
fn whole_request_error_answers_every_entry() {
    let req = request(
        vec![
            entry(vec![("user", Some("alice"))], vec![]),
            entry(vec![("client-id", Some("app"))], vec![]),
        ],
        false,
    );

    let resp = whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied");

    let expected = expected_response(vec![
        expected_entry(
            ("user", Some("alice")),
            CLUSTER_AUTHORIZATION_FAILED,
            Some("denied".into()),
        ),
        expected_entry(
            ("client-id", Some("app")),
            CLUSTER_AUTHORIZATION_FAILED,
            Some("denied".into()),
        ),
    ]);
    assert!(resp == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_entry() {
    let version = 1;
    broker_fixture!(
        (broker_handle, _dir, broker),
        deny_all,
        context(ctx, "alice")
    );
    let req = request(
        vec![entry(vec![("user", Some("alice"))], vec![(
            "producer_byte_rate",
            1024.0,
            false,
        )])],
        false,
    );

    assert_handled_response()
        .broker(&broker)
        .request(req)
        .version(version)
        .context(&ctx)
        .expected(expected_response(vec![expected_entry(
            ("user", Some("alice")),
            CLUSTER_AUTHORIZATION_FAILED,
            Some("Cluster authorization failed.".into()),
        )]))
        .call()
        .await;
    assert!(quota_value(&broker_handle, "alice", "producer_byte_rate") == None);
    broker_handle.shutdown().await;
}

/// Kafka's `ControllerApis.handleAlterClientQuotas` authorizes `AlterConfigs`
/// on the cluster (#664). `All` implies it. `Alter` does not, so a principal
/// that can reassign partitions cannot change quotas.
#[tokio::test]
async fn cluster_alter_configs_gates_the_quota_write() {
    let version = 1;
    let empty_super_users = std::collections::HashSet::new();
    broker_fixture!(
        (broker_handle, _dir, broker),
        start_broker(Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
            empty_super_users,
        ),))
    );
    let peer = peer();

    for (user, grant, allowed) in [
        ("no-grant", None, false),
        ("alter", Some(AclOperation::Alter), false),
        (
            "describe-configs",
            Some(AclOperation::DescribeConfigs),
            false,
        ),
        ("alter-configs", Some(AclOperation::AlterConfigs), true),
        ("all", Some(AclOperation::All), true),
    ] {
        if let Some(operation) = grant {
            grant_cluster_operation(&broker_handle, user, operation).await;
        }
        let principal = principal(user);
        let ctx = test_context(&principal, &peer);
        let req = request(
            vec![entry(vec![("user", Some(user))], vec![(
                "producer_byte_rate",
                1024.0,
                false,
            )])],
            false,
        );

        let (error_code, error_message) = if allowed {
            (0, None)
        } else {
            (
                CLUSTER_AUTHORIZATION_FAILED,
                Some("Cluster authorization failed.".to_string()),
            )
        };
        assert_handled_response()
            .broker(&broker)
            .request(req)
            .version(version)
            .context(&ctx)
            .expected(expected_response(vec![expected_entry(
                ("user", Some(user)),
                error_code,
                error_message,
            )]))
            .call()
            .await;
        let stored = allowed.then_some(1024.0);
        assert2::check!(
            quota_value(&broker_handle, user, "producer_byte_rate") == stored,
            "user {user} with grant {grant:?}"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_entry_results_and_submits_valid_changes() {
    let version = 1;
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "admin")
    );
    let req = request(
        vec![
            entry(vec![("user", Some("alice"))], vec![(
                "producer_byte_rate",
                1024.0,
                false,
            )]),
            entry(vec![("user", Some("bob"))], vec![(
                "unknown_quota_key",
                1.0,
                false,
            )]),
        ],
        false,
    );

    let resp = handle(&broker, req, version, &ctx).await.expect("handle");

    let expected = expected_response(vec![
        expected_entry(("user", Some("alice")), 0, None),
        expected_entry(
            ("user", Some("bob")),
            INVALID_REQUEST,
            Some("Invalid configuration key unknown_quota_key".into()),
        ),
    ]);
    assert!(resp == expected);
    for (user, quota_key, want) in [
        ("alice", "producer_byte_rate", Some(1024.0)),
        ("bob", "unknown_quota_key", None),
    ] {
        assert!(
            quota_value(&broker_handle, user, quota_key) == want,
            "user {user}"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_validate_only_reports_success_without_submitting() {
    let version = 1;
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "admin")
    );
    let req = request(
        vec![entry(vec![("user", Some("carol"))], vec![(
            "producer_byte_rate",
            2048.0,
            false,
        )])],
        true,
    );

    let resp = handle(&broker, req, version, &ctx).await.expect("handle");

    let expected = expected_response(vec![expected_entry(("user", Some("carol")), 0, None)]);
    assert!(resp == expected);
    assert!(quota_value(&broker_handle, "carol", "producer_byte_rate") == None);
    broker_handle.shutdown().await;
}

/// Kafka keys the response by entity (#675): a repeated entity answers one
/// row with "Ignoring duplicate entity", and the first entry's change is
/// still written. `validate_only` gives the same rows and writes nothing.
#[tokio::test]
async fn repeated_entity_answers_one_row_with_and_without_validate_only() {
    let version = 1;
    test_ctx!(ctx, "admin");
    let row = |name: &str, code: i16, message: Option<&str>| {
        expected_entry(("user", Some(name)), code, message.map(Into::into))
    };
    let expected = expected_response(vec![
        row(
            "dave",
            INVALID_REQUEST,
            Some("Ignoring duplicate entity ClientQuotaEntity(entries={user=dave})"),
        ),
        row("erin", 0, None),
    ]);

    for (validate_only, stored) in [(true, [None, None]), (false, [Some(1024.0), Some(4.0)])] {
        broker_fixture!((broker_handle, _dir, broker), allow_all);
        let req = request(
            vec![
                entry(vec![("user", Some("dave"))], vec![(
                    "producer_byte_rate",
                    1024.0,
                    false,
                )]),
                entry(vec![("user", Some("erin"))], vec![(
                    "request_percentage",
                    4.0,
                    false,
                )]),
                entry(vec![("user", Some("dave"))], vec![(
                    "consumer_byte_rate",
                    2048.0,
                    false,
                )]),
            ],
            validate_only,
        );

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        assert2::check!(resp == expected, "validate_only {validate_only}");
        assert2::check!(
            [
                quota_value(&broker_handle, "dave", "producer_byte_rate"),
                quota_value(&broker_handle, "erin", "request_percentage"),
            ] == stored,
            "validate_only {validate_only}"
        );
        assert2::check!(
            quota_value(&broker_handle, "dave", "consumer_byte_rate") == None,
            "validate_only {validate_only}"
        );
        broker_handle.shutdown().await;
    }
}
