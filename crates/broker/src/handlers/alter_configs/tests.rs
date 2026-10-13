//! End-to-end tests for the `AlterConfigs` handler: the per-resource
//! authorization preamble, the resource identity an unsupported type keeps,
//! and the response an accepted broker resource produces.
//!
//! Each of them drives a live broker, so they are kept out of the module root.

use {
    super::{
        RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP,
        RESOURCE_TYPE_TOPIC, dispatch,
        test_support::{
            broker_resource, client_metrics_resource, drive_many, drive_one, group_resource,
            resource, test_context,
        },
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        codes,
        coordinator::unified::streams::config::{
            KEY_NUM_STANDBY_REPLICAS, KEY_TASK_OFFSET_INTERVAL_MS,
        },
        handlers::{
            alter_configs::test_support::{
                image_with_client_metrics_config, image_with_group_config, image_with_topic_config,
                topic_resource,
            },
            test_support::{
                assert_dispatch_response, duplicate_config_resource_response, ordered_rows,
            },
        },
        test_support::{DenyAll, start_broker_with_authorizer as start_broker, test_ctx},
        throttle::LEADER_THROTTLED_RATE_KEY,
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        alter_configs_request::{AlterConfigsRequest, AlterableConfig},
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
    std::sync::Arc,
};

/// Local preprocessing errors retain their positions among controller results,
/// including both occurrences of a duplicate resource.
#[tokio::test]
async fn dispatch_preserves_mixed_resource_order_and_duplicates() {
    broker_fixture!((handle, _dir, broker), allow_all, context(ctx, "admin"));
    let duplicate_topic = duplicate_config_resource_response(RESOURCE_TYPE_TOPIC, "duplicate");
    let rows = [
        (
            broker_resource("2", &[]),
            krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "2".into(),
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Unexpected broker id, expected 1, but received 2".into())
            }),
        ),
        (
            broker_resource("1", &[]),
            krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".into(),
                error_code: codes::NONE,
                error_message: None
            }),
        ),
        (topic_resource("duplicate", &[]), duplicate_topic.clone()),
        (
            resource(77, "unsupported"),
            krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                resource_type: 77,
                resource_name: "unsupported".into(),
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Unknown resource type 77".into())
            }),
        ),
        (
            topic_resource("missing", &[]),
            krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "missing".into(),
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("The topic 'missing' does not exist.".into())
            }),
        ),
        (topic_resource("duplicate", &[]), duplicate_topic),
    ];
    for order in [[0, 1, 2, 3, 4, 5], [5, 4, 3, 2, 1, 0]] {
        let ordered = ordered_rows(&rows, &order);
        let request = AlterConfigsRequest {
            resources: ordered.requests,
            validate_only: true,
            ..Default::default()
        };
        let expected = AlterConfigsResponse {
            responses: ordered.responses,
            ..Default::default()
        };
        let broker_ref = broker.as_ref();
        let context_ref = &ctx;
        assert_dispatch_response(&request, &expected, 2, |body| async move {
            dispatch(broker_ref, 2, &body, context_ref).await
        })
        .await;
    }
    handle.shutdown().await;
}

macro_rules! invalid_config_row {
    (($response:ident, $row:ident), $request:expr) => {
        let $response = Box::pin(drive_one(Arc::new(DenyAll), $request)).await;
        assert!($response.responses.len() == 1);
        let $row = &$response.responses[0];
        assert!($row.error_code == codes::INVALID_REQUEST);
    };
}

#[tokio::test]
async fn handle_preserves_resource_identity_for_unsupported_type() {
    let resp = Box::pin(drive_one(
        Arc::new(AllowAllAuthorizer),
        resource(77, "mystery"),
    ))
    .await;

    let expected = krabka_macros::unthrottled_wire!(AlterConfigsResponse {
        responses: vec![krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Unknown resource type 77".to_string()),
            resource_type: 77,
            resource_name: "mystery".to_string(),
        })],
    });
    assert!(resp == expected);
}

#[derive(Clone, Copy)]
enum DeniedResourceCase {
    Topic,
    Group,
    ClientMetrics,
    Broker,
}

#[yare::parameterized(
    topic = { DeniedResourceCase::Topic },
    group = { DeniedResourceCase::Group },
    client_metrics = { DeniedResourceCase::ClientMetrics },
    broker = { DeniedResourceCase::Broker },
)]
#[test_macro(tokio::test)]
async fn denied_resources_report_their_authorization_error(case: DeniedResourceCase) {
    let (request, resource_type, resource_name, error_code, error_message) = match case {
        DeniedResourceCase::Topic => (
            resource(RESOURCE_TYPE_TOPIC, "orders"),
            RESOURCE_TYPE_TOPIC,
            "orders",
            codes::TOPIC_AUTHORIZATION_FAILED,
            "Topic authorization failed.",
        ),
        // Legacy AlterConfigs authorizes GROUP resources against
        // AlterConfigs on Group(name), as IncrementalAlterConfigs does.
        DeniedResourceCase::Group => (
            group_resource("streams-app", &[]),
            RESOURCE_TYPE_GROUP,
            "streams-app",
            codes::GROUP_AUTHORIZATION_FAILED,
            "Group authorization failed.",
        ),
        // CLIENT_METRICS is authorized against the cluster, like BROKER, and
        // takes its message from the controller path.
        DeniedResourceCase::ClientMetrics => (
            client_metrics_resource("sub-a", &[]),
            RESOURCE_TYPE_CLIENT_METRICS,
            "sub-a",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        ),
        DeniedResourceCase::Broker => (
            resource(RESOURCE_TYPE_BROKER, "1"),
            RESOURCE_TYPE_BROKER,
            "1",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        ),
    };
    let resp = drive_one(Arc::new(DenyAll), request).await;

    let expected = krabka_macros::unthrottled_wire!(AlterConfigsResponse {
        responses: vec![krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            error_code,
            error_message: Some(error_message.to_string()),
            resource_type,
            resource_name: resource_name.to_string(),
        })],
    });
    assert2::assert!(resp == expected);
}

#[tokio::test]
async fn authorized_group_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(AllowAllAuthorizer),
        group_resource("streams-app", &[(KEY_NUM_STANDBY_REPLICAS, "1")]),
    ))
    .await;

    let expected = krabka_macros::unthrottled_wire!(AlterConfigsResponse {
        responses: vec![krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".to_string(),
        })],
    });
    assert!(resp == expected);
}

#[tokio::test]
async fn authorized_client_metrics_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(AllowAllAuthorizer),
        client_metrics_resource("sub-a", &[("interval.ms", "60000")]),
    ))
    .await;

    let expected = krabka_macros::unthrottled_wire!(AlterConfigsResponse {
        responses: vec![krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
        })],
    });
    assert!(resp == expected);
}

/// A resource that appears twice in the request is a Kafka `preprocess`
/// shape error, so it earns `INVALID_REQUEST` on both rows -- even for a
/// principal `DenyAll` would otherwise refuse for a different reason. This is
/// the validate-before-authorize ordering the legacy handler now matches.
#[tokio::test]
async fn duplicate_resource_reference_is_a_validation_error_even_when_unauthorized() {
    let resp = Box::pin(drive_many(Arc::new(DenyAll), vec![
        resource(RESOURCE_TYPE_TOPIC, "orders"),
        resource(RESOURCE_TYPE_TOPIC, "orders"),
    ]))
    .await;

    assert!(resp.responses.len() == 2);
    for row in &resp.responses {
        check!(row.error_code == codes::INVALID_REQUEST);
        check!(row.error_message.as_deref() == Some("Each resource must appear at most once."));
        check!(row.resource_type == RESOURCE_TYPE_TOPIC);
        check!(row.resource_name == "orders");
    }
}

/// Duplicate config keys within one resource are a shape error, checked
/// before authorization runs.
#[tokio::test]
async fn duplicate_config_key_is_a_validation_error_even_when_unauthorized() {
    let mut duplicated = resource(RESOURCE_TYPE_TOPIC, "orders");
    duplicated.configs.push(duplicated.configs[0].clone());

    invalid_config_row!((resp, row), duplicated);
    assert!(row.error_message.as_deref() == Some("Error due to duplicate config keys"));
}

/// A null config value is a shape error for legacy `AlterConfigs`: unlike
/// `IncrementalAlterConfigs`, there is no DELETE operation that a null value
/// could mean, so it is simply malformed. Checked before authorization.
#[tokio::test]
async fn null_config_value_is_a_validation_error_even_when_unauthorized() {
    let mut resource = resource(RESOURCE_TYPE_TOPIC, "orders");
    resource.configs = ["retention.ms", "cleanup.policy"]
        .into_iter()
        .map(|name| AlterableConfig {
            name: name.into(),
            value: None,
            ..Default::default()
        })
        .collect();

    invalid_config_row!((resp, row), resource);
    // Kafka joins the names with `String.join(", ", ...)`.
    assert!(
        row.error_message.as_deref()
            == Some("Null value not supported for : retention.ms, cleanup.policy")
    );
}

/// A valid, uniquely-named resource with well-formed configs still gets an
/// authorization check: the validate-first ordering does not skip
/// authorization for a resource that passes validation.
#[tokio::test]
async fn valid_resource_still_gets_an_authorization_check() {
    for (label, req_resource, want_code) in [
        (
            "topic",
            resource(RESOURCE_TYPE_TOPIC, "orders"),
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        (
            "group",
            group_resource("streams-app", &[]),
            codes::GROUP_AUTHORIZATION_FAILED,
        ),
        (
            "client-metrics",
            client_metrics_resource("sub-a", &[]),
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        (
            "broker",
            resource(RESOURCE_TYPE_BROKER, "1"),
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
    ] {
        let resp = Box::pin(drive_one(Arc::new(DenyAll), req_resource)).await;
        assert!(resp.responses.len() == 1);
        check!(resp.responses[0].error_code == want_code, "{label}");
    }
}

#[tokio::test]
async fn authorized_broker_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(AllowAllAuthorizer),
        broker_resource("1", &[(LEADER_THROTTLED_RATE_KEY, "2048")]),
    ))
    .await;

    let expected = krabka_macros::unthrottled_wire!(AlterConfigsResponse {
        responses: vec![krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_BROKER,
            resource_name: "1".to_string(),
        })],
    });
    assert!(resp == expected);
}

/// A topic `AlterConfigs` replaces the whole override map, so the audit record
/// has to name the keys the request deletes by omitting them as well as the
/// ones it writes. A key restated at its stored value changed nothing, and a
/// controller-managed key the record builder carries forward was never the
/// client's to delete.
///
/// Keys only: a config value can be a password or a key store path, and none
/// of them reach the record.
#[test]
fn a_topic_replacement_audits_every_key_whose_value_moves() {
    use crate::{config_keys, handlers::audit_resource};

    /// The topic's stored overrides, the complete replacement the request
    /// carries, and the audit resources it earns.
    type Audited<'a> = (
        &'a str,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
        Vec<krabka_audit::AuditResource>,
    );

    let topic = |keys: &[&str]| {
        let mut expected = vec![audit_resource("Topic", "orders")];
        expected.extend(keys.iter().map(|key| audit_resource("ConfigKey", *key)));
        expected
    };
    let cases: [Audited<'_>; 4] = [
        (
            "a replacement that drops a stored key",
            &[
                (config_keys::RETENTION_MS, "60000"),
                (config_keys::CLEANUP_POLICY, "compact"),
            ],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[config_keys::CLEANUP_POLICY]),
        ),
        (
            "a replacement that changes a stored value",
            &[(config_keys::RETENTION_MS, "60000")],
            &[(config_keys::RETENTION_MS, "120000")],
            topic(&[config_keys::RETENTION_MS]),
        ),
        (
            "a replacement that adds a key",
            &[],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[config_keys::RETENTION_MS]),
        ),
        (
            "a replacement that restates every stored value",
            &[(config_keys::RETENTION_MS, "60000")],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[]),
        ),
    ];

    for (label, stored, replacement, expected) in cases {
        let image = image_with_topic_config("orders", stored);

        let audited = super::audit_resources_for(&topic_resource("orders", replacement), &image);

        assert!(audited == expected, "{label}");
    }
}

/// A GROUP or `CLIENT_METRICS` `AlterConfigs` also replaces the whole override
/// map, so a replacement that omits a stored key must audit that key as
/// changed -- the same rule topic resources already get, extended to the
/// full-map-replacement types #1122 added authorization for.
#[test]
fn a_group_or_client_metrics_replacement_audits_a_key_it_drops_by_omission() {
    use crate::handlers::audit_resource;

    let group_image = image_with_group_config("streams-app", &[
        (KEY_NUM_STANDBY_REPLICAS, "1"),
        (KEY_TASK_OFFSET_INTERVAL_MS, "5000"),
    ]);
    let group_audited = super::audit_resources_for(
        &group_resource("streams-app", &[(KEY_NUM_STANDBY_REPLICAS, "1")]),
        &group_image,
    );
    assert!(
        group_audited
            == vec![
                audit_resource("Group", "streams-app"),
                audit_resource(
                    "ConfigKey",
                    crate::coordinator::unified::streams::config::KEY_TASK_OFFSET_INTERVAL_MS
                ),
            ]
    );

    let client_metrics_image =
        image_with_client_metrics_config("sub1", &[("interval.ms", "30000"), ("metrics", "*")]);
    let client_metrics_audited = super::audit_resources_for(
        &client_metrics_resource("sub1", &[("interval.ms", "30000")]),
        &client_metrics_image,
    );
    assert!(
        client_metrics_audited
            == vec![
                audit_resource("ClientMetrics", "sub1"),
                audit_resource("ConfigKey", "metrics"),
            ]
    );
}

/// The size rule is shared by both alter APIs and holds for every record type
/// a resource turns into.
#[test]
fn an_oversized_value_is_refused_in_every_kind_of_config_record() {
    use krabka_metadata::{
        BrokerConfigRecord, ClientMetricsConfigRecord, GroupConfigRecord, MetadataRecord,
        TopicConfigRecord,
    };

    let long = "a".repeat(32_768);
    let map = |value: &str| maplit::btreemap! {"k".to_owned() => value.to_owned()};
    let records = |value: &str| {
        vec![
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: map(value),
            }),
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::NodeId(1),
                config_name: "k".into(),
                config_value: Some(value.to_owned()),
            }),
            MetadataRecord::V1GroupConfig(GroupConfigRecord {
                group_id: "g".into(),
                configs: map(value),
            }),
            MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
                name: "s".into(),
                configs: map(value),
            }),
        ]
    };
    for record in records(&long) {
        check!(
            super::config_value_size_error(std::slice::from_ref(&record))
                == Some((
                    codes::INVALID_CONFIG,
                    "The configuration value cannot be added because it exceeds the maximum \
                     value size of 32767 bytes."
                        .to_owned()
                )),
            "{record:?}"
        );
    }
    for record in records(&long[1..]) {
        check!(
            super::config_value_size_error(std::slice::from_ref(&record)).is_none(),
            "{record:?}"
        );
    }
    // A deletion writes no value.
    check!(
        super::config_value_size_error(&[MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: "k".into(),
            config_value: None,
        })])
        .is_none()
    );
}

/// Kafka's `ConfigurationControlManager.validateAlterConfig` refuses, for
/// every resource type, a value longer than `Short.MAX_VALUE` UTF-16 code
/// units with `INVALID_CONFIG`, whatever the key is. A key Kafka does not
/// define is stored as it is, so it is the one an oversized value reaches.
#[tokio::test]
async fn a_config_value_past_short_max_value_is_invalid_config() {
    let refused = "The configuration value cannot be added because it exceeds the maximum \
                   value size of 32767 bytes.";
    // 16384 emoji are 32768 UTF-16 code units: Java's `String.length()` counts
    // each surrogate pair twice, so the value is over the limit at half the
    // `char` count.
    let cases = [
        ("at the limit", "a".repeat(32_767), codes::NONE, None),
        (
            "one past the limit",
            "a".repeat(32_768),
            codes::INVALID_CONFIG,
            Some(refused),
        ),
        (
            "surrogate pairs count twice",
            "\u{1F600}".repeat(16_384),
            codes::INVALID_CONFIG,
            Some(refused),
        ),
    ];
    for (label, value, want_code, want_message) in cases {
        let resp = Box::pin(drive_one(
            Arc::new(AllowAllAuthorizer),
            broker_resource("1", &[("custom.setting", &value)]),
        ))
        .await;
        assert!(resp.responses.len() == 1, "{label}");
        check!(resp.responses[0].error_code == want_code, "{label}");
        check!(
            resp.responses[0].error_message.as_deref() == want_message,
            "{label}"
        );
    }
}
