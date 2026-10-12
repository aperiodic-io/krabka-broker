//! `DescribeClientQuotas` (`api_key` 48, KIP-13/124).

use krabka_metadata::{EntityKey, ResourceType};
use krabka_protocol::owned::{
    describe_client_quotas_request::{ComponentData, DescribeClientQuotasRequest},
    describe_client_quotas_response::{
        DescribeClientQuotasResponse, EntityData, EntryData, ValueData,
    },
};

use super::acl_wire::CLUSTER_RESOURCE_NAME;
#[cfg(test)]
use crate::authorizer::SimpleAclAuthorizer;
use crate::{
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, NONE, UNSUPPORTED_VERSION},
    handlers::{RequestContext, acl_denied},
};

/// Wire `match_type`: entity name must equal `match_` exactly (KIP-546 `EXACT`).
const MATCH_TYPE_EXACT: i8 = 0;
/// Wire `match_type`: only the default (unnamed) entity matches (KIP-546 `DEFAULT`).
const MATCH_TYPE_DEFAULT: i8 = 1;
/// Wire `match_type`: any entity of the given type matches (KIP-546 `ANY`).
const MATCH_TYPE_ANY: i8 = 2;

/// The entity types a filter component may name.
const USER: &str = "user";
const CLIENT_ID: &str = "client-id";
const IP: &str = "ip";

/// A filter rejection: the top-level error code and message Kafka sends.
#[derive(Debug, PartialEq, Eq)]
struct FilterError {
    code: i16,
    message: String,
}

fn invalid(message: impl Into<String>) -> FilterError {
    FilterError {
        code: INVALID_REQUEST,
        message: message.into(),
    }
}

/// Checks the filter components as Kafka's `ClientQuotasImage.describe`
/// does, in the same order, before any match runs.
///
/// # Errors
///
/// Returns the first rule the filter breaks, with Kafka's error code and
/// message.
fn validate_filter(components: &[ComponentData]) -> Result<(), FilterError> {
    let mut seen: Vec<&str> = Vec::with_capacity(components.len());
    for comp in components {
        let entity_type = comp.entity_type.as_str();
        if entity_type.is_empty() {
            return Err(invalid("Invalid empty entity type."));
        }
        if seen.contains(&entity_type) {
            return Err(invalid(format!(
                "Entity type {entity_type} cannot appear more than once in the filter."
            )));
        }
        if ![IP, USER, CLIENT_ID].contains(&entity_type) {
            return Err(FilterError {
                code: UNSUPPORTED_VERSION,
                message: format!("Unsupported entity type {entity_type}"),
            });
        }
        match (comp.match_type, comp.match_.is_some()) {
            (MATCH_TYPE_EXACT, false) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_EXACT, but set match string to null.",
                ));
            }
            (MATCH_TYPE_DEFAULT, true) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_DEFAULT, but also specified a match string.",
                ));
            }
            (MATCH_TYPE_ANY, true) => {
                return Err(invalid(
                    "Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.",
                ));
            }
            (MATCH_TYPE_EXACT | MATCH_TYPE_DEFAULT | MATCH_TYPE_ANY, _) => {}
            (other, _) => return Err(invalid(format!("Unknown match type {other}"))),
        }
        seen.push(entity_type);
    }
    if seen.contains(&IP) && (seen.contains(&USER) || seen.contains(&CLIENT_ID)) {
        return Err(invalid(
            "Invalid entity filter component combination. IP filter component should not be \
             used with user or clientId filter component.",
        ));
    }
    Ok(())
}

pub(crate) fn handle(
    broker: &Broker,
    req: &DescribeClientQuotasRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> DescribeClientQuotasResponse {
    let image = broker.controller.current_image();
    // Kafka's `KafkaApis.handleDescribeClientQuotasRequest` authorizes
    // `DescribeConfigs` on the cluster. A denial goes through
    // `ApiError.fromThrowable`, which drops the default message, so the
    // response carries a null `error_message`.
    if acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
        krabka_metadata::AclOperation::DescribeConfigs,
    ) {
        return DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
            ..Default::default()
        };
    }

    // Kafka's `ClientQuotasImage.describe` throws on a bad filter, and
    // `KafkaApis.handleError` answers with `entries = null`.
    if let Err(err) = validate_filter(&req.components) {
        return DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: err.code,
            error_message: Some(err.message),
            entries: None,
            ..Default::default()
        };
    }

    let mut entries: Vec<EntryData> = Vec::new();
    let matching = if req.strict {
        QuotaEntityScope::ExactTypes
    } else {
        QuotaEntityScope::IncludeAdditionalTypes
    };
    for (stored_key, configs) in image.client_quotas() {
        if !entity_matches_filter(stored_key, &req.components, matching) {
            continue;
        }
        entries.push(EntryData {
            entity: stored_key
                .iter()
                .map(|(t, n)| EntityData {
                    entity_type: t.clone(),
                    entity_name: n.clone(),
                    ..Default::default()
                })
                .collect(),
            values: configs
                .iter()
                .map(|(k, v)| ValueData {
                    key: k.clone(),
                    value: *v,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
    }

    // Kafka answers from `ClientQuotasImage.describe`, which fills a bare
    // `new DescribeClientQuotasResponseData()`: the message keeps its
    // generated default, the empty string, not null.
    DescribeClientQuotasResponse {
        throttle_time_ms: 0,
        error_code: NONE,
        error_message: Some(String::new()),
        entries: Some(entries),
        ..Default::default()
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum QuotaEntityScope {
    ExactTypes,
    #[default]
    IncludeAdditionalTypes,
}

pub(crate) fn entity_matches_filter(
    stored: &EntityKey,
    components: &[ComponentData],
    scope: QuotaEntityScope,
) -> bool {
    if scope == QuotaEntityScope::ExactTypes && stored.len() != components.len() {
        return false;
    }
    for comp in components {
        let Some(stored_entity) = stored.iter().find(|(t, _)| t == &comp.entity_type) else {
            return false;
        };
        let ok = match comp.match_type {
            MATCH_TYPE_EXACT => stored_entity.1.as_deref() == comp.match_.as_deref(),
            MATCH_TYPE_DEFAULT => stored_entity.1.is_none(),
            MATCH_TYPE_ANY => true,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::MetadataRecord;

    use super::*;
    use crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        quota::test_support::{QuotaRecordSetup, QuotaValue},
        test_support::DenyAll,
    };

    const VERSION: i16 = 1;

    #[derive(Clone, Copy)]
    struct QuotaMatchCode(i8);

    #[derive(Clone, Copy, Default)]
    enum QuotaNameMatch {
        Exact,
        DefaultEntity,
        #[default]
        Any,
        Malformed(QuotaMatchCode),
    }

    impl QuotaNameMatch {
        fn wire_code(self) -> i8 {
            match self {
                Self::Exact => MATCH_TYPE_EXACT,
                Self::DefaultEntity => MATCH_TYPE_DEFAULT,
                Self::Any => MATCH_TYPE_ANY,
                Self::Malformed(code) => code.0,
            }
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct QuotaComponentSetup<'a> {
        #[default("user")]
        entity_type: &'a str,
        matching: QuotaNameMatch,
        name: Option<&'a str>,
    }

    fn comp(setup: QuotaComponentSetup<'_>) -> ComponentData {
        ComponentData {
            entity_type: setup.entity_type.into(),
            match_type: setup.matching.wire_code(),
            match_: setup.name.map(Into::into),
            ..Default::default()
        }
    }

    fn exact_user_filter(name: &str) -> Vec<ComponentData> {
        vec![comp(QuotaComponentSetup {
            matching: QuotaNameMatch::Exact,
            name: Some(name),
            ..Default::default()
        })]
    }

    fn key(parts: Vec<(&str, Option<&str>)>) -> EntityKey {
        parts
            .into_iter()
            .map(|(t, n)| (t.into(), n.map(Into::into)))
            .collect()
    }

    #[derive(Default)]
    struct QuotaRequestSetup {
        components: Vec<ComponentData>,
        matching: QuotaEntityScope,
    }

    fn request(setup: QuotaRequestSetup) -> DescribeClientQuotasRequest {
        DescribeClientQuotasRequest {
            components: setup.components,
            strict: setup.matching == QuotaEntityScope::ExactTypes,
            ..Default::default()
        }
    }

    crate::test_support::context_helper!(client_id = "admin-client");

    use crate::test_support::{
        describe_configs_acl_cases, grant_cluster_operation,
        start_broker_with_authorizer_no_audit as start_broker, test_ctx,
    };

    async fn seed_quota(handle: &BrokerHandle, setup: QuotaRecordSetup<'_>) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1ClientQuota(
                crate::quota::test_support::quota_record(setup),
            )])
            .await
            .expect("seed quota");
    }

    async fn seed_alice_quota(handle: &BrokerHandle) {
        seed_quota(
            handle,
            QuotaRecordSetup {
                value: QuotaValue(1024.0),
                ..Default::default()
            },
        )
        .await;
    }

    fn allowed_user_quota_response(name: Option<&str>, value: f64) -> DescribeClientQuotasResponse {
        DescribeClientQuotasResponse {
            throttle_time_ms: 0,
            error_code: NONE,
            error_message: Some(String::new()),
            entries: Some(vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: name.map(Into::into),
                    ..Default::default()
                }],
                values: vec![ValueData {
                    key: "producer_byte_rate".into(),
                    value,
                    ..Default::default()
                }],
                ..Default::default()
            }]),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    #[test]
    fn strict_exact_match_filters_correctly() {
        let stored = key(vec![("user", Some("alice"))]);
        let filter = exact_user_filter("alice");
        assert!(entity_matches_filter(
            &stored,
            &filter,
            QuotaEntityScope::ExactTypes
        ));
        assert!(!entity_matches_filter(
            &stored,
            &filter[..0],
            QuotaEntityScope::ExactTypes
        )); // strict: type-count mismatch
    }

    #[test]
    fn non_strict_filter_returns_supersets() {
        // Stored has (user, client-id); filter only mentions user.
        let stored = key(vec![("client-id", Some("app1")), ("user", Some("alice"))]);
        let filter = exact_user_filter("alice");
        assert!(entity_matches_filter(
            &stored,
            &filter,
            QuotaEntityScope::IncludeAdditionalTypes
        ));
        assert!(!entity_matches_filter(
            &stored,
            &filter,
            QuotaEntityScope::ExactTypes
        )); // strict rejects superset
    }

    #[test]
    fn default_match_type_filters_by_none_entity_name() {
        let stored_default = key(vec![("user", None)]);
        let stored_named = key(vec![("user", Some("alice"))]);
        let filter = vec![comp(QuotaComponentSetup {
            matching: QuotaNameMatch::DefaultEntity,
            ..Default::default()
        })];
        assert!(entity_matches_filter(
            &stored_default,
            &filter,
            QuotaEntityScope::ExactTypes
        ));
        assert!(!entity_matches_filter(
            &stored_named,
            &filter,
            QuotaEntityScope::ExactTypes
        ));
    }

    #[test]
    fn any_match_type_returns_all_names_of_type() {
        let stored1 = key(vec![("user", Some("alice"))]);
        let stored2 = key(vec![("user", None)]);
        let filter = vec![comp(QuotaComponentSetup::default())];
        assert!(entity_matches_filter(
            &stored1,
            &filter,
            QuotaEntityScope::ExactTypes
        ));
        assert!(entity_matches_filter(
            &stored2,
            &filter,
            QuotaEntityScope::ExactTypes
        ));
    }

    #[tokio::test]
    async fn denied_response_preserves_error_fields() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            deny_all,
            context(ctx, "alice")
        );

        let resp = handle(
            &broker,
            &request(QuotaRequestSetup {
                components: exact_user_filter("alice"),
                matching: QuotaEntityScope::ExactTypes,
            }),
            VERSION,
            &ctx,
        );

        let expected = unthrottled_wire!(DescribeClientQuotasResponse {
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
        });
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    /// Kafka's `KafkaApis.handleDescribeClientQuotasRequest` authorizes
    /// `DescribeConfigs` on the cluster (#663). `AlterConfigs` and `All`
    /// imply it. `Describe`, and `Alter` (which implies `Describe`), do not.
    /// A denial carries the default message, which Kafka sends as null.
    #[tokio::test]
    async fn cluster_describe_configs_gates_the_quota_read() {
        let (broker_handle, _dir) = start_broker(Arc::new(SimpleAclAuthorizer::new(
            std::collections::HashSet::new(),
        )))
        .await;
        seed_alice_quota(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        let allowed = allowed_user_quota_response(Some("alice"), 1024.0);
        let denied = unthrottled_wire!(DescribeClientQuotasResponse {
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            entries: None,
        });

        for (user, grant, expected) in describe_configs_acl_cases(&allowed, &denied) {
            if let Some(operation) = grant {
                grant_cluster_operation(&broker_handle, user, operation).await;
            }
            test_ctx!(ctx, user);

            let resp = handle(
                &broker,
                &request(QuotaRequestSetup {
                    components: vec![comp(QuotaComponentSetup::default())],
                    ..Default::default()
                }),
                VERSION,
                &ctx,
            );

            check!(resp == *expected, "user {user} with grant {grant:?}");
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_returns_matching_quota_entry_fields() {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        seed_quota(
            &broker_handle,
            QuotaRecordSetup {
                entity: vec![("client-id", Some("app-1")), ("user", Some("alice"))],
                value: QuotaValue(2048.0),
                ..Default::default()
            },
        )
        .await;
        seed_quota(
            &broker_handle,
            QuotaRecordSetup {
                entity: vec![("user", Some("bob"))],
                key: "consumer_byte_rate",
                value: QuotaValue(512.0),
            },
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "admin");

        let resp = handle(
            &broker,
            &request(QuotaRequestSetup {
                components: exact_user_filter("alice"),
                ..Default::default()
            }),
            VERSION,
            &ctx,
        );

        check!(resp.throttle_time_ms == 0, "{resp:?}");
        check!(resp.error_code == 0, "{resp:?}");
        check!(resp.error_message.as_deref() == Some(""), "{resp:?}");
        let entries = resp.entries.expect("entries");
        assert!(entries.len() == 1, "{entries:?}");
        let entry = &entries[0];
        assert!(entry.entity.len() == 2, "{entry:?}");
        let by_type: std::collections::HashMap<_, _> = entry
            .entity
            .iter()
            .map(|e| (e.entity_type.as_str(), e.entity_name.as_deref()))
            .collect();
        check!(
            by_type.get("client-id") == Some(&Some("app-1")),
            "{entry:?}"
        );
        check!(by_type.get("user") == Some(&Some("alice")), "{entry:?}");
        check!(entry.values.len() == 1, "{entry:?}");
        check!(
            entry.values[0].key.as_str() == "producer_byte_rate",
            "{entry:?}"
        );
        check!(
            (entry.values[0].value - 2048.0).abs() < f64::EPSILON,
            "{entry:?}"
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `ClientQuotasImage.describe` rejects a bad filter before any
    /// match, with `entries = null` (#674). Valid filters still match.
    #[tokio::test]
    async fn invalid_filters_answer_kafkas_top_level_error() {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        seed_alice_quota(&broker_handle).await;
        seed_quota(
            &broker_handle,
            QuotaRecordSetup {
                entity: vec![("user", None)],
                value: QuotaValue(2048.0),
                ..Default::default()
            },
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "admin");

        let error = |code: i16, message: &str| {
            unthrottled_wire!(DescribeClientQuotasResponse {
                error_code: code,
                error_message: Some(message.into()),
                entries: None,
            })
        };
        let rows: Vec<(&str, Vec<ComponentData>, DescribeClientQuotasResponse)> = vec![
            (
                "empty entity type",
                vec![comp(QuotaComponentSetup {
                    entity_type: "",
                    ..Default::default()
                })],
                error(INVALID_REQUEST, "Invalid empty entity type."),
            ),
            (
                "repeated entity type",
                vec![
                    comp(QuotaComponentSetup {
                        matching: QuotaNameMatch::Exact,
                        name: Some("alice"),
                        ..Default::default()
                    }),
                    comp(QuotaComponentSetup::default()),
                ],
                error(
                    INVALID_REQUEST,
                    "Entity type user cannot appear more than once in the filter.",
                ),
            ),
            (
                "unknown entity type",
                vec![comp(QuotaComponentSetup {
                    entity_type: "group",
                    ..Default::default()
                })],
                error(UNSUPPORTED_VERSION, "Unsupported entity type group"),
            ),
            (
                "exact with null match",
                vec![comp(QuotaComponentSetup {
                    matching: QuotaNameMatch::Exact,
                    ..Default::default()
                })],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_EXACT, but set match string to null.",
                ),
            ),
            (
                "default with a match string",
                vec![comp(QuotaComponentSetup {
                    matching: QuotaNameMatch::DefaultEntity,
                    name: Some("alice"),
                    ..Default::default()
                })],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_DEFAULT, but also specified a match string.",
                ),
            ),
            (
                "specified with a match string",
                vec![comp(QuotaComponentSetup {
                    name: Some("alice"),
                    ..Default::default()
                })],
                error(
                    INVALID_REQUEST,
                    "Request specified MATCH_TYPE_SPECIFIED, but also specified a match string.",
                ),
            ),
            (
                "unknown match type",
                vec![comp(QuotaComponentSetup {
                    matching: QuotaNameMatch::Malformed(QuotaMatchCode(7)),
                    ..Default::default()
                })],
                error(INVALID_REQUEST, "Unknown match type 7"),
            ),
            (
                "ip with user",
                vec![
                    comp(QuotaComponentSetup {
                        entity_type: "ip",
                        ..Default::default()
                    }),
                    comp(QuotaComponentSetup::default()),
                ],
                error(
                    INVALID_REQUEST,
                    "Invalid entity filter component combination. IP filter component should \
                     not be used with user or clientId filter component.",
                ),
            ),
            (
                "client-id with ip",
                vec![
                    comp(QuotaComponentSetup {
                        entity_type: "client-id",
                        matching: QuotaNameMatch::DefaultEntity,
                        ..Default::default()
                    }),
                    comp(QuotaComponentSetup {
                        entity_type: "ip",
                        matching: QuotaNameMatch::Exact,
                        name: Some("1.2.3.4"),
                    }),
                ],
                error(
                    INVALID_REQUEST,
                    "Invalid entity filter component combination. IP filter component should \
                     not be used with user or clientId filter component.",
                ),
            ),
            (
                "valid exact",
                exact_user_filter("alice"),
                allowed_user_quota_response(Some("alice"), 1024.0),
            ),
            (
                "valid default",
                vec![comp(QuotaComponentSetup {
                    matching: QuotaNameMatch::DefaultEntity,
                    ..Default::default()
                })],
                allowed_user_quota_response(None, 2048.0),
            ),
        ];
        for (name, components, expected) in rows {
            let resp = handle(
                &broker,
                &request(QuotaRequestSetup {
                    components,
                    matching: QuotaEntityScope::ExactTypes,
                }),
                VERSION,
                &ctx,
            );
            check!(resp == expected, "row {name}");
        }

        // A valid SPECIFIED filter matches every user entity, named or default.
        let resp = handle(
            &broker,
            &request(QuotaRequestSetup {
                components: vec![comp(QuotaComponentSetup::default())],
                matching: QuotaEntityScope::ExactTypes,
            }),
            VERSION,
            &ctx,
        );
        let mut names: Vec<Option<String>> = resp
            .entries
            .expect("entries")
            .into_iter()
            .flat_map(|e| e.entity.into_iter().map(|d| d.entity_name))
            .collect();
        names.sort();
        check!(names == vec![None, Some("alice".to_owned())]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_empty_match_uses_some_empty_entries() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );

        let resp = handle(
            &broker,
            &request(QuotaRequestSetup {
                components: exact_user_filter("missing"),
                matching: QuotaEntityScope::ExactTypes,
            }),
            VERSION,
            &ctx,
        );

        let expected = unthrottled_wire!(DescribeClientQuotasResponse {
            error_code: 0,
            error_message: Some(String::new()),
            entries: Some(Vec::new()),
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
