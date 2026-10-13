//! Response assembly for `CreateTopics`: the per-topic result rows, the
//! response envelope around them, the admin audit event for the topics that
//! were created, and the KIP-219 throttle window recorded on the request
//! context for the connection loop to enforce after the reply is written.

use {
    crate::{
        broker::Broker,
        codes,
        handlers::{RequestContext, audit_admin_success, audit_resource},
        metrics::QuotaType,
        quota::throttle_time_ms,
    },
    krabka_protocol::owned::create_topics_response::{
        CreatableTopicConfigs, CreatableTopicResult, CreateTopicsResponse,
    },
    krabka_units::Time,
};

pub(super) fn topic_error_result(
    name: String,
    error_code: i16,
    error_message: Option<String>,
) -> CreatableTopicResult {
    CreatableTopicResult {
        name,
        error_code,
        error_message,
        // Kafka never sets `Configs` on a refused row, so it goes out as the
        // generated Java default: an empty list, not null (`FieldSpec`
        // gives an array field without `"default": "null"` `new ArrayList(0)`).
        configs: Some(Vec::new()),
        ..Default::default()
    }
}

/// KIP-525: the topic's effective configuration, in the shape a v5+
/// `CreatableTopicResult` carries it.
///
/// It is the very list `DescribeConfigs` answers a `TOPIC` resource with --
/// Kafka reaches `ConfigurationControlManager.computeEffectiveTopicConfigs`
/// from both paths, and a client that reads `createTopics(...).config(topic)`
/// instead of issuing a follow-up `DescribeConfigs` must see the same values.
/// `CreatableTopicConfigs` has no `config_type`, `documentation` or synonym
/// field, so those parts of the entry are dropped and the rest travels
/// unchanged, sensitive values withheld included.
///
/// `read_only` is the one place krabka reports more than Kafka does. Kafka
/// hardcodes `false` here (`KafkaConfigSchema.toConfigEntry`, "readonly is
/// always false, for now") because no topic key in its schema is read-only.
/// Two of krabka's are -- [`crate::config_keys::DISKLESS`] and
/// [`crate::config_keys::WRITE_FREEZE`] -- and reporting them as writable
/// would contradict this broker's own `DescribeConfigs` and its two alter
/// paths.
/// The supplied entries already resolve the create request's override map,
/// including a validate-only topic that is absent from the metadata image.
pub(super) fn creatable_topic_configs(
    entries: Vec<krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult>,
) -> Vec<CreatableTopicConfigs> {
    entries
        .into_iter()
        .map(|entry| CreatableTopicConfigs {
            name: entry.name,
            value: entry.value,
            read_only: entry.read_only,
            config_source: entry.config_source,
            is_sensitive: entry.is_sensitive,
            ..Default::default()
        })
        .collect()
}

pub(super) fn create_topics_response(
    topics: Vec<CreatableTopicResult>,
    throttle_time_ms: i32,
) -> CreateTopicsResponse {
    CreateTopicsResponse {
        topics,
        throttle_time_ms,
        ..Default::default()
    }
}

/// The topics an audit record may claim were created. A `validate_only`
/// request commits nothing, so its `NONE` rows are a verdict rather than a
/// creation and must never reach the audit trail.
pub(super) fn created_topic_resources(
    results: &[CreatableTopicResult],
    validate_only: bool,
) -> Vec<krabka_audit::AuditResource> {
    if validate_only {
        return Vec::new();
    }
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .map(|t| audit_resource("Topic", t.name.clone()))
        .collect()
}

pub(super) fn finish_response(
    broker: &Broker,
    context: &RequestContext<'_>,
    results: Vec<CreatableTopicResult>,
    validate_only: bool,
    delay: Time,
) -> CreateTopicsResponse {
    audit_admin_success(
        broker.audit_log.as_ref(),
        context,
        "CreateTopics",
        created_topic_resources(&results, validate_only),
    );
    // KIP-599: the controller-mutation delay goes to the dispatch loop, which
    // resolves it with the KIP-124 request quota in one metrics call and
    // reports the larger of the two, as Kafka's
    // `sendResponseMaybeThrottleWithControllerQuota` does. The response
    // carries the controller-mutation delay now; the dispatch loop raises it
    // when the request quota asks for more.
    context.defer_quota_charge((QuotaType::ControllerMutation, delay).into());
    create_topics_response(results, throttle_time_ms(delay))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn created_topic_resources_include_only_successful_topics() {
        let results = vec![
            CreatableTopicResult {
                name: "ok".into(),
                error_code: codes::NONE,
                ..Default::default()
            },
            CreatableTopicResult {
                name: "bad".into(),
                error_code: codes::TOPIC_ALREADY_EXISTS,
                ..Default::default()
            },
        ];

        let resources = created_topic_resources(&results, false);

        let expected = vec![krabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn created_topic_resources_are_empty_for_a_validate_only_request() {
        let results = vec![CreatableTopicResult {
            name: "ok".into(),
            error_code: codes::NONE,
            ..Default::default()
        }];

        assert!(created_topic_resources(&results, true) == Vec::new());
    }
}
