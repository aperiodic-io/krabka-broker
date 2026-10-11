//! `AlterConfigs` (`api_key=33`) for topic and broker resources.
//!
//! The handler builds each resource's full override map from the request.
//! That map is the *complete* set of non-default values for the resource.
//! Topic configs use one authoritative `V1TopicConfig` record. Broker configs
//! use Kafka-compatible per-key `V1BrokerConfig` records, including tombstones
//! for overrides omitted from the replacement. An empty broker resource name
//! targets Kafka's cluster-wide default broker config.
//!
//! Controller-managed broker keys stand outside the replacement. The handler
//! rejects a request that names one, and the tombstone sweep leaves them in
//! place. See [`crate::config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS`].
//!
//! A topic resource has the same rule. KFC-9's
//! [`crate::config_keys::WRITE_FREEZE`] is synthesised for `DescribeConfigs`
//! and is never stored, so the handler rejects a request that names it. See
//! [`crate::config_keys::topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! This file holds the wire entry point and the resource-type constants. The
//! per-resource work lives in `resource`, and the record builders it
//! dispatches to live in `topic_configs` and `broker_configs`.

use krabka_protocol::owned::{
    alter_configs_request::AlterConfigsRequest, alter_configs_response::AlterConfigsResponse,
};

mod broker_configs;
mod client_metrics_configs;
mod group_configs;
mod resource;
mod topic_configs;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::resource::process_resource;
use crate::{
    codes::{INVALID_CONFIG, INVALID_REQUEST, NONE, NOT_CONTROLLER, UNKNOWN_SERVER_ERROR},
    config_keys::{MIN_INSYNC_REPLICAS, is_controller_managed_topic_config},
    handlers::{
        audit_resource,
        describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
};

/// Both config APIs validate and audit resources in request order.
macro_rules! config_alter_handler {
    ($request:ty => $($response:ident)::+, $operation:literal, $audit:path, $process:path) => {
        pub(crate) async fn handle(
            broker: &crate::broker::Broker,
            req: $request,
            _version: i16,
            ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<$($response)::+, crate::error::BrokerError> {
            let image = broker.controller.current_image();
            let mut responses = Vec::with_capacity(req.resources.len());
            let validate_only = req.validate_only;
            let mut audited: Vec<krabka_audit::AuditResource> = Vec::new();
            let duplicate_flags = crate::handlers::alter_configs::duplicate_resource_flags(
                req.resources.iter().map(|resource| (resource.resource_type, resource.resource_name.as_str())),
            );
            for (resource, is_duplicate) in req.resources.into_iter().zip(duplicate_flags) {
                let named = $audit(&resource, &image);
                let response = $process(broker, &image, ctx, resource, validate_only, is_duplicate).await;
                // A dry run stores nothing and cannot claim a changed resource.
                if response.error_code == crate::codes::NONE && !validate_only {
                    audited.extend(named);
                }
                responses.push(response);
            }
            crate::handlers::audit_admin_success(broker.audit_log.as_ref(), ctx, $operation, audited);
            Ok($($response)::+ { responses, throttle_time_ms: 0, ..Default::default() })
        }
    };
}
pub(super) use config_alter_handler;

config_alter_handler!(AlterConfigsRequest => AlterConfigsResponse, "AlterConfigs", audit_resources_for, process_resource);

/// Shape checks precede authorization for both config APIs; refused rows retain their resource.
macro_rules! config_resource_preamble {
    (($out:ident, $broker:ident, $image:ident, $ctx:ident, $resource:ident), $($row:ident)::+, $include_logger:expr, $shape:expr) => {
        let mut $out = $($row)::+ {
            resource_type: $resource.resource_type,
            resource_name: $resource.resource_name.clone(),
            error_code: crate::codes::NONE,
            error_message: None,
            ..Default::default()
        };
        if let Err((code, message)) = $shape {
            return $out.with_error(code, message);
        }
        if let Some((code, message)) = crate::handlers::config_resource_refusal(
            $broker.config.authorizer.as_ref(), $image, $ctx,
            ($resource.resource_type, &$resource.resource_name), $include_logger,
        ) {
            return $out.with_error(code, message);
        }
    };
}
pub(super) use config_resource_preamble;

/// Both mutation APIs map controller submission failures to the same Kafka code.
pub(super) fn submission_code<T>(
    result: Result<T, krabka_raft::RaftError>,
    log_failure: impl FnOnce(krabka_raft::RaftError),
) -> i16 {
    match result {
        Ok(_) => NONE,
        Err(krabka_raft::RaftError::NotLeader { .. } | krabka_raft::RaftError::LeaderUnknown) => {
            NOT_CONTROLLER
        }
        Err(error) => {
            log_failure(error);
            UNKNOWN_SERVER_ERROR
        }
    }
}

/// Names the audited resource and the keys the request changes on it.
///
/// The values never reach the audit record. A config value can be a password
/// or a key store path, and the record only has to say who changed what.
fn audit_resources_for(
    resource: &krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Vec<krabka_audit::AuditResource> {
    let mut out = vec![crate::handlers::audit_resource(
        config_resource_type(resource.resource_type),
        resource.resource_name.clone(),
    )];
    let keys = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => changed_config_keys(
            resource,
            image.topic_config(&resource.resource_name),
            is_controller_managed_topic_config,
        ),
        RESOURCE_TYPE_GROUP => changed_config_keys(
            resource,
            image.group_config(&resource.resource_name),
            |_| false,
        ),
        RESOURCE_TYPE_CLIENT_METRICS => changed_config_keys(
            resource,
            image.client_metrics_config(&resource.resource_name),
            |_| false,
        ),
        _ => resource
            .configs
            .iter()
            .map(|config| config.name.clone())
            .collect(),
    };
    out.extend(keys.into_iter().map(|key| audit_resource("ConfigKey", key)));
    out
}

/// The config keys a full-map-replacement resource's request changes.
///
/// `AlterConfigs` replaces the whole override map for a Topic, Group, or
/// `ClientMetrics` resource, so a stored key the request omits is deleted by
/// the request as surely as one it restates with a new value. Naming only
/// the request's own keys would leave those deletions out of the audit
/// trail: replacing `{retention.ms, cleanup.policy}` with `{retention.ms}`
/// removes the compaction policy and would record nothing. A key whose
/// stored value the request restates unchanged changed nothing and is left
/// out.
///
/// `is_controller_managed` excludes keys that stand outside the
/// replacement: no client can name one and the record builder carries the
/// stored value forward, so their absence from the request is not a
/// deletion. Only Topic resources have such keys; Group and `ClientMetrics`
/// pass a predicate that always returns `false`.
fn changed_config_keys(
    resource: &krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
    stored: Option<&std::collections::BTreeMap<String, String>>,
    is_controller_managed: impl Fn(&str) -> bool,
) -> Vec<String> {
    let replacement: std::collections::BTreeMap<&str, &str> = resource
        .configs
        .iter()
        .map(|config| (config.name.as_str(), config.value.as_deref().unwrap_or("")))
        .collect();
    let mut changed: std::collections::BTreeSet<String> = replacement
        .iter()
        .filter(|(key, value)| {
            stored
                .and_then(|stored| stored.get(**key))
                .map(String::as_str)
                != Some(**value)
        })
        .map(|(key, _)| (*key).to_owned())
        .collect();
    changed.extend(
        stored
            .into_iter()
            .flatten()
            .filter(|(key, _)| {
                !is_controller_managed(key) && !replacement.contains_key(key.as_str())
            })
            .map(|(key, _)| key.clone()),
    );
    changed.into_iter().collect()
}

/// Kafka's `ConfigAdminManager.preprocess` rejects a request that names the
/// same `(resource_type, resource_name)` pair more than once, on every row
/// that names it. This computes that flag for each resource in request order
/// before any of them is authorized or processed, since the duplicate check
/// has to see the whole request at once.
///
/// `resources` yields each row's `(resource_type, resource_name)` in request
/// order. `AlterConfigs` and `IncrementalAlterConfigs` both call this.
pub(super) fn duplicate_resource_flags<'a>(
    resources: impl IntoIterator<Item = (i8, &'a str)>,
) -> Vec<bool> {
    let keys: Vec<(i8, &str)> = resources.into_iter().collect();
    let mut counts: std::collections::HashMap<(i8, &str), usize> = std::collections::HashMap::new();
    for key in &keys {
        *counts.entry(*key).or_insert(0) += 1;
    }
    keys.iter().map(|key| counts[key] > 1).collect()
}

/// Kafka's `ConfigAdminManager.preprocess` shape checks, which run before
/// any authorization: a resource named more than once in the request, a
/// config key named more than once within a resource, and a null value where
/// the API does not allow one.
///
/// `configs` yields each config's `(name, null_not_allowed)`, where the flag
/// is set when the config carries no value and the operation needs one.
/// Legacy `AlterConfigs` never deletes by omitting a value, so every null is
/// refused there. `IncrementalAlterConfigs` allows a null on DELETE only.
pub(super) fn validate_resource_shape<'a>(
    is_duplicate: bool,
    configs: impl IntoIterator<Item = (&'a str, bool)>,
) -> Result<(), (i16, String)> {
    if is_duplicate {
        return Err((
            INVALID_REQUEST,
            "Each resource must appear at most once.".into(),
        ));
    }
    let configs: Vec<(&str, bool)> = configs.into_iter().collect();
    let mut seen = std::collections::BTreeSet::new();
    if configs.iter().any(|(name, _)| !seen.insert(*name)) {
        return Err((INVALID_REQUEST, "Error due to duplicate config keys".into()));
    }
    let null_names: Vec<&str> = configs
        .iter()
        .filter(|(_, null_not_allowed)| *null_not_allowed)
        .map(|(name, _)| *name)
        .collect();
    if !null_names.is_empty() {
        return Err((
            INVALID_REQUEST,
            format!("Null value not supported for : {}", null_names.join(", ")),
        ));
    }
    Ok(())
}

/// The longest config value Kafka's controller writes: `Short.MAX_VALUE`
/// UTF-16 code units, which is what `String.length()` counts.
const MAX_CONFIG_VALUE_LENGTH: usize = 32_767;

/// A changed minimum ISR clears ELR; incremental topic requests opt in only
/// when they mention the key, while legacy replacement always considers it.
pub(super) fn changes_min_isr(
    image: &krabka_metadata::MetadataImage,
    record: &krabka_metadata::MetadataRecord,
    include_topic: bool,
) -> bool {
    match record {
        krabka_metadata::MetadataRecord::V1TopicConfig(config) => {
            include_topic
                && image
                    .topic_config(&config.topic)
                    .and_then(|current| current.get(MIN_INSYNC_REPLICAS))
                    != config.overrides.get(MIN_INSYNC_REPLICAS)
        }
        krabka_metadata::MetadataRecord::V1BrokerConfig(config) => {
            config.node_id == krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                && config.config_name == MIN_INSYNC_REPLICAS
                && image
                    .broker_config(config.node_id)
                    .and_then(|current| current.get(&config.config_name))
                    != config.config_value.as_ref()
        }
        _ => false,
    }
}

/// Kafka's `ConfigurationControlManager.validateAlterConfig` refuses, for
/// every resource type, a written value longer than [`MAX_CONFIG_VALUE_LENGTH`]
/// with `INVALID_CONFIG` (`DISALLOWED_CONFIG_VALUE_SIZE_ERROR`), because a
/// `ConfigRecord` cannot carry more.
///
/// `records` are the ones a resource's alter ended up building, which hold
/// every value the alter writes.
pub(super) fn config_value_size_error(
    records: &[krabka_metadata::MetadataRecord],
) -> Option<(i16, String)> {
    use krabka_metadata::MetadataRecord;

    let too_long = |value: &String| value.encode_utf16().count() > MAX_CONFIG_VALUE_LENGTH;
    let oversized = records.iter().any(|record| match record {
        MetadataRecord::V1TopicConfig(config) => config.overrides.values().any(too_long),
        MetadataRecord::V1BrokerConfig(config) => config.config_value.iter().any(too_long),
        MetadataRecord::V1GroupConfig(config) => config.configs.values().any(too_long),
        MetadataRecord::V1ClientMetricsConfig(config) => config.configs.values().any(too_long),
        _ => false,
    });
    oversized.then(|| {
        (
            INVALID_CONFIG,
            format!(
                "The configuration value cannot be added because it exceeds the maximum value \
                 size of {MAX_CONFIG_VALUE_LENGTH} bytes."
            ),
        )
    })
}

/// The audit `resource_type` for a KIP-133 config resource-type discriminant.
pub(super) fn config_resource_type(resource_type: i8) -> &'static str {
    match resource_type {
        RESOURCE_TYPE_TOPIC => "Topic",
        RESOURCE_TYPE_BROKER => "Broker",
        RESOURCE_TYPE_BROKER_LOGGER => "BrokerLogger",
        RESOURCE_TYPE_CLIENT_METRICS => "ClientMetrics",
        RESOURCE_TYPE_GROUP => "Group",
        _ => "Unknown",
    }
}
