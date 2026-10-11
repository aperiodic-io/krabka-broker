//! `IncrementalAlterConfigs` (`api_key=44`). Same target as `AlterConfigs`
//! (a single `V1TopicConfig` record per resource) but the wire request
//! carries per-key operations (SET/DELETE/APPEND/SUBTRACT). The handler
//! reads the current overrides from the metadata image, applies the ops,
//! validates the result, and submits the merged map.
//!
//! Supported operations:
//! - SET (0): set or replace
//! - DELETE (1): remove
//! - APPEND (2) and SUBTRACT (3) merge items into, or out of, a key Kafka
//!   types `LIST`, starting from its default when the resource does not hold
//!   it. Any other key refuses them with `INVALID_CONFIG`, in Kafka's words.
//!
//! `BROKER_LOGGER` (8) is the one resource type that stages no metadata
//! record at all: it retargets this node's live `tracing` filter and nothing
//! else, which is what a JVM broker does with its log4j2 context. See
//! [`broker_logger_scope`].
//!
//! A controller-managed key is refused whatever the operation is. KFC-9's
//! [`crate::config_keys::WRITE_FREEZE`] is synthesised for `DescribeConfigs`
//! and is never stored, so a DELETE of it is an attempt to lift the freeze and
//! gets the same refusal a SET gets. See
//! [`crate::config_keys::topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! This file holds the request entry point and the per-resource dispatch. Each
//! resource type has its own submodule that owns the key whitelist, the value
//! validation, and the metadata record that it stages.

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    incremental_alter_configs_request::{AlterConfigsResource, IncrementalAlterConfigsRequest},
    incremental_alter_configs_response::{
        AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
    },
};

use crate::{
    config_keys::{MIN_INSYNC_REPLICAS, clear_elr_records, group::GroupBounds},
    handlers::{RequestContext, audit_resource, response_encoding::ErrorRow as _},
};

mod broker_logger_scope;
mod broker_scope;
mod client_metrics_scope;
mod group_scope;
#[cfg(test)]
mod test_support;
mod topic_scope;

use self::{
    broker_logger_scope::handle_broker_logger_scoped, broker_scope::handle_broker_scoped,
    client_metrics_scope::handle_client_metrics_scoped, group_scope::handle_group_scoped,
    topic_scope::topic_config_record,
};
use crate::{
    broker::Broker,
    codes,
    handlers::describe_configs::{
        RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
        RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    },
};
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

super::alter_configs::config_alter_handler!(
    IncrementalAlterConfigsRequest => IncrementalAlterConfigsResponse,
    "IncrementalAlterConfigs", audit_resources_for, process_resource
);

/// Audit names include the resource and keys, never sensitive config values.
fn audit_resources_for(
    resource: &AlterConfigsResource,
    _image: &MetadataImage,
) -> Vec<krabka_audit::AuditResource> {
    std::iter::once(audit_resource(
        super::alter_configs::config_resource_type(resource.resource_type),
        resource.resource_name.clone(),
    ))
    .chain(
        resource
            .configs
            .iter()
            .map(|config| audit_resource("ConfigKey", config.name.clone())),
    )
    .collect()
}

/// Flags each resource named more than once in the request. See
/// [`super::alter_configs::duplicate_resource_flags`].
#[cfg(test)]
fn resource_duplicate_flags(resources: &[AlterConfigsResource]) -> Vec<bool> {
    super::alter_configs::duplicate_resource_flags(
        resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str())),
    )
}

/// Kafka's `ConfigAdminManager.preprocess` shape checks, with a null value
/// legal on DELETE only. See
/// [`super::alter_configs::validate_resource_shape`].
fn validate_resource_shape(
    resource: &AlterConfigsResource,
    is_duplicate: bool,
) -> Result<(), (i16, String)> {
    super::alter_configs::validate_resource_shape(
        is_duplicate,
        resource.configs.iter().map(|config| {
            (
                config.name.as_str(),
                config.config_operation != OP_DELETE && config.value.is_none(),
            )
        }),
    )
}

async fn process_resource(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    super::alter_configs::config_resource_preamble!(
        (out, broker, image, ctx, resource),
        AlterConfigsResourceResponse,
        true,
        validate_resource_shape(&resource, is_duplicate)
    );

    // After ACL pass: dispatch by resource type.
    let mut to_submit: Vec<MetadataRecord> = Vec::new();

    match resource.resource_type {
        RESOURCE_TYPE_TOPIC => {
            match topic_config_record(
                &resource,
                image,
                &broker.config.topic_policy,
                broker.config.remote_storage_backend.is_some(),
                broker.config.features.unstable_api_versions,
            ) {
                Ok(record) => to_submit.push(record),
                Err((code, message)) => {
                    return out.with_error(code, message);
                }
            }
        }
        RESOURCE_TYPE_BROKER => {
            handle_broker_scoped(
                &resource,
                image,
                krabka_metadata::NodeId(broker.config.node_id.0),
                (
                    &broker.config.broker_log_dirs(),
                    broker.config.features.unstable_api_versions,
                ),
                &mut out,
                &mut to_submit,
            );
            if out.error_code != codes::NONE {
                return out;
            }
        }
        RESOURCE_TYPE_BROKER_LOGGER => {
            // Node-local and never persisted, so this returns without
            // reaching `submit_change` whatever the outcome.
            handle_broker_logger_scoped(
                &resource,
                broker.config.broker_id,
                &broker.config.log_levels,
                validate_only,
                &mut out,
            );
            return out;
        }
        RESOURCE_TYPE_CLIENT_METRICS => {
            handle_client_metrics_scoped(&resource, image, &mut out, &mut to_submit);
            if out.error_code != codes::NONE {
                return out;
            }
        }
        RESOURCE_TYPE_GROUP => {
            handle_group_scoped(
                &resource,
                image,
                &GroupBounds::of(&broker.config),
                broker.config.features.unstable_api_versions,
                &mut out,
                &mut to_submit,
            );
            if out.error_code != codes::NONE {
                return out;
            }
        }
        other => unreachable!("resource type {other} passed the ACL dispatch"),
    }

    if let Some((code, message)) = super::alter_configs::config_value_size_error(&to_submit) {
        return out.with_error(code, message);
    }

    if to_submit.iter().any(|record| {
        let include_topic = !matches!(record, MetadataRecord::V1TopicConfig(_))
            || resource
                .configs
                .iter()
                .any(|item| item.name == MIN_INSYNC_REPLICAS);
        super::alter_configs::changes_min_isr(image, record, include_topic)
    }) {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        to_submit.extend(clear_elr_records(image, topic));
    }

    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return out;
    }
    out.error_code = super::alter_configs::submission_code(
        broker.controller.submit_change(to_submit).await,
        |e| tracing::error!(error = %e, "IncrementalAlterConfigs submit_change failed"),
    );
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;

    fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
        AlterableConfig {
            name: name.into(),
            config_operation: operation,
            value: value.map(str::to_owned),
            ..Default::default()
        }
    }

    /// Kafka's `ConfigAdminManager.preprocess` shape checks, which answer
    /// before any authorization. Each row is a request's resources and the
    /// outcome for each.
    #[test]
    fn the_request_shape_is_checked_the_way_kafka_preprocesses_it() {
        let resource =
            |resource_type: i8, name: &str, configs: Vec<AlterableConfig>| AlterConfigsResource {
                resource_type,
                resource_name: name.into(),
                configs,
                ..Default::default()
            };
        let duplicate = Err((
            codes::INVALID_REQUEST,
            "Each resource must appear at most once.".to_owned(),
        ));
        let cases = [
            (
                vec![
                    resource(RESOURCE_TYPE_TOPIC, "t", vec![]),
                    resource(RESOURCE_TYPE_TOPIC, "t", vec![]),
                    resource(RESOURCE_TYPE_GROUP, "t", vec![]),
                ],
                vec![duplicate.clone(), duplicate, Ok(())],
            ),
            (
                vec![resource(
                    RESOURCE_TYPE_TOPIC,
                    "t",
                    vec![
                        config("retention.ms", OP_SET, Some("1")),
                        config("retention.ms", OP_DELETE, None),
                    ],
                )],
                vec![Err((
                    codes::INVALID_REQUEST,
                    "Error due to duplicate config keys".to_owned(),
                ))],
            ),
            (
                vec![resource(
                    RESOURCE_TYPE_GROUP,
                    "g",
                    vec![
                        config("a", OP_SET, None),
                        config("b", OP_DELETE, None),
                        config("c", OP_APPEND, None),
                    ],
                )],
                vec![Err((
                    codes::INVALID_REQUEST,
                    "Null value not supported for : a, c".to_owned(),
                ))],
            ),
        ];
        for (resources, want) in cases {
            let flags = resource_duplicate_flags(&resources);
            let got: Vec<Result<(), (i16, String)>> = resources
                .iter()
                .zip(flags)
                .map(|(resource, is_duplicate)| validate_resource_shape(resource, is_duplicate))
                .collect();
            check!(got == want);
        }
    }
}
