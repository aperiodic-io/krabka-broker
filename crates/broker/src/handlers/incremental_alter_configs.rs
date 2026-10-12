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

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    incremental_alter_configs_request::{
        self, AlterConfigsResource, IncrementalAlterConfigsRequest,
    },
    incremental_alter_configs_response::{
        AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
    },
};
use krabka_raft::RaftError;

use crate::{
    config_keys::{
        MIN_INSYNC_REPLICAS, broker_dynamic::BrokerConfigContext, clear_elr_records,
        group::GroupBounds,
    },
    error::BrokerError,
    handlers::{
        RequestContext, audit_resource, config_resource_refusal, decode_request, encode_response,
        forward_to_controller::{controller_listener_dispatch, to_active_controller},
        response_encoding::ErrorRow as _,
    },
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

/// Forward operations before merging them with authoritative metadata.
/// Broker ids and local log directories are preprocessed on the addressed
/// broker; logger resources continue to run there.
pub(crate) async fn dispatch(
    broker: &Broker,
    version: i16,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut req = decode_request::<IncrementalAlterConfigsRequest>(req_bytes, version)?;
    controller_listener_dispatch!(
        broker,
        ctx,
        req,
        version,
        self::handle,
        self::request_refusal
    );
    let image = broker.controller.current_image();
    let duplicates = super::alter_configs::duplicate_resource_flags(
        req.resources
            .iter()
            .map(|resource| (resource.resource_type, resource.resource_name.as_str())),
    );
    let mut local = Vec::new();
    let mut local_indices = Vec::new();
    let mut indexed_responses = Vec::new();
    let mut forwarded = Vec::new();
    let mut forwarded_indices = Vec::new();
    for (index, (resource, duplicate)) in std::mem::take(&mut req.resources)
        .into_iter()
        .zip(duplicates)
        .enumerate()
    {
        if resource.resource_type == RESOURCE_TYPE_BROKER_LOGGER {
            local_indices.push(index);
            local.push(resource);
            continue;
        }
        if resource.resource_type == RESOURCE_TYPE_BROKER {
            let shape = validate_resource_shape(&resource, duplicate);
            let preprocessed = shape.and_then(|()| {
                broker_scope::preprocess_broker_resource(
                    &resource,
                    BrokerConfigContext {
                        image: &image,
                        serving: broker.config.node_id,
                        log_dirs: &broker.config.broker_log_dirs(),
                        unstable: broker.config.features.unstable_api_versions,
                    },
                )
            });
            if let Err((code, message)) = preprocessed {
                let (code, message) = if validate_resource_shape(&resource, duplicate).is_err() {
                    (code, message)
                } else {
                    config_resource_refusal(
                        broker.config.authorizer.as_ref(),
                        &image,
                        ctx,
                        (resource.resource_type, &resource.resource_name),
                        true,
                    )
                    .unwrap_or((code, message))
                };
                indexed_responses.push((
                    index,
                    resource_refusal(&resource, code).with_error(code, message),
                ));
                continue;
            }
        }
        forwarded_indices.push(index);
        forwarded.push(resource);
    }
    req.resources = forwarded;
    let mut response = if req.resources.is_empty() {
        IncrementalAlterConfigsResponse::default()
    } else {
        let body = encode_response(&req, version)?;
        match to_active_controller(
            broker,
            incremental_alter_configs_request::API_KEY,
            &body,
            version,
            ctx,
            |code, message| encode_response(&request_refusal(&req, code, message), version),
        )
        .await
        {
            Some(answer) => decode_request::<IncrementalAlterConfigsResponse>(&answer?, version)?,
            None => handle(broker, req.clone(), version, ctx).await?,
        }
    };
    indexed_responses.extend(forwarded_indices.into_iter().zip(response.responses));
    if !local.is_empty() {
        req.resources = local;
        indexed_responses.extend(
            local_indices
                .into_iter()
                .zip(handle(broker, req, version, ctx).await?.responses),
        );
    }
    indexed_responses.sort_unstable_by_key(|(index, _)| *index);
    response.responses = indexed_responses.into_iter().map(|(_, row)| row).collect();
    encode_response(&response, version)
}

fn request_refusal(
    req: &IncrementalAlterConfigsRequest,
    code: i16,
    message: Option<&str>,
) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse {
        responses: req
            .resources
            .iter()
            .map(|resource| AlterConfigsResourceResponse {
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                error_code: code,
                error_message: message.map(str::to_owned),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

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
    _image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    // Read and admission are optimistic; the engine atomically checks that
    // image when it appends. Rebuild every operation after a conflicting commit.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut images = broker.controller.watch_image();
        let image = images.borrow_and_update().clone();
        let planned = tokio::time::timeout_at(
            deadline,
            process_resource_once(
                broker,
                &image,
                ctx,
                resource.clone(),
                validate_only,
                is_duplicate,
            ),
        )
        .await;
        let Ok(planned) = planned else {
            return resource_refusal(&resource, codes::REQUEST_TIMED_OUT);
        };
        match planned {
            Ok(response) => return response,
            Err(RaftError::StaleMetadataImage) if tokio::time::Instant::now() < deadline => {}
            Err(RaftError::UncommittedTail) if tokio::time::Instant::now() < deadline => {
                // Some committed control records advance the high watermark
                // without publishing a different image. Recheck admission even
                // when the image watch has no notification for that commit.
                tokio::select! {
                    changed = images.changed() => {
                        if changed.is_err() {
                            return resource_refusal(&resource, codes::NOT_CONTROLLER);
                        }
                    }
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return resource_refusal(&resource, codes::NOT_CONTROLLER);
            }
            Err(RaftError::StaleMetadataImage | RaftError::UncommittedTail) => {
                return resource_refusal(&resource, codes::REQUEST_TIMED_OUT);
            }
            Err(error) => {
                tracing::error!(%error, "IncrementalAlterConfigs submit_change failed");
                return resource_refusal(&resource, codes::UNKNOWN_SERVER_ERROR);
            }
        }
    }
}

fn resource_refusal(resource: &AlterConfigsResource, code: i16) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: code,
        error_message: None,
        ..Default::default()
    }
}

async fn process_resource_once(
    broker: &Broker,
    image: &Arc<MetadataImage>,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> Result<AlterConfigsResourceResponse, RaftError> {
    let mut out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_message: None,
        ..Default::default()
    };
    if let Err((code, message)) = validate_resource_shape(&resource, is_duplicate) {
        return Ok(out.with_error(code, message));
    }
    if let Some((code, message)) = config_resource_refusal(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        (resource.resource_type, &resource.resource_name),
        true,
    ) {
        return Ok(out.with_error(code, message));
    }

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
                    return Ok(out.with_error(code, message));
                }
            }
        }
        RESOURCE_TYPE_BROKER => {
            let serving = if ctx.forwarded && !resource.resource_name.is_empty() {
                resource
                    .resource_name
                    .parse::<u64>()
                    .map_or(broker.config.node_id, krabka_metadata::NodeId)
            } else {
                broker.config.node_id
            };
            let log_dirs = if ctx.forwarded {
                Vec::new()
            } else {
                broker.config.broker_log_dirs()
            };
            handle_broker_scoped(
                &resource,
                image,
                serving,
                (&log_dirs, broker.config.features.unstable_api_versions),
                &mut out,
                &mut to_submit,
            );
            if out.error_code != codes::NONE {
                return Ok(out);
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
            return Ok(out);
        }
        RESOURCE_TYPE_CLIENT_METRICS => {
            handle_client_metrics_scoped(&resource, image, &mut out, &mut to_submit);
            if out.error_code != codes::NONE {
                return Ok(out);
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
                return Ok(out);
            }
        }
        other => unreachable!("resource type {other} passed the ACL dispatch"),
    }

    if let Some((code, message)) = super::alter_configs::config_value_size_error(&to_submit) {
        return Ok(out.with_error(code, message));
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
        return Ok(out);
    }
    broker
        .controller
        .submit_change_if_image(to_submit, image.clone())
        .await?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;
    use crate::{
        handlers::incremental_alter_configs::test_support::make_scoped_resource,
        test_support::{context_helper, start_broker_with_authorizer as start_broker, test_ctx},
    };

    context_helper!(client_id = "admin-client");

    /// The wire response interleaves logger results, local broker refusals and
    /// controller results in the original order, keeping duplicate rows.
    #[tokio::test]
    async fn dispatch_preserves_mixed_resource_order_and_duplicates() {
        broker_fixture!((handle, _dir, broker), allow_all, context(ctx, "admin"));
        let resource = |kind, name| make_scoped_resource(kind, name, vec![]);
        let duplicate_topic = tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "duplicate".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let duplicate_logger = tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_BROKER_LOGGER,
            resource_name: "2".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let rows = [
            (
                resource(RESOURCE_TYPE_BROKER_LOGGER, "1"),
                tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER_LOGGER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_BROKER, "2"),
                tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER,
                    resource_name: "2".into(),
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("Unexpected broker id, expected 1, but received 2".into())
                }),
            ),
            (
                resource(RESOURCE_TYPE_TOPIC, "duplicate"),
                duplicate_topic.clone(),
            ),
            (
                resource(RESOURCE_TYPE_BROKER, "1"),
                tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_TOPIC, "missing"),
                tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "missing".into(),
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some("The topic 'missing' does not exist.".into())
                }),
            ),
            (resource(RESOURCE_TYPE_TOPIC, "duplicate"), duplicate_topic),
            (
                resource(RESOURCE_TYPE_BROKER_LOGGER, "2"),
                duplicate_logger.clone(),
            ),
            (resource(RESOURCE_TYPE_BROKER_LOGGER, "2"), duplicate_logger),
        ];
        for order in [[0, 1, 2, 3, 4, 5, 6, 7], [7, 6, 5, 4, 3, 2, 1, 0]] {
            let request = IncrementalAlterConfigsRequest {
                resources: order.iter().map(|&index| rows[index].0.clone()).collect(),
                validate_only: true,
                ..Default::default()
            };
            let bytes = encode_response(&request, 1).unwrap();
            let bytes = Box::pin(dispatch(&broker, 1, &bytes, &ctx)).await.unwrap();
            let response = decode_request::<IncrementalAlterConfigsResponse>(&bytes, 1).unwrap();
            let expected = IncrementalAlterConfigsResponse {
                responses: order.iter().map(|&index| rows[index].1.clone()).collect(),
                ..Default::default()
            };
            assert!(response == expected);
        }
        handle.shutdown().await;
    }

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
