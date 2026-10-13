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

use {
    crate::{
        config_keys::{
            MIN_INSYNC_REPLICAS, broker_dynamic::BrokerConfigContext, clear_elr_records,
            group::GroupBounds,
        },
        handlers::{
            ApiVersion, DispatchResult, RequestContext, audit_resource, config_resource_refusal,
            encode_response, forward_to_controller::to_active_controller_or_handle,
            response_encoding::ErrorRow as _,
        },
    },
    krabka_metadata::{MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        incremental_alter_configs_request::{
            self, AlterConfigsResource, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
    },
    krabka_raft::RaftError,
    std::{sync::Arc, time::Duration},
};

mod broker_logger_scope;
mod broker_scope;
mod client_metrics_scope;
mod group_scope;
#[cfg(test)]
mod test_support;
mod topic_scope;

fn set_error_response(out: &mut AlterConfigsResourceResponse, (code, message): (i16, String)) {
    out.error_code = code;
    out.error_message = Some(message);
}

use {
    self::{
        broker_logger_scope::handle_broker_logger_scoped, broker_scope::handle_broker_scoped,
        client_metrics_scope::handle_client_metrics_scoped, group_scope::handle_group_scoped,
        topic_scope::topic_config_record,
    },
    crate::{
        broker::Broker,
        codes,
        handlers::describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
};
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

krabka_macros::decoded_dispatch! {
    /// Keep logger changes on this node and validate broker keys against local settings.
    /// Topic, group, and other resource operations use the controller's image.
    pub(crate) async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    )
        [request = IncrementalAlterConfigsRequest, binding(req)] -> DispatchResult
    {
        let mut req = krabka_macros::controller_request!(broker, ctx, req, version; self::handle, self::request_refusal);
        let image = broker.controller.current_image();
        let duplicates = super::alter_configs::duplicate_resource_flags(req.resources.iter());
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
                    broker_scope::preprocess_broker_resource(&resource, BrokerConfigContext {
                        image: &image,
                        serving: broker.config.node_id,
                        log_dirs: &broker.config.broker_log_dirs(),
                        unstable: broker.config.features.unstable_api_versions,
                    })
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
            to_active_controller_or_handle(
                broker,
                req.clone(),
                incremental_alter_configs_request::API_KEY,
                version,
                ctx,
                request_refusal,
                |incremental_request| handle(broker, incremental_request, version, ctx),
            )
            .await?
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
        response.responses = super::alter_configs::ordered_resource_responses(indexed_responses);
        encode_response(&response, version)
    }
}

fn request_refusal(
    req: &IncrementalAlterConfigsRequest,
    code: i16,
    message: Option<&str>,
) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse {
        responses: super::alter_configs::resource_error_rows(req.resources.iter(), code, message),
        ..Default::default()
    }
}

krabka_macros::config_alter_handler! { request = IncrementalAlterConfigsRequest, response = IncrementalAlterConfigsResponse, operation = "IncrementalAlterConfigs", audit = audit_resources_for, process = process_resource, audits_in_process = false }

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
    super::alter_configs::duplicate_resource_flags(resources.iter())
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

/// Queue the records of one scope's validation, or answer its refusal on the
/// resource's response.
fn submit_or_refuse(
    result: Result<impl IntoIterator<Item = MetadataRecord>, (i16, String)>,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    match result {
        Ok(records) => to_submit.extend(records),
        Err(error) => set_error_response(out, error),
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
    let mut out: AlterConfigsResourceResponse =
        super::alter_configs::empty_resource_response(&resource);
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
                BrokerConfigContext {
                    image,
                    serving,
                    log_dirs: &log_dirs,
                    unstable: broker.config.features.unstable_api_versions,
                },
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

    use {
        super::*,
        crate::{
            handlers::{
                incremental_alter_configs::test_support::{
                    AlterableConfig, check, make_scoped_resource,
                },
                test_support::{assert_dispatch_response, ordered_rows},
            },
            test_support::start_broker_with_authorizer as start_broker,
        },
    };

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    /// The wire response interleaves logger results, local broker refusals and
    /// controller results in the original order, keeping duplicate rows.
    #[tokio::test]
    async fn dispatch_preserves_mixed_resource_order_and_duplicates() {
        krabka_macros::broker_test_setup! {
            bind(handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            user ["admin"];
        };
        let resource = |kind, name| make_scoped_resource(kind, name, vec![]);
        let duplicate_topic = krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "duplicate".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let duplicate_logger = krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_BROKER_LOGGER,
            resource_name: "2".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let rows = [
            (
                resource(RESOURCE_TYPE_BROKER_LOGGER, "1"),
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER_LOGGER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_BROKER, "2"),
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
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
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_TOPIC, "missing"),
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
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
            let ordered = ordered_rows(&rows, &order);
            let request = IncrementalAlterConfigsRequest {
                resources: ordered.requests,
                validate_only: true,
                ..Default::default()
            };
            let expected = IncrementalAlterConfigsResponse {
                responses: ordered.responses,
                ..Default::default()
            };
            let broker_ref = broker.as_ref();
            let context_ref = &ctx;
            assert_dispatch_response(&request, &expected, 1, |body| async move {
                dispatch(broker_ref, 1, &body, context_ref).await
            })
            .await;
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
                vec![resource(RESOURCE_TYPE_TOPIC, "t", vec![
                    config("retention.ms", OP_SET, Some("1")),
                    config("retention.ms", OP_DELETE, None),
                ])],
                vec![Err((
                    codes::INVALID_REQUEST,
                    "Error due to duplicate config keys".to_owned(),
                ))],
            ),
            (
                vec![resource(RESOURCE_TYPE_GROUP, "g", vec![
                    config("a", OP_SET, None),
                    config("b", OP_DELETE, None),
                    config("c", OP_APPEND, None),
                ])],
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
