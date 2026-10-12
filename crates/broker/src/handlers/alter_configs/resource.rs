//! The per-resource `AlterConfigs` work: the Kafka `preprocess` shape checks,
//! the authorization preamble, the dispatch to the resource's record
//! builder, and the metadata submit.
//!
//! One resource's outcome never depends on another's beyond whether it is a
//! duplicate resource reference (computed for the whole request up front), so
//! this module turns a single `AlterConfigsResource` into the one response
//! row it earns and the request entry point only loops over it.
//!
//! Kafka's `ConfigAdminManager.preprocess` validates the request shape before
//! `ControllerApis.authorizeAlterResource` runs, so a malformed row —
//! a resource named twice, a config key named twice, or a config with no
//! value — earns `INVALID_REQUEST` whether or not the principal may touch the
//! resource. That check runs first here too.

use std::{sync::Arc, time::Duration};

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};
use krabka_protocol::owned::{
    alter_configs_request::AlterConfigsResource,
    alter_configs_response::AlterConfigsResourceResponse,
};
use krabka_raft::RaftError;

use super::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    broker_configs::broker_config_records, client_metrics_configs::client_metrics_config_record,
    group_configs::group_config_record, topic_configs::topic_config_record,
    validate_resource_shape,
};
use crate::{
    broker::Broker,
    codes,
    config_keys::{broker_dynamic::BrokerConfigContext, clear_elr_records, group::GroupBounds},
    handlers::{
        RequestContext, audit_admin_success, config_resource_refusal,
        forward_to_controller::guarded_operation, response_encoding::ErrorRow as _,
    },
};

pub(super) async fn process_resource(
    broker: &Broker,
    _image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    let result = guarded_operation(
        broker,
        tokio::time::Instant::now() + Duration::from_secs(5),
        |image| {
            process_resource_once(
                broker,
                image,
                ctx,
                resource.clone(),
                validate_only,
                is_duplicate,
            )
        },
    )
    .await;
    let code = match result {
        Ok(Ok(response)) => return response,
        Err(_) => codes::REQUEST_TIMED_OUT,
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => codes::NOT_CONTROLLER,
        Ok(Err(error)) => {
            tracing::error!(%error, "AlterConfigs submit_change failed");
            codes::UNKNOWN_SERVER_ERROR
        }
    };
    AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name,
        error_code: code,
        error_message: None,
        ..Default::default()
    }
}

async fn process_resource_once(
    broker: &Broker,
    image: Arc<MetadataImage>,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> Result<AlterConfigsResourceResponse, RaftError> {
    let out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_message: None,
        ..Default::default()
    };
    if let Err((code, message)) = validate_resource_shape(
        is_duplicate,
        resource
            .configs
            .iter()
            .map(|cfg| (cfg.name.as_str(), cfg.value.is_none())),
    ) {
        return Ok(out.with_error(code, message));
    }
    if let Some((code, message)) = config_resource_refusal(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        (resource.resource_type, &resource.resource_name),
        false,
    ) {
        return Ok(out.with_error(code, message));
    }

    let records = resource_records(broker, &image, ctx, &resource);
    let mut records = match records {
        Ok(records) => records,
        Err((code, message)) => {
            return Ok(out.with_error(code, message));
        }
    };
    if let Some((code, message)) = super::config_value_size_error(&records) {
        return Ok(out.with_error(code, message));
    }
    let min_isr_changed = records
        .iter()
        .any(|record| super::changes_min_isr(&image, record, true));
    if min_isr_changed {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        records.extend(clear_elr_records(&image, topic));
    }
    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return Ok(out);
    }
    if records.is_empty() {
        return Ok(out);
    }
    let audited = super::audit_resources_for(&resource, &image);
    broker
        .controller
        .submit_change_if_image(records, image)
        .await?;
    audit_admin_success(broker.audit_log.as_ref(), ctx, "AlterConfigs", audited);
    Ok(out)
}

fn resource_records(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: &AlterConfigsResource,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    match resource.resource_type {
        RESOURCE_TYPE_TOPIC => topic_config_record(
            resource,
            image,
            &broker.config.topic_policy,
            broker.config.remote_storage_backend.is_some(),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_BROKER => broker_config_records(
            resource,
            BrokerConfigContext {
                image,
                serving: if ctx.forwarded && !resource.resource_name.is_empty() {
                    resource
                        .resource_name
                        .parse::<u64>()
                        .map_or(broker.config.node_id, NodeId)
                } else {
                    broker.config.node_id
                },
                log_dirs: &if ctx.forwarded {
                    Vec::new()
                } else {
                    broker.config.broker_log_dirs()
                },
                unstable: broker.config.features.unstable_api_versions,
            },
        ),
        RESOURCE_TYPE_GROUP => group_config_record(
            resource,
            &GroupBounds::of(&broker.config),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_CLIENT_METRICS => {
            client_metrics_config_record(resource).map(|record| vec![record])
        }
        _ => unreachable!("resource type passed ACL dispatch"),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerConfigRecord, MetadataRecord};
    use krabka_raft::NodeId;

    use super::*;
    use crate::{
        api_catalog::UnstableApiVersions,
        config_keys::REMOTE_STORAGE_ENABLE,
        handlers::{
            alter_configs::test_support::{
                broker_resource, image_with_broker, image_with_topic_config, topic_resource,
            },
            forward_to_controller::guarded_operation_on_source,
        },
        metadata_source::MetadataSource,
        test_support::FakeMetadataSource,
        throttle::{FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY},
        topic_policy::TopicPolicy,
    };

    async fn submit_replacement(
        source: &FakeMetadataSource,
        resource: &AlterConfigsResource,
        build: impl Fn(
            &AlterConfigsResource,
            &MetadataImage,
        ) -> Result<Vec<MetadataRecord>, (i16, String)>
        + Sync,
    ) -> Result<(), (i16, String)> {
        guarded_operation_on_source(
            source,
            tokio::time::Instant::now() + Duration::from_secs(1),
            |image| {
                let build = &build;
                async move {
                    let records = match build(resource, &image) {
                        Ok(records) => records,
                        Err(error) => return Ok(Err(error)),
                    };
                    source.submit_change_if_image(records, image).await?;
                    Ok(Ok(()))
                }
            },
        )
        .await
        .expect("bounded retry")
        .expect("guarded submission")
    }

    /// A full replacement must delete overrides committed after its first
    /// planning attempt as well as those the first image contained.
    #[tokio::test]
    async fn replacement_replans_tombstones_after_an_intervening_override() {
        let record = |key: &str, value: Option<&str>| {
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: key.into(),
                config_value: value.map(str::to_owned),
            })
        };
        let mut initial = image_with_broker(1);
        initial.apply(&record(LEADER_THROTTLED_RATE_KEY, Some("1")));
        let mut newer = initial.clone();
        newer.apply(&record(FOLLOWER_THROTTLED_RATE_KEY, Some("2")));
        let source = FakeMetadataSource::builder()
            .image(initial)
            .leader(Some(NodeId(1)))
            .before_guarded_submit(move |source| source.set_image(newer))
            .build();
        let resource = broker_resource("1", &[]);
        let result = submit_replacement(&source, &resource, |resource, image| {
            broker_config_records(
                resource,
                BrokerConfigContext {
                    image,
                    serving: NodeId(1),
                    log_dirs: &[],
                    unstable: UnstableApiVersions::Disabled,
                },
            )
        })
        .await;
        assert!(result == Ok(()));
        assert!(
            source.submitted_records()
                == vec![
                    record(FOLLOWER_THROTTLED_RATE_KEY, None),
                    record(LEADER_THROTTLED_RATE_KEY, None)
                ]
        );
    }

    /// A storage enable committed between validation and append changes the
    /// safety precondition for a disable. The retry must reject it.
    #[tokio::test]
    async fn replacement_rechecks_remote_storage_disable_after_a_conflict() {
        let initial = image_with_topic_config("t", &[(REMOTE_STORAGE_ENABLE, "false")]);
        let newer = image_with_topic_config("t", &[(REMOTE_STORAGE_ENABLE, "true")]);
        let source = FakeMetadataSource::builder()
            .image(initial)
            .leader(Some(NodeId(1)))
            .before_guarded_submit(move |source| source.set_image(newer))
            .build();
        let resource = topic_resource("t", &[(REMOTE_STORAGE_ENABLE, "false")]);
        let result = submit_replacement(&source, &resource, |resource, image| {
            topic_config_record(
                resource,
                image,
                &TopicPolicy::default(),
                true,
                UnstableApiVersions::Disabled,
            )
            .map(|record| vec![record])
        })
        .await;
        assert!(matches!(result, Err((codes::INVALID_CONFIG, _))));
        assert!(source.submitted_records().is_empty());
    }
}
