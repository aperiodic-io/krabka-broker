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

use krabka_protocol::owned::{
    alter_configs_request::AlterConfigsResource,
    alter_configs_response::AlterConfigsResourceResponse,
};

use super::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    broker_configs::broker_config_records, client_metrics_configs::client_metrics_config_record,
    group_configs::group_config_record, topic_configs::topic_config_record,
    validate_resource_shape,
};
use crate::{broker::Broker, handlers::response_encoding::ErrorRow as _};

pub(super) async fn process_resource(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    super::config_resource_preamble!(
        (out, broker, image, ctx, resource),
        AlterConfigsResourceResponse,
        false,
        validate_resource_shape(
            is_duplicate,
            resource
                .configs
                .iter()
                .map(|cfg| (cfg.name.as_str(), cfg.value.is_none())),
        )
    );

    let records = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => topic_config_record(
            &resource,
            image,
            &broker.config.topic_policy,
            broker.config.remote_storage_backend.is_some(),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_BROKER => broker_config_records(
            &resource,
            crate::config_keys::broker_dynamic::BrokerConfigContext {
                image,
                serving: krabka_metadata::NodeId(broker.config.node_id.0),
                log_dirs: &broker.config.broker_log_dirs(),
                unstable: broker.config.features.unstable_api_versions,
            },
        ),
        RESOURCE_TYPE_GROUP => group_config_record(
            &resource,
            &crate::config_keys::group::GroupBounds::of(&broker.config),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_CLIENT_METRICS => {
            client_metrics_config_record(&resource).map(|record| vec![record])
        }
        _ => unreachable!("resource type passed ACL dispatch"),
    };
    let mut records = match records {
        Ok(records) => records,
        Err((code, message)) => {
            return out.with_error(code, message);
        }
    };
    if let Some((code, message)) = super::config_value_size_error(&records) {
        return out.with_error(code, message);
    }
    let min_isr_changed = records
        .iter()
        .any(|record| super::changes_min_isr(image, record, true));
    if min_isr_changed {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        records.extend(crate::config_keys::clear_elr_records(image, topic));
    }
    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return out;
    }
    if records.is_empty() {
        return out;
    }
    out.error_code = super::submission_code(
        broker.controller.submit_change(records).await,
        |e| tracing::error!(error = %e, "AlterConfigs submit_change failed"),
    );
    out
}
