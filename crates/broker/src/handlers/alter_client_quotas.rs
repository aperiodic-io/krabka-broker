//! `AlterClientQuotas` (`api_key` 49, KIP-13/124/257).
//!
//! This file holds the wire entry point: the cluster `AlterConfigs` authorization
//! preamble, the loop that validates each entry, and the single metadata
//! submit that carries every accepted entry. Entry validation and the records
//! it produces live in `entries`; the response rows live in `response`.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, audit_admin_success, audit_resource,
            cluster_alter_configs_denied,
        },
    },
    krabka_metadata::MetadataRecord,
    krabka_protocol::owned::{
        alter_client_quotas_request::AlterClientQuotasRequest,
        alter_client_quotas_response::AlterClientQuotasResponse,
    },
    std::collections::HashSet,
};

mod entries;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        entries::{Alteration, alter_client_quotas},
        response::{apply_submit_error, err_entry, ok_entry, whole_request_error},
    },
    crate::{
        codes::{CLUSTER_AUTHORIZATION_FAILED, NONE},
        quota::parse_ip_literal,
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<AlterClientQuotasResponse, BrokerError> {
    let image = broker.controller.current_image();
    // Kafka's `ControllerApis.handleAlterClientQuotas` authorizes
    // `AlterConfigs` on the cluster. `Alter` does not imply it. A denial
    // answers `AlterClientQuotasRequest.getErrorResponse`: every entry
    // carries the error code and the default message of the error.
    if cluster_alter_configs_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return Ok(whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        ));
    }

    let resolvable = resolve_ip_names(&req).await;
    let ip_is_valid = |name: &str| parse_ip_literal(name).is_some() || resolvable.contains(name);
    let Alteration { results, records } =
        alter_client_quotas(&req.entries, image.client_quotas(), &ip_is_valid);
    let mut entry_results: Vec<_> = results
        .iter()
        .map(|(entity, outcome)| match outcome {
            Ok(()) => ok_entry(entity),
            Err((code, msg)) => err_entry(entity, *code, msg.clone()),
        })
        .collect();
    // Kafka's `QuorumController.alterClientQuotas` drops the records of a
    // `validate_only` request and keeps its results.
    let to_submit: Vec<MetadataRecord> = if req.validate_only {
        Vec::new()
    } else {
        records
    };

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "alter-client-quotas submit failed");
        apply_submit_error(&mut entry_results, e);
    }

    if !req.validate_only {
        audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "AlterClientQuotas",
            entry_results
                .iter()
                .filter(|entry| entry.error_code == NONE)
                .map(|entry| audit_resource("ClientQuotaEntity", quota_entity_name(entry)))
                .collect(),
        );
    }

    Ok(krabka_macros::unthrottled_wire!(
        AlterClientQuotasResponse {
            entries: entry_results,
        }
    ))
}

/// Resolves every named `ip` entity that is not an IP literal.
///
/// Kafka's `isValidIpEntity` accepts any name `InetAddress.getByName`
/// resolves, which covers host names as well as IPv4 and IPv6 literals. The
/// lookup runs here, before the synchronous validation, so that validation
/// stays a pure function of the request and the quota state.
async fn resolve_ip_names(req: &AlterClientQuotasRequest) -> HashSet<String> {
    let mut resolvable = HashSet::new();
    for entry in &req.entries {
        for component in &entry.entity {
            let Some(name) = component.entity_name.as_deref() else {
                continue;
            };
            if component.entity_type != "ip"
                || name.is_empty()
                || parse_ip_literal(name).is_some()
                || resolvable.contains(name)
            {
                continue;
            }
            // A literal address never reaches this lookup. A host name is
            // unresolvable on a platform without name resolution, WASI.
            if krabka_client_core::transport::resolve(&format!("{name}:0"))
                .await
                .is_ok_and(|addrs| !addrs.is_empty())
            {
                resolvable.insert(name.to_owned());
            }
        }
    }
    resolvable
}

/// Renders a quota entity as the audit record's resource name.
///
/// KIP-546 addresses a quota by a set of `(entity_type, entity_name)` pairs,
/// and a `None` name is the default entity for that type — what
/// `kafka-configs --entity-type users --entity-default` sets. `<default>` is
/// the spelling the JVM tools print for it.
fn quota_entity_name(
    entry: &krabka_protocol::owned::alter_client_quotas_response::EntryData,
) -> String {
    entry
        .entity
        .iter()
        .map(|e| {
            format!(
                "{}={}",
                e.entity_type,
                e.entity_name.as_deref().unwrap_or("<default>")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
