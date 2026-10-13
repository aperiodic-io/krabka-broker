//! `DeleteAcls` handler (`api_key` 31).
//!
//! This handler authorizes `Alter` on `Cluster`. For each filter it decodes
//! the wire axes into Kafka's `AclBindingFilter` and reports the ACL entries
//! that match. It then submits one deletion record per distinct matched entry,
//! as Kafka's `AclControlManager.deleteAcls` does, and returns one filter
//! result per request filter.
//!
//! On a cluster with no authorizer configured every filter is refused with
//! `SECURITY_DISABLED`, the same answer its `DescribeAcls` counterpart gives.
//!
//! This file keeps the whole-request cluster gate and the per-filter loop.
//! Filter decoding lives in `filter`, the response rows in `response`, and
//! the audit trail in `audit`.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, cluster_alter_denied},
    },
    krabka_metadata::{AclEntry, MetadataRecord},
    krabka_protocol::{
        ProtocolError,
        owned::{
            delete_acls_request::DeleteAclsRequest,
            delete_acls_response::{DeleteAclsFilterResult, DeleteAclsResponse},
        },
    },
    std::collections::HashSet,
};

mod audit;
mod filter;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        audit::{audit_deleted_acls, deleted_acl_resources},
        filter::{build_filter, exact_filter},
        response::{apply_submit_error, delete_acls_response, filter_result, matching_acl_result},
    },
    super::acl_wire::{
        MAX_ACL_RECORDS_PER_REQUEST, NO_AUTHORIZER_EXCEPTION_MESSAGE,
        binding_filter::{AclBindingFilter, UnknownElement},
    },
    crate::codes,
};

/// The message of a cluster-alter refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_ALTER_DENIED_MESSAGE: &str = "Request DeleteAcls needs ALTER permission.";

/// The distinct ACLs a request removes, in the order its filters first match
/// them.
#[derive(Default)]
struct Doomed<'a> {
    entries: Vec<&'a AclEntry>,
    /// The position in the image's ACL iteration of each entry, which
    /// identifies it without comparing the whole tuple.
    positions: HashSet<usize>,
}

/// The ACLs of `acls` that `filter` matches, added to `doomed`, or `None` once
/// a match would take a request past `bound` removals.
///
/// `AclControlManager.deleteAclsForFilter` checks the bound before it adds a
/// match, so the match after the last one that fits fails the request, even
/// when an earlier filter already collected that ACL.
fn match_filter<'a>(
    acls: impl Iterator<Item = &'a AclEntry>,
    filter: &AclBindingFilter,
    doomed: &mut Doomed<'a>,
    bound: usize,
) -> Option<Vec<&'a AclEntry>> {
    let mut matched = Vec::new();
    for (position, entry) in acls.enumerate().filter(|(_, entry)| filter.matches(entry)) {
        if doomed.entries.len() >= bound {
            return None;
        }
        matched.push(entry);
        if doomed.positions.insert(position) {
            doomed.entries.push(entry);
        }
    }
    Some(matched)
}

pub(crate) async fn handle(
    broker: &Broker,
    req: DeleteAclsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DeleteAclsResponse, BrokerError> {
    // Kafka's `DeleteAclsRequest` constructor refuses an `UNKNOWN` element in
    // any filter while the request parses, before any authorization, and the
    // broker closes the connection. The error return is that close.
    let filters = req
        .filters
        .iter()
        .map(build_filter)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|UnknownElement| {
            ProtocolError::InvalidValue("Filters contain UNKNOWN elements")
        })?;

    let image = broker.controller.current_image();

    // Whole-request cluster-alter gate.
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let filter_results = filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some(CLUSTER_ALTER_DENIED_MESSAGE.into()),
                    Vec::new(),
                )
            })
            .collect();
        return Ok(delete_acls_response(filter_results));
    }

    // No authorizer: there is nothing to delete. Kafka builds this response
    // from `DeleteAclsRequest.getErrorResponse` with a
    // `SecurityDisabledException`, which stamps the same code and message on
    // one filter result per filter and leaves every matching-ACL list empty.
    if !broker.config.authorizer.is_configured() {
        let filter_results = filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::SECURITY_DISABLED,
                    Some(NO_AUTHORIZER_EXCEPTION_MESSAGE.into()),
                    Vec::new(),
                )
            })
            .collect();
        return Ok(delete_acls_response(filter_results));
    }

    // Kafka's `AclControlManager.deleteAcls` matches every filter against the
    // same ACL set, so an ACL two filters match is listed under both, and it
    // collects the removal records into a set, so that ACL is removed once.
    let mut filter_results: Vec<DeleteAclsFilterResult> = Vec::with_capacity(filters.len());
    let mut doomed = Doomed::default();
    for filter in &filters {
        if let Some(message) = filter.unknown_message() {
            filter_results.push(filter_result(
                codes::INVALID_REQUEST,
                Some(message.into()),
                Vec::new(),
            ));
            continue;
        }
        let Some(matched) = match_filter(
            image.all_acls(),
            filter,
            &mut doomed,
            MAX_ACL_RECORDS_PER_REQUEST,
        ) else {
            // No filter keeps a partial result and nothing is deleted.
            let message = format!(
                "Cannot remove more than {MAX_ACL_RECORDS_PER_REQUEST} acls in a single delete \
                 operation."
            );
            let filter_results = filters
                .iter()
                .map(|_| filter_result(codes::INVALID_REQUEST, Some(message.clone()), Vec::new()))
                .collect();
            return Ok(delete_acls_response(filter_results));
        };
        filter_results.push(filter_result(
            codes::NONE,
            None,
            matched.iter().map(|e| matching_acl_result(e)).collect(),
        ));
    }

    let to_submit: Vec<MetadataRecord> = doomed
        .entries
        .into_iter()
        .map(|entry| MetadataRecord::V1DeleteAccessControlEntry(exact_filter(entry)))
        .collect();
    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "delete-acls submit failed");
        apply_submit_error(&mut filter_results, &e);
    }

    // Audit: emit one AdminOperation record for successfully-deleted ACLs.
    // Collect resource_name from each matching ACL in every filter result that
    // committed without error (error_code == 0).
    audit_deleted_acls(
        broker.audit_log.as_ref(),
        ctx,
        deleted_acl_resources(&filter_results),
    );

    Ok(delete_acls_response(filter_results))
}
