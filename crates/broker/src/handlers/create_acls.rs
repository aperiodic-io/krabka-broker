//! `CreateAcls` handler (`api_key` 30).
//!
//! This handler authorizes `Alter` on `Cluster`. For each binding, it
//! validates the resource shape and submits a `V1AccessControlEntry` to the
//! controller. It returns one result per binding.
//!
//! On a cluster with no authorizer configured every binding is refused with
//! `SECURITY_DISABLED`, so nothing is stored that no decision point would
//! read.
//!
//! This file keeps the whole-request cluster gate and the per-binding loop.
//! Binding validation lives in `validate`, the result rows in `response`, and
//! the audit trail in `audit`.

use {
    krabka_metadata::{
        AclEntry, AclOperation, MetadataImage, MetadataRecord, PatternType, PermissionType,
        ResourceType,
    },
    krabka_protocol::owned::{
        create_acls_request::CreateAclsRequest,
        create_acls_response::{AclCreationResult, CreateAclsResponse},
    },
    std::collections::HashSet,
};

mod audit;
mod response;
mod validate;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        audit::{audit_created_acls, created_acl_resources},
        response::{
            acl_error_result, acl_success_result, apply_submit_error, create_acls_response,
        },
        validate::{HostCheck, has_filter_only_element, has_unknown_element, validate},
    },
    super::acl_wire::{MAX_ACL_RECORDS_PER_REQUEST, NO_AUTHORIZER_EXCEPTION_MESSAGE},
    crate::codes,
};

/// The message Kafka's controller gives the `PolicyViolationException` that
/// `EventHandlerExceptionInfo` makes of a `BoundedListTooLongException`.
const EXCESSIVE_BATCH_MESSAGE: &str = "Unable to perform excessively large batch operation.";

/// The seven fields that make one ACL, as a value a hash set can hold. The
/// image's [`AclEntry`] is not `Hash`, and a set is what keeps a request of
/// tens of thousands of bindings out of quadratic time.
type AclKey<'a> = (
    ResourceType,
    &'a str,
    PatternType,
    &'a str,
    &'a str,
    AclOperation,
    PermissionType,
);

fn acl_key(entry: &AclEntry) -> AclKey<'_> {
    (
        entry.resource_type,
        &entry.resource_name,
        entry.pattern_type,
        &entry.principal,
        &entry.host,
        entry.operation,
        entry.permission_type,
    )
}

/// How many distinct ACLs of `to_submit` the image does not hold yet, counted
/// only up to one past the bound the caller compares it with.
///
/// Kafka's `AclControlManager.createAcls` tests each binding against a hash set
/// of the existing ACLs and another of the ones the request has added, so this
/// does too: a request of 10 000 bindings that are mostly duplicates is a few
/// hash probes each, not a scan of the request and the image for every one.
fn count_new_acls(image: &MetadataImage, to_submit: &[(usize, MetadataRecord)]) -> usize {
    let existing: HashSet<AclKey<'_>> = image.all_acls().map(acl_key).collect();
    let mut new = HashSet::new();
    for (_, record) in to_submit {
        let MetadataRecord::V1AccessControlEntry(entry) = record else {
            continue;
        };
        let key = acl_key(entry);
        if !existing.contains(&key) && new.insert(key) && new.len() > MAX_ACL_RECORDS_PER_REQUEST {
            break;
        }
    }
    new.len()
}

context_handler! {
    CreateAclsRequest => CreateAclsResponse,
    (broker, req, _version, ctx),
    {
        // Kafka's `CreateAclsRequest.validate` refuses a wire `UNKNOWN` element
        // while it parses the request, before authorization, and the socket
        // server closes the connection with no response. A handler error closes
        // the connection the same way.
        if has_unknown_element(&req.creations) {
            return Err(crate::error::BrokerError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue("CreatableAcls contain unknown elements"),
            ));
        }

        let image = broker.controller.current_image();

        // Whole-request cluster-alter gate.
        if crate::handlers::cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            let results = req
                .creations
                .iter()
                .map(|_| acl_error_result(codes::CLUSTER_AUTHORIZATION_FAILED, "create-acls denied"))
                .collect();
            return Ok(create_acls_response(results));
        }

        // No authorizer: refuse every creation rather than durably storing
        // bindings that no decision point will ever read. Kafka builds this
        // response from `CreateAclsRequest.getErrorResponse` with a
        // `SecurityDisabledException`, which stamps the same code and message on
        // one result per creation.
        if !broker.config.authorizer.is_configured() {
            let results = req
                .creations
                .iter()
                .map(|_| acl_error_result(codes::SECURITY_DISABLED, NO_AUTHORIZER_EXCEPTION_MESSAGE))
                .collect();
            return Ok(create_acls_response(results));
        }

        // An `ANY` or `MATCH` element fails Kafka's binding construction for the
        // whole request; see `has_filter_only_element`.
        if has_filter_only_element(&req.creations) {
            let results = req
                .creations
                .iter()
                .map(|_| AclCreationResult {
                    error_code: codes::UNKNOWN_SERVER_ERROR,
                    error_message: None,
                    ..Default::default()
                })
                .collect();
            return Ok(create_acls_response(results));
        }

        // Kafka 4.3.1 has no host check: any host is stored as text. Trunk's
        // `validateHostPattern` (empty host, KIP-1276 CIDR ranges) applies only
        // under `unstable.feature.versions.enable`.
        //
        // KIP-1276: whether a `/`-bearing host may be a CIDR range, computed once
        // per request rather than per binding, the way
        // `AclControlManager.createAcls` resolves `metadataVersion.isCidrAclSupported()`
        // a single time and passes it into `validateNewAcl` for every creation.
        // `require_feature` is deliberately not used here: it passes any
        // unfinalized metadata.version, whereas `cidr_hosts_supported` compares
        // the bootstrap level, 4.3-IV0, against the CIDR floor.
        let host_check = if broker.config.features.unstable_feature_versions
            == krabka_raft::UnstableFeatureVersions::Enabled
        {
            HostCheck::Trunk {
                cidr_hosts_supported: crate::features::cidr_hosts_supported(&image),
            }
        } else {
            HostCheck::Unchecked
        };

        let mut results: Vec<AclCreationResult> = Vec::with_capacity(req.creations.len());
        let mut to_submit: Vec<(usize, MetadataRecord)> = Vec::with_capacity(req.creations.len());

        for c in &req.creations {
            match validate(c, host_check) {
                Ok(entry) => {
                    let idx = results.len();
                    results.push(acl_success_result());
                    to_submit.push((idx, MetadataRecord::V1AccessControlEntry(entry)));
                }
                Err((code, msg)) => {
                    results.push(acl_error_result(code, msg));
                }
            }
        }

        // `AclControlManager.createAcls` collects the records of the ACLs that are
        // new into a `BoundedList` of 10,000. It does not catch the overflow, so
        // the controller answers every binding, valid or not, with
        // `PolicyViolationException`.
        if to_submit.len() > MAX_ACL_RECORDS_PER_REQUEST
            && count_new_acls(&image, &to_submit) > MAX_ACL_RECORDS_PER_REQUEST
        {
            let results = req
                .creations
                .iter()
                .map(|_| acl_error_result(codes::POLICY_VIOLATION, EXCESSIVE_BATCH_MESSAGE))
                .collect();
            return Ok(create_acls_response(results));
        }

        if !to_submit.is_empty() {
            let records: Vec<MetadataRecord> = to_submit.iter().map(|(_, r)| r.clone()).collect();
            if let Err(e) = broker.controller.submit_change(records).await {
                tracing::warn!(error = %e, "create-acls submit failed");
                apply_submit_error(&mut results, &to_submit, &e);
            }
        }

        // Audit: emit one AdminOperation record for successfully-created ACLs.
        // `to_submit` carries (result_idx, record) for every creation that passed
        // validation; entries whose result slot still has error_code == 0 were committed.
        audit_created_acls(
            broker.audit_log.as_ref(),
            ctx,
            created_acl_resources(&req, &results, &to_submit),
        );

        Ok(create_acls_response(results))
    }
}
