//! The `DeleteAcls` response rows: the matching-ACL echo Kafka sends back for
//! every entry a filter removed, the per-filter result that carries them, the
//! envelope, the bulk stamp a failed controller submit leaves behind.
//!
//! `kafka-acls --remove` prints the matching-ACL list verbatim, so the wire
//! bytes here are what the operator reads. Keeping the row constructors
//! together makes the one-row-per-filter invariant easy to see.

use {
    crate::{
        codes,
        handlers::{
            acl_wire::{
                operation_to_wire, pattern_type_to_wire, permission_to_wire, resource_type_to_wire,
            },
            stamp_unset,
        },
    },
    krabka_metadata::AclEntry,
    krabka_protocol::owned::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
    },
};

/// The echo of one ACL a filter removed.
///
/// Kafka's `DeleteAclsResponse.matchingAcl` writes `ApiError.NONE`'s code and
/// message for a successful deletion, and that message is null.
pub(super) fn matching_acl_result(e: &AclEntry) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        error_code: codes::NONE,
        error_message: None,
        resource_type: resource_type_to_wire(e.resource_type),
        resource_name: e.resource_name.clone(),
        pattern_type: pattern_type_to_wire(e.pattern_type),
        principal: e.principal.clone(),
        host: e.host.clone(),
        operation: operation_to_wire(e.operation),
        permission_type: permission_to_wire(e.permission_type),
        ..Default::default()
    }
}

pub(super) fn filter_result(
    error_code: i16,
    error_message: Option<String>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> DeleteAclsFilterResult {
    DeleteAclsFilterResult {
        error_code,
        error_message,
        matching_acls,
        ..Default::default()
    }
}

pub(super) fn delete_acls_response(
    filter_results: Vec<DeleteAclsFilterResult>,
) -> DeleteAclsResponse {
    DeleteAclsResponse {
        filter_results,
        ..Default::default()
    }
}

pub(super) fn apply_submit_error<E: std::fmt::Display>(
    filter_results: &mut [DeleteAclsFilterResult],
    err: E,
) {
    stamp_unset(
        filter_results,
        codes::COORDINATOR_NOT_AVAILABLE,
        &format!("submit failed: {err}"),
    );
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::{
            acl_test_support::expected_deleted_orders_acl,
            delete_acls::test_support::{OPERATION_WRITE, PATTERN_TYPE_PREFIXED, acl},
        },
        assert2::assert,
        krabka_metadata::{AclOperation, PatternType},
    };

    #[test]
    fn helpers_preserve_matching_acl_and_submit_error_fields() {
        let matched = matching_acl_result(&acl().call());
        let expected_matched = expected_deleted_orders_acl();
        assert!(matched == expected_matched);

        let mut prefixed_acl = acl()
            .resource_name("orders-")
            .principal("User:bob")
            .operation(AclOperation::Write)
            .call();
        prefixed_acl.pattern_type = PatternType::Prefixed;
        let matched = matching_acl_result(&prefixed_acl);
        assert!(matched.pattern_type == PATTERN_TYPE_PREFIXED);
        assert!(matched.operation == OPERATION_WRITE);

        let mut results = vec![
            filter_result(codes::NONE, None, vec![matched.clone()]),
            filter_result(
                codes::INVALID_REQUEST,
                Some("Unknown entryFilter.".into()),
                Vec::new(),
            ),
        ];
        apply_submit_error(&mut results, "not controller");

        let expected_results = vec![
            tagged_wire!(DeleteAclsFilterResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                matching_acls: vec![matched],
            }),
            tagged_wire!(DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Unknown entryFilter.".into()),
                matching_acls: Vec::new(),
            }),
        ];
        assert!(results == expected_results);
    }
}
