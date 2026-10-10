//! Fixtures shared by the `delete_acls` test modules.
//!
//! The wire constants, the `AclEntry` and `DeleteAclsFilter` builders, the
//! request envelope, and the context helper are used from more
//! than one of the sibling test modules, so they live here rather than being
//! repeated in each.

use krabka_protocol::owned::delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest};

pub(super) use crate::handlers::acl_test_support::{
    OPERATION_ANY, OPERATION_READ, OPERATION_WRITE, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL,
    PATTERN_TYPE_MATCH, PATTERN_TYPE_PREFIXED, PERMISSION_ALLOW, PERMISSION_ANY,
    RESOURCE_TYPE_TOPIC,
};
pub(super) const VERSION: i16 = 3;

pub(super) use crate::test_support::allow_acl as acl;

pub(super) fn filter(resource_name: Option<&str>, principal: Option<&str>) -> DeleteAclsFilter {
    DeleteAclsFilter {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: resource_name.map(Into::into),
        pattern_type_filter: PATTERN_TYPE_LITERAL,
        principal_filter: principal.map(Into::into),
        host_filter: Some("*".into()),
        operation: OPERATION_READ,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

pub(super) fn request(filters: Vec<DeleteAclsFilter>) -> DeleteAclsRequest {
    DeleteAclsRequest {
        filters,
        ..Default::default()
    }
}

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

pub(super) use crate::handlers::acl_test_support::configured_authorizer;

/// Build ordered name/principal filters without sharing any expected response facts.
pub(super) fn named_filters(filters: &[(&str, &str)]) -> DeleteAclsRequest {
    request(
        filters
            .iter()
            .map(|&(name, principal)| filter(Some(name), Some(principal)))
            .collect(),
    )
}
