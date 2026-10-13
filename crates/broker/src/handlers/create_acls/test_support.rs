//! Fixtures shared by the `create_acls` test modules.
//!
//! The wire constants, the `AclCreation` and `CreateAclsRequest` builders, the
//! one-argument `validate` shim that allows CIDR hosts, and the context helper
//! are used from more than one of the sibling test modules, so they live here
//! rather than being repeated in each.

pub(super) use crate::handlers::acl_test_support::{
    OPERATION_READ, OPERATION_WRITE, PATTERN_TYPE_LITERAL, PERMISSION_ALLOW, RESOURCE_TYPE_TOPIC,
};
use {
    super::validate::AclRefusal,
    krabka_metadata::AclEntry,
    krabka_protocol::owned::create_acls_request::{AclCreation, CreateAclsRequest},
};
pub(super) const VERSION: i16 = 3;

pub(super) fn creation(resource_name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: resource_name.into(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

pub(super) fn request(creations: Vec<AclCreation>) -> CreateAclsRequest {
    CreateAclsRequest {
        creations,
        ..Default::default()
    }
}

krabka_macros::wire_codec_helpers! {
    pub(super) test_context ["admin-client"];
}

pub(super) use crate::handlers::acl_test_support::all_acls;

/// Validates `c` with CIDR ACL hosts supported, which is what every test not
/// about the CIDR gate wants.
pub(super) fn validate(c: &AclCreation) -> Result<AclEntry, AclRefusal> {
    super::validate::validate(c, super::validate::HostCheck::Trunk {
        cidr_hosts_supported: true,
    })
}

pub(super) use crate::handlers::acl_test_support::configured_authorizer;
