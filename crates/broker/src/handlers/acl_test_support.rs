//! Shared independent ACL wire models and live-handler metadata fixtures.

use {
    crate::{
        authorizer::{Authorizer, SimpleAclAuthorizer},
        broker::BrokerHandle,
        handlers::acl_wire::binding_filter::AclBindingFilter,
        test_support::allow_acl,
    },
    krabka_metadata::{AclEntry, MetadataRecord},
    std::sync::Arc,
};

pub(super) const RESOURCE_TYPE_TOPIC: i8 = 2;
pub(super) const PATTERN_TYPE_ANY: i8 = 1;
pub(super) const PATTERN_TYPE_MATCH: i8 = 2;
pub(super) const PATTERN_TYPE_LITERAL: i8 = 3;
pub(super) const PATTERN_TYPE_PREFIXED: i8 = 4;
pub(super) const OPERATION_ANY: i8 = 1;
pub(super) const OPERATION_READ: i8 = 3;
pub(super) const OPERATION_WRITE: i8 = 4;
pub(super) const PERMISSION_ANY: i8 = 1;
pub(super) const PERMISSION_ALLOW: i8 = 3;

/// An explicitly configured authorizer with `admin` as its only super user.
pub(super) fn configured_authorizer() -> Arc<dyn Authorizer> {
    Arc::new(SimpleAclAuthorizer::new(
        std::iter::once("admin".to_owned()).collect(),
    ))
}

/// The matching orders ACL shared by live ACL handler fixtures.
pub(super) fn alice_orders_acl() -> AclEntry { allow_acl().call() }

/// Independent wire oracle for a successful deletion of the standard orders ACL.
pub(super) fn expected_deleted_orders_acl()
-> krabka_protocol::owned::delete_acls_response::DeleteAclsMatchingAcl {
    krabka_macros::tagged_wire!(
        krabka_protocol::owned::delete_acls_response::DeleteAclsMatchingAcl {
            error_code: crate::codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
        }
    )
}

/// Retain the broker and directory while the unconfigured authorizer's ACL is seeded.
macro_rules! security_disabled_acl_fixture {
    ($bindings:tt) => {
        seeded_acl_fixture!(
            $bindings,
            crate::test_support::start_broker_with_authorizer_no_audit(std::sync::Arc::new(
                crate::authorizer::AllowAllAuthorizer
            )),
            vec![crate::handlers::acl_test_support::alice_orders_acl()],
            "admin"
        );
    };
}

/// Two independent topic ACLs for matching and deletion fixtures.
pub(super) fn orders_payments_acls() -> Vec<AclEntry> {
    vec![
        alice_orders_acl(),
        crate::test_support::allow_acl()
            .resource_name("payments")
            .principal("User:bob")
            .operation(krabka_metadata::AclOperation::Write)
            .call(),
    ]
}

pub(super) async fn seed_acls(handle: &BrokerHandle, entries: Vec<AclEntry>) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(
            entries
                .into_iter()
                .map(MetadataRecord::V1AccessControlEntry)
                .collect(),
        )
        .await
        .expect("seed ACLs");
}

pub(super) fn all_acls(handle: &BrokerHandle) -> Vec<AclEntry> {
    handle
        .controller_image_for_test()
        .all_acls()
        .cloned()
        .collect()
}

/// The empty-string filter used to verify that emptiness never means wildcard.
macro_rules! empty_match_acl_filter {
    ($ty:ident) => {
        $ty {
            resource_type_filter: crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: crate::handlers::acl_test_support::PATTERN_TYPE_MATCH,
            principal_filter: Some(String::new()),
            host_filter: None,
            operation: crate::handlers::acl_test_support::OPERATION_ANY,
            permission_type: crate::handlers::acl_test_support::PERMISSION_ANY,
            ..Default::default()
        }
    };
}

pub(super) fn expected_empty_match_filter() -> AclBindingFilter {
    use crate::handlers::acl_wire::binding_filter::{
        AclBindingFilter, AxisFilter, PatternTypeFilter,
    };
    AclBindingFilter {
        resource_type: AxisFilter::Exact(krabka_metadata::ResourceType::Topic),
        resource_name: Some(String::new()),
        pattern_type: PatternTypeFilter::Match,
        principal: Some(String::new()),
        host: None,
        operation: AxisFilter::Any,
        permission_type: AxisFilter::Any,
    }
}

/// Keep seeded ACLs visible before acquiring the handler's broker and context.
macro_rules! seeded_acl_fixture {
    (
        ($handle:ident, $directory:ident, $broker:ident, $context:ident),
        $start:expr,
        $entries:expr,
        $user:expr
    ) => {
        let ($handle, $directory) = $start.await;
        crate::handlers::acl_test_support::seed_acls(&$handle, $entries).await;
        let $broker = $handle.broker_arc_for_test();
        test_ctx!($context, $user);
    };
}

/// Seed a table case's ACLs without submitting or consuming an empty case.
macro_rules! seed_case_acls {
    ($broker:expr, $entries:expr) => {
        if !$entries.is_empty() {
            $broker
                .controller
                .submit_change(
                    $entries
                        .into_iter()
                        .map(krabka_metadata::MetadataRecord::V1AccessControlEntry)
                        .collect(),
                )
                .await
                .expect("seed acls");
        }
    };
}
pub(crate) use seed_case_acls;
