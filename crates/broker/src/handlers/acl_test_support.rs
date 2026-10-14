//! Shared independent ACL wire models and live-handler metadata fixtures.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker;
use {
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer, SimpleAclAuthorizer},
        broker::{Broker, BrokerHandle},
        handlers::acl_wire::binding_filter::AclBindingFilter,
        test_support::{allow_acl, start_broker_with_authorizer_no_audit},
    },
    krabka_metadata::{AclEntry, MetadataRecord},
    std::{future::Future, sync::Arc},
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

macro_rules! acl_request_filter {
    ($request:ident, $resource_name:expr, $principal:expr, $operation:expr) => {
        $request {
            resource_type_filter: $crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC,
            resource_name_filter: $resource_name.map(Into::into),
            pattern_type_filter: $crate::handlers::acl_test_support::PATTERN_TYPE_LITERAL,
            principal_filter: $principal.map(Into::into),
            host_filter: Some("*".into()),
            operation: $operation,
            permission_type: $crate::handlers::acl_test_support::PERMISSION_ALLOW,
            ..Default::default()
        }
    };
}

pub(super) use acl_request_filter;

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

/// Evaluate ACL entries after startup, then expose the retained broker resources.
#[bon::builder]
pub(super) async fn start_seeded_acls(
    start: impl Future<Output = StartedBroker>,
    entries: impl FnOnce() -> Vec<AclEntry>,
) -> StartedBroker {
    let started = start.await;
    seed_acls(&started.broker, entries()).await;
    started
}

/// The configured ACL authorizer, with entries evaluated after broker startup.
#[bon::builder]
pub(super) fn start_configured_acls(
    entries: impl FnOnce() -> Vec<AclEntry>,
) -> impl Future<Output = StartedBroker> {
    let start = start_broker_with_authorizer_no_audit(configured_authorizer());
    start_seeded_acls().start(start).entries(entries).call()
}

pub(super) fn start_orders_acl() -> impl Future<Output = StartedBroker> {
    start_configured_acls()
        .entries(|| vec![alice_orders_acl()])
        .call()
}
pub(super) fn start_orders_payments_acls() -> impl Future<Output = StartedBroker> {
    start_configured_acls().entries(orders_payments_acls).call()
}
pub(super) async fn start_security_disabled_acls() -> StartedBroker {
    start_seeded_acls()
        .start(start_broker_with_authorizer_no_audit(Arc::new(
            AllowAllAuthorizer,
        )))
        .entries(|| vec![alice_orders_acl()])
        .call()
        .await
}

/// Commit the nonempty ACL entries selected by a caller's case.
pub(crate) async fn seed_case_acls(broker: &Broker, entries: Vec<AclEntry>) {
    broker
        .controller
        .submit_change(
            entries
                .into_iter()
                .map(MetadataRecord::V1AccessControlEntry)
                .collect(),
        )
        .await
        .expect("seed acls");
}

pub(super) fn all_acls(handle: &BrokerHandle) -> Vec<AclEntry> {
    handle
        .controller_image_for_test()
        .all_acls()
        .cloned()
        .collect()
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
