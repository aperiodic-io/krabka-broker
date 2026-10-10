//! Fixture builders shared by the `SimpleAclAuthorizer` unit tests.
//!
//! The principals, hosts, metadata images, ACL entries, and requests below are
//! needed both by the decision-order tests in [`super::tests`] and by the
//! entry-matching tests in [`super::matching`], so they live in one module
//! instead of being duplicated in each.

use std::{collections::HashSet, net::SocketAddr};

use krabka_metadata::{
    AclEntry, AclOperation, FeatureLevelRecord, MetadataImage, MetadataRecord, ResourceType,
    metadata_version::{CIDR_ACL_MIN_LEVEL, METADATA_VERSION_FEATURE},
};
use krabka_security::Principal;
use uuid::Uuid;

use super::SimpleAclAuthorizer;
use crate::{AuthorizationRequest, AuthorizationResult, Authorizer};

/// The default test caller, retaining one authorizer across all decisions in a case.
pub(super) struct AliceAuthorizer {
    pub principal: Principal,
    pub host: SocketAddr,
    pub authorizer: SimpleAclAuthorizer,
}

impl Default for AliceAuthorizer {
    fn default() -> Self {
        Self {
            principal: alice(),
            host: addr(),
            authorizer: SimpleAclAuthorizer::new(no_super()),
        }
    }
}

impl AliceAuthorizer {
    pub fn authorize(
        &self,
        image: &MetadataImage,
        name: &str,
        operation: AclOperation,
    ) -> AuthorizationResult {
        self.authorizer.authorize(
            image,
            &req(
                &self.principal,
                &self.host,
                QuerySetup {
                    name,
                    op: operation,
                    ..Default::default()
                },
            ),
        )
    }

    pub fn authorize_by_resource_type(
        &self,
        image: &MetadataImage,
        resource_type: ResourceType,
        operation: AclOperation,
    ) -> AuthorizationResult {
        self.authorizer.authorize_by_resource_type(
            image,
            &self.principal,
            &self.host,
            resource_type,
            operation,
        )
    }

    pub fn check(
        &self,
        image: &MetadataImage,
        name: &str,
        operation: AclOperation,
        expected: AuthorizationResult,
    ) {
        assert2::assert!(self.authorize(image, name, operation) == expected);
    }

    pub fn check_resource_type(
        &self,
        image: &MetadataImage,
        resource_type: ResourceType,
        operation: AclOperation,
        expected: AuthorizationResult,
    ) {
        assert2::assert!(
            self.authorize_by_resource_type(image, resource_type, operation) == expected
        );
    }

    pub fn authorize_on(
        &self,
        image: &MetadataImage,
        resource_type: ResourceType,
        name: &str,
        operation: AclOperation,
    ) -> AuthorizationResult {
        self.authorizer.authorize(
            image,
            &req(
                &self.principal,
                &self.host,
                QuerySetup {
                    rt: resource_type,
                    name,
                    op: operation,
                },
            ),
        )
    }
}

/// Check one decision with the default principal, host, and authorizer.
pub(super) fn check_topic_access(
    image: &MetadataImage,
    name: &str,
    operation: AclOperation,
    expected: AuthorizationResult,
) {
    AliceAuthorizer::default().check(image, name, operation, expected);
}

pub(super) fn check_resource_type_access(
    image: &MetadataImage,
    resource_type: ResourceType,
    operation: AclOperation,
    expected: AuthorizationResult,
) {
    AliceAuthorizer::default().check_resource_type(image, resource_type, operation, expected);
}

/// Build the default metadata image with the supplied ACLs in their original order.
pub(super) fn acl_image(entries: impl IntoIterator<Item = AclEntry>) -> MetadataImage {
    image_with_acls(img(), entries)
}

pub(super) fn image_with_acls(
    mut image: MetadataImage,
    entries: impl IntoIterator<Item = AclEntry>,
) -> MetadataImage {
    for entry in entries {
        image.apply(&MetadataRecord::V1AccessControlEntry(entry));
    }
    image
}

pub(super) fn no_super() -> HashSet<String> {
    HashSet::new()
}
pub(super) fn one_super(name: &str) -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(name.to_string());
    s
}

pub(super) fn alice() -> Principal {
    Principal {
        name: "alice".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    }
}

pub(super) fn addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

pub(super) fn img() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

/// An image whose `metadata.version` is high enough (4.4-IV1) for a host
/// containing `/` to be a CIDR range (KIP-1276).
pub(super) fn cidr_img() -> MetadataImage {
    let mut image = img();
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: METADATA_VERSION_FEATURE.into(),
        level: CIDR_ACL_MIN_LEVEL,
    }));
    image
}

pub(super) use crate::test_support::{AclSetup, acl as topic_acl};

#[derive(Clone, Copy)]
pub(super) struct QuerySetup<'a> {
    pub rt: ResourceType,
    pub name: &'a str,
    pub op: AclOperation,
}

impl Default for QuerySetup<'_> {
    fn default() -> Self {
        Self {
            rt: ResourceType::Topic,
            name: "foo",
            op: AclOperation::Read,
        }
    }
}

pub(super) fn req<'a>(
    p: &'a Principal,
    host: &'a SocketAddr,
    setup: QuerySetup<'a>,
) -> AuthorizationRequest<'a> {
    let QuerySetup { rt, name, op } = setup;
    AuthorizationRequest {
        principal: p,
        host,
        resource_type: rt,
        resource_name: name,
        operation: op,
    }
}
