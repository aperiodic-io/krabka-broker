//! Independent ACL data shared by the image, cache and authorizer tests.

use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};

#[derive(Clone, Copy)]
pub(crate) struct AclSetup<'a> {
    pub rt: ResourceType,
    pub permission: PermissionType,
    pub op: AclOperation,
    pub principal: &'a str,
    pub host: &'a str,
    pub pattern: PatternType,
    pub name: &'a str,
}

impl Default for AclSetup<'_> {
    fn default() -> Self {
        Self {
            rt: ResourceType::Topic,
            permission: PermissionType::Allow,
            op: AclOperation::Read,
            principal: "User:alice",
            host: "*",
            pattern: PatternType::Literal,
            name: "foo",
        }
    }
}

#[bon::builder]
pub(crate) fn acl(
    #[builder(default = AclSetup::default().rt)] rt: ResourceType,
    #[builder(default = AclSetup::default().permission)] permission: PermissionType,
    #[builder(default = AclSetup::default().op)] op: AclOperation,
    #[builder(default = AclSetup::default().principal)] principal: &'_ str,
    #[builder(default = AclSetup::default().host)] host: &'_ str,
    #[builder(default = AclSetup::default().pattern)] pattern: PatternType,
    #[builder(default = AclSetup::default().name)] name: &'_ str,
) -> AclEntry {
    let setup = AclSetup {
        rt,
        permission,
        op,
        principal,
        host,
        pattern,
        name,
    };
    let AclSetup {
        rt,
        permission,
        op,
        principal,
        host,
        pattern,
        name,
    } = setup;
    AclEntry {
        resource_type: rt,
        resource_name: name.into(),
        pattern_type: pattern,
        principal: principal.into(),
        host: host.into(),
        operation: op,
        permission_type: permission,
    }
}
