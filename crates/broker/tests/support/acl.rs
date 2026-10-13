//! ACL fixtures whose commit/application settling behavior matches quota suites.

use {
    krabka_broker::BrokerHandle,
    krabka_metadata::{
        AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    },
};

/// Install the simple authorizer using exactly the broker's current super-user set.
pub fn use_simple_acl_authorizer(config: &mut krabka_broker::BrokerConfig) {
    config.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        config.super_users.clone(),
    ));
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TopicAclSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    #[default("User:alice")]
    pub principal: &'a str,
    #[default(AclOperation::Read)]
    pub operation: AclOperation,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ClusterAclSetup<'a> {
    #[default("User:alice")]
    pub principal: &'a str,
    #[default("*")]
    pub host: &'a str,
    #[default(AclOperation::ClusterAction)]
    pub operation: AclOperation,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct AllowAclSetup<'a> {
    #[default(ResourceType::Topic)]
    pub resource_type: ResourceType,
    #[default("orders")]
    pub resource_name: &'a str,
    #[default("User:alice")]
    pub principal: &'a str,
    #[default("*")]
    pub host: &'a str,
    #[default(AclOperation::Read)]
    pub operation: AclOperation,
}

pub async fn seed_topic_acl(broker: &BrokerHandle, setup: TopicAclSetup<'_>) {
    broker
        .submit_metadata_record_for_test(topic_acl_record(setup))
        .await
        .expect("seed topic ACL");
    // Preserve the fixtures' commit/application settle before their RPC retry checks.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

/// A literal Allow ACL on one topic and principal, without committing or waiting.
pub fn topic_acl_record(setup: TopicAclSetup<'_>) -> MetadataRecord {
    literal_allow_acl(AllowAclSetup {
        resource_name: setup.topic,
        principal: setup.principal,
        operation: setup.operation,
        ..Default::default()
    })
}

/// A literal cluster Allow ACL, including the host the original request must match.
pub fn cluster_acl_record(setup: ClusterAclSetup<'_>) -> MetadataRecord {
    literal_allow_acl(AllowAclSetup {
        resource_type: ResourceType::Cluster,
        resource_name: "kafka-cluster",
        principal: setup.principal,
        host: setup.host,
        operation: setup.operation,
    })
}

fn literal_allow_acl(setup: AllowAclSetup<'_>) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: setup.resource_type,
        resource_name: setup.resource_name.into(),
        pattern_type: PatternType::Literal,
        principal: setup.principal.into(),
        host: setup.host.into(),
        operation: setup.operation,
        permission_type: PermissionType::Allow,
    })
}

// Literal expected Kafka bit positions, independent of production ACL helpers.
pub const BIT_READ: i32 = 1 << 3;
pub const BIT_WRITE: i32 = 1 << 4;
pub const BIT_CREATE: i32 = 1 << 5;
pub const BIT_DELETE: i32 = 1 << 6;
pub const BIT_ALTER: i32 = 1 << 7;
pub const BIT_DESCRIBE: i32 = 1 << 8;
pub const BIT_CLUSTER_ACTION: i32 = 1 << 9;
pub const BIT_DESCRIBE_CONFIGS: i32 = 1 << 10;
pub const BIT_ALTER_CONFIGS: i32 = 1 << 11;
pub const BIT_IDEMPOTENT_WRITE: i32 = 1 << 12;

pub const TOPIC_FULL_MASK: i32 = BIT_READ
    | BIT_WRITE
    | BIT_CREATE
    | BIT_DELETE
    | BIT_ALTER
    | BIT_DESCRIBE
    | BIT_DESCRIBE_CONFIGS
    | BIT_ALTER_CONFIGS;

/// A literal `Allow` binding on any resource, with the original wildcard host.
pub fn resource_allow_acl(setup: AllowAclSetup<'_>) -> MetadataRecord { literal_allow_acl(setup) }

pub async fn seed_compat_shim_disable_acl(handle: &BrokerHandle) {
    seed_topic_acl(handle, TopicAclSetup {
        topic: "__compat_shim_disable__",
        principal: "User:admin",
        ..Default::default()
    })
    .await;
}

pub async fn seed_alice_write_acl(handle: &BrokerHandle, topic: &str) {
    seed_topic_acl(handle, TopicAclSetup {
        topic,
        operation: AclOperation::Write,
        ..Default::default()
    })
    .await;
}

/// Populate an irrelevant binding so the permissive empty-image shim is disabled.
pub async fn seed_dummy_acl(handle: &krabka_broker::BrokerHandle) {
    handle
        .submit_metadata_record_for_test(topic_acl_record(TopicAclSetup {
            topic: "_nothing",
            principal: "User:admin",
            operation: krabka_metadata::AclOperation::Read,
        }))
        .await
        .expect("seed dummy ACL");
}
