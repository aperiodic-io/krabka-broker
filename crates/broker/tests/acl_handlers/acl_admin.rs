//! Request builders and drivers for the admin APIs a super-user drives in
//! this suite: `CreateTopics`, which materialises the partitions the
//! enforcement tests then read and write, and the `CreateAcls` /
//! `DescribeAcls` / `DeleteAcls` trio.
//!
//! Same shape as `drive_alter_user_scram_credentials_as_plain`: one
//! `ApiVersions` warm-up, one `SaslHandshake`, one `SaslAuthenticate`, then
//! the typed request. Each helper authenticates fresh on a new TCP stream
//! because that is the simplest model for "a client doing one admin
//! action"; reuse is unnecessary for these tests.

use {
    crate::{
        CREATE_ACLS_VERSION, DELETE_ACLS_VERSION, DESCRIBE_ACLS_VERSION, OPERATION_ANY,
        PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PERMISSION_ALLOW, PERMISSION_ANY,
        RESOURCE_TYPE_TOPIC,
        framing::CLIENT_ID,
        kafka_wire::create_topic_as_super_user,
        support::topics::{ConfiguredTopicSetup, TopicPartitionCount},
    },
    krabka_protocol::owned::{
        create_acls_request::{AclCreation, CreateAclsRequest},
        create_acls_response::CreateAclsResponse,
        delete_acls_request::DeleteAclsRequest,
        delete_acls_response::DeleteAclsResponse,
        describe_acls_request::DescribeAclsRequest,
        describe_acls_response::DescribeAclsResponse,
    },
    std::net::SocketAddr,
};

/// Shorthand for `Allow <op> on Topic LITERAL <name> for <principal> from *`.
/// Every test in this file uses literal Topic ACLs with host `*`, so the only
/// dimensions that vary per binding are `resource_name`, `principal`, and
/// `operation`. This helper wraps them and keeps the test bodies short.
pub fn topic_allow_creation(name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: name.to_string(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

/// Permissive `DescribeAclsRequest` for `Topic`. Every other axis is wildcard.
pub fn describe_all_topic_acls() -> DescribeAclsRequest {
    DescribeAclsRequest {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: None,
        pattern_type_filter: PATTERN_TYPE_ANY,
        principal_filter: None,
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

/// Drive a single `CreateTopics` against `addr` authenticated as
/// `admin` / `admin-secret`. Asserts the response has `error_code=0`
/// for the requested topic. The T23 tests use it to materialise a
/// partition before they produce to it or fetch from it.
pub async fn create_topic_as_admin(addr: SocketAddr, name: &str, partitions: i32) {
    create_topic_as_super_user()
        .addr(addr)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: name.to_string(),
            partitions: TopicPartitionCount(partitions),
            ..Default::default()
        })
        .call()
        .await;
}

krabka_macros::wire_test_driver!(plain pub drive_create_acls_as_plain {
    request: CreateAclsRequest,
    response: CreateAclsResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(30),
        version: krabka_ids::ApiVersion(CREATE_ACLS_VERSION),
    },
    operation: "CreateAcls",
});

krabka_macros::wire_test_driver!(plain pub drive_describe_acls_as_plain {
    request: DescribeAclsRequest,
    response: DescribeAclsResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(29),
        version: krabka_ids::ApiVersion(DESCRIBE_ACLS_VERSION),
    },
    operation: "DescribeAcls",
});

krabka_macros::wire_test_driver!(plain pub drive_delete_acls_as_plain {
    request: DeleteAclsRequest,
    response: DeleteAclsResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(31),
        version: krabka_ids::ApiVersion(DELETE_ACLS_VERSION),
    },
    operation: "DeleteAcls",
});
