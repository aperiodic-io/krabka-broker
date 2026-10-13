//! Discovery requests with explicit client identity and lookup targets.

use krabka_protocol::owned::{
    api_versions_request::ApiVersionsRequest,
    find_coordinator_request::FindCoordinatorRequest,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
};

pub fn api_versions_request_for(
    client_software_name: impl Into<String>,
    client_software_version: impl Into<String>,
) -> ApiVersionsRequest {
    ApiVersionsRequest {
        client_software_name: client_software_name.into(),
        client_software_version: client_software_version.into(),
        ..Default::default()
    }
}

/// Send the standard client-identifying `ApiVersions` request.
pub async fn api_versions(
    client: &krabka_client_core::Client,
) -> krabka_protocol::owned::api_versions_response::ApiVersionsResponse {
    client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions")
}

pub fn coordinator_lookup_request(
    key: impl Into<String>,
    key_type: i8,
    coordinator_keys: Vec<String>,
) -> FindCoordinatorRequest {
    FindCoordinatorRequest {
        key: key.into(),
        key_type,
        coordinator_keys,
        ..Default::default()
    }
}

pub fn topic_metadata_request(topics: Option<Vec<MetadataRequestTopic>>) -> MetadataRequest {
    MetadataRequest {
        topics,
        ..Default::default()
    }
}

pub fn named_topic_metadata(topic: impl Into<String>) -> MetadataRequest {
    topic_metadata_request(Some(vec![crate::support::topics::metadata_topic(
        Some(topic.into()),
        krabka_protocol::primitives::uuid::Uuid::default(),
    )]))
}

/// Compare the complete coordinator row or response against independent call-site expectations.
/// Every ordinary field is explicit; tagged fields retain the original defaults.
pub fn expected_coordinator(
    key: &str,
    node_id: i32,
    host: String,
    port: i32,
    error_code: i16,
    error_message: Option<String>,
) -> krabka_protocol::owned::find_coordinator_response::Coordinator {
    krabka_protocol::owned::find_coordinator_response::Coordinator {
        key: key.into(),
        node_id,
        host,
        port,
        error_code,
        error_message,
        ..Default::default()
    }
}

pub fn expected_coordinator_response(
    coordinator: krabka_protocol::owned::find_coordinator_response::Coordinator,
) -> krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse {
    krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse {
        coordinators: vec![coordinator],
        ..Default::default()
    }
}

/// The first partition of the first metadata topic whose name matches exactly.
pub fn metadata_first_partition<'a>(
    response: &'a krabka_protocol::owned::metadata_response::MetadataResponse,
    topic: &str,
) -> Option<&'a krabka_protocol::owned::metadata_response::MetadataResponsePartition> {
    response
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(topic))
        .and_then(|row| row.partitions.first())
}

/// The first matching partition's leader, with a caller-selected missing-row value.
pub fn metadata_first_leader(
    response: &krabka_protocol::owned::metadata_response::MetadataResponse,
    topic: &str,
    default: i32,
) -> i32 {
    metadata_first_partition(response, topic).map_or(default, |partition| partition.leader_id)
}
