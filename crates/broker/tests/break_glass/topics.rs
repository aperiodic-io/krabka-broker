//! The ordinary Kafka topic requests the cases set a cluster up with and read
//! it back through.
//!
//! `delete_topic` is both of those and a gated transition in its own right, so
//! it answers the row's error code instead of asserting on it. The gate cases
//! need the refusal, and the setup cases assert on the code themselves.

use {
    crate::support::discovery::topic_metadata_request,
    assert2::assert,
    krabka_broker::codes,
    krabka_client_core::{Client, ClientError},
    krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
        metadata_request::MetadataRequest,
    },
    std::time::Duration,
};

/// `retry.backoff.ms`: the first wait of a Kafka admin client before a retry.
const RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// `retry.backoff.max.ms`: the admin client doubles the wait up to this bound.
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);
/// `default.api.timeout.ms`: how long the admin client retries one call.
const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(60);

/// Create `name` with one partition and the given replication factor.
pub(super) async fn create_topic(client: &Client, name: &str, replication_factor: i16) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_owned(),
                num_partitions: 1,
                replication_factor,
                ..CreatableTopic::default()
            }],
            timeout_ms: 10_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    let code = response.topics.first().map(|topic| topic.error_code);
    assert!(code == Some(codes::NONE), "create {name}: {response:?}");
}

/// Delete `name`, and answer the row's error code.
pub(super) async fn delete_topic(client: &Client, name: &str) -> i16 {
    try_delete_topic(client, name).await.expect("DeleteTopics")
}

/// Delete `name`, and answer the row's error code or the client error.
async fn try_delete_topic(client: &Client, name: &str) -> Result<i16, ClientError> {
    let response = client
        .send(DeleteTopicsRequest {
            // The encoder writes `topics` at version 6 and later and
            // `topic_names` below it, so filling both leaves the negotiated
            // version to pick.
            topics: vec![DeleteTopicState {
                name: Some(name.to_owned()),
                ..DeleteTopicState::default()
            }],
            topic_names: vec![name.to_owned()],
            timeout_ms: 10_000,
            ..DeleteTopicsRequest::default()
        })
        .await?;
    Ok(response
        .responses
        .first()
        .map_or(codes::UNKNOWN_SERVER_ERROR, |row| row.error_code))
}

/// Delete `name` the way a Kafka admin client does, and answer the final row
/// error code.
///
/// `KafkaAdminClient` sends `DeleteTopics` to the controller that `Metadata`
/// names. In `KRaft` mode that is any live broker, and just after a failover it
/// can still be the broker that stopped. The client retries two failures: a
/// row that answers `NOT_CONTROLLER`, and a connection that fails. For both, it
/// reads `Metadata` again and retries the call. It waits `retry.backoff.ms`
/// before the first retry and doubles the wait up to `retry.backoff.max.ms`. It
/// stops when `default.api.timeout.ms` passes. Any other code is final, so this
/// helper answers it at once.
pub(super) async fn delete_topic_as_admin_client(bootstrap: &Client, name: &str) -> i16 {
    let deadline = tokio::time::Instant::now() + DEFAULT_API_TIMEOUT;
    let mut backoff = RETRY_BACKOFF;
    loop {
        let failure = match delete_through_controller(bootstrap, name).await {
            Ok(code) if code != codes::NOT_CONTROLLER => return code,
            Ok(_) => "NOT_CONTROLLER".to_owned(),
            Err(error) => error.to_string(),
        };
        assert!(
            tokio::time::Instant::now() + backoff < deadline,
            "DeleteTopics {name} still failed after {DEFAULT_API_TIMEOUT:?}: {failure}"
        );
        eprintln!("DeleteTopics {name} failed with {failure}; retrying after {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
    }
}

/// Send `DeleteTopics` for `name` to the controller that `bootstrap`'s
/// `Metadata` names.
async fn delete_through_controller(bootstrap: &Client, name: &str) -> Result<i16, ClientError> {
    let metadata = bootstrap
        .send(topic_metadata_request(Some(Vec::new())))
        .await?;
    let controller = metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == metadata.controller_id)
        .unwrap_or_else(|| panic!("Metadata names no live controller: {metadata:?}"));
    let client =
        super::cluster::plain_client(&format!("{}:{}", controller.host, controller.port)).await;
    try_delete_topic(&client, name).await
}

/// Whether `client`'s cluster still knows `name`.
pub(super) async fn topic_exists(client: &Client, name: &str) -> bool {
    client
        // Null topics is "every topic", the request `kafka-topics --list`
        // sends; an empty list would ask for none.
        .send(MetadataRequest {
            allow_auto_topic_creation: false,
            ..topic_metadata_request(None)
        })
        .await
        .expect("Metadata")
        .topics
        .iter()
        .any(|topic| topic.name.as_deref() == Some(name) && topic.error_code == codes::NONE)
}
