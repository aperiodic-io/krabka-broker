//! The check that `CreatePartitions` runs before it touches a single topic:
//! the duplicate names Kafka refuses outright. The batch authorization that
//! decides which topic rows short-circuit to `TOPIC_AUTHORIZATION_FAILED` is
//! [`crate::handlers::denied_topics`].

use {
    crate::handlers::request_names::duplicate_names as find_duplicate_names,
    krabka_protocol::owned::create_partitions_request::CreatePartitionsTopic,
};

/// The names that more than one request row carries, in the order of their
/// first row. Kafka's `ControllerApis.createPartitions` answers each once
/// with `INVALID_REQUEST` and grows none of them.
pub(super) fn duplicate_names(topics: &[CreatePartitionsTopic]) -> Vec<String> {
    find_duplicate_names(topics.iter().map(|topic| topic.name.as_str()))
}
