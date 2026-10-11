//! The per-partition checks that `AddPartitionsToTxn` runs before a
//! transaction reaches the coordinator.
//!
//! Kafka's `KafkaApis.handleAddPartitionsToTxnRequest` checks every requested
//! partition first. A partition is `TOPIC_AUTHORIZATION_FAILED` when its topic
//! is not authorized, else `UNKNOWN_TOPIC_OR_PARTITION` when the metadata has
//! no such partition. When any partition fails, the whole transaction fails:
//! nothing is added, and every other partition answers
//! `OPERATION_NOT_ATTEMPTED`.
//!
//! Versions 0 to 3 come from clients and need `Write` on each topic, and an
//! internal topic is never authorized. Versions 4 and later come from brokers,
//! which the handler already checked for `ClusterAction`, so every topic is
//! authorized.

use std::net::SocketAddr;

use krabka_metadata::{AclOperation, MetadataImage};
use krabka_protocol::owned::common::{
    add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    add_partitions_to_txn_response::{
        add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
        add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    },
};
use krabka_security::Principal;

use crate::{
    authorizer::{AuthorizationResult, Authorizer, authorize_topics},
    codes,
    config::BrokerConfig,
    internal_topics::is_internal_topic,
};

/// Who sent the request, and so which topics need a `Write` check.
#[derive(Clone, Copy)]
pub(super) enum TopicAuthorization<'a> {
    /// Versions 0 to 3: a client. Each non-internal topic needs `Write`, and
    /// an internal topic is refused.
    Client {
        authorizer: &'a dyn Authorizer,
        principal: &'a Principal,
        peer: &'a SocketAddr,
        config: &'a BrokerConfig,
    },
    /// Versions 4 and later: a broker that holds `ClusterAction`. Every topic
    /// is authorized.
    Broker,
}

/// The result rows of a transaction that fails its partition checks, or
/// `None` when every partition is authorized and exists.
pub(super) fn failed_partitions(
    image: &MetadataImage,
    authorization: TopicAuthorization<'_>,
    topics: &[AddPartitionsToTxnTopic],
) -> Option<Vec<AddPartitionsToTxnTopicResult>> {
    let denied: std::collections::HashSet<&str> = match authorization {
        TopicAuthorization::Broker => std::collections::HashSet::new(),
        TopicAuthorization::Client {
            authorizer,
            principal,
            peer,
            config,
        } => {
            let checked = topics
                .iter()
                .map(|topic| topic.name.as_str())
                .filter(|name| !is_internal_topic(config, name));
            let allowed: std::collections::HashSet<&str> = authorize_topics(
                authorizer,
                image,
                principal,
                peer,
                AclOperation::Write,
                checked,
            )
            .into_iter()
            .filter(|(_, result)| *result == AuthorizationResult::Allow)
            .map(|(name, _)| name)
            .collect();
            topics
                .iter()
                .map(|topic| topic.name.as_str())
                .filter(|name| !allowed.contains(name))
                .collect()
        }
    };

    let mut failed = false;
    let rows = topics
        .iter()
        .map(|topic| AddPartitionsToTxnTopicResult {
            name: topic.name.clone(),
            results_by_partition: topic
                .partitions
                .iter()
                .map(|&partition| {
                    let code = if denied.contains(topic.name.as_str()) {
                        codes::TOPIC_AUTHORIZATION_FAILED
                    } else if image.partition(&topic.name, partition).is_none() {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    } else {
                        codes::OPERATION_NOT_ATTEMPTED
                    };
                    failed |= code != codes::OPERATION_NOT_ATTEMPTED;
                    AddPartitionsToTxnPartitionResult {
                        partition_index: partition,
                        partition_error_code: code,
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    failed.then_some(rows)
}
