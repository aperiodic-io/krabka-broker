//! The rows `Metadata` answers for a requested topic that does not exist, and
//! the topic auto-creation behind them.
//!
//! Kafka's `KafkaApis.getTopicMetadata` answers each missing topic that the
//! principal may describe with a row that carries no partitions and the zero
//! id. A name that `Topic.validate` refuses gets `INVALID_TOPIC_EXCEPTION`
//! (17); any other name gets `UNKNOWN_TOPIC_OR_PARTITION` (3). When the
//! request allows auto-creation and `auto.create.topics.enable` is on,
//! `DefaultAutoTopicCreationManager.createTopics` first sends a
//! `CreateTopics` request for the valid names with the requester's principal,
//! and still answers each of them with 3: the topic is not described until a
//! later request finds it in the metadata image. It lists the invalid names
//! ahead of the created ones.
//!
//! The auto-creation is [`crate::auto_topic_creation::AutoTopicCreation`]:
//! it sends the `CreateTopics` request to the active controller in a KIP-590
//! Envelope that names the client, and does not wait for the answer. The
//! controller authorizes the client and charges its controller-mutation
//! quota, as Kafka's `ControllerApis.createTopics` does. A coordinator topic
//! gets its configured partition count, replication factor and topic configs
//! (`creatableTopic`). A name whose creation
//! is in flight already, from an earlier `Metadata` request or from a
//! coordinator lookup, is not sent again (`filterCreatableTopics`). It is
//! answered 3 among the invalid names.

use {
    crate::{broker::Broker, codes, handlers::RequestContext, topic_creator::ForwardedIdentity},
    krabka_log::topic_name::validate_topic_name,
    krabka_protocol::owned::metadata_response::MetadataResponseTopic,
};

/// The rows for `names`, the missing topics that the principal may describe
/// and, when `auto_create` is set, may create. With `auto_create` set this
/// also asks for the valid names to be created, in the name of the client of
/// the request `ctx` serves.
pub(super) fn missing_topic_rows(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    names: &[&str],
    auto_create: bool,
) -> Vec<MetadataResponseTopic> {
    if auto_create {
        return broker.auto_topic_creation.create_topics(
            broker,
            names,
            Some(ForwardedIdentity::of(ctx)),
        );
    }
    names
        .iter()
        .map(|name| {
            let error_code = if validate_topic_name(name).is_ok() {
                codes::UNKNOWN_TOPIC_OR_PARTITION
            } else {
                codes::INVALID_TOPIC_EXCEPTION
            };
            super::topic_error_row(broker, name, error_code)
        })
        .collect()
}

#[cfg(test)]
mod tests;
