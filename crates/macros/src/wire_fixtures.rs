//! Wire and metadata fixtures shared across test crates.

use moxy::{
    ast::ParseError,
    token::{Span, TokenStream, TokenTree},
};

pub(crate) fn topic_record(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(tokens)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(name: &str, topic_id: ::uuid::Uuid) -> ::krabka_metadata::TopicRecord {
            ::krabka_metadata::TopicRecord {
                name: name.into(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }
        }
    })
}

pub(crate) fn producer_batch(input: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments: Vec<_> = input.into_iter().collect();
    let (name, value) = match arguments.as_slice() {
        [TokenTree::Ident(name), comma, value @ ..]
            if comma.is_punct_comma() && !value.is_empty() =>
        {
            (name, TokenStream::from(value))
        }
        _ => {
            return Err(ParseError::new(
                Span::call_site(),
                "expected function name, record value expression",
            ));
        }
    };
    Ok(moxy::template! {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub(crate) struct BatchProducerEpoch(pub i16);
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub(crate) struct BatchSequence(pub i32);
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) struct BatchRecordCount(pub i32);
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) struct BatchTimestamp(pub i64);
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) struct BatchProducer {
            pub id: ::krabka_ids::ProducerId,
            pub epoch: BatchProducerEpoch,
        }
        impl BatchProducer {
            /// Wrap a producer identity returned by the wire codec.
            pub(crate) fn from_wire((id, epoch): (i64, i16)) -> Self {
                Self { id: ::krabka_ids::ProducerId(id), epoch: BatchProducerEpoch(epoch) }
            }
        }
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub(crate) enum BatchTransactionMode {
            #[default]
            Ordinary,
            Transactional,
        }
        #[derive(Clone, Copy)]
        pub(crate) struct ProducerBatchSetup {
            pub producer: BatchProducer,
            pub base_sequence: BatchSequence,
            pub records: BatchRecordCount,
            pub max_timestamp: BatchTimestamp,
            pub transaction: BatchTransactionMode,
        }
        impl Default for ProducerBatchSetup {
            fn default() -> Self {
                Self { producer: BatchProducer::from_wire((7, 0)), base_sequence: BatchSequence::default(), records: BatchRecordCount(1), max_timestamp: BatchTimestamp(1_000), transaction: BatchTransactionMode::Ordinary }
            }
        }
        pub(crate) fn {{ name }}(setup: ProducerBatchSetup) -> ::krabka_protocol::records::RecordBatch {
            let ProducerBatchSetup { producer, base_sequence, records, max_timestamp, transaction } = setup;
            let producer_id = producer.id.0;
            let producer_epoch = producer.epoch.0;
            let base_sequence = base_sequence.0;
            let records = records.0;
            let max_timestamp = max_timestamp.0;
            ::krabka_protocol::records::RecordBatch {
                attributes: ::krabka_protocol::records::Attributes::default().with_transactional(transaction == BatchTransactionMode::Transactional),
                last_offset_delta: records - 1, base_timestamp: max_timestamp, max_timestamp,
                producer_id, producer_epoch, base_sequence,
                records: (0..records).map(|offset_delta| ::krabka_protocol::records::Record {
                    offset_delta, value: Some({{ value }}), ..Default::default()
                }).collect(),
                ..Default::default()
            }
        }
    })
}

pub(crate) fn create_topic(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::Display, ::derive_more::From, ::derive_more::Into)]
            pub(crate) struct TopicPartitionCount(pub i32);
            impl Default for TopicPartitionCount {
                fn default() -> Self { Self(1) }
            }
            #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::Display, ::derive_more::From, ::derive_more::Into)]
            pub(crate) struct TopicReplicationFactor(pub i16);
            impl Default for TopicReplicationFactor {
                fn default() -> Self { Self(1) }
            }
            #[derive(Clone, Copy)]
            pub(crate) struct CreateTopicSetup<'a> {
                pub topic: &'a str,
                pub configs: &'a [(&'a str, &'a str)],
                pub num_partitions: TopicPartitionCount,
                pub replication_factor: TopicReplicationFactor,
                pub timeout: ::krabka_units::Time,
            }
            impl Default for CreateTopicSetup<'_> {
                fn default() -> Self {
                    Self { topic: "orders", configs: &[], num_partitions: TopicPartitionCount(1), replication_factor: TopicReplicationFactor(1), timeout: ::krabka_units::millis(5_000) }
                }
            }
            pub(crate) fn {{ name }}(setup: CreateTopicSetup<'_>) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                use ::krabka_units::convert::TimeExt as _;
                let CreateTopicSetup { topic, configs, num_partitions, replication_factor, timeout } = setup;
                ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                    topics: vec![::krabka_protocol::owned::create_topics_request::CreatableTopic {
                        name: topic.to_owned(), num_partitions: num_partitions.0, replication_factor: replication_factor.0,
                        configs: configs.iter().map(|(name, value)| ::krabka_protocol::owned::create_topics_request::CreatableTopicConfig {
                            name: (*name).to_owned(), value: Some((*value).to_owned()), ..Default::default()
                        }).collect(),
                        ..Default::default()
                    }], timeout_ms: i32::try_from(timeout.millis_i64()).expect("fixture timeout fits Kafka field"), ..Default::default()
                }
            }
        }
    })
}

pub(crate) fn consumer_fetch(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::function(
        input,
        &moxy::template! { pub(crate) },
        &moxy::template! { topic: &str },
        &moxy::template! { ::krabka_protocol::owned::fetch_request::FetchRequest },
        &moxy::template! {
            ::krabka_protocol::owned::fetch_request::FetchRequest {
                replica_id: -1, max_wait_ms: 0, min_bytes: 1, max_bytes: 1 << 20,
                topics: vec![::krabka_protocol::owned::fetch_request::FetchTopic {
                    topic: topic.to_string(), partitions: vec![::krabka_protocol::owned::fetch_request::FetchPartition {
                        partition: 0, fetch_offset: 0, partition_max_bytes: 1 << 20, ..Default::default()
                    }], ..Default::default()
                }], ..Default::default()
            }
        },
    )
}

pub(crate) fn single_replica_partition(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::function(
        input,
        &moxy::template! { pub(crate) },
        &moxy::template! { topic: &str, partition: ::krabka_ids::PartitionIndex, node: ::krabka_metadata::NodeId, },
        &moxy::template! { ::krabka_metadata::PartitionRecord },
        &moxy::template! {
            ::krabka_metadata::PartitionRecord {
                topic: topic.to_owned(), partition: partition.0, leader: node, replicas: vec![node], isr: vec![node],
                leader_epoch: ::krabka_metadata::LeaderEpoch(0), adding_replicas: vec![],
                removing_replicas: vec![], directories: vec![], partition_epoch: 0,
            }
        },
    )
}

/// The version-zero Kafka control-record key and value fixture bytes.
pub(crate) fn control_marker(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(tokens)?;
    Ok(moxy::template! {
        mod {{ name }} {
            fn versioned(payload: &[u8]) -> ::bytes::Bytes {
                let mut bytes = Vec::with_capacity(2 + payload.len());
                bytes.extend_from_slice(&0i16.to_be_bytes());
                bytes.extend_from_slice(payload);
                ::bytes::Bytes::from(bytes)
            }
            pub fn control_key(marker_type: i16) -> ::bytes::Bytes {
                versioned(&marker_type.to_be_bytes())
            }
            pub fn control_value(coordinator_epoch: i32) -> ::bytes::Bytes {
                versioned(&coordinator_epoch.to_be_bytes())
            }
        }
        pub use {{ name }}::{control_key, control_value};
    })
}
