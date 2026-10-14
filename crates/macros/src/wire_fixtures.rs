//! Wire and metadata fixtures shared across test crates.

use {
    crate::{
        fixtures::{function, name, named_items, record_batch_records},
        meta::item_body,
    },
    moxy::{
        ast::ParseError,
        token::{Span, TokenStream, TokenTree},
    },
};

pub(crate) fn share_produce_parameters(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    if let Some(token) = meta.into_iter().next() {
        return Err(ParseError::new(token.span(), "expected no arguments"));
    }
    let mut tokens = item_body(item, "share_produce_parameters", TokenTree::is_keyword_fn)?.tokens;
    let function = tokens.iter().position(TokenTree::is_keyword_fn).unwrap();
    let Some(TokenTree::Group(arguments)) = tokens.get_mut(function + 2) else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected nongeneric function parameters",
        ));
    };
    if !arguments.delim.is_paren() || arguments.tokens.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "expected nonempty function parameters",
        ));
    }
    if !arguments
        .tokens
        .iter()
        .last()
        .is_some_and(TokenTree::is_punct_comma)
    {
        arguments.tokens.extend(moxy::template! { , });
    }
    arguments.tokens.extend(moxy::template! {
        #[builder(default = "t")] topic: &'_ str,
        #[builder(default)] topic_id: ::uuid::Uuid,
        #[builder(into, default)] partition: ::krabka_ids::PartitionIndex,
    });
    Ok(tokens.into())
}

pub(crate) fn retained_timestamp_bounds(
    floor: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    let floor = name(floor)?;
    let tokens = item_body(item, "retained_timestamp_bounds", TokenTree::is_keyword_fn)?.tokens;
    let item: TokenStream = tokens.into();
    let view: TokenStream = "@".parse().expect("Creusot view operator");
    let requires = moxy::template! { requires };
    Ok(moxy::template! {
        #[{{ requires }}(super::time_index::sparse_timestamp_window_valid(window.0{{ view }}, window.1{{ view }}, window.2{{ view }}))]
        #[{{ requires }}(base{{ view }} >= 0 && {{ floor }}{{ view }} >= 0)]
        {{ item }}
    })
}

pub(crate) fn topic_record(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
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
    let record_list =
        record_batch_records(&moxy::template! { records }, &TokenStream::new(), &value);
    Ok(moxy::template! {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ::derive_more::From, ::derive_more::Into)]
        pub(crate) struct BatchProducerEpoch(pub i16);
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ::derive_more::From, ::derive_more::Into)]
        pub(crate) struct BatchSequence(pub i32);
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::From, ::derive_more::Into)]
        pub(crate) struct BatchRecordCount(pub i32);
        #[derive(Debug, Clone, Copy, PartialEq, Eq, ::derive_more::From, ::derive_more::Into)]
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
        #[bon::builder]
        pub(crate) fn {{ name }}(
            #[builder(default = BatchProducer::from_wire((7, 0)))] producer: BatchProducer,
            #[builder(into, default = BatchSequence(0))] base_sequence: BatchSequence,
            #[builder(into, default = BatchRecordCount(1))] records: BatchRecordCount,
            #[builder(into, default = BatchTimestamp(1_000))] max_timestamp: BatchTimestamp,
            #[builder(default = BatchTransactionMode::Ordinary)] transaction: BatchTransactionMode,
            #[builder(default)] base_offset: i64,
        ) -> ::krabka_protocol::records::RecordBatch {
            let producer_id = producer.id.0;
            let producer_epoch = producer.epoch.0;
            let base_sequence = base_sequence.0;
            let records = records.0;
            let max_timestamp = max_timestamp.0;
            ::krabka_protocol::records::RecordBatch {
                base_offset,
                attributes: ::krabka_protocol::records::Attributes::default().with_transactional(transaction == BatchTransactionMode::Transactional),
                last_offset_delta: records - 1, base_timestamp: max_timestamp, max_timestamp,
                producer_id, producer_epoch, base_sequence,
                {{ record_list }}
                ..Default::default()
            }
        }
    })
}

pub(crate) fn create_topic(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
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
            #[bon::builder]
            pub(crate) fn {{ name }}(
                #[builder(default = "orders")] topic: &'_ str,
                #[builder(default = &[])] configs: &'_ [(&'_ str, &'_ str)],
                #[builder(into, default = TopicPartitionCount(1))] num_partitions: TopicPartitionCount,
                #[builder(into, default = TopicReplicationFactor(1))] replication_factor: TopicReplicationFactor,
                #[builder(default = ::krabka_units::millis(5_000))] timeout: ::krabka_units::Time,
            ) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                use ::krabka_units::convert::TimeExt as _;
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
            pub(crate) fn configured_topic_request_from(
                setup: CreateTopicSetup<'_>,
            ) -> ::krabka_protocol::owned::create_topics_request::CreateTopicsRequest {
                configured_topic_request()
                    .topic(setup.topic)
                    .configs(setup.configs)
                    .num_partitions(setup.num_partitions.0)
                    .replication_factor(setup.replication_factor.0)
                    .timeout(setup.timeout)
                    .call()
            }
        }
    })
}

pub(crate) fn ephemeral_port(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
            /// Bind and release a loopback port for a test container, so
            /// concurrent suites do not collide on a fixed port.
            fn {{ name }}() -> u16 {
                let listener = std::net::TcpListener::bind("127.0.0.1:0")
                    .expect("bind an ephemeral port");
                listener.local_addr().expect("the bound listener has an address").port()
            }
        }
    })
}

pub(crate) fn consumer_fetch(input: TokenStream) -> Result<TokenStream, ParseError> {
    function(
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
    function(
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
    let name = name(tokens)?;
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

#[cfg(test)]
mod fixture_tests {
    use {
        super::{producer_batch, retained_timestamp_bounds, share_produce_parameters},
        assert2::check,
        moxy::ast::{File, ItemFn},
    };

    #[test]
    fn producer_batch_emits_a_parseable_record_batch_builder() {
        let expanded =
            producer_batch("make_batch, ::bytes::Bytes::new()".parse().unwrap()).unwrap();
        let _: File = moxy::parse!({ expanded } as File).unwrap();
    }

    #[yare::parameterized(
        trailing_comma = { "pub async fn fixture(client: &Client, values: Vec<Bytes>,) {}" },
        no_trailing_comma = { "pub async fn fixture(client: &Client, values: Vec<Bytes>) {}" },
    )]
    fn produce_parameters_preserve_the_function_and_append_three_setters(declaration: &str) {
        let expanded =
            share_produce_parameters("".parse().unwrap(), declaration.parse().unwrap()).unwrap();
        let function = moxy::parse!({ expanded } as ItemFn).unwrap();
        check!(function.sig.ident == "fixture");
        check!(function.sig.asyncness.is_some());
        check!(function.sig.params.inner.inputs.len() == 5);
    }

    #[test]
    fn timestamp_bounds_retain_the_signature_and_add_two_preconditions() {
        let expanded = retained_timestamp_bounds(
            "minimum".parse().unwrap(),
            "fn candidate(base: i64, minimum: i64) -> i64 { base.max(minimum) }"
                .parse()
                .unwrap(),
        )
        .unwrap();
        check!(expanded.to_string().contains("requires"));
        let function = moxy::parse!({ expanded } as ItemFn).unwrap();
        check!(function.sig.ident == "candidate");
        check!(function.sig.params.inner.inputs.len() == 2);
        check!(function.attrs.len() == 2);
    }

    #[yare::parameterized(
        missing_floor = { "", "fn fixture() {}" },
        multiple_floors = { "first, second", "fn fixture() {}" },
        wrong_item = { "minimum", "struct Fixture {}" },
    )]
    fn rejects_invalid_timestamp_bound_inputs(floor: &str, declaration: &str) {
        check!(
            retained_timestamp_bounds(floor.parse().unwrap(), declaration.parse().unwrap())
                .is_err()
        );
    }

    #[yare::parameterized(
        options = { "unexpected", "fn fixture(client: &Client,) {}" },
        wrong_item = { "", "struct Fixture {}" },
        generic_function = { "", "fn fixture<T>(client: &T,) {}" },
        empty_parameters = { "", "fn fixture() {}" },
    )]
    fn rejects_unsupported_signatures(options: &str, declaration: &str) {
        check!(
            share_produce_parameters(options.parse().unwrap(), declaration.parse().unwrap())
                .is_err()
        );
    }
}
