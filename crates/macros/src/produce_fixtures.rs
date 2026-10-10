//! A single-partition Produce request shared by wire-test fixtures.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn single_partition_produce(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
            #[derive(Clone, Copy)]
            pub(crate) struct WireAcknowledgements(pub i16);
            #[derive(Clone, Copy, Default)]
            pub(crate) enum ProduceAcknowledgements {
                NoResponse,
                #[default]
                Leader,
                AllReplicas,
                Unsupported(WireAcknowledgements),
            }
            impl ProduceAcknowledgements {
                /// Preserve observed and deliberately malformed acknowledgement codes.
                pub(crate) fn from_wire(code: WireAcknowledgements) -> Self {
                    match code.0 { 0 => Self::NoResponse, 1 => Self::Leader, -1 => Self::AllReplicas, _ => Self::Unsupported(code) }
                }
                fn wire_code(self) -> i16 {
                    match self { Self::NoResponse => 0, Self::Leader => 1, Self::AllReplicas => -1, Self::Unsupported(code) => code.0 }
                }
            }
            #[derive(Clone, Copy)]
            pub(crate) struct ProduceTimeoutMillis(pub i32);
            impl Default for ProduceTimeoutMillis {
                fn default() -> Self { Self(5_000) }
            }
            pub(crate) struct SinglePartitionProduceSetup {
                pub topic: String,
                pub topic_id: ::krabka_protocol::primitives::uuid::Uuid,
                pub partition: ::krabka_ids::PartitionIndex,
                pub records: Option<::krabka_protocol::records::RecordsPayload>,
                pub acknowledgements: ProduceAcknowledgements,
                pub timeout: ProduceTimeoutMillis,
            }
            impl Default for SinglePartitionProduceSetup {
                fn default() -> Self {
                    Self { topic: "orders".into(), topic_id: Default::default(), partition: ::krabka_ids::PartitionIndex(0),
                        records: None, acknowledgements: ProduceAcknowledgements::Leader, timeout: ProduceTimeoutMillis::default() }
                }
            }
            impl SinglePartitionProduceSetup {
                /// The common quorum-acknowledged fixture, with scenario overrides.
                pub(crate) fn replicated() -> Self {
                    Self { acknowledgements: ProduceAcknowledgements::AllReplicas, ..Self::default() }
                }
            }
            pub(crate) fn {{ name }}(setup: SinglePartitionProduceSetup) -> ::krabka_protocol::owned::produce_request::ProduceRequest {
                ::krabka_protocol::owned::produce_request::ProduceRequest {
                    acks: setup.acknowledgements.wire_code(),
                    timeout_ms: setup.timeout.0,
                    topic_data: vec![::krabka_protocol::owned::produce_request::TopicProduceData {
                        name: setup.topic,
                        topic_id: setup.topic_id,
                        partition_data: vec![::krabka_protocol::owned::produce_request::PartitionProduceData {
                            index: setup.partition.0,
                            records: setup.records,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            }
        }
    })
}
