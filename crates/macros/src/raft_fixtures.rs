//! Consensus log and voter fixtures shared by unit and integration tests.

use {
    crate::{fixtures::name, meta::arguments},
    moxy::{ast::ParseError, token::TokenStream},
};

pub(crate) fn empty_endpoint_voters(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(ids: &[::krabka_ids::NodeId]) -> ::krabka_metadata::voters::VoterSet {
            ::krabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
                ::krabka_metadata::voters::Voter {
                    id,
                    directory_id: ::uuid::Uuid::nil(),
                    endpoints: Vec::new(),
                    kraft_version: ::krabka_metadata::voters::KRaftVersionRange::default(),
                }
            }))
        }
    })
}

pub(crate) fn epoch_record_batch(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(base: i64, epoch: i32, value: &[u8]) -> ::krabka_protocol::records::RecordBatch {
            ::krabka_protocol::records::RecordBatch {
                base_offset: base,
                partition_leader_epoch: epoch,
                attributes: ::krabka_protocol::records::Attributes::default(),
                last_offset_delta: 0,
                base_timestamp: 0,
                max_timestamp: 0,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                records: vec![::krabka_protocol::records::Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: None,
                    value: Some(::bytes::Bytes::copy_from_slice(value)),
                    headers: Vec::new(),
                }],
            }
        }
    })
}

pub(crate) fn epoch_log_view(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut arguments = arguments(input.clone(), 2)
        .or_else(|_| arguments(input, 3))?
        .into_iter();
    let name = name(arguments.next().unwrap())?;
    let root = arguments.next().unwrap();
    let context = arguments
        .next()
        .unwrap_or_else(|| moxy::template! { "log length fits in i64" });
    Ok(moxy::template! {
        impl {{ root }}::LogView for {{ name }} {
            fn end_offset(&self) -> i64 {
                i64::try_from(self.epochs.len()).expect({{ context }})
            }
            fn last_epoch(&self) -> {{ root }}::Epoch {
                self.epochs.last().copied().unwrap_or(0)
            }
            fn end_offset_for_epoch(&self, epoch: {{ root }}::Epoch) -> {{ root }}::LogOffsetMetadata {
                {{ root }}::LogOffsetMetadata::end_of_epoch_in(&self.epochs, epoch)
            }
        }
    })
}
