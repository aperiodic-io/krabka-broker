//! Timestamped record fixtures shared by local and remote read tests.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::function(
        input,
        &moxy::template! {},
        &moxy::template! { base_offset: i64, timestamps: &[i64], value_byte: u8 },
        &moxy::template! { ::krabka_protocol::records::RecordBatch },
        &moxy::template! {
            let base_timestamp = timestamps.first().copied().unwrap_or_default();
            ::krabka_protocol::records::RecordBatch {
                base_offset,
                last_offset_delta: i32::try_from(timestamps.len().saturating_sub(1)).unwrap(),
                base_timestamp,
                max_timestamp: timestamps.iter().copied().max().unwrap_or_default(),
                records: timestamps.iter().enumerate().map(|(offset_delta, timestamp)| ::krabka_protocol::records::Record {
                    timestamp_delta: timestamp - base_timestamp,
                    offset_delta: i32::try_from(offset_delta).unwrap(),
                    value: Some(::bytes::Bytes::from(vec![value_byte; 4])),
                    ..Default::default()
                }).collect(),
                ..Default::default()
            }
        },
    )
}
