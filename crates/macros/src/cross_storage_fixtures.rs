//! Explicit wire inputs and feature oracles shared by storage and broker tests.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn supported_feature_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(name: &str, min_version: i16, max_version: i16)
            -> ::krabka_protocol::owned::api_versions_response::SupportedFeatureKey
        {
            ::krabka_protocol::owned::api_versions_response::SupportedFeatureKey {
                name: name.into(), min_version, max_version, ..Default::default()
            }
        }
    })
}

pub(crate) fn supported_features_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [name, supported]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    let name = crate::fixtures::name(name)?;
    let supported = crate::fixtures::name(supported)?;
    Ok(moxy::template! {
        fn {{ name }}(
            metadata_version: ::krabka_protocol::owned::api_versions_response::SupportedFeatureKey,
            share_max: i16,
        ) -> Vec<::krabka_protocol::owned::api_versions_response::SupportedFeatureKey> {
            vec![
                metadata_version,
                {{ supported }}("group.version", 0, 1),
                {{ supported }}("transaction.version", 0, 2),
                {{ supported }}("share.version", 0, share_max),
                {{ supported }}("streams.version", 0, 1),
                {{ supported }}("eligible.leader.replicas.version", 0, 1),
                {{ supported }}("kraft.version", 0, 1),
                {{ supported }}("krabka.version", 0, 1),
            ]
        }
    })
}

pub(crate) fn finalized_feature_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(name: &str, level: i16)
            -> ::krabka_protocol::owned::api_versions_response::FinalizedFeatureKey
        {
            ::krabka_protocol::owned::api_versions_response::FinalizedFeatureKey {
                name: name.into(), max_version_level: level, min_version_level: level,
                ..Default::default()
            }
        }
    })
}

pub(crate) fn record_limit_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        /// A record with a `value_len`-byte value, a key, a header and a
        /// two-byte timestamp delta.
        fn {{ name }}(value_len: usize) -> ::krabka_protocol::records::Record {
            ::krabka_protocol::records::Record {
                timestamp_delta: 300,
                key: Some(::bytes::Bytes::from_static(b"key")),
                value: Some(::bytes::Bytes::from(vec![7_u8; value_len])),
                headers: vec![::krabka_protocol::records::RecordHeader {
                    key: "h".into(), value: None,
                }],
                ..Default::default()
            }
        }
    })
}

pub(crate) fn compaction_record_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        /// A one-record data batch of `key` and `value`. `producer` is `(id, epoch,
        /// base_sequence)`, or `None` for a client with no idempotence.
        fn {{ name }}(
            producer: Option<(i64, i16, i32)>,
            transactional: bool,
            (key, value): (&str, &str),
            timestamp: i64,
        ) -> ::krabka_protocol::records::RecordBatch {
            let (producer_id, producer_epoch, base_sequence) = producer.unwrap_or((-1, -1, -1));
            ::krabka_protocol::records::RecordBatch {
                attributes: ::krabka_protocol::records::Attributes::default().with_transactional(transactional),
                base_timestamp: timestamp,
                max_timestamp: timestamp,
                producer_id, producer_epoch, base_sequence,
                records: vec![::krabka_protocol::records::Record {
                    key: Some(::bytes::Bytes::copy_from_slice(key.as_bytes())),
                    value: Some(::bytes::Bytes::copy_from_slice(value.as_bytes())),
                    ..Default::default()
                }],
                ..::krabka_protocol::records::RecordBatch::default()
            }
        }
    })
}

pub(crate) fn registration_feature_projection(
    input: TokenStream,
) -> Result<TokenStream, ParseError> {
    let [name, feature_type]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    let name = crate::fixtures::name(name)?;
    Ok(moxy::template! {
        fn {{ name }}(features: &[{{ feature_type }}]) -> ::std::collections::BTreeMap<String, (i16, i16)> {
            features.iter().map(|feature| {
                (feature.name.clone(), (feature.min_supported_version, feature.max_supported_version))
            }).collect()
        }
    })
}

/// Independently encode the expected bytes without calling the storage append path.
pub(crate) fn compression_bytes_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [name, level]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    let name = crate::fixtures::name(name)?;
    let level = crate::fixtures::name(level)?;
    Ok(moxy::template! {
        #[derive(Clone, Copy)]
        struct {{ level }}(i32);

        fn {{ name }}(
            batch: &::krabka_protocol::records::RecordBatch,
            level: {{ level }},
        ) -> ::bytes::Bytes {
            let mut expected = ::bytes::BytesMut::new();
            batch.encode_with_compression_level(&mut expected, Some(level.0)).unwrap();
            expected.freeze()
        }
    })
}
