//! Metadata fixtures shared by in-memory and topic-backed manager tests.

use moxy::{ast::ParseError, token::TokenStream};

fn started_metadata(
    root: &TokenStream,
    id: &TokenStream,
    start: &TokenStream,
    end: &TokenStream,
    timestamp: &TokenStream,
    event_timestamp: &TokenStream,
    size: &TokenStream,
) -> TokenStream {
    moxy::template! {
        {{ root }}::RemoteLogSegmentMetadata::new(
            {{ id }}, {{ start }}, {{ end }}, {{ timestamp }}, 1, {{ event_timestamp }},
            {{ root }}::RemoteLogSegmentDetails::new(
                {{ size }},
                {{ root }}::RemoteLogSegmentState::CopySegmentStarted,
                ::maplit::btreemap! { ::krabka_ids::LeaderEpoch(0) => {{ start }} },
            ),
        ).unwrap()
    }
}

pub(crate) fn remote_started_segment(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, mut arguments) = crate::fixtures::named_arguments(input, 2)?;
    let root = arguments.next().unwrap();
    let metadata = started_metadata(
        &root,
        &moxy::template! { id },
        &moxy::template! { start },
        &moxy::template! { end },
        &moxy::template! { timestamp },
        &moxy::template! { 100 },
        &moxy::template! { 2048 },
    );
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(
            id: {{ root }}::RemoteLogSegmentId,
            start: i64,
            end: i64,
            timestamp: i64,
        ) -> {{ root }}::RemoteLogSegmentMetadata {
            {{ metadata }}
        }
    })
}

/// `started_name, finish_name, metadata_crate, end | next_offset`.
pub(crate) fn segment(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [started, finish, root, timestamp]: [TokenStream; 4] = crate::meta::arguments(input, 4)?
        .try_into()
        .expect("four arguments");
    let started = crate::fixtures::name(started)?;
    let finish = crate::fixtures::name(finish)?;
    let timestamp = match crate::meta::mode(timestamp, ["end", "next_offset"])? {
        "end" => moxy::template! { end },
        "next_offset" => moxy::template! { end + 1 },
        _ => unreachable!(),
    };
    let metadata = started_metadata(
        &root,
        &moxy::template! { {{ root }}::RemoteLogSegmentId::new(tp(), ::uuid::Uuid::from_u128(id)) },
        &moxy::template! { start },
        &moxy::template! { end },
        &timestamp,
        &moxy::template! { 100 },
        &moxy::template! { 2048 },
    );
    Ok(moxy::template! {
        pub(crate) fn {{ started }}(id: u128, start: i64, end: i64) -> {{ root }}::RemoteLogSegmentMetadata {
            {{ metadata }}
        }

        pub(crate) fn {{ finish }}(id: u128) -> {{ root }}::RemoteLogSegmentMetadataUpdate {
            {{ root }}::RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: {{ root }}::RemoteLogSegmentId::new(tp(), ::uuid::Uuid::from_u128(id)),
                event_timestamp_ms: 200,
                custom_metadata: Some({{ root }}::CustomMetadata(vec![7])),
                state: {{ root }}::RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            }
        }
    })
}

/// Indexed, version-zero WORM metadata with the caller's segment-id namespace.
pub(crate) fn worm_segment(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [name, root, id_base, topic, partition, span]: [TokenStream; 6] =
        crate::meta::arguments(input, 6)?
            .try_into()
            .expect("six arguments");
    let name = crate::fixtures::name(name)?;
    let metadata = started_metadata(
        &root,
        &moxy::template! {
            {{ root }}::RemoteLogSegmentId::new(
                {{ root }}::TopicIdPartition::new(::uuid::Uuid::from_u128(1), {{ topic }}, {{ partition }}),
                ::uuid::Uuid::from_u128({{ id_base }} + u128::try_from(index).unwrap()),
            )
        },
        &moxy::template! { start },
        &moxy::template! { start + {{ span }} - 1 },
        &moxy::template! { 1_713_000_000_000 },
        &moxy::template! { 1_713_000_001_000 },
        &moxy::template! { 4096 },
    );
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(index: usize) -> {{ root }}::RemoteLogSegmentMetadata {
            let start = i64::try_from(index).unwrap() * {{ span }};
            {{ metadata }}
        }
    })
}

/// `check_name, metadata_crate`; callers retain the unknown partition identity.
pub(crate) fn missing(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, root) = crate::fixtures::named_root(input)?;
    Ok(moxy::template! {
        fn {{ name }}(
            manager: &impl {{ root }}::RemoteLogMetadataManager,
            partition: &{{ root }}::TopicIdPartition,
        ) {
            ::assert2::check!(manager.remote_log_segment_metadata(partition, ::krabka_ids::LeaderEpoch(0), 0).unwrap() == None);
            ::assert2::check!(manager.highest_offset_for_epoch(partition, ::krabka_ids::LeaderEpoch(0)).unwrap() == None);
            ::assert2::check!(manager.list_remote_log_segments(partition).unwrap().is_empty());
        }
    })
}

pub(crate) fn wal_capture_topic_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, mut arguments) = crate::fixtures::named_arguments(input, 4)?;
    let topic_id = arguments.next().unwrap();
    let topic = arguments.next().unwrap();
    let partitions = arguments.next().unwrap();
    Ok(moxy::template! {
        fn {{ name }}() -> ::std::collections::HashMap<::uuid::Uuid, (String, i32)> {
            ::std::collections::HashMap::from([({{ topic_id }}, ({{ topic }}.to_owned(), {{ partitions }}))])
        }
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::segment;

    #[test]
    fn timestamp_policy_is_explicit_and_bounded_to_the_supported_fixtures() {
        for input in [
            "started, finish, crate, end",
            "started, finish, krabka_remote_storage, next_offset",
        ] {
            assert!(segment(input.parse().unwrap()).is_ok(), "{input}");
        }
        for input in [
            "started, finish, crate",
            "started, finish, crate, last",
            "started, finish, , end",
        ] {
            assert!(segment(input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
