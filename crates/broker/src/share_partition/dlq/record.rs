//! The dead-letter records of one produce round: Kafka's
//! `ShareGroupDLQRecordHelper`.
//!
//! Each source offset becomes one record in the dead-letter topic. It carries
//! six headers that say where the record came from and why it is here, and,
//! when the group asks for it, the key and the value of the source record. The
//! original headers are not copied.
//!
//! A range whose records do not fit in one batch of at most the topic's
//! `max.message.bytes` goes out in several rounds, so [`build_round`] takes
//! the offset to start from and reports the last offset it packed.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_protocol::records::{HEADER_LEN, Record, RecordBatch, RecordHeader};

use crate::share_partition::state::DlqCause;

/// The topic the record came from.
pub(super) const HEADER_TOPIC: &str = "__dlq.errors.topic";
/// The partition of the source topic.
pub(super) const HEADER_PARTITION: &str = "__dlq.errors.partition";
/// The offset of the record in the source partition.
pub(super) const HEADER_OFFSET: &str = "__dlq.errors.offset";
/// The share group that gave up on the record.
pub(super) const HEADER_GROUP: &str = "__dlq.errors.group";
/// How many times the record was delivered.
pub(super) const HEADER_DELIVERY_COUNT: &str = "__dlq.errors.delivery.count";
/// Why the record is here: the message of a [`DlqCause`].
pub(super) const HEADER_MESSAGE: &str = "__dlq.errors.message";

/// The key and the value of a source record, which a group that has
/// `errors.deadletterqueue.copy.record.enable` puts in the dead-letter record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SourceRecord {
    pub(super) key: Option<Bytes>,
    pub(super) value: Option<Bytes>,
}

/// What the headers of every record in a range have in common.
#[derive(Debug, Clone, Copy)]
pub(super) struct RangeContext<'a> {
    pub(super) group: &'a str,
    /// The name of the source topic, or its id when the name is not known.
    pub(super) source_topic: &'a str,
    pub(super) source_partition: i32,
    pub(super) delivery_count: i16,
    pub(super) cause: DlqCause,
}

/// The offsets of a range that a round is to pack.
#[derive(Debug, Clone, Copy)]
pub(super) struct RoundBounds {
    /// The first offset of the round.
    pub(super) next: i64,
    /// The last offset of the whole range.
    pub(super) last: i64,
    /// The last offset that the read of the source records reached. An offset
    /// past it was not read, and stays for a later round, which reads it with a
    /// fresh budget.
    pub(super) last_resolved: i64,
    /// The `max.message.bytes` of the dead-letter topic.
    pub(super) max_message_bytes: i32,
}

/// One round of a range: a batch of dead-letter records.
#[derive(Debug, PartialEq)]
pub(super) struct Round {
    pub(super) batch: RecordBatch,
    /// The last source offset that the batch holds.
    pub(super) last_offset: i64,
}

fn header(key: &str, value: &str) -> RecordHeader {
    RecordHeader {
        key: key.to_owned(),
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
    }
}

/// The headers of the dead-letter record of `offset`: Kafka's
/// `ShareGroupDLQRecordHelper.headers`.
fn headers(context: &RangeContext<'_>, offset: i64) -> Vec<RecordHeader> {
    vec![
        header(HEADER_TOPIC, context.source_topic),
        header(HEADER_PARTITION, &context.source_partition.to_string()),
        header(HEADER_OFFSET, &offset.to_string()),
        header(HEADER_GROUP, context.group),
        header(HEADER_DELIVERY_COUNT, &context.delivery_count.to_string()),
        header(HEADER_MESSAGE, context.cause.message()),
    ]
}

/// Kafka's `ShareGroupDLQRecordHelper.dlqDestinationPartition`: the source
/// partition modulo the partition count of the dead-letter topic.
pub(super) fn destination_partition(source_partition: i32, dlq_partitions: i32) -> i32 {
    source_partition % dlq_partitions
}

/// Packs the records of `bounds.next..=bounds.last` that fit in one batch:
/// Kafka's `ShareGroupDLQRecordHelper.buildDLQRecords`.
///
/// The round stops at `bounds.last_resolved`, and it stops before a record
/// that would take the batch past `max_message_bytes`. It always takes at
/// least one record, the first of the round, even when that one record alone
/// is over the limit: the broker then reports the limit, and the range does not
/// stall. A first record whose copied key and value take it over the limit goes
/// with its headers alone, as the fetch does for a source record that is too
/// big for the topic; only a record that its headers alone take over the limit
/// is sent over it. A record with no entry in `sources` gets its headers and no
/// key and no value.
///
/// Every record has `now_ms` as its timestamp. It has to be the wall clock, as
/// log retention judges a segment by the timestamps of its records.
pub(super) fn build_round(
    context: &RangeContext<'_>,
    sources: &BTreeMap<i64, SourceRecord>,
    bounds: RoundBounds,
    now_ms: i64,
) -> Round {
    let limit = i64::from(bounds.max_message_bytes);
    let end = bounds.next.max(bounds.last.min(bounds.last_resolved));
    let mut records: Vec<Record> = Vec::new();
    // The size of the batch so far, tracked as `RecordBatch::encoded_len`
    // computes it, so the walk is not quadratic in the offsets.
    let mut size = i64::try_from(HEADER_LEN).unwrap_or(i64::MAX);
    for offset in bounds.next..=end {
        let source = sources.get(&offset);
        let mut record = Record {
            offset_delta: i32::try_from(records.len()).unwrap_or(i32::MAX),
            key: source.and_then(|source| source.key.clone()),
            value: source.and_then(|source| source.value.clone()),
            headers: headers(context, offset),
            ..Default::default()
        };
        let mut record_size = i64::try_from(record.encoded_len()).unwrap_or(i64::MAX);
        if size.saturating_add(record_size) > limit {
            if !records.is_empty() {
                break;
            }
            // The first record of the round is over the limit by itself. The
            // source record fit the read budget, but the headers and the batch
            // header add to it. The copy is best effort, so the record goes
            // with its headers alone rather than have the broker refuse the
            // whole write as too large.
            record.key = None;
            record.value = None;
            record_size = i64::try_from(record.encoded_len()).unwrap_or(i64::MAX);
        }
        records.push(record);
        size = size.saturating_add(record_size);
        if size > limit {
            break;
        }
    }
    let count = i64::try_from(records.len()).unwrap_or(i64::MAX);
    Round {
        batch: RecordBatch {
            last_offset_delta: i32::try_from(count - 1).unwrap_or(0),
            base_timestamp: now_ms,
            max_timestamp: now_ms,
            records,
            ..Default::default()
        },
        last_offset: bounds.next + count - 1,
    }
}

#[cfg(test)]
pub(super) fn projected_cases_match<T: PartialEq>(
    actual: impl IntoIterator<Item = T>,
    expected: impl IntoIterator<Item = T>,
) -> bool {
    actual.into_iter().eq(expected)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn context(cause: DlqCause) -> RangeContext<'static> {
        RangeContext {
            group: "g1",
            source_topic: "orders",
            source_partition: 3,
            delivery_count: 5,
            cause,
        }
    }

    fn bounds(next: i64, last: i64) -> RoundBounds {
        RoundBounds {
            next,
            last,
            last_resolved: last,
            max_message_bytes: 1_048_588,
        }
    }

    krabka_macros::record_header_text!(header_value, strict);

    /// Kafka's `headers`: six headers for each offset, in that order, with the
    /// message of the cause, and no key and no value when nothing was copied.
    #[test]
    fn a_range_becomes_one_record_per_offset_with_six_headers() {
        let round = build_round(
            &context(DlqCause::ClientReject),
            &BTreeMap::new(),
            bounds(10, 11),
            1_700_000_000_000,
        );

        let header_keys: Vec<&str> = round.batch.records[0]
            .headers
            .iter()
            .map(|h| h.key.as_str())
            .collect();
        assert!(
            (
                round.last_offset,
                round.batch.base_timestamp,
                round.batch.last_offset_delta,
                round.batch.records.len(),
                header_keys,
                round
                    .batch
                    .records
                    .iter()
                    .map(|r| (
                        r.offset_delta,
                        r.key.clone(),
                        r.value.clone(),
                        header_value(r, HEADER_OFFSET),
                        header_value(r, HEADER_TOPIC),
                        header_value(r, HEADER_PARTITION),
                        header_value(r, HEADER_GROUP),
                        header_value(r, HEADER_DELIVERY_COUNT),
                        header_value(r, HEADER_MESSAGE),
                    ))
                    .collect::<Vec<_>>(),
            ) == (
                11,
                1_700_000_000_000,
                1,
                2,
                vec![
                    "__dlq.errors.topic",
                    "__dlq.errors.partition",
                    "__dlq.errors.offset",
                    "__dlq.errors.group",
                    "__dlq.errors.delivery.count",
                    "__dlq.errors.message",
                ],
                [10, 11]
                    .iter()
                    .enumerate()
                    .map(|(delta, offset)| (
                        i32::try_from(delta).unwrap(),
                        None,
                        None,
                        Some(offset.to_string()),
                        Some("orders".to_owned()),
                        Some("3".to_owned()),
                        Some("g1".to_owned()),
                        Some("5".to_owned()),
                        Some("Offset rejected by client.".to_owned()),
                    ))
                    .collect::<Vec<_>>(),
            )
        );
    }

    #[test]
    fn the_message_names_the_cause() {
        let messages: Vec<Option<String>> =
            [DlqCause::ClientReject, DlqCause::DeliveryCountExceeded]
                .into_iter()
                .map(|cause| {
                    let round = build_round(&context(cause), &BTreeMap::new(), bounds(0, 0), 0);
                    header_value(&round.batch.records[0], HEADER_MESSAGE)
                })
                .collect();

        assert!(
            messages
                == vec![
                    Some("Offset rejected by client.".to_owned()),
                    Some("Offset delivery count exceeded the threshold.".to_owned()),
                ]
        );
    }

    /// A group that copies records gets the key and the value of each source
    /// record that was read, and headers alone for an offset that was not.
    #[test]
    fn a_copied_record_carries_the_key_and_the_value() {
        let mut sources = BTreeMap::new();
        sources.insert(
            4,
            SourceRecord {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"v")),
            },
        );

        let round = build_round(&context(DlqCause::ClientReject), &sources, bounds(4, 5), 0);

        assert!(
            round
                .batch
                .records
                .iter()
                .map(|r| (r.key.clone(), r.value.clone()))
                .collect::<Vec<_>>()
                == vec![
                    (
                        Some(Bytes::from_static(b"k")),
                        Some(Bytes::from_static(b"v"))
                    ),
                    (None, None),
                ]
        );
    }

    /// Kafka's `lastResolvedOffset`: a round stops where the read did, and
    /// always takes the offset it starts at, so it is never empty.
    #[test]
    fn a_round_stops_at_the_last_resolved_offset_and_is_never_empty() {
        // (last resolved, last offset in the round)
        let cases = [(12, 12), (10, 10), (5, 10)];
        let actual: Vec<i64> = cases
            .iter()
            .map(|(last_resolved, _)| {
                build_round(
                    &context(DlqCause::ClientReject),
                    &BTreeMap::new(),
                    RoundBounds {
                        last_resolved: *last_resolved,
                        ..bounds(10, 20)
                    },
                    0,
                )
                .last_offset
            })
            .collect();

        assert!(actual == cases.iter().map(|(_, last)| *last).collect::<Vec<_>>());
    }

    /// A range that does not fit in one batch of `max.message.bytes` goes out
    /// in several rounds, and each round is within the limit unless it is a
    /// single record that is over it.
    #[test]
    fn a_range_that_is_too_big_for_one_batch_goes_in_rounds() {
        let sources: BTreeMap<i64, SourceRecord> = (0..6)
            .map(|offset| {
                (
                    offset,
                    SourceRecord {
                        key: None,
                        value: Some(Bytes::from(vec![7_u8; 100])),
                    },
                )
            })
            .collect();
        // Room for two records and not a third.
        let max_message_bytes = i32::try_from(
            build_round(&context(DlqCause::ClientReject), &sources, bounds(0, 1), 0)
                .batch
                .encoded_len(),
        )
        .unwrap();

        let mut next = 0;
        let mut rounds = Vec::new();
        while next <= 5 {
            let round = build_round(
                &context(DlqCause::ClientReject),
                &sources,
                RoundBounds {
                    next,
                    last: 5,
                    last_resolved: 5,
                    max_message_bytes,
                },
                0,
            );
            rounds.push((
                next,
                round.last_offset,
                round.batch.encoded_len() <= usize::try_from(max_message_bytes).unwrap(),
            ));
            next = round.last_offset + 1;
        }

        assert!(rounds == vec![(0, 1, true), (2, 3, true), (4, 5, true)]);
    }

    /// One record that is over the limit on its own is sent alone, so the
    /// range makes progress and the broker reports the limit.
    #[test]
    fn a_record_over_the_limit_on_its_own_is_sent_alone() {
        let round = build_round(
            &context(DlqCause::ClientReject),
            &BTreeMap::new(),
            RoundBounds {
                max_message_bytes: 1,
                ..bounds(3, 9)
            },
            0,
        );

        assert!((round.last_offset, round.batch.records.len()) == (3, 1));
    }

    /// The source record of offset 4: a key and a value of 100 bytes.
    fn source_of_offset_4() -> SourceRecord {
        SourceRecord {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from(vec![7_u8; 100])),
        }
    }

    /// The round that holds only offset 4, with the copied key and value or
    /// with its headers alone.
    fn round_of_offset_4(copied: bool) -> Round {
        let source = source_of_offset_4();
        Round {
            batch: RecordBatch {
                records: vec![Record {
                    key: source.key.filter(|_| copied),
                    value: source.value.filter(|_| copied),
                    headers: headers(&context(DlqCause::ClientReject), 4),
                    ..Default::default()
                }],
                ..Default::default()
            },
            last_offset: 4,
        }
    }

    /// A source record that fit the fetch budget can still be over
    /// `max.message.bytes` once the six headers and the batch header are added.
    /// The record then goes with its headers alone: the write is not lost to a
    /// `MESSAGE_TOO_LARGE` that ends it. A record that fits, and one that its
    /// headers alone take over the limit, are packed as before.
    #[test]
    fn a_copied_record_that_the_headers_push_over_the_limit_goes_without_the_copy() {
        let sources = BTreeMap::from([(4, source_of_offset_4())]);
        let size_of = |sources: &BTreeMap<i64, SourceRecord>| {
            i32::try_from(
                build_round(&context(DlqCause::ClientReject), sources, bounds(4, 4), 0)
                    .batch
                    .encoded_len(),
            )
            .unwrap()
        };
        let (with_copy, headers_only) = (size_of(&sources), size_of(&BTreeMap::new()));
        // (max.message.bytes, whether the round keeps the copy)
        let cases = [
            (with_copy, true),
            (with_copy - 1, false),
            (headers_only, false),
            // The headers alone are over the limit: sent anyway, for the
            // broker to report.
            (headers_only - 1, false),
        ];
        let expected: Vec<Round> = cases
            .iter()
            .map(|(_, copied)| round_of_offset_4(*copied))
            .collect();

        let actual: Vec<Round> = cases
            .iter()
            .map(|(max_message_bytes, _)| {
                build_round(
                    &context(DlqCause::ClientReject),
                    &sources,
                    RoundBounds {
                        max_message_bytes: *max_message_bytes,
                        ..bounds(4, 4)
                    },
                    0,
                )
            })
            .collect();

        assert!(actual == expected);
    }

    /// Kafka's `dlqDestinationPartition`.
    #[test]
    fn the_destination_partition_wraps_the_source_partition() {
        let cases = [
            ((0, 3), 0),
            ((2, 3), 2),
            ((3, 3), 0),
            ((7, 3), 1),
            ((5, 1), 0),
        ];

        assert!(projected_cases_match(
            cases
                .iter()
                .map(|((source, count), _)| destination_partition(*source, *count)),
            cases.iter().map(|(_, want)| *want),
        ));
    }
}
