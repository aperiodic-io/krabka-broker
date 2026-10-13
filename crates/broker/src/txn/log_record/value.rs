//! Codec for the Kafka `TransactionLogValue` record, in both its v0
//! non-flexible and its v1 flexible form.
//!
//! The encoder is deterministic: it groups and sorts a transaction's
//! partitions before it writes them, so every replica produces identical
//! bytes for the same entry. The decoder accepts either version, reads the
//! v1 tagged fields it knows, and ignores bytes after the last field, as
//! Kafka's generated reader does.

use {
    crate::{
        error::BrokerError,
        txn::{
            state::{TopicPartition, TxnEntry, TxnState},
            version::TxnVersion,
        },
    },
    bytes::{Bytes, BytesMut},
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_protocol::{
        ProtocolError,
        primitives::{
            array::{get_array_len, get_nullable_array_len, put_array_len, put_nullable_array_len},
            fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
            string_bytes::{
                get_compact_string_owned, get_string_owned, put_compact_string, put_string,
            },
        },
        tagged_fields::{UnknownTaggedFields, WriteTaggedFields, read_tagged_fields},
    },
    std::collections::{BTreeMap, HashSet},
};

/// Tagged-field tags for `TransactionLogValue` v1.
const TAG_PREV_PRODUCER_ID: u32 = 0;
const TAG_NEXT_PRODUCER_ID: u32 = 1;
const TAG_CLIENT_TXN_VERSION: u32 = 2;
const TAG_NEXT_PRODUCER_EPOCH: u32 = 3;
const TAG_LAST_PRODUCER_EPOCH: u32 = 4;

/// Kafka's tagged-field default for the producer-id bookkeeping fields.
const PRODUCER_ID_NONE: i64 = -1;

/// A topic and its partition ids, in ascending order.
struct TopicGroup<'a> {
    topic: &'a str,
    ids: Vec<i32>,
}

/// Group `partitions` into one entry per topic with ascending partition ids.
///
/// The function orders the topics lexicographically. This order matters: the
/// `HashSet` iteration order is nondeterministic, but replicas must produce
/// identical snapshot bytes.
fn group_partitions(partitions: &HashSet<TopicPartition>) -> Vec<TopicGroup<'_>> {
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for tp in partitions {
        by_topic
            .entry(&tp.topic)
            .or_default()
            .push(tp.partition.get());
    }
    by_topic
        .into_iter()
        .map(|(topic, mut ids)| {
            ids.sort_unstable();
            TopicGroup { topic, ids }
        })
        .collect()
}

/// Encode the Kafka `TransactionLogValue` at `txnv`.
///
/// `TV_1` and above select v1, the flexible form; `TV_0` selects v0, the
/// non-flexible form. The output is deterministic: the function groups and
/// sorts the partitions before it encodes them.
///
/// `last_epoch_tag` writes tag 4, `LastProducerEpoch`. Kafka trunk persists it
/// (KAFKA-20357). Kafka 4.3.1 defines tags 0 to 3 only, keeps the last epoch in
/// memory and reloads it as `NO_PRODUCER_EPOCH`, so without the tag a v1 record
/// carries exactly the tags 4.3.1 writes.
pub(crate) fn encode_value(entry: &TxnEntry, txnv: TxnVersion, last_epoch_tag: bool) -> Vec<u8> {
    let flexible = txnv.flexible_records();
    let version: i16 = i16::from(flexible);
    let mut buf = BytesMut::new();

    put_i16(&mut buf, version);
    put_i64(&mut buf, entry.producer_id.get());
    put_i16(&mut buf, entry.producer_epoch);
    put_i32(&mut buf, entry.txn_timeout_ms);
    put_i8(&mut buf, entry.state.to_kafka_status());

    // TransactionPartitions: `TransactionLog.serializeValue` writes null only
    // for an `Empty` transaction, and an empty array in every other state.
    let groups = group_partitions(&entry.partitions);
    if entry.state == TxnState::Empty {
        put_nullable_array_len(&mut buf, None, flexible);
    } else {
        put_nullable_array_len(&mut buf, Some(groups.len()), flexible);
        for TopicGroup { topic, ids } in &groups {
            if flexible {
                put_compact_string(&mut buf, topic);
            } else {
                put_string(&mut buf, topic);
            }
            put_array_len(&mut buf, ids.len(), flexible);
            for id in ids {
                put_i32(&mut buf, *id);
            }
            // PartitionsSchema has no tagged fields of its own; in v1 it still
            // ends with an (always-empty) tagged-field section.
            if flexible {
                WriteTaggedFields::new().write(&mut buf, &UnknownTaggedFields::default());
            }
        }
    }

    put_i64(&mut buf, entry.last_update_ms);
    put_i64(&mut buf, entry.start_ms);

    // Top-level tagged-field section (v1 only). Each tag is written only when
    // it is not its Kafka default.
    if flexible {
        let mut tagged = WriteTaggedFields::new();
        if entry.client_transaction_version != 0 {
            tagged.add(
                TAG_CLIENT_TXN_VERSION,
                i16_to_bytes(entry.client_transaction_version),
            );
        }
        if !entry.prev_producer_id.is_none() {
            tagged.add(
                TAG_PREV_PRODUCER_ID,
                i64_to_bytes(entry.prev_producer_id.get()),
            );
        }
        if !entry.next_producer_id.is_none() {
            tagged.add(
                TAG_NEXT_PRODUCER_ID,
                i64_to_bytes(entry.next_producer_id.get()),
            );
        }
        if entry.next_producer_epoch != -1 {
            tagged.add(
                TAG_NEXT_PRODUCER_EPOCH,
                i16_to_bytes(entry.next_producer_epoch),
            );
        }
        if last_epoch_tag && entry.last_producer_epoch != -1 {
            tagged.add(
                TAG_LAST_PRODUCER_EPOCH,
                i16_to_bytes(entry.last_producer_epoch),
            );
        }
        tagged.write(&mut buf, &UnknownTaggedFields::default());
    }

    buf.to_vec()
}

fn i64_to_bytes(v: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(8);
    put_i64(&mut b, v);
    b.freeze()
}

fn i16_to_bytes(v: i16) -> Bytes {
    let mut b = BytesMut::with_capacity(2);
    put_i16(&mut b, v);
    b.freeze()
}

/// Decode a `TransactionLogValue`. `transactional_id` is supplied from the
/// companion key (it is not present in the value record).
///
/// `last_epoch_tag` reads tag 4, `LastProducerEpoch`, as
/// [`encode_value`] writes it. Without it the tag is skipped as an unknown
/// one and the last epoch reloads as `-1`, as Kafka 4.3.1's
/// `TransactionLog.read` passes `NO_PRODUCER_EPOCH`.
pub(crate) fn decode_value(
    bytes: &[u8],
    transactional_id: String,
    last_epoch_tag: bool,
) -> Result<TxnEntry, BrokerError> {
    let mut buf = bytes;
    let version = get_i16(&mut buf)?;
    let flexible = match version {
        0 => false,
        1 => true,
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unsupported TransactionLogValue version",
            )));
        }
    };

    let producer_id = get_i64(&mut buf)?;
    let producer_epoch = get_i16(&mut buf)?;
    // Kafka's `TransactionLog.read` accepts any timeout, and so does this
    // decoder: the idle reaper's decision is total over it.
    let txn_timeout_ms = get_i32(&mut buf)?;
    let status = get_i8(&mut buf)?;
    let state = TxnState::from_kafka_status(status).ok_or(BrokerError::Protocol(
        ProtocolError::InvalidValue("unknown TransactionStatus"),
    ))?;

    let mut partitions = HashSet::new();
    if let Some(count) = get_nullable_array_len(&mut buf, flexible)? {
        for _ in 0..count {
            let topic = if flexible {
                get_compact_string_owned(&mut buf)?
            } else {
                get_string_owned(&mut buf)?
            };
            let id_count = get_array_len(&mut buf, flexible)?;
            for _ in 0..id_count {
                let partition = get_i32(&mut buf)?;
                partitions.insert(TopicPartition {
                    topic: topic.clone(),
                    partition: PartitionIndex(partition),
                });
            }
            // PartitionsSchema tagged-field section (v1 only); no known tags.
            if flexible {
                read_tagged_fields(&mut buf, |_, _| Ok(false))?;
            }
        }
    }

    let last_update_ms = get_i64(&mut buf)?;
    let start_ms = get_i64(&mut buf)?;

    let mut prev_producer_id = PRODUCER_ID_NONE;
    let mut next_producer_id = PRODUCER_ID_NONE;
    let mut next_producer_epoch = -1;
    let mut last_producer_epoch = -1;
    let mut client_transaction_version = 0;
    if flexible {
        read_tagged_fields(&mut buf, |tag, payload| match tag {
            TAG_PREV_PRODUCER_ID => {
                prev_producer_id = get_i64(payload)?;
                Ok(true)
            }
            TAG_NEXT_PRODUCER_ID => {
                next_producer_id = get_i64(payload)?;
                Ok(true)
            }
            TAG_CLIENT_TXN_VERSION => {
                client_transaction_version = get_i16(payload)?;
                Ok(true)
            }
            TAG_NEXT_PRODUCER_EPOCH => {
                next_producer_epoch = get_i16(payload)?;
                Ok(true)
            }
            TAG_LAST_PRODUCER_EPOCH if last_epoch_tag => {
                last_producer_epoch = get_i16(payload)?;
                Ok(true)
            }
            _ => Ok(false),
        })?;
    }

    Ok(TxnEntry {
        transactional_id,
        // Wrap the decoded raw `i64`s into `ProducerId` at the codec boundary.
        producer_id: ProducerId(producer_id),
        producer_epoch,
        state,
        txn_timeout_ms,
        partitions,
        prev_producer_id: ProducerId(prev_producer_id),
        next_producer_id: ProducerId(next_producer_id),
        next_producer_epoch,
        last_producer_epoch,
        // Whether the fence that recorded the last epoch failed is in-memory
        // only, as in Kafka, so a replayed entry starts with no failed fence.
        has_failed_epoch_fence: false,
        last_update_ms,
        start_ms,
        client_transaction_version,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::txn::version::TxnVersion,
        assert2::{assert, check},
    };

    /// Real captured v1 record (`TV_1`, Ongoing, 48 bytes).
    #[rustfmt::skip]
    const SAMPLE: &[u8] = &[
        0x00, 0x01, // version = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ProducerId = 0
        0x00, 0x00, // ProducerEpoch = 0
        0x00, 0x00, 0xea, 0x60, // TransactionTimeoutMs = 60000
        0x01, // TransactionStatus = 1 (Ongoing)
        0x02, // partitions compact-array len = 1+1
        0x07, b't', b'x', b't', b'e', b's', b't', // topic compact-string "txtest"
        0x02, // PartitionIds compact-array len = 1+1
        0x00, 0x00, 0x00, 0x00, // partition 0
        0x00, // PartitionsSchema tagged-count = 0
        0x00, 0x00, 0x01, 0x9e, 0x7b, 0x4b, 0x36, 0x7a, // LastUpdate ts
        0x00, 0x00, 0x01, 0x9e, 0x7b, 0x4b, 0x36, 0x7a, // Start ts
        0x00, // top-level tagged-count = 0
    ];

    const SAMPLE_TS: i64 = 0x0000_019e_7b4b_367a;

    fn sample_entry() -> TxnEntry {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "txtest".into(),
            partition: PartitionIndex(0),
        });
        TxnEntry {
            transactional_id: "my-txn-id".into(),
            producer_id: ProducerId(0),
            producer_epoch: 0,
            state: TxnState::Ongoing,
            txn_timeout_ms: 60_000,
            partitions,
            prev_producer_id: ProducerId(-1),
            next_producer_id: ProducerId(-1),
            next_producer_epoch: -1,
            last_producer_epoch: -1,
            has_failed_epoch_fence: false,
            last_update_ms: SAMPLE_TS,
            start_ms: SAMPLE_TS,
            // The captured record carries no ClientTransactionVersion, which
            // is the TV_0 a client below the version-2 protocol sends.
            client_transaction_version: 0,
        }
    }

    #[test]
    fn sample_bytes_decode() {
        let entry = decode_value(SAMPLE, "my-txn-id".into(), false).unwrap();
        check!(entry.producer_id == 0);
        check!(entry.producer_epoch == 0);
        check!(entry.txn_timeout_ms == 60_000);
        check!(entry.state == TxnState::Ongoing);
        check!(entry.prev_producer_id == -1);
        check!(entry.next_producer_id == -1);
        check!(entry.next_producer_epoch == -1);
        check!(entry.last_update_ms == SAMPLE_TS);
        check!(entry.start_ms == SAMPLE_TS);
        let expected: HashSet<TopicPartition> = [TopicPartition {
            topic: "txtest".into(),
            partition: PartitionIndex(0),
        }]
        .into_iter()
        .collect();
        check!(entry.partitions == expected);
    }

    #[test]
    fn sample_bytes_encode_byte_identical() {
        let encoded = encode_value(&sample_entry(), TxnVersion::Flexible, false);
        assert!(
            encoded == SAMPLE,
            "encode_value did not byte-match SAMPLE\n  expected: {:02x?}\n  actual:   {:02x?}",
            SAMPLE,
            encoded
        );
    }

    #[test]
    fn v1_round_trip_multi_topic_nondefault_ids() {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "zebra".into(),
            partition: PartitionIndex(5),
        });
        partitions.insert(TopicPartition {
            topic: "zebra".into(),
            partition: PartitionIndex(1),
        });
        partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: PartitionIndex(3),
        });
        let entry = TxnEntry {
            transactional_id: "tid".into(),
            producer_id: ProducerId(42),
            producer_epoch: 7,
            state: TxnState::PrepareCommit,
            txn_timeout_ms: 30_000,
            partitions,
            prev_producer_id: ProducerId(100),
            next_producer_id: ProducerId(200),
            next_producer_epoch: 8,
            last_producer_epoch: -1,
            has_failed_epoch_fence: false,
            last_update_ms: 1_234_567,
            start_ms: 1_000_000,
            client_transaction_version: 2,
        };

        let first = encode_value(&entry, TxnVersion::Verified, false);
        let decoded = decode_value(&first, "tid".into(), false).unwrap();

        check!(decoded.producer_id == 42);
        check!(decoded.producer_epoch == 7);
        check!(decoded.state == TxnState::PrepareCommit);
        check!(decoded.txn_timeout_ms == 30_000);
        check!(decoded.prev_producer_id == 100);
        check!(decoded.next_producer_id == 200);
        check!(decoded.next_producer_epoch == 8);
        check!(decoded.last_update_ms == 1_234_567);
        check!(decoded.start_ms == 1_000_000);
        check!(decoded.partitions == entry.partitions);

        // Re-encode is byte-identical (determinism).
        let second = encode_value(&decoded, TxnVersion::Verified, false);
        assert!(first == second);
    }

    #[test]
    fn v0_round_trip_no_tagged_section() {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "t".into(),
            partition: PartitionIndex(0),
        });
        let entry = TxnEntry {
            transactional_id: "tid".into(),
            producer_id: ProducerId(9),
            producer_epoch: 2,
            state: TxnState::Ongoing,
            txn_timeout_ms: 60_000,
            partitions,
            // Even with non-default ids, v0 has no tagged section, so they are
            // dropped on encode and come back as the -1 default.
            prev_producer_id: ProducerId(5),
            next_producer_id: ProducerId(6),
            next_producer_epoch: 3,
            last_producer_epoch: -1,
            has_failed_epoch_fence: false,
            last_update_ms: 111,
            start_ms: 222,
            client_transaction_version: 0,
        };

        let encoded = encode_value(&entry, TxnVersion::Classic, false);
        // version header is `00 00`.
        assert!(encoded[0] == 0x00 && encoded[1] == 0x00);

        let decoded = decode_value(&encoded, "tid".into(), false).unwrap();
        check!(decoded.producer_id == 9);
        check!(decoded.state == TxnState::Ongoing);
        check!(decoded.partitions == entry.partitions);
        check!(decoded.last_update_ms == 111);
        check!(decoded.start_ms == 222);
        // v0 carries no tagged fields; bookkeeping ids default to -1.
        check!(decoded.prev_producer_id == -1);
        check!(decoded.next_producer_id == -1);
        check!(decoded.next_producer_epoch == -1);
    }

    #[test]
    fn encode_is_deterministic_across_hashset_orders() {
        /// A topic partition to insert, in the order a case inserts it.
        struct Insert {
            topic: &'static str,
            partition: i32,
        }
        let insert = |topic, partition| Insert { topic, partition };
        let make = |order: &[Insert]| {
            let mut partitions = HashSet::new();
            for Insert { topic, partition } in order {
                partitions.insert(TopicPartition {
                    topic: (*topic).into(),
                    partition: PartitionIndex(*partition),
                });
            }
            TxnEntry {
                partitions,
                transactional_id: "tid".into(),
                state: TxnState::Ongoing,
                producer_id: ProducerId(1),
                producer_epoch: 0,
                txn_timeout_ms: 60_000,
                client_transaction_version: 1,
                start_ms: 1,
                last_update_ms: 1,
                prev_producer_id: ProducerId(-1),
                next_producer_id: ProducerId(-1),
                next_producer_epoch: -1,
                last_producer_epoch: -1,
                has_failed_epoch_fence: false,
            }
        };

        let a = make(&[
            insert("b", 2),
            insert("a", 1),
            insert("b", 0),
            insert("a", 3),
        ]);
        let b = make(&[
            insert("a", 3),
            insert("b", 0),
            insert("a", 1),
            insert("b", 2),
        ]);
        assert!(
            encode_value(&a, TxnVersion::Flexible, false)
                == encode_value(&b, TxnVersion::Flexible, false)
        );
        assert!(
            encode_value(&a, TxnVersion::Classic, false)
                == encode_value(&b, TxnVersion::Classic, false)
        );
    }

    #[test]
    fn decode_value_rejects_truncated_input() {
        // A prefix of the valid SAMPLE must error, not panic.
        for input in [&SAMPLE[..10], &SAMPLE[..1], &[][..]] {
            assert!(decode_value(input, "t".into(), false).is_err());
        }
    }

    #[test]
    fn decode_value_rejects_unknown_version() {
        // Version 2 is not a valid TransactionLogValue version.
        let mut bad = SAMPLE.to_vec();
        bad[0] = 0x00;
        bad[1] = 0x02; // version = 2
        assert!(decode_value(&bad, "t".into(), false).is_err());
    }

    #[test]
    fn decode_value_ignores_trailing_bytes() {
        let mut extra = SAMPLE.to_vec();
        extra.push(0xff); // one trailing byte
        assert!(
            decode_value(&extra, "t".into(), false).unwrap()
                == decode_value(SAMPLE, "t".into(), false).unwrap()
        );
    }

    /// `TransactionLog.serializeValue` writes a null partition array only for
    /// an `Empty` transaction, and an empty array in every other state. Both
    /// decode back to an empty set.
    #[test]
    fn a_null_partition_array_is_written_only_for_an_empty_transaction() {
        let mut entry = TxnEntry::new_empty("tid".into(), ProducerId(5), 0, 30_000, 100);
        for txnv in [TxnVersion::Classic, TxnVersion::Verified] {
            let flexible = txnv.flexible_records();
            // The array length sits right after the one-byte status.
            let length_at = 2 + 8 + 2 + 4 + 1;
            entry.state = TxnState::Empty;
            let empty = encode_value(&entry, txnv, false);
            let null_length: &[u8] = if flexible {
                &[0x00]
            } else {
                &[0xff, 0xff, 0xff, 0xff]
            };
            assert!(
                &empty[length_at..length_at + null_length.len()] == null_length,
                "{txnv:?}"
            );
            assert!(
                decode_value(&empty, "tid".into(), false)
                    .expect("decode")
                    .partitions
                    .is_empty()
            );

            entry.state = TxnState::CompleteCommit;
            let completed = encode_value(&entry, txnv, false);
            let empty_length: &[u8] = if flexible {
                &[0x01]
            } else {
                &[0x00, 0x00, 0x00, 0x00]
            };
            assert!(
                &completed[length_at..length_at + empty_length.len()] == empty_length,
                "{txnv:?}"
            );
            assert!(
                decode_value(&completed, "tid".into(), false)
                    .expect("decode")
                    .partitions
                    .is_empty()
            );
        }
    }

    /// `TransactionLog.serializeValue` writes `ClientTransactionVersion` (tag 2)
    /// at value version 1, and the decoder reads it back. `LastProducerEpoch`
    /// (tag 4) is trunk's (KAFKA-20357): Kafka 4.3.1 writes tags 0 to 3 only and
    /// reloads the last epoch as `NO_PRODUCER_EPOCH`.
    #[test]
    fn the_v1_tagged_fields_round_trip() {
        let mut entry = TxnEntry::new_empty("tid".into(), ProducerId(5), 9, 30_000, 100);
        entry.state = TxnState::CompleteCommit;
        entry.last_producer_epoch = 8;
        entry.client_transaction_version = 2;

        // (label, whether the tag is on, the last epoch that comes back)
        for (mode, last_epoch_tag, reloaded_last_epoch) in
            [("4.3.1", false, -1), ("trunk", true, 8)]
        {
            for txnv in [TxnVersion::Flexible, TxnVersion::Verified] {
                let decoded = decode_value(
                    &encode_value(&entry, txnv, last_epoch_tag),
                    "tid".into(),
                    last_epoch_tag,
                )
                .expect("decode");
                assert!(
                    decoded.last_producer_epoch == reloaded_last_epoch,
                    "{mode} {txnv:?}"
                );
                assert!(decoded.client_transaction_version == 2, "{mode} {txnv:?}");
            }

            // v0 has no tagged section, so both fields come back at their default.
            let decoded = decode_value(
                &encode_value(&entry, TxnVersion::Classic, last_epoch_tag),
                "tid".into(),
                last_epoch_tag,
            )
            .expect("decode");
            assert!(decoded.last_producer_epoch == -1, "{mode}");
            assert!(decoded.client_transaction_version == 0, "{mode}");
        }

        // A default last epoch is not written, and the tag is written only for
        // trunk.
        entry.last_producer_epoch = -1;
        let without = encode_value(&entry, TxnVersion::Flexible, true);
        entry.last_producer_epoch = 8;
        let with = encode_value(&entry, TxnVersion::Flexible, true);
        let as_4_3_1 = encode_value(&entry, TxnVersion::Flexible, false);
        assert!(with.len() > without.len());
        assert!(as_4_3_1 == without);
    }

    /// A record that trunk wrote, with tag 4, still reads under 4.3.1's rules:
    /// the decoder skips the tag as one it does not know.
    #[test]
    fn a_trunk_record_reads_without_its_last_epoch_in_4_3_1_mode() {
        let mut entry = TxnEntry::new_empty("tid".into(), ProducerId(5), 9, 30_000, 100);
        entry.state = TxnState::CompleteAbort;
        entry.last_producer_epoch = 8;
        let trunk_bytes = encode_value(&entry, TxnVersion::Verified, true);

        let decoded = decode_value(&trunk_bytes, "tid".into(), false).expect("decode");
        assert!(decoded.last_producer_epoch == -1);
        assert!(decoded.producer_epoch == 9);
    }
}
