//! Classification of Kafka control batches and of the krabka barrier
//! marker, which the append path and both recovery walks share.
//!
//! The control-record type lives in the key of a control batch's first
//! record, so every caller that must tell a transaction end marker from a
//! barrier marker reads it through this module.

use krabka_protocol::records::RecordBatch;

use crate::error::LogError;

/// Control-record type of a krabka barrier marker.
///
/// Kafka assigns the control-record types 0 to 6: `ABORT`, `COMMIT`,
/// `LEADER_CHANGE`, `SNAPSHOT_HEADER`, `SNAPSHOT_FOOTER`, `KRAFT_VERSION`, and
/// `VOTERS`. The value 1000 starts a krabka-private range that Kafka cannot
/// reach by normal growth. `krabka-raft` uses the same convention for its
/// private api keys at 1003 and 1004.
///
/// A barrier marker is a control batch with one record. The record key holds
/// this type. The batch sets `producer_id` to -1, it sets `producer_epoch` to
/// -1, and it clears the transactional attribute bit. The log keeps no
/// transaction state and no producer state for such a batch. Kafka's
/// `ControlRecordType.parse` reports an unknown type for the value 1000 and
/// skips the batch, so a JVM consumer never sees the record.
///
/// The broker builds the marker batch and the log classifies it. Both read
/// this one constant.
pub const BARRIER_CONTROL_TYPE: i16 = 1000;

/// Control-record type of Kafka's ABORT end marker.
pub const ABORT_CONTROL_TYPE: i16 = 0;

/// Control-record type of Kafka's COMMIT end marker.
pub const COMMIT_CONTROL_TYPE: i16 = 1;

/// Whether the first control key closes a transaction as an abort or commit,
/// as [`batch_control_marker_type`] reads it.
///
/// # Errors
/// Returns the error of [`batch_control_marker_type`].
pub(super) fn transaction_marker_flags(batch: &RecordBatch) -> Result<(bool, bool), LogError> {
    let marker = batch_control_marker_type(batch)?;
    Ok((
        marker == Some(ABORT_CONTROL_TYPE),
        marker == Some(COMMIT_CONTROL_TYPE),
    ))
}

/// The control-record key version this build writes and parses, Kafka's
/// `ControlRecordType.CURRENT_CONTROL_RECORD_KEY_VERSION`.
///
/// It is part of the 1.x on-disk contract because control batches are in the
/// `.log`. Kafka owns the number; krabka follows it.
pub const CONTROL_RECORD_KEY_VERSION: i16 = 0;

/// The `EndTxnMarker` value version this build writes and parses, Kafka's
/// `EndTxnMarker.HIGHEST_SUPPORTED_VERSION`.
///
/// It is part of the 1.x on-disk contract because end markers are in the
/// `.log`. Kafka owns the number; krabka follows it.
pub const END_TXN_MARKER_VALUE_VERSION: i16 = 0;

/// What the log does with one control batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlBatchKind {
    /// A transaction end marker. The log closes the producer's open
    /// transaction, it stamps the committed data, and it records an aborted
    /// range. Every control type outside the krabka-private range lands here,
    /// because Kafka writes no other control type into a data partition.
    Transaction,
    /// A krabka barrier marker, control type [`BARRIER_CONTROL_TYPE`]. The log
    /// leaves `pending`, `pending_stamp_ranges`, `coordinator_epochs`, the
    /// active `.txnindex`, and the producer state unchanged for it.
    Barrier,
}

/// Classify one batch for the append path and for the two recovery paths.
///
/// Returns `None` for a data batch. The control-record type comes from the key
/// of the batch's first record, in the layout that
/// [`parse_control_marker_type`] reads. A control batch with no records is a
/// [`ControlBatchKind::Transaction`], which is the classification such a batch
/// had before the barrier type existed. Compaction emits one for the
/// `RETAIN_EMPTY` rule.
pub fn control_batch_kind(batch: &RecordBatch) -> Option<ControlBatchKind> {
    use krabka_verified::transaction::{LogBatchKind, log_batch_kind};
    let key = batch
        .records
        .first()
        .and_then(|record| record.key.as_deref())
        .unwrap_or(&[]);
    match log_batch_kind(batch.attributes.is_control_batch(), key) {
        LogBatchKind::Data => None,
        LogBatchKind::Barrier => Some(ControlBatchKind::Barrier),
        LogBatchKind::Abort | LogBatchKind::Commit | LogBatchKind::OtherControl => {
            Some(ControlBatchKind::Transaction)
        }
    }
}

/// The length of a control-record key: `(version: i16, type: i16)`, Kafka's
/// `ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE`.
const CONTROL_RECORD_KEY_BYTES: usize = 4;

/// The length of an end-marker value: `(version: i16, coordinator_epoch:
/// i32)`, the bytes Kafka's `EndTxnMarker` version 0 holds.
const END_TXN_MARKER_VALUE_BYTES: usize = 6;

/// Parse the control-marker type from the key of a control record. The key
/// encodes `(version: i16, type: i16)` in big-endian. Returns
/// [`ABORT_CONTROL_TYPE`] for ABORT, [`COMMIT_CONTROL_TYPE`] for COMMIT, and
/// [`BARRIER_CONTROL_TYPE`] for a krabka barrier marker. A missing key reads
/// as an empty one.
///
/// This is Kafka's `ControlRecordType.parseTypeId`. A key shorter than four
/// bytes, or one with a negative version, is corrupt data. A version above
/// [`CONTROL_RECORD_KEY_VERSION`] parses as that version: Kafka's schema for
/// the key may only grow compatibly, so it reads the type a later version
/// carries in the same place. That forward tolerance is Kafka's, and krabka
/// keeps it rather than refuse a marker that a JVM broker accepts.
///
/// # Errors
/// Returns [`LogError::InvalidControlRecordSize`] for a key shorter than four
/// bytes and [`LogError::InvalidControlRecordVersion`] for a negative
/// version.
pub fn parse_control_marker_type(key: Option<&[u8]>) -> Result<i16, LogError> {
    let key = key.unwrap_or_default();
    let Some((&[v0, v1, t0, t1], _)) = key.split_first_chunk::<CONTROL_RECORD_KEY_BYTES>() else {
        return Err(LogError::InvalidControlRecordSize {
            record: "end control record key",
            needed: CONTROL_RECORD_KEY_BYTES,
            found: key.len(),
        });
    };
    let version = i16::from_be_bytes([v0, v1]);
    if version < CONTROL_RECORD_KEY_VERSION {
        return Err(LogError::InvalidControlRecordVersion {
            record: "control record",
            version,
        });
    }
    Ok(i16::from_be_bytes([t0, t1]))
}

/// Parse the transaction coordinator epoch from an end-marker value, which
/// Kafka encodes as `(version: i16, coordinator_epoch: i32)` in big-endian.
/// A missing value reads as an empty one.
///
/// This is Kafka's `EndTransactionMarker.deserializeValue`. A value shorter
/// than six bytes, or one with a negative version, is corrupt data. A version
/// above [`END_TXN_MARKER_VALUE_VERSION`] parses as that version, Kafka's
/// forward tolerance for a marker that a later broker wrote: the fields a
/// later version adds follow the coordinator epoch.
///
/// # Errors
/// Returns [`LogError::InvalidControlRecordSize`] for a value shorter than
/// six bytes and [`LogError::InvalidControlRecordVersion`] for a negative
/// version.
pub fn parse_control_marker_coordinator_epoch(value: Option<&[u8]>) -> Result<i32, LogError> {
    let value = value.unwrap_or_default();
    let Some((&[v0, v1, e0, e1, e2, e3], _)) =
        value.split_first_chunk::<END_TXN_MARKER_VALUE_BYTES>()
    else {
        return Err(LogError::InvalidControlRecordSize {
            record: "end transaction marker value",
            needed: END_TXN_MARKER_VALUE_BYTES,
            found: value.len(),
        });
    };
    let version = i16::from_be_bytes([v0, v1]);
    if version < END_TXN_MARKER_VALUE_VERSION {
        return Err(LogError::InvalidControlRecordVersion {
            record: "end transaction marker",
            version,
        });
    }
    Ok(i32::from_be_bytes([e0, e1, e2, e3]))
}

/// The control-record type of a batch's first record, as
/// [`parse_control_marker_type`] reads it. Returns `None` for a batch with
/// no record, which an earlier compaction pass emptied; Kafka skips such a
/// batch without parsing it.
///
/// # Errors
/// Returns the error of [`parse_control_marker_type`].
pub fn batch_control_marker_type(batch: &RecordBatch) -> Result<Option<i16>, LogError> {
    batch
        .records
        .first()
        .map(|record| parse_control_marker_type(record.key.as_deref()))
        .transpose()
}

/// The coordinator epoch of a batch's first record, as
/// [`parse_control_marker_coordinator_epoch`] reads it. Returns `None` for a
/// batch with no record.
///
/// # Errors
/// Returns the error of [`parse_control_marker_coordinator_epoch`].
pub fn batch_control_marker_coordinator_epoch(
    batch: &RecordBatch,
) -> Result<Option<i32>, LogError> {
    batch
        .records
        .first()
        .map(|record| parse_control_marker_coordinator_epoch(record.value.as_deref()))
        .transpose()
}

/// Check a control batch's marker before the batch is written, as Kafka's
/// `ProducerAppendInfo` deserializes an end marker before the segment write:
/// the key of any control batch, and the value of an ABORT or COMMIT marker.
/// A data batch passes unchecked.
///
/// # Errors
/// Returns [`LogError::InvalidControlRecordSize`] or
/// [`LogError::InvalidControlRecordVersion`] for a marker Kafka refuses.
pub fn check_control_record_versions(batch: &RecordBatch) -> Result<(), LogError> {
    if !batch.attributes.is_control_batch() {
        return Ok(());
    }
    if matches!(
        batch_control_marker_type(batch)?,
        Some(ABORT_CONTROL_TYPE | COMMIT_CONTROL_TYPE)
    ) {
        batch_control_marker_coordinator_epoch(batch)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use krabka_ids::{Offset, ProducerId};
    use krabka_protocol::records::Record;
    use krabka_units::prelude::mebibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        log::{
            Log,
            test_support::{
                BarrierEpoch, BarrierMarkerSetup, PartitionState, abort_marker, barrier_marker,
                barrier_marker_from_producer, commit_marker, compact_test_log, compaction_ctx,
                control_key, control_value, keyed_batch, partition_state, sample_batch, test_log,
                transactional_batch,
            },
        },
        producer_snapshot::ProducerSnapshotEntry,
        segment::Segment,
    };

    /// What one parse came to, with a refusal reduced to the fields that say
    /// which rule refused it.
    #[derive(Debug, PartialEq, Eq)]
    enum Parsed<T> {
        Value(T),
        TooShort { record: &'static str, found: usize },
        BadVersion { record: &'static str, version: i16 },
        OtherError(String),
    }

    impl<T> From<Result<T, LogError>> for Parsed<T> {
        fn from(result: Result<T, LogError>) -> Self {
            match result {
                Ok(value) => Self::Value(value),
                Err(LogError::InvalidControlRecordSize { record, found, .. }) => {
                    Self::TooShort { record, found }
                }
                Err(LogError::InvalidControlRecordVersion { record, version }) => {
                    Self::BadVersion { record, version }
                }
                Err(other) => Self::OtherError(other.to_string()),
            }
        }
    }

    /// One parse case: its name, the bytes, and what the parse comes to.
    type ParseCase<T> = (&'static str, Option<&'static [u8]>, Parsed<T>);

    /// One append case: its name, the marker's key and value, and what the
    /// append comes to.
    type AppendCase = (&'static str, Option<Bytes>, Option<Bytes>, Parsed<()>);

    const KEY: &str = "end control record key";
    const VALUE: &str = "end transaction marker value";

    /// The golden bytes of the two control-record shapes Kafka fixes, as a
    /// JVM broker writes them: an end-marker key `(version 0, type)` and an
    /// end-marker value `(version 0, coordinator epoch)`.
    #[test]
    fn end_marker_golden_bytes_decode() {
        check!(
            Parsed::from(parse_control_marker_type(Some(&[0x00, 0x00, 0x00, 0x01])))
                == Parsed::Value(1)
        );
        check!(
            Parsed::from(parse_control_marker_coordinator_epoch(Some(&[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x11
            ]))) == Parsed::Value(17)
        );
    }

    /// Kafka's `ControlRecordType.parseTypeId`: the type is bytes 2..4 of the
    /// key; a short or missing key and a negative version are corrupt, and a
    /// later version parses as version 0.
    #[test]
    fn a_control_key_parses_as_kafka_parses_it() {
        let cases: [ParseCase<i16>; 12] = [
            ("ABORT", Some(&[0, 0, 0, 0]), Parsed::Value(0)),
            ("COMMIT", Some(&[0, 0, 0, 1]), Parsed::Value(1)),
            ("barrier", Some(&[0, 0, 0x03, 0xe8]), Parsed::Value(1000)),
            ("a longer key", Some(&[0, 0, 0, 1, 9, 9]), Parsed::Value(1)),
            ("a later version", Some(&[0, 1, 0, 1]), Parsed::Value(1)),
            (
                "the largest version",
                Some(&[0x7f, 0xff, 0, 1]),
                Parsed::Value(1),
            ),
            (
                "minus one",
                Some(&[0xff, 0xff, 0, 1]),
                Parsed::BadVersion {
                    record: "control record",
                    version: -1,
                },
            ),
            (
                "the smallest version",
                Some(&[0x80, 0x00, 0, 1]),
                Parsed::BadVersion {
                    record: "control record",
                    version: i16::MIN,
                },
            ),
            (
                "no key",
                None,
                Parsed::TooShort {
                    record: KEY,
                    found: 0,
                },
            ),
            (
                "one byte",
                Some(&[0]),
                Parsed::TooShort {
                    record: KEY,
                    found: 1,
                },
            ),
            (
                "two bytes",
                Some(&[0, 0]),
                Parsed::TooShort {
                    record: KEY,
                    found: 2,
                },
            ),
            (
                "three bytes",
                Some(&[0, 0, 0]),
                Parsed::TooShort {
                    record: KEY,
                    found: 3,
                },
            ),
        ];
        for (what, key, want) in cases {
            check!(
                Parsed::from(parse_control_marker_type(key)) == want,
                "case {what}"
            );
        }
    }

    /// Kafka's `EndTransactionMarker.deserializeValue`: the coordinator epoch
    /// is bytes 2..6 of the value; a short or missing value and a negative
    /// version are corrupt, and a later version parses as version 0.
    #[test]
    fn an_end_marker_value_parses_as_kafka_parses_it() {
        let cases: [ParseCase<i32>; 10] = [
            ("current", Some(&[0, 0, 0, 0, 0, 7]), Parsed::Value(7)),
            (
                "a longer value",
                Some(&[0, 0, 0, 0, 0, 7, 9]),
                Parsed::Value(7),
            ),
            (
                "a later version",
                Some(&[0, 1, 0, 0, 0, 7, 9]),
                Parsed::Value(7),
            ),
            (
                "the largest version",
                Some(&[0x7f, 0xff, 0, 0, 0, 7]),
                Parsed::Value(7),
            ),
            (
                "minus one",
                Some(&[0xff, 0xff, 0, 0, 0, 7]),
                Parsed::BadVersion {
                    record: "end transaction marker",
                    version: -1,
                },
            ),
            (
                "the smallest version",
                Some(&[0x80, 0x00, 0, 0, 0, 7]),
                Parsed::BadVersion {
                    record: "end transaction marker",
                    version: i16::MIN,
                },
            ),
            (
                "no value",
                None,
                Parsed::TooShort {
                    record: VALUE,
                    found: 0,
                },
            ),
            (
                "version only",
                Some(&[0, 0]),
                Parsed::TooShort {
                    record: VALUE,
                    found: 2,
                },
            ),
            (
                "four bytes",
                Some(&[0, 0, 0, 0]),
                Parsed::TooShort {
                    record: VALUE,
                    found: 4,
                },
            ),
            (
                "five bytes",
                Some(&[0, 0, 0, 0, 0]),
                Parsed::TooShort {
                    record: VALUE,
                    found: 5,
                },
            ),
        ];
        for (what, value, want) in cases {
            check!(
                Parsed::from(parse_control_marker_coordinator_epoch(value)) == want,
                "case {what}"
            );
        }
    }

    fn append_open_barrier_transaction(log: &mut Log) {
        let mut data = transactional_batch(1000, 2, &["a", "b"]);
        data.base_sequence = 0;
        log.append(&mut data).unwrap(); // offsets 0 and 1
    }

    /// An end marker Kafka would refuse stops the append, as Kafka's
    /// `ProducerAppendInfo` does when `EndTransactionMarker.deserialize`
    /// throws, and the log is unchanged.
    #[test]
    fn an_end_marker_kafka_refuses_is_not_appended() {
        let cases: [AppendCase; 4] = [
            (
                "a negative key version",
                Some(Bytes::from_static(&[0xff, 0xff, 0x00, 0x01])),
                Some(control_value(17)),
                Parsed::BadVersion {
                    record: "control record",
                    version: -1,
                },
            ),
            (
                "a short key",
                Some(Bytes::from_static(&[0x00, 0x00, 0x00])),
                Some(control_value(17)),
                Parsed::TooShort {
                    record: KEY,
                    found: 3,
                },
            ),
            (
                "a negative value version",
                Some(control_key(COMMIT_CONTROL_TYPE)),
                Some(Bytes::from_static(&[0xff, 0xfe, 0, 0, 0, 17])),
                Parsed::BadVersion {
                    record: "end transaction marker",
                    version: -2,
                },
            ),
            (
                "no value",
                Some(control_key(COMMIT_CONTROL_TYPE)),
                None,
                Parsed::TooShort {
                    record: VALUE,
                    found: 0,
                },
            ),
        ];
        for (what, key, value, want) in cases {
            let (_dir, mut log) = test_log();
            let mut data = transactional_batch(1000, 2, &["a"]);
            data.base_sequence = 0;
            log.append(&mut data).unwrap();
            let before = partition_state(&log, &[1000]);

            let mut marker = commit_marker(1000, 2);
            marker.records[0].key = key;
            marker.records[0].value = value;
            let got = Parsed::from(log.append(&mut marker).map(|_| ()));

            check!(got == want, "case {what}");
            check!(partition_state(&log, &[1000]) == before, "case {what}");
            check!(log.log_end_offset() == Offset(1), "case {what}");
        }
    }

    // ---- barrier-marker tests ----

    /// A barrier marker changes no transaction state and no producer state
    /// while a transaction is open.
    ///
    /// The log routes a control batch by its control-record type, not by its
    /// producer id, so the second case carries the open transaction's
    /// producer id and still closes nothing.
    #[test]
    fn a_barrier_marker_leaves_an_open_transaction_untouched() {
        for (name, mut barrier) in [
            (
                "no producer id",
                barrier_marker(BarrierMarkerSetup {
                    epoch: BarrierEpoch(7),
                    ..Default::default()
                }),
            ),
            (
                "with a producer id",
                barrier_marker_from_producer(crate::log::test_support::HostileBarrierSetup {
                    marker: BarrierMarkerSetup {
                        epoch: BarrierEpoch(7),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            ),
        ] {
            let (_dir, mut log) = crate::log::test_support::stamped_test_log(40, 1);

            append_open_barrier_transaction(&mut log);
            let before = partition_state(&log, &[1000, -1]);

            log.append(&mut barrier).unwrap(); // offset 2

            check!(partition_state(&log, &[1000, -1]) == before, "case {name}");
            // The marker still takes an offset of its own, and it carries no
            // stamp, because a control batch is never stamped.
            check!(log.log_end_offset() == Offset(3), "case {name}");
            check!(log.stamp_for_offset(Offset(2)) == None, "case {name}");
        }
        // The marker's identity fields give no producer tail, so the append
        // writes no producer entry for it.
        check!(Log::data_producer_tail(ProducerId(-1), -1, 0, Offset(2)).unwrap() == None);
        check!(Log::data_producer_tail(ProducerId(1000), -1, 0, Offset(2)).unwrap() == None);
    }

    /// A barrier marker moves the last-stable-offset exactly as an ordinary
    /// non-transactional data batch moves it.
    #[test]
    fn a_barrier_marker_moves_the_lso_like_a_data_batch() {
        for (name, open_transaction, want_lso, want_log_end) in [
            ("no open transaction", false, Offset(1), Offset(1)),
            ("open transaction", true, Offset(0), Offset(2)),
        ] {
            for (kind, is_barrier) in [("data batch", false), ("barrier marker", true)] {
                let (_dir, mut log) = test_log();
                if open_transaction {
                    let mut data = transactional_batch(1000, 0, &["a"]);
                    data.base_sequence = 0;
                    log.append(&mut data).unwrap();
                }
                let mut batch = if is_barrier {
                    barrier_marker(BarrierMarkerSetup::default())
                } else {
                    sample_batch(1)
                };
                log.append(&mut batch).unwrap();
                check!(log.lso() == want_lso, "case {name} / {kind}");
                check!(log.log_end_offset() == want_log_end, "case {name} / {kind}");
            }
        }
    }

    /// A barrier marker between a transaction's data and its end marker does
    /// not disturb what the end marker does.
    #[test]
    fn a_barrier_marker_does_not_disturb_a_following_end_marker() {
        let committed = ProducerSnapshotEntry {
            producer_id: ProducerId(1000),
            producer_epoch: 2,
            last_sequence: 1,
            last_offset: Offset(1),
            offset_delta: 1,
            timestamp: 0,
            coordinator_epoch: 17,
            current_txn_first_offset: None,
        };
        for (name, mut marker, want_aborted, want_stamps) in [
            (
                "commit",
                commit_marker(1000, 2),
                Vec::new(),
                vec![Some(40), Some(40), None, None],
            ),
            (
                "abort",
                abort_marker(1000, 2),
                vec![crate::test_support::aborted_txn(
                    crate::test_support::AbortedTxnSetup::default(),
                )],
                vec![None, None, None, None],
            ),
        ] {
            let (_dir, mut log) = crate::log::test_support::stamped_test_log(40, 1);

            append_open_barrier_transaction(&mut log);
            log.append(&mut barrier_marker(BarrierMarkerSetup {
                epoch: BarrierEpoch(9),
                ..Default::default()
            }))
            .unwrap(); // offset 2
            check!(
                log.lso() == Offset(0),
                "case {name}: the barrier holds the LSO"
            );
            log.append(&mut marker).unwrap(); // offset 3
            // A high watermark past the marker releases the transaction.
            log.release_replicated_transactions(Offset(4));

            check!(
                partition_state(&log, &[1000])
                    == PartitionState {
                        lso: Offset(4),
                        transactions: vec![(1000, (17, None))],
                        aborted: want_aborted,
                        producers: vec![committed],
                    },
                "case {name}"
            );
            let stamps: Vec<Option<u64>> =
                (0..4).map(|o| log.stamp_for_offset(Offset(o))).collect();
            check!(stamps == want_stamps, "case {name}");
        }
    }

    /// Recovery mirrors the append path. A reopened log rebuilds the same
    /// producer state and the same transaction state across barrier markers.
    #[test]
    fn barrier_markers_rebuild_identical_state_after_reopen() {
        let dir = tempdir().unwrap();
        let ids = [1000, 2000, 3000, -1];
        let before = {
            let mut log = crate::test_support::open_log(dir.path());
            log.append(&mut barrier_marker(BarrierMarkerSetup::default()))
                .unwrap(); // 0

            let mut committed = transactional_batch(1000, 2, &["a"]);
            committed.base_sequence = 0;
            log.append(&mut committed).unwrap(); // 1
            log.append(&mut barrier_marker(BarrierMarkerSetup {
                epoch: BarrierEpoch(2),
                ..Default::default()
            }))
            .unwrap(); // 2
            log.append(&mut commit_marker(1000, 2)).unwrap(); // 3

            let mut rolled_back = transactional_batch(2000, 5, &["b", "c"]);
            rolled_back.base_sequence = 0;
            log.append(&mut rolled_back).unwrap(); // 4 and 5
            log.append(&mut barrier_marker(BarrierMarkerSetup {
                epoch: BarrierEpoch(3),
                ..Default::default()
            }))
            .unwrap(); // 6
            log.append(&mut abort_marker(2000, 5)).unwrap(); // 7

            let mut still_open = transactional_batch(3000, 1, &["d"]);
            still_open.base_sequence = 0;
            log.append(&mut still_open).unwrap(); // 8
            // A marker that carries the open transaction's producer id closes
            // nothing on the append path, and recovery reaches the same
            // result.
            log.append(&mut barrier_marker_from_producer(
                crate::log::test_support::HostileBarrierSetup {
                    marker: BarrierMarkerSetup {
                        epoch: BarrierEpoch(4),
                        ..Default::default()
                    },
                    producer: crate::ProducerId(3000),
                    producer_epoch: crate::log::test_support::ProducerEpoch(1),
                },
            ))
            .unwrap(); // 9

            partition_state(&log, &ids)
        };
        // No high watermark has passed a marker yet, so the committed
        // transaction of producer 1000 still holds the LSO at its first offset.
        check!(before.lso == Offset(1));

        let mut reopened = crate::test_support::open_log(dir.path());
        check!(partition_state(&reopened, &ids) == before);
        check!(reopened.log_end_offset() == Offset(10));
        // Once the high watermark passes both markers, the open transaction of
        // producer 3000 holds the LSO at its first offset, and the barrier that
        // follows does not release it.
        check!(reopened.last_stable_offset(Offset(10)) == Offset(8));
    }

    /// Recovery rebuilds a transaction's stamp ranges across a barrier
    /// marker, so a commit that lands after a restart still stamps the
    /// transaction's data.
    #[test]
    fn a_barrier_marker_keeps_stamp_ranges_across_a_restart() {
        let dir = tempdir().unwrap();
        {
            let mut log = crate::test_support::open_log(dir.path());
            append_open_barrier_transaction(&mut log);
            // A marker that carries the open transaction's producer id clears
            // no stamp range, on the append path or on the recovery path.
            log.append(&mut barrier_marker_from_producer(
                crate::log::test_support::HostileBarrierSetup {
                    marker: BarrierMarkerSetup {
                        epoch: BarrierEpoch(5),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ))
            .unwrap(); // offset 2
        }

        let mut reopened = crate::test_support::open_log(dir.path());
        crate::log::test_support::install_stamps(&mut reopened, 40, 1);
        check!(reopened.lso() == Offset(0));

        reopened.append(&mut commit_marker(1000, 2)).unwrap(); // offset 3

        check!(reopened.lso() == Offset(0));
        check!(reopened.last_stable_offset(Offset(4)) == Offset(4));
        let stamps: Vec<Option<u64>> = (0..4)
            .map(|offset| reopened.stamp_for_offset(Offset(offset)))
            .collect();
        check!(stamps == vec![Some(40), Some(40), None, None]);
    }

    /// Compaction keeps every barrier marker, and the marker key never enters
    /// the dedup map.
    #[test]
    fn compaction_keeps_barrier_markers_and_never_indexes_their_key() {
        let (_dir, mut log) = compact_test_log();
        log.append(&mut keyed_batch(0, &[(0, b"k1", b"v0")]))
            .unwrap(); // 0
        log.append(&mut barrier_marker(BarrierMarkerSetup::default()))
            .unwrap(); // 1
        log.append(&mut keyed_batch(0, &[(0, b"k1", b"v1")]))
            .unwrap(); // 2
        log.append(&mut barrier_marker(BarrierMarkerSetup {
            epoch: BarrierEpoch(2),
            ..Default::default()
        }))
        .unwrap(); // 3
        log.append(&mut keyed_batch(0, &[(0, b"tail", b"t")]))
            .unwrap(); // 4, active

        // The dedup map holds the data key only. `should_index_key` keeps the
        // marker key out of it, so no barrier can shadow another.
        let sealed: Vec<&Segment> = log.segments.iter().collect();
        let mut indexed: Vec<Bytes> = crate::compact::build_offset_map(&sealed, vec![], None)
            .unwrap()
            .into_keys()
            .collect();
        indexed.sort();
        check!(indexed == vec![Bytes::from_static(b"k1")]);

        log.compact(&compaction_ctx()).unwrap();

        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        // Both markers survive the pass, unchanged.
        let kept_markers: Vec<Record> = out
            .batches
            .iter()
            .filter(|batch| batch.attributes.is_control_batch())
            .flat_map(|batch| batch.records.iter().cloned())
            .collect();
        let want_markers: Vec<Record> = [1, 2]
            .into_iter()
            .flat_map(|epoch| {
                barrier_marker(BarrierMarkerSetup {
                    epoch: BarrierEpoch(epoch),
                    ..Default::default()
                })
                .records
            })
            .collect();
        check!(kept_markers == want_markers);
        // The data still dedups newest-wins around them.
        let kept_data: Vec<(Bytes, Bytes)> = out
            .batches
            .iter()
            .filter(|batch| !batch.attributes.is_control_batch())
            .flat_map(|batch| batch.records.iter())
            .map(|record| (record.key.clone().unwrap(), record.value.clone().unwrap()))
            .collect();
        check!(
            kept_data
                == vec![
                    (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"tail"), Bytes::from_static(b"t")),
                ]
        );
    }
}
