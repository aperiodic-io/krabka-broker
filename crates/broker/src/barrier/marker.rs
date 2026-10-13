//! The barrier marker control record.
//!
//! A marker is a Kafka control batch with one record. Every Kafka consumer
//! skips a control batch, so a marker is invisible to a client and only the
//! offset it occupies is observable. That is what lets a barrier group cut
//! across live topics without disturbing anything that reads them.
//!
//! The record key follows Kafka's control-record layout, and the type code
//! sits beyond Kafka's assigned range. See [`krabka_log::BARRIER_CONTROL_TYPE`].
//!
//! Wire, in field order:
//!
//! ```text
//! key:
//!   version i16 = 0
//!   type    i16 = 1000
//!
//! value:
//!   version      i16 = 0
//!   group        string          i16 byte length, then UTF-8
//!   epoch        i64
//!   triggered_at i64             milliseconds since the Unix epoch
//! ```
//!
//! The batch carries `producer_id` -1, `producer_epoch` -1 and
//! `base_sequence` -1, and it clears the transactional attribute bit. Those
//! values are what keep the marker out of the log's producer and transaction
//! bookkeeping, and what make compaction keep it.

use {
    crate::{coordinator::unified::persistence::put_string, error::BrokerError},
    bytes::Bytes,
    krabka_log::{BARRIER_CONTROL_TYPE, Offset},
    krabka_protocol::{
        ProtocolError,
        primitives::{
            fixed::{get_i16, get_i64, put_i16, put_i64},
            string_bytes::get_string_owned,
        },
        records::{Attributes, Record, RecordBatch},
    },
};

/// The control-record key version. Kafka writes 0.
const CONTROL_KEY_VERSION: i16 = 0;

/// The marker value version.
const VALUE_VERSION: i16 = 0;

/// The contents of one barrier marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarrierMarker {
    /// The barrier group this marker belongs to.
    pub group: String,
    /// The epoch this marker stamps.
    pub epoch: i64,
    /// When the coordinator started the injection, in milliseconds since the
    /// Unix epoch.
    pub triggered_at: i64,
}

/// Build the control batch for one barrier marker.
///
/// `base_offset` is the partition's log end offset, which the partition writer
/// overwrites with the offset it assigns. `leader_epoch` is the partition's
/// current leader epoch: the writer does not stamp it, and a batch that keeps
/// the default of zero carries a false leader epoch in its header.
///
/// # Errors
/// Returns [`BrokerError::Protocol`] when the group name is longer than 32767
/// bytes, which the `i16` length cannot carry.
pub(crate) fn build_barrier_batch(
    marker: &BarrierMarker,
    base_offset: Offset,
    leader_epoch: i32,
) -> Result<RecordBatch, BrokerError> {
    let mut key = Vec::with_capacity(4);
    put_i16(&mut key, CONTROL_KEY_VERSION);
    put_i16(&mut key, BARRIER_CONTROL_TYPE);

    let mut value = Vec::with_capacity(20 + marker.group.len());
    put_i16(&mut value, VALUE_VERSION);
    put_string(&mut value, &marker.group)?;
    put_i64(&mut value, marker.epoch);
    put_i64(&mut value, marker.triggered_at);

    Ok(RecordBatch {
        // `RecordBatch::default` already carries producer_id -1,
        // producer_epoch -1 and base_sequence -1, which is what a
        // non-transactional control batch needs.
        attributes: Attributes::default().with_control(true),
        base_offset: base_offset.0,
        partition_leader_epoch: leader_epoch,
        last_offset_delta: 0,
        // A marker with a zero timestamp would look older than the Unix epoch
        // to time-based retention, and its segment could age out at once.
        base_timestamp: marker.triggered_at,
        max_timestamp: marker.triggered_at,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Record::default()
        }],
        ..RecordBatch::default()
    })
}

/// Read the contents of a barrier marker back out of its record.
///
/// The `krabka barrier verify` path uses this function to prove that the
/// marker at a cut's offset is the one the cut names.
///
/// # Errors
/// Returns a [`ProtocolError`] when the record carries no key or no value,
/// when the key is not a barrier control-record key, when either the key
/// version or the value version is unsupported, or when the value is
/// truncated, holds a non-UTF-8 group name, or has trailing bytes.
pub fn parse_barrier_marker(record: &Record) -> Result<BarrierMarker, ProtocolError> {
    let key = record
        .key
        .as_deref()
        .ok_or(ProtocolError::InvalidValue("barrier marker has no key"))?;
    let mut cur = key;
    if get_i16(&mut cur)? != CONTROL_KEY_VERSION {
        return Err(ProtocolError::InvalidValue(
            "unsupported control record key version",
        ));
    }
    if get_i16(&mut cur)? != BARRIER_CONTROL_TYPE {
        return Err(ProtocolError::InvalidValue(
            "control record is not a barrier marker",
        ));
    }

    let value = record
        .value
        .as_deref()
        .ok_or(ProtocolError::InvalidValue("barrier marker has no value"))?;
    let mut cur = value;
    if get_i16(&mut cur)? != VALUE_VERSION {
        return Err(ProtocolError::InvalidValue(
            "unsupported barrier marker version",
        ));
    }
    let group = get_string_owned(&mut cur)?;
    let epoch = get_i64(&mut cur)?;
    let triggered_at = get_i64(&mut cur)?;
    if !cur.is_empty() {
        return Err(ProtocolError::InvalidValue(
            "trailing bytes after barrier marker",
        ));
    }
    Ok(BarrierMarker {
        group,
        epoch,
        triggered_at,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::persistence::MAX_STRING_BYTES,
            txn::marker::{MarkerType, build_marker_batch},
        },
        assert2::assert,
    };

    fn sample() -> BarrierMarker {
        BarrierMarker {
            group: "orders-cut".to_owned(),
            epoch: 7,
            triggered_at: 1_724_500_000_000,
        }
    }

    fn batch() -> RecordBatch { build_barrier_batch(&sample(), Offset(42), 3).expect("builds") }

    #[test]
    fn a_marker_round_trips_through_its_record() {
        let built = batch();
        assert!(parse_barrier_marker(&built.records[0]).ok() == Some(sample()));
    }

    /// The group name is a string with an `i16` length. The builder writes one
    /// of 32767 bytes and refuses a longer one with an error, because a panic
    /// here would take down the connection of the request that named the group.
    #[test]
    fn a_group_name_of_32767_bytes_builds_and_one_of_32768_is_refused() {
        for (length, builds) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let marker = BarrierMarker {
                group: "g".repeat(length),
                ..sample()
            };
            let built = build_barrier_batch(&marker, Offset(0), 0);
            assert!(built.is_ok() == builds, "{length} bytes");
            if let Ok(batch) = built {
                assert!(
                    parse_barrier_marker(&batch.records[0]).ok() == Some(marker),
                    "{length} bytes"
                );
            }
        }
    }

    #[test]
    fn a_marker_is_a_control_batch_and_not_transactional() {
        let built = batch();
        assert!(built.attributes.is_control_batch());
        assert!(!built.attributes.is_transactional());
    }

    /// The log keeps a barrier marker out of its producer and transaction
    /// bookkeeping by these three values, and compaction keeps the marker
    /// because the producer is not transactional.
    #[test]
    fn a_marker_carries_no_producer_identity() {
        let built = batch();
        assert!(built.producer_id == -1);
        assert!(built.producer_epoch == -1);
        assert!(built.base_sequence == -1);
    }

    #[test]
    fn a_marker_holds_exactly_one_record() {
        let built = batch();
        assert!(built.records.len() == 1);
        assert!(built.last_offset_delta == 0);
    }

    #[test]
    fn a_marker_carries_the_supplied_offset_and_leader_epoch() {
        let built = batch();
        assert!(built.base_offset == 42);
        assert!(built.partition_leader_epoch == 3);
    }

    /// A zero timestamp would look older than the Unix epoch to time-based
    /// retention, and the marker's segment could age out at once.
    #[test]
    fn a_marker_is_stamped_with_its_trigger_time() {
        let built = batch();
        assert!(built.base_timestamp == 1_724_500_000_000);
        assert!(built.max_timestamp == 1_724_500_000_000);
    }

    #[test]
    fn the_key_is_the_kafka_control_layout() {
        let built = batch();
        let key = built.records[0].key.as_ref().expect("marker has a key");
        let mut expected = Vec::new();
        put_i16(&mut expected, 0);
        put_i16(&mut expected, 1000);
        assert!(&key[..] == &expected[..]);
    }

    /// The builder here and the classifier in `krabka-log` each encode the
    /// control-record key themselves. This test is the seam between them: it
    /// appends a real marker to a real log and checks that the log treats it
    /// as a barrier and not as a transaction marker.
    #[test]
    fn the_log_classifies_a_built_marker_as_a_barrier() {
        use krabka_log::{Log, LogConfig, ProducerId};

        let dir = tempfile::tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");

        // Open a transaction, so the last stable offset can only move if the
        // log mistakes the marker for a transaction marker.
        let mut txn = RecordBatch {
            attributes: Attributes::default().with_transactional(true),
            producer_id: ProducerId(7).get(),
            producer_epoch: 0,
            base_sequence: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut txn).expect("append transactional data");
        let lso_before = log.lso();

        let mut built = build_barrier_batch(&sample(), log.log_end_offset(), 0).expect("builds");
        let (assigned, _) = log.append(&mut built).expect("append barrier marker");

        assert!(log.lso() == lso_before);
        let read = log
            .read(assigned, krabka_units::mebibytes(1))
            .expect("read back");
        let marker = read
            .batches
            .iter()
            .find(|b| b.attributes.is_control_batch())
            .expect("marker is in the log");
        assert!(parse_barrier_marker(&marker.records[0]).ok() == Some(sample()));
    }

    #[test]
    fn parsing_rejects_a_transaction_marker() {
        let commit = build_marker_batch(
            krabka_log::ProducerId(1000),
            0,
            Offset(0),
            MarkerType::Commit,
            19,
        );
        assert!(parse_barrier_marker(&commit.records[0]).is_err());
    }

    #[test]
    fn parsing_rejects_a_record_without_a_key_or_value() {
        let no_key = Record {
            key: None,
            value: Some(Bytes::from_static(b"x")),
            ..Record::default()
        };
        assert!(parse_barrier_marker(&no_key).is_err());

        let mut no_value = batch().records.remove(0);
        no_value.value = None;
        assert!(parse_barrier_marker(&no_value).is_err());
    }

    #[test]
    fn parsing_rejects_a_wrong_value_version() {
        let mut built = batch();
        let mut value = built.records[0].value.as_ref().unwrap().to_vec();
        value[1] = 1;
        built.records[0].value = Some(Bytes::from(value));
        assert!(parse_barrier_marker(&built.records[0]).is_err());
    }

    #[test]
    fn parsing_rejects_trailing_bytes() {
        let mut built = batch();
        let mut value = built.records[0].value.as_ref().unwrap().to_vec();
        value.push(0);
        built.records[0].value = Some(Bytes::from(value));
        assert!(parse_barrier_marker(&built.records[0]).is_err());
    }

    #[test]
    fn parsing_rejects_a_truncated_value() {
        let built = batch();
        let value = built.records[0].value.as_ref().unwrap().to_vec();
        for len in 0..value.len() {
            let mut record = built.records[0].clone();
            record.value = Some(Bytes::copy_from_slice(&value[..len]));
            assert!(parse_barrier_marker(&record).is_err(), "length {len}");
        }
    }
}
