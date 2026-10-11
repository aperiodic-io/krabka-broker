//! Control-record construction. A commit/abort marker is a single-
//! record `RecordBatch` with `is_control_batch=true` and
//! `is_transactional=true` in attributes.
//!
//! The record key layout matches Apache Kafka `EndTransactionMarker`:
//!   version: i16 (big-endian) = 0
//!   type:    i16 (big-endian), 0 = ABORT, 1 = COMMIT
//! The record value matches Kafka's `EndTxnMarker` schema:
//!   version:           i16 (big-endian) = 0
//!   `coordinator_epoch`: i32 (big-endian)

use bytes::Bytes;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use crate::{codes, error::BrokerError, txn::util::now_millis};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerType {
    Commit,
    Abort,
}

impl MarkerType {
    fn type_code(self) -> i16 {
        match self {
            MarkerType::Commit => 1,
            MarkerType::Abort => 0,
        }
    }
}

/// `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43). Nothing in this broker raises it,
/// so it has no constant in [`codes`]; the marker classification still has to
/// name it.
const UNSUPPORTED_FOR_MESSAGE_FORMAT: i16 = 43;

/// What the `EndTxn` marker fan-out does with one partition's
/// `WriteTxnMarkers` answer, one variant per branch of Kafka's
/// `TransactionMarkerRequestCompletionHandler.onComplete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerCodeClass {
    /// `NONE`: the marker is written, and the partition leaves the pending
    /// set.
    Written,
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT` or `UNSUPPORTED_VERSION`: the producer
    /// could not have written to the partition either, so it leaves the
    /// pending set without a marker.
    Dropped,
    /// `UNKNOWN_TOPIC_OR_PARTITION`, `NOT_LEADER_OR_FOLLOWER`,
    /// `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`,
    /// `REQUEST_TIMED_OUT` or `KAFKA_STORAGE_ERROR`: retry this partition.
    Retriable,
    /// `INVALID_PRODUCER_EPOCH` or `TRANSACTION_COORDINATOR_FENCED`: a newer
    /// producer or coordinator generation superseded this fan-out, so it is
    /// cancelled.
    Fenced,
    /// Any other code. Kafka names `CORRUPT_MESSAGE`, `MESSAGE_TOO_LARGE`,
    /// `RECORD_LIST_TOO_LARGE` and `INVALID_REQUIRED_ACKS` as fatal and
    /// throws `IllegalStateException` for them and for every code it does not
    /// list. The throw ends the completion handler without re-enqueuing a
    /// marker or writing the completion, so the transaction stays prepared
    /// until the coordinator reloads its partition and sends the markers
    /// again.
    Unexpected,
}

/// Classify one partition's `WriteTxnMarkers` error code the way Kafka's
/// `TransactionMarkerRequestCompletionHandler` does.
pub(crate) fn classify_marker_code(code: i16) -> MarkerCodeClass {
    match code {
        codes::NONE => MarkerCodeClass::Written,
        UNSUPPORTED_FOR_MESSAGE_FORMAT | codes::UNSUPPORTED_VERSION => MarkerCodeClass::Dropped,
        codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::NOT_LEADER_OR_FOLLOWER
        | codes::NOT_ENOUGH_REPLICAS
        | codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND
        | codes::REQUEST_TIMED_OUT
        | codes::KAFKA_STORAGE_ERROR => MarkerCodeClass::Retriable,
        codes::INVALID_PRODUCER_EPOCH | codes::TRANSACTION_COORDINATOR_FENCED => {
            MarkerCodeClass::Fenced
        }
        _ => MarkerCodeClass::Unexpected,
    }
}

/// How the `EndTxn` marker fan-out reacts to one partition's marker write
/// failing. A [`MarkerCodeClass::Written`] or [`MarkerCodeClass::Dropped`]
/// answer is not a failure, so it has no variant here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerFailureClass {
    /// Retry this partition; the fan-out has not been superseded.
    Retriable,
    /// A newer producer or coordinator generation has fenced this fan-out.
    /// Retrying cannot succeed, so the fan-out must stop.
    Fenced,
    /// The leader answered a code Kafka's completion handler treats as an
    /// illegal state. The fan-out stops, and the transaction waits in its
    /// `Prepare*` state for the coordinator's next load to resend it.
    Unexpected,
}

impl MarkerFailureClass {
    /// Whether this failure ends the fan-out instead of queueing a retry.
    pub(crate) fn stops_fan_out(self) -> bool {
        match self {
            MarkerFailureClass::Retriable => false,
            MarkerFailureClass::Fenced | MarkerFailureClass::Unexpected => true,
        }
    }
}

/// Classify one marker-write failure.
///
/// Both the local append path
/// ([`append_marker_and_materialize`](crate::txn::handlers::write_txn_markers::append_marker_and_materialize),
/// via [`BrokerError::ProducerEpochFenced`] / [`BrokerError::CoordinatorEpochFenced`])
/// and the remote `WriteTxnMarkers` RPC (via [`BrokerError::MarkerWriteRefused`])
/// go through [`classify_marker_code`], so a fenced producer or coordinator
/// generation cancels the fan-out the same way whether the failing partition
/// is local or remote.
///
/// Every other error carries no wire code: the leader could not be reached,
/// its response did not name the partition, or the local append failed on
/// the way. Kafka re-enqueues every marker of a request whose connection
/// dropped, so those retry.
pub(crate) fn classify_marker_failure(error: &BrokerError) -> MarkerFailureClass {
    let code = match error {
        BrokerError::ProducerEpochFenced { .. } => codes::INVALID_PRODUCER_EPOCH,
        BrokerError::CoordinatorEpochFenced { .. } => codes::TRANSACTION_COORDINATOR_FENCED,
        BrokerError::MarkerWriteRefused { code, .. } => *code,
        _ => return MarkerFailureClass::Retriable,
    };
    match classify_marker_code(code) {
        MarkerCodeClass::Fenced => MarkerFailureClass::Fenced,
        MarkerCodeClass::Retriable => MarkerFailureClass::Retriable,
        // A written or dropped partition is never recorded as a failure, so
        // reaching here with one of their codes is itself an illegal state.
        MarkerCodeClass::Written | MarkerCodeClass::Dropped | MarkerCodeClass::Unexpected => {
            MarkerFailureClass::Unexpected
        }
    }
}

pub fn build_marker_batch(
    producer_id: ProducerId,
    producer_epoch: i16,
    base_offset: Offset,
    marker_type: MarkerType,
    coordinator_epoch: i32,
) -> RecordBatch {
    let mut key = Vec::with_capacity(4);
    key.extend_from_slice(&0i16.to_be_bytes()); // version
    key.extend_from_slice(&marker_type.type_code().to_be_bytes());

    let mut value = Vec::with_capacity(6);
    value.extend_from_slice(&0i16.to_be_bytes()); // version
    value.extend_from_slice(&coordinator_epoch.to_be_bytes());

    let attrs = Attributes::default()
        .with_transactional(true)
        .with_control(true);

    // Kafka's `MemoryRecords.withEndTransactionMarker` stamps the marker
    // with the broker's clock. The marker's timestamp becomes the producer's
    // `ProducerStateEntry.lastTimestamp`, which `producer.id.expiration.ms`
    // ages the producer by.
    let now = now_millis();
    RecordBatch {
        attributes: attrs,
        base_offset: base_offset.0,
        last_offset_delta: 0,
        base_timestamp: now,
        max_timestamp: now,
        // Unwrap into the raw-`i64` protocol `RecordBatch` field at the wire seam.
        producer_id: producer_id.get(),
        producer_epoch,
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// One row per branch of Kafka's
    /// `TransactionMarkerRequestCompletionHandler.onComplete` error match.
    #[test]
    fn classifies_every_write_txn_markers_code_as_kafka_does() {
        let cases = [
            (codes::NONE, MarkerCodeClass::Written),
            (UNSUPPORTED_FOR_MESSAGE_FORMAT, MarkerCodeClass::Dropped),
            (codes::UNSUPPORTED_VERSION, MarkerCodeClass::Dropped),
            (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                MarkerCodeClass::Retriable,
            ),
            (codes::NOT_LEADER_OR_FOLLOWER, MarkerCodeClass::Retriable),
            (codes::NOT_ENOUGH_REPLICAS, MarkerCodeClass::Retriable),
            (
                codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
                MarkerCodeClass::Retriable,
            ),
            (codes::REQUEST_TIMED_OUT, MarkerCodeClass::Retriable),
            (codes::KAFKA_STORAGE_ERROR, MarkerCodeClass::Retriable),
            (codes::INVALID_PRODUCER_EPOCH, MarkerCodeClass::Fenced),
            (
                codes::TRANSACTION_COORDINATOR_FENCED,
                MarkerCodeClass::Fenced,
            ),
            (codes::CORRUPT_MESSAGE, MarkerCodeClass::Unexpected),
            (codes::MESSAGE_TOO_LARGE, MarkerCodeClass::Unexpected),
            (codes::RECORD_LIST_TOO_LARGE, MarkerCodeClass::Unexpected),
            (codes::INVALID_REQUIRED_ACKS, MarkerCodeClass::Unexpected),
            (codes::UNKNOWN_SERVER_ERROR, MarkerCodeClass::Unexpected),
            (
                codes::CLUSTER_AUTHORIZATION_FAILED,
                MarkerCodeClass::Unexpected,
            ),
            (codes::NOT_COORDINATOR, MarkerCodeClass::Unexpected),
            (
                codes::COORDINATOR_LOAD_IN_PROGRESS,
                MarkerCodeClass::Unexpected,
            ),
            (codes::INVALID_TXN_STATE, MarkerCodeClass::Unexpected),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (code, class) in cases {
            actual.push((code, classify_marker_code(code)));
            expected.push((code, class));
        }
        assert!(actual == expected);
    }

    #[test]
    fn classifies_marker_failures_by_the_code_they_carry() {
        let refused = |code| BrokerError::MarkerWriteRefused {
            code,
            message: "refused".into(),
        };
        let cases: &[(BrokerError, MarkerFailureClass)] = &[
            (
                BrokerError::ProducerEpochFenced {
                    producer_id: 7,
                    current: 3,
                    requested: 1,
                },
                MarkerFailureClass::Fenced,
            ),
            (
                BrokerError::CoordinatorEpochFenced {
                    current: 9,
                    requested: 3,
                },
                MarkerFailureClass::Fenced,
            ),
            (
                refused(codes::INVALID_PRODUCER_EPOCH),
                MarkerFailureClass::Fenced,
            ),
            (
                refused(codes::TRANSACTION_COORDINATOR_FENCED),
                MarkerFailureClass::Fenced,
            ),
            (
                refused(codes::NOT_LEADER_OR_FOLLOWER),
                MarkerFailureClass::Retriable,
            ),
            (
                refused(codes::UNKNOWN_TOPIC_OR_PARTITION),
                MarkerFailureClass::Retriable,
            ),
            (
                refused(codes::REQUEST_TIMED_OUT),
                MarkerFailureClass::Retriable,
            ),
            (
                refused(codes::CORRUPT_MESSAGE),
                MarkerFailureClass::Unexpected,
            ),
            (
                refused(codes::UNKNOWN_SERVER_ERROR),
                MarkerFailureClass::Unexpected,
            ),
            // Neither code is ever recorded as a failure.
            (refused(codes::NONE), MarkerFailureClass::Unexpected),
            (
                refused(codes::UNSUPPORTED_VERSION),
                MarkerFailureClass::Unexpected,
            ),
            // No wire code: an unreachable leader, which Kafka re-enqueues.
            (
                BrokerError::Txn("connect failed".into()),
                MarkerFailureClass::Retriable,
            ),
        ];
        for (error, expected) in cases {
            assert!(classify_marker_failure(error) == *expected, "{error:?}");
        }
    }

    #[test]
    fn only_fenced_and_unexpected_failures_stop_the_fan_out() {
        let cases = [
            (MarkerFailureClass::Retriable, false),
            (MarkerFailureClass::Fenced, true),
            (MarkerFailureClass::Unexpected, true),
        ];
        for (class, stops) in cases {
            assert!(class.stops_fan_out() == stops, "{class:?}");
        }
    }

    #[test]
    fn commit_marker_attribute_bits_set() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(7), MarkerType::Commit, 19);
        assert!(b.attributes.is_transactional());
        assert!(b.attributes.is_control_batch());
    }

    #[test]
    fn abort_marker_key_starts_with_version_zero_then_type_zero() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Abort, 19);
        let key = b.records[0].key.as_ref().unwrap();
        // i16 BE version 0, then i16 BE control type 0 (abort).
        assert!(&key[..] == &[0u8, 0, 0, 0][..]);
    }

    #[test]
    fn commit_marker_key_type_is_one() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let key = b.records[0].key.as_ref().unwrap();
        assert!(&key[2..] == &1i16.to_be_bytes());
    }

    #[test]
    fn marker_value_contains_version_and_coordinator_epoch() {
        let b = build_marker_batch(ProducerId(1000), 0, Offset(0), MarkerType::Commit, 19);
        let value = b.records[0].value.as_ref().unwrap();
        assert!(&value[..2] == &0i16.to_be_bytes());
        assert!(&value[2..] == &19i32.to_be_bytes());
    }
}
