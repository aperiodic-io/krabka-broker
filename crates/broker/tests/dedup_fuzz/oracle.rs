//! What the broker may answer, and what it must do with a batch it accepts.
//!
//! The expectations are computed from the bytes that were sent, read with
//! the protocol crate's own validating parser, and with the dedup identity
//! rule applied here by a plain search over the headers rather than the
//! lookup the broker uses.

use {
    crate::mutate::{DEDUP_HEADER, Mode},
    krabka_broker::codes,
    krabka_compression::RecordDecompressionPolicy,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse,
        records::{Attributes, TimestampType, validate_one_v2_batch},
    },
};

/// The window every fuzz topic deduplicates within: one day, so that the
/// generated timestamps are live unless a mutation moved them far away.
pub const WINDOW_MS: i64 = 86_400_000;

/// How close to the window edge a timestamp is too close to call: the time
/// between the classification and the oracle's clock read.
const EDGE_MS: i64 = 60_000;

/// How far ahead of the broker a record may be dated before Kafka's default
/// `message.timestamp.after.max.ms` refuses it.
const AFTER_MAX_MS: i64 = 3_600_000;

/// The row codes a produce of malformed or adversarial records may get.
///
/// `UNKNOWN_SERVER_ERROR` is not among them: Kafka answers it only for an
/// exception it did not expect, and every input here is one a broker must
/// expect.
pub const ALLOWED_CODES: [i16; 13] = [
    codes::NONE,
    codes::CORRUPT_MESSAGE,
    codes::INVALID_RECORD,
    codes::INVALID_TIMESTAMP,
    codes::INVALID_REQUEST,
    codes::UNSUPPORTED_COMPRESSION_TYPE,
    codes::MESSAGE_TOO_LARGE,
    codes::RECORD_LIST_TOO_LARGE,
    codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
    codes::INVALID_PRODUCER_EPOCH,
    codes::INVALID_PRODUCER_ID_MAPPING,
    codes::INVALID_TXN_STATE,
    codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
];

/// One record of an accepted batch, as the producer sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SentRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub timestamp_ms: i64,
    /// Whether the topic's identity rule gives the record an identity.
    pub identity: bool,
}

/// A batch the broker accepted, read back from the bytes that were sent.
#[derive(Clone, Debug)]
pub struct Accepted {
    pub attributes: Attributes,
    pub producer_id: i64,
    pub records: Vec<SentRecord>,
}

impl Accepted {
    /// Whether a second send of the same bytes goes through dedup again: a
    /// plain producer, a `CreateTime` batch of records, and a codec id that
    /// names a codec. An idempotent batch sent twice is answered by the
    /// producer-sequence check instead.
    pub fn resend_is_deduplicated(&self) -> bool {
        self.producer_id < 0
            && !self.attributes.is_transactional()
            && !self.attributes.is_control_batch()
            && self.attributes.timestamp_type() == TimestampType::CreateTime
            && codec_id(self.attributes) <= 4
    }

    /// Whether a record is dated so close to the edge of Kafka's
    /// `message.timestamp.after.max.ms` that a second send may be refused
    /// where the first was not.
    pub fn near_future_edge(&self, now_ms: i64) -> bool {
        self.records
            .iter()
            .any(|record| (record.timestamp_ms - (now_ms + AFTER_MAX_MS)).abs() < EDGE_MS)
    }

    /// The records a second send at `now_ms` drops: every record with an
    /// identity whose timestamp is still inside the window, as a range,
    /// because a record dated at the window edge can go either way.
    pub fn resend_drops(&self, now_ms: i64) -> (u64, u64) {
        let live_from = now_ms - WINDOW_MS;
        let mut surely = 0;
        let mut maybe = 0;
        for record in self.records.iter().filter(|record| record.identity) {
            if record.timestamp_ms >= live_from + EDGE_MS {
                surely += 1;
            } else if record.timestamp_ms >= live_from - EDGE_MS {
                maybe += 1;
            }
        }
        (surely, surely + maybe)
    }
}

/// The low three attribute bits: the codec id, whether or not it names one.
pub fn codec_id(attributes: Attributes) -> i16 { attributes.0 & 0x07 }

/// Read `bytes` as exactly one v2 batch with the protocol crate, and apply
/// the identity rule of `mode` to each record.
pub fn parse(bytes: &[u8], mode: Mode) -> Result<Accepted, String> {
    let validated = validate_one_v2_batch(bytes).map_err(|error| error.to_string())?;
    if validated.total_len != bytes.len() {
        return Err(format!(
            "one batch of {} bytes in a field of {}",
            validated.total_len,
            bytes.len()
        ));
    }
    let base_timestamp = validated.header.base_timestamp.get();
    let mut records = Vec::new();
    validated
        .validate_records_with(RecordDecompressionPolicy::default(), |record| {
            let headers: Vec<(String, Option<Vec<u8>>)> = record
                .headers
                .iter()
                .map(|header| (header.key.to_owned(), header.value.map(<[u8]>::to_vec)))
                .collect();
            let identity = match mode {
                Mode::Key => record.key.is_some(),
                Mode::Header => headers
                    .iter()
                    .find(|(name, _)| name == DEDUP_HEADER)
                    .is_some_and(|(_, value)| value.is_some()),
            };
            records.push(SentRecord {
                key: record.key.map(<[u8]>::to_vec),
                value: record.value.map(<[u8]>::to_vec),
                headers,
                timestamp_ms: base_timestamp.saturating_add(record.timestamp_delta),
                identity,
            });
        })
        .map_err(|error| error.to_string())?;
    Ok(Accepted {
        attributes: Attributes(validated.header.attributes.get()),
        producer_id: validated.header.producer_id.get(),
        records,
    })
}

/// Whether `stored` is `sent` with some records left out, in order.
pub fn is_subsequence(stored: &[SentRecord], sent: &[SentRecord]) -> bool {
    let mut remaining = sent.iter();
    stored.iter().all(|record| {
        remaining.any(|candidate| {
            candidate.key == record.key
                && candidate.value == record.value
                && candidate.headers == record.headers
        })
    })
}

/// The indices of the records that header mode refuses for lacking the
/// header, or `None` when the bytes do not parse and the set is unknown.
/// Empty in key mode and for a transactional batch, which bypasses dedup.
pub fn missing_header(mode: Mode, parsed: Option<&Accepted>) -> Option<Vec<i32>> {
    let parsed = parsed?;
    if mode == Mode::Key || parsed.attributes.is_transactional() {
        return Some(Vec::new());
    }
    Some(
        parsed
            .records
            .iter()
            .zip(0..)
            .filter(|(record, _)| !record.identity)
            .map(|(_, index)| index)
            .collect(),
    )
}

/// The `batch_index` of each `record_errors` row of `row`.
fn error_indices(row: &PartitionProduceResponse) -> Vec<i32> {
    row.record_errors
        .iter()
        .map(|error| error.batch_index)
        .collect()
}

/// Whether a dedup topic's answer is the answer of the same topic without
/// dedup, but for the records that header mode refuses for lacking the
/// header (`missing`). Dedup never changes whether malformed bytes are
/// accepted; it only adds its own refusal.
pub fn same_answer_but_dedup(
    missing: Option<&[i32]>,
    dedup: &PartitionProduceResponse,
    plain: &PartitionProduceResponse,
) -> Result<(), String> {
    let mismatch = || {
        Err(format!(
            "dedup answered {} {:?}, the same topic without dedup {} {:?}",
            dedup.error_code,
            error_indices(dedup),
            plain.error_code,
            error_indices(plain)
        ))
    };
    let Some(missing) = missing else {
        // The bytes do not parse here, so only the code can be compared, and
        // a header refusal is the one difference dedup may add.
        let header_refusal =
            dedup.error_code == codes::INVALID_RECORD && !dedup.record_errors.is_empty();
        return if dedup.error_code == plain.error_code || header_refusal {
            Ok(())
        } else {
            mismatch()
        };
    };
    let expected = if plain.error_code == codes::NONE && !missing.is_empty() {
        (codes::INVALID_RECORD, missing.to_vec())
    } else if plain.record_errors.is_empty() {
        (plain.error_code, Vec::new())
    } else {
        let mut union = error_indices(plain);
        union.extend_from_slice(missing);
        union.sort_unstable();
        union.dedup();
        (plain.error_code, union)
    };
    if (dedup.error_code, error_indices(dedup)) == expected {
        Ok(())
    } else {
        mismatch()
    }
}
