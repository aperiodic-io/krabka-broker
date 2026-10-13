//! Reading the dedup identity and timestamp of each record in a batch.
//!
//! The produce handler, the follower path and the rebuild all read records
//! through these functions, so the three agree on which records an index
//! holds. The produce handler reads borrowed records from the producer's
//! bytes; the follower path and the rebuild read the records of the log.

use {
    krabka_dedup::{DedupRecord, IdentityDigest, IdentityLookup, identity_of},
    krabka_protocol::records::{Attributes, Record, RecordBatch, RecordBorrowed, TimestampType},
    std::sync::Arc,
};

/// How one record takes part in deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordIdentity {
    /// The record has an identity, or has none and passes through
    /// (a null key under the record-key rule).
    Indexed(DedupRecord),
    /// The topic names a header and the record does not carry a value for it.
    /// The produce path refuses the batch. The follower path and the rebuild
    /// skip the record, because the log already holds it.
    MissingHeader,
}

/// Read the identity of one record from its key and headers.
pub(crate) fn record_identity<'a, H>(
    key: Option<&'a [u8]>,
    headers: H,
    key_header: Option<&str>,
    timestamp_ms: i64,
) -> RecordIdentity
where
    H: IntoIterator<Item = (&'a str, Option<&'a [u8]>)>,
{
    let identity = match identity_of(key, headers, key_header) {
        IdentityLookup::Identity(bytes) => Some(IdentityDigest::of(bytes)),
        IdentityLookup::NoIdentity => None,
        IdentityLookup::MissingHeader => return RecordIdentity::MissingHeader,
    };
    RecordIdentity::Indexed(DedupRecord {
        identity,
        timestamp_ms,
    })
}

/// The timestamp of one record of a stored batch, as a consumer reads it.
///
/// Under `LogAppendTime` the broker's stamp in `max_timestamp` replaces every
/// record's own timestamp.
pub(crate) fn stored_record_timestamp(
    attributes: Attributes,
    base_timestamp: i64,
    max_timestamp: i64,
    timestamp_delta: i64,
) -> i64 {
    match attributes.timestamp_type() {
        TimestampType::LogAppendTime => max_timestamp,
        TimestampType::CreateTime => base_timestamp.saturating_add(timestamp_delta),
    }
}

/// Whether a stored batch holds records that an index reads.
///
/// A control batch holds markers, not records. A transactional batch is
/// outside dedup, because its records can still abort: an index that held
/// them would drop a later record that duplicates a record no consumer of
/// committed data ever reads.
pub(crate) fn indexes_batch(attributes: Attributes) -> bool {
    !attributes.is_control_batch() && !attributes.is_transactional()
}

/// Call `visit` with the identity of every record of an owned batch.
///
/// `append_time_ms` replaces every record's timestamp for a batch that the
/// log stamps with its append time after this call. A stored batch already
/// carries its stamp and passes `None`.
pub(crate) fn visit_owned_batch(
    batch: &RecordBatch,
    key_header: Option<&str>,
    append_time_ms: Option<i64>,
    mut visit: impl FnMut(RecordIdentity),
) {
    if !indexes_batch(batch.attributes) {
        return;
    }
    for record in &batch.records {
        let timestamp_ms = append_time_ms.unwrap_or_else(|| {
            stored_record_timestamp(
                batch.attributes,
                batch.base_timestamp,
                batch.max_timestamp,
                record.timestamp_delta,
            )
        });
        visit(owned_record_identity(record, key_header, timestamp_ms));
    }
}

fn owned_record_identity(
    record: &Record,
    key_header: Option<&str>,
    timestamp_ms: i64,
) -> RecordIdentity {
    record_identity(
        record.key.as_deref(),
        record
            .headers
            .iter()
            .map(|header| (header.key.as_str(), header.value.as_deref())),
        key_header,
        timestamp_ms,
    )
}

/// The identities of a client batch, in record order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CollectedIdentities {
    /// One entry per record that has a value for the identity rule.
    pub(crate) records: Vec<DedupRecord>,
    /// The batch index of each record that lacks the configured header.
    pub(crate) missing_header: Vec<i32>,
}

/// Collects the identities of a client batch while the produce handler walks
/// its records.
///
/// The handler already walks every record to validate it, so the collector
/// adds no pass of its own. A client batch is always `CreateTime`; the writer
/// replaces the timestamps on a `LogAppendTime` topic.
#[derive(Debug)]
pub(crate) struct IdentityCollector {
    key_header: Option<Arc<str>>,
    base_timestamp: i64,
    next_index: i32,
    collected: CollectedIdentities,
}

impl IdentityCollector {
    pub(crate) fn new(key_header: Option<Arc<str>>, base_timestamp: i64, records: usize) -> Self {
        Self {
            key_header,
            base_timestamp,
            next_index: 0,
            collected: CollectedIdentities {
                records: Vec::with_capacity(records),
                missing_header: Vec::new(),
            },
        }
    }

    pub(crate) fn visit_borrowed(&mut self, record: &RecordBorrowed<'_>) {
        let identity = record_identity(
            record.key,
            record
                .headers
                .iter()
                .map(|header| (header.key, header.value)),
            self.key_header.as_deref(),
            self.base_timestamp.saturating_add(record.timestamp_delta),
        );
        self.push(identity);
    }

    pub(crate) fn visit_owned(&mut self, record: &Record) {
        let identity = owned_record_identity(
            record,
            self.key_header.as_deref(),
            self.base_timestamp.saturating_add(record.timestamp_delta),
        );
        self.push(identity);
    }

    fn push(&mut self, identity: RecordIdentity) {
        match identity {
            RecordIdentity::Indexed(record) => self.collected.records.push(record),
            RecordIdentity::MissingHeader => self.collected.missing_header.push(self.next_index),
        }
        self.next_index = self.next_index.saturating_add(1);
    }

    pub(crate) fn finish(self) -> CollectedIdentities { self.collected }
}
