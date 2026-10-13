//! Reading the dedup identity and timestamp of each record in a batch.
//!
//! The produce handler, the follower path and the rebuild all read records
//! through these functions, so the three agree on which records an index
//! holds. The produce handler reads borrowed records from the producer's
//! bytes; the follower path and the rebuild read the records of the log.

use {
    bytes::Bytes,
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::{DedupRecord, IdentityDigest, IdentityLookup, identity_of},
    krabka_protocol::records::{
        Attributes, Record, RecordBatch, RecordBorrowed, TimestampType, validate_one_v2_batch,
    },
    std::sync::Arc,
};

/// How many identities a collector reserves room for up front.
///
/// The record count a client batch declares is the producer's claim, read
/// before any record is parsed: a 61-byte batch can declare `i32::MAX`
/// records. Reserving room for the claim would let one request allocate tens
/// of gibibytes, so the collector reserves at most this many and grows past
/// it only as records actually parse.
const PREALLOCATED_IDENTITIES: usize = 4096;

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

/// Call `visit` with the identity of every record of an owned batch, with
/// each record's timestamp as a consumer reads it.
pub(crate) fn visit_owned_batch(
    batch: &RecordBatch,
    key_header: Option<&str>,
    mut visit: impl FnMut(RecordIdentity),
) {
    if !indexes_batch(batch.attributes) {
        return;
    }
    for record in &batch.records {
        let timestamp_ms = stored_record_timestamp(
            batch.attributes,
            batch.base_timestamp,
            batch.max_timestamp,
            record.timestamp_delta,
        );
        visit(owned_record_identity(record, key_header, timestamp_ms));
    }
}

/// Call `visit` with the identity of every record of one wire batch, with
/// each record's timestamp as a consumer reads it.
///
/// A batch that does not validate is skipped. The log accepted these bytes,
/// so this happens only to a batch the index cannot read, and an index that
/// lacks some identities is narrower, never wrong.
pub(crate) fn visit_wire_batch(
    bytes: &[u8],
    key_header: Option<&str>,
    policy: RecordDecompressionPolicy,
    mut visit: impl FnMut(RecordIdentity),
) {
    let Ok(validated) = validate_one_v2_batch(bytes) else {
        return;
    };
    let attributes = Attributes(validated.header.attributes.get());
    if !indexes_batch(attributes) {
        return;
    }
    let base_timestamp = validated.header.base_timestamp.get();
    let max_timestamp = validated.header.max_timestamp.get();
    let _ = validated.validate_records_with(policy, |record| {
        visit(record_identity(
            record.key,
            record
                .headers
                .iter()
                .map(|header| (header.key, header.value)),
            key_header,
            stored_record_timestamp(
                attributes,
                base_timestamp,
                max_timestamp,
                record.timestamp_delta,
            ),
        ));
    });
}

/// A batch an index reads identities from, in either shape the writer holds.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BatchSource<'a> {
    /// No records an index reads.
    Empty,
    /// One wire batch.
    Wire(&'a Bytes),
    /// A decoded batch.
    Owned(&'a RecordBatch),
}

/// The identities of the records of `source` that have one, with their
/// timestamps as a consumer reads them. A record that lacks the configured
/// header is skipped: the log already holds it.
///
/// The index reads a batch through this before the batch moves into the log,
/// so it keeps one digest and timestamp per record rather than a copy of the
/// batch.
pub(crate) fn identities_of(
    source: BatchSource<'_>,
    key_header: Option<&str>,
    policy: RecordDecompressionPolicy,
) -> Vec<DedupRecord> {
    let mut records = Vec::new();
    let collect = |identity: RecordIdentity| {
        if let RecordIdentity::Indexed(record) = identity
            && record.identity.is_some()
        {
            records.push(record);
        }
    };
    match source {
        BatchSource::Empty => {}
        BatchSource::Wire(bytes) => visit_wire_batch(bytes, key_header, policy, collect),
        BatchSource::Owned(batch) => visit_owned_batch(batch, key_header, collect),
    }
    records
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
    /// A collector for a batch that declares `records` records; see
    /// [`PREALLOCATED_IDENTITIES`] for why the declaration is only a hint.
    pub(crate) fn new(key_header: Option<Arc<str>>, base_timestamp: i64, records: usize) -> Self {
        Self {
            key_header,
            base_timestamp,
            next_index: 0,
            collected: CollectedIdentities {
                records: Vec::with_capacity(records.min(PREALLOCATED_IDENTITIES)),
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
