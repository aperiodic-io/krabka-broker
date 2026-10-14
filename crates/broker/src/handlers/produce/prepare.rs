//! The decision that turns one partition's records field into a
//! [`PreparedBatch`]: verbatim passthrough of the producer's own bytes, or the
//! owned-decode fallback, plus the client-origin batch-header invariants that
//! both paths apply.

use {
    super::{
        framing::{NextBatch, PartitionPayload, V2_HEADER_LEN, next_batch},
        owned_decode::decode_owned_batch,
        topic_settings::{TimestampPolicy, TimestampWindow},
    },
    crate::{
        codes,
        dedup::{CollectedIdentities, DedupSettings, IdentityCollector},
        metrics::BrokerMetrics,
        time_util::now_ms,
    },
    bytes::Bytes,
    krabka_compression::RecordDecompressionPolicy,
    krabka_protocol::{
        owned::produce_response::BatchIndexAndErrorMessage,
        records::{
            Attributes, RecordBatch, RecordsError, RecordsPayload, TimestampType, ValidatedBatch,
            validate_one_v2_batch,
        },
    },
    krabka_verified::produce::{ProduceBatchAdmission, produce_batch_admission},
    std::sync::Arc,
};

/// The topic-and-metrics context [`prepare_batch`] and [`owned_fallback`]
/// need beside the batch's own bytes: what to label the decode-time metrics
/// with, and how hard [`decode_owned_batch`] may let decompression work.
/// Bundled into one parameter so the two functions stay under Clippy's
/// argument-count lint. Every field is a reference or itself `Copy`, so this
/// is too, and passes by value like they would have.
#[derive(Clone, Copy)]
pub(super) struct DecodeEnv<'a> {
    pub(super) topic_name: &'a Arc<str>,
    pub(super) metrics: &'a BrokerMetrics,
    pub(super) policy: RecordDecompressionPolicy,
    /// The topic's KFC-10 dedup settings, when it has any. The walk that
    /// validates the records then also reads each record's identity.
    pub(super) dedup: Option<&'a DedupSettings>,
}

impl DecodeEnv<'_> {
    /// A collector for a batch with these attributes, or `None` when the
    /// topic does not deduplicate or the batch is transactional, which
    /// bypasses deduplication.
    fn dedup_collector(
        &self,
        attributes: Attributes,
        base_timestamp: i64,
        records: usize,
    ) -> Option<IdentityCollector> {
        self.dedup
            .filter(|_| !attributes.is_transactional())
            .map(|settings| {
                IdentityCollector::new(settings.key_header.clone(), base_timestamp, records)
            })
    }
}

/// All the per-batch HEADER fields that the broker's produce gates need.
///
/// The gates are the leadership epoch stamp, the transactional verify, the
/// idempotent dedup, and the `acks=-1` HW target. The struct holds these
/// fields without materializing owned records. On the verbatim path they come
/// from the v2 batch header through
/// [`validate_one_v2_batch`]. On the owned fallback they come from the decoded
/// [`RecordBatch`] header. The values are identical on both paths.
#[derive(Debug)]
pub(super) struct PreparedBatch {
    pub(super) attributes: Attributes,
    pub(super) last_offset_delta: i32,
    pub(super) max_timestamp: i64,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) base_sequence: i32,
    /// The index in the batch of every record that has no key, when the topic
    /// is compacted. Kafka's `LogValidator.validateKey` refuses each one, and
    /// the pipeline answers the batch with one `record_errors` row per index
    /// once the leadership gate has passed. Empty on a topic that is not
    /// compacted, because the walk does not look at keys there.
    pub(super) keyless_records: Vec<i32>,
    /// One `record_errors` row per record whose timestamp Kafka's
    /// `LogValidator.validateTimestamp` refuses: the topic's
    /// `message.timestamp.{before,after}.max.ms` window, checked only on the
    /// records the compacted-key walk above did not already refuse (Kafka
    /// checks a record's key first and its timestamp only when the key
    /// passed). Empty on a topic whose window bounds nothing. The pipeline
    /// answers the batch with these once the leadership gate has passed, the
    /// same as [`Self::keyless_records`].
    pub(super) invalid_timestamp_records: Vec<BatchIndexAndErrorMessage>,
    /// The record identities of a batch on a KFC-10 dedup topic, read during
    /// the same walk. `None` when the topic does not deduplicate, or the
    /// batch is transactional. A record that lacks the header
    /// `dedup.key.header` names is listed in `missing_header`, and the
    /// pipeline refuses the batch with it.
    pub(super) dedup: Option<CollectedIdentities>,
    /// The append source. It is either the producer's verbatim bytes on the
    /// passthrough path, or the decoded owned batch on the fallback path. On
    /// the verbatim path the writer stamps the leader epoch at append time. On
    /// the owned path the code below stamps it onto the owned batch.
    pub(super) source: PreparedSource,
}

#[derive(Debug)]
pub(super) enum PreparedSource {
    /// Validated, single, CRC-checked v2 batch. The writer appends the
    /// producer's exact bytes after every declared record was parsed.
    Verbatim(Bytes),
    /// Decoded owned batch. This is the complete fallback path. When the
    /// producer compressed the batch, `RecordBatch::decode` decompressed it
    /// here.
    Owned(RecordBatch),
}

impl PreparedBatch {
    fn from_header(
        header: ValidatedHeader,
        bytes: Bytes,
        (keyless_records, invalid_timestamp_records): (Vec<i32>, Vec<BatchIndexAndErrorMessage>),
        dedup: Option<CollectedIdentities>,
    ) -> Self {
        Self {
            keyless_records,
            invalid_timestamp_records,
            dedup,
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            source: PreparedSource::Verbatim(bytes),
        }
    }

    /// Walks `batch.records` once, in index order, and sorts each one into
    /// [`Self::keyless_records`] or [`Self::invalid_timestamp_records`] the
    /// way Kafka's `LogValidator.validateRecord` does: the key first, on a
    /// compacted topic, and the timestamp window only for a record whose key
    /// passed that check (or every record, off a compacted topic).
    fn from_owned(
        batch: RecordBatch,
        compacted_topic: bool,
        timestamps: TimestampPolicy,
        env: &DecodeEnv<'_>,
    ) -> Self {
        let now_ms = timestamps.bounds_records().then(now_ms);
        let mut keyless_records = Vec::new();
        let mut invalid_timestamp_records = Vec::new();
        let mut dedup =
            env.dedup_collector(batch.attributes, batch.base_timestamp, batch.records.len());
        for (index, record) in batch.records.iter().enumerate() {
            if let Some(dedup) = &mut dedup {
                dedup.visit_owned(record);
            }
            let index = i32::try_from(index).unwrap_or(i32::MAX);
            if compacted_topic && record.key.is_none() {
                keyless_records.push(index);
                continue;
            }
            let Some(now_ms) = now_ms else { continue };
            let timestamp = batch.base_timestamp.saturating_add(record.timestamp_delta);
            if timestamps.rejects_record(timestamp, now_ms) {
                let offset = batch
                    .base_offset
                    .saturating_add(i64::from(record.offset_delta));
                invalid_timestamp_records.push(timestamp_error(
                    index,
                    offset,
                    timestamp,
                    timestamps.window(now_ms),
                ));
            }
        }
        Self {
            keyless_records,
            invalid_timestamp_records,
            dedup: dedup.map(IdentityCollector::finish),
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            source: PreparedSource::Owned(batch),
        }
    }

    /// Wire length of the record set the writer appends: the producer's own
    /// bytes on the verbatim path, and the measure [`Self::stored_len`] takes
    /// on the owned one. What Kafka's `UnifiedLog.append` holds against the
    /// topic's `segment.bytes`.
    pub(super) fn appended_len(&self, config: &krabka_log::LogConfig) -> usize {
        match &self.source {
            PreparedSource::Verbatim(bytes) => bytes.len(),
            PreparedSource::Owned(_) => self
                .stored_len(config.compression_type, Some(config))
                .unwrap_or(0),
        }
    }

    /// Wire length of this batch as the writer will store it, when storing it
    /// means encoding it afresh.
    ///
    /// `None` is the verbatim path. Those bytes are the producer's own, byte
    /// for byte, so the length that arrived is the length that lands and the
    /// `max.message.bytes` gate already measured it before `prepare_batch`
    /// ran.
    ///
    /// The owned path re-encodes, and re-encoding moves the number. A batch
    /// the producer compressed that the topic stores under a different
    /// `compression.type` changes by the whole ratio between the two codecs,
    /// and `uncompressed` is the direction that grows: a 2 KiB gzip batch of
    /// repeated bytes is hundreds of kilobytes once the writer expands it. A
    /// legacy `MessageSet` moves too, by the v2 up-conversion.
    ///
    /// Kafka measures exactly this, in the same place. `message.max.bytes` is
    /// documented in `ServerConfigs` as "The largest record batch size allowed
    /// by Kafka (after compression if compression is enabled)", and
    /// `UnifiedLog.append` re-runs its per-batch size check over the
    /// *validated* records whenever `LogValidator` reports
    /// `messageSizeMaybeChanged`, throwing the same `RecordTooLargeException`
    /// its pre-append check throws.
    ///
    /// `None` also answers an encode this measurement cannot perform, because
    /// that is an encode the writer cannot perform either: the append fails on
    /// its own and reports its own error rather than borrowing this gate's.
    pub(super) fn stored_len(
        &self,
        topic_compression: Option<krabka_compression::CompressionType>,
        config: Option<&krabka_log::LogConfig>,
    ) -> Option<usize> {
        let PreparedSource::Owned(batch) = &self.source else {
            return None;
        };
        match topic_compression {
            Some(target) if target != batch.attributes.compression() => {
                let mut stored = batch.clone();
                stored.attributes = stored.attributes.with_compression(target);
                encoded_len(&stored, config)
            }
            _ => encoded_len(batch, config),
        }
    }
}

/// Bytes that [`RecordBatch::encode`] writes, which for a compressed batch
/// only an encode can answer.
fn encoded_len(batch: &RecordBatch, config: Option<&krabka_log::LogConfig>) -> Option<usize> {
    let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
    let level = config.and_then(|config| config.compression_level(batch.attributes.compression()));
    batch
        .encode_with_compression_level(&mut buf, level)
        .ok()
        .map(|()| buf.len())
}

/// Decide the append shape for one partition's records and extract the header
/// fields that the gates need without materializing owned records on the
/// verbatim path.
///
/// The verbatim-passthrough predicate holds only when ALL of these hold. It
/// matches the writer's recompression gate exactly:
///   1. the records are a v≥3 native-v2 slice, not legacy and not a wire-null
///      field;
///   2. the slice is exactly one complete, CRC-valid v2 batch whose body
///      contains exactly the declared structurally valid records;
///   3. `timestamp_type == CreateTime`; a client-supplied log-append-time
///      batch is invalid;
///   4. there is no broker-side recompression. The topic's `compression.type`
///      is `producer` pass-through, which is `None`, OR it equals the batch's
///      own codec.
///
/// On any miss the function decodes the records into an owned `RecordBatch`.
/// That is the complete fallback. The verbatim path transiently decompresses
/// compressed bodies only to validate their record structure, then discards
/// that buffer and retains the original compressed wire bytes.
/// [`decode_owned_batch`] up-converts the legacy v0-2 payloads.
///
/// The function returns the response error *code* on a bad field.
pub(super) fn prepare_batch(
    payload: PartitionPayload,
    topic_compression: Option<krabka_compression::CompressionType>,
    timestamps: TimestampPolicy,
    compacted_topic: bool,
    env: DecodeEnv<'_>,
    version: i16,
) -> Result<PreparedBatch, i16> {
    let DecodeEnv {
        topic_name,
        metrics,
        policy,
        ..
    } = env;
    let bytes = match payload {
        // Legacy / pre-decoded payload: always owned.
        PartitionPayload::Owned(rp) => {
            let batch = decode_owned_batch(rp, topic_name, metrics, policy)?;
            validate_owned_client_batch(&batch, version)?;
            return Ok(PreparedBatch::from_owned(
                batch,
                compacted_topic,
                timestamps,
                &env,
            ));
        }
        PartitionPayload::Null => return Err(codes::INVALID_REQUEST),
        PartitionPayload::Slice(b) => b,
    };

    // Extract the header fields into owned values up front so the borrow of
    // `bytes` (via the `ValidatedBatch`) ends before the final
    // `Verbatim(bytes)` move.
    let validated = match validate_one_v2_batch(&bytes) {
        Ok(batch) if batch.total_len == bytes.len() => batch,
        _ => {
            return owned_fallback(&bytes, timestamps, compacted_topic, env, version);
        }
    };
    let header = ValidatedHeader::from(&validated);
    let attributes = header.attributes;
    validate_client_batch_header(header, version)?;

    // (4) No recompression: producer pass-through, or target == current codec.
    if let Some(target) = topic_compression
        && target != attributes.compression()
    {
        return owned_fallback(&bytes, timestamps, compacted_topic, env, version);
    }
    let mut keyless_records = Vec::new();
    let mut invalid_timestamp_records = Vec::new();
    let mut dedup = env.dedup_collector(
        attributes,
        header.base_timestamp,
        usize::try_from(header.records_count).unwrap_or(0),
    );
    if timestamps.bounds_records() || compacted_topic || dedup.is_some() {
        let now_ms = now_ms();
        let mut index = 0_i32;
        validated
            .validate_records_with(policy, |record| {
                if let Some(dedup) = &mut dedup {
                    dedup.visit_borrowed(record);
                }
                // Kafka's `LogValidator.validateRecord` checks the key first
                // and checks the timestamp only of a record whose key passed.
                if compacted_topic && record.key.is_none() {
                    keyless_records.push(index);
                } else {
                    let timestamp = header.base_timestamp.saturating_add(record.timestamp_delta);
                    if timestamps.rejects_record(timestamp, now_ms) {
                        let offset = header
                            .base_offset
                            .saturating_add(i64::from(record.offset_delta));
                        invalid_timestamp_records.push(timestamp_error(
                            index,
                            offset,
                            timestamp,
                            timestamps.window(now_ms),
                        ));
                    }
                }
                index = index.saturating_add(1);
            })
            .map_err(|_| codes::INVALID_RECORD)?;
    } else {
        validated
            .validate_records(policy)
            .map_err(|_| codes::INVALID_RECORD)?;
    }
    Ok(PreparedBatch::from_header(
        header,
        bytes,
        (keyless_records, invalid_timestamp_records),
        dedup.map(IdentityCollector::finish),
    ))
}

/// The per-record message of Kafka's `LogValidator.validateTimestamp`, for one
/// `record_errors` row: `"Timestamp {ts} of message with offset {offset} is
/// out of range. The timestamp should be within [{low}, {high}]"`.
fn timestamp_error(
    index: i32,
    offset: i64,
    timestamp: i64,
    window: TimestampWindow,
) -> BatchIndexAndErrorMessage {
    BatchIndexAndErrorMessage {
        batch_index: index,
        batch_index_error_message: Some(invalid_timestamp_message(offset, timestamp, window)),
        ..Default::default()
    }
}

fn invalid_timestamp_message(offset: i64, timestamp: i64, window: TimestampWindow) -> String {
    format!(
        "Timestamp {timestamp} of message with offset {offset} is out of range. The timestamp \
         should be within [{}, {}]",
        window.low, window.high
    )
}

/// The owned-decode fallback for a v≥3 records slice that the verbatim
/// predicate rejects.
///
/// Routes the raw field bytes through `RecordsPayload::from_bytes` — which
/// dispatches v2 (parse every batch) vs legacy (v0/v1 `MessageSet`, kept
/// opaque) by the magic byte — then through [`decode_owned_batch`], the same
/// pipeline the request decoder used before the verbatim path existed. This is
/// what up-converts a v1 `MessageSet` carried over a v≥3 produce (older
/// message-format clients) and surfaces `CORRUPT_MESSAGE` or `INVALID_RECORD` on
/// malformed bytes, whichever [`decode_failure_code`] says Kafka answers.
pub(super) fn owned_fallback(
    bytes: &Bytes,
    timestamps: TimestampPolicy,
    compacted_topic: bool,
    env: DecodeEnv<'_>,
    version: i16,
) -> Result<PreparedBatch, i16> {
    let DecodeEnv {
        topic_name,
        metrics,
        policy,
        ..
    } = env;
    // Kafka frames a field before it decodes any of it, and a size or a magic
    // that no batch can have is `CorruptRecordException` however the field
    // would have decoded, a legacy `MessageSet` included.
    if matches!(next_batch(bytes), NextBatch::Corrupt) {
        return Err(codes::CORRUPT_MESSAGE);
    }
    match RecordsPayload::from_bytes_with_policy(bytes.clone(), policy) {
        Ok(rp) => decode_owned_batch(rp, topic_name, metrics, policy).and_then(|batch| {
            validate_owned_client_batch(&batch, version)?;
            Ok(PreparedBatch::from_owned(
                batch,
                compacted_topic,
                timestamps,
                &env,
            ))
        }),
        Err(error) => Err(decode_failure_code(&error, bytes)),
    }
}

/// The row code of a records field that failed to decode.
///
/// Kafka answers `CORRUPT_MESSAGE` for what it raises as
/// `CorruptRecordException`, and the JVM producer retries it: a size field or
/// magic byte that no batch can have (`ByteBufferLogInputStream
/// .nextBatchSize`, which `ProduceRequest.validateRecords` runs), a batch too
/// short to hold a v2 header, and a batch whose CRC does not match
/// (`UnifiedLog.analyzeAndValidateRecords`, `!batch.isValid()`). What is left
/// is `InvalidRecordException`, `INVALID_RECORD`, which no producer retries:
/// no complete batch, a magic below 2, more than one batch, and a batch whose
/// CRC holds but whose records do not parse.
///
/// Kafka frames the batches before the log checks a CRC, so a second batch
/// decides the answer whatever the first one's CRC is. [`owned_fallback`]
/// has already answered a first batch that cannot be framed.
fn decode_failure_code(error: &RecordsError, records: &[u8]) -> i16 {
    let NextBatch::Batch { len, magic } = next_batch(records) else {
        return codes::INVALID_RECORD;
    };
    if magic != 2 {
        return codes::INVALID_RECORD;
    }
    match next_batch(&records[len..]) {
        NextBatch::Corrupt => return codes::CORRUPT_MESSAGE,
        NextBatch::Batch { .. } => return codes::INVALID_RECORD,
        NextBatch::Incomplete => {}
    }
    if len < V2_HEADER_LEN || matches!(error, RecordsError::CrcMismatch { .. }) {
        codes::CORRUPT_MESSAGE
    } else {
        codes::INVALID_RECORD
    }
}

/// The v2 batch header fields that the gates need, copied out of a borrowed
/// [`ValidatedBatch`] so that the code can move the verbatim `Bytes`
/// afterward.
#[derive(Debug, Clone, Copy)]
struct ValidatedHeader {
    base_offset: i64,
    base_timestamp: i64,
    attributes: Attributes,
    last_offset_delta: i32,
    records_count: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl From<&ValidatedBatch<'_>> for ValidatedHeader {
    fn from(v: &ValidatedBatch<'_>) -> Self {
        Self {
            base_offset: v.header.base_offset.get(),
            base_timestamp: v.header.base_timestamp.get(),
            attributes: Attributes(v.header.attributes.get()),
            last_offset_delta: v.header.last_offset_delta.get(),
            records_count: v.header.records_count.get(),
            max_timestamp: v.header.max_timestamp.get(),
            producer_id: v.header.producer_id.get(),
            producer_epoch: v.header.producer_epoch.get(),
            base_sequence: v.header.base_sequence.get(),
        }
    }
}

/// Kafka's `ProduceRequest.MIN_VERSION_FOR_ZSTD_COMPRESSION`: the first
/// `Produce` version whose consumers are assumed to understand zstd. Below
/// it, `ProduceRequest.validateRecords` refuses a zstd batch with
/// `UNSUPPORTED_COMPRESSION_TYPE` before the batch reaches any other check --
/// old clients otherwise cannot decode what they fetch back.
const FIRST_ZSTD_PRODUCE_VERSION: i16 = 7;

/// Apply Kafka's client-origin v2 batch-header invariants without decoding
/// the record body. Every field is covered by the batch CRC that
/// [`validate_one_v2_batch`] checked before this function runs.
fn validate_client_batch_header(batch: ValidatedHeader, version: i16) -> Result<(), i16> {
    validate_client_batch_fields(
        batch.attributes,
        batch.last_offset_delta,
        batch.records_count,
        batch.producer_id,
        batch.base_sequence,
        version,
    )
}

fn validate_owned_client_batch(batch: &RecordBatch, version: i16) -> Result<(), i16> {
    let records_count = i32::try_from(batch.records.len()).map_err(|_| codes::INVALID_RECORD)?;
    validate_client_batch_fields(
        batch.attributes,
        batch.last_offset_delta,
        records_count,
        batch.producer_id,
        batch.base_sequence,
        version,
    )
}

fn validate_client_batch_fields(
    attributes: Attributes,
    last_offset_delta: i32,
    records_count: i32,
    producer_id: i64,
    base_sequence: i32,
    version: i16,
) -> Result<(), i16> {
    if attributes.compression() == krabka_compression::CompressionType::Zstd
        && version < FIRST_ZSTD_PRODUCE_VERSION
    {
        return Err(codes::UNSUPPORTED_COMPRESSION_TYPE);
    }
    match produce_batch_admission(
        last_offset_delta,
        records_count,
        attributes.is_control_batch(),
        producer_id,
        base_sequence,
        attributes.timestamp_type() == TimestampType::CreateTime,
    ) {
        ProduceBatchAdmission::Admit => Ok(()),
        ProduceBatchAdmission::InvalidRecord => Err(codes::INVALID_RECORD),
        ProduceBatchAdmission::InvalidTimestamp => Err(codes::INVALID_TIMESTAMP),
    }
}

#[cfg(test)]
mod tests;
