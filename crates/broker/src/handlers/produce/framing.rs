//! Header-only framing of a `Produce` request, and the request decode that
//! builds it.
//!
//! The framing keeps each partition's records field as the bytes the producer
//! sent, so the hot path can decide verbatim passthrough per partition instead
//! of materializing owned records for the whole request.

use {
    crate::error::BrokerError,
    bytes::Bytes,
    krabka_protocol::{
        Decode,
        owned::produce_request::ProduceRequest,
        primitives::uuid::Uuid as WireUuid,
        records::{Attributes, RecordsPayload, count_records_in_v2_batches, produce_framing},
    },
};

/// One partition's records, as they arrived on the wire and BEFORE any owned
/// decode or decompression.
///
/// The verbatim hot path keeps the producer's exact bytes here. The owned
/// legacy path carries an already-decoded payload.
pub(super) enum PartitionPayload {
    /// v≥3 native records bytes captured zero-copy from the request frame.
    /// The value is a refcount view and not a copy. Nothing has validated or
    /// decompressed it yet. The per-partition dispatch validates the header
    /// and record structure, then decides between verbatim and owned.
    Slice(Bytes),
    /// Legacy v0-2 payload, or any pre-decoded payload. It always takes the
    /// owned path. The handler up-converts a v0/v1 `MessageSet` and never
    /// passes it through.
    Owned(RecordsPayload),
    /// Wire-null records field → `INVALID_REQUEST`.
    Null,
}

impl PartitionPayload {
    /// Records-field wire length in bytes.
    ///
    /// For the owned form it is `RecordsPayload::payload_len`. For the
    /// verbatim form it is the slice's own length. The KIP-13 bytes-in metrics
    /// and the producer byte-rate quota both use it.
    pub(super) fn payload_len(&self) -> usize {
        match self {
            Self::Slice(b) => b.len(),
            Self::Owned(p) => p.payload_len(),
            Self::Null => 0,
        }
    }

    /// Wire length of the largest single record batch in the field.
    ///
    /// This is what `max.message.bytes` bounds. Kafka measures each batch on
    /// its own -- `RecordBatch.sizeInBytes()`, header included -- so a field
    /// carrying several small batches is not the sum of them.
    ///
    /// A verbatim slice is walked one v2 batch header at a time, reading only
    /// each header's `batch_length`. Nothing is decompressed and no CRC is
    /// verified, which is the point: an oversized batch is refused before the
    /// broker spends anything on it. A slice that is not v2, or whose walk
    /// hits a malformed header, contributes the bytes that are left, so a
    /// junk payload is still measured and `prepare_batch` still gets to
    /// reject it as malformed when it is small enough to reach that far.
    ///
    /// The owned form is one batch by construction --
    /// `owned_decode::decode_owned_batch` up-converts a whole v0/v1
    /// `MessageSet` into a single v2 batch and refuses a v2 sequence that is
    /// not exactly one batch -- so its whole payload length is that batch.
    pub(super) fn largest_batch_len(&self) -> usize {
        match self {
            Self::Slice(b) => largest_v2_batch_len(b),
            Self::Owned(p) => p.payload_len(),
            Self::Null => 0,
        }
    }

    /// Number of records across the field's batches, for `messages_in_total`.
    ///
    /// Verbatim slices read each v2 batch header's `records_count` WITHOUT
    /// decompression. Owned payloads sum `records.len()` over their v2
    /// batches.
    pub(super) fn message_count(&self) -> u64 {
        match self {
            Self::Slice(b) => count_records_in_v2_batches(b),
            Self::Owned(p) => p.as_v2().map_or(0, |batches| {
                batches.iter().map(|b| b.records.len() as u64).sum()
            }),
            Self::Null => 0,
        }
    }

    /// Whether any v2 batch in this field carries the KIP-98 transactional
    /// attribute bit.
    ///
    /// This is Kafka's `RequestUtils.hasTransactionalRecords` per partition:
    /// the ACL preamble needs it for every partition of the request, before
    /// any batch is otherwise decoded, so it peeks each v2 batch header's
    /// `attributes` field the same way `largest_batch_len` peeks
    /// `batch_length` -- no CRC check, no decompression, no record body
    /// read.
    pub(super) fn has_transactional_batch(&self) -> bool {
        match self {
            Self::Slice(b) => any_v2_batch_transactional(b),
            Self::Owned(p) => p
                .as_v2()
                .is_some_and(|batches| batches.iter().any(|b| b.attributes.is_transactional())),
            Self::Null => false,
        }
    }
}

/// Offset of the 2-byte `attributes` field in a v2 batch header, counted from
/// the header's start (`base_offset`). `MAGIC_OFFSET` (the `magic` byte) is
/// followed by the 4-byte `crc`, then `attributes`.
const ATTRIBUTES_OFFSET: usize = MAGIC_OFFSET + 1 + 4;

/// Whether any v2 batch in `buf` carries the transactional attribute bit.
///
/// Walks the same header sequence `largest_v2_batch_len` walks, stopping at
/// the same malformed or truncated tail: such a tail cannot decode as a
/// transactional batch downstream either, since `prepare_batch` rejects it
/// before an append is possible. A slice that is not v2 at all (a legacy
/// `MessageSet`) is never transactional -- the attribute predates neither the
/// idempotent producer nor v2 batches.
fn any_v2_batch_transactional(buf: &[u8]) -> bool {
    if buf.len() <= MAGIC_OFFSET || buf[MAGIC_OFFSET] != 2 {
        return false;
    }
    let mut remaining = buf;
    while let Some(SplitBatch { batch, tail }) = complete_v2_batch(remaining) {
        let attributes =
            i16::from_be_bytes([batch[ATTRIBUTES_OFFSET], batch[ATTRIBUTES_OFFSET + 1]]);
        if Attributes(attributes).is_transactional() {
            return true;
        }
        remaining = tail;
    }
    false
}

/// Offset of the magic byte in a v2 batch header. Only magic 2 carries the
/// `batch_length` layout this walk reads.
const MAGIC_OFFSET: usize = 16;
/// Bytes of `base_offset` and `batch_length` that precede what `batch_length`
/// itself counts. Kafka calls the pair `Records.LOG_OVERHEAD`.
const LOG_OVERHEAD: usize = 12;
/// Bytes in a complete v2 batch header.
pub(super) const V2_HEADER_LEN: usize = 61;

/// What Kafka's `ByteBufferLogInputStream.nextBatch` finds at the front of a
/// records field.
pub(super) enum NextBatch {
    /// No whole batch is there: the field is empty, ends inside a header, or
    /// claims more bytes than it holds. Kafka's iteration ends, and
    /// `ProduceRequest.validateRecords` calls a field with no first batch an
    /// `InvalidRecordException`.
    Incomplete,
    /// A size field or a magic byte that no batch can have: Kafka throws
    /// `CorruptRecordException`, out of the very `hasNext()` that looks for
    /// the batch.
    Corrupt,
    /// A whole batch: its length in bytes, header included, and its magic.
    Batch { len: usize, magic: u8 },
}

/// Kafka's `LegacyRecord.RECORD_OVERHEAD_V0`: the smallest size field
/// `ByteBufferLogInputStream.nextBatchSize` accepts.
const MIN_RECORD_SIZE: i32 = 14;

/// Frame the first batch of `buf` from its size and magic bytes alone, in
/// Kafka's own order: no CRC, no compression, no record.
pub(super) fn next_batch(buf: &[u8]) -> NextBatch {
    let Some(size) = buf.get(8..LOG_OVERHEAD) else {
        return NextBatch::Incomplete;
    };
    let size = i32::from_be_bytes([size[0], size[1], size[2], size[3]]);
    if size < MIN_RECORD_SIZE {
        return NextBatch::Corrupt;
    }
    let Some(&magic) = buf.get(MAGIC_OFFSET) else {
        return NextBatch::Incomplete;
    };
    // Kafka reads the magic as a signed byte, so a high bit is negative.
    if magic > 2 {
        return NextBatch::Corrupt;
    }
    match usize::try_from(size)
        .ok()
        .and_then(|size| size.checked_add(LOG_OVERHEAD))
    {
        Some(len) if len <= buf.len() => NextBatch::Batch { len, magic },
        _ => NextBatch::Incomplete,
    }
}

/// Splits one complete v2 batch from the tail, stopping at the same
/// malformed or truncated boundary for both header-only walks.
fn complete_v2_batch(buf: &[u8]) -> Option<SplitBatch<'_>> {
    if buf.len() < V2_HEADER_LEN || buf[MAGIC_OFFSET] != 2 {
        return None;
    }
    let length = i32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let length = usize::try_from(length).ok()?.checked_add(LOG_OVERHEAD)?;
    (V2_HEADER_LEN..=buf.len()).contains(&length).then(|| {
        let (batch, tail) = buf.split_at(length);
        SplitBatch { batch, tail }
    })
}

/// One complete v2 batch and the bytes that follow it.
struct SplitBatch<'a> {
    batch: &'a [u8],
    tail: &'a [u8],
}

/// Length of the largest v2 batch in `buf`, header included.
///
/// Returns `buf.len()` for a slice that is not v2 at all, which is a legacy
/// `MessageSet` the owned path up-converts into one batch.
fn largest_v2_batch_len(buf: &[u8]) -> usize {
    if buf.len() <= MAGIC_OFFSET || buf[MAGIC_OFFSET] != 2 {
        return buf.len();
    }
    let mut largest = 0usize;
    let mut remaining = buf;
    while let Some(SplitBatch { batch, tail }) = complete_v2_batch(remaining) {
        largest = largest.max(batch.len());
        remaining = tail;
    }
    // A malformed or truncated tail is still bytes the producer sent, and the
    // gate must not shrink away from it.
    largest.max(remaining.len())
}

/// Header-only framing of a `ProduceRequest`.
///
/// The field names match the owned struct's field names, so the handler body
/// differs only in the records form.
pub(super) struct ProduceFramed {
    pub(super) transactional_id: Option<String>,
    pub(super) acks: i16,
    pub(super) timeout_ms: i32,
    pub(super) topic_data: Vec<FramedTopic>,
}

pub(super) struct FramedTopic {
    pub(super) name: String,
    pub(super) topic_id: WireUuid,
    pub(super) partition_data: Vec<FramedPartition>,
}

pub(super) struct FramedPartition {
    pub(super) index: i32,
    pub(super) payload: PartitionPayload,
}

impl ProduceFramed {
    /// v≥3: build from the header-only `produce_framing` walk. This function
    /// decodes and decompresses no record body.
    fn from_framing(f: krabka_protocol::records::ProduceFraming) -> Self {
        Self {
            transactional_id: f.transactional_id,
            acks: f.acks,
            timeout_ms: f.timeout_ms,
            topic_data: f
                .topics
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: WireUuid(t.topic_id.0),
                    partition_data: t
                        .partitions
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.partition,
                            payload: match p.records {
                                Some(b) => PartitionPayload::Slice(b),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// Kafka's `RequestUtils.hasTransactionalRecords(produceRequest)`: true
    /// when any batch of any partition of this request carries the
    /// transactional attribute, regardless of what `transactional_id` the
    /// request itself carries.
    ///
    /// The ACL preamble uses this, not `transactional_id.is_some()`, to
    /// decide whether the `TransactionalId` `Write` check applies at all.
    /// Gating on the request field alone would both skip the check for a
    /// transactional batch sent with no `transactional_id` -- letting a
    /// principal with plain topic `Write` write into another producer's open
    /// transaction, since `verify_transactional_produce` resolves the
    /// transaction from the batch's `producer_id` -- and wrongly run it for a
    /// request that names a `transactional_id` but carries only
    /// non-transactional batches.
    pub(super) fn has_transactional_batch(&self) -> bool {
        self.topic_data.iter().any(|topic| {
            topic
                .partition_data
                .iter()
                .any(|p| p.payload.has_transactional_batch())
        })
    }

    /// v0-2: wrap the fully-decoded legacy request. Every partition takes the
    /// owned path, because a legacy `MessageSet` up-conversion is never a
    /// passthrough.
    fn from_owned(req: ProduceRequest) -> Self {
        Self {
            transactional_id: req.transactional_id,
            acks: req.acks,
            timeout_ms: req.timeout_ms,
            topic_data: req
                .topic_data
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: t.topic_id,
                    partition_data: t
                        .partition_data
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.index,
                            payload: match p.records {
                                Some(rp) => PartitionPayload::Owned(rp),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

pub(super) fn decode_produce_request(
    request_bytes: &[u8],
    body_bytes: Bytes,
    version: i16,
) -> Result<ProduceFramed, BrokerError> {
    if !(0..3).contains(&version) {
        return Ok(ProduceFramed::from_framing(produce_framing(
            body_bytes, version,
        )?));
    }
    let mut cursor = request_bytes;
    let owned: ProduceRequest =
        krabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(
            &mut cursor,
            version,
        )?
        .into();
    Ok(ProduceFramed::from_owned(owned))
}

#[cfg(test)]
mod tests {
    use {super::*, crate::handlers::produce::test_support::encode_batch, assert2::assert};

    fn batch_with_value(len: usize) -> krabka_protocol::records::RecordBatch {
        krabka_protocol::records::RecordBatch {
            last_offset_delta: 0,
            max_timestamp: 1,
            producer_id: -1,
            records: vec![krabka_protocol::records::Record {
                offset_delta: 0,
                value: Some(Bytes::from(vec![b'x'; len])),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn largest_batch_len_measures_one_v2_batch_whole() {
        let batch = batch_with_value(100);
        let encoded = encode_batch(&batch);
        let payload = PartitionPayload::Slice(encoded.clone());
        assert!(payload.largest_batch_len() == encoded.len());
        assert!(payload.largest_batch_len() == batch.encoded_len());
    }

    #[test]
    fn largest_batch_len_takes_the_largest_of_several_batches_not_their_sum() {
        // Kafka bounds each batch on its own, so a field carrying a small
        // batch and a large one is measured as the large one.
        let small = encode_batch(&batch_with_value(10));
        let large = encode_batch(&batch_with_value(1_000));
        let mut joined = Vec::with_capacity(small.len() + large.len());
        joined.extend_from_slice(&small);
        joined.extend_from_slice(&large);
        let payload = PartitionPayload::Slice(Bytes::from(joined));
        assert!(payload.largest_batch_len() == large.len());
    }

    #[test]
    fn largest_batch_len_counts_a_truncated_tail_it_cannot_walk() {
        // The walk stops at a batch header that claims more bytes than are
        // there. Those bytes were still sent, and a gate that shrank away
        // from them would let a malformed giant through.
        let batch = encode_batch(&batch_with_value(1_000));
        let truncated = batch.slice(..batch.len() - 1);
        let payload = PartitionPayload::Slice(truncated.clone());
        assert!(payload.largest_batch_len() == truncated.len());
    }

    #[test]
    fn largest_batch_len_measures_a_non_v2_slice_whole() {
        // A legacy `MessageSet` has no v2 magic byte, and the owned path
        // up-converts the whole set into one batch.
        let legacy = Bytes::from(vec![7u8; 200]);
        let payload = PartitionPayload::Slice(legacy.clone());
        assert!(payload.largest_batch_len() == legacy.len());
    }

    #[test]
    fn largest_batch_len_is_zero_for_a_wire_null_field() {
        assert!(PartitionPayload::Null.largest_batch_len() == 0);
    }

    #[test]
    fn largest_batch_len_of_an_owned_payload_is_its_one_batch() {
        let batch = batch_with_value(100);
        let want = batch.encoded_len();
        let payload = PartitionPayload::Owned(RecordsPayload::V2(vec![batch]));
        assert!(payload.largest_batch_len() == want);
    }
}
