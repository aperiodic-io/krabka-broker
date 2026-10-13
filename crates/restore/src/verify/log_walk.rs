//! The single pass over an archived segment's `.log`, which is where the
//! framing and CRC of every batch are checked and where the facts the archive
//! metadata cannot be trusted for are derived. It is its own file because it is
//! the only part of verification that reads record bytes, and every sidecar
//! check below it consumes the `end_offset` this pass establishes.

use {
    super::offset_as_u64,
    crate::{discover::SegmentInventory, error::RestoreError},
    krabka_ids::Offset,
    krabka_protocol::records::{RecordsError, validate_one_v2_batch},
    object_store::path::Path,
};

/// What one pass over a `.log`'s batches established.
pub(super) struct LogWalk {
    pub(super) end_offset: Offset,
    pub(super) max_timestamp_ms: i64,
    pub(super) batches: u64,
    pub(super) records: u64,
}

/// Walk every v2 record batch in `log_bytes`, checking framing and CRC, and
/// derive the segment's true `end_offset`, `max_timestamp_ms`, batch count,
/// and record count.
///
/// This checks CRC and framing only, through [`validate_one_v2_batch`]. It
/// does not also call `ValidatedBatch::validate_records` to re-verify the
/// declared `records_count` against the decoded records: the CRC already
/// guarantees every byte of the batch is exactly what the producer wrote, a
/// restore only ever forwards `records_count` as a diagnostic count rather
/// than a safety invariant a later stage relies on, and `validate_records`
/// would decompress every compressed batch a second time for no benefit this
/// pass needs.
pub(super) fn walk_log(
    key: &Path,
    segment: &SegmentInventory,
    log_bytes: &[u8],
) -> Result<LogWalk, RestoreError> {
    let mut cursor = log_bytes;
    let mut position: u64 = 0;
    let mut batches: u64 = 0;
    let mut records: u64 = 0;
    let mut max_timestamp_ms: i64 = -1;
    let mut minimum_base = segment.base_offset.0;
    let mut end_offset: Option<Offset> = None;

    while !cursor.is_empty() {
        let validated = match validate_one_v2_batch(cursor) {
            Ok(validated) => validated,
            Err(RecordsError::CrcMismatch { expected, computed }) => {
                return Err(RestoreError::ChecksumMismatch {
                    key: key.to_string(),
                    position,
                    expected,
                    computed,
                });
            }
            Err(
                RecordsError::HeaderTooShort { needed } | RecordsError::BodyTooShort { needed },
            ) => {
                return Err(truncated_at(key, position, cursor.len(), needed));
            }
            Err(other) => return Err(RestoreError::Records(other)),
        };

        let header = validated.header;
        let base_offset = header.base_offset.get();

        // The first batch must open exactly where the archive key says the
        // segment starts, and every later batch must start at or after the
        // previous one ends. Kafka compaction can leave valid offset gaps;
        // overlap or regression means the `.log` bytes and the
        // segment's own key name disagree about the segment's extent, which
        // is corruption or a mis-keyed archive; `TruncatedSegment` is reused
        // for it because it already carries the right shape ("the archive's
        // account of this segment's extent does not hold up"), even though
        // its rendered message text ("ends inside a batch ... N declared, M
        // available") was written for the framing case below and reads
        // awkwardly here. A dedicated variant would fit better; see the
        // implementer's report.
        if end_offset.is_none() && base_offset != segment.base_offset.0 {
            return Err(RestoreError::TruncatedSegment {
                key: key.to_string(),
                position,
                declared: offset_as_u64(segment.base_offset.0),
                available: offset_as_u64(base_offset),
            });
        }
        let Some(next_base) = krabka_verified::restore_batch_step(
            minimum_base,
            base_offset,
            header.last_offset_delta.get(),
        ) else {
            return Err(RestoreError::TruncatedSegment {
                key: key.to_string(),
                position,
                declared: offset_as_u64(minimum_base),
                available: offset_as_u64(base_offset),
            });
        };

        minimum_base = next_base;
        end_offset = Some(Offset(next_base - 1));
        batches += 1;
        records =
            records.saturating_add(u64::try_from(header.records_count.get().max(0)).unwrap_or(0));
        let timestamp = header.max_timestamp.get();
        if timestamp != -1 {
            max_timestamp_ms = max_timestamp_ms.max(timestamp);
        }

        let total_len = validated.total_len;
        cursor = &cursor[total_len..];
        position = position.saturating_add(u64::try_from(total_len).unwrap_or(u64::MAX));
    }

    Ok(LogWalk {
        // An empty `.log` (no batches at all) has nothing to derive an end
        // offset from; it holds exactly what its base offset already states.
        end_offset: end_offset.unwrap_or(segment.base_offset),
        max_timestamp_ms,
        batches,
        records,
    })
}

/// Build the [`RestoreError::TruncatedSegment`] a short header or a short
/// body reports. Both `RecordsError::HeaderTooShort` and `BodyTooShort` carry
/// `needed`, the count of bytes still missing from `cursor`; `available` is
/// what `cursor` actually holds at `position`, and `declared` is what the
/// batch's framing says `cursor` needed to hold: `available + needed`.
fn truncated_at(key: &Path, position: u64, cursor_len: usize, needed: usize) -> RestoreError {
    let available = u64::try_from(cursor_len).unwrap_or(u64::MAX);
    let declared = available.saturating_add(u64::try_from(needed).unwrap_or(u64::MAX));
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position,
        declared,
        available,
    }
}
