//! The consistency checks for a segment's three sparse sidecar indexes, the
//! `.index`, the `.timeindex`, and the `.txnindex`. They share one shape --
//! fixed-width entries whose fields must be monotonic and must stay inside the
//! extent the `.log` walk established -- and the diagnostics that report a bad
//! entry by its byte position, so they sit together in one file.

use {
    super::offset_as_u64,
    crate::error::RestoreError,
    krabka_ids::Offset,
    krabka_remote_storage::index::{
        AbortedTxnIndexEntry, OffsetIndexEntry, TimeIndexEntry, parse_offset_index,
        parse_time_index, parse_txn_index,
    },
    krabka_verified::log_index::IndexEntry,
    object_store::path::Path,
};

/// Byte length of one serialized offset-index entry. `krabka_remote_storage`
/// keeps its own copy of this constant private, so it is recomputed here from
/// the zerocopy struct it mirrors.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

/// Byte length of one serialized time-index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

/// Byte length of one serialized aborted-transaction index entry. See
/// [`OFFSET_INDEX_ENTRY_LEN`].
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

/// Byte position of one fixed-width index entry, for a
/// [`RestoreError::TruncatedSegment`].
fn index_position(entry_index: usize, entry_len: usize) -> u64 {
    u64::try_from(entry_index.saturating_mul(entry_len)).unwrap_or(u64::MAX)
}

/// Build the [`RestoreError::TruncatedSegment`] one bad index entry reports.
fn index_error(
    key: &Path,
    entry_index: usize,
    entry_len: usize,
    declared: u64,
    available: u64,
) -> RestoreError {
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position: index_position(entry_index, entry_len),
        declared,
        available,
    }
}

/// Reject a trailing partial fixed-width entry. The shared remote reader is
/// intentionally lenient because Kafka may expose a preallocated index; an
/// offline restore instead requires the archived object to end on an entry
/// boundary before it trusts the sidecar.
fn require_complete_entries(
    key: &Path,
    bytes: &[u8],
    entry_len: usize,
) -> Result<(), RestoreError> {
    let remainder = bytes.len() % entry_len;
    if remainder == 0 {
        return Ok(());
    }
    Err(index_error(
        key,
        bytes.len() / entry_len,
        entry_len,
        u64::try_from(entry_len).unwrap_or(u64::MAX),
        u64::try_from(remainder).unwrap_or(u64::MAX),
    ))
}

fn index_frontier(
    key: &Path,
    entry_len: usize,
    base_offset: Offset,
    end_offset: Offset,
) -> Result<i64, RestoreError> {
    krabka_verified::restore_index_frontier(base_offset.0, end_offset.0).ok_or_else(|| {
        index_error(
            key,
            0,
            entry_len,
            offset_as_u64(base_offset.0),
            offset_as_u64(end_offset.0),
        )
    })
}

/// Check that `bytes`, parsed as a Kafka `.index`, is internally consistent
/// with the segment the `.log` walk established: `relative_offset` and
/// `position` both strictly increasing across entries, every `position` inside the
/// verified `.log`, and every `relative_offset` at or before the segment's
/// last offset.
pub(super) fn validate_offset_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
    log_bytes: u64,
) -> Result<(), RestoreError> {
    require_complete_entries(key, bytes, OFFSET_INDEX_ENTRY_LEN)?;
    let entries = parse_offset_index(bytes)?;
    let max_relative = index_frontier(key, OFFSET_INDEX_ENTRY_LEN, base_offset, end_offset)?;
    let mut previous = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let relative_offset = entry.relative_offset.get();
        let position = entry.position.get();

        if !krabka_verified::restore_offset_index_entry_valid(
            previous.map(|row: &OffsetIndexEntry| {
                IndexEntry::new(row.relative_offset.get(), row.position.get())
            }),
            relative_offset,
            position,
            max_relative,
            log_bytes,
        ) {
            return Err(index_error(
                key,
                entry_index,
                OFFSET_INDEX_ENTRY_LEN,
                offset_as_u64(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some(entry);
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.timeindex`, is internally
/// consistent: `relative_offset` strictly increases, `timestamp` never
/// decreases, and every `relative_offset` is at or before the segment's last
/// offset.
///
/// A time-index entry has no on-disk byte position to check against
/// `log_bytes`, unlike an offset-index entry, so that check does not apply
/// here; `timestamp` is checked non-decreasing in its place, the analogous
/// invariant the broker's append-only writer also holds for this index.
pub(super) fn validate_time_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    require_complete_entries(key, bytes, TIME_INDEX_ENTRY_LEN)?;
    let entries = parse_time_index(bytes)?;
    let max_relative = index_frontier(key, TIME_INDEX_ENTRY_LEN, base_offset, end_offset)?;
    let mut previous = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let timestamp = entry.timestamp.get();
        let relative_offset = entry.relative_offset.get();

        if !krabka_verified::restore_time_index_entry_valid(
            previous.map(|row: &TimeIndexEntry| {
                IndexEntry::new(row.timestamp.get(), row.relative_offset.get())
            }),
            timestamp,
            relative_offset,
            max_relative,
        ) {
            return Err(index_error(
                key,
                entry_index,
                TIME_INDEX_ENTRY_LEN,
                offset_as_u64(max_relative),
                u64::from(relative_offset),
            ));
        }

        previous = Some(entry);
    }
    Ok(())
}

/// Check that `bytes`, parsed as a Kafka `.txnindex`, is internally
/// consistent: `last_offset` (the abort marker) strictly increasing across
/// entries and inside `[base_offset, end_offset]`, `0 <= start_offset <=
/// last_offset` within each entry, and a nonnegative producer ID. An entry's
/// `start_offset` may precede `base_offset` (the transaction began before a
/// segment roll) and need not increase (transactions interleave), matching
/// Kafka's `TransactionIndex`.
pub(super) fn validate_txn_index(
    key: &Path,
    bytes: &[u8],
    base_offset: Offset,
    end_offset: Offset,
) -> Result<(), RestoreError> {
    require_complete_entries(key, bytes, TXN_INDEX_ENTRY_LEN)?;
    let entries = parse_txn_index(bytes)?;
    let segment = krabka_verified::RestoreSegmentExtent {
        base_offset: base_offset.0,
        last_offset: end_offset.0,
    };
    let mut previous_last: Option<i64> = None;

    for (entry_index, entry) in entries.iter().enumerate() {
        let entry = krabka_verified::RestoreAbortedTxn {
            producer_id: entry.producer_id.get(),
            start_offset: entry.start_offset.get(),
            last_offset: entry.last_offset.get(),
        };

        if !krabka_verified::restore_txn_index_entry_valid(previous_last, entry, segment) {
            return Err(index_error(
                key,
                entry_index,
                TXN_INDEX_ENTRY_LEN,
                offset_as_u64(end_offset.0),
                offset_as_u64(entry.last_offset.max(entry.start_offset)),
            ));
        }

        previous_last = Some(entry.last_offset);
    }
    Ok(())
}
