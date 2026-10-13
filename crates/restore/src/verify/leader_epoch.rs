//! The strict parser for a segment's `.leader_epoch_checkpoint`. It is the one
//! archived artifact that is text rather than binary framing, and it is the
//! only one verification reads for its content rather than only for its
//! integrity, because the epoch-to-offset rows it yields are handed on to the
//! restored partition.

use {
    crate::error::RestoreError,
    krabka_ids::{LeaderEpoch, Offset},
    object_store::path::Path,
};

/// Build the [`RestoreError::TruncatedSegment`] a malformed
/// `.leader_epoch_checkpoint` row reports. `declared` is the row count the
/// checkpoint's own header line states; `available` is how many rows had
/// already parsed successfully when the malformed row was reached. The
/// checkpoint format carries no byte-length field of its own to compare
/// against, unlike the `.log` and index cases, so this is the closest
/// reading of `TruncatedSegment`'s "declared vs. available" shape that the
/// format supports.
fn checkpoint_error(key: &Path, position: u64, declared: u64, available: usize) -> RestoreError {
    RestoreError::TruncatedSegment {
        key: key.to_string(),
        position,
        declared,
        available: u64::try_from(available).unwrap_or(u64::MAX),
    }
}

/// Parse a `.leader_epoch_checkpoint`: `"0\n{n}\n"` followed by exactly `n`
/// `"{epoch} {offset}"` rows, matching the format
/// `krabka_log::LeaderEpochCheckpoint` and the broker's remote-log-manager
/// copy path both write. Rows must carry nonnegative, strictly increasing
/// epochs and start offsets within the archived segment's inclusive range.
///
/// Unlike `LeaderEpochCheckpoint::parse`, which is written for a local file a
/// process itself created and is lenient about a bad version line or a short
/// count, this rejects any deviation: the bytes here came out of an archive
/// that can be corrupt, so the version line, the count, and every row are all
/// checked strictly. The declared row count is never used to size an
/// allocation: the loop below grows a `Vec` by `push`, and it can iterate at
/// most as many times as the text actually has lines, regardless of what a
/// corrupt or hostile count line claims.
pub(super) fn parse_leader_epoch_checkpoint(
    key: &Path,
    bytes: &[u8],
    segment_base: Offset,
    segment_end: Offset,
) -> Result<Vec<(LeaderEpoch, Offset)>, RestoreError> {
    let text = std::str::from_utf8(bytes).map_err(|_| checkpoint_error(key, 0, 0, 0))?;
    let mut lines = text.split('\n');
    let mut position: u64 = 0;

    let version_line = lines.next().unwrap_or_default();
    if version_line != "0" {
        return Err(checkpoint_error(key, position, 0, 0));
    }
    position += u64::try_from(version_line.len().saturating_add(1)).unwrap_or(u64::MAX);

    let count_line = lines
        .next()
        .ok_or_else(|| checkpoint_error(key, position, 0, 0))?;
    let count: usize = count_line
        .trim()
        .parse()
        .map_err(|_| checkpoint_error(key, position, 0, 0))?;
    let declared = u64::try_from(count).unwrap_or(u64::MAX);
    position += u64::try_from(count_line.len().saturating_add(1)).unwrap_or(u64::MAX);

    let mut entries = Vec::new();
    let mut previous: Option<(i32, i64)> = None;
    for _ in 0..count {
        let line = lines
            .next()
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        let mut parts = line.split_whitespace();
        let epoch: i32 = parts
            .next()
            .and_then(|token| token.parse().ok())
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        let start_offset: i64 = parts
            .next()
            .and_then(|token| token.parse().ok())
            .ok_or_else(|| checkpoint_error(key, position, declared, entries.len()))?;
        if parts.next().is_some() {
            return Err(checkpoint_error(key, position, declared, entries.len()));
        }
        if !krabka_verified::restore_leader_epoch_entry_valid(
            previous,
            epoch,
            start_offset,
            segment_base.0,
            segment_end.0,
        ) {
            return Err(checkpoint_error(key, position, declared, entries.len()));
        }
        entries.push((LeaderEpoch(epoch), Offset(start_offset)));
        previous = Some((epoch, start_offset));
        position += u64::try_from(line.len().saturating_add(1)).unwrap_or(u64::MAX);
    }

    // The broker's writer always emits exactly `count` rows after the header,
    // so anything left beyond a single trailing empty split (the newline
    // after the last row) is itself a count mismatch.
    if lines.any(|remaining| !remaining.is_empty()) {
        return Err(checkpoint_error(key, position, declared, entries.len()));
    }

    Ok(entries)
}
