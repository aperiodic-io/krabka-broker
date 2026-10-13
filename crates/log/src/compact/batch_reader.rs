//! Streaming decode of every `RecordBatch` in a sealed segment's `.log` file.
//! The offset-map pass and the rewrite pass both need it, so it sits beside
//! them instead of inside either one.

use {
    crate::{error::LogError, name, segment::Segment},
    krabka_protocol::records::RecordBatch,
    krabka_verified::compaction::{CompactionDecodeStep, compaction_decode_step},
};

/// Read every `RecordBatch` from a sealed segment. This function streams the
/// whole `.log` file directly.
///
/// It avoids `Segment::read`, because that path returns early when the
/// segment's in-memory `last_offset` is stale. A sealed segment loaded from
/// disk through `Segment::open_unscanned` has `last_offset = base_offset - 1` until a
/// tail scan fills it in, so `Segment::read(base_offset, ..)` would
/// short-circuit to an empty result.
pub(super) fn read_all_batches(seg: &Segment) -> Result<Vec<RecordBatch>, LogError> {
    let path = name::log_path(seg.dir(), seg.base_offset().0);
    let bytes = std::fs::read(&path)?;
    let mut cursor: &[u8] = &bytes;
    let mut out: Vec<RecordBatch> = Vec::new();
    loop {
        let remaining_before = cursor.len();
        if compaction_decode_step(remaining_before, false, remaining_before)
            == CompactionDecodeStep::Done
        {
            return Ok(out);
        }

        match RecordBatch::decode(&mut cursor) {
            Ok(batch) => match compaction_decode_step(remaining_before, true, cursor.len()) {
                CompactionDecodeStep::Continue => out.push(batch),
                CompactionDecodeStep::Done | CompactionDecodeStep::Corrupt => {
                    return Err(LogError::Corrupt(
                        "record batch decoder made no progress during compaction".into(),
                    ));
                }
            },
            Err(error) => return Err(LogError::Records(error)),
        }
    }
}
