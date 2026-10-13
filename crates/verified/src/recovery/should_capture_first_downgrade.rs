use {super::ReplayCursorDecision, crate::restore::restore_batch_step, creusot_std::prelude::*};

/// Validate one decoded replay batch and compute its exclusive next cursor.
///
/// Compacted logs may contain forward gaps. Overlap, malformed spans, batches
/// outside the captured replay bound, and an unrepresentable successor stop
/// replay without advancing.
#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => match batch {
        Some((base, last_delta)) => cursor@ < end@
            && last_delta@ >= 0
            && base@ >= cursor@
            && next_offset@ == base@ + last_delta@ + 1
            && next_offset@ > cursor@
            && next_offset@ <= end@,
        None => false,
    },
    ReplayCursorDecision::Stop => match batch {
        None => true,
        Some((base, last_delta)) => cursor@ >= end@
            || last_delta@ < 0
            || base@ < cursor@
            || base@ + last_delta@ + 1 > i64::MAX@
            || base@ + last_delta@ + 1 > end@,
    },
})]
#[must_use]
pub fn replay_batch_cursor_decision(
    cursor: i64,
    end: i64,
    batch: Option<(i64, i32)>,
) -> ReplayCursorDecision {
    if cursor >= end {
        return ReplayCursorDecision::Stop;
    }
    let Some((base, last_delta)) = batch else {
        return ReplayCursorDecision::Stop;
    };
    let Some(next) = restore_batch_step(cursor, base, last_delta) else {
        return ReplayCursorDecision::Stop;
    };
    if next > end {
        ReplayCursorDecision::Stop
    } else {
        ReplayCursorDecision::Advance(next)
    }
}

/// Capture a metadata-version downgrade snapshot only for the first downgrade
/// replay meets; a later downgrade never replaces the pending one.
#[ensures(result == (!pending_exists && is_downgrade))]
#[must_use]
pub const fn should_capture_first_downgrade(pending_exists: bool, is_downgrade: bool) -> bool {
    !pending_exists && is_downgrade
}
