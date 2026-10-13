use {
    super::{DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts},
    creusot_std::prelude::*,
};

/// Admit a trim and cap it at every logical deletion frontier.
///
/// `-1` means the current high watermark, which is always admitted. An
/// explicit request above the high watermark is refused
/// `RejectOutOfRange`, matching `UnifiedLog.maybeIncrementLogStartOffset`
/// on Kafka trunk: an explicit offset never enters the uncommitted tail,
/// even when it is still below the log end offset. A scheduled topic adds
/// its delivery watermark as a second cap on top of the high watermark.
/// Stale and repeated requests return the current start and never move it
/// backwards.
#[must_use]
#[ensures({
    let malformed = facts.requested@ < -1
        || facts.current_start@ < 0
        || facts.high_watermark@ < facts.current_start@
        || facts.log_end@ < facts.high_watermark@
        || (facts.has_delivery_watermark
            && facts.delivery_watermark@ < facts.current_start@);
    let out_of_range = !malformed
        && facts.requested@ != -1
        && facts.requested@ > facts.high_watermark@;
    let resolved = if facts.requested@ == -1 {
        facts.high_watermark@
    } else {
        facts.requested@
    };
    let bounded = if facts.has_delivery_watermark
        && facts.delivery_watermark@ < resolved
    {
        facts.delivery_watermark@
    } else {
        resolved
    };
    match result {
        DeleteRecordsTrimDecision::RejectMalformed => malformed,
        DeleteRecordsTrimDecision::RejectOutOfRange => out_of_range,
        DeleteRecordsTrimDecision::Noop { frontier } => {
            !malformed && !out_of_range
                && bounded <= facts.current_start@
                && frontier@ == facts.current_start@
        }
        DeleteRecordsTrimDecision::Apply { frontier } => {
            !malformed && !out_of_range
                && bounded > facts.current_start@
                && frontier@ == bounded
                && frontier@ <= facts.high_watermark@
                && frontier@ <= facts.log_end@
                && (!facts.has_delivery_watermark
                    || frontier@ <= facts.delivery_watermark@)
        }
    }
})]
pub const fn delete_records_trim_decision(
    facts: DeleteRecordsTrimFacts,
) -> DeleteRecordsTrimDecision {
    if facts.requested < -1
        || facts.current_start < 0
        || facts.high_watermark < facts.current_start
        || facts.log_end < facts.high_watermark
        || (facts.has_delivery_watermark && facts.delivery_watermark < facts.current_start)
    {
        return DeleteRecordsTrimDecision::RejectMalformed;
    }
    if facts.requested != -1 && facts.requested > facts.high_watermark {
        return DeleteRecordsTrimDecision::RejectOutOfRange;
    }
    let resolved = if facts.requested == -1 {
        facts.high_watermark
    } else {
        facts.requested
    };
    let bounded = if facts.has_delivery_watermark {
        if resolved < facts.delivery_watermark {
            resolved
        } else {
            facts.delivery_watermark
        }
    } else {
        resolved
    };
    if bounded <= facts.current_start {
        DeleteRecordsTrimDecision::Noop {
            frontier: facts.current_start,
        }
    } else {
        DeleteRecordsTrimDecision::Apply { frontier: bounded }
    }
}

/// Choose one monotonic trim step, with WAL ordered before the local log.
///
/// Re-evaluating this function after a failed step is retry-safe: the chosen
/// frontier is the maximum of the request and both observed frontiers, so no
/// retry can regress either store. Completion requires exact equality.
#[must_use]
#[ensures({
    let frontier = if requested@ > wal_start@ {
        if requested@ > local_start@ { requested@ } else { local_start@ }
    } else if wal_start@ > local_start@ {
        wal_start@
    } else {
        local_start@
    };
    match result {
        DeleteRecordsTrimApplication::RejectMalformed => {
            requested@ < 0 || wal_start@ < 0 || local_start@ < 0
        }
        DeleteRecordsTrimApplication::TrimWal { frontier: next } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ < frontier && next@ == frontier
        }
        DeleteRecordsTrimApplication::TrimLocal { frontier: next } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ == frontier && local_start@ < frontier
                && next@ == frontier
        }
        DeleteRecordsTrimApplication::Complete { frontier: done } => {
            requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0
                && wal_start@ == frontier && local_start@ == frontier
                && done@ == frontier
        }
    }
})]
pub const fn delete_records_trim_application(
    requested: i64,
    wal_start: i64,
    local_start: i64,
) -> DeleteRecordsTrimApplication {
    if requested < 0 || wal_start < 0 || local_start < 0 {
        return DeleteRecordsTrimApplication::RejectMalformed;
    }
    let request_or_wal = if requested > wal_start {
        requested
    } else {
        wal_start
    };
    let frontier = if request_or_wal > local_start {
        request_or_wal
    } else {
        local_start
    };
    if wal_start < frontier {
        DeleteRecordsTrimApplication::TrimWal { frontier }
    } else if local_start < frontier {
        DeleteRecordsTrimApplication::TrimLocal { frontier }
    } else {
        DeleteRecordsTrimApplication::Complete { frontier }
    }
}

/// Non-negative KIP-932 backlog above the effective share start offset.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[cfg_attr(test, mutants::skip)]
pub fn effective_share_backlog_model(hwm: i64, spso: i64, log_start: i64) -> Int {
    pearlite! {
        let base = if spso@ >= 0 && spso@ > log_start@ { spso@ } else { log_start@ };
        let difference = hwm@ - base;
        if difference <= 0 {
            0
        } else if difference > 9223372036854775807 {
            9223372036854775807
        } else {
            difference
        }
    }
}

#[ensures(result@ == effective_share_backlog_model(hwm, spso, log_start))]
#[must_use]
pub fn effective_share_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 && spso > log_start {
        spso
    } else {
        log_start
    };
    let difference = hwm.saturating_sub(base);
    if difference > 0 { difference } else { 0 }
}
