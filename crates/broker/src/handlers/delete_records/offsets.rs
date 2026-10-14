//! The verified offset boundary decision a `DeleteRecords` trim makes before
//! it touches a log.

use {
    krabka_log::Offset,
    krabka_verified::{
        DeleteRecordsTrimDecision, DeleteRecordsTrimFacts, delete_records_trim_decision,
    },
};

pub(super) fn trim_decision(
    requested: i64,
    high_watermark: Offset,
    log_end: Offset,
    current_start: Offset,
    delivery_watermark: Option<Offset>,
) -> DeleteRecordsTrimDecision {
    delete_records_trim_decision(DeleteRecordsTrimFacts {
        requested,
        high_watermark: high_watermark.0,
        log_end: log_end.0,
        current_start: current_start.0,
        has_delivery_watermark: delivery_watermark.is_some(),
        delivery_watermark: delivery_watermark.unwrap_or(Offset(0)).0,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        DeleteRecordsTrimDecision::{Apply, Noop, RejectMalformed, RejectOutOfRange},
    };

    fn assert_trim_decision_case(
        requested: i64,
        hw: i64,
        leo: i64,
        start: i64,
        delivery: Option<i64>,
        expected: DeleteRecordsTrimDecision,
    ) {
        assert2::assert!(
            trim_decision(
                requested,
                Offset(hw),
                Offset(leo),
                Offset(start),
                delivery.map(Offset),
            ) == expected,
            "requested={requested}"
        );
    }

    #[yare::parameterized(
        latest = { -1, 8, 10, 2, None, Apply { frontier: 8 } },
        delivery_frontier = { 8, 8, 10, 2, Some(5), Apply { frontier: 5 } },
        stale_request = { 3, 8, 10, 5, None, Noop { frontier: 5 } },
        exact_retry = { 5, 8, 10, 5, None, Noop { frontier: 5 } },
    )]
    fn trim_decision_covers_bounds_stale_requests_and_retries(
        requested: i64,
        hw: i64,
        leo: i64,
        start: i64,
        delivery: Option<i64>,
        expected: DeleteRecordsTrimDecision,
    ) {
        assert_trim_decision_case(requested, hw, leo, start, delivery, expected);
    }

    #[yare::parameterized(
        negative_request = { -2, 8, 10, 2, None, RejectMalformed },
        high_watermark_above_log_end = { 1, 1, 0, 0, None, RejectMalformed },
        start_above_log_end = { 1, 3, 10, 4, None, RejectMalformed },
        delivery_below_start = { 1, 8, 10, 2, Some(1), RejectMalformed },
        delivery_above_request = { 8, 8, 10, 2, Some(9), Apply { frontier: 8 } },
        uncommitted_tail = { 9, 8, 10, 2, None, RejectOutOfRange },
        above_log_end = { 11, 8, 10, 2, None, RejectOutOfRange },
        maximum_request = { i64::MAX, 8, 10, 2, None, RejectOutOfRange },
    )]
    fn trim_decision_fails_closed_on_malformed_and_overflow_edges(
        requested: i64,
        hw: i64,
        leo: i64,
        start: i64,
        delivery: Option<i64>,
        expected: DeleteRecordsTrimDecision,
    ) {
        assert_trim_decision_case(requested, hw, leo, start, delivery, expected);
    }
}
