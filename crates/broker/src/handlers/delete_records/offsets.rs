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
        assert2::assert,
    };

    #[test]
    fn trim_decision_covers_bounds_stale_requests_and_retries() {
        let cases = [
            (-1, 8, 10, 2, None, Apply { frontier: 8 }),
            // Scheduled delivery adds a second deletion frontier.
            (8, 8, 10, 2, Some(5), Apply { frontier: 5 }),
            // A stale request and an exact retry preserve the current start.
            (3, 8, 10, 5, None, Noop { frontier: 5 }),
            (5, 8, 10, 5, None, Noop { frontier: 5 }),
        ];
        for (requested, hw, leo, start, delivery, expected) in cases {
            assert!(
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
    }

    #[test]
    fn trim_decision_fails_closed_on_malformed_and_overflow_edges() {
        for (requested, hw, leo, start, delivery, expected) in [
            (-2, 8, 10, 2, None, RejectMalformed),
            (1, 1, 0, 0, None, RejectMalformed),
            (1, 3, 10, 4, None, RejectMalformed),
            (1, 8, 10, 2, Some(1), RejectMalformed),
            (8, 8, 10, 2, Some(9), Apply { frontier: 8 }),
            // Explicit requests cannot enter the uncommitted tail, even when
            // they stay below the log end offset (Kafka:
            // `UnifiedLog.maybeIncrementLogStartOffset`).
            (9, 8, 10, 2, None, RejectOutOfRange),
            (11, 8, 10, 2, None, RejectOutOfRange),
            (i64::MAX, 8, 10, 2, None, RejectOutOfRange),
        ] {
            assert!(
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
    }
}
