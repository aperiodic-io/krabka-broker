//! Shared utilities for the transaction subsystem.

krabka_macros::epoch_millis_fn!(
/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Transaction handlers use this to stamp `last_update_ms` on `TxnEntry`.
#[inline]
pub(crate) fn now_millis,
0
);

/// Request version at and above which `AddPartitionsToTxn`, `AddOffsetsToTxn`
/// and `EndTxn` carry `PRODUCER_FENCED` (90, KIP-360). Below it, Kafka's
/// `KafkaApis` downgrades that answer to the legacy `INVALID_PRODUCER_EPOCH`
/// (47).
pub(crate) const PRODUCER_FENCED_MIN_VERSION: i16 = 2;

/// Kafka `KafkaApis.handleAddOffsetsToTxnRequest` and `handleEndTxnRequest`:
/// a client below version 2 does not know `PRODUCER_FENCED`, so it gets
/// `INVALID_PRODUCER_EPOCH`. Every other code passes through.
pub(crate) fn producer_fenced_wire_code(version: i16, code: i16) -> i16 {
    if version < PRODUCER_FENCED_MIN_VERSION && code == crate::codes::PRODUCER_FENCED {
        crate::codes::INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

/// The timed transaction sweep tasks share cadence and shutdown handling.
macro_rules! reaper_task {
    ($(#[$doc:meta])* $name:ident($coord:ident, $controller:ident, $interval:ident $(, $arg:ident: $arg_type:ty)*; $shutdown:ident) => $sweep:expr; $message:literal) => {
        $(#[$doc])*
        pub(crate) async fn $name(
            $coord: std::sync::Arc<$crate::txn::coordinator::TxnCoordinator>,
            $controller: std::sync::Arc<dyn $crate::metadata_source::MetadataSource>,
            $interval: krabka_units::Time,
            $($arg: $arg_type,)*
            $shutdown: tokio_util::sync::CancellationToken,
        ) {
            use krabka_units::convert::TimeExt as _;
            let tick = tokio::time::interval($interval.to_std());
            $crate::task_util::run_every(tick, &$shutdown, || $sweep).await;
            tracing::info!($message);
        }
    };
}
pub(super) use reaper_task;

/// Independent expected wire codes shared by the utility and response tests.
#[cfg(test)]
pub(crate) fn producer_fenced_cases(other_code_version: i16) -> [(i16, i16, i16); 5] {
    [
        (
            0,
            crate::codes::PRODUCER_FENCED,
            crate::codes::INVALID_PRODUCER_EPOCH,
        ),
        (
            1,
            crate::codes::PRODUCER_FENCED,
            crate::codes::INVALID_PRODUCER_EPOCH,
        ),
        (
            2,
            crate::codes::PRODUCER_FENCED,
            crate::codes::PRODUCER_FENCED,
        ),
        (
            5,
            crate::codes::PRODUCER_FENCED,
            crate::codes::PRODUCER_FENCED,
        ),
        (
            other_code_version,
            crate::codes::CONCURRENT_TRANSACTIONS,
            crate::codes::CONCURRENT_TRANSACTIONS,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn producer_fenced_is_invalid_producer_epoch_below_version_2() {
        // (version, code, expected code on the wire)
        let cases = producer_fenced_cases(1);
        for (version, code, expected) in cases {
            check!(
                producer_fenced_wire_code(version, code) == expected,
                "v{version} {code}"
            );
        }
    }
}
