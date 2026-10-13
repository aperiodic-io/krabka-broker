//! Shared utilities for the transaction subsystem.

krabka_macros::epoch_millis_fn!(
/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Transaction handlers use this to stamp `last_update_ms` on `TxnEntry`.
#[inline]
pub(crate) fn now_millis,
0
);

/// A producer's id and the epoch it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerIdentity {
    pub(crate) producer_id: ProducerId,
    pub(crate) producer_epoch: i16,
}

/// Request version at and above which `AddPartitionsToTxn`, `AddOffsetsToTxn`
/// and `EndTxn` carry `PRODUCER_FENCED` (90, KIP-360). Below it, Kafka's
/// `KafkaApis` downgrades that answer to the legacy `INVALID_PRODUCER_EPOCH`
/// (47).
pub(crate) const PRODUCER_FENCED_MIN_VERSION: i16 = 2;

/// Kafka `KafkaApis.handleAddOffsetsToTxnRequest` and `handleEndTxnRequest`:
/// a client below version 2 does not know `PRODUCER_FENCED`, so it gets
/// `INVALID_PRODUCER_EPOCH`. Every other code passes through.
pub(crate) fn producer_fenced_wire_code(version: i16, code: i16) -> i16 {
    if version < PRODUCER_FENCED_MIN_VERSION && code == PRODUCER_FENCED {
        INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

use {
    crate::codes::{INVALID_PRODUCER_EPOCH, PRODUCER_FENCED},
    krabka_log::ProducerId,
};

/// Independent expected wire codes shared by the utility and response tests.
#[cfg(test)]
pub(crate) fn producer_fenced_cases(other_code_version: i16) -> [(i16, i16, i16); 5] {
    use crate::codes::{CONCURRENT_TRANSACTIONS, INVALID_PRODUCER_EPOCH, PRODUCER_FENCED};

    [
        (0, PRODUCER_FENCED, INVALID_PRODUCER_EPOCH),
        (1, PRODUCER_FENCED, INVALID_PRODUCER_EPOCH),
        (2, PRODUCER_FENCED, PRODUCER_FENCED),
        (5, PRODUCER_FENCED, PRODUCER_FENCED),
        (
            other_code_version,
            CONCURRENT_TRANSACTIONS,
            CONCURRENT_TRANSACTIONS,
        ),
    ]
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

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
