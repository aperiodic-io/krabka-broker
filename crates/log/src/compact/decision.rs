//! The pure KIP-534 retain and delete-horizon decision cores. Each function
//! decides the fate of one record from plain facts, with no file access, so
//! the stateright model, the proptest fuzz, and the production rewrite path
//! can all drive the same code.

use krabka_ids::ProducerId;
// ---------------------------------------------------------------------------
// KIP-534 pure decision cores
//
// The retain/horizon core now lives in `krabka-verified`, where its contract
// is proven with Creusot. Thin typed wrappers keep log-local `ProducerId`
// boundaries explicit while `compact_model.rs` and `core_tests` keep driving
// the exact production path.
// ---------------------------------------------------------------------------
pub(crate) use krabka_verified::{RecordMeta, RetainDecision, TxnDataState};

/// Per-batch facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchMeta {
    pub is_control: bool,
    pub producer_id: ProducerId,
    /// The batch's existing delete horizon, which is `base_timestamp` when
    /// bit 6 is set. It is `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

#[cfg(test)]
pub(crate) struct RetentionMetadata {
    pub(crate) record: RecordMeta,
    pub(crate) batch: BatchMeta,
}

#[bon::builder]
#[cfg(test)]
pub(crate) fn data_retention_metadata(
    has_key: bool,
    has_value: bool,
    existing_horizon: Option<i64>,
) -> RetentionMetadata {
    RetentionMetadata {
        record: RecordMeta { has_key, has_value },
        batch: BatchMeta {
            is_control: false,
            producer_id: ProducerId(-1),
            existing_horizon,
        },
    }
}

#[bon::builder]
#[cfg(test)]
pub(crate) fn marker_retention_metadata(
    producer_id: i64,
    existing_horizon: Option<i64>,
) -> RetentionMetadata {
    RetentionMetadata {
        record: RecordMeta {
            has_key: true,
            has_value: false,
        },
        batch: BatchMeta {
            is_control: true,
            producer_id: ProducerId(producer_id),
            existing_horizon,
        },
    }
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`. The log
/// retains the tombstone or the marker until the wall clock reaches this
/// value.
#[must_use]
#[cfg(test)]
pub(crate) const fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    krabka_verified::compute_horizon(now_ms, delete_retention_ms)
}

/// The single per-record KIP-534 retain decision.
#[must_use]
pub(crate) const fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    krabka_verified::retain_decision(
        rec,
        krabka_verified::BatchMeta {
            is_control: batch.is_control,
            producer_id: batch.producer_id.0,
            existing_horizon: batch.existing_horizon,
        },
        is_newest_for_key,
        txn,
        now_ms,
        delete_retention_ms,
    )
}

/// `build_offset_map` filter. The control-batch bug fix is here.
/// Control-batch records carry a control-type key, a commit or abort marker,
/// that must NEVER enter the dedup map. Null-key data is also never indexed.
pub(crate) fn should_index_key(key: Option<&[u8]>, is_control_batch: bool) -> bool {
    !is_control_batch && key.is_some()
}

/// Reinterpret the per-record `i64` timestamp deltas when a delete horizon
/// goes into `base_timestamp`. Each record keeps its absolute timestamp.
///
/// Only `core_tests` exercises this function. The production rewrite path
/// delegates the same arithmetic to `RecordBatch::with_delete_horizon`, so the
/// function is `dead_code` outside tests.
#[cfg(test)]
pub(crate) fn rewrite_batch_horizon(
    base_timestamp: i64,
    deltas: &[i64],
    horizon: i64,
) -> RewrittenTimestamps {
    let deltas = deltas
        .iter()
        .map(|d| base_timestamp.saturating_add(*d).saturating_sub(horizon))
        .collect();
    RewrittenTimestamps {
        base_timestamp: horizon,
        deltas,
    }
}

/// The base timestamp and per-record timestamp deltas that
/// [`rewrite_batch_horizon`] produces.
#[cfg(test)]
pub(crate) struct RewrittenTimestamps {
    pub(crate) base_timestamp: i64,
    pub(crate) deltas: Vec<i64>,
}

#[cfg(test)]
mod core_tests {
    use {super::*, assert2::check};

    fn data(has_key: bool, has_value: bool) -> RecordMeta { RecordMeta { has_key, has_value } }

    fn batch(is_control: bool, producer_id: i64, existing_horizon: Option<i64>) -> BatchMeta {
        BatchMeta {
            is_control,
            producer_id: ProducerId(producer_id),
            existing_horizon,
        }
    }

    #[yare::parameterized(
        control_marker = { "control marker key", Some(b"\x00\x00\x00\x01".as_ref()), true, false },
        null_data = { "null data key", None, false, false },
        ordinary_data = { "ordinary data key", Some(b"k".as_ref()), false, true },
    )]
    fn control_batch_key_is_never_indexed(
        name: &str,
        key: Option<&[u8]>,
        is_control: bool,
        want: bool,
    ) {
        // A control batch's key (commit/abort marker) must NOT enter the
        // dedup map, regardless of whether the key is present.
        check!(
            should_index_key(key, is_control) == want,
            "case {name}: key={key:?} is_control={is_control}"
        );
    }

    #[yare::parameterized(
        stamp_new_horizon = {
            "stamp new horizon",
            None,
            true,
            100,
            RetainDecision::SetHorizon(150),
        },
        keep_before_horizon = { "keep before horizon", Some(150), true, 149, RetainDecision::Keep },
        delete_at_horizon = { "delete at horizon", Some(150), true, 150, RetainDecision::Delete },
        delete_superseded = { "delete superseded", None, false, 100, RetainDecision::Delete },
    )]
    fn tombstone_sets_horizon_then_deletes_after_expiry(
        name: &str,
        existing_horizon: Option<i64>,
        is_newest: bool,
        now_ms: i64,
        want: RetainDecision,
    ) {
        // Keyed, null-value record (tombstone).
        check!(
            retain_decision(
                data(true, false),
                batch(false, -1, existing_horizon),
                is_newest,
                TxnDataState::NotTransactional,
                now_ms,
                50
            ) == want,
            "case {name}: horizon={existing_horizon:?} newest={is_newest} now={now_ms}"
        );
    }

    #[yare::parameterized(
        ordinary_sum = { 100, 50, 150 },
        saturates_maximum = { i64::MAX - 1, 50, i64::MAX },
        saturates_minimum = { i64::MIN + 1, -50, i64::MIN },
    )]
    fn compute_horizon_saturates_at_i64_bounds(timestamp: i64, retention: i64, expected: i64) {
        assert2::assert!(compute_horizon(timestamp, retention) == expected);
    }

    #[yare::parameterized(
        keep_while_data_survives = {
            "keep while data survives",
            None,
            TxnDataState::DataSurvives,
            100,
            RetainDecision::Keep,
        },
        stamp_after_data_gone = {
            "stamp after data gone",
            None,
            TxnDataState::DataFullyGone,
            100,
            RetainDecision::SetHorizon(150),
        },
        delete_aged_marker = {
            "delete aged marker",
            Some(150),
            TxnDataState::DataFullyGone,
            150,
            RetainDecision::Delete,
        },
    )]
    fn marker_retained_while_data_survives_then_ages(
        name: &str,
        existing_horizon: Option<i64>,
        txn_state: TxnDataState,
        now_ms: i64,
        want: RetainDecision,
    ) {
        // Control records carry a key, no value.
        check!(
            retain_decision(
                data(true, false),
                batch(true, 1000, existing_horizon),
                false,
                txn_state,
                now_ms,
                50
            ) == want,
            "case {name}: horizon={existing_horizon:?} now={now_ms}"
        );
    }

    #[yare::parameterized(
        keep_newest_keyed = { "keep newest keyed data", true, true, RetainDecision::Keep },
        drop_null_key = { "drop null key", false, true, RetainDecision::Delete },
        drop_superseded = { "drop superseded keyed data", true, false, RetainDecision::Delete },
    )]
    fn live_data_kept_nullkey_dropped(
        name: &str,
        has_key: bool,
        is_newest: bool,
        want: RetainDecision,
    ) {
        check!(
            retain_decision(
                data(has_key, true),
                batch(false, -1, None),
                is_newest,
                TxnDataState::NotTransactional,
                100,
                50
            ) == want,
            "case {name}: has_key={has_key} newest={is_newest}"
        );
    }

    #[test]
    fn rewrite_batch_horizon_preserves_absolute_timestamps() {
        let RewrittenTimestamps {
            base_timestamp: base,
            deltas,
        } = rewrite_batch_horizon(1000, &[0, 5, 20], 9999);
        // Reconstructed absolute timestamps (base + delta) must equal the
        // originals: 1000, 1005, 1020.
        let reconstructed: Vec<i64> = deltas.iter().map(|d| base + d).collect();
        assert2::assert!(base == 9999);
        assert2::assert!(reconstructed == vec![1000, 1005, 1020]);
    }
}
