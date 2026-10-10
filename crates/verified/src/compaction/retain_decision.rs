use creusot_std::prelude::*;

use super::{BatchMeta, CompactionDecodeStep, RecordMeta, RetainDecision, TxnDataState};

/// Classify one compaction batch-stream decode step.
///
/// A decode failure can never be mistaken for end-of-stream: completion is
/// admitted only when no input remains, and a successful decode must consume
/// at least one byte.
#[ensures((result == CompactionDecodeStep::Done) == (remaining_before@ == 0))]
#[ensures((result == CompactionDecodeStep::Continue) == (remaining_before@ > 0
    && decode_succeeded
    && remaining_after@ < remaining_before@))]
#[ensures((result == CompactionDecodeStep::Corrupt) == (remaining_before@ > 0
    && (!decode_succeeded || remaining_after@ >= remaining_before@)))]
#[must_use]
pub const fn compaction_decode_step(
    remaining_before: usize,
    decode_succeeded: bool,
    remaining_after: usize,
) -> CompactionDecodeStep {
    if remaining_before == 0 {
        CompactionDecodeStep::Done
    } else if decode_succeeded && remaining_after < remaining_before {
        CompactionDecodeStep::Continue
    } else {
        CompactionDecodeStep::Corrupt
    }
}

/// The delete horizon timestamp as an unbounded integer: `now +
/// delete.retention.ms`, clamped to the `i64` range.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn compute_horizon_model(now_ms: i64, delete_retention_ms: i64) -> Int {
    pearlite! {
        if now_ms@ + delete_retention_ms@ > 9223372036854775807 {
            9223372036854775807
        } else if now_ms@ + delete_retention_ms@ < -9223372036854775807 - 1 {
            -9223372036854775807 - 1
        } else {
            now_ms@ + delete_retention_ms@
        }
    }
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`,
/// saturated at the `i64` bounds.
///
/// The tombstone or marker is retained until the wall clock reaches this
/// value.
#[ensures(result@ == compute_horizon_model(now_ms, delete_retention_ms))]
#[must_use]
pub const fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

open_logic! {
fn horizon_decision_matches(existing: Option<i64>, now: i64, retention: i64, decision: RetainDecision) -> bool {
    pearlite! { match existing {
        None => exists<h: i64> decision == RetainDecision::SetHorizon(h)
            && h@ == compute_horizon_model(now, retention),
        Some(h) => decision == if now@ >= h@ { RetainDecision::Delete } else { RetainDecision::Keep },
    } }
}
}

#[ensures(horizon_decision_matches(existing, now_ms, delete_retention_ms, result))]
const fn horizon_retention(
    existing: Option<i64>,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    match existing {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}

/// The single per-record KIP-534 retain decision.
///
/// Control batches, that is transaction commit and abort markers, are retained
/// as long as the pass reads data of their transaction. Once the pass finds no
/// data of it, the marker ages out through the delete horizon. Data records
/// dedup newest-wins. A tombstone, that is a record with a null value, ages out
/// through the delete horizon after it becomes the newest entry for its key.
#[ensures(batch.is_control && (txn == TxnDataState::DataSurvives || txn == TxnDataState::NotTransactional)
    ==> result == RetainDecision::Keep)]
#[ensures(batch.is_control && txn == TxnDataState::DataFullyGone
    ==> horizon_decision_matches(batch.existing_horizon, now_ms, delete_retention_ms, result))]
#[ensures(!batch.is_control && !rec.has_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && !is_newest_for_key ==> result == RetainDecision::Delete)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && rec.has_value
    ==> result == RetainDecision::Keep)]
#[ensures(!batch.is_control && rec.has_key && is_newest_for_key && !rec.has_value
    ==> horizon_decision_matches(batch.existing_horizon, now_ms, delete_retention_ms, result))]
#[must_use]
pub const fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => {
                horizon_retention(batch.existing_horizon, now_ms, delete_retention_ms)
            }
        };
    }
    if !rec.has_key {
        return RetainDecision::Delete;
    }
    if !is_newest_for_key {
        return RetainDecision::Delete;
    }
    if rec.has_value {
        return RetainDecision::Keep;
    }
    // Newest-for-key tombstone: age out via the delete horizon.
    horizon_retention(batch.existing_horizon, now_ms, delete_retention_ms)
}
