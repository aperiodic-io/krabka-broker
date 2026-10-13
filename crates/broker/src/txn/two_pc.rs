//! KIP-939 two-phase-commit (2PC) participation: pure decision cores.
//!
//! In a 2PC transaction, an *external* transaction coordinator owns the
//! commit/abort decision. That coordinator is, for example, an XA resource
//! manager or Apache Flink's sink. Kafka acts as a 2PC participant. After the
//! producer has prepared, the coordinator MUST keep the transaction completable
//! indefinitely, and it MUST NOT proactively abort the transaction on the
//! transaction timeout. Only the external coordinator, through `EndTxn`, or an
//! explicit `InitProducerId` may end it.
//!
//! ## How "this is a 2PC transaction" is encoded
//!
//! Krabka follows Apache Kafka exactly, so 2PC is NOT a new persisted field.
//! The already-persisted `TransactionTimeoutMs` encodes it as the sentinel
//! `i32::MAX`. Kafka's
//! `TransactionMetadata.isDistributedTwoPhaseCommitTxn()` is literally
//! `txnTimeoutMs == Integer.MAX_VALUE`, and `InitProducerId` resolves the
//! stored timeout to `Int.MaxValue` when `enable2Pc` is set. Because the
//! timeout round-trips through `TransactionLogValue`, the property survives
//! coordinator failover and log replay without any schema change.
//!
//! These functions are pure, and [`super::two_pc_model`] model-checks them
//! exhaustively. The live coordinator calls the same functions, so the model's
//! guarantees bind production behaviour. The callers are the idle-transaction
//! reaper in [`super::expiration`] and the `InitProducerId` handler.

use {
    super::state::TxnState,
    crate::codes::INVALID_TRANSACTION_TIMEOUT,
    krabka_verified::transaction::{IdleTransactionState, should_abort_idle_transaction},
};

/// Persisted 2PC timeout sentinel.
pub(crate) const NO_TIMEOUT_MS: i32 = krabka_verified::transaction::NO_TRANSACTION_TIMEOUT_MS;

/// The `TransactionTimeoutMs` to persist for an `InitProducerId`, or
/// `INVALID_TRANSACTION_TIMEOUT` when the request asks for a timeout that
/// Kafka refuses.
///
/// Kafka's `TransactionStateManager.validateTransactionTimeoutMs` is
/// `enableTwoPC || (txnTimeoutMs <= config.transactionMaxTimeoutMs &&
/// txnTimeoutMs > 0)`, and `TransactionCoordinator.handleInitProducerId`
/// answers `INVALID_TRANSACTION_TIMEOUT` when it fails. A valid timeout is
/// stored as the client sent it.
///
/// * `enable_2pc` gives `i32::MAX`: the external transaction manager owns the
///   commit decision, so the broker never times the transaction out. The
///   requested timeout is not read, and `transaction.max.timeout.ms` does not
///   apply (KIP-939).
///
/// # Errors
///
/// Returns `INVALID_TRANSACTION_TIMEOUT` (50) for a timeout that is not
/// positive, or that is above `max_timeout_ms`.
pub(crate) fn resolve_txn_timeout(
    enable_2pc: bool,
    requested_ms: i32,
    max_timeout_ms: i32,
) -> Result<i32, i16> {
    if enable_2pc {
        return Ok(NO_TIMEOUT_MS);
    }
    if requested_ms <= 0 || requested_ms > max_timeout_ms {
        return Err(INVALID_TRANSACTION_TIMEOUT);
    }
    Ok(requested_ms)
}

/// Reports whether this persisted timeout marks a 2PC (externally-coordinated)
/// transaction. The `i32::MAX` sentinel identifies it, exactly like
/// Kafka's `isDistributedTwoPhaseCommitTxn`.
#[must_use]
#[cfg(test)]
pub(crate) fn is_two_phase_commit(txn_timeout_ms: i32) -> bool { txn_timeout_ms == NO_TIMEOUT_MS }

/// THE safety-critical decision: should the idle-transaction reaper abort the
/// transaction in `state` as of `now_ms`? The transaction carries the persisted
/// `txn_timeout_ms` and became `Ongoing` at `start_ms`.
///
/// Returns `true` iff ALL of the following hold:
///  - the transaction is [`TxnState::Ongoing`]. Only an *open* transaction can
///    time out. The reaper never reaps `Empty` / `Prepare*` / `Complete*` /
///    `Dead`. Prepare\* is a transient commit/abort that the coordinator is
///    already driving, and a separate, much longer transactional-id expiry,
///    [`crate::txn::coordinator::expiry`], reclaims the terminal and idle
///    states, not this reaper. That sweep refuses every `Prepare*` state in
///    turn, so neither reaper takes a prepared 2PC decision away from the
///    transaction manager that owns it.
///  - it is NOT a 2PC transaction (`!is_two_phase_commit`). This is **the
///    KIP-939 guarantee**: the reaper never unilaterally aborts a prepared 2PC
///    transaction.
///  - it has been open longer than `txn_timeout_ms`
///    (`start_ms + txn_timeout_ms < now_ms`), Kafka's strict comparison in
///    `TransactionStateManager.timedOutTransactions`.
///
/// Pure and total over every persisted timeout, as Kafka is: the transaction
/// log accepts any `TransactionTimeoutMs`, and a zero or negative one is
/// compared arithmetically. A backwards clock (`now_ms <= start_ms`) never
/// aborts a transaction with a nonnegative timeout, which is every timeout
/// [`resolve_txn_timeout`] persists.
#[must_use]
pub(crate) fn should_abort_idle_txn(
    state: TxnState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    let state = if state == TxnState::Ongoing {
        IdleTransactionState::Ongoing
    } else {
        IdleTransactionState::Other
    };
    should_abort_idle_transaction(state, txn_timeout_ms, start_ms, now_ms)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::codes::INVALID_TRANSACTION_TIMEOUT, assert2::assert};

    #[test]
    fn resolve_timeout_2pc_is_sentinel_regardless_of_request() {
        // Even an out-of-range request (-5) is ignored under 2PC.
        for requested in [30_000, 0, i32::MAX, -5] {
            assert!(
                resolve_txn_timeout(true, requested, 8_000) == Ok(NO_TIMEOUT_MS),
                "{requested}"
            );
        }
    }

    /// Kafka `validateTransactionTimeoutMs`.
    #[test]
    fn resolve_timeout_refuses_what_kafka_refuses() {
        let refused = Err(INVALID_TRANSACTION_TIMEOUT);
        for (requested, want) in [
            (1, Ok(1)),
            (5_000, Ok(5_000)),
            (8_000, Ok(8_000)),
            (8_001, refused),
            (0, refused),
            (-1, refused),
            (i32::MIN, refused),
            (i32::MAX, refused),
        ] {
            assert!(
                resolve_txn_timeout(false, requested, 8_000) == want,
                "{requested}"
            );
        }
    }

    #[test]
    fn non_2pc_resolution_never_collides_with_the_sentinel() {
        use assert2::check;
        // The ceiling is far below i32::MAX, so a non-2PC transaction can
        // never accidentally look like a 2PC one.
        check!(!is_two_phase_commit(
            resolve_txn_timeout(false, 8_000, 8_000).expect("a valid timeout")
        ));
        check!(is_two_phase_commit(
            resolve_txn_timeout(true, 1, 8_000).expect("2PC")
        ));
    }

    #[test]
    fn reaper_aborts_an_expired_ongoing_non_2pc_txn() {
        // Opened at t=0 with a 60s timeout: Kafka's `start + timeout < now`
        // makes it reapable once t passes 60s.
        assert!(should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 60_001));
        assert!(should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 120_000));
    }

    #[test]
    fn reaper_spares_a_not_yet_expired_ongoing_txn() {
        // Exactly at, and one ms short of, the timeout.
        assert!(!should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 60_000));
        assert!(!should_abort_idle_txn(TxnState::Ongoing, 60_000, 0, 59_999));
        // Exactly opened "now".
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            60_000,
            1_000,
            1_000
        ));
    }

    #[test]
    fn reaper_never_touches_a_2pc_txn_even_in_the_far_future() {
        // The headline KIP-939 property at the unit level: a 2PC (sentinel
        // timeout) Ongoing transaction is never reaped, no matter how much time
        // has elapsed — including a `now_ms` past `i32::MAX` ms.
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            NO_TIMEOUT_MS,
            0,
            i64::MAX
        ));
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            NO_TIMEOUT_MS,
            0,
            60_000
        ));
    }

    #[test]
    fn reaper_only_acts_on_ongoing() {
        for state in [
            TxnState::Empty,
            TxnState::PrepareCommit,
            TxnState::PrepareAbort,
            TxnState::CompleteCommit,
            TxnState::CompleteAbort,
            TxnState::Dead,
        ] {
            assert!(
                !should_abort_idle_txn(state, 60_000, 0, i64::MAX),
                "{state:?} must never be reaped by the idle-txn timeout"
            );
        }
    }

    #[test]
    fn reaper_is_robust_to_a_backwards_clock() {
        // now < start (clock skew) must not underflow into a spurious abort.
        assert!(!should_abort_idle_txn(
            TxnState::Ongoing,
            60_000,
            100_000,
            0
        ));
    }
}
