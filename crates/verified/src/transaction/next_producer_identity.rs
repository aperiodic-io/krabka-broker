#[cfg(creusot)]
use super::first_unstable_frontier;
use {
    super::{IdleTransactionState, NO_TRANSACTION_TIMEOUT_MS},
    creusot_std::prelude::*,
};

/// Select the first unstable transaction offset, or the log end when no
/// transaction is open. A pending start beyond the log end is rejected.
///
/// `Some` and `None` partition the inputs: the result is `Some` exactly when
/// every start is at or below the log end.
#[ensures(match result {
    Some(lso) => first_unstable_frontier(starts@, log_end@, lso@),
    None => exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > log_end@,
})]
#[must_use]
pub fn first_unstable_offset(starts: &[i64], log_end: i64) -> Option<i64> {
    let mut lso = log_end;
    let mut index = 0usize;
    #[invariant(index@ <= starts@.len())]
    #[invariant(lso@ <= log_end@)]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> starts@[i]@ <= log_end@)]
    #[invariant(index@ == 0 ==> lso@ == log_end@)]
    #[invariant(index@ > 0 ==> exists<i: Int> 0 <= i && i < index@ && lso@ == starts@[i]@)]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> lso@ <= starts@[i]@)]
    #[variant(starts@.len() - index@)]
    while index < starts.len() {
        let start = starts[index];
        if start > log_end {
            return None;
        }
        if start < lso {
            lso = start;
        }
        index += 1;
    }
    Some(lso)
}

/// A valid COMMIT or ABORT marker closes state only for its matching pending
/// producer.
#[ensures(result == ((is_abort || is_commit) && !(is_abort && is_commit) && has_pending))]
#[must_use]
pub fn transaction_marker_closes(is_abort: bool, is_commit: bool, has_pending: bool) -> bool {
    (is_abort || is_commit) && !(is_abort && is_commit) && has_pending
}

/// Construct one aborted transaction's inclusive interval only from a live,
/// nonnegative producer and ordered marker bounds.
#[ensures(match result {
    Some((start, last)) => producer_id@ >= 0
        && pending_start == Some(start)
        && last@ == marker_last@
        && start@ <= last@,
    None => producer_id@ < 0
        || pending_start == None
        || match pending_start { Some(start) => start@ > marker_last@, None => false },
})]
#[must_use]
pub fn aborted_transaction_interval(
    pending_start: Option<i64>,
    marker_last: i64,
    producer_id: i64,
) -> Option<(i64, i64)> {
    if producer_id < 0 {
        return None;
    }
    let start = pending_start?;
    if start > marker_last {
        return None;
    }
    Some((start, marker_last))
}

/// Whether a valid inclusive aborted interval intersects a nonempty half-open
/// Fetch range.
#[ensures(result == (entry_start@ <= entry_last@
    && query_start@ < query_end@
    && entry_start@ < query_end@
    && entry_last@ >= query_start@))]
#[must_use]
pub fn aborted_transaction_overlaps(
    entry_start: i64,
    entry_last: i64,
    query_start: i64,
    query_end: i64,
) -> bool {
    entry_start <= entry_last
        && query_start < query_end
        && entry_start < query_end
        && entry_last >= query_start
}

/// Whether the idle reaper may abort one persisted transaction.
///
/// This is Kafka's `TransactionStateManager.timedOutTransactions`: the
/// transaction is `ONGOING`, it is not a KIP-939 two-phase-commit transaction
/// (`TransactionMetadata.isDistributedTwoPhaseCommitTxn`, a timeout of
/// `Integer.MAX_VALUE`), and `txnStartTimestamp + txnTimeoutMs < now`. The
/// comparison is strict, so a transaction is reapable only once its timeout
/// has passed, not when it is reached. Kafka evaluates the sum in a Java
/// `long`; the kernel compares exactly, without overflow.
///
/// The decision is total over every persisted timeout. Kafka's
/// `TransactionLog.read` accepts any `TransactionTimeoutMs`, and
/// `InitProducerId` refuses a timeout that is not positive
/// (`validateTransactionTimeoutMs`), so a zero or negative timeout reaches the
/// reaper only from a transaction-log record another writer produced. Kafka
/// treats that timeout arithmetically, and so does this kernel.
///
/// A backwards clock never aborts a transaction with a nonnegative timeout,
/// which is every timeout `InitProducerId` persists.
#[ensures(result == (state == IdleTransactionState::Ongoing
    && txn_timeout_ms@ != NO_TRANSACTION_TIMEOUT_MS@
    && start_ms@ + txn_timeout_ms@ < now_ms@))]
#[ensures(now_ms@ <= start_ms@ && txn_timeout_ms@ >= 0 ==> !result)]
#[must_use]
pub fn should_abort_idle_transaction(
    state: IdleTransactionState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    let ongoing = match state {
        IdleTransactionState::Ongoing => true,
        IdleTransactionState::Other => false,
    };
    // Saturation keeps the order against any `i32` timeout: an elapsed time
    // above `i64::MAX` exceeds it, and one below `i64::MIN` does not.
    ongoing
        && txn_timeout_ms != NO_TRANSACTION_TIMEOUT_MS
        && now_ms.saturating_sub(start_ms) > i64::from(txn_timeout_ms)
}

#[logic(open)]
fn identity_after_increment(result: Option<(i64, i16)>, pid: i64, epoch: i16) -> bool {
    pearlite! { match result {
        Some((result_pid, result_epoch)) => result_pid == pid && result_epoch@ == epoch@ + 1,
        None => false,
    }}
}

/// Choose the producer identity exposed after transaction completion.
///
/// Verified normal completion reserves `i16::MAX` for the transaction marker,
/// while a staged recovery identity may use that epoch once before rotating.
#[ensures(!verified ==> result == Some((pid, epoch)))]
#[ensures(verified && !recovery && epoch@ < i16::MAX@ - 1 ==>
    identity_after_increment(result, pid, epoch))]
#[ensures(verified && recovery && epoch@ < i16::MAX@ ==>
    identity_after_increment(result, pid, epoch))]
#[ensures(verified
    && ((!recovery && epoch@ >= i16::MAX@ - 1) || (recovery && epoch@ >= i16::MAX@)) ==>
    match (result, fresh) {
        (Some((result_pid, result_epoch)), Some(fresh_pid)) =>
            result_pid == fresh_pid && result_epoch@ == 0,
        (None, None) => true,
        _ => false,
    })]
#[must_use]
pub fn next_producer_identity(
    verified: bool,
    recovery: bool,
    pid: i64,
    epoch: i16,
    fresh: Option<i64>,
) -> Option<(i64, i16)> {
    if !verified {
        return Some((pid, epoch));
    }
    let can_increment = if recovery {
        epoch < i16::MAX
    } else {
        epoch < i16::MAX - 1
    };
    if can_increment {
        Some((pid, epoch + 1))
    } else {
        fresh.map(|fresh_pid| (fresh_pid, 0))
    }
}
