use {
    super::*,
    crate::log::test_support::{
        TempLog, commit_marker, sample_batch, test_log, transactional_batch,
    },
    assert2::assert,
    krabka_ids::Offset,
};

const PID: i64 = 1000;
const CLOCK: (i64, i64) = (1_000, 86_400_000);

fn batch(epoch: i16, base_sequence: i32, is_transactional: bool) -> TransactionalBatch {
    TransactionalBatch {
        producer_id: ProducerId(PID),
        producer_epoch: epoch,
        base_sequence,
        is_transactional,
        is_control: false,
    }
}

/// The partition state a case starts from.
#[derive(Debug, Clone, Copy)]
enum Start {
    /// No producer state at all.
    Empty,
    /// A committed transaction at epoch 3, sequences 0 and 1.
    CommittedAtEpoch3,
    /// An open transaction at epoch 3, sequences 0 and 1.
    OpenAtEpoch3,
}

#[derive(Debug, Clone, Copy)]
enum Presented {
    Sentinel,
    StartedGuard,
    /// A guard started, then a marker for the producer landed.
    GuardBeforeMarker,
}

fn log_at(start: Start) -> TempLog {
    let TempLog { dir, mut log } = test_log();
    if matches!(start, Start::CommittedAtEpoch3 | Start::OpenAtEpoch3) {
        let mut data = transactional_batch(PID, 3, &["a", "b"]);
        data.base_sequence = 0;
        let guard = log
            .maybe_start_transaction_verification(batch(3, 0, true), false, CLOCK)
            .unwrap();
        log.check_transactional_append(batch(3, 0, true), guard)
            .unwrap();
        log.append(&mut data).unwrap();
    }
    if matches!(start, Start::CommittedAtEpoch3) {
        log.append(&mut commit_marker(PID, 3)).unwrap();
    }
    TempLog { dir, log }
}

/// Kafka's `UnifiedLog.maybeStartTransactionVerification`: a stale epoch is
/// refused, an open transaction at the batch epoch needs no guard, and any
/// other transactional batch gets one guard that a repeated start keeps.
#[yare::parameterized(
    no_producer_state = { "no producer state", Start::Empty, 0, Ok(true) },
    after_commit = { "after a commit", Start::CommittedAtEpoch3, 3, Ok(true) },
    open_transaction = { "open transaction", Start::OpenAtEpoch3, 3, Ok(false) },
    stale_epoch = {
        "open transaction, stale epoch",
        Start::OpenAtEpoch3,
        2,
        Err(TransactionAppendRefusal::StaleProducerEpoch),
    },
    newer_epoch = { "open transaction, newer epoch", Start::OpenAtEpoch3, 4, Ok(true) },
)]
fn a_verification_starts_only_outside_an_open_transaction(
    name: &str,
    start: Start,
    epoch: i16,
    want_guard: Result<bool, TransactionAppendRefusal>,
) {
    let TempLog { dir: _dir, mut log } = log_at(start);
    let first = log.maybe_start_transaction_verification(batch(epoch, 2, true), false, CLOCK);
    let again = log.maybe_start_transaction_verification(batch(epoch, 2, true), false, CLOCK);
    assert2::assert!(first == again, "{name}");
    assert2::assert!(
        first.map(|guard| guard != VerificationGuard::SENTINEL) == want_guard,
        "{name}"
    );
}

/// The append-time check of `UnifiedLog.analyzeAndValidateProducerState` and
/// `ProducerAppendInfo`.
#[yare::parameterized(
    no_transaction_no_guard = {
        "no transaction, no guard",
        Start::CommittedAtEpoch3,
        batch(3, 2, true),
        Presented::Sentinel,
        Err(TransactionAppendRefusal::InvalidTransactionState),
    },
    no_transaction_current_guard = {
        "no transaction, current guard",
        Start::CommittedAtEpoch3,
        batch(3, 2, true),
        Presented::StartedGuard,
        Ok(()),
    },
    marker_cleared_guard = {
        "a marker cleared the guard",
        Start::OpenAtEpoch3,
        batch(3, 2, true),
        Presented::GuardBeforeMarker,
        Err(TransactionAppendRefusal::InvalidTransactionState),
    },
    open_transaction = {
        "open transaction",
        Start::OpenAtEpoch3,
        batch(3, 2, true),
        Presented::Sentinel,
        Ok(()),
    },
    stale_epoch = {
        "stale epoch",
        Start::CommittedAtEpoch3,
        batch(2, 2, true),
        Presented::Sentinel,
        Err(TransactionAppendRefusal::StaleProducerEpoch),
    },
    non_transactional_in_open = {
        "non-transactional batch in an open transaction",
        Start::OpenAtEpoch3,
        batch(3, 2, false),
        Presented::Sentinel,
        Err(TransactionAppendRefusal::InvalidTransactionState),
    },
    non_transactional_after_commit = {
        "non-transactional batch after a commit",
        Start::CommittedAtEpoch3,
        batch(3, 2, false),
        Presented::Sentinel,
        Ok(()),
    },
)]
fn an_append_needs_an_open_transaction_or_the_current_guard(
    name: &str,
    start: Start,
    appended: TransactionalBatch,
    presented: Presented,
    want: Result<(), TransactionAppendRefusal>,
) {
    let TempLog { dir: _dir, mut log } = log_at(start);
    let guard = match presented {
        Presented::Sentinel => VerificationGuard::SENTINEL,
        Presented::StartedGuard => log
            .maybe_start_transaction_verification(appended, false, CLOCK)
            .unwrap(),
        Presented::GuardBeforeMarker => {
            // The verification started for the next epoch, then the
            // coordinator ended the open transaction.
            let next = TransactionalBatch {
                producer_epoch: 4,
                ..appended
            };
            let guard = log
                .maybe_start_transaction_verification(next, false, CLOCK)
                .unwrap();
            log.append(&mut commit_marker(PID, 3)).unwrap();
            guard
        }
    };
    assert2::assert!(
        log.check_transactional_append(appended, guard) == want,
        "{name}"
    );
}

/// Kafka's `ProducerAppendInfo.checkSequence` with a verification state.
#[yare::parameterized(
    version_two_sequence_zero = {
        "transaction version 2, no batch, sequence 0",
        true,
        0,
        0,
        Ok(()),
    },
    version_two_sequence_five = {
        "transaction version 2, no batch, sequence 5",
        true,
        5,
        5,
        Err(TransactionAppendRefusal::OutOfOrderSequence),
    },
    version_one_sequence_five = {
        "transaction version 1, no batch, sequence 5",
        false,
        5,
        5,
        Ok(()),
    },
    above_lowest_sequence = {
        "above the lowest verified sequence",
        false,
        5,
        7,
        Err(TransactionAppendRefusal::OutOfOrderSequence),
    },
)]
fn a_verified_first_batch_keeps_the_verified_sequence(
    name: &str,
    supports_epoch_bump: bool,
    verified_sequence: i32,
    appended_sequence: i32,
    want: Result<(), TransactionAppendRefusal>,
) {
    let TempLog { dir: _dir, mut log } = log_at(Start::Empty);
    let guard = log
        .maybe_start_transaction_verification(
            batch(0, verified_sequence, true),
            supports_epoch_bump,
            CLOCK,
        )
        .unwrap();
    assert2::assert!(
        log.check_transactional_append(batch(0, appended_sequence, true), guard) == want,
        "{name}"
    );
}

/// The first transactional append clears the guard, and verification state
/// older than the expiration window is dropped when another one starts.
#[test]
fn an_append_and_the_expiration_window_clear_the_guard() {
    let TempLog { dir: _dir, mut log } = log_at(Start::Empty);
    let guard = log
        .maybe_start_transaction_verification(batch(0, 0, true), false, CLOCK)
        .unwrap();
    let mut data = transactional_batch(PID, 0, &["a"]);
    data.base_sequence = 0;
    log.append(&mut data).unwrap();
    assert!(log.verification_states.is_empty());
    assert!(log.check_transactional_append(batch(0, 1, true), guard) == Ok(()));

    let other = TransactionalBatch {
        producer_id: ProducerId(2000),
        ..batch(0, 0, true)
    };
    log.maybe_start_transaction_verification(other, false, CLOCK)
        .unwrap();
    let mut plain = sample_batch(1);
    log.append(&mut plain).unwrap();
    let later = TransactionalBatch {
        producer_id: ProducerId(3000),
        ..batch(0, 0, true)
    };
    log.maybe_start_transaction_verification(later, false, (CLOCK.0 + CLOCK.1, CLOCK.1))
        .unwrap();
    assert!(log.verification_states.keys().copied().collect::<Vec<_>>() == vec![ProducerId(3000)]);
}

/// A truncation and a hard reset drop the verification state, as a reopen of
/// the log does, so a verification from before them cannot refuse a batch
/// after them.
#[yare::parameterized(
    truncate = { "truncate" },
    reset = { "reset" },
)]
fn a_truncation_and_a_reset_clear_the_verification_state(name: &str) {
    let TempLog { dir: _dir, mut log } = log_at(Start::Empty);
    log.append(&mut sample_batch(2)).unwrap();
    let guard = log
        .maybe_start_transaction_verification(batch(0, 5, true), true, CLOCK)
        .unwrap();
    if name == "truncate" {
        log.truncate_to(Offset(1)).unwrap();
    } else {
        log.reset_to(Offset(1)).unwrap();
    }
    assert2::assert!(log.verification_states.is_empty(), "{name}");
    assert2::assert!(
        log.check_transactional_append(batch(0, 5, true), guard)
            == Err(TransactionAppendRefusal::InvalidTransactionState),
        "{name}"
    );
}

#[test]
fn verification_guard_verifies_semantics() {
    assert!(!VerificationGuard::SENTINEL.verifies(VerificationGuard::SENTINEL));
    let g1 = VerificationGuard(1);
    let g2 = VerificationGuard(2);
    assert!(!g1.verifies(VerificationGuard::SENTINEL));
    assert!(!g1.verifies(g2));
    assert!(g1.verifies(g1));
}

#[test]
fn verification_state_updates_lowest_sequence_and_epoch() {
    let TempLog { dir: _dir, mut log } = log_at(Start::Empty);
    let guard = log
        .maybe_start_transaction_verification(batch(0, 10, true), false, CLOCK)
        .unwrap();
    // Same epoch, lower sequence lowers lowest_sequence
    let guard2 = log
        .maybe_start_transaction_verification(batch(0, 5, true), false, CLOCK)
        .unwrap();
    assert!(guard == guard2);
    // Appending sequence 7 is refused because 7 > lowest_sequence (5)
    assert!(
        log.check_transactional_append(batch(0, 7, true), guard)
            == Err(TransactionAppendRefusal::OutOfOrderSequence)
    );
    // Appending sequence 5 is accepted
    assert!(log.check_transactional_append(batch(0, 5, true), guard) == Ok(()));

    // Higher epoch updates epoch and resets lowest_sequence
    let guard3 = log
        .maybe_start_transaction_verification(batch(1, 20, true), false, CLOCK)
        .unwrap();
    assert!(guard3 == guard);
    assert!(log.check_transactional_append(batch(1, 20, true), guard3) == Ok(()));
    assert!(
        log.check_transactional_append(batch(1, 21, true), guard3)
            == Err(TransactionAppendRefusal::OutOfOrderSequence)
    );
}

#[test]
fn check_transactional_append_validates_producer_id_zero() {
    let TempLog { dir: _dir, log } = log_at(Start::Empty);
    let mut b = batch(0, 0, true);
    b.producer_id = ProducerId(0);
    // Producer 0 without verification guard must be refused
    assert!(
        log.check_transactional_append(b, VerificationGuard::SENTINEL)
            == Err(TransactionAppendRefusal::InvalidTransactionState)
    );
    // Control batch with producer 0 is accepted
    let mut ctrl = b;
    ctrl.is_control = true;
    assert!(log.check_transactional_append(ctrl, VerificationGuard::SENTINEL) == Ok(()));
}

#[test]
fn clear_verification_after_append_preserves_guard_only_for_epoch_bump_control() {
    let TempLog { dir: _dir, mut log } = log_at(Start::Empty);
    // Start verification with supports_epoch_bump = true
    let _guard = log
        .maybe_start_transaction_verification(batch(0, 0, true), true, CLOCK)
        .unwrap();
    assert!(!log.verification_states.is_empty());

    // Mismatched epoch control marker clears guard
    log.clear_verification_after_append(ProducerId(PID), 1, true);
    assert!(log.verification_states.is_empty());

    // Re-start and verify matching epoch control marker preserves guard
    log.maybe_start_transaction_verification(batch(0, 0, true), true, CLOCK)
        .unwrap();
    log.clear_verification_after_append(ProducerId(PID), 0, true);
    assert!(!log.verification_states.is_empty());

    // Non-control batch at matching epoch clears guard
    log.clear_verification_after_append(ProducerId(PID), 0, false);
    assert!(log.verification_states.is_empty());
}
