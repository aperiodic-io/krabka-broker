use {super::*, assert2::assert};

#[yare::parameterized(
    prepared_exactly = {
        snapshot().call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::Proceed,
    },
    zero_identity = {
        snapshot().producer_id(0).producer_epoch(0).call(),
        snapshot().producer_id(0).producer_epoch(0).call(),
        snapshot().producer_id(0).producer_epoch(1).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::Proceed,
    },
    changed_prepared_state = {
        snapshot().call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        false,
        TransactionReaperCompletionDecision::RejectChangedPreparedState,
    },
    current_state_changed = {
        snapshot().state(ONGOING).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectChangedPreparedState,
    },
    epoch_advanced = {
        snapshot().producer_epoch(5).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectStaleIdentity,
    },
    pid_rotated = {
        snapshot().producer_id(9).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        false,
        TransactionReaperCompletionDecision::RejectStaleIdentity,
    },
    completion_identity_pending = {
        snapshot().producer_epoch(4).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectStaleIdentity,
    },
    already_complete_not_exact = {
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        false,
        TransactionReaperCompletionDecision::AlreadyComplete,
    },
    already_complete_exact = {
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::AlreadyComplete,
    },
    negative_current_pid = {
        snapshot().producer_id(-1).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
    negative_current_epoch = {
        snapshot().producer_epoch(-1).call(),
        snapshot().call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
    negative_prepared_pid = {
        snapshot().call(),
        snapshot().producer_id(-1).call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
    negative_prepared_epoch = {
        snapshot().call(),
        snapshot().producer_epoch(-1).call(),
        snapshot().producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
    negative_completion_pid = {
        snapshot().call(),
        snapshot().call(),
        snapshot().producer_id(-1).producer_epoch(4).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
    negative_completion_epoch = {
        snapshot().call(),
        snapshot().call(),
        snapshot().producer_epoch(-1).state(COMPLETE_ABORT).call(),
        true,
        TransactionReaperCompletionDecision::RejectMalformed,
    },
)]
fn reaper_completion_requires_the_exact_prepared_snapshot(
    current: TransactionSnapshot,
    prepared: TransactionSnapshot,
    completion: TransactionSnapshot,
    exact: bool,
    expected: TransactionReaperCompletionDecision,
) {
    assert2::assert!(
        transaction_reaper_completion_decision(current, prepared, completion, exact) == expected,
        "current={current:?}, prepared={prepared:?}, completion={completion:?}, exact={exact}"
    );
}

#[test]
fn partition_registration_fences_generation_and_retries_exactly() {
    use TransactionRegistrationDecision::{
        PersistRegistration, PersistRetry, RejectNotCoordinator, RejectPendingTransition,
        RejectProducerEpoch, RejectProducerId, RejectState,
    };

    let admitted = TransactionRegistrationFacts {
        ownership: TransactionRegistrationOwnershipFacts {
            is_coordinator: true,
            producer_id_valid: true,
            entry_exists: true,
        },
        identity: TransactionRegistrationIdentityFacts {
            pending_transition: false,
            matching: TransactionRegistrationIdentityMatchFacts {
                transactional_id_matches: true,
                producer_id_matches: true,
                producer_epoch_matches: true,
            },
        },
        state: TransactionRegistrationStateFacts {
            state_allows_registration: true,
            state_is_ongoing: true,
            exact_partitions_registered: false,
        },
    };

    let mut facts = admitted;
    facts.ownership.is_coordinator = false;
    assert!(transaction_partition_registration(facts) == RejectNotCoordinator);
    // The pending-transition check runs ahead of the producer id and
    // epoch checks, so it wins even when both of those also mismatch.
    let mut facts = admitted;
    facts.identity.pending_transition = true;
    facts.identity.matching.producer_id_matches = false;
    facts.identity.matching.producer_epoch_matches = false;
    assert!(transaction_partition_registration(facts) == RejectPendingTransition);

    let mut facts = admitted;
    facts.identity.matching.producer_id_matches = false;
    assert!(transaction_partition_registration(facts) == RejectProducerId);

    let mut facts = admitted;
    facts.identity.matching.producer_epoch_matches = false;
    assert!(transaction_partition_registration(facts) == RejectProducerEpoch);

    let mut facts = admitted;
    facts.state.state_allows_registration = false;
    assert!(transaction_partition_registration(facts) == RejectState);

    let mut facts = admitted;
    facts.state.exact_partitions_registered = true;
    assert!(transaction_partition_registration(facts) == PersistRetry);

    // The retry optimization requires the current state to be exactly
    // Ongoing; a stale exact match left over from a completed or
    // not-yet-started transaction must still persist.
    let mut facts = admitted;
    facts.state.exact_partitions_registered = true;
    facts.state.state_is_ongoing = false;
    assert!(transaction_partition_registration(facts) == PersistRegistration);

    assert!(transaction_partition_registration(admitted) == PersistRegistration);
}

#[yare::parameterized(
    invalid_producer_id = { false, true, true },
    missing_entry = { true, false, true },
    wrong_transactional_id = { true, true, false },
)]
fn partition_registration_rejects_each_unknown_producer_fact(
    producer_id_valid: bool,
    entry_exists: bool,
    transactional_id_matches: bool,
) {
    let facts = TransactionRegistrationFacts {
        ownership: TransactionRegistrationOwnershipFacts {
            is_coordinator: true,
            producer_id_valid,
            entry_exists,
        },
        identity: TransactionRegistrationIdentityFacts {
            pending_transition: false,
            matching: TransactionRegistrationIdentityMatchFacts {
                transactional_id_matches,
                producer_id_matches: true,
                producer_epoch_matches: true,
            },
        },
        state: TransactionRegistrationStateFacts {
            state_allows_registration: true,
            state_is_ongoing: true,
            exact_partitions_registered: false,
        },
    };

    assert2::assert!(
        transaction_partition_registration(facts)
            == TransactionRegistrationDecision::RejectUnknownProducer
    );
}

#[test]
fn local_lso_marker_and_aborted_interval_decisions_fail_closed() {
    assert2::assert!(first_unstable_offset(&[], 20) == Some(20));
    assert2::assert!(first_unstable_offset(&[20], 20) == Some(20));
    assert2::assert!(first_unstable_offset(&[9, 3, 14], 20) == Some(3));
    assert2::assert!(first_unstable_offset(&[9, 21], 20).is_none());
    // A start beyond the log end rejects even behind a lower start.
    assert2::assert!(first_unstable_offset(&[5, 30], 20).is_none());
    assert2::assert!(transaction_marker_closes(true, false, true));
    assert2::assert!(!transaction_marker_closes(false, false, true));
    assert2::assert!(!transaction_marker_closes(true, false, false));
    assert2::assert!(aborted_transaction_interval(Some(7), 7, 0) == Some((7, 7)));
    assert2::assert!(aborted_transaction_interval(Some(3), 7, 1) == Some((3, 7)));
    assert2::assert!(aborted_transaction_interval(Some(8), 7, 1).is_none());
    assert2::assert!(aborted_transaction_interval(Some(3), 7, -1).is_none());
    assert2::assert!(aborted_transaction_interval(None, 7, 1).is_none());
    assert2::assert!(aborted_transaction_overlaps(10, 14, 0, 11));
    assert2::assert!(!aborted_transaction_overlaps(10, 14, 0, 10));
    assert2::assert!(!aborted_transaction_overlaps(14, 10, 0, 20));
    assert2::assert!(!aborted_transaction_overlaps(10, 14, 20, 20));
    assert2::assert!(!aborted_transaction_overlaps(10, 14, 12, 12));
}
