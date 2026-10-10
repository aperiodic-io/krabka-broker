use assert2::assert;

use super::*;

#[test]
fn reaper_completion_requires_the_exact_prepared_snapshot() {
    use TransactionReaperCompletionDecision::{
        AlreadyComplete, Proceed, RejectChangedPreparedState, RejectMalformed, RejectStaleIdentity,
    };

    let prepared = snapshot(TransactionSnapshotSetup::default());
    let completion = snapshot(TransactionSnapshotSetup {
        producer_epoch: ProducerEpoch(4),
        state: TransactionStateCode(COMPLETE_ABORT),
        ..Default::default()
    });
    // (current, prepared, completion, exact snapshot, expected).
    let cases = [
        // The entry is exactly as the reaper prepared it.
        (prepared, prepared, completion, true, Proceed),
        (
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(0),
                producer_epoch: ProducerEpoch(0),
                ..Default::default()
            }),
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(0),
                producer_epoch: ProducerEpoch(0),
                ..Default::default()
            }),
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(0),
                producer_epoch: ProducerEpoch(1),
                state: TransactionStateCode(COMPLETE_ABORT),
            }),
            true,
            Proceed,
        ),
        // Same identity and state, but another field of the entry moved,
        // such as a late partition registration.
        (
            prepared,
            prepared,
            completion,
            false,
            RejectChangedPreparedState,
        ),
        // Same identity, different state: another caller moved it.
        (
            snapshot(TransactionSnapshotSetup {
                state: TransactionStateCode(ONGOING),
                ..Default::default()
            }),
            prepared,
            completion,
            true,
            RejectChangedPreparedState,
        ),
        // An `InitProducerId` bumped the epoch or rotated the PID.
        (
            snapshot(TransactionSnapshotSetup {
                producer_epoch: ProducerEpoch(5),
                ..Default::default()
            }),
            prepared,
            completion,
            true,
            RejectStaleIdentity,
        ),
        (
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(9),
                ..Default::default()
            }),
            prepared,
            completion,
            false,
            RejectStaleIdentity,
        ),
        // The completion identity at the prepare state is not the
        // completion.
        (
            snapshot(TransactionSnapshotSetup {
                producer_epoch: ProducerEpoch(4),
                ..Default::default()
            }),
            prepared,
            completion,
            true,
            RejectStaleIdentity,
        ),
        // The intended completion is already durable, whatever the
        // snapshot comparison says.
        (completion, prepared, completion, false, AlreadyComplete),
        (completion, prepared, completion, true, AlreadyComplete),
        // A negative PID or epoch in any snapshot.
        (
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(-1),
                ..Default::default()
            }),
            prepared,
            completion,
            true,
            RejectMalformed,
        ),
        (
            snapshot(TransactionSnapshotSetup {
                producer_epoch: ProducerEpoch(-1),
                ..Default::default()
            }),
            prepared,
            completion,
            true,
            RejectMalformed,
        ),
        (
            prepared,
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(-1),
                ..Default::default()
            }),
            completion,
            true,
            RejectMalformed,
        ),
        (
            prepared,
            snapshot(TransactionSnapshotSetup {
                producer_epoch: ProducerEpoch(-1),
                ..Default::default()
            }),
            completion,
            true,
            RejectMalformed,
        ),
        (
            prepared,
            prepared,
            snapshot(TransactionSnapshotSetup {
                producer_id: ProducerId(-1),
                producer_epoch: ProducerEpoch(4),
                state: TransactionStateCode(COMPLETE_ABORT),
            }),
            true,
            RejectMalformed,
        ),
        (
            prepared,
            prepared,
            snapshot(TransactionSnapshotSetup {
                producer_epoch: ProducerEpoch(-1),
                state: TransactionStateCode(COMPLETE_ABORT),
                ..Default::default()
            }),
            true,
            RejectMalformed,
        ),
    ];
    for (current, prepared, completion, exact, expected) in cases {
        assert!(
            transaction_reaper_completion_decision(current, prepared, completion, exact)
                == expected,
            "current={current:?}, prepared={prepared:?}, completion={completion:?}, \
             exact={exact}"
        );
    }
}

#[test]
fn partition_registration_fences_generation_and_retries_exactly() {
    use TransactionRegistrationDecision::{
        PersistRegistration, PersistRetry, RejectNotCoordinator, RejectPendingTransition,
        RejectProducerEpoch, RejectProducerId, RejectState, RejectUnknownProducer,
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
    for malformed in [
        (false, true, true),
        (true, false, true),
        (true, true, false),
    ] {
        let mut facts = admitted;
        facts.ownership.producer_id_valid = malformed.0;
        facts.ownership.entry_exists = malformed.1;
        facts.identity.matching.transactional_id_matches = malformed.2;
        assert!(transaction_partition_registration(facts) == RejectUnknownProducer);
    }

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
