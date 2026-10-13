use {super::*, assert2::assert};

const PID: ProducerId = ProducerId(7);
const EPOCH: i16 = 4;

fn entry(state: TxnState) -> TxnEntry {
    let mut entry = TxnEntry::new_empty("tid-end".to_owned(), PID, EPOCH, 60_000, 0);
    entry.state = state;
    entry
}

/// The identity a request carries, relative to the entry.
#[derive(Debug, Clone, Copy)]
enum Identity {
    /// The entry's own epoch.
    Current,
    /// One below the entry's epoch: the retry of a completion that bumped it.
    Retry,
    /// Two below: neither the current epoch nor a retry.
    Stale,
    /// Another producer id at the entry's epoch.
    OtherProducer,
}

fn request(identity: Identity) -> (ProducerId, i16) {
    match identity {
        Identity::Current => (PID, EPOCH),
        Identity::Retry => (PID, EPOCH - 1),
        Identity::Stale => (PID, EPOCH - 2),
        Identity::OtherProducer => (ProducerId(PID.get() + 1), EPOCH),
    }
}

fn prepare(state: TxnState, no_partition_added: bool) -> EndTxnDecision {
    EndTxnDecision::Prepare {
        state,
        no_partition_added,
    }
}

/// The table above `endTransaction` in `TransactionCoordinator.scala`, both
/// halves, plus the identity checks that run before it.
#[test]
fn the_state_table_follows_the_transaction_version() {
    // (state, result, identity, version 1 decision, version 2 decision)
    let cases: [(TxnState, bool, Identity, EndTxnDecision, EndTxnDecision); 20] = [
        (
            TxnState::Ongoing,
            true,
            Identity::Current,
            prepare(TxnState::PrepareCommit, false),
            prepare(TxnState::PrepareCommit, false),
        ),
        (
            TxnState::Ongoing,
            false,
            Identity::Current,
            prepare(TxnState::PrepareAbort, false),
            prepare(TxnState::PrepareAbort, false),
        ),
        (
            TxnState::Ongoing,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        // Empty: version 2 accepts an abort and bumps the epoch for it.
        (
            TxnState::Empty,
            false,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            prepare(TxnState::PrepareAbort, true),
        ),
        (
            TxnState::Empty,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        (
            TxnState::Empty,
            true,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::Empty,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
        // CompleteAbort.
        (
            TxnState::CompleteAbort,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::AlreadyComplete,
        ),
        (
            TxnState::CompleteAbort,
            false,
            Identity::Current,
            EndTxnDecision::AlreadyComplete,
            prepare(TxnState::PrepareAbort, true),
        ),
        // Kafka 4.3.1 answers INVALID_TXN_STATE here, and trunk answers
        // PRODUCER_FENCED (KAFKA-20785), see the switch below the table.
        (
            TxnState::CompleteAbort,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::CompleteAbort,
            true,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        // CompleteCommit.
        (
            TxnState::CompleteCommit,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::AlreadyComplete,
        ),
        (
            TxnState::CompleteCommit,
            true,
            Identity::Current,
            EndTxnDecision::AlreadyComplete,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::CompleteCommit,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::CompleteCommit,
            false,
            Identity::Current,
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
            prepare(TxnState::PrepareAbort, true),
        ),
        // The prepare states.
        (
            TxnState::PrepareCommit,
            true,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS),
        ),
        (
            TxnState::PrepareCommit,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
        ),
        (
            TxnState::PrepareAbort,
            false,
            Identity::Retry,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS),
        ),
        // The identity checks.
        (
            TxnState::Ongoing,
            true,
            Identity::OtherProducer,
            EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
            EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
        ),
        (
            TxnState::Ongoing,
            true,
            Identity::Stale,
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
        ),
    ];

    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for (state, committed, identity, classic, verified) in cases {
        let row = format!(
            "{state:?} {} {identity:?}",
            if committed { "commit" } else { "abort" }
        );
        // Trunk changes one arm of the version-2 table. The version-1 table
        // has no trunk arm.
        let trunk = if (state, committed) == (TxnState::CompleteAbort, true)
            && matches!(identity, Identity::Retry)
        {
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED)
        } else {
            verified
        };
        expected.push((row.clone(), classic, classic, verified, trunk));
        actual.push((
            row,
            end_txn_decision(&entry(state), request(identity), committed, (false, false)),
            end_txn_decision(&entry(state), request(identity), committed, (false, true)),
            end_txn_decision(&entry(state), request(identity), committed, (true, false)),
            end_txn_decision(&entry(state), request(identity), committed, (true, true)),
        ));
    }
    assert!(actual == expected);
}

/// The producer id from before a rotation retries with the exhausted epoch,
/// and transaction version 2 admits it (`retryOnOverflow`).
#[test]
fn a_rotated_producer_id_retries_with_the_exhausted_epoch() {
    let mut rotated = entry(TxnState::CompleteCommit);
    rotated.producer_id = ProducerId(11);
    rotated.producer_epoch = 0;
    rotated.prev_producer_id = PID;
    let overflow = (PID, i16::MAX - 1);

    // (result, version 2 decision)
    let cases = [
        (true, EndTxnDecision::AlreadyComplete),
        (false, EndTxnDecision::Refuse(codes::INVALID_TXN_STATE)),
    ];
    for (committed, expected) in cases {
        assert!(
            end_txn_decision(&rotated, overflow, committed, (true, false)) == expected,
            "committed={committed}"
        );
        // Below version 2 the same request names neither the entry's producer
        // id nor its epoch.
        assert!(
            end_txn_decision(&rotated, overflow, committed, (false, false))
                == EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING),
            "committed={committed} below version 2"
        );
    }
}

/// A `Prepare*` entry that rotates the producer id: Kafka holds `(old id,
/// i16::MAX)` with the new id only pending, so a retry that names the old id at
/// `i16::MAX - 1` is `retryOnEpochBump` and answers `CONCURRENT_TRANSACTIONS`
/// (or `INVALID_TXN_STATE` for the other result), and any other epoch of the
/// old id is fenced. Here the new id is staged for the client, and it must not
/// hide the old one.
#[test]
fn a_retry_of_a_rotating_prepare_names_the_old_producer_id() {
    const NEW_PID: ProducerId = ProducerId(11);
    const OLD_EPOCH: i16 = i16::MAX - 1;
    let rotating = |state| {
        let mut rotating = entry(state);
        rotating.producer_epoch = i16::MAX;
        rotating.last_producer_epoch = OLD_EPOCH;
        rotating.next_producer_id = NEW_PID;
        rotating.next_producer_epoch = 0;
        rotating
    };
    let concurrent = EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS);
    let invalid_state = EndTxnDecision::Refuse(codes::INVALID_TXN_STATE);
    let fenced = EndTxnDecision::Refuse(codes::PRODUCER_FENCED);
    let unknown = EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING);
    // (state, result, request, decision)
    let cases = [
        (TxnState::PrepareCommit, true, (PID, OLD_EPOCH), concurrent),
        (TxnState::PrepareAbort, false, (PID, OLD_EPOCH), concurrent),
        (
            TxnState::PrepareCommit,
            false,
            (PID, OLD_EPOCH),
            invalid_state,
        ),
        (
            TxnState::PrepareAbort,
            true,
            (PID, OLD_EPOCH),
            invalid_state,
        ),
        (TxnState::PrepareCommit, true, (PID, OLD_EPOCH - 1), fenced),
        (TxnState::PrepareCommit, true, (PID, i16::MAX), fenced),
        (
            TxnState::PrepareCommit,
            true,
            (ProducerId(PID.get() + 1), OLD_EPOCH),
            unknown,
        ),
    ];
    for (state, committed, request, expected) in cases {
        assert!(
            end_txn_decision(&rotating(state), request, committed, (true, false)) == expected,
            "{state:?} committed={committed} {request:?}"
        );
    }

    // A recovery identity holds epoch 1 or more in a `Prepare*` entry, so it is
    // not a rotation: the identity from before the recovery stays fenced.
    let mut recovered = rotating(TxnState::PrepareCommit);
    recovered.next_producer_epoch = 1;
    assert!(end_txn_decision(&recovered, (PID, OLD_EPOCH), true, (true, false)) == unknown);
}

/// A KIP-939 recovery stages a new identity on the entry. That identity is the
/// one the recovered client holds, so it is the one that may end the
/// transaction, and the identity from before the recovery is fenced.
#[test]
fn a_staged_recovery_identity_is_the_one_that_ends_the_transaction() {
    let mut staged = entry(TxnState::Ongoing);
    staged.next_producer_id = PID;
    staged.next_producer_epoch = EPOCH + 1;
    for verified in [false, true] {
        assert!(
            end_txn_decision(&staged, (PID, EPOCH + 1), true, (verified, false))
                == prepare(TxnState::PrepareCommit, false),
            "verified={verified}: the staged identity"
        );
        assert!(
            end_txn_decision(&staged, (PID, EPOCH), true, (verified, false))
                == EndTxnDecision::Refuse(codes::PRODUCER_FENCED),
            "verified={verified}: the identity from before the recovery"
        );
    }
}
