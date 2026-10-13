//! Kafka's `EndTxn` state tables, one per transaction version.
//!
//! `TransactionCoordinator.endTransactionWithTV1` and its transaction-version-2
//! twin decide from the producer identity, the state of the entry and the
//! requested result. The tables differ: version 2 accepts an abort from
//! `Empty`, `CompleteCommit` and `CompleteAbort`, bumps the epoch for it and
//! writes an abort over an empty partition set, and it recognises the retry of
//! a completed transaction by the epoch before the bump. Version 1 has no
//! retry, and accepts a commit only from `CompleteCommit` and an abort only
//! from `CompleteAbort`, both without a write. Kafka picks the table from the
//! client's transaction version, which is the `EndTxn` request version and not
//! the cluster's `transaction.version`.
//!
//! The table in `TransactionCoordinator.scala` above `endTransaction` is what
//! this module implements, with `PF` = `PRODUCER_FENCED`,
//! `ITS` = `INVALID_TXN_STATE`, `NONE` = no error and no write, and
//! `EB` = no error and an epoch bump.

use {
    super::producer_identity::client_producer_identity,
    crate::{
        codes,
        txn::{
            state::{TxnEntry, TxnState},
            util::ProducerIdentity,
        },
    },
    krabka_log::ProducerId,
};

/// Which table `EndTxn` consults: the transaction version of the client, and
/// the Kafka rules that apply to its one changed answer.
#[derive(Debug, Clone, Copy)]
pub(super) struct TableRules {
    /// Transaction version 2 and above for the client.
    pub(super) verified: bool,
    /// The one answer that Kafka trunk changed after 4.3.1.
    pub(super) trunk_rules: bool,
}

/// What `EndTxn` does with the entry it found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EndTxnDecision {
    /// Prepare the transition to `state`. `no_partition_added` marks Kafka's
    /// `prepareAbortOrCommit(..., noPartitionAdded = true)`: an abort of a
    /// transaction that added no partition, which starts from the update time
    /// and has no partition to mark.
    Prepare {
        state: TxnState,
        no_partition_added: bool,
    },
    /// The transaction already reached the requested result. Answer `NONE`
    /// with the entry's identity and write nothing.
    AlreadyComplete,
    /// Answer this Kafka error code.
    Refuse(i16),
}

/// The `EndTxn` decision for one request against one entry.
///
/// `verified` is transaction version 2 and above for the *client*, that is an
/// `EndTxn` v5 (Kafka's `TransactionVersion.transactionVersionForEndTxn`),
/// where completion bumps the producer epoch. `trunk_rules` selects the one
/// answer that Kafka trunk changed after 4.3.1, see [`verified_state_decision`].
pub(super) fn end_txn_decision(
    entry: &TxnEntry,
    request: ProducerIdentity,
    committed: bool,
    rules: TableRules,
) -> EndTxnDecision {
    let ProducerIdentity {
        producer_id: request_pid,
        producer_epoch: request_epoch,
    } = request;
    let TableRules {
        verified,
        trunk_rules,
    } = rules;
    let ProducerIdentity {
        producer_id: entry_pid,
        producer_epoch: entry_epoch,
    } = compared_identity(entry, request_pid);
    let retry_on_epoch_bump =
        entry_pid == request_pid && request_epoch.checked_add(1) == Some(entry_epoch);
    let retry_on_overflow =
        entry.prev_producer_id == request_pid && request_epoch == i16::MAX - 1 && entry_epoch == 0;
    let is_retry = verified && (retry_on_epoch_bump || retry_on_overflow);
    if verified {
        if entry_pid != request_pid && !retry_on_overflow {
            return EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if !valid_verified_epoch(
            entry.state,
            entry_epoch,
            request_epoch,
            retry_on_epoch_bump,
            retry_on_overflow,
        ) {
            return EndTxnDecision::Refuse(codes::PRODUCER_FENCED);
        }
    } else {
        if entry_pid != request_pid {
            return EndTxnDecision::Refuse(codes::INVALID_PRODUCER_ID_MAPPING);
        }
        if entry_epoch != request_epoch {
            return EndTxnDecision::Refuse(codes::PRODUCER_FENCED);
        }
    }
    if verified {
        verified_state_decision(entry.state, committed, is_retry, trunk_rules)
    } else {
        classic_state_decision(entry.state, committed)
    }
}

/// The identity `request_pid` is compared with.
///
/// The identity a client holds is the staged one when the entry has one (a
/// KIP-939 recovery identity, or the identity a rotation hands out), and the
/// live one otherwise.
///
/// Kafka has no staged identity to compare with: it holds a `Prepare*` entry
/// that rotates the producer id at `(old id, i16::MAX)`, the producer that ends
/// the transaction at the epoch its marker reserves, with the new id only in
/// `nextProducerId`. A retry that names `(old id, i16::MAX - 1)` is
/// `retryOnEpochBump` there and gets `CONCURRENT_TRANSACTIONS`, and any other
/// epoch of the old id is `PRODUCER_FENCED`, as `endTransaction` in
/// `TransactionCoordinator.scala` decides. So a request that names the old id
/// of such an entry is compared with the live identity. A recovery identity
/// that a `Prepare*` entry holds is one epoch past its first, so a new
/// producer id at epoch 0 marks the rotation.
fn compared_identity(entry: &TxnEntry, request_pid: ProducerId) -> ProducerIdentity {
    let rotating_prepare = matches!(
        entry.state,
        TxnState::PrepareCommit | TxnState::PrepareAbort
    ) && entry.has_staged_producer_identity()
        && entry.next_producer_epoch == 0
        && entry.next_producer_id != entry.producer_id;
    if rotating_prepare && entry.producer_id == request_pid {
        ProducerIdentity {
            producer_id: entry.producer_id,
            producer_epoch: entry.producer_epoch,
        }
    } else {
        let (producer_id, producer_epoch) = client_producer_identity(entry);
        ProducerIdentity {
            producer_id,
            producer_epoch,
        }
    }
}

/// Kafka's `isValidEpoch` for transaction version 2: which epoch each state
/// accepts.
fn valid_verified_epoch(
    state: TxnState,
    entry_epoch: i16,
    request_epoch: i16,
    retry_on_epoch_bump: bool,
    retry_on_overflow: bool,
) -> bool {
    match state {
        TxnState::Ongoing | TxnState::Empty | TxnState::Dead => request_epoch == entry_epoch,
        TxnState::PrepareCommit | TxnState::PrepareAbort => retry_on_epoch_bump,
        TxnState::CompleteCommit | TxnState::CompleteAbort => {
            retry_on_epoch_bump || retry_on_overflow || request_epoch == entry_epoch
        }
    }
}

/// The transaction-version-2 table.
///
/// Kafka 4.3.1 answers `INVALID_TXN_STATE` for a commit at the retry epoch of a
/// `CompleteAbort` entry. Kafka trunk answers `PRODUCER_FENCED` instead
/// (KAFKA-20785), and `trunk_rules` turns that arm on. Every other row is the
/// same in both.
fn verified_state_decision(
    state: TxnState,
    committed: bool,
    is_retry: bool,
    trunk_rules: bool,
) -> EndTxnDecision {
    let prepare = |no_partition_added| EndTxnDecision::Prepare {
        state: if committed {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        },
        no_partition_added,
    };
    match (state, committed, is_retry) {
        (TxnState::Ongoing, _, _) => prepare(false),
        // An abort of a transaction that added no partition is accepted, and
        // it bumps the epoch. That is how a KIP-890 client fences itself after
        // a failure it cannot classify.
        (TxnState::Empty, false, _) => EndTxnDecision::Prepare {
            state: TxnState::PrepareAbort,
            no_partition_added: true,
        },
        (TxnState::CompleteCommit, true, true) | (TxnState::CompleteAbort, false, true) => {
            EndTxnDecision::AlreadyComplete
        }
        (TxnState::CompleteCommit | TxnState::CompleteAbort, false, false) => {
            EndTxnDecision::Prepare {
                state: TxnState::PrepareAbort,
                no_partition_added: true,
            }
        }
        // Trunk only (KAFKA-20785, after 4.3.1): a commit at the pre-abort
        // epoch raced a coordinator-side abort, for example on
        // `transaction.timeout.ms`. It cannot have taken effect, so trunk
        // answers the recoverable `PRODUCER_FENCED`. 4.3.1 falls through to
        // `INVALID_TXN_STATE` below.
        (TxnState::CompleteAbort, true, true) if trunk_rules => {
            EndTxnDecision::Refuse(codes::PRODUCER_FENCED)
        }
        (TxnState::PrepareCommit, true, _) | (TxnState::PrepareAbort, false, _) => {
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS)
        }
        _ => EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
    }
}

/// The table below transaction version 2.
fn classic_state_decision(state: TxnState, committed: bool) -> EndTxnDecision {
    match (state, committed) {
        (TxnState::Ongoing, _) => EndTxnDecision::Prepare {
            state: if committed {
                TxnState::PrepareCommit
            } else {
                TxnState::PrepareAbort
            },
            no_partition_added: false,
        },
        (TxnState::CompleteCommit, true) | (TxnState::CompleteAbort, false) => {
            EndTxnDecision::AlreadyComplete
        }
        (TxnState::PrepareCommit, true) | (TxnState::PrepareAbort, false) => {
            EndTxnDecision::Refuse(codes::CONCURRENT_TRANSACTIONS)
        }
        _ => EndTxnDecision::Refuse(codes::INVALID_TXN_STATE),
    }
}

#[cfg(test)]
mod tests;
