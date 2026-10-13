#[cfg(creusot)]
use super::has_identity;
use {
    super::{
        EXHAUSTED_PRODUCER_EPOCH, InitProducerIdIdentityDecision, TransactionCompletionDecision,
        TransactionIdentity, TransactionSnapshot,
    },
    creusot_std::prelude::*,
};

/// What an `InitProducerId` caller's producer identity may do to the entry it
/// names (KIP-360).
///
/// A request producer id of `-1` supplies no identity, which every
/// `InitProducerId` below v3 and every first initialisation does.
///
/// Kafka admits a supplied identity in `TransactionCoordinator`'s
/// `isValidProducerId`: the identity names the entry's producer id, whatever
/// its epoch, or it names the producer id from before the last rotation
/// together with an exhausted epoch. The epoch then decides the outcome in
/// `TransactionMetadata.prepareIncrementProducerEpoch`: the entry's own epoch
/// bumps it, the epoch before the last bump is a retry of that bump, and
/// every other epoch is fenced.
///
/// The retry rule also covers a failed epoch fence: that path records the
/// epoch the producer still holds as the last epoch, so the producer that
/// owns the transaction is the one the rule admits.
///
/// Kafka's `isValidProducerId` has a third admission clause, which this
/// kernel does not model: `txnMetadata.producerEpoch ==
/// RecordBatch.NO_PRODUCER_EPOCH` admits every supplied identity, and
/// `prepareIncrementProducerEpoch` then bumps. Only the metadata that
/// `handleInitProducerId` has just created for an unknown transactional ID
/// carries that epoch. The host covers the clause: it answers a transactional
/// ID with no entry by allocating a fresh identity without calling this
/// kernel, and every entry it does pass here has a nonnegative epoch, because
/// allocation hands out nonnegative epochs and
/// [`crate::transaction::transaction_pid_install_decision`] rejects a replayed entry with a
/// negative current or staged epoch.
#[ensures((result == InitProducerIdIdentityDecision::BumpWithoutIdentity)
    == (request_pid@ == -1))]
#[ensures((result == InitProducerIdIdentityDecision::Bump)
    == (request_pid@ != -1
        && request_pid@ == entry_pid@
        && request_epoch@ == entry_epoch@))]
#[ensures((result == InitProducerIdIdentityDecision::Retry)
    == (request_pid@ != -1
        && (request_pid@ == entry_pid@
            || (request_pid@ == prev_pid@
                && request_epoch@ >= EXHAUSTED_PRODUCER_EPOCH@))
        && !(request_pid@ == entry_pid@ && request_epoch@ == entry_epoch@)
        && request_epoch@ == last_epoch@))]
#[ensures((result == InitProducerIdIdentityDecision::Fenced)
    == (request_pid@ != -1
        && (!(request_pid@ == entry_pid@
            || (request_pid@ == prev_pid@
                && request_epoch@ >= EXHAUSTED_PRODUCER_EPOCH@))
            || (!(request_pid@ == entry_pid@ && request_epoch@ == entry_epoch@)
                && request_epoch@ != last_epoch@))))]
#[must_use]
pub fn init_producer_id_identity_decision(
    entry_pid: i64,
    entry_epoch: i16,
    last_epoch: i16,
    prev_pid: i64,
    request_pid: i64,
    request_epoch: i16,
) -> InitProducerIdIdentityDecision {
    if request_pid == -1 {
        return InitProducerIdIdentityDecision::BumpWithoutIdentity;
    }
    let admitted = request_pid == entry_pid
        || (request_pid == prev_pid && request_epoch >= EXHAUSTED_PRODUCER_EPOCH);
    if !admitted {
        return InitProducerIdIdentityDecision::Fenced;
    }
    if request_pid == entry_pid && request_epoch == entry_epoch {
        return InitProducerIdIdentityDecision::Bump;
    }
    if request_epoch == last_epoch {
        return InitProducerIdIdentityDecision::Retry;
    }
    InitProducerIdIdentityDecision::Fenced
}

/// Revalidate the transaction entry after the marker fan-out released its lock.
///
/// `expected` and `prepare_state` are what the caller prepared; `completion`
/// and `complete_state` are the completion it intends to write. The live entry
/// already holding that completion is an idempotent success; otherwise the
/// entry must still hold the prepared identity (else it was fenced) and the
/// prepared state (else another caller moved it).
///
/// The two state tags are one Kafka `Prepare* -> Complete*` pairing, which the
/// host takes from a fixed mapping, so they differ.
#[requires(prepare_state != complete_state)]
#[ensures((result == TransactionCompletionDecision::AlreadyComplete)
    == (has_identity(current, completion) && current.state == complete_state))]
#[ensures((result == TransactionCompletionDecision::RejectStaleIdentity)
    == (!(has_identity(current, completion) && current.state == complete_state)
        && !has_identity(current, expected)))]
#[ensures((result == TransactionCompletionDecision::RejectState)
    == (!(has_identity(current, completion) && current.state == complete_state)
        && has_identity(current, expected)
        && current.state != prepare_state))]
#[ensures((result == TransactionCompletionDecision::Proceed)
    == (has_identity(current, expected) && current.state == prepare_state))]
#[must_use]
pub fn transaction_completion_decision(
    current: TransactionSnapshot,
    expected: TransactionIdentity,
    completion: TransactionIdentity,
    prepare_state: i8,
    complete_state: i8,
) -> TransactionCompletionDecision {
    if current.pid == completion.pid
        && current.epoch == completion.epoch
        && current.state == complete_state
    {
        return TransactionCompletionDecision::AlreadyComplete;
    }
    if current.pid != expected.pid || current.epoch != expected.epoch {
        return TransactionCompletionDecision::RejectStaleIdentity;
    }
    if current.state == prepare_state {
        TransactionCompletionDecision::Proceed
    } else {
        TransactionCompletionDecision::RejectState
    }
}
