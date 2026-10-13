#[cfg(creusot)]
use super::{is_identity, snapshot_eq};
use {
    super::{
        TransactionCompletionDecision, TransactionPidInstallDecision,
        TransactionReaperCompletionDecision, TransactionRegistrationDecision,
        TransactionRegistrationFacts, TransactionSnapshot, transaction_completion_decision,
    },
    creusot_std::prelude::*,
};

/// Recheck a prepared snapshot after the abort- or commit-marker fan-out.
///
/// This is [`transaction_completion_decision`] with two additions: every
/// snapshot must name a nonnegative producer identity, because the live entry
/// and the prepared one come from the replayed transaction log; and
/// `exact_prepared_snapshot` must hold for `Proceed`. The host computes it
/// from equality over the complete persisted transaction entry, including its
/// staged identity, partition set, timeout, and timestamps, so a registration
/// or recovery that kept the identity and state still blocks the completion.
///
/// `prepared.state` and `completion.state` are the prepare and complete tags
/// of one Kafka completion, which the host takes from a fixed
/// `Prepare* -> Complete*` pairing, so they differ.
#[requires(prepared.state != completion.state)]
#[ensures((result == TransactionReaperCompletionDecision::RejectMalformed)
    == !(is_identity(current) && is_identity(prepared) && is_identity(completion)))]
#[ensures((result == TransactionReaperCompletionDecision::AlreadyComplete)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && snapshot_eq(current, completion)))]
#[ensures((result == TransactionReaperCompletionDecision::RejectStaleIdentity)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && !snapshot_eq(current, completion)
        && !(current.pid == prepared.pid && current.epoch == prepared.epoch)))]
#[ensures((result == TransactionReaperCompletionDecision::RejectChangedPreparedState)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && !snapshot_eq(current, completion)
        && current.pid == prepared.pid
        && current.epoch == prepared.epoch
        && !(current.state == prepared.state && exact_prepared_snapshot)))]
#[ensures((result == TransactionReaperCompletionDecision::Proceed)
    == (is_identity(current)
        && is_identity(prepared)
        && is_identity(completion)
        && snapshot_eq(current, prepared)
        && exact_prepared_snapshot))]
#[must_use]
pub fn transaction_reaper_completion_decision(
    current: TransactionSnapshot,
    prepared: TransactionSnapshot,
    completion: TransactionSnapshot,
    exact_prepared_snapshot: bool,
) -> TransactionReaperCompletionDecision {
    if current.pid < 0
        || current.epoch < 0
        || prepared.pid < 0
        || prepared.epoch < 0
        || completion.pid < 0
        || completion.epoch < 0
    {
        return TransactionReaperCompletionDecision::RejectMalformed;
    }
    match transaction_completion_decision(
        current,
        prepared.identity(),
        completion.identity(),
        prepared.state,
        completion.state,
    ) {
        TransactionCompletionDecision::AlreadyComplete => {
            TransactionReaperCompletionDecision::AlreadyComplete
        }
        TransactionCompletionDecision::RejectStaleIdentity => {
            TransactionReaperCompletionDecision::RejectStaleIdentity
        }
        TransactionCompletionDecision::RejectState => {
            TransactionReaperCompletionDecision::RejectChangedPreparedState
        }
        TransactionCompletionDecision::Proceed => {
            if exact_prepared_snapshot {
                TransactionReaperCompletionDecision::Proceed
            } else {
                TransactionReaperCompletionDecision::RejectChangedPreparedState
            }
        }
    }
}

/// Admit only a well-formed, uniquely owned producer-ID pair from the
/// transaction log partition selected by its transactional ID.
#[ensures((result == TransactionPidInstallDecision::RejectWrongPartition)
    == !partition_matches)]
#[ensures((result == TransactionPidInstallDecision::RejectCurrentIdentity)
    == (partition_matches && (producer_id@ < 0 || producer_epoch@ < 0)))]
#[ensures((result == TransactionPidInstallDecision::RejectStagedIdentity)
    == (partition_matches
        && producer_id@ >= 0
        && producer_epoch@ >= 0
        && !((next_producer_id@ == -1 && next_producer_epoch@ == -1)
            || (next_producer_id@ >= 0 && next_producer_epoch@ >= 0))))]
#[ensures((result == TransactionPidInstallDecision::RejectCollision)
    == (pid_install_input_valid(partition_matches, producer_id@, producer_epoch@, next_producer_id@, next_producer_epoch@)
        && (!current_owner_matches
            || (next_producer_id@ >= 0 && !next_owner_matches))))]
#[ensures((result == TransactionPidInstallDecision::Apply)
    == (pid_install_input_valid(partition_matches, producer_id@, producer_epoch@, next_producer_id@, next_producer_epoch@)
        && current_owner_matches
        && (next_producer_id@ < 0 || next_owner_matches)))]
#[must_use]
pub fn transaction_pid_install_decision(
    partition_matches: bool,
    producer_id: i64,
    producer_epoch: i16,
    next_producer_id: i64,
    next_producer_epoch: i16,
    current_owner_matches: bool,
    next_owner_matches: bool,
) -> TransactionPidInstallDecision {
    if !partition_matches {
        TransactionPidInstallDecision::RejectWrongPartition
    } else if producer_id < 0 || producer_epoch < 0 {
        TransactionPidInstallDecision::RejectCurrentIdentity
    } else if !((next_producer_id == -1 && next_producer_epoch == -1)
        || (next_producer_id >= 0 && next_producer_epoch >= 0))
    {
        TransactionPidInstallDecision::RejectStagedIdentity
    } else if !current_owner_matches || (next_producer_id >= 0 && !next_owner_matches) {
        TransactionPidInstallDecision::RejectCollision
    } else {
        TransactionPidInstallDecision::Apply
    }
}

open_logic! {
fn registration_generation_ready(facts: TransactionRegistrationFacts) -> bool {
    pearlite! { facts.ownership.is_coordinator
    && facts.ownership.producer_id_valid
    && facts.ownership.entry_exists
    && facts.identity.matching.transactional_id_matches
    && !facts.identity.pending_transition }
}
}

/// Fence a partition registration against coordinator ownership and one exact
/// transactional-id, producer-id, and producer-epoch generation.
#[ensures((result == TransactionRegistrationDecision::RejectNotCoordinator)
    == !facts.ownership.is_coordinator)]
#[ensures((result == TransactionRegistrationDecision::RejectUnknownProducer)
    == (facts.ownership.is_coordinator
        && (!facts.ownership.producer_id_valid
            || !facts.ownership.entry_exists
            || !facts.identity.matching.transactional_id_matches)))]
#[ensures((result == TransactionRegistrationDecision::RejectPendingTransition)
    == (facts.ownership.is_coordinator
        && facts.ownership.producer_id_valid
        && facts.ownership.entry_exists
        && facts.identity.matching.transactional_id_matches
        && facts.identity.pending_transition))]
#[ensures((result == TransactionRegistrationDecision::RejectProducerId)
    == (registration_generation_ready(facts)
        && !facts.identity.matching.producer_id_matches))]
#[ensures((result == TransactionRegistrationDecision::RejectProducerEpoch)
    == (registration_generation_ready(facts)
        && facts.identity.matching.producer_id_matches
        && !facts.identity.matching.producer_epoch_matches))]
#[ensures((result == TransactionRegistrationDecision::RejectState)
    == (registration_generation_ready(facts)
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && !facts.state.state_allows_registration))]
#[ensures((result == TransactionRegistrationDecision::PersistRetry)
    == (registration_generation_ready(facts)
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && facts.state.state_allows_registration
        && facts.state.state_is_ongoing
        && facts.state.exact_partitions_registered))]
#[ensures((result == TransactionRegistrationDecision::PersistRegistration)
    == (registration_generation_ready(facts)
        && facts.identity.matching.producer_id_matches
        && facts.identity.matching.producer_epoch_matches
        && facts.state.state_allows_registration
        && !(facts.state.state_is_ongoing && facts.state.exact_partitions_registered)))]
#[must_use]
pub fn transaction_partition_registration(
    facts: TransactionRegistrationFacts,
) -> TransactionRegistrationDecision {
    if !facts.ownership.is_coordinator {
        TransactionRegistrationDecision::RejectNotCoordinator
    } else if !facts.ownership.producer_id_valid
        || !facts.ownership.entry_exists
        || !facts.identity.matching.transactional_id_matches
    {
        TransactionRegistrationDecision::RejectUnknownProducer
    } else if facts.identity.pending_transition {
        TransactionRegistrationDecision::RejectPendingTransition
    } else if !facts.identity.matching.producer_id_matches {
        TransactionRegistrationDecision::RejectProducerId
    } else if !facts.identity.matching.producer_epoch_matches {
        TransactionRegistrationDecision::RejectProducerEpoch
    } else if !facts.state.state_allows_registration {
        TransactionRegistrationDecision::RejectState
    } else if facts.state.state_is_ongoing && facts.state.exact_partitions_registered {
        TransactionRegistrationDecision::PersistRetry
    } else {
        TransactionRegistrationDecision::PersistRegistration
    }
}

open_logic! {
/// The partition matches and both producer-ID pairs are well formed before ownership checks.
fn pid_install_input_valid(
    partition_matches: bool,
    producer: Int,
    epoch: Int,
    next_producer: Int,
    next_epoch: Int,
) -> bool {
    pearlite! { partition_matches && producer >= 0 && epoch >= 0
    && ((next_producer == -1 && next_epoch == -1) || (next_producer >= 0 && next_epoch >= 0)) }
}
}
