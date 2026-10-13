//! Shared initialization transitions and independent Kafka fence oracle for the
//! transaction decision and timeout models.

use {
    super::{
        coordinator::completion::{
            PreparedCompletion, apply_completion, completion_decision, completion_for,
        },
        handlers::end_txn::{
            completion_producer_identity, prepare_server_abort_identities_with_fresh,
        },
        state::{TxnEntry, TxnState},
        version::TxnVersion,
    },
    krabka_log::ProducerId,
    krabka_verified::transaction::TransactionReaperCompletionDecision,
};

/// Drive the common begin gate and generation stamp, with an optional start-clock write.
pub(crate) fn begin_transaction(
    status: &mut i8,
    generation: &mut i16,
    epoch: i16,
    start: impl FnOnce(),
) -> Option<()> {
    let prior = TxnState::from_kafka_status(*status).expect("valid TxnState id in model");
    if !prior.can_transition_to(TxnState::Ongoing) {
        return None;
    }
    if prior != TxnState::Ongoing {
        start();
        *generation = epoch;
    }
    *status = TxnState::Ongoing.to_kafka_status();
    Some(())
}

pub struct Initialized {
    pub entry: TxnEntry,
    pub fence_matches: bool,
    pub reset: bool,
}

/// Kafka's coordinator abort, restated from `TransactionMetadata` rather than
/// read back from the production function. The epoch advances once; `TV_2`
/// records the held epoch and version, while earlier versions clear both.
pub fn fenced_as_kafka(entry: &TxnEntry, held: i16, version: TxnVersion) -> bool {
    let verified = version == TxnVersion::Verified;
    entry.producer_epoch == held + 1
        && entry.last_producer_epoch == if verified { held } else { -1 }
        && entry.client_transaction_version == if verified { 2 } else { 0 }
        && completion_producer_identity(entry) == (entry.producer_id, held + 1)
}

/// Drive the handler's prepared-state gate, ongoing fence and terminal reset.
/// An ongoing fence retains its previous timeout until completion and retry.
pub fn initialize(
    mut entry: TxnEntry,
    version: TxnVersion,
    timeout_ms: i32,
    now_ms: i64,
) -> Option<Initialized> {
    if completion_for(entry.state).is_some() {
        return None;
    }
    let reset = entry.state != TxnState::Ongoing;
    let fence_matches = if reset {
        let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
            true,
            false,
            entry.producer_id.get(),
            entry.producer_epoch,
            None,
        )
        .expect("model epochs never reach the rotation boundary");
        entry = TxnEntry::new_empty(
            "tid".to_string(),
            ProducerId(pid),
            epoch,
            timeout_ms,
            now_ms,
        );
        true
    } else {
        let held = entry.producer_epoch;
        entry.state = TxnState::PrepareAbort;
        prepare_server_abort_identities_with_fresh(&mut entry, version, None)
            .expect("model epochs never reach the rotation boundary");
        fenced_as_kafka(&entry, held, version)
    };
    Some(Initialized {
        entry,
        fence_matches,
        reset,
    })
}

/// Complete the prepared snapshot atomically with the production completion
/// gate and identity. Each model keeps its own finalization history and oracle.
pub fn complete_prepared(entry: &TxnEntry, now_ms: i64) -> Option<(TxnEntry, TxnState)> {
    let PreparedCompletion { complete, .. } = completion_for(entry.state)?;
    match completion_decision(entry, entry, (entry.state, complete)) {
        TransactionReaperCompletionDecision::Proceed => {
            let mut completed = entry.clone();
            let identity = completion_producer_identity(&completed);
            apply_completion(&mut completed, complete, identity, now_ms);
            Some((completed, complete))
        }
        TransactionReaperCompletionDecision::AlreadyComplete
        | TransactionReaperCompletionDecision::RejectMalformed
        | TransactionReaperCompletionDecision::RejectStaleIdentity
        | TransactionReaperCompletionDecision::RejectChangedPreparedState => None,
    }
}

#[cfg(test)]
mod begin_tests {
    use {super::begin_transaction, crate::txn::state::TxnState, assert2::assert, std::cell::Cell};

    #[test]
    fn begin_stamps_only_a_fresh_transaction_and_refusal_keeps_every_field() {
        struct BeginCase {
            prior: TxnState,
            allowed: bool,
            generation: i16,
            starts: usize,
        }
        for case in [
            BeginCase {
                prior: TxnState::Empty,
                allowed: true,
                generation: 9,
                starts: 1,
            },
            BeginCase {
                prior: TxnState::Ongoing,
                allowed: true,
                generation: 4,
                starts: 0,
            },
            BeginCase {
                prior: TxnState::PrepareCommit,
                allowed: false,
                generation: 4,
                starts: 0,
            },
        ] {
            let mut status = case.prior.to_kafka_status();
            let mut generation = 4;
            let starts = Cell::new(0);
            let result = begin_transaction(&mut status, &mut generation, 9, || {
                starts.set(starts.get() + 1);
            });
            assert!(result.is_some() == case.allowed);
            assert!(generation == case.generation && starts.get() == case.starts);
            assert!(
                status
                    == if case.allowed {
                        TxnState::Ongoing
                    } else {
                        case.prior
                    }
                    .to_kafka_status()
            );
        }
    }
}
