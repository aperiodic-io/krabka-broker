//! Shared initialization transitions and independent Kafka fence oracle for the
//! transaction decision and timeout models.

use {
    super::{
        coordinator::completion::{apply_completion, completion_decision, completion_for},
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
macro_rules! begin_transaction {
    ($state:ident; $($start:tt)*) => {
        let prior = st($state.state);
        if !prior.can_transition_to(TxnState::Ongoing) {
            return None;
        }
        if prior != TxnState::Ongoing {
            $($start)*
            $state.generation = $state.epoch;
        }
        $state.state = TxnState::Ongoing.to_kafka_status();
        Some(())
    };
}
pub(crate) use begin_transaction;

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
    let (_, complete) = completion_for(entry.state)?;
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
