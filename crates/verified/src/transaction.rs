//! Transaction-completion fencing after the `EndTxn` marker fan-out.

use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The stable frontier is the minimum pending start, or the end of an idle log.
pub(crate) fn first_unstable_frontier(starts: Seq<i64>, log_end: Int, lso: Int) -> bool {
    pearlite! {
        lso <= log_end
        && (forall<i: Int> 0 <= i && i < starts.len() ==> starts[i]@ <= log_end)
        && ((starts.len() == 0 && lso == log_end)
            || (starts.len() > 0
                && (exists<i: Int> 0 <= i && i < starts.len() && lso == starts[i]@)
                && (forall<i: Int> 0 <= i && i < starts.len() ==> lso <= starts[i]@)))
    }
}

/// Classification of the actual first control-record key, shared by live
/// append and recovery. Versions are ignored as in Kafka's marker type parser.
#[krabka_macros::model_type]
pub enum LogBatchKind {
    Data,
    Abort,
    Commit,
    Barrier,
    OtherControl,
}

/// Whether one transaction record may install its live producer identities.
#[krabka_macros::model_type]
pub enum TransactionPidInstallDecision {
    RejectWrongPartition,
    RejectCurrentIdentity,
    RejectStagedIdentity,
    RejectCollision,
    Apply,
}

/// Whether one transaction marker may append and publish committed offsets.
#[krabka_macros::model_type]
pub enum TransactionMarkerMaterializationDecision {
    RejectMalformed,
    RejectProducerEpoch,
    RejectCoordinatorEpoch,
    Retry,
    AppendWithoutOffsetPublication,
    AppendAndPublishOffsets,
}

/// One transaction marker as it reaches one data partition.
#[krabka_macros::model_type]
pub struct TransactionMarkerRequest {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// The sending coordinator's epoch; `-1` names no coordinator generation.
    pub coordinator_epoch: i32,
    /// The marker is a COMMIT rather than an ABORT.
    pub is_commit: bool,
    /// The partition is a `__consumer_offsets` partition.
    pub is_offsets_partition: bool,
}

/// The partition's latest producer state for the marker's producer ID.
#[krabka_macros::model_type]
pub struct TransactionMarkerPartitionState {
    /// The latest producer epoch, or `-1` when the partition has none.
    pub producer_epoch: i16,
    /// The latest coordinator epoch, or `-1` when the partition has none.
    pub coordinator_epoch: i32,
    /// The producer has an open transaction on this partition.
    pub has_pending_transaction: bool,
}

/// Whether the idle reaper or the completion task may publish one prepared
/// completion.
#[krabka_macros::model_type]
pub enum TransactionReaperCompletionDecision {
    /// A snapshot names a negative producer ID or epoch.
    RejectMalformed,
    /// The live entry no longer holds the prepared producer identity.
    RejectStaleIdentity,
    /// The live entry holds the prepared identity but is no longer the exact
    /// prepared snapshot.
    RejectChangedPreparedState,
    /// The live entry already holds the intended completion.
    AlreadyComplete,
    /// The live entry is the exact prepared snapshot.
    Proceed,
}

/// Whether one transaction generation may persist a partition registration.
///
/// The ordering mirrors Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction`:
/// a pending transition (`pendingTransitionInProgress`) is checked first so it
/// can complete before any identity check runs, then the producer id, then the
/// producer epoch, then the `PrepareCommit`/`PrepareAbort` state check. Every
/// one of those four rejections is retriable on Kafka's wire
/// (`CONCURRENT_TRANSACTIONS`, `INVALID_PRODUCER_ID_MAPPING`, or
/// `PRODUCER_FENCED`), which the host maps; this module only orders the facts.
#[krabka_macros::model_type]
pub enum TransactionRegistrationDecision {
    RejectNotCoordinator,
    RejectUnknownProducer,
    RejectPendingTransition,
    RejectProducerId,
    RejectProducerEpoch,
    RejectState,
    PersistRetry,
    PersistRegistration,
}

/// Facts used to fence one transaction partition registration.
#[krabka_macros::model_type]
pub struct TransactionRegistrationFacts {
    pub ownership: TransactionRegistrationOwnershipFacts,
    pub identity: TransactionRegistrationIdentityFacts,
    pub state: TransactionRegistrationStateFacts,
}

#[krabka_macros::model_type]
pub struct TransactionRegistrationOwnershipFacts {
    pub is_coordinator: bool,
    pub producer_id_valid: bool,
    pub entry_exists: bool,
}

#[krabka_macros::model_type]
pub struct TransactionRegistrationIdentityFacts {
    /// Kafka's `txnMetadata.pendingTransitionInProgress`: a staged producer
    /// identity is waiting on an older transaction to complete first.
    pub pending_transition: bool,
    pub matching: TransactionRegistrationIdentityMatchFacts,
}

#[krabka_macros::model_type]
pub struct TransactionRegistrationIdentityMatchFacts {
    pub transactional_id_matches: bool,
    pub producer_id_matches: bool,
    pub producer_epoch_matches: bool,
}

#[krabka_macros::model_type]
pub struct TransactionRegistrationStateFacts {
    pub state_allows_registration: bool,
    /// Kafka's `txnMetadata.state == ONGOING`. The retry optimization below
    /// applies only in that exact state, never against a stale partition set
    /// left over from a completed or not-yet-started transaction.
    pub state_is_ongoing: bool,
    pub exact_partitions_registered: bool,
}

/// Whether an `EndTxn` caller may finalize the entry it prepared.
#[krabka_macros::model_type]
pub enum TransactionCompletionDecision {
    Proceed,
    AlreadyComplete,
    RejectStaleIdentity,
    RejectState,
}

/// Sentinel persisted for a transaction controlled by an external 2PC owner.
pub const NO_TRANSACTION_TIMEOUT_MS: i32 = i32::MAX;

/// State fact needed by the idle-transaction reaper.
#[krabka_macros::model_type]
pub enum IdleTransactionState {
    Ongoing,
    Other,
}

/// The persisted identity and state observed after marker fan-out.
#[krabka_macros::model_type]
pub struct TransactionSnapshot {
    pub pid: i64,
    pub epoch: i16,
    pub state: i8,
}

/// A producer identity captured before marker fan-out.
#[krabka_macros::model_type]
pub struct TransactionIdentity {
    pub pid: i64,
    pub epoch: i16,
}

impl TransactionSnapshot {
    /// The producer identity this snapshot holds.
    #[ensures(result.pid == self.pid && result.epoch == self.epoch)]
    #[must_use]
    pub fn identity(self) -> TransactionIdentity {
        TransactionIdentity {
            pid: self.pid,
            epoch: self.epoch,
        }
    }
}

/// What `InitProducerId` does with the producer identity a caller supplies.
#[krabka_macros::model_type]
pub enum InitProducerIdIdentityDecision {
    /// The caller names no identity. Kafka bumps the epoch and records no
    /// last epoch (`prepareIncrementProducerEpoch` with an empty expected
    /// epoch).
    BumpWithoutIdentity,
    /// The caller names the entry's live epoch. Kafka bumps the epoch and
    /// records the epoch it held as the last epoch.
    Bump,
    /// The caller names the epoch the entry held before its last bump. Kafka
    /// treats this as a retry of the call that made that bump: it answers the
    /// entry's identity and writes nothing.
    Retry,
    /// Kafka answers `PRODUCER_FENCED`.
    Fenced,
}

/// The epoch at and above which Kafka treats a producer epoch as exhausted
/// (`TransactionMetadata.isEpochExhausted`). The coordinator keeps one epoch
/// in hand to fence the producer with, so it never hands out `i16::MAX`.
pub const EXHAUSTED_PRODUCER_EPOCH: i16 = i16::MAX - 1;

mod marker;
#[cfg(creusot)]
pub use marker::{
    has_identity, is_identity, marker_completed_retry, marker_generation_current,
    marker_publishes_offsets, marker_well_formed, snapshot_eq,
};
pub use marker::{
    log_batch_kind, transaction_marker_equal_epoch_fenced,
    transaction_marker_materialization_decision,
};

mod reaper;
pub use reaper::{
    transaction_partition_registration, transaction_pid_install_decision,
    transaction_reaper_completion_decision,
};

mod next_producer_identity;
pub use next_producer_identity::{
    aborted_transaction_interval, aborted_transaction_overlaps, first_unstable_offset,
    next_producer_identity, should_abort_idle_transaction, transaction_marker_closes,
};

mod abort_rows;
pub use abort_rows::unique_aborted_transaction_rows;

mod producer_identity;
pub use producer_identity::{init_producer_id_identity_decision, transaction_completion_decision};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every prefix bounded by the log and live transactions is below this limit.
pub(crate) fn unstable_fetch_limit_maximal(
    starts: Seq<i64>,
    log_end: Int,
    high_watermark: Int,
    deliverable: Int,
    limit: Int,
) -> bool {
    pearlite! { forall<v: Int> v <= log_end && v <= high_watermark && v <= deliverable
    && (forall<i: Int> 0 <= i && i < starts.len() ==> v <= starts[i]@) ==> v <= limit }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every coherent pending transaction bounds both stability and the consumer fetch prefix.
pub(crate) fn pending_starts_bound_fetch(
    starts: Seq<i64>,
    end: Int,
    stable: Int,
    limit: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < starts.len()
    ==> starts[i]@ <= end && stable <= starts[i]@ && limit <= starts[i]@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every remaining transaction starts inside the prefix preceding the marker.
pub(crate) fn pending_starts_before_marker(starts: Seq<i64>, base: Int) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < starts.len() ==> 0 <= starts[i]@ && starts[i]@ <= base }
}

#[cfg(test)]
mod tests;
