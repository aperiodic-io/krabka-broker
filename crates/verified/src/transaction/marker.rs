#[cfg(creusot)]
use super::{TransactionIdentity, TransactionSnapshot};
use {
    super::{
        LogBatchKind, TransactionMarkerMaterializationDecision, TransactionMarkerPartitionState,
        TransactionMarkerRequest,
    },
    creusot_std::prelude::*,
};

/// Transaction version 2 requires an epoch bump for a pending transaction's
/// marker, except at the final marker epoch. Completed retries remain allowed.
#[ensures(result == (transaction_version@ >= 2
    && producer_epoch == current_producer_epoch
    && has_pending_transaction && producer_epoch@ != i16::MAX@))]
#[must_use]
pub fn transaction_marker_equal_epoch_fenced(
    transaction_version: i16,
    producer_epoch: i16,
    current_producer_epoch: i16,
    has_pending_transaction: bool,
) -> bool {
    transaction_version >= 2
        && producer_epoch == current_producer_epoch
        && has_pending_transaction
        && producer_epoch != i16::MAX
}

#[ensures((result == LogBatchKind::Data) == !is_control)]
#[ensures((result == LogBatchKind::Abort) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 0 && key@[3]@ == 0))]
#[ensures((result == LogBatchKind::Commit) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 0 && key@[3]@ == 1))]
#[ensures((result == LogBatchKind::Barrier) ==
    (is_control && key@.len() >= 4 && key@[2]@ == 3 && key@[3]@ == 232))]
#[ensures((result == LogBatchKind::OtherControl) == (is_control
    && !(key@.len() >= 4 && ((key@[2]@ == 0 && (key@[3]@ == 0 || key@[3]@ == 1))
        || (key@[2]@ == 3 && key@[3]@ == 232)))))]
#[must_use]
pub fn log_batch_kind(is_control: bool, key: &[u8]) -> LogBatchKind {
    if !is_control {
        return LogBatchKind::Data;
    }
    if key.len() < 4 {
        return LogBatchKind::OtherControl;
    }
    match (key[2], key[3]) {
        (0, 0) => LogBatchKind::Abort,
        (0, 1) => LogBatchKind::Commit,
        (3, 232) => LogBatchKind::Barrier,
        _ => LogBatchKind::OtherControl,
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The marker names a nonnegative producer identity, every epoch is at least
/// the `-1` sentinel, and a pending transaction has a real producer epoch.
pub fn marker_well_formed(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        request.producer_id@ >= 0
            && request.producer_epoch@ >= 0
            && request.coordinator_epoch@ >= -1
            && current.producer_epoch@ >= -1
            && current.coordinator_epoch@ >= -1
            && (!current.has_pending_transaction || current.producer_epoch@ >= 0)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Neither the producer nor the coordinator generation of the marker is
/// older than the partition's (`ProducerAppendInfo.checkProducerEpoch` and
/// `appendEndTxnMarker`'s coordinator-epoch check).
pub fn marker_generation_current(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        request.producer_epoch@ >= current.producer_epoch@
            && request.coordinator_epoch@ >= current.coordinator_epoch@
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The marker repeats the exact generation that already closed the
/// producer's transaction on this partition.
pub fn marker_completed_retry(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        !current.has_pending_transaction
            && request.producer_epoch@ == current.producer_epoch@
            && request.coordinator_epoch@ == current.coordinator_epoch@
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Only a COMMIT that closes a pending transaction on `__consumer_offsets`
/// publishes the transaction's offset commits.
pub fn marker_publishes_offsets(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! {
        current.has_pending_transaction && request.is_commit && request.is_offsets_partition
    }
}

/// Fence a transaction marker against the partition's latest producer and
/// coordinator generations, suppress an exact completed retry, and publish
/// offsets only for a pending commit on `__consumer_offsets`.
#[ensures((result == TransactionMarkerMaterializationDecision::RejectMalformed)
    == !marker_well_formed(request, current))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectProducerEpoch)
    == (marker_well_formed(request, current)
        && request.producer_epoch@ < current.producer_epoch@))]
#[ensures((result == TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch)
    == (marker_well_formed(request, current)
        && request.producer_epoch@ >= current.producer_epoch@
        && request.coordinator_epoch@ < current.coordinator_epoch@))]
#[ensures((result == TransactionMarkerMaterializationDecision::Retry)
    == (marker_well_formed(request, current) && marker_completed_retry(request, current)))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendAndPublishOffsets)
    == (marker_well_formed(request, current)
        && marker_generation_current(request, current)
        && marker_publishes_offsets(request, current)))]
#[ensures((result == TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication)
    == (marker_well_formed(request, current)
        && marker_generation_current(request, current)
        && !marker_completed_retry(request, current)
        && !marker_publishes_offsets(request, current)))]
#[must_use]
pub fn transaction_marker_materialization_decision(
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> TransactionMarkerMaterializationDecision {
    if request.producer_id < 0
        || request.producer_epoch < 0
        || request.coordinator_epoch < -1
        || current.producer_epoch < -1
        || current.coordinator_epoch < -1
        || (current.has_pending_transaction && current.producer_epoch == -1)
    {
        TransactionMarkerMaterializationDecision::RejectMalformed
    } else if request.producer_epoch < current.producer_epoch {
        TransactionMarkerMaterializationDecision::RejectProducerEpoch
    } else if request.coordinator_epoch < current.coordinator_epoch {
        TransactionMarkerMaterializationDecision::RejectCoordinatorEpoch
    } else if !current.has_pending_transaction
        && request.producer_epoch == current.producer_epoch
        && request.coordinator_epoch == current.coordinator_epoch
    {
        TransactionMarkerMaterializationDecision::Retry
    } else if current.has_pending_transaction && request.is_commit && request.is_offsets_partition {
        TransactionMarkerMaterializationDecision::AppendAndPublishOffsets
    } else {
        TransactionMarkerMaterializationDecision::AppendWithoutOffsetPublication
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// `snapshot` names a producer identity: its PID and epoch are nonnegative.
pub fn is_identity(snapshot: TransactionSnapshot) -> bool {
    pearlite! { snapshot.pid@ >= 0 && snapshot.epoch@ >= 0 }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// `snapshot` holds the producer identity `identity`.
pub fn has_identity(snapshot: TransactionSnapshot, identity: TransactionIdentity) -> bool {
    pearlite! { snapshot.pid == identity.pid && snapshot.epoch == identity.epoch }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Two snapshots agree on producer identity and state.
pub fn snapshot_eq(left: TransactionSnapshot, right: TransactionSnapshot) -> bool {
    pearlite! {
        left.pid == right.pid && left.epoch == right.epoch && left.state == right.state
    }
}
