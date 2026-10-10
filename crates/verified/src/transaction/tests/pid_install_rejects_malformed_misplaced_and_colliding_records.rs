use assert2::assert;

use super::*;

#[test]
fn pid_install_rejects_malformed_misplaced_and_colliding_records() {
    use TransactionPidInstallDecision::{
        Apply, RejectCollision, RejectCurrentIdentity, RejectStagedIdentity, RejectWrongPartition,
    };

    for (arguments, expected) in [
        ((false, 1, 0, -1, -1, true, true), RejectWrongPartition),
        ((true, -1, 0, -1, -1, true, true), RejectCurrentIdentity),
        ((true, 1, -1, -1, -1, true, true), RejectCurrentIdentity),
        ((true, 1, 0, 2, -1, true, true), RejectStagedIdentity),
        ((true, 1, 0, -1, 0, true, true), RejectStagedIdentity),
        ((true, 1, 0, 1, 0, true, true), Apply),
        ((true, 1, 0, -1, -1, false, true), RejectCollision),
        ((true, 1, 0, 2, 0, true, false), RejectCollision),
        ((true, 1, 0, -1, -1, true, true), Apply),
        ((true, 0, 0, -1, -1, true, true), Apply),
        ((true, 1, 0, 2, 0, true, true), Apply),
    ] {
        assert!(
            transaction_pid_install_decision(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                arguments.6,
            ) == expected
        );
    }
}

fn malformed_marker_cases() -> Vec<(
    TransactionMarkerRequest,
    TransactionMarkerPartitionState,
    TransactionMarkerMaterializationDecision,
)> {
    use TransactionMarkerMaterializationDecision::RejectMalformed;
    std::vec![
        // A negative producer ID names no producer.
        (
            marker(MarkerSetup {
                producer_id: ProducerId(-1),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(-1),
                coordinator_epoch: CoordinatorEpoch(-1),
                ..Default::default()
            }),
            RejectMalformed,
        ),
        // A negative marker epoch.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(-1),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup::default()),
            RejectMalformed,
        ),
        // A coordinator epoch below the `-1` sentinel.
        (
            marker(MarkerSetup {
                coordinator_epoch: CoordinatorEpoch(-2),
                outcome: TransactionOutcome::Abort,
                target: MarkerTarget::Data,
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                coordinator_epoch: CoordinatorEpoch(-1),
                ..Default::default()
            }),
            RejectMalformed,
        ),
        // Partition state below the sentinels.
        (
            marker(MarkerSetup::default()),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(-2),
                coordinator_epoch: CoordinatorEpoch(-1),
                ..Default::default()
            }),
            RejectMalformed,
        ),
        (
            marker(MarkerSetup::default()),
            partition(MarkerPartitionSetup {
                coordinator_epoch: CoordinatorEpoch(-2),
                pending: PendingTransaction::Absent,
                ..Default::default()
            }),
            RejectMalformed,
        ),
    ]
}

fn marker_fencing_and_publication_cases() -> Vec<(
    TransactionMarkerRequest,
    TransactionMarkerPartitionState,
    TransactionMarkerMaterializationDecision,
)> {
    use TransactionMarkerMaterializationDecision::{
        AppendAndPublishOffsets, AppendWithoutOffsetPublication, RejectCoordinatorEpoch,
        RejectMalformed, RejectProducerEpoch, Retry,
    };
    std::vec![
        // A pending transaction without a producer epoch.
        (
            marker(MarkerSetup::default()),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(-1),
                ..Default::default()
            }),
            RejectMalformed,
        ),
        // `ProducerFencedException`: the marker's epoch is older.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(2),
                coordinator_epoch: CoordinatorEpoch(9),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                ..Default::default()
            }),
            RejectProducerEpoch,
        ),
        // `TransactionCoordinatorFencedException`: an older coordinator.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(7),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                ..Default::default()
            }),
            RejectCoordinatorEpoch,
        ),
        // The same generation already closed the transaction: append
        // nothing.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                pending: PendingTransaction::Absent,
            }),
            Retry,
        ),
        (
            marker(MarkerSetup::default()),
            partition(MarkerPartitionSetup {
                pending: PendingTransaction::Absent,
                ..Default::default()
            }),
            Retry,
        ),
        // A pending COMMIT on `__consumer_offsets` publishes its offsets.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(7),
                ..Default::default()
            }),
            AppendAndPublishOffsets,
        ),
        // An ABORT, a data partition, or no pending transaction appends
        // without publishing.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                outcome: TransactionOutcome::Abort,
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(7),
                ..Default::default()
            }),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                target: MarkerTarget::Data,
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(7),
                ..Default::default()
            }),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(MarkerSetup {
                coordinator_epoch: CoordinatorEpoch(-1),
                outcome: TransactionOutcome::Abort,
                target: MarkerTarget::Data,
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                coordinator_epoch: CoordinatorEpoch(-1),
                ..Default::default()
            }),
            AppendWithoutOffsetPublication,
        ),
        // A newer generation than the closed one is not a retry.
        (
            marker(MarkerSetup {
                producer_epoch: ProducerEpoch(4),
                coordinator_epoch: CoordinatorEpoch(9),
                ..Default::default()
            }),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(3),
                coordinator_epoch: CoordinatorEpoch(8),
                pending: PendingTransaction::Absent,
            }),
            AppendWithoutOffsetPublication,
        ),
        (
            marker(MarkerSetup::default()),
            partition(MarkerPartitionSetup {
                producer_epoch: ProducerEpoch(-1),
                pending: PendingTransaction::Absent,
                ..Default::default()
            }),
            AppendWithoutOffsetPublication,
        ),
    ]
}

/// Kafka `ProducerStateManager`'s `appendEndTxnMarker` fencing, plus the
/// exact-retry and offset-publication rules.
#[test]
fn marker_materialization_fences_retries_and_offset_publication() {
    for (request, current, expected) in malformed_marker_cases()
        .into_iter()
        .chain(marker_fencing_and_publication_cases())
    {
        assert!(
            transaction_marker_materialization_decision(request, current) == expected,
            "request={request:?}, current={current:?}"
        );
    }
}
