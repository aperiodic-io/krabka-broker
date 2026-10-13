use {super::*, assert2::assert};

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
            marker().producer_id(-1).call(),
            partition().producer_epoch(-1).coordinator_epoch(-1).call(),
            RejectMalformed,
        ),
        // A negative marker epoch.
        (
            marker().producer_epoch(-1).call(),
            partition().call(),
            RejectMalformed,
        ),
        // A coordinator epoch below the `-1` sentinel.
        (
            marker()
                .coordinator_epoch(-2)
                .outcome(TransactionOutcome::Abort)
                .target(MarkerTarget::Data)
                .call(),
            partition().coordinator_epoch(-1).call(),
            RejectMalformed,
        ),
        // Partition state below the sentinels.
        (
            marker().call(),
            partition().producer_epoch(-2).coordinator_epoch(-1).call(),
            RejectMalformed,
        ),
        (
            marker().call(),
            partition()
                .coordinator_epoch(-2)
                .pending(PendingTransaction::Absent)
                .call(),
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
            marker().call(),
            partition().producer_epoch(-1).call(),
            RejectMalformed,
        ),
        // `ProducerFencedException`: the marker's epoch is older.
        (
            marker().producer_epoch(2).coordinator_epoch(9).call(),
            partition().producer_epoch(3).coordinator_epoch(8).call(),
            RejectProducerEpoch,
        ),
        // `TransactionCoordinatorFencedException`: an older coordinator.
        (
            marker().producer_epoch(3).coordinator_epoch(7).call(),
            partition().producer_epoch(3).coordinator_epoch(8).call(),
            RejectCoordinatorEpoch,
        ),
        // The same generation already closed the transaction: append
        // nothing.
        (
            committed_marker(),
            partition()
                .producer_epoch(3)
                .coordinator_epoch(8)
                .pending(PendingTransaction::Absent)
                .call(),
            Retry,
        ),
        (
            marker().call(),
            partition().pending(PendingTransaction::Absent).call(),
            Retry,
        ),
        // A pending COMMIT on `__consumer_offsets` publishes its offsets.
        (
            committed_marker(),
            partition().producer_epoch(3).coordinator_epoch(7).call(),
            AppendAndPublishOffsets,
        ),
        // An ABORT, a data partition, or no pending transaction appends
        // without publishing.
        (
            marker()
                .producer_epoch(3)
                .coordinator_epoch(8)
                .outcome(TransactionOutcome::Abort)
                .call(),
            partition().producer_epoch(3).coordinator_epoch(7).call(),
            AppendWithoutOffsetPublication,
        ),
        (
            marker()
                .producer_epoch(3)
                .coordinator_epoch(8)
                .target(MarkerTarget::Data)
                .call(),
            partition().producer_epoch(3).coordinator_epoch(7).call(),
            AppendWithoutOffsetPublication,
        ),
        (
            marker()
                .coordinator_epoch(-1)
                .outcome(TransactionOutcome::Abort)
                .target(MarkerTarget::Data)
                .call(),
            partition().coordinator_epoch(-1).call(),
            AppendWithoutOffsetPublication,
        ),
        // A newer generation than the closed one is not a retry.
        (
            marker().producer_epoch(4).coordinator_epoch(9).call(),
            partition()
                .producer_epoch(3)
                .coordinator_epoch(8)
                .pending(PendingTransaction::Absent)
                .call(),
            AppendWithoutOffsetPublication,
        ),
        (
            marker().call(),
            partition()
                .producer_epoch(-1)
                .pending(PendingTransaction::Absent)
                .call(),
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
