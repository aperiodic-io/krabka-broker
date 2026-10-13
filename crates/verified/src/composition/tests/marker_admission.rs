use {
    super::admitted_marker_bounds_committed_fetch,
    crate::transaction::{
        TransactionMarkerMaterializationDecision as Decision, TransactionMarkerPartitionState,
        TransactionMarkerRequest,
    },
    assert2::assert,
    proptest::prelude::*,
};

fn check(
    version: i16,
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
    start: i64,
    span: (i64, i32),
    others: &[i64],
    caps: (i64, i64),
) {
    let admitted = request.producer_id >= 0
        && request.producer_epoch >= current.producer_epoch
        && request.coordinator_epoch >= current.coordinator_epoch
        && (version < 2
            || request.producer_epoch > current.producer_epoch
            || request.producer_epoch == i16::MAX);
    let last = span.0 + i64::from(span.1);
    let before = others
        .iter()
        .copied()
        .fold(start, i64::min)
        .min(caps.0)
        .min(caps.1);
    let after = if admitted && caps.0 > last {
        others
            .iter()
            .copied()
            .fold(last + 1, i64::min)
            .min(caps.0)
            .min(caps.1)
    } else {
        before
    };
    let aborted = (admitted && !request.is_commit).then_some((start, last));
    let actual = admitted_marker_bounds_committed_fetch(
        version, request, current, start, span, others, caps,
    );
    assert!((actual.1, actual.2, actual.3) == (before, after, aborted));
    let append = matches!(
        actual.0,
        Decision::AppendAndPublishOffsets | Decision::AppendWithoutOffsetPublication
    );
    assert!(append == admitted && actual.0 != Decision::Retry);
    assert!(
        (actual.0 == Decision::AppendAndPublishOffsets)
            == (admitted && request.is_commit && request.is_offsets_partition)
    );
}

#[test]
fn stale_generations_and_unreplicated_markers_cannot_release_a_transaction() {
    let current = TransactionMarkerPartitionState {
        producer_epoch: 3,
        coordinator_epoch: 5,
        has_pending_transaction: true,
    };
    for version in [i16::MIN, 1, 2, i16::MAX] {
        for pid in [-1, 0, i64::MAX] {
            for epoch in [-1, 2, 3, 4, i16::MAX] {
                for coordinator in [-2, 4, 5, i32::MAX] {
                    for commit in [false, true] {
                        for offsets in [false, true] {
                            let request = TransactionMarkerRequest {
                                producer_id: pid,
                                producer_epoch: epoch,
                                coordinator_epoch: coordinator,
                                is_commit: commit,
                                is_offsets_partition: offsets,
                            };
                            for others in [&[][..], &[1], &[3], &[7]] {
                                for caps in [(10, 11), (11, 11), (11, 2), (i64::MIN, i64::MAX)] {
                                    check(version, request, current, 3, (8, 2), others, caps);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn final_epoch_exception_and_maximum_offset_preserve_progress() {
    let current = TransactionMarkerPartitionState {
        producer_epoch: i16::MAX,
        coordinator_epoch: i32::MAX,
        has_pending_transaction: true,
    };
    let request = TransactionMarkerRequest {
        producer_id: i64::MAX,
        producer_epoch: i16::MAX,
        coordinator_epoch: i32::MAX,
        is_commit: false,
        is_offsets_partition: true,
    };
    check(
        2,
        request,
        current,
        i64::MAX - 2,
        (i64::MAX - 1, 0),
        &[],
        (i64::MAX, i64::MAX),
    );
    let result = admitted_marker_bounds_committed_fetch(
        2,
        request,
        current,
        i64::MAX - 2,
        (i64::MAX - 1, 0),
        &[],
        (i64::MAX, i64::MAX),
    );
    assert!(result.2 == i64::MAX && result.2 > result.1);
}

proptest! {
    #[test]
    fn admitted_visibility_matches_the_complete_open_transaction_oracle(
        version in any::<i16>(), pid in any::<i64>(), epoch in any::<i16>(),
        coordinator in any::<i32>(), current_epoch in 0_i16..=i16::MAX,
        current_coordinator in -1_i32..=i32::MAX, commit in any::<bool>(), offsets in any::<bool>(),
        base in 0_i64..=100, start in 0_i64..=100, delta in 0_i32..=5,
        others in prop::collection::vec(0_i64..=100, 0..16),
        hw in any::<i64>(), delivery in any::<i64>(),
    ) {
        let others: Vec<_> = others.into_iter().map(|offset| offset.min(base)).collect();
        let current = TransactionMarkerPartitionState { producer_epoch: current_epoch,
            coordinator_epoch: current_coordinator, has_pending_transaction: true };
        let request = TransactionMarkerRequest { producer_id: pid, producer_epoch: epoch,
            coordinator_epoch: coordinator, is_commit: commit, is_offsets_partition: offsets };
        check(version, request, current, start.min(base), (base, delta), &others, (hw, delivery));
    }
}
