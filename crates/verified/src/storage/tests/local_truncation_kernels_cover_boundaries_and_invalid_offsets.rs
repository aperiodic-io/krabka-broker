use super::*;

krabka_macros::remote_segment_transition_matrix!(expected_transitions);

#[test]
fn local_truncation_kernels_cover_boundaries_and_invalid_offsets() {
    assert2::check!(
        local_truncation_plan(&[0, 10, 20], Some(30), 20)
            == LocalTruncationPlan {
                retained_sealed: 2,
                keep_active: false,
            }
    );
    assert2::check!(
        local_truncation_plan(&[0, 10], Some(15), 20)
            == LocalTruncationPlan {
                retained_sealed: 2,
                keep_active: true,
            }
    );
    assert2::check!(
        local_truncation_plan(&[0, 10], Some(20), 20)
            == LocalTruncationPlan {
                retained_sealed: 2,
                keep_active: false,
            }
    );

    assert2::check!(truncation_relative_offset(0, 0) == Some(0));
    assert2::check!(truncation_relative_offset(0, i64::from(u32::MAX)) == Some(u32::MAX));
    assert2::check!(truncation_relative_offset(10, 15) == Some(5));
    assert2::check!(truncation_relative_offset(-1, 0).is_none());
    assert2::check!(truncation_relative_offset(10, 9).is_none());
    assert2::check!(truncation_relative_offset(0, i64::from(u32::MAX) + 1).is_none());

    assert2::check!(truncation_batch_retained(19, 20));
    assert2::check!(!truncation_batch_retained(20, 20));
    assert2::check!(truncation_frontier(12, 20) == 12);
    assert2::check!(truncation_frontier(21, 20) == 20);
}

#[test]
fn local_append_coordinates_admit_the_frontier_or_beyond_and_fail_closed() {
    assert2::check!(local_append_coordinates(0, 0, 0) == Some((0, 1)));
    assert2::check!(local_append_coordinates(10, 10, 2) == Some((12, 13)));
    // A base past the frontier is a hole in the offsets, not a refusal.
    assert2::check!(local_append_coordinates(10, 12, 2) == Some((14, 15)));
    assert2::check!(local_append_coordinates(10, 9, 0).is_none());
    assert2::check!(local_append_coordinates(-1, -1, 0).is_none());
    assert2::check!(local_append_coordinates(10, 10, -1).is_none());
    assert2::check!(local_append_coordinates(i64::MAX, i64::MAX, 0).is_none());
    assert2::check!(
        local_append_coordinates(i64::MAX - 1, i64::MAX - 1, 0) == Some((i64::MAX - 1, i64::MAX))
    );
}

#[test]
fn future_log_swap_requires_equal_frontiers() {
    assert2::assert!(future_log_swap_admission(7, 7));
    assert2::assert!(!future_log_swap_admission(7, 6));
    assert2::assert!(!future_log_swap_admission(7, 8));
}

#[test]
fn remote_segment_transition_matrix_matches_kafka() {
    use RemoteSegmentLifecycle::{CopyFinished, CopyStarted, DeleteFinished, DeleteStarted};

    let states = [CopyStarted, CopyFinished, DeleteStarted, DeleteFinished];
    // Kafka's `RemoteLogSegmentState.isValidTransition`, row = source.
    let expected = expected_transitions();
    for (from, row) in states.into_iter().zip(expected) {
        for (to, want) in states.into_iter().zip(row) {
            assert2::check!(
                remote_segment_transition(from, to) == want,
                "{from:?} -> {to:?}"
            );
        }
    }
}

#[test]
fn remote_partition_delete_transition_matrix_matches_kafka() {
    use RemotePartitionDeleteLifecycle::{Finished, Marked, Started};

    // Kafka's `RemotePartitionDeleteState.isValidTransition`, row =
    // source, the first row being no prior state.
    let expected = [
        (None, [true, false, false]),
        (Some(Marked), [true, true, false]),
        (Some(Started), [false, true, true]),
        (Some(Finished), [false, false, true]),
    ];
    for (from, row) in expected {
        for (to, want) in [Marked, Started, Finished].into_iter().zip(row) {
            assert2::check!(
                remote_partition_delete_transition(from, to) == want,
                "{from:?} -> {to:?}"
            );
        }
    }
}

#[yare::parameterized(
    cannot_resurrect = {
        "an update cannot resurrect",
        None,
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::Reject,
    },
    missing_tombstone = {
        "a missing tombstone is idempotent",
        None,
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Noop,
    },
    exact_retry = {
        "an exact retry",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyStarted,
        true,
        RemoteCacheAction::Noop,
    },
    conflicting_duplicate = {
        "a conflicting duplicate",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyStarted,
        false,
        RemoteCacheAction::Reject,
    },
    copy_finishes = {
        "the copy finishes",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::StoreFinished,
    },
    unfinished_copy_deleted = {
        "an unfinished copy is deleted",
        Some(RemoteSegmentLifecycle::CopyStarted),
        RemoteSegmentLifecycle::DeleteStarted,
        false,
        RemoteCacheAction::StoreHidden,
    },
    finished_copy_deleted = {
        "a finished copy is deleted",
        Some(RemoteSegmentLifecycle::CopyFinished),
        RemoteSegmentLifecycle::DeleteStarted,
        false,
        RemoteCacheAction::StoreHidden,
    },
    delete_skips_start = {
        "a delete cannot skip its start",
        Some(RemoteSegmentLifecycle::CopyFinished),
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Reject,
    },
    delete_finishes = {
        "the delete finishes",
        Some(RemoteSegmentLifecycle::DeleteStarted),
        RemoteSegmentLifecycle::DeleteFinished,
        false,
        RemoteCacheAction::Remove,
    },
    lifecycle_never_moves_back = {
        "the lifecycle never moves back",
        Some(RemoteSegmentLifecycle::DeleteStarted),
        RemoteSegmentLifecycle::CopyFinished,
        false,
        RemoteCacheAction::Reject,
    },
    finished_delete_stays_finished = {
        "a finished delete stays finished",
        Some(RemoteSegmentLifecycle::DeleteFinished),
        RemoteSegmentLifecycle::CopyStarted,
        false,
        RemoteCacheAction::Reject,
    },
)]
fn remote_cache_actions_are_idempotent_and_never_resurrect(
    what: &'static str,
    current: Option<RemoteSegmentLifecycle>,
    target: RemoteSegmentLifecycle,
    retry: bool,
    expected: RemoteCacheAction,
) {
    assert2::check!(
        remote_cache_action(current, target, retry) == expected,
        "{what}"
    );
}
