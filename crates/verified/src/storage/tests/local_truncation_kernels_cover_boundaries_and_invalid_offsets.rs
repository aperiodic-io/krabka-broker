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

#[test]
fn remote_cache_actions_are_idempotent_and_never_resurrect() {
    use {
        RemoteCacheAction::{Noop, Reject, Remove, StoreFinished, StoreHidden},
        RemoteSegmentLifecycle::{CopyFinished, CopyStarted, DeleteFinished, DeleteStarted},
    };

    for (what, current, target, retry, expected) in [
        (
            "an update cannot resurrect",
            None,
            CopyFinished,
            false,
            Reject,
        ),
        (
            "a missing tombstone is idempotent",
            None,
            DeleteFinished,
            false,
            Noop,
        ),
        ("an exact retry", Some(CopyStarted), CopyStarted, true, Noop),
        (
            "a conflicting duplicate",
            Some(CopyStarted),
            CopyStarted,
            false,
            Reject,
        ),
        (
            "the copy finishes",
            Some(CopyStarted),
            CopyFinished,
            false,
            StoreFinished,
        ),
        (
            "an unfinished copy is deleted",
            Some(CopyStarted),
            DeleteStarted,
            false,
            StoreHidden,
        ),
        (
            "a finished copy is deleted",
            Some(CopyFinished),
            DeleteStarted,
            false,
            StoreHidden,
        ),
        (
            "a delete cannot skip its start",
            Some(CopyFinished),
            DeleteFinished,
            false,
            Reject,
        ),
        (
            "the delete finishes",
            Some(DeleteStarted),
            DeleteFinished,
            false,
            Remove,
        ),
        (
            "the lifecycle never moves back",
            Some(DeleteStarted),
            CopyFinished,
            false,
            Reject,
        ),
        (
            "a finished delete stays finished",
            Some(DeleteFinished),
            CopyStarted,
            false,
            Reject,
        ),
    ] {
        assert2::check!(
            remote_cache_action(current, target, retry) == expected,
            "{what}"
        );
    }
}
