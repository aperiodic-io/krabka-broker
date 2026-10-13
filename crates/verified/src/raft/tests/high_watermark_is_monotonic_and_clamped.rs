use {super::*, assert2::assert};

#[test]
fn high_watermark_is_monotonic_and_clamped() {
    for (previous, requested, log_end, expected) in
        [(2, 5, 4, 4), (2, 1, 4, 2), (2, 3, 4, 3), (5, 1, 4, 5)]
    {
        assert!(advance_high_watermark(previous, requested, log_end) == expected);
    }
}

#[test]
fn windows_and_frontiers_use_exact_boundaries() {
    assert!(in_half_open_window(5, 5, 8));
    assert!(in_half_open_window(7, 5, 8));
    assert!(!in_half_open_window(4, 5, 8));
    assert!(!in_half_open_window(8, 5, 8));
    assert!(!frontier_reaches(4, 5));
    assert!(frontier_reaches(5, 5));
}

#[test]
fn control_history_frontier_is_strictly_half_open() {
    let offsets = [-1, 2, 5, 9];
    for (frontier, expected) in [(-1, 0), (0, 1), (2, 1), (5, 2), (9, 3), (10, 4)] {
        assert!(control_history_frontier(&offsets, frontier) == expected);
    }
    assert!(control_history_frontier(&[], 5) == 0);
}

#[test]
fn metadata_offset_deltas_are_contiguous_and_fail_closed() {
    for (record_count, expected) in [
        (0, None),
        (1, Some(vec![0])),
        (3, Some(vec![0, 1, 2])),
        // One past the largest `lastOffsetDelta` Kafka can encode.
        (0x8000_0001, None),
        (usize::MAX, None),
    ] {
        assert!(metadata_record_offset_deltas(record_count) == expected);
    }
}

#[test]
fn fetch_response_is_fenced_before_one_exclusive_mutation() {
    use FetchResponseMutation::{Append, Discover, HighWatermark, Reject, Snapshot, Truncate};

    // Following leader 2 in epoch 3.
    let following = FetchFence {
        discovering: false,
        role_leader: Some(2),
        current_leader: Some(2),
        current_epoch: 3,
    };
    // A leaderless observer in epoch 3.
    let discovering = FetchFence {
        discovering: true,
        role_leader: None,
        current_leader: None,
        current_epoch: 3,
    };
    let from_leader = FetchResponseFacts {
        from: 2,
        leader: Some(2),
        epoch: 3,
        error_none: true,
    };
    // Follower 1 answers NOT_LEADER_OR_FOLLOWER naming leader 2
    // (`validateLeaderOnlyRequest`).
    let from_follower = FetchResponseFacts {
        from: 1,
        error_none: false,
        ..from_leader
    };
    let content = |has_snapshot, has_divergence, has_records| FetchContent {
        has_snapshot,
        has_divergence,
        has_records,
    };
    let everything = content(true, true, true);
    let cases = [
        (
            "snapshot wins",
            following,
            from_leader,
            everything,
            Snapshot,
        ),
        (
            "divergence",
            following,
            from_leader,
            content(false, true, true),
            Truncate,
        ),
        (
            "records",
            following,
            from_leader,
            content(false, false, true),
            Append,
        ),
        (
            "watermark only",
            following,
            from_leader,
            content(false, false, false),
            HighWatermark,
        ),
        // KafkaRaftClient.maybeHandleCommonResponse: same epoch, a leader,
        // and no known leader transitions to follower of that leader,
        // whoever answered and whatever the error.
        (
            "leader answers observer",
            discovering,
            from_leader,
            everything,
            Discover,
        ),
        (
            "follower redirects observer",
            discovering,
            from_follower,
            everything,
            Discover,
        ),
        (
            "leaderless answer to observer",
            discovering,
            FetchResponseFacts {
                leader: None,
                ..from_follower
            },
            everything,
            Reject,
        ),
        // An older epoch is no longer relevant; a newer one is the host's
        // BeginQuorumEpoch path.
        (
            "stale epoch while discovering",
            discovering,
            FetchResponseFacts {
                epoch: 2,
                ..from_leader
            },
            everything,
            Reject,
        ),
        (
            "newer epoch while discovering",
            discovering,
            FetchResponseFacts {
                epoch: 4,
                ..from_leader
            },
            everything,
            Reject,
        ),
        (
            "observer that knows a leader",
            FetchFence {
                role_leader: Some(2),
                ..discovering
            },
            from_follower,
            everything,
            Reject,
        ),
        (
            "voter without a leader",
            FetchFence {
                discovering: false,
                ..discovering
            },
            from_leader,
            everything,
            Reject,
        ),
        (
            "durable leader differs",
            FetchFence {
                current_leader: Some(3),
                ..following
            },
            from_leader,
            everything,
            Reject,
        ),
        (
            "role has no leader",
            FetchFence {
                role_leader: None,
                ..following
            },
            from_leader,
            everything,
            Reject,
        ),
        (
            "follower answers a follower",
            following,
            from_follower,
            everything,
            Reject,
        ),
        (
            "sender names another leader",
            following,
            FetchResponseFacts {
                leader: Some(3),
                ..from_leader
            },
            everything,
            Reject,
        ),
        (
            "leader answers with an error",
            following,
            FetchResponseFacts {
                error_none: false,
                ..from_leader
            },
            everything,
            Reject,
        ),
        (
            "newer epoch while following",
            following,
            FetchResponseFacts {
                epoch: 4,
                ..from_leader
            },
            everything,
            Reject,
        ),
    ];
    for (case, fence, response, body, expected) in cases {
        assert!(
            fetch_response_mutation(fence, response, body) == expected,
            "{case}"
        );
    }
}
