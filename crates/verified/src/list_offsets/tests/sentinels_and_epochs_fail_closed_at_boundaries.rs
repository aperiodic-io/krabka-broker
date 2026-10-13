use {super::*, assert2::assert};

#[test]
fn sentinels_and_epochs_fail_closed_at_boundaries() {
    assert!(list_offsets_kind(-2, 0) == ListOffsetsKind::Earliest);
    assert!(list_offsets_kind(-1, 0) == ListOffsetsKind::Latest);
    assert!(list_offsets_kind(-3, 6) == ListOffsetsKind::Unsupported);
    assert!(list_offsets_kind(-3, 7) == ListOffsetsKind::MaxTimestamp);
    assert!(list_offsets_kind(-4, 7) == ListOffsetsKind::Unsupported);
    assert!(list_offsets_kind(-4, 8) == ListOffsetsKind::EarliestLocal);
    assert!(list_offsets_kind(-5, 8) == ListOffsetsKind::Unsupported);
    assert!(list_offsets_kind(-5, 9) == ListOffsetsKind::LatestTiered);
    assert!(list_offsets_kind(-6, 10) == ListOffsetsKind::Unsupported);
    assert!(list_offsets_kind(-6, 11) == ListOffsetsKind::EarliestPendingUpload);
    assert!(list_offsets_kind(-7, 12) == ListOffsetsKind::Unsupported);
    assert!(list_offsets_kind(0, 0) == ListOffsetsKind::Timestamp);
    assert!(list_offsets_kind(1, 0) == ListOffsetsKind::Timestamp);
    assert!(list_offsets_kind(i64::MAX, 0) == ListOffsetsKind::Timestamp);

    assert!(list_offsets_epoch_decision(0, -1) == ListOffsetsEpochDecision::RejectMalformed);
    assert!(list_offsets_epoch_decision(-1, -1) == ListOffsetsEpochDecision::RejectMalformed);
    assert!(list_offsets_epoch_decision(0, 0) == ListOffsetsEpochDecision::Proceed);
    assert!(list_offsets_epoch_decision(-1, 0) == ListOffsetsEpochDecision::Proceed);
    assert!(list_offsets_epoch_decision(1, 0) == ListOffsetsEpochDecision::Unknown);
    assert!(list_offsets_epoch_decision(-2, 3) == ListOffsetsEpochDecision::Fenced);
    assert!(list_offsets_epoch_decision(0, 3) == ListOffsetsEpochDecision::Fenced);
    assert!(list_offsets_epoch_decision(2, 3) == ListOffsetsEpochDecision::Fenced);
    assert!(list_offsets_epoch_decision(3, 3) == ListOffsetsEpochDecision::Proceed);
    assert!(list_offsets_epoch_decision(4, 3) == ListOffsetsEpochDecision::Unknown);
    assert!(list_offsets_epoch_decision(-1, 3) == ListOffsetsEpochDecision::Proceed);
}

#[test]
fn bounds_and_earliest_cover_isolation_tiers_and_overflow_edges() {
    use ListOffsetsBoundDecision::{Bound, RejectMalformed};

    let facts = |replica_id, isolation_level| ListOffsetsBoundFacts {
        replica_id,
        isolation_level,
        log_end: 10,
        high_watermark: 8,
        last_stable: 6,
    };
    assert!(list_offsets_bound_decision(facts(2, 1)) == Bound { offset: 10 });
    assert!(list_offsets_bound_decision(facts(-1, 0)) == Bound { offset: 8 });
    assert!(list_offsets_bound_decision(facts(-1, 1)) == Bound { offset: 6 });
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            high_watermark: -1,
            ..facts(-1, 0)
        }) == RejectMalformed
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            log_end: 0,
            high_watermark: 6,
            ..facts(-1, 0)
        }) == Bound { offset: 6 }
    );

    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: 1,
            isolation_level: 0,
            log_end: 0,
            high_watermark: -1,
            last_stable: -1,
        }) == Bound { offset: 0 }
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: 1,
            isolation_level: 0,
            log_end: -1,
            high_watermark: 5,
            last_stable: 5,
        }) == RejectMalformed
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 0,
            log_end: -1,
            high_watermark: 0,
            last_stable: -1,
        }) == Bound { offset: 0 }
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 0,
            log_end: 5,
            high_watermark: -1,
            last_stable: -1,
        }) == RejectMalformed
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 1,
            log_end: 10,
            high_watermark: 5,
            last_stable: 0,
        }) == Bound { offset: 0 }
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 1,
            log_end: 10,
            high_watermark: 5,
            last_stable: -1,
        }) == RejectMalformed
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 0,
            log_end: 10,
            high_watermark: 5,
            last_stable: -1,
        }) == Bound { offset: 5 }
    );
    assert!(
        list_offsets_bound_decision(ListOffsetsBoundFacts {
            replica_id: -1,
            isolation_level: 1,
            log_end: 10,
            high_watermark: 3,
            last_stable: 7,
        }) == Bound { offset: 3 }
    );

    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 0,
            has_remote: false,
            remote: -1,
            has_diskless: false,
            diskless: -1,
        }) == Some(0)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: -1,
            has_remote: false,
            remote: 0,
            has_diskless: false,
            diskless: 0,
        }) == None
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 9,
            has_remote: true,
            remote: 2,
            has_diskless: true,
            diskless: 0,
        }) == Some(0)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 9,
            has_remote: true,
            remote: -1,
            has_diskless: false,
            diskless: 0,
        }) == None
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: true,
            remote: 0,
            has_diskless: false,
            diskless: -1,
        }) == Some(0)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: false,
            remote: 0,
            has_diskless: false,
            diskless: 0,
        }) == Some(5)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: false,
            remote: 2,
            has_diskless: false,
            diskless: 1,
        }) == Some(5)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: true,
            remote: 2,
            has_diskless: false,
            diskless: 1,
        }) == Some(2)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 2,
            has_remote: true,
            remote: 5,
            has_diskless: false,
            diskless: 1,
        }) == Some(2)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: false,
            remote: 0,
            has_diskless: true,
            diskless: 1,
        }) == Some(1)
    );
    assert!(
        list_offsets_earliest(ListOffsetsEarliestFacts {
            local: 5,
            has_remote: false,
            remote: 0,
            has_diskless: true,
            diskless: -1,
        }) == None
    );
}
