use {super::*, assert2::assert};

#[test]
fn broker_arithmetic_edges_are_explicit() {
    use DeleteRecordsTrimDecision::{Apply, Noop, RejectMalformed, RejectOutOfRange};

    let facts = |requested, high_watermark, log_end, current_start, has_delivery, delivery| {
        DeleteRecordsTrimFacts {
            requested,
            high_watermark,
            log_end,
            current_start,
            has_delivery_watermark: has_delivery,
            delivery_watermark: delivery,
        }
    };

    // Malformed checks
    assert!(delete_records_trim_decision(facts(-2, 7, 9, 2, false, 0)) == RejectMalformed);
    assert!(delete_records_trim_decision(facts(5, 7, 9, -1, false, 0)) == RejectMalformed);
    assert!(delete_records_trim_decision(facts(5, 1, 9, 2, false, 0)) == RejectMalformed);
    assert!(delete_records_trim_decision(facts(5, 7, 6, 2, false, 0)) == RejectMalformed);
    assert!(delete_records_trim_decision(facts(5, 7, 9, 2, true, 1)) == RejectMalformed);
    assert!(delete_records_trim_decision(facts(5, 7, 9, 2, false, 1)) == Apply { frontier: 5 });

    // Out of range: an explicit offset above the log end, and one above
    // the high watermark but still within the log end (KIP-107: the
    // uncommitted tail is never a valid explicit target).
    assert!(delete_records_trim_decision(facts(10, 7, 9, 2, false, 0)) == RejectOutOfRange);
    assert!(delete_records_trim_decision(facts(8, 7, 9, 2, false, 0)) == RejectOutOfRange);
    assert!(delete_records_trim_decision(facts(9, 9, 9, 2, false, 0)) == Apply { frontier: 9 });

    // Requested = -1 resolves to high_watermark and is always admitted,
    // even though an explicit request for that same offset is refused.
    assert!(delete_records_trim_decision(facts(-1, 7, 9, 2, false, 0)) == Apply { frontier: 7 });

    // Zero boundary
    assert!(delete_records_trim_decision(facts(0, 0, 0, 0, false, 0)) == Noop { frontier: 0 });
    assert!(delete_records_trim_decision(facts(0, 0, 0, 0, true, 0)) == Noop { frontier: 0 });

    // Clamping by delivery_watermark
    assert!(delete_records_trim_decision(facts(7, 7, 9, 2, true, 6)) == Apply { frontier: 6 });
    assert!(delete_records_trim_decision(facts(5, 7, 9, 2, true, 6)) == Apply { frontier: 5 });

    // Noop when bounded <= current_start
    assert!(delete_records_trim_decision(facts(2, 7, 9, 2, false, 0)) == Noop { frontier: 2 });
    assert!(delete_records_trim_decision(facts(1, 7, 9, 2, false, 0)) == Noop { frontier: 2 });

    assert!(effective_share_backlog(12, -1, 4) == 8);
    assert!(effective_share_backlog(5, 9, 4) == 0);
    assert!(effective_share_backlog(i64::MAX, i64::MIN, i64::MIN) == i64::MAX);
}

#[test]
fn delete_records_application_orders_retries_wal_first() {
    use DeleteRecordsTrimApplication::{Complete, RejectMalformed, TrimLocal, TrimWal};

    assert!(delete_records_trim_application(-1, 0, 0) == RejectMalformed);
    assert!(delete_records_trim_application(0, -1, 0) == RejectMalformed);
    assert!(delete_records_trim_application(0, 0, -1) == RejectMalformed);
    assert!(delete_records_trim_application(0, 0, 0) == Complete { frontier: 0 });
    assert!(delete_records_trim_application(8, 2, 2) == TrimWal { frontier: 8 });
    assert!(delete_records_trim_application(8, 8, 2) == TrimLocal { frontier: 8 });
    assert!(delete_records_trim_application(8, 8, 8) == Complete { frontier: 8 });
    // A retry repairs either side at the highest frontier and never
    // regresses a partially applied trim.
    assert!(delete_records_trim_application(5, 8, 3) == TrimLocal { frontier: 8 });
    assert!(delete_records_trim_application(5, 3, 8) == TrimWal { frontier: 8 });
    assert!(delete_records_trim_application(i64::MAX, 0, 0) == TrimWal { frontier: i64::MAX });
}

#[test]
fn find_coordinator_admission_is_exhaustive_and_fail_closed() {
    use FindCoordinatorAdmission::{
        AllowGroup, AllowShare, AllowTransaction, DenyCluster, DenyGroup, DenyTransaction,
        InvalidRequest,
    };

    for share_key_valid in [false, true] {
        assert!(find_coordinator_admission(0, 0, false, share_key_valid) == DenyGroup);
        assert!(find_coordinator_admission(0, 0, true, share_key_valid) == AllowGroup);
        assert!(find_coordinator_admission(0, 1, false, share_key_valid) == DenyTransaction);
        assert!(find_coordinator_admission(0, 1, true, share_key_valid) == AllowTransaction);
    }
    for version in [i16::MIN, 0, 5] {
        assert!(find_coordinator_admission(version, 2, false, true) == InvalidRequest);
        assert!(find_coordinator_admission(version, 2, true, true) == InvalidRequest);
    }
    assert!(find_coordinator_admission(6, 2, false, false) == InvalidRequest);
    assert!(find_coordinator_admission(6, 2, true, false) == InvalidRequest);
    assert!(find_coordinator_admission(6, 2, false, true) == DenyCluster);
    assert!(find_coordinator_admission(6, 2, true, true) == AllowShare);

    for unknown in [i8::MIN, -1, 3, i8::MAX] {
        for acl_allowed in [false, true] {
            for share_key_valid in [false, true] {
                assert!(
                    find_coordinator_admission(6, unknown, acl_allowed, share_key_valid)
                        == InvalidRequest
                );
            }
        }
    }
}

#[test]
fn unclean_recovery_commit_requires_the_selection_snapshot() {
    assert!(unclean_recovery_commit_admission(
        7,
        7,
        &[1, 2],
        &[1, 2],
        2,
        false
    ));
    assert!(!unclean_recovery_commit_admission(
        7,
        8,
        &[1, 2],
        &[1, 2],
        2,
        false
    ));
    assert!(!unclean_recovery_commit_admission(
        7,
        7,
        &[1, 2],
        &[2, 1],
        2,
        false
    ));
    assert!(!unclean_recovery_commit_admission(
        7,
        7,
        &[1, 2],
        &[1, 2, 3],
        2,
        false
    ));
    assert!(!unclean_recovery_commit_admission(
        7,
        7,
        &[1, 2],
        &[1, 2],
        3,
        false
    ));
    assert!(!unclean_recovery_commit_admission(
        7,
        7,
        &[1, 2],
        &[1, 2],
        2,
        true
    ));
}

#[yare::parameterized(
    kafka_group = { "g:BQUFBQUFBQUFBQUFBQUFBQ:0", 50, Some(2) },
    consumer_group = { "consumer-group", 50, Some(38) },
    unicode_group = { "🦀:BQUFBQUFBQUFBQUFBQUFBQ:7", 17, Some(8) },
    minimum_hash = { "polygenelubricants", 50, Some(0) },
    zero_partitions = { "", 0, None },
    negative_partitions = { "", -1, None },
)]
fn java_string_hash_partition_matches_jvm_goldens(
    key: &'static str,
    partitions: i32,
    expected: Option<i32>,
) {
    let units: Vec<u16> = key.encode_utf16().collect();
    assert2::assert!(java_string_hash_partition(&units, partitions) == expected);
}

#[test]
fn broker_arithmetic_matches_wide_integer_oracles() {
    let values = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
    for hwm in values {
        for spso in values {
            for log_start in values {
                let base = if spso >= 0 {
                    spso.max(log_start)
                } else {
                    log_start
                };
                let expected = i64::try_from(
                    (i128::from(hwm) - i128::from(base)).clamp(0, i128::from(i64::MAX)),
                )
                .expect("oracle is clamped to the i64 range");
                assert!(effective_share_backlog(hwm, spso, log_start) == expected);
            }
        }
    }
}
