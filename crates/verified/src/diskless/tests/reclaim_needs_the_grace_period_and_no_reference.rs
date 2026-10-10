use super::*;

#[test]
fn reclaim_needs_the_grace_period_and_no_reference() {
    check!(diskless_object_reclaimable(false, true));
    check!(!diskless_object_reclaimable(true, true));
    check!(!diskless_object_reclaimable(false, false));
}

#[test]
fn trim_is_bounded_non_regressing_and_overflow_safe() {
    for (frontier, high_watermark, lag, current, expected) in [
        (90, 100, 10, 50, (true, 90)),
        (100, 90, 10, 50, (true, 80)),
        (80, 90, 10, 80, (false, 80)),
        (70, 90, 10, 80, (false, 80)),
        (i64::MAX, i64::MAX, 0, i64::MAX - 1, (true, i64::MAX)),
        (i64::MAX, 0, i64::MAX, 0, (false, 0)),
        (10, 10, -1, 0, (true, 10)),
        (-1, 10, 0, 0, (false, 0)),
        (10, -1, 0, 0, (false, 0)),
        (10, 10, 0, -1, (false, -1)),
        (0, 0, 0, 0, (false, 0)),
    ] {
        let decision = diskless_trim_decision(frontier, high_watermark, lag, current);
        check!(
            decision
                == DisklessTrimDecision {
                    should_trim: expected.0,
                    target: expected.1,
                }
        );
    }
}

/// Every case runs over the same three ranges: 100 bytes each, covering
/// offsets 0-4, 5-9 and 10-14, read at `now_ms = 1_000`. Only the batch
/// timestamps and the topic's retention change, which is what each Kafka
/// predicate keys on.
#[test]
fn retention_prefix_applies_each_kafka_predicate_and_keeps_the_newest_range() {
    const BYTE_LENS: [u64; 3] = [100, 100, 100];
    const LAST_OFFSETS: [i64; 3] = [4, 9, 14];
    const NOW_MS: i64 = 1_000;

    // `(what, batch max timestamps, retention.ms, retention.bytes,
    // DeleteRecords floor, expired prefix)`.
    for (what, max_timestamps, retention_ms, retention_bytes, floor, expired) in [
        (
            "nothing configured expires nothing",
            [10, 20, 30],
            None,
            None,
            0,
            0,
        ),
        (
            "time leaves what is newer than now - 500",
            [100, 200, 900],
            Some(500),
            None,
            0,
            2,
        ),
        (
            "time past every range still keeps the newest",
            [100, 200, 300],
            Some(500),
            None,
            0,
            2,
        ),
        (
            "time stops at the first range it must keep",
            [100, 900, 100],
            Some(500),
            None,
            0,
            1,
        ),
        // Kafka's `diff` is 150: the first range leaves 50, which the
        // second cannot cover.
        (
            "bytes expires the oldest range only",
            [10, 20, 30],
            None,
            Some(150),
            0,
            1,
        ),
        // `diff` is 100, and `100 - 100 >= 0` is Kafka's delete rule.
        (
            "bytes expires a range that pays the debt off exactly",
            [10, 20, 30],
            None,
            Some(200),
            0,
            1,
        ),
        // `diff` is 50, and no range fits inside it.
        (
            "bytes never deletes past its own budget",
            [10, 20, 30],
            None,
            Some(250),
            0,
            0,
        ),
        (
            "a budget the index already fits expires nothing",
            [10, 20, 30],
            None,
            Some(300),
            0,
            0,
        ),
        (
            "the floor expires every range that ends below it",
            [10, 20, 30],
            None,
            None,
            5,
            1,
        ),
        (
            "a floor past every range still keeps the newest",
            [10, 20, 30],
            None,
            None,
            99,
            2,
        ),
        // The floor clears the first range, time the second, and neither
        // reaches the third.
        (
            "the predicates union",
            [100, 100, 900],
            Some(500),
            None,
            5,
            2,
        ),
        // Time clears the first range, which pays `diff` 150 down to 50;
        // the second range needs 100.
        (
            "a range time expires still pays the size debt",
            [100, 900, 900],
            Some(500),
            Some(150),
            0,
            1,
        ),
    ] {
        let prefix = diskless_retention_prefix(
            &max_timestamps,
            &BYTE_LENS,
            &LAST_OFFSETS,
            policy(DisklessRetentionSetup {
                retention: retention_ms.map(RetentionMillis),
                bytes: retention_bytes.map(RetentionBytes),
                start: LogicalOffset(floor),
                now: UnixMillis(NOW_MS),
            }),
        );
        check!(prefix == expired, "{what}");
    }
}

#[test]
fn retention_prefix_boundaries_follow_kafkas_strict_and_inclusive_comparisons() {
    const THREE_TIMESTAMPS: &[i64] = &[10, 20, 30];
    const THREE_BYTE_LENS: &[u64] = &[100, 100, 100];
    const THREE_LAST_OFFSETS: &[i64] = &[4, 9, 14];
    // `(what, max timestamps, byte lens, last offsets, policy, expired)`.
    for (what, max_timestamps, byte_lens, last_offsets, policy, expired) in [
        (
            "a floor equal to the last offset keeps the range",
            &[100, 200][..],
            &[10, 10][..],
            &[10, 20][..],
            policy(DisklessRetentionSetup {
                start: LogicalOffset(10),
                ..Default::default()
            }),
            0,
        ),
        (
            "a max timestamp equal to the horizon keeps the range",
            &[500, 900][..],
            &[10, 10][..],
            &[10, 20][..],
            policy(DisklessRetentionSetup {
                retention: Some(RetentionMillis(500)),
                ..Default::default()
            }),
            0,
        ),
        // `diff` is 0 and `0 - 0 >= 0`.
        (
            "a zero diff still expires a zero-byte range",
            &[100, 200][..],
            &[0, 10][..],
            &[10, 20][..],
            policy(DisklessRetentionSetup {
                bytes: Some(RetentionBytes(10)),
                ..Default::default()
            }),
            1,
        ),
        (
            "a horizon below i64::MIN expires nothing",
            THREE_TIMESTAMPS,
            THREE_BYTE_LENS,
            THREE_LAST_OFFSETS,
            policy(DisklessRetentionSetup {
                retention: Some(RetentionMillis(1)),
                now: UnixMillis(i64::MIN),
                ..Default::default()
            }),
            0,
        ),
        (
            "a horizon above i64::MAX expires nothing",
            THREE_TIMESTAMPS,
            THREE_BYTE_LENS,
            THREE_LAST_OFFSETS,
            policy(DisklessRetentionSetup {
                retention: Some(RetentionMillis(-1)),
                now: UnixMillis(i64::MAX),
                ..Default::default()
            }),
            0,
        ),
        (
            "u64::MAX ranges do not overflow the size sum",
            THREE_TIMESTAMPS,
            &[u64::MAX, u64::MAX, u64::MAX][..],
            THREE_LAST_OFFSETS,
            policy(DisklessRetentionSetup {
                bytes: Some(RetentionBytes(u64::MAX)),
                ..Default::default()
            }),
            2,
        ),
        (
            "one range is the newest range, whatever retention says",
            &[10][..],
            &[100][..],
            &[4][..],
            policy(DisklessRetentionSetup {
                retention: Some(RetentionMillis(1)),
                bytes: Some(RetentionBytes(0)),
                start: LogicalOffset(99),
                ..Default::default()
            }),
            0,
        ),
        (
            "an empty index expires nothing",
            &[][..],
            &[][..],
            &[][..],
            policy(DisklessRetentionSetup {
                retention: Some(RetentionMillis(1)),
                bytes: Some(RetentionBytes(0)),
                start: LogicalOffset(99),
                ..Default::default()
            }),
            0,
        ),
    ] {
        check!(
            diskless_retention_prefix(max_timestamps, byte_lens, last_offsets, policy) == expired,
            "{what}"
        );
    }
}
