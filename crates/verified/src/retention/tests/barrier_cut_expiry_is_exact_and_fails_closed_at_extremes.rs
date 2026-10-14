use super::*;

#[yare::parameterized(
    before_expiry = { 10, 3, 6, true },
    at_expiry = { 10, 3, 7, true },
    after_expiry = { 10, 3, 8, false },
    no_retained_cuts = { 10, 0, i64::MIN, false },
    negative_retained_cuts = { 10, -1, i64::MIN, false },
    minimum_published = { i64::MIN, 1, i64::MIN, false },
    minimum_successor = { i64::MIN + 1, 1, i64::MIN, true },
    maximum_published = { i64::MAX, i32::MAX, i64::MAX, false },
)]
fn barrier_cut_expiry_is_exact_and_fails_closed_at_extremes(
    published: i64,
    retained: i32,
    held: i64,
    expected: bool,
) {
    check!(barrier_cut_expired(published, retained, held) == expected);
}

/// Kafka scenarios for `UnifiedLog.deleteOldSegments`: the log-start
/// breach, `retention.bytes` and `retention.ms` passes, in that order,
/// over every local segment with the active one last. Every expected
/// count is worked from Kafka's passes by hand.
#[test]
fn local_prefix_matches_kafkas_delete_old_segments_passes() {
    /// A name, the segments (the active one last), the size debt, and how
    /// many Kafka deletes.
    type Case<'a> = (&'a str, &'a [LocalRetentionSegment], Option<u64>, usize);

    let cases: [Case<'_>; 21] = [
        ("no segments", &[], Some(100), 0),
        (
            "no pressure keeps everything",
            &[fresh(10), fresh(10)],
            None,
            0,
        ),
        (
            "a debt of 15 over three 10-byte segments deletes one, not two",
            &[fresh(10), fresh(10), fresh(10)],
            Some(15),
            1,
        ),
        (
            "a debt of exactly two segments deletes two",
            &[fresh(10), fresh(10), fresh(10)],
            Some(20),
            2,
        ),
        (
            "a debt below the oldest segment deletes nothing",
            &[fresh(10), fresh(1)],
            Some(5),
            0,
        ),
        (
            "a zero debt still deletes leading empty segments",
            &[fresh(0), fresh(0), fresh(10)],
            Some(0),
            2,
        ),
        (
            "no size pass keeps empty segments",
            &[fresh(0), fresh(10)],
            None,
            0,
        ),
        (
            "time pass deletes the expired prefix",
            &[expired(10), expired(10), fresh(10), expired(10)],
            None,
            2,
        ),
        (
            "the size pass ends at the first misfit and never resumes",
            &[expired(10), fresh(1)],
            Some(5),
            1,
        ),
        (
            "the size pass runs first and the time pass continues after it",
            &[fresh(10), expired(10), fresh(1)],
            Some(12),
            2,
        ),
        (
            "an expired segment that fits is charged to the debt",
            &[expired(3), fresh(10)],
            Some(10),
            1,
        ),
        (
            "the size pass reaches past an expired prefix",
            &[expired(3), fresh(3), fresh(10)],
            Some(6),
            2,
        ),
        (
            "a blocked segment stops every pass",
            &[expired(10), blocked(true, 10), expired(10)],
            Some(100),
            1,
        ),
        (
            "a blocked oldest segment deletes nothing",
            &[blocked(true, 10), expired(10)],
            Some(100),
            0,
        ),
        (
            "an expired log goes whole, the active segment included",
            &[expired(10), expired(10)],
            None,
            2,
        ),
        (
            "a debt that covers the whole log takes the active segment too",
            &[fresh(10), fresh(10)],
            Some(20),
            2,
        ),
        (
            "an empty active segment is never deleted",
            &[expired(10), expired(0)],
            None,
            1,
        ),
        (
            "an empty active segment stays under any debt",
            &[fresh(10), fresh(0)],
            Some(u64::MAX),
            1,
        ),
        ("a lone empty segment stays", &[expired(0)], Some(0), 0),
        (
            "a lone active segment that breaches goes",
            &[expired(1)],
            None,
            1,
        ),
        (
            "the full u64 range fits without overflow",
            &[fresh(u64::MAX), fresh(1)],
            Some(u64::MAX),
            1,
        ),
    ];
    for (name, segments, size_debt, expected) in cases {
        check!(
            local_retention_prefix(segments, size_debt) == expected,
            "{name}"
        );
    }
}

/// Kafka scenarios for `RemoteLogRetentionHandler`: per segment, the
/// log-start breach, then `retention.ms`, then `retention.bytes`. Every
/// expected count is worked from Kafka's handler by hand.
#[test]
fn remote_prefix_matches_kafkas_remote_log_retention_handler() {
    /// A name, the segments, the size debt, and how many Kafka deletes.
    type Case<'a> = (&'a str, &'a [RemoteRetentionSegment], u64, usize);

    let kept = remote(false, false, 10);
    let old = remote(false, true, 10);
    let breached = remote(true, false, 10);
    let cases: [Case<'_>; 12] = [
        ("no segments", &[], 100, 0),
        ("no pressure keeps everything", &[kept, kept], 0, 0),
        (
            "a debt of 15 over three 10-byte segments deletes one, not two",
            &[kept, kept, kept],
            15,
            1,
        ),
        (
            "a debt of exactly two segments deletes two",
            &[kept, kept, kept],
            20,
            2,
        ),
        (
            "a zero debt never deletes an empty segment",
            &[remote(false, false, 0)],
            0,
            0,
        ),
        (
            "a positive debt deletes an empty segment",
            &[remote(false, false, 0), kept],
            5,
            1,
        ),
        (
            "the time axis stops at the first segment in the window",
            &[old, old, kept, old],
            0,
            2,
        ),
        (
            "a time deletion lowers the debt, never below zero",
            &[remote(false, true, 30), remote(false, false, 1)],
            20,
            1,
        ),
        (
            "size resumes after a time deletion charged against the debt",
            &[old, remote(false, false, 5), kept],
            20,
            2,
        ),
        (
            "a log-start breach leaves the debt alone",
            &[breached, kept, kept],
            10,
            2,
        ),
        (
            "the breach covers what the time axis would stop at",
            &[breached, old, kept],
            0,
            2,
        ),
        (
            "the breach alone takes every breached segment",
            &[breached; 3],
            0,
            3,
        ),
    ];
    for (name, segments, size_debt, expected) in cases {
        check!(
            remote_retention_prefix(true, segments, size_debt) == expected,
            "{name}"
        );
        check!(
            remote_retention_prefix(false, segments, size_debt) == 0,
            "{name}: a tier that accepts no delete keeps everything"
        );
    }
}
