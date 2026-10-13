use {super::*, crate::log_index::IndexEntry};

#[test]
fn tiered_frontiers_are_finished_valid_and_exact() {
    let starts = [20, 0, -1, 40];
    let ends = [39, 19, 100, 59];
    let finished = [true, true, true, false];
    check!(tiered_earliest_finished_index(&starts, &ends, &finished) == Some(1));
    check!(tiered_latest_finished_index(&starts, &ends, &finished) == Some(0));
    check!(tiered_earliest_finished_index(&[10, 20], &[15, 25], &[true, true]) == Some(0));
    check!(tiered_latest_finished_index(&[10, 20], &[15, 25], &[true, true]) == Some(1));
    check!(tiered_earliest_finished_index(&[], &[], &[]) == None);
    check!(tiered_latest_finished_index(&[], &[], &[]) == None);
}

#[test]
fn tiered_epoch_owner_has_the_greatest_valid_start() {
    let epochs = [0, 2, -1, 4];
    let starts = [20, 30, 38, 35];
    check!(tiered_owning_epoch_index(&epochs, &starts, 20, 39) == Some(3));
    check!(tiered_owning_epoch_index(&[0, 1], &[30, 20], 10, 50) == Some(0));
    check!(tiered_owning_epoch_index(&[7], &[40], 20, 39) == None);
}

#[test]
fn remote_time_index_selects_strict_predecessor_prefix() {
    let entries = [
        IndexEntry::new(1_000, 0),
        IndexEntry::new(2_000, 10),
        IndexEntry::new(2_000, 20),
        IndexEntry::new(3_000, 30),
    ];
    for (target, expected) in [
        (500, 0),
        (1_000, 0),
        (1_500, 1),
        (2_000, 1),
        (2_500, 3),
        (4_000, 4),
    ] {
        check!(remote_time_index_candidate_count(&entries, target) == expected);
    }
}

#[test]
fn remote_time_index_stops_at_padding_or_the_target() {
    // `(what, entries, target, count)`.
    for (what, entries, target, expected) in [
        (
            "zeroed padding ends the index",
            &[
                IndexEntry::new(1_000, 0),
                IndexEntry::new(2_000, 10),
                IndexEntry::new(0, 0),
                IndexEntry::new(9_000, 20),
            ][..],
            i64::MAX,
            2,
        ),
        (
            "a repeated offset is padding",
            &[
                IndexEntry::new(1_000, 0),
                IndexEntry::new(2_000, 10),
                IndexEntry::new(3_000, 10),
            ][..],
            i64::MAX,
            2,
        ),
        ("an empty index", &[][..], i64::MAX, 0),
        (
            "the first entry at the target",
            &[IndexEntry::new(1_000, 0), IndexEntry::new(2_000, 10)][..],
            1_000,
            0,
        ),
        // Remote bytes need not be sorted; the count still stops at the
        // first entry that is not strictly below the target.
        (
            "unsorted timestamps stop at the first one at the target",
            &[
                IndexEntry::new(1_000, 0),
                IndexEntry::new(5_000, 10),
                IndexEntry::new(2_000, 20),
            ][..],
            3_000,
            1,
        ),
    ] {
        check!(
            remote_time_index_candidate_count(entries, target) == expected,
            "{what}"
        );
    }
}

#[test]
fn fetch_end_position_handles_exact_and_overflow_boundaries() {
    for (start, segment, max_bytes, expected) in [
        (0, 2, 1, Some(0)),
        (0, 1, 1, None),
        (0, 1, 0, None),
        (u32::MAX - 2, u32::MAX, 1, Some(u32::MAX - 2)),
        (u32::MAX - 1, u32::MAX, 1, None),
        (u32::MAX, u32::MAX, u32::MAX, None),
    ] {
        check!(remote_fetch_end_position(start, segment, max_bytes) == expected);
    }
}

#[test]
fn admits_exact_finished_lineage_range_and_rejects_every_other_case() {
    for (start, end, requested, finished, epoch_start, next_epoch, expected) in [
        (100, 199, 100, true, Some(100), None, Some(0)),
        (100, 199, 199, true, Some(100), None, Some(99)),
        (100, 199, 99, true, Some(100), None, None),
        (100, 199, 200, true, Some(100), None, None),
        (200, 100, 150, true, Some(200), None, None),
        (100, 199, 150, false, Some(100), None, None),
        (100, 199, 150, true, None, None, None),
        (0, 99, 49, true, Some(0), Some(50), Some(49)),
        (0, 99, 49, true, Some(0), Some(99), Some(49)),
        (0, 99, 50, true, Some(0), Some(50), None),
        (0, 99, 49, true, Some(50), None, None),
        (0, 99, 50, true, Some(50), None, Some(50)),
        (0, 99, 49, true, Some(0), Some(100), None),
        (
            i64::MIN,
            i64::MAX,
            i64::MAX,
            true,
            Some(i64::MIN),
            None,
            None,
        ),
        (
            i64::MIN,
            i64::MAX,
            i64::MIN,
            true,
            Some(i64::MIN),
            None,
            Some(0),
        ),
        (
            i64::MIN,
            i64::MAX,
            i64::MIN + i64::from(u32::MAX),
            true,
            Some(i64::MIN),
            None,
            Some(u32::MAX),
        ),
    ] {
        check!(
            remote_read_relative_offset(start, end, requested, finished, epoch_start, next_epoch,)
                == expected
        );
    }
}
