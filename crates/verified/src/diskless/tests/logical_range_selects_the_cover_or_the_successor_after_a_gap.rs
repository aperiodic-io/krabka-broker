use {super::*, crate::log_index::IndexEntry};

#[test]
fn logical_range_selects_the_cover_or_the_successor_after_a_gap() {
    let entries = [
        IndexEntry::new(0, 4),
        IndexEntry::new(7, 9),
        IndexEntry::new(12, 15),
    ];
    for (requested, expected) in [
        (-1, None),
        (0, Some(0)),
        (5, Some(1)),
        (15, Some(2)),
        (16, None),
    ] {
        check!(diskless_logical_range(&entries, requested) == expected);
    }
}

#[test]
fn span_extends_only_across_a_contiguous_range_of_the_same_object() {
    // `(what, current start, current len, next start, next len, same
    // object, max bytes, extended span)`.
    for (what, start, len, next_start, next_len, same_object, max_bytes, expected) in [
        (
            "contiguous and within the cap",
            10,
            5,
            15,
            7,
            true,
            12,
            Some(12),
        ),
        ("contiguous and over the cap", 10, 5, 15, 7, true, 11, None),
        ("a gap", 10, 5, 16, 7, true, 12, None),
        ("another object", 10, 5, 15, 7, false, 12, None),
        ("an end past u64::MAX", u64::MAX, 1, 0, 1, true, 2, None),
        (
            "a total past u64::MAX",
            0,
            u64::MAX,
            u64::MAX,
            1,
            true,
            u64::MAX,
            None,
        ),
    ] {
        check!(
            diskless_span_extension(start, len, next_start, next_len, same_object, max_bytes)
                == expected,
            "{what}"
        );
    }
}

#[test]
fn batch_steps_advance_by_the_encoded_length_or_stop() {
    use DisklessBatchStep::{Continue, Invalid, Skip, Start, Stop};

    // `(what, selected start, batch start, encoded len, base offset,
    // last offset delta, floor, max bytes, step)`.
    for (what, selected, batch_start, encoded_len, base, delta, floor, max_bytes, expected) in [
        ("below the floor", None, 0, 10, 0, 0, 1, 5, Skip(10)),
        ("at the floor", None, 10, 10, 1, 0, 1, 5, Start(20)),
        (
            "within the cap",
            Some(10),
            20,
            10,
            2,
            0,
            1,
            20,
            Continue(30),
        ),
        (
            "the first batch",
            Some(20),
            20,
            10,
            2,
            0,
            1,
            20,
            Continue(30),
        ),
        ("over the cap", Some(10), 20, 10, 2, 0, 1, 19, Stop),
        ("empty", None, 0, 0, 0, 0, 1, 5, Invalid),
        ("a negative delta", None, 0, 10, 0, -1, 1, 5, Invalid),
        (
            "an end past usize::MAX",
            None,
            usize::MAX,
            1,
            0,
            0,
            0,
            usize::MAX,
            Invalid,
        ),
        (
            "a last offset past i64::MAX",
            None,
            0,
            1,
            i64::MAX,
            1,
            0,
            usize::MAX,
            Invalid,
        ),
        (
            "a run that starts later",
            Some(21),
            20,
            10,
            2,
            0,
            1,
            20,
            Invalid,
        ),
    ] {
        check!(
            diskless_batch_step(
                selected,
                batch_start,
                encoded_len,
                base,
                delta,
                floor,
                max_bytes
            ) == expected,
            "{what}"
        );
    }
}
