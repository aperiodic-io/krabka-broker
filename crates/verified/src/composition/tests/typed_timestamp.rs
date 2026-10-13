use {
    super::*,
    crate::{list_offsets::ListOffsetsSelectionDecision, timestamp::timestamp_record_time},
};

proptest! {
    #[test]
    fn typed_batch_matches_an_independent_visible_record_oracle(
        rows in proptest::collection::btree_map(0_i32..=i32::MAX, any::<i64>(), 0..12),
        base in 0_i64..=(i64::MAX - i64::from(i32::MAX)), batch_time in any::<i64>(),
        append_time in proptest::option::of(any::<i64>()),
        minimum in 0_i64..=i64::MAX, target in 0_i64..=i64::MAX,
        bound in 0_i64..=i64::MAX, epoch in -1_i32..=i32::MAX,
    ) {
        prop_assume!(append_time.is_some() || rows.values().all(|delta|
            (i128::from(i64::MIN)..=i128::from(i64::MAX))
                .contains(&(i128::from(batch_time) + i128::from(*delta)))));
        let records: Vec<_> = rows.into_iter().collect();
        let expected = records.iter().find_map(|(delta, time_delta)| {
            let offset = base + i64::from(*delta);
            let timestamp = append_time.unwrap_or_else(|| batch_time + time_delta);
            (minimum <= offset && offset < bound && timestamp >= target)
                .then_some(ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch: epoch })
        }).unwrap_or(ListOffsetsSelectionDecision::Unknown);
        prop_assert_eq!(typed_timestamp_records_preserve_visibility(
            &records, base, batch_time, append_time, (minimum, target, bound), epoch), expected);
    }
}

#[test]
fn append_time_replaces_overflowing_producer_fields_before_visibility() {
    for (batch_time, delta) in [(i64::MAX, 1), (i64::MIN, -1), (0, i64::MAX)] {
        assert2::assert!(timestamp_record_time(batch_time, delta, Some(2_400)) == Some(2_400));
        for (floor, bound, expected) in [
            (10, 13, Some(10)),
            (11, 13, Some(11)),
            (12, 12, None),
            (13, 13, None),
        ] {
            let expected = expected.map_or(ListOffsetsSelectionDecision::Unknown, |offset| {
                ListOffsetsSelectionDecision::Resolved {
                    offset,
                    timestamp: 2_400,
                    leader_epoch: 0,
                }
            });
            assert2::assert!(
                typed_timestamp_records_preserve_visibility(
                    &[(0, delta), (1, delta), (2, delta)],
                    10,
                    batch_time,
                    Some(2_400),
                    (floor, 2_400, bound),
                    0
                ) == expected
            );
        }
    }
    assert2::assert!(timestamp_record_time(i64::MAX, 1, None) == None);
    assert2::assert!(timestamp_record_time(i64::MIN, -1, None) == None);
    assert2::assert!(timestamp_record_time(i64::MAX, 0, None) == Some(i64::MAX));
    assert2::assert!(
        typed_timestamp_records_preserve_visibility(
            &[],
            i64::MAX,
            i64::MAX,
            None,
            (0, 0, i64::MAX),
            -1
        ) == ListOffsetsSelectionDecision::Unknown
    );
}
