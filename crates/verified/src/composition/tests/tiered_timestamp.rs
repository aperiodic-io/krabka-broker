use {super::*, crate::list_offsets::ListOffsetsSelectionDecision};

proptest! {
    #[test]
    fn tier_union_matches_a_filtered_minimum_oracle(
        remote in proptest::collection::btree_map(0_i64..=i64::MAX, any::<i64>(), 0..20),
        local in proptest::collection::btree_map(0_i64..=i64::MAX, any::<i64>(), 0..20),
        target in 0_i64..=i64::MAX, minimum in 0_i64..=i64::MAX, bound in 0_i64..=i64::MAX, epoch in -1_i32..=i32::MAX,
    ) {
        let remote_records: Vec<_> = remote.iter().map(|(o,t)| (*o,*t)).collect();
        let local_records: Vec<_> = local.iter().map(|(o,t)| (*o,*t)).collect();
        let expected = remote.iter().chain(local.iter())
            .filter(|(o,t)| minimum <= **o && **o < bound && **t >= target)
            .min_by_key(|(o,_)| **o)
            .map_or(ListOffsetsSelectionDecision::Unknown, |(o,t)|
                ListOffsetsSelectionDecision::Resolved { offset: *o, timestamp: *t, leader_epoch: epoch });
        prop_assert_eq!(tiered_timestamp_lookup_preserves_first(&remote_records, &local_records, target, minimum, bound, epoch), expected);
    }
}

#[test]
fn a_later_remote_hit_cannot_hide_a_visible_local_match() {
    for (remote, local, bound, expected) in [
        ((7, 7_000), (2, 1_600), 6, Some((2, 1_600))),
        ((2, 1_600), (7, 7_000), 6, Some((2, 1_600))),
        ((7, 7_000), (2, 1_600), 2, None),
        ((2, 1_600), (2, 1_600), 6, Some((2, 1_600))),
        (
            (i64::MAX, i64::MAX),
            (i64::MAX - 1, i64::MAX),
            i64::MAX,
            Some((i64::MAX - 1, i64::MAX)),
        ),
    ] {
        let expected = expected.map_or(
            ListOffsetsSelectionDecision::Unknown,
            |(offset, timestamp)| ListOffsetsSelectionDecision::Resolved {
                offset,
                timestamp,
                leader_epoch: 0,
            },
        );
        assert2::assert!(
            tiered_timestamp_lookup_preserves_first(&[remote], &[local], 1_500, 0, bound, 0)
                == expected
        );
    }
    assert2::assert!(
        tiered_timestamp_lookup_preserves_first(
            &[(7, 7_000)],
            &[(2, 1_600), (4, 2_000)],
            1_500,
            4,
            8,
            0
        ) == ListOffsetsSelectionDecision::Resolved {
            offset: 4,
            timestamp: 2_000,
            leader_epoch: 0
        }
    );
    assert2::assert!(
        tiered_timestamp_lookup_preserves_first(&[], &[], 0, 0, 0, -1)
            == ListOffsetsSelectionDecision::Unknown
    );
}
