use {super::*, crate::list_offsets::ListOffsetsSelectionDecision, std::vec};

fn visible_oracle(
    record_offsets: &[u32],
    record_timestamps: &[i64],
    pending_starts: &[i64],
    frontiers: (i64, i64, i64),
    request: (i32, i8, i64),
    candidate_epoch: i32,
) -> ListOffsetsSelectionDecision {
    let boundary = if request.0 == -1 {
        if request.1 == 1 {
            pending_starts
                .iter()
                .copied()
                .chain([frontiers.2])
                .min()
                .unwrap()
        } else {
            frontiers.2
        }
    } else {
        frontiers.1
    };
    record_offsets
        .iter()
        .zip(record_timestamps)
        .find_map(|(&offset, &timestamp)| {
            let offset = frontiers.0 + i64::from(offset);
            (offset < boundary && timestamp >= request.2).then_some(
                ListOffsetsSelectionDecision::Resolved {
                    offset,
                    timestamp,
                    leader_epoch: candidate_epoch,
                },
            )
        })
        .unwrap_or(ListOffsetsSelectionDecision::Unknown)
}

proptest! {
    #[test]
    fn timestamp_response_matches_a_filtered_visible_record_oracle(
        records in proptest::collection::btree_map(any::<u32>(),
            prop_oneof![0_i64..1_000, any::<i64>()], 0..32),
        starts in proptest::collection::vec(any::<u32>(), 0..8),
        base in 0_i64..i64::MAX - i64::from(u32::MAX),
        hwm in any::<u32>(),
        replica in prop_oneof![Just(-1_i32), Just(-2_i32), 0_i32..i32::MAX],
        isolation in prop_oneof![Just(0_i8), Just(1_i8), any::<i8>()],
        target in prop_oneof![0_i64..1_000, 0_i64..=i64::MAX],
        step in 1usize..8,
        epoch in -1_i32..=i32::MAX,
    ) {
        let (offsets, timestamps, entries) = indexed_timestamp_records(&records, step);
        let starts: Vec<_> = starts.iter().map(|s| base + i64::from(*s)).collect();
        let frontiers = (base, base + i64::from(u32::MAX) + 1, base + i64::from(hwm));
        let request = (replica, isolation, target);
        prop_assert_eq!(timestamp_list_offsets_finds_first_visible(
            &entries, &offsets, &timestamps, &starts, frontiers, request, epoch),
            visible_oracle(&offsets, &timestamps, &starts, frontiers, request, epoch));
    }
}

#[test]
fn visibility_is_exclusive_and_does_not_assume_monotone_timestamps() {
    let offsets = [0, 2, 5, 7, 9];
    let timestamps = [100, 300, 200, 400, 300];
    let entries = [(100, 0), (300, 2), (300, 5), (400, 7), (400, 9)];
    let frontiers = (10, 20, 20);
    for (starts, request, expected) in [
        (&[15][..], (-1, 1, 250), Some((12, 300))),
        (&[12][..], (-1, 1, 250), None),
        (&[15][..], (-1, 1, 350), None),
        (&[15][..], (-1, 0, 350), Some((17, 400))),
        (&[15][..], (-2, 1, 350), Some((17, 400))),
        (&[][..], (-1, 1, 350), Some((17, 400))),
        (&[][..], (-1, 1, 500), None),
    ] {
        let result = timestamp_list_offsets_finds_first_visible(
            &entries,
            &offsets,
            &timestamps,
            starts,
            frontiers,
            request,
            -1,
        );
        let expected = expected.map_or(
            ListOffsetsSelectionDecision::Unknown,
            |(offset, timestamp)| ListOffsetsSelectionDecision::Resolved {
                offset,
                timestamp,
                leader_epoch: -1,
            },
        );
        assert2::assert!(result == expected);
        assert2::assert!(
            result == visible_oracle(&offsets, &timestamps, starts, frontiers, request, -1)
        );
    }
    for (base, offsets, timestamps, starts, end, hw, expected) in [
        (
            0,
            vec![],
            vec![],
            vec![],
            0,
            0,
            ListOffsetsSelectionDecision::Unknown,
        ),
        (
            i64::MAX - 1,
            vec![0],
            vec![i64::MAX],
            vec![],
            i64::MAX,
            i64::MAX,
            ListOffsetsSelectionDecision::Resolved {
                offset: i64::MAX - 1,
                timestamp: i64::MAX,
                leader_epoch: -1,
            },
        ),
    ] {
        assert2::assert!(
            timestamp_list_offsets_finds_first_visible(
                &[],
                &offsets,
                &timestamps,
                &starts,
                (base, end, hw),
                (-1, 1, i64::MAX),
                -1
            ) == expected
        );
    }
}
