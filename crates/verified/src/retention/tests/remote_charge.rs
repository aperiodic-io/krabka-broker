use {super::*, assert2::assert, proptest::prelude::*};

proptest! {
    #[test]
    fn remote_prefix_matches_cumulative_charge(
        inputs in prop::collection::vec((any::<bool>(), any::<bool>(), any::<u64>()), 0..32),
        debt in any::<u64>(), allowed in any::<bool>(),
    ) {
        let rows: Vec<_> = inputs.iter().map(|&(floor, time, size)| remote(floor, time, size)).collect();
        let mut charge = 0u128;
        let expected = if allowed {
            rows.iter().position(|row| {
                let before = charge;
                charge += if row.log_start_breached { 0 } else { u128::from(row.size) };
                !(row.log_start_breached || row.time_expired
                    || (before < u128::from(debt) && charge <= u128::from(debt)))
            }).unwrap_or(rows.len())
        } else { 0 };
        assert!(remote_retention_prefix(allowed, &rows, debt) == expected);
    }
}

#[test]
fn remote_charge_handles_exhaustion_and_free_floor_breaches() {
    let cases: &[(&[RemoteRetentionSegment], u64, usize)] = &[
        (&[remote(false, false, 0)], 0, 0),
        (&[remote(false, false, 0)], 1, 1),
        (
            &[remote(true, true, u64::MAX), remote(false, false, 1)],
            1,
            2,
        ),
        (
            &[remote(false, true, u64::MAX), remote(false, false, 0)],
            1,
            1,
        ),
        (
            &[
                remote(false, true, u64::MAX),
                remote(false, true, 1),
                remote(false, false, 0),
            ],
            u64::MAX,
            2,
        ),
        (
            &[
                remote(false, true, 3),
                remote(false, false, 7),
                remote(false, false, 0),
            ],
            10,
            2,
        ),
    ];
    for &(rows, debt, expected) in cases {
        assert!(remote_retention_prefix(true, rows, debt) == expected);
        assert!(remote_retention_prefix(false, rows, debt) == 0);
    }
}
