use {
    super::remote_coverage_bounds_local_retention, crate::retention::LocalRetentionSegment,
    assert2::assert, proptest::prelude::*,
};

fn check(remote: &[(i64, i64)], local: &[(i64, i64, u64, bool)], debt: Option<u64>, active: u64) {
    let anchor = local.first().map_or(0, |row| row.0);
    let covered = if remote.iter().any(|&(start, end)| start > end) {
        None
    } else {
        remote.iter().map(|row| row.1).max().and_then(|end| {
            (anchor..=end)
                .take_while(|offset| {
                    remote
                        .iter()
                        .any(|&(start, end)| start <= *offset && *offset <= end)
                })
                .last()
        })
    };
    let mut facts: Vec<_> = local
        .iter()
        .map(|row| LocalRetentionSegment {
            blocked: covered.is_none_or(|end| row.1 > end),
            expired: row.3,
            size: row.2,
        })
        .collect();
    facts.push(LocalRetentionSegment {
        blocked: true,
        expired: false,
        size: active,
    });
    // Independent sequential size and time passes, rather than the kernel's fold.
    let mut count = 0;
    if let Some(mut remaining) = debt {
        while count < local.len() && !facts[count].blocked && local[count].2 <= remaining {
            remaining -= local[count].2;
            count += 1;
        }
    }
    while count < local.len() && !facts[count].blocked && local[count].3 {
        count += 1;
    }
    let target = count.checked_sub(1).and_then(|i| local[i].1.checked_add(1));
    assert!(
        remote_coverage_bounds_local_retention(remote, local, debt, active)
            == (covered, facts, count, target)
    );
}

proptest! {
    #[test]
    fn derived_eligibility_and_plan_match_coordinate_and_sequential_pass_oracles(
        raw in prop::collection::vec((-10i64..900, -10i64..900), 0..16),
        rows in prop::collection::btree_map(0i64..100, (0u64..100, any::<bool>(), 0i64..6), 0..16),
        debt in prop::option::of(0u64..400), active in any::<u64>(),
    ) {
        let local: Vec<_> = rows.iter().map(|(&start, &(size, expired, width))| (start * 8, start * 8 + width, size, expired)).collect();
        let mut remote = raw;
        remote.sort_unstable();
        check(&remote, &local, debt, active);
        let mut valid: Vec<_> = remote.iter().map(|&(start, end)| (start, start.max(end))).collect();
        check(&valid, &local, debt, active);
        // Ensure generated tests also exercise complete coverage and real deletion.
        if let (Some(first), Some(last)) = (local.first(), local.last()) {
            valid.push((first.0, last.1));
            valid.sort_unstable();
            let expired: Vec<_> = local.iter().map(|&(start, end, size, _)| (start, end, size, true)).collect();
            check(&valid, &expired, None, active);
        }
    }

    #[test]
    fn byte_exhaustion_zero_size_and_active_protection_match_the_policy(
        sizes in prop::collection::vec(any::<u64>(), 0..12),
        debt in prop::option::of(any::<u64>()), active in any::<u64>(), expired in any::<bool>(),
    ) {
        let local: Vec<_> = sizes.iter().enumerate().map(|(i, &size)| {
            let start = i64::try_from(i).unwrap() * 3;
            (start, start + 1, size, expired)
        }).collect();
        check(&[(0, 100)], &local, debt, active);
        check(&[(0, 100)], &local, Some(0), active);
        check(&[(0, 100)], &local, Some(u64::MAX), active);
    }
}

#[test]
fn a_global_maximum_cannot_cover_a_hole_or_hide_later_anchored_coverage() {
    let remote = [(0, 9), (20, 29)];
    for local in [
        &[(0, 29, 10, true)][..],
        &[(0, 9, 5, true), (20, 29, 5, true)],
        &[(20, 29, 10, true)],
        &[(20, 29, 10, false)],
        &[],
    ] {
        for debt in [None, Some(0), Some(5), Some(10), Some(u64::MAX)] {
            check(&remote, local, debt, 0);
            check(&[(0, 9)], local, debt, u64::MAX);
            check(&[(0, 9), (20, 19)], local, debt, 0);
            check(&[(0, 9), (10, 29)], local, debt, 1);
            check(&[(0, 9), (5, 29), (20, 29)], local, debt, 1);
        }
    }
    check(
        &[(0, 99)],
        &[(0, 9, 0, false), (20, 29, 0, false)],
        Some(0),
        0,
    );
}

#[test]
fn maximum_offset_can_be_selected_but_has_no_representable_delete_target() {
    for (remote, local, through, count, target) in [
        (
            &[(i64::MIN, i64::MAX)][..],
            (0, i64::MAX, 0, true),
            i64::MAX,
            1,
            None,
        ),
        (
            &[(i64::MAX, i64::MAX)],
            (i64::MAX, i64::MAX, u64::MAX, true),
            i64::MAX,
            1,
            None,
        ),
        (
            &[(i64::MAX - 1, i64::MAX)],
            (i64::MAX - 1, i64::MAX - 1, 0, true),
            i64::MAX,
            1,
            Some(i64::MAX),
        ),
        (
            &[(i64::MAX, i64::MAX)],
            (i64::MAX, i64::MAX, 0, false),
            i64::MAX,
            0,
            None,
        ),
    ] {
        let facts = vec![
            LocalRetentionSegment {
                blocked: false,
                expired: local.3,
                size: local.2,
            },
            LocalRetentionSegment {
                blocked: true,
                expired: false,
                size: u64::MAX,
            },
        ];
        assert!(
            remote_coverage_bounds_local_retention(remote, &[local], None, u64::MAX)
                == (Some(through), facts, count, target)
        );
    }
}

#[test]
fn funded_prefixes_use_whole_byte_cost_and_cannot_restart_after_expiry() {
    for (sizes, expiry) in [
        ([2, 3, 0], [false; 3]),
        ([0, 0, 0], [false; 3]),
        ([u64::MAX, 1, 0], [false; 3]),
        ([u64::MAX, 1, 0], [true, true, false]),
        ([u64::MAX, 0, 0], [true, false, false]),
        ([0, u64::MAX, 1], [false, true, false]),
    ] {
        let local: Vec<_> = sizes
            .iter()
            .zip(expiry)
            .enumerate()
            .map(|(i, (&size, expired))| {
                let start = i64::try_from(i).unwrap() * 3;
                (start, start + 1, size, expired)
            })
            .collect();
        for debt in [
            None,
            Some(0),
            Some(4),
            Some(5),
            Some(u64::MAX - 1),
            Some(u64::MAX),
        ] {
            for active in [0, 1, u64::MAX] {
                check(&[(0, 100)], &local, debt, active);
                check(&[(0, 1), (6, 100)], &local, debt, active);
            }
        }
    }
}
