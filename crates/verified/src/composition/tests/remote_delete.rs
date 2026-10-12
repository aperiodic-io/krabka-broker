use assert2::assert;

use super::*;
use crate::retention::RemoteRetentionSegment;

fn check_plan(
    current: i64,
    published: Option<i64>,
    allowed: bool,
    rows: &[(i64, i64, u64, bool)],
    debt: u64,
    outcomes: &[bool],
) {
    let actual =
        completed_remote_retention_bounds_floor(current, published, allowed, rows, debt, outcomes);
    let facts: Vec<_> = rows
        .iter()
        .map(|&(_, end, size, expired)| RemoteRetentionSegment {
            blocked: false,
            size,
            time_expired: expired,
            log_start_breached: published.is_some_and(|floor| end < floor),
        })
        .collect();
    let planned = if allowed {
        facts
            .iter()
            .enumerate()
            .position(|(i, row)| {
                let before: u128 = facts[..i]
                    .iter()
                    .filter(|r| !r.log_start_breached)
                    .map(|r| u128::from(r.size))
                    .sum();
                !(row.log_start_breached
                    || row.time_expired
                    || (before < u128::from(debt)
                        && before + u128::from(row.size) <= u128::from(debt)))
            })
            .unwrap_or(rows.len())
    } else {
        0
    };
    let done = outcomes
        .iter()
        .take(planned)
        .take_while(|done| **done)
        .count();
    // Geometry of the whole completed prefix, independently of the floor step:
    // each range must reach the initial floor or one of its predecessors.
    let connected = rows[..done]
        .iter()
        .enumerate()
        .position(|(i, row)| {
            row.1 == i64::MAX
                || (row.0 > current
                    && !rows[..i]
                        .iter()
                        .any(|previous| i128::from(row.0) <= i128::from(previous.1) + 1))
        })
        .unwrap_or(done);
    let floor = rows[..connected]
        .iter()
        .map(|row| row.1 + 1)
        .max()
        .map_or(current, |end| current.max(end));
    assert!(actual == (facts, planned, done, floor));
}

proptest! {
    #[test]
    fn completed_deletion_matches_charge_and_range_graph_oracles(
        current in 0i64..101, published in prop::option::of(0i64..101),
        allowed in any::<bool>(), debt in any::<u64>(),
        inputs in prop::collection::vec((0i64..110, 0i64..110, any::<u64>(), any::<bool>()), 0..24),
        outcomes in prop::collection::vec(any::<bool>(), 0..24),
    ) {
        let rows: Vec<_> = inputs.iter().map(|&(a, b, size, expired)| (a.min(b), a.max(b), size, expired)).collect();
        check_plan(current, published.map(|p| p.min(current)), allowed, &rows, debt, &outcomes);
    }
}

#[test]
fn selected_rows_cannot_move_the_floor_past_failed_deletes_or_gaps() {
    let cases: &[&[(i64, i64, u64, bool)]] = &[
        &[(0, 4, 1, true), (5, 9, 1, true)],
        &[(0, 4, 1, true), (6, 9, 1, true), (5, 8, 1, true)],
        &[(0, 9, 1, true), (1, 4, 1, true), (10, 14, 1, true)],
        &[(0, 4, u64::MAX, false), (5, 9, 1, false)],
        &[(0, 4, 2, true), (5, 9, 1, false)],
        &[(0, i64::MAX - 1, 0, true), (0, i64::MAX, 0, true)],
        &[(0, i64::MAX, 0, true), (0, 1, 0, true)],
    ];
    for rows in cases {
        for current in [0, 5, i64::MAX] {
            for published in [None, Some(current)] {
                for allowed in [false, true] {
                    for debt in [0, 1, u64::MAX] {
                        for outcomes in [
                            &[][..],
                            &[false, true],
                            &[true, false, true],
                            &[true, true, true],
                        ] {
                            check_plan(current, published, allowed, rows, debt, outcomes);
                        }
                    }
                }
            }
        }
    }
}
