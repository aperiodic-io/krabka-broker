use {super::*, assert2::assert};

pub(super) fn logical_oracle(
    f: DeleteRecordsTrimFacts,
    wal: i64,
    local: i64,
    snapshots: &[i64],
) -> Result<(i64, Option<usize>, i64), DeleteRecordsTrimDecision> {
    if f.requested < -1
        || f.current_start < 0
        || f.high_watermark < f.current_start
        || f.log_end < f.high_watermark
        || (f.has_delivery_watermark && f.delivery_watermark < f.current_start)
    {
        return Err(DeleteRecordsTrimDecision::RejectMalformed);
    }
    if f.requested != -1 && f.requested > f.high_watermark {
        return Err(DeleteRecordsTrimDecision::RejectOutOfRange);
    }
    let resolved = if f.requested == -1 {
        f.high_watermark
    } else {
        f.requested
    };
    let bounded = if f.has_delivery_watermark {
        resolved.min(f.delivery_watermark)
    } else {
        resolved
    };
    let floor = bounded.max(f.current_start).max(wal).max(local);
    let selected = snapshots
        .iter()
        .enumerate()
        .filter(|(_, offset)| floor < **offset && **offset <= f.log_end)
        .max_by_key(|(i, offset)| (**offset, std::cmp::Reverse(*i)))
        .map(|(i, _)| i);
    Ok((floor, selected, selected.map_or(floor, |i| snapshots[i])))
}

pub(super) fn check_logical(f: DeleteRecordsTrimFacts, wal: i64, local: i64, snapshots: &[i64]) {
    assert!(
        admitted_trim_bounds_reload_and_retry(f, wal, local, snapshots)
            == logical_oracle(f, wal, local, snapshots)
    );
}

fn physical_oracle(
    indexed: i64,
    hw: i64,
    lag: i64,
    wal: i64,
    local: i64,
    trace: &[bool],
) -> (i64, i64) {
    let target = indexed.min(hw - lag.max(0));
    if local < 0 || local >= target {
        return (wal, local);
    }
    let frontier = target.max(wal);
    let completed = trace.iter().filter(|done| **done).count();
    (
        if completed > 0 { frontier } else { wal },
        if completed > 1 || (completed > 0 && wal == frontier) {
            frontier
        } else {
            local
        },
    )
}

pub(super) fn check_physical(
    indexed: i64,
    hw: i64,
    lag: i64,
    wal: i64,
    local: i64,
    trace: &[bool],
) {
    assert!(
        diskless_trim_reconciliation_preserves_coverage(indexed, hw, lag, wal, local, trace)
            == physical_oracle(indexed, hw, lag, wal, local, trace)
    );
}

fn check_read(
    f: DeleteRecordsTrimFacts,
    stores: (i64, i64),
    snapshots: &[i64],
    window: SparseTimestampWindow<'_>,
    base: i64,
    target: i64,
) {
    check_logical(f, stores.0, stores.1, snapshots);
    let expected =
        logical_oracle(f, stores.0, stores.1, snapshots).map(|(floor, snapshot, cursor)| {
            let selected =
                window.0.iter().zip(window.1).position(|(&offset, &time)| {
                    base + i64::from(offset) >= floor && time >= target
                });
            (floor, snapshot, cursor, selected)
        });
    assert!(
        completed_trim_preserves_retained_timestamp(f, stores, snapshots, window, base, target)
            == expected
    );
}

fn check_eviction(
    frontiers: (i64, i64, i64, i64, i64),
    trace: &[bool],
    window: SparseTimestampWindow<'_>,
    base: i64,
    floor: i64,
    target: i64,
) {
    let (indexed, hw, lag, wal, local) = frontiers;
    check_physical(indexed, hw, lag, wal, local, trace);
    let (wal, local) = physical_oracle(indexed, hw, lag, wal, local, trace);
    let selected = window
        .0
        .iter()
        .zip(window.1)
        .position(|(&offset, &time)| base + i64::from(offset) >= floor && time >= target)
        .map(|i| (i, base + i64::from(window.0[i]) < local));
    assert!(
        physical_eviction_routes_retained_timestamp(frontiers, trace, window, base, floor, target)
            == (wal, local, selected)
    );
}

proptest! {
    #[test]
    fn completed_trim_read_matches_independent_floor_snapshot_and_record_oracles(
        records in prop::collection::btree_map(0u32..100, any::<i64>(), 0..24),
        requested in -2i64..105, current in -1i64..101,
        wal in 0i64..101, local in 0i64..101,
        delivery in any::<bool>(), snapshots in prop::collection::vec(-1i64..104, 0..16), target in any::<i64>(),
    ) {
        let (offsets, times, rows) = timestamp_records(&records, 1, 0);
        let f = DeleteRecordsTrimFacts { requested, current_start: current, high_watermark: 100, log_end: 100,
            has_delivery_watermark: delivery, delivery_watermark: wal.max(local) };
        check_read(f, (wal, local), &snapshots, (&offsets, &times, &rows), 0, target);
    }

    #[test]
    fn eviction_changes_source_without_changing_logical_first_match(
        records in prop::collection::btree_map(any::<u32>(), any::<i64>(), 0..24),
        indexed in 0i64..101, hw_extra in 0i64..101, lag in -1i64..51,
        wal_seed in any::<u16>(), local_seed in any::<u16>(), floor in 0i64..101,
        target in any::<i64>(), trace in prop::collection::vec(any::<bool>(), 0..16),
    ) {
        let hw = indexed + hw_extra + lag.max(0);
        let wal = i64::from(wal_seed) % (indexed + 1); let local = i64::from(local_seed) % (indexed + 1);
        let (offsets, times, rows) = timestamp_records(&records, 2, 0);
        check_eviction((indexed, hw, lag, wal, local), &trace, (&offsets, &times, &rows), 0, floor, target);
    }
}

#[test]
fn producer_replay_cursor_cannot_replace_the_logical_read_floor() {
    let f = DeleteRecordsTrimFacts {
        requested: 1,
        current_start: 0,
        high_watermark: 10,
        log_end: 10,
        has_delivery_watermark: false,
        delivery_watermark: 0,
    };
    let window = constant_time_window(&[0, 2, 8]);
    check_read(f, (0, 0), &[0, 1, 6, 6, 11], window, 0, 100);
    assert!(
        completed_trim_preserves_retained_timestamp(f, (0, 0), &[6], window, 0, 100)
            == Ok((1, Some(0), 6, Some(1)))
    );
    check_read(
        DeleteRecordsTrimFacts { requested: 11, ..f },
        (0, 0),
        &[6],
        window,
        0,
        100,
    );
    check_read(
        DeleteRecordsTrimFacts { requested: -2, ..f },
        (0, 0),
        &[6],
        window,
        0,
        100,
    );
}

#[test]
fn physical_eviction_preserves_remote_matches_and_reports_disabled_state_exactly() {
    let window = constant_time_window(&[1, 3, 9]);
    for trace in [&[][..], &[false, false], &[true], &[
        false, true, false, true,
    ]] {
        check_eviction((10, 10, 2, 2, 2), trace, window, 0, 2, 100);
    }
    assert!(
        physical_eviction_routes_retained_timestamp(
            (10, 10, 2, 2, 2),
            &[true, true],
            window,
            0,
            2,
            100
        ) == (8, 8, Some((1, true)))
    );
    check_physical(10, 10, 2, 0, -1, &[true, true]);
    check_physical(10, 10, 2, 0, 11, &[true, true]);
    check_physical(i64::MAX, i64::MAX, i64::MAX, 0, 0, &[true, true]);
    let f = DeleteRecordsTrimFacts {
        requested: -1,
        current_start: i64::MAX,
        high_watermark: i64::MAX,
        log_end: i64::MAX,
        has_delivery_watermark: true,
        delivery_watermark: i64::MAX,
    };
    check_read(
        f,
        (i64::MAX, i64::MAX),
        &[i64::MAX],
        (&[], &[], &[]),
        i64::MAX,
        i64::MIN,
    );
}
