use {super::*, assert2::assert};

proptest! {
    #[test]
    fn sparse_timestamp_composition_matches_full_record_oracle(
        records in proptest::collection::btree_map(any::<u32>(), any::<i64>(), 0..32),
        target in any::<i64>(),
        step in 1usize..8,
        span in 0usize..5,
    ) {
        let (offsets, timestamps, rows) = timestamp_records(&records, step, span);
        let entries: Vec<_> = rows.iter().map(|(indexed, through)|
            running_maximum_index_entry(&offsets, &timestamps, *indexed, *through)).collect();
        let expected = timestamps.iter().position(|timestamp| *timestamp >= target);
        assert!(indexed_timestamp_scan_finds_first(&entries, &offsets, &timestamps, target) == expected);
        assert!(constructed_time_index_preserves_first(&offsets, &timestamps, &rows, target) == (entries.clone(), expected));
        let (count, floor) = validated_time_scan::prefix_oracle(&entries, target);
        assert!(remote_timestamp_scan_preserves_first(&entries, &offsets, &timestamps, target) == (count, floor, expected));
        assert!(validated_remote_and_local_time_starts_agree(&entries, i64::from(u32::MAX), target) == Ok((count, floor, floor)));
    }
}

proptest! {
    #[test]
    fn trim_compositions_match_frontier_and_progress_oracles(
        requested in 0i64..=i64::MAX,
        wal in 0i64..=i64::MAX,
        local in 0i64..=i64::MAX,
        applied in proptest::collection::vec(any::<bool>(), 0..32),
        snapshots in proptest::collection::vec(any::<i64>(), 0..16),
    ) {
        let frontier = requested.max(wal).max(local);
        let completed = applied.iter().filter(|applied| **applied).count();
        let expected = (
            if completed > 0 { frontier } else { wal },
            if completed > 1 || (completed > 0 && wal == frontier) { frontier } else { local },
        );
        assert!(trim_steps_converge(requested, wal, local, &applied) == expected);
        let facts = DeleteRecordsTrimFacts {
            requested,
            current_start: local,
            high_watermark: frontier,
            log_end: frontier,
            has_delivery_watermark: true,
            delivery_watermark: wal.max(local),
        };
        trim_witnesses::check_logical(facts, wal, local, &snapshots);
        trim_witnesses::check_physical(frontier, frontier, 0, wal, local, &applied);
    }
}

proptest! {
    #[test]
    fn checked_wal_copy_matches_independent_extent_and_byte_oracles(
        frames in proptest::collection::vec((0i32..8, proptest::collection::vec(any::<u8>(), 1..16)), 0..12),
        start in 0i64..64,
        position in any::<u64>(),
        capacity in 0u64..128,
        mutation in 0usize..5,
        matching_target in any::<bool>(),
        other_target in 0i64..128,
    ) {
        let mut cursor = start;
        let source: Vec<WalCopyBatch> = frames.into_iter().map(|(delta, bytes)| {
            let base = cursor;
            cursor += i64::from(delta) + 1;
            (base, delta, bytes)
        }).collect();
        let mut stored = source.clone();
        if let Some(first) = stored.first_mut() {
            match mutation {
                1 => first.2[0] ^= 1,
                2 => first.0 += 1,
                3 => first.1 += 1,
                _ => {},
            }
        }
        if mutation == 4 { stored.pop(); }
        let target = if matching_target { cursor } else { other_target };
        let file_end = position.saturating_add(capacity);
        let bytes: u128 = source.iter().map(|(_, _, bytes)| bytes.len() as u128).sum();
        let admitted = source == stored && target == cursor
            && u128::from(position) + bytes <= u128::from(file_end);
        let expected = admitted.then(|| (target, u64::try_from(u128::from(position) + bytes).unwrap()));
        assert!(checked_wal_copy_replays_exactly(&source, &stored, start, target, position, file_end) == expected);
    }
}

proptest! {
    #[test]
    fn covering_copy_matches_independent_bytes_extents_and_visible_offset_oracles(
        widths in prop::collection::vec(1i32..7, 0..16),
        physical in 0i64..20,
        floor_seed in any::<u16>(), prior_seed in any::<u16>(), request_seed in any::<u16>(),
        position in any::<u64>(), shortfall in 0u64..3, divergent in any::<bool>(),
    ) {
        let mut target = physical;
        let source: Vec<WalCopyBatch> = widths.iter().enumerate().map(|(i, width)| {
            let base = target;
            target += i64::from(*width);
            (base, width - 1, std::vec![u8::try_from(i).unwrap(), 42])
        }).collect();
        let start = physical + i64::from(floor_seed) % i64::from(widths.first().copied().unwrap_or(1));
        let prior = physical + i64::from(prior_seed) % (target - physical + 2);
        let source: Vec<_> = source.into_iter().filter(|batch|
            batch.0 + i64::from(batch.1) + 1 > start.max(prior)).collect();
        let mut stored = source.clone();
        if divergent && !stored.is_empty() { stored[0].2[0] ^= 1; }
        let copied_base = source.first().map_or(start.max(prior), |batch| batch.0);
        let requested = physical - 1 + i64::from(request_seed) % (target - physical + 3);
        let total = u64::try_from(source.len()).unwrap() * 2;
        let file_end = position.saturating_add(total).saturating_sub(shortfall);
        let expected = if prior <= target && source == stored
            && position.checked_add(total).is_some_and(|end| end <= file_end) {
            let selected = (requested >= start.max(prior) && requested < target)
                .then(|| source.iter().position(|batch| batch.0 <= requested
                    && requested < batch.0 + i64::from(batch.1) + 1).unwrap());
            Some((copied_base, position + total, selected))
        } else { None };
        assert!(covering_copy_preserves_logical_fetch(&source, &stored, (start, prior), target,
            position, file_end, requested) == expected);
    }
}

#[test]
fn covering_copy_hides_trimmed_records_without_losing_whole_batch_bytes() {
    let source = [(0, 2, std::vec![1, 2]), (3, 2, std::vec![3, 4])];
    for requested in -1..=7 {
        let selected = match requested {
            2 => Some(0),
            3..=5 => Some(1),
            _ => None,
        };
        assert!(
            covering_copy_preserves_logical_fetch(&source, &source, (1, 2), 6, 10, 14, requested)
                == Some((0, 14, selected))
        );
    }
    assert!(
        covering_copy_preserves_logical_fetch(&source, &source, (1, 2), 5, 10, 14, 2).is_none()
    );
    assert!(
        covering_copy_preserves_logical_fetch(&source[1..], &source[1..], (1, 4), 6, 10, 12, 4)
            == Some((3, 12, Some(0)))
    );
    assert!(
        covering_copy_preserves_logical_fetch(&source, &source, (3, 2), 6, 10, 14, 3).is_none()
    );
    assert!(
        covering_copy_preserves_logical_fetch(
            &[],
            &[],
            (i64::MAX, i64::MAX),
            i64::MAX,
            u64::MAX,
            u64::MAX,
            i64::MAX
        ) == Some((i64::MAX, u64::MAX, None))
    );
}

proptest! {
    #[test]
    fn checkpoint_recovery_matches_independent_batch_boundary_oracle(
        widths in prop::collection::vec(1i64..8, 0..24),
        physical_start in 0i64..20,
        floor_seed in any::<u16>(),
        start in -1i64..210,
        cut in -1i64..210,
        hw in any::<i64>(), lso in any::<i64>(), deliverable in any::<i64>(),
    ) {
        let mut end = physical_start;
        let ends: Vec<_> = widths.into_iter().map(|width| { end += width; end }).collect();
        let floor = physical_start + i64::from(floor_seed) % (end - physical_start + 1);
        let expected = if floor <= start && start <= cut && cut <= end
            && (start == cut || ends.contains(&cut)) {
            Some((if start == cut { 0 } else { ends.iter().filter(|next| **next <= cut).count() },
                hw.min(lso).min(deliverable).min(cut)))
        } else { None };
        assert!(checkpoint_truncation_bounds_fetch(&ends, physical_start,
            FetchWatermarks { log_start: floor, log_end: end, hw, lso, deliverable }, start, cut) == expected);
    }
}

#[test]
fn checkpoint_recovery_handles_empty_interior_and_extreme_frontiers() {
    for (ends, physical_start, floor, start, cut, expected) in [
        (&[3, 6][..], 0, 0, 0, 1, None),
        (&[3, 6][..], 0, 0, 1, 3, Some((1, 3))),
        (&[3, 6][..], 0, 1, 1, 1, Some((0, 1))),
        (
            &[][..],
            i64::MAX,
            i64::MAX,
            i64::MAX,
            i64::MAX,
            Some((0, i64::MAX)),
        ),
        (
            &[i64::MAX][..],
            i64::MAX - 3,
            i64::MAX - 2,
            i64::MAX - 2,
            i64::MAX,
            Some((1, i64::MAX)),
        ),
    ] {
        assert!(
            checkpoint_truncation_bounds_fetch(
                ends,
                physical_start,
                FetchWatermarks {
                    log_start: floor,
                    log_end: ends.last().copied().unwrap_or(physical_start),
                    hw: i64::MAX,
                    lso: i64::MAX,
                    deliverable: i64::MAX
                },
                start,
                cut
            ) == expected
        );
    }
}
