use {
    super::{ProducerDecision, ProducerSnapshotEntryFacts, completed_batches_preserve_first_retry},
    assert2::assert,
    proptest::prelude::*,
    std::collections::BTreeMap,
};

type Row = ProducerSnapshotEntryFacts;

fn modulo(value: i64) -> i32 { i32::try_from(value.rem_euclid(1_i64 << 31)).unwrap() }

fn row(epoch: i16, last: i64, delta: i32, sequence: i32) -> Row {
    Row {
        producer_id: 42,
        producer_epoch: epoch,
        last_sequence: sequence,
        last_offset: last,
        offset_delta: delta,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    }
}

fn check(
    end: i64,
    hwm: i64,
    current: Option<i16>,
    old: &[Row],
    incoming: Row,
    request: (i16, i32, i32),
) {
    let accepted = current.is_none_or(|epoch| incoming.producer_epoch >= epoch);
    let mut candidates = BTreeMap::new();
    if current.is_some_and(|epoch| incoming.producer_epoch <= epoch) {
        for (index, row) in old.iter().enumerate() {
            candidates.insert(row.last_offset, index);
        }
    }
    if accepted {
        candidates.entry(incoming.last_offset).or_insert(old.len());
    }
    let sources: Vec<_> = candidates
        .values()
        .rev()
        .take(5)
        .copied()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let get = |source| {
        if source == old.len() {
            incoming
        } else {
            old[source]
        }
    };
    let found = sources.iter().enumerate().find(|(_, source)| {
        let row = get(**source);
        request.0 == row.producer_epoch
            && request.1 == modulo(i64::from(row.last_sequence) - i64::from(row.offset_delta))
            && row.last_sequence == modulo(i64::from(request.1) + i64::from(request.2))
    });
    let epoch = sources
        .last()
        .map_or_else(|| current.unwrap(), |source| get(*source).producer_epoch);
    let decision = if let Some((slot, _)) = found {
        ProducerDecision::Duplicate {
            retained: if slot + 1 == sources.len() { 4 } else { slot },
        }
    } else if request.0 < epoch {
        ProducerDecision::Fenced
    } else if request.0 > epoch {
        if request.1 == 0 {
            ProducerDecision::Append
        } else {
            ProducerDecision::OutOfOrder
        }
    } else if request.1
        == sources.last().map_or(0, |source| {
            modulo(i64::from(get(*source).last_sequence) + 1)
        })
    {
        ProducerDecision::Append
    } else {
        ProducerDecision::OutOfOrder
    };
    let witness = found.map(|(_, &source)| {
        let row = get(source);
        (
            source,
            row.last_offset - i64::from(row.offset_delta),
            row.last_offset + 1,
            hwm > row.last_offset,
        )
    });
    let ends: Vec<_> = old.iter().map(|row| row.last_offset).collect();
    assert!(
        crate::producer::producer_completion_window(
            current,
            incoming.producer_epoch,
            &ends,
            incoming.last_offset
        ) == (accepted, sources.clone())
    );
    assert!(
        completed_batches_preserve_first_retry(end, hwm, current, old, incoming, request)
            == (accepted, sources, decision, witness)
    );
}

proptest! {
    #[test]
    fn completed_windows_match_sorted_distinct_physical_batches(
        batches in proptest::collection::vec((0_i32..4, 0_i32..=i32::MAX), 0..=5),
        old_epoch in 0_i16..5, new_epoch in 0_i16..5, absent in any::<bool>(),
        new_last in 0_i64..100, new_delta in 0_i32..4, sequence in 0_i32..=i32::MAX,
        hwm in 0_i64..=101, request in (any::<i16>(), any::<i32>(), any::<i32>()),
    ) {
        let mut last = 0;
        let old: Vec<_> = batches.into_iter().map(|(delta, sequence)| {
            last += i64::from(delta) + 2;
            row(old_epoch, last, delta, sequence)
        }).collect();
        let current = if absent && old.is_empty() { None } else { Some(old_epoch) };
        let incoming = row(new_epoch, new_last, new_delta.min(i32::try_from(new_last).unwrap()), sequence);
        check(101, hwm, current, &old, incoming, request);
        for batch in old.iter().chain(std::iter::once(&incoming)) {
            check(101, hwm, current, &old, incoming,
                (batch.producer_epoch, modulo(i64::from(batch.last_sequence) - i64::from(batch.offset_delta)), batch.offset_delta));
        }
    }
}

#[test]
fn older_alias_cannot_return_after_leaving_the_completed_window() {
    let old: Vec<_> = (1..=5).map(|offset| row(7, offset, 0, 0)).collect();
    check(6, 6, Some(7), &old, row(7, 0, 0, 0), (7, 0, 0));
    let (_, sources, _, Some((source, base, frontier, ready))) =
        completed_batches_preserve_first_retry(6, 6, Some(7), &old, row(7, 0, 0, 0), (7, 0, 0))
    else {
        panic!("a retained alias must match")
    };
    assert!(sources == vec![0, 1, 2, 3, 4]);
    assert!((source, base, frontier, ready) == (0, 1, 2, true));
}

#[yare::parameterized(
    older_request_epoch = { (6, 0, 0) },
    current_epoch = { (7, 0, 0) },
    current_epoch_nonzero_sequence = { (7, 1, 0) },
    newer_epoch = { (8, 0, 0) },
)]
fn marker_epoch_keeps_exact_retry_bounds(request: (i16, i32, i32)) {
    check(10, 0, Some(7), &[], row(6, 2, 0, 0), request);
}

#[yare::parameterized(
    hwm_at_last_offset = { i64::MAX - 1 },
    hwm_at_exclusive_end = { i64::MAX },
)]
fn maximum_offset_keeps_exact_retry_bounds(hwm: i64) {
    let old = [row(7, i64::MAX - 1, 2, 0)];
    check(
        i64::MAX,
        hwm,
        Some(7),
        &old,
        row(7, 0, 0, 9),
        (7, i32::MAX - 1, 2),
    );
}

#[test]
fn physical_selection_needs_only_ordered_offsets() {
    use crate::producer::producer_completion_window as select;

    assert!(select(Some(-1), -2, &[-9, -8], -7) == (false, vec![0, 1]));
    assert!(select(Some(-1), 0, &[-9, -8], -7) == (true, vec![2]));
    assert!(
        select(Some(i16::MIN), i16::MIN, &[i64::MIN, -1, i64::MAX], 0) == (true, vec![0, 1, 3, 2])
    );
    assert!(select(None, i16::MIN, &[], i64::MIN) == (true, vec![0]));
}
