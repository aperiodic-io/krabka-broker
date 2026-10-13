use {
    super::{
        ProducerReloadRange, producer_snapshot_latest_index, producer_snapshot_replay_start,
        truncation_frontier,
    },
    creusot_std::prelude::*,
};

/// Truncation selects exactly a newest surviving snapshot and constructs the
/// exact replay cursor. No eligible snapshot is lost, and a snapshot in the
/// discarded tail cannot suppress replay. Tied newest offsets may select either
/// index. Snapshot decoding/corruption fallback and persistence remain external.
#[requires(replay_range_covers_cut(range, cut@))]
#[requires(0 <= range.local_start@ && range.local_start@ <= cut@)]
#[ensures(result != None)]
#[ensures(match result {
    None => true,
    Some((None, cursor)) => cursor@ == range.log_start@.max(range.local_start@),
    Some((Some(index), cursor)) => index@ < offsets@.len()
        && cursor@ == range.local_start@.max(offsets@[index@]@),
})]
#[ensures(match result {
    None => true,
    Some((selected, cursor)) =>
        range.log_start@ <= cursor@ && range.local_start@ <= cursor@ && cursor@ <= cut@
        && match selected {
            None => forall<i: Int> 0 <= i && i < offsets@.len()
                    ==> !(range.log_start@ < offsets@[i]@ && offsets@[i]@ <= cut@),
            Some(index) => index@ < offsets@.len()
                && range.log_start@ < offsets@[index@]@ && offsets@[index@]@ <= cut@
                && forall<i: Int> 0 <= i && i < offsets@.len()
                    && range.log_start@ < offsets@[i]@ && offsets@[i]@ <= cut@
                    ==> offsets@[i]@ <= offsets@[index@]@,
        },
})]
pub(super) fn truncated_snapshot_selection_bounds_replay(
    offsets: &[i64],
    range: ProducerReloadRange,
    cut: i64,
) -> Option<(Option<usize>, i64)> {
    let shortened = ProducerReloadRange {
        log_end: truncation_frontier(range.log_end, cut),
        ..range
    };
    let selected = producer_snapshot_latest_index(offsets, shortened);
    let snapshot = selected.map(|index| offsets[index]);
    let cursor = producer_snapshot_replay_start(shortened, snapshot)?;
    Some((selected, cursor))
}

/// Repeated newest-snapshot selection discards only corrupt reads (outcome 1),
/// loads decoded state (0), and stops on I/O failure (2). The returned index
/// names the original candidate, even after removals; ties may choose either.
/// The decoder's classification and filesystem effects remain host facts.
#[requires(replay_range_covers_cut(range, cut@))]
#[requires(0 <= range.local_start@ && range.local_start@ <= cut@)]
#[requires(offsets@.len() == outcomes@.len())]
#[requires(forall<i: Int> 0 <= i && i < outcomes@.len() ==> outcomes@[i]@ <= 2)]
#[ensures(match result {
    Ok((None, cursor)) => cursor@ == range.log_start@.max(range.local_start@),
    Ok((Some(index), cursor)) => index@ < offsets@.len()
        && cursor@ == range.local_start@.max(offsets@[index@]@),
    Err(_) => true,
})]
#[ensures(match result {
    Ok((None, cursor)) => cursor@ == range.log_start@.max(range.local_start@)
        && forall<i: Int> 0 <= i && i < offsets@.len()
            && range.log_start@ < offsets@[i]@ && offsets@[i]@ <= cut@
            ==> outcomes@[i] == 1u8,
    Ok((Some(index), cursor)) => index@ < offsets@.len() && outcomes@[index@] == 0u8
        && range.log_start@ < offsets@[index@]@ && offsets@[index@]@ <= cut@
        && cursor@ == range.local_start@.max(offsets@[index@]@)
        && newest_readable_snapshot(offsets@, outcomes@, range.log_start@, cut@, index@),
    Err(index) => index@ < offsets@.len() && outcomes@[index@] == 2u8
        && range.log_start@ < offsets@[index@]@ && offsets@[index@]@ <= cut@
        && newest_readable_snapshot(offsets@, outcomes@, range.log_start@, cut@, index@),
})]
pub(super) fn corrupt_snapshot_fallback_preserves_replay(
    offsets: &[i64],
    outcomes: &[u8],
    range: ProducerReloadRange,
    cut: i64,
) -> Result<(Option<usize>, i64), usize> {
    let shortened = ProducerReloadRange {
        log_end: truncation_frontier(range.log_end, cut),
        ..range
    };
    let mut candidates: Vec<i64> = Vec::new();
    let mut origins: Vec<usize> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(candidates@.len() == i@ && origins@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> candidates@[j] == offsets@[j] && origins@[j]@ == j)]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        candidates.push(offsets[i]);
        origins.push(i);
        i += 1;
    }
    #[invariant(candidates@.len() == origins@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < origins@.len()
        ==> origins@[j]@ < offsets@.len() && candidates@[j] == offsets@[origins@[j]@])]
    #[invariant(forall<k: Int> 0 <= k && k < offsets@.len() && outcomes@[k] != 1u8
        ==> crate::sequence::contains_source_index(origins@, k))]
    #[variant(candidates@.len())]
    loop {
        let Some(selected) = producer_snapshot_latest_index(&candidates, shortened) else {
            let cursor = producer_snapshot_replay_start(shortened, None).ok_or(offsets.len())?;
            return Ok((None, cursor));
        };
        let origin = origins[selected];
        match outcomes[origin] {
            0 => {
                let cursor = producer_snapshot_replay_start(shortened, Some(offsets[origin]))
                    .ok_or(offsets.len())?;
                return Ok((Some(origin), cursor));
            }
            2 => return Err(origin),
            _ => {
                // Removing preserves candidate membership; the host's swap-remove
                // changes order only, which the tied-maximum contract permits.
                let _before = snapshot!(origins@);
                proof_assert!(forall<k: Int> 0 <= k && k < offsets@.len() && outcomes@[k] != 1u8
                    ==> exists<j: Int> 0 <= j && j < origins@.len()
                        && j != selected@ && origins@[j]@ == k);
                candidates.remove(selected);
                origins.remove(selected);
                proof_assert!(forall<j: Int> 0 <= j && j < _before.len() && j != selected@
                    ==> if j < selected@ { origins@[j] == _before[j] }
                        else { origins@[j - 1] == _before[j] });
            }
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// No other noncorrupt retained snapshot lies above this selected candidate.
fn newest_readable_snapshot(
    offsets: Seq<i64>,
    outcomes: Seq<u8>,
    floor: Int,
    cut: Int,
    index: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < offsets.len()
    && outcomes[i] != 1u8 && floor < offsets[i]@ && offsets[i]@ <= cut
    ==> offsets[i]@ <= offsets[index]@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The logical retained range contains the truncation cut used for replay.
fn replay_range_covers_cut(range: ProducerReloadRange, cut: Int) -> bool {
    pearlite! { 0 <= range.log_start@ && range.log_start@ <= cut && cut <= range.log_end@ }
}
