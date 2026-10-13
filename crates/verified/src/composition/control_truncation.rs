use {
    crate::{
        raft::{control_history_frontier, frontier_reaches},
        storage::{truncation_batch_retained, truncation_frontier},
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A complete ordered batch sequence begins after its nonnegative physical floor.
pub fn physical_batch_sequence_valid(ends: Seq<i64>, start: Int, floor: Int) -> bool {
    pearlite! {
        0 <= start && start <= floor
        && (forall<i: Int> 0 <= i && i < ends.len() ==> start < ends[i]@)
        && (crate::sequence::strictly_increasing(ends))
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn physical_truncation_input_valid(
    ends: Seq<i64>,
    start: Int,
    cut: Int,
    hwm: Int,
) -> bool {
    pearlite! {
        physical_batch_sequence_valid(ends, start, cut)
        && 0 <= hwm && hwm <= if ends.len() == 0 { start } else { ends[ends.len() - 1]@ }
    }
}

// Retained batches, actual end, HWM, retained/committed history lengths,
// pending target still present, pending target committed.
type TruncatedControls = (usize, i64, i64, usize, usize, bool, bool);

/// Whole-batch physical truncation must bound every logical projection by the
/// actual retained end, including a cut inside a control batch. Histories may
/// carry a snapshot/genesis baseline before `physical_start`. Complete decoded
/// batch ends and ordered history offsets are host facts. Cuts below the first
/// local offset use reset-to and are outside this retained-prefix composition.
#[requires(physical_truncation_input_valid(ends@, physical_start@, cut@, previous_hwm@))]
#[requires(crate::sequence::strictly_increasing(history@))]
#[requires(0 <= pending_end@)]
#[ensures((result.0@ <= ends@.len())
    && (forall<i: Int> 0 <= i && i < ends@.len() ==> (i < result.0@) == (ends@[i]@ <= cut@))
    && (result.1@ == if result.0@ == 0 { physical_start@ } else { ends@[result.0@ - 1]@ })
    && (physical_start@ <= result.1@ && result.1@ <= cut@)
    && (result.2@ == previous_hwm@.min(result.1@) && 0 <= result.2@ && result.2@ <= result.1@)
    && (result.4@ <= result.3@ && result.3@ <= history@.len())
    && (forall<i: Int> 0 <= i && i < history@.len() ==>
    (i < result.3@) == (history@[i]@ < result.1@)
    && (i < result.4@) == (history@[i]@ < previous_hwm@ && history@[i]@ < result.1@))
    && (forall<i: Int> 0 <= i && i < history@.len()
    && result.0@ < ends@.len() && result.1@ <= history@[i]@ && history@[i]@ < ends@[result.0@]@
    ==> result.3@ <= i && result.4@ <= i)
    && (result.5 == (pending_end@ <= result.1@))
    && (result.6 == (pending_end@ <= previous_hwm@ && pending_end@ <= result.1@))
    && (result.6 ==> result.5))]
pub(super) fn whole_batch_truncation_bounds_controls(
    ends: &[i64],
    physical_start: i64,
    cut: i64,
    previous_hwm: i64,
    history: &[i64],
    pending_end: i64,
) -> TruncatedControls {
    let mut kept = 0usize;
    let mut end = physical_start;
    #[invariant(kept@ <= ends@.len())]
    #[invariant(end@ == if kept@ == 0 { physical_start@ } else { ends@[kept@ - 1]@ })]
    #[invariant(physical_start@ <= end@ && end@ <= cut@)]
    #[invariant(forall<i: Int> 0 <= i && i < kept@ ==> ends@[i]@ <= cut@)]
    #[variant(ends@.len() - kept@)]
    while kept < ends.len() && truncation_batch_retained(ends[kept] - 1, cut) {
        end = ends[kept];
        kept += 1;
    }
    proof_assert!(forall<i: Int> kept@ <= i && i < ends@.len() ==> cut@ < ends@[i]@);
    let hwm = truncation_frontier(previous_hwm, end);
    let retained = control_history_frontier(history, end);
    let committed = control_history_frontier(history, hwm);
    proof_assert!(committed@ <= retained@);
    let present = frontier_reaches(end, pending_end);
    let ready = frontier_reaches(hwm, pending_end);
    (kept, end, hwm, retained, committed, present, ready)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The physical frontier is the greatest whole batch end at or below the truncation cut.
pub(super) fn whole_batch_frontier(ends: Seq<i64>, start: i64, cut: Int, frontier: i64) -> bool {
    pearlite! { start@ <= frontier@ && frontier@ <= cut
    && (frontier == start || exists<i: Int> 0 <= i && i < ends.len() && ends[i] == frontier)
    && (forall<i: Int> 0 <= i && i < ends.len() && ends[i]@ <= cut ==> ends[i]@ <= frontier@) }
}
