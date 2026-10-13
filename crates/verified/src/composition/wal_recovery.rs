#[cfg(creusot)]
use super::control_truncation::physical_batch_sequence_valid;
use {
    super::{
        DeleteRecordsTrimApplication, FetchWatermarks, delete_records_trim_application,
        fetch_visibility, in_half_open_window, truncation_batch_retained, truncation_frontier,
        wal_checkpoint_range_valid,
    },
    creusot_std::prelude::*,
};

/// Recovery admission is complete for whole-batch ends, permits interior
/// logical floors, and discards exactly the uncertain suffix. Empty checkpoints
/// reset at their floor. Clamping visibility to the actual retained end then
/// prevents Fetch from exposing that suffix. `ends` must be the complete,
/// accurately decoded local batch sequence; publication/fsync remain host effects.
#[requires(physical_batch_sequence_valid(ends@, physical_start@, w.log_start@))]
#[requires(w.log_start@ <= w.log_end@)]
#[requires(w.log_end@ == if ends@.len() == 0 { physical_start@ }
    else { ends@[ends@.len() - 1]@ })]
#[ensures((result != None) == (
    w.log_start@ <= start@ && start@ <= cut@
    && cut@ <= (if ends@.len() == 0 { physical_start@ } else { ends@[ends@.len() - 1]@ })
    && (start == cut || exists<i: Int> 0 <= i && i < ends@.len() && ends@[i] == cut)
))]
#[ensures(match result {
    None => true,
    Some((kept, limit)) => kept@ <= ends@.len()
        && (start == cut ==> kept@ == 0)
        && (start != cut ==> kept@ > 0 && ends@[kept@ - 1] == cut
            && forall<i: Int> 0 <= i && i < ends@.len() ==>
                (i < kept@) == (ends@[i]@ <= cut@))
        && limit@ == w.hw@.min(cut@).min(w.lso@.min(cut@)).min(w.deliverable@.min(cut@))
        && limit@ <= cut@ && limit@ <= w.hw@ && limit@ <= w.lso@ && limit@ <= w.deliverable@,
})]
pub(super) fn checkpoint_truncation_bounds_fetch(
    ends: &[i64],
    physical_start: i64,
    w: FetchWatermarks,
    start: i64,
    cut: i64,
) -> Option<(usize, i64)> {
    let recovered_end = w.log_end;
    if w.log_start > start || start > cut || cut > recovered_end {
        return None;
    }
    let mut kept = 0usize;
    let mut actual_end = physical_start;
    let mut observed_last: Option<i64> = None;
    let mut i = 0usize;
    #[invariant(i@ <= ends@.len() && kept@ <= i@)]
    #[invariant(0 <= actual_end@)]
    #[invariant(actual_end@ == if kept@ == 0 { physical_start@ } else { ends@[kept@ - 1]@ })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> (j < kept@) == (ends@[j]@ <= cut@))]
    #[invariant((match observed_last { Some(last) => last@ == cut@ - 1, None => false }) ==
        (exists<j: Int> 0 <= j && j < i@ && ends@[j] == cut))]
    #[invariant((observed_last == None) ==
        (forall<j: Int> 0 <= j && j < i@ ==> ends@[j]@ < cut@))]
    #[invariant(observed_last != None ==> exists<j: Int> 0 <= j && j < i@
        && (match observed_last { Some(last) => last@ == ends@[j]@ - 1, None => false }) && cut@ <= ends@[j]@)]
    #[variant(ends@.len() - i@)]
    while i < ends.len() {
        let last = ends[i] - 1;
        if observed_last.is_none() && cut <= ends[i] {
            observed_last = Some(last);
        }
        if truncation_batch_retained(last, cut) {
            actual_end = ends[i];
            kept += 1;
        }
        i += 1;
    }
    if !wal_checkpoint_range_valid(w.log_start, recovered_end, start, cut, observed_last) {
        return None;
    }
    if start == cut {
        kept = 0;
        actual_end = start;
    }
    proof_assert!(actual_end == cut);
    let w = FetchWatermarks {
        log_start: start,
        log_end: actual_end,
        hw: truncation_frontier(w.hw, actual_end),
        lso: truncation_frontier(w.lso, actual_end),
        deliverable: truncation_frontier(w.deliverable, actual_end),
    };
    Some((kept, fetch_visibility(false, true, w, start).limit_offset))
}

/// Recovery after each completed publication step uses the old durable prefix
/// until sync and WAL publication finish, then the newly synced full prefix.
/// Phase 2 permits any partially applied native floor below the published one.
/// The rows describe the complete batches still physically present at reopen;
/// bytes, atomic checkpoint publication, and prefix-only unlinking are host facts.
#[requires(w.log_start@ >= 0 && w.log_start@ <= w.log_end@ && requested@ > w.log_start@)]
#[requires(phase.0@ <= 3 && w.log_start@ <= phase.1@)]
#[requires(if phase.0@ < 2 { phase.1 == w.log_start }
    else { phase.1@ <= w.log_end@.min(requested@) })]
#[requires(phase.0@ == 3 ==> phase.1@ == w.log_end@.min(requested@))]
#[requires(physical_batch_sequence_valid(ends@, physical_start@, phase.1@))]
#[requires(w.log_end@ == if ends@.len() == 0 { physical_start@ }
    else { ends@[ends@.len() - 1]@ })]
#[requires(w.log_start@ <= prior_end@ && prior_end@ <= w.log_end@)]
#[requires(phase.0@ < 2 ==> prior_end == w.log_start
    || exists<i: Int> 0 <= i && i < ends@.len() && ends@[i] == prior_end)]
#[ensures(result != None)]
#[ensures(match result {
    None => true,
    Some((floor, end, kept, limit, visible)) =>
        floor@ == (if phase.0@ < 2 { w.log_start@ } else { w.log_end@.min(requested@) })
        && end@ == (if phase.0@ < 2 { prior_end@ } else { w.log_end@ })
        && phase.1@ <= floor@ && floor@ <= end@ && end@ <= w.log_end@
        && kept@ <= ends@.len()
        && (floor == end ==> kept@ == 0)
        && (floor != end ==> kept@ > 0 && ends@[kept@ - 1] == end
            && forall<i: Int> 0 <= i && i < ends@.len() ==> (i < kept@) == (ends@[i]@ <= end@))
        && limit@ == end@.min(w.hw@).min(w.lso@).min(w.deliverable@)
        && visible == (floor@ <= probe@ && probe@ < limit@),
})]
pub(super) fn published_trim_bounds_recovery(
    ends: &[i64],
    physical_start: i64,
    w: FetchWatermarks,
    prior_end: i64,
    requested: i64,
    phase: (u8, i64), // completed steps: sync, WAL publication, native trim; observed native floor
    probe: i64,
) -> Option<(i64, i64, usize, i64, bool)> {
    let capped = truncation_frontier(w.log_end, requested);
    let floor = match delete_records_trim_application(capped, w.log_start, w.log_start) {
        DeleteRecordsTrimApplication::RejectMalformed => return None,
        DeleteRecordsTrimApplication::TrimWal { frontier }
        | DeleteRecordsTrimApplication::TrimLocal { frontier }
        | DeleteRecordsTrimApplication::Complete { frontier } => frontier,
    };
    let (checkpoint_floor, checkpoint_end) = if phase.0 >= 2 {
        (floor, w.log_end)
    } else {
        (w.log_start, prior_end)
    };
    let (kept, limit) = checkpoint_truncation_bounds_fetch(
        ends,
        physical_start,
        FetchWatermarks {
            log_start: phase.1,
            ..w
        },
        checkpoint_floor,
        checkpoint_end,
    )?;
    Some((
        checkpoint_floor,
        checkpoint_end,
        kept,
        limit,
        in_half_open_window(probe, checkpoint_floor, limit),
    ))
}
