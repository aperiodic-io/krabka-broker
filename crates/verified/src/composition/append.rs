use {
    super::{
        local_append_coordinates, local_recovery_batch_step, produce_durability_frontier,
        reserve_offsets, timestamp_scan_next, wal_reservation_frontier,
    },
    creusot_std::prelude::*,
};

/// Actual outputs of the five paths over the same decoded batch.
#[cfg_attr(not(creusot), derive(Debug, PartialEq, Eq))]
pub(super) struct AppendFrontiers {
    pub append: (i64, i64),
    pub reservation: (i64, i64),
    pub recovery: (i64, i64),
    pub acknowledgement: i64,
    pub scan: i64,
}

/// Allocation, live append, recovery, scanning, and acknowledgement agree on
/// one exclusive frontier. The synthetic byte extent checks coordinates only;
/// byte validity, persistence and serialized allocation remain host obligations.
#[ensures((result != None)
    == (base@ >= 0 && delta@ >= 0 && base@ + delta@ + 1 <= i64::MAX@))]
#[ensures(match result {
    None => true,
    Some(w) => w.append.0@ == base@ + delta@
        && w.append.1@ == base@ + delta@ + 1
        && w.reservation.0@ == base@
        && w.reservation.1@ == w.append.1@
        && w.recovery.0@ == w.append.0@
        && w.recovery.1@ == w.append.1@
        && w.acknowledgement@ == w.append.1@
        && w.scan@ == w.append.1@
        && base@ <= w.append.0@ && w.append.0@ < w.append.1@,
})]
pub(super) fn append_frontiers_agree(base: i64, delta: i32) -> Option<AppendFrontiers> {
    let append = local_append_coordinates(base, base, delta)?;
    let acknowledgement = produce_durability_frontier(base, delta)?;
    let reservation = reserve_offsets(base, i64::from(delta) + 1)?;
    let recovery = local_recovery_batch_step(0, 1, base, base, delta, 1)?;
    let scan = timestamp_scan_next(base, base, delta)?;
    Some(AppendFrontiers {
        append,
        reservation,
        recovery: (recovery.last_offset, recovery.next_offset),
        acknowledgement,
        scan,
    })
}

/// Return the start, split and end of two admitted contiguous reservations.
/// This assumes serialized use of the returned frontier, not a lock. Failure
/// rejects the pair; it does not roll back an already issued first reservation.
#[ensures((result != None)
    == (base@ >= 0 && first_count@ > 0 && second_count@ > 0
        && base@ + first_count@ + second_count@ <= i64::MAX@))]
#[ensures(match result {
    None => true,
    Some((start, split, end)) => start@ == base@
        && split@ == base@ + first_count@
        && end@ == split@ + second_count@
        && start@ < split@ && split@ < end@
        && forall<offset: Int> start@ <= offset && offset < split@
            ==> !(split@ <= offset && offset < end@),
})]
pub(super) fn reservations_do_not_overlap(
    base: i64,
    first_count: i64,
    second_count: i64,
) -> Option<(i64, i64, i64)> {
    let (first, split) = reserve_offsets(base, first_count)?;
    let frontier = wal_reservation_frontier(base, first, first_count)?;
    let (_second, next) = reserve_offsets(frontier, second_count)?;
    proof_assert!(frontier == split && _second == split);
    Some((first, split, next))
}

/// Two controller reservations map to strictly ordered acknowledgement waits
/// and matching recovered/scan frontiers. No batch can reuse an offset from
/// the prior reservation. This consumes the exported composition contracts;
/// controller serialization, batch bytes and durable votes remain external.
#[ensures((result != None)
    == (base@ >= 0 && first_delta@ >= 0 && second_delta@ >= 0
        && base@ + first_delta@ + second_delta@ + 2 <= i64::MAX@))]
#[ensures(match result {
    None => true,
    Some((first, second)) => first.reservation.0@ == base@
        && first.append.0@ == base@ + first_delta@
        && first.append.1@ == first.append.0@ + 1
        && first.reservation.1@ == first.append.1@
        && first.recovery.0@ == first.append.0@ && first.recovery.1@ == first.append.1@
        && first.acknowledgement@ == first.append.1@ && first.scan@ == first.append.1@
        && second.reservation.0@ == first.acknowledgement@
        && second.append.0@ == second.reservation.0@ + second_delta@
        && second.append.1@ == second.append.0@ + 1
        && second.reservation.1@ == second.append.1@
        && second.recovery.0@ == second.append.0@ && second.recovery.1@ == second.append.1@
        && second.acknowledgement@ == second.append.1@ && second.scan@ == second.append.1@
        && first.acknowledgement@ < second.acknowledgement@
        && first.append.0@ < second.reservation.0@,
})]
pub(super) fn reserved_pair_preserves_recovery_and_ack_order(
    base: i64,
    first_delta: i32,
    second_delta: i32,
) -> Option<(AppendFrontiers, AppendFrontiers)> {
    if first_delta < 0 || second_delta < 0 {
        return None;
    }
    let (_, split, _end) = reservations_do_not_overlap(
        base,
        i64::from(first_delta) + 1,
        i64::from(second_delta) + 1,
    )?;
    let first = append_frontiers_agree(base, first_delta)?;
    let second = append_frontiers_agree(split, second_delta)?;
    proof_assert!(first.acknowledgement == split && second.acknowledgement == _end);
    Some((first, second))
}
