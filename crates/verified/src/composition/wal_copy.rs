use {
    super::{
        DeleteRecordsTrimApplication, FetchWatermarks, WalCopyBatch,
        delete_records_trim_application, fetch_visibility, in_half_open_window,
        local_append_coordinates, local_recovery_batch_step, produce_durability_frontier,
        wal_batch_equal, wal_covering_batch_range,
    },
    creusot_std::prelude::*,
};

open_logic! {
/// Equality of decoded coordinates and the complete observed byte payload.
pub fn copy_batch_equal(left: WalCopyBatch, right: WalCopyBatch) -> bool {
    pearlite! { left.0 == right.0 && left.1 == right.1 && left.2@ == right.2@ }
}
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
#[requires(0 <= count && count <= batches.len())]
#[ensures(result >= 0)]
#[variant(count)]
pub(crate) fn wal_copy_byte_count(batches: Seq<WalCopyBatch>, count: Int) -> Int {
    pearlite! {
        if count == 0 { 0 } else {
            wal_copy_byte_count(batches, count - 1) + batches[count - 1].2@.len()
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= lower && lower <= upper && upper <= batches.len())]
#[ensures(wal_copy_byte_count(batches, lower) <= wal_copy_byte_count(batches, upper))]
#[variant(upper)]
fn lemma_wal_copy_byte_count_monotone(batches: Seq<WalCopyBatch>, lower: Int, upper: Int) {
    if lower < upper {
        lemma_wal_copy_byte_count_monotone(batches, lower, upper - 1);
    }
}

/// Checking actual batch bytes, appending, acknowledging, and replaying any
/// length of copied prefix agree on both logical and byte extents. Metadata
/// must faithfully decode the observed bytes; reads, copying, and fsync are
/// host obligations. Admission completeness excludes an always-reject proof.
#[ensures((result != None) == (
    start@ >= 0 && source@.len() == stored@.len() && position@ <= file_end@
    && position@ + wal_copy_byte_count(source@, source@.len()) <= file_end@
    && (forall<i: Int> 0 <= i && i < source@.len() ==>
        copy_batch_coordinates_valid(source@[i])
        && source@[i].0@ == (if i == 0 { start@ }
            else { source@[i - 1].0@ + source@[i - 1].1@ + 1 })
        && source@[i].2@.len() > 0
        && copy_batch_equal(source@[i], stored@[i]))
    && target@ == (if source@.len() == 0 { start@ }
        else { source@[source@.len() - 1].0@ + source@[source@.len() - 1].1@ + 1 })
))]
#[ensures(match result {
    None => true,
    Some((end, bytes_end)) => end == target && start@ <= end@
        && (source@.len() > 0 ==> start@ < end@ && position@ < bytes_end@)
        && bytes_end@ == position@ + wal_copy_byte_count(source@, source@.len())
        && position@ <= bytes_end@ && bytes_end@ <= file_end@,
})]
#[ensures(match result {
    None => true,
    Some(_) => source@.len() == stored@.len()
        && forall<i: Int> 0 <= i && i < source@.len()
            ==> copy_batch_equal(source@[i], stored@[i]),
})]
pub(super) fn checked_wal_copy_replays_exactly(
    source: &[WalCopyBatch],
    stored: &[WalCopyBatch],
    start: i64,
    target: i64,
    position: u64,
    file_end: u64,
) -> Option<(i64, u64)> {
    if source.len() != stored.len() || start < 0 || position > file_end {
        return None;
    }
    let mut cursor = start;
    let mut byte_cursor = position;
    let mut i = 0usize;
    #[invariant(i@ <= source@.len())]
    #[invariant(cursor@ >= 0 && start@ <= cursor@)]
    #[invariant(i@ > 0 ==> start@ < cursor@ && position@ < byte_cursor@)]
    #[invariant(cursor@ == if i@ == 0 { start@ }
        else { source@[i@ - 1].0@ + source@[i@ - 1].1@ + 1 })]
    #[invariant(byte_cursor@ == position@ + wal_copy_byte_count(source@, i@))]
    #[invariant(position@ <= byte_cursor@ && byte_cursor@ <= file_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        copy_batch_coordinates_valid(source@[j])
        && source@[j].0@ == (if j == 0 { start@ }
            else { source@[j - 1].0@ + source@[j - 1].1@ + 1 })
        && source@[j].2@.len() > 0
        && copy_batch_equal(source@[j], stored@[j]))]
    #[variant(source@.len() - i@)]
    while i < source.len() {
        #[cfg(creusot)]
        proof_assert!({
            lemma_wal_copy_byte_count_monotone(source@, i@ + 1, source@.len());
            wal_copy_byte_count(source@, i@ + 1) <= wal_copy_byte_count(source@, source@.len())
        });
        // General replication admits compaction holes; a WAL copy requires
        // the exact contiguous prefix checked by the production raw reader.
        if source[i].0 != cursor {
            return None;
        }
        let (last, next) = local_append_coordinates(cursor, source[i].0, source[i].1)?;
        let (stored_last, _) = local_append_coordinates(cursor, stored[i].0, stored[i].1)?;
        if !wal_batch_equal(
            (source[i].0, last, &source[i].2),
            (stored[i].0, stored_last, &stored[i].2),
        ) {
            return None;
        }
        let acknowledgement = produce_durability_frontier(source[i].0, source[i].1)?;
        let step = local_recovery_batch_step(
            byte_cursor,
            file_end,
            cursor,
            stored[i].0,
            stored[i].1,
            stored[i].2.len() as u64,
        )?;
        if step.next_offset != next || acknowledgement != next {
            return None;
        }
        cursor = step.next_offset;
        byte_cursor = step.valid_end;
        i += 1;
    }
    if cursor != target {
        return None;
    }
    Some((cursor, byte_cursor))
}

/// Covering-read validation, actual-byte copy, append/ack/recovery geometry,
/// and logical Fetch admission compose even when retention cuts inside a batch.
/// Every visible offset has a concrete byte-identical copied-batch witness;
/// offsets before either retained floor have none. Metadata and host I/O must
/// faithfully represent the compared bytes; this does not prove crash publication.
#[ensures((result != None) == (
    0 <= floors.0@ && 0 <= floors.1@ && floors.0@ <= target@ && floors.1@ <= target@
    && source@.len() == stored@.len() && position@ <= file_end@
    && position@ + wal_copy_byte_count(source@, source@.len()) <= file_end@
    && (if source@.len() == 0 { floors.0@.max(floors.1@) == target@ } else {
        floors.0@.max(floors.1@) < target@ && source@[0].0@ <= floors.0@.max(floors.1@)
        && floors.0@.max(floors.1@) < source@[0].0@ + source@[0].1@ + 1
        && target@ == source@[source@.len() - 1].0@ + source@[source@.len() - 1].1@ + 1
    })
    && (forall<i: Int> 0 <= i && i < source@.len() ==>
        copy_batch_coordinates_valid(source@[i])
        && (i > 0 ==> source@[i].0@ == source@[i - 1].0@ + source@[i - 1].1@ + 1)
        && source@[i].2@.len() > 0
        && copy_batch_equal(source@[i], stored@[i]))
))]
#[ensures(match result {
    None => true,
    Some((physical, bytes_end, selected)) =>
        physical@ == (if source@.len() == 0 { floors.0@.max(floors.1@) } else { source@[0].0@ })
        && 0 <= physical@ && physical@ <= floors.0@.max(floors.1@)
        && bytes_end@ == position@ + wal_copy_byte_count(source@, source@.len())
        && bytes_end@ <= file_end@
        && (forall<i: Int> 0 <= i && i < source@.len() ==>
            physical@ <= source@[i].0@ && source@[i].0@ + source@[i].1@ + 1 <= target@)
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < source@.len() ==>
            source@[i].0@ + source@[i].1@ < source@[j].0@)
        && (match selected { None => false, Some(_) => true }) ==
            (floors.0@ <= requested@ && floors.1@ <= requested@ && requested@ < target@)
        && (match selected {
            None => true,
            Some(i) => i@ < source@.len() && i@ < stored@.len()
                && source@[i@].0@ <= requested@
                && requested@ < source@[i@].0@ + source@[i@].1@ + 1
                && copy_batch_equal(source@[i@], stored@[i@]),
        }),
})]
pub(super) fn covering_copy_preserves_logical_fetch(
    source: &[WalCopyBatch],
    stored: &[WalCopyBatch],
    floors: (i64, i64),
    target: i64,
    position: u64,
    file_end: u64,
    requested: i64,
) -> Option<(i64, u64, Option<usize>)> {
    let floor = match delete_records_trim_application(0, floors.0, floors.1) {
        DeleteRecordsTrimApplication::RejectMalformed => return None,
        DeleteRecordsTrimApplication::TrimWal { frontier }
        | DeleteRecordsTrimApplication::TrimLocal { frontier }
        | DeleteRecordsTrimApplication::Complete { frontier } => frontier,
    };
    if target < floor {
        return None;
    }
    let mut bases: Vec<i64> = Vec::new();
    let mut lasts: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= source@.len() && bases@.len() == i@ && lasts@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        bases@[j] == source@[j].0 && lasts@[j]@ == source@[j].0@ + source@[j].1@
        && copy_batch_coordinates_valid(source@[j]))]
    #[variant(source@.len() - i@)]
    while i < source.len() {
        let (last, _) = local_append_coordinates(source[i].0, source[i].0, source[i].1)?;
        bases.push(source[i].0);
        lasts.push(last);
        i += 1;
    }
    let physical = wal_covering_batch_range(&bases, &lasts, floor, target)?;
    let (end, bytes_end) =
        checked_wal_copy_replays_exactly(source, stored, physical, target, position, file_end)?;
    let visibility = fetch_visibility(
        true,
        false,
        FetchWatermarks {
            log_start: floor,
            log_end: end,
            hw: end,
            lso: end,
            deliverable: end,
        },
        requested,
    );
    if visibility.out_of_range || visibility.empty {
        return Some((physical, bytes_end, None));
    }
    let mut j = 0usize;
    #[invariant(j@ <= source@.len())]
    #[invariant(requested@ >= if j@ == 0 { physical@ }
        else { source@[j@ - 1].0@ + source@[j@ - 1].1@ + 1 })]
    #[variant(source@.len() - j@)]
    while j < source.len() {
        let next = produce_durability_frontier(source[j].0, source[j].1)?;
        if in_half_open_window(requested, source[j].0, next) {
            return Some((physical, bytes_end, Some(j)));
        }
        j += 1;
    }
    Some((physical, bytes_end, None))
}

open_logic! {
/// A copied data batch has a nonnegative base and representable exclusive end.
fn copy_batch_coordinates_valid(batch: WalCopyBatch) -> bool {
    pearlite! { batch.0@ >= 0 && batch.1@ >= 0 && batch.0@ + batch.1@ + 1 <= i64::MAX@ }
}
}
