#[cfg(creusot)]
use super::offset_index::offset_archive_valid;
use {
    super::{first_timestamp_index, validated_index_bounds_lookup},
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn offset_seek_input_valid(
    rows: Seq<(u32, u32)>,
    batches: Seq<(u32, u32)>,
    max_relative: Int,
    log_bytes: Int,
) -> bool {
    pearlite! {
        offset_archive_valid(rows, max_relative, log_bytes)
        && offset_archive_valid(batches, max_relative, log_bytes)
        && if batches.len() == 0 { log_bytes == 0 && rows.len() == 0 }
            else { batches[0].1@ == 0
                && forall<i: Int> 0 <= i && i < rows.len()
                    ==> exists<j: Int> 0 <= j && j < batches.len() && rows[i] == batches[j] }
    }
}

/// Validate sparse rows against complete decoded (last-relative-offset, byte
/// position) batch rows. The exported floor cannot skip any matching batch;
/// the masked scan returns the globally first match, bounded by a sparse ceiling
/// when present. The scalar >= search kernel is shared with timestamp lookup.
/// Faithful decoding, complete batch enumeration, physical lengths and I/O are
/// external; structural index validity alone does not prove those facts.
#[ensures(match result {
    Err(()) => !offset_seek_input_valid(rows@, batches@, max_relative@, log_bytes@),
    Ok((floor, ceiling, selected)) => offset_seek_input_valid(rows@, batches@, max_relative@, log_bytes@)
        && (batches@.len() == 0 || floor@ < log_bytes@)
        && (forall<i: Int> 0 <= i && i < batches@.len() && batches@[i].0@ >= target@
            ==> floor@ <= batches@[i].1@)
        && match selected {
            None => ceiling == None && forall<i: Int> 0 <= i && i < batches@.len() ==> batches@[i].0@ < target@,
            Some(index) => index@ < batches@.len() && batches@[index@].0@ >= target@
                && floor@ <= batches@[index@].1@ && batches@[index@].1@ < log_bytes@
                && (forall<i: Int> 0 <= i && i < index@ ==> batches@[i].0@ < target@)
                && match ceiling { None => true, Some(position) => batches@[index@].1@ <= position@ && position@ < log_bytes@ },
        },
})]
pub(super) fn indexed_offset_scan_preserves_first_batch(
    rows: &[(u32, u32)],
    batches: &[(u32, u32)],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) -> Result<(u32, Option<u32>, Option<usize>), ()> {
    let (floor, ceiling) = validated_index_bounds_lookup(rows, target, max_relative, log_bytes)?;
    let _batch_cursors = validated_index_bounds_lookup(batches, target, max_relative, log_bytes)?;
    match (batches.len(), rows.len(), log_bytes) {
        (0, 0, 0) => return Ok((floor, ceiling, None)),
        (0, _, _) => return Err(()),
        _ => {}
    }
    if batches[0].1 != 0 {
        return Err(());
    }
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> exists<k: Int> 0 <= k && k < batches@.len() && rows@[j] == batches@[k])]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let mut j = 0usize;
        #[invariant(j@ <= batches@.len())]
        #[invariant(forall<k: Int> 0 <= k && k < j@ ==> rows@[i@] != batches@[k])]
        #[variant(batches@.len() - j@)]
        while j < batches.len() && (rows[i].0 != batches[j].0 || rows[i].1 != batches[j].1) {
            j += 1;
        }
        if j == batches.len() {
            return Err(());
        }
        i += 1;
    }
    proof_assert!(forall<j: Int> 0 <= j && j < batches@.len() && batches@[j].0@ >= target@
        ==> floor@ <= batches@[j].1@);
    let mut scan: Vec<i64> = Vec::new();
    i = 0;
    #[invariant(i@ <= batches@.len() && scan@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> scan@[j]@ == if batches@[j].1@ >= floor@ { batches@[j].0@ } else { -1 })]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        scan.push(if batches[i].1 >= floor {
            i64::from(batches[i].0)
        } else {
            -1
        });
        i += 1;
    }
    let selected = first_timestamp_index(&scan, i64::from(target));
    Ok((floor, ceiling, selected))
}
