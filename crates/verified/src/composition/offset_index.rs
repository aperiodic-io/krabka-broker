use {
    super::{
        OffsetEntry, offset_index_lookup, offset_index_position_at_or_after,
        restore_offset_index_entry_valid,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn offset_archive_valid(entries: Seq<OffsetEntry>, max_relative: Int, log_bytes: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < entries.len()
            ==> entries[i].key@ <= max_relative && entries[i].coordinate@ < log_bytes)
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < entries.len()
            ==> entries[i].key@ < entries[j].key@ && entries[i].coordinate@ < entries[j].coordinate@)
    }
}

krabka_macros::validate_archive_rows! {
/// Return actual floor/ceiling byte positions after complete row validation.
/// Invalid archives are rejected exactly. Empty indexes keep the zero fallback,
/// including empty logs. Byte bounds alone do not establish truthful batch rows.
#[ensures(|result| match result {
    Err(()) => !offset_archive_valid(entries@, max_relative@, log_bytes@),
    Ok((floor, ceiling)) => offset_archive_valid(entries@, max_relative@, log_bytes@)
        && (entries@.len() == 0 || floor@ < log_bytes@)
        && ((floor@ == 0 && forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].key@ > target@)
            || exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].key@ <= target@
                && floor == entries@[i].coordinate
                && forall<j: Int> i < j && j < entries@.len() ==> entries@[j].key@ > target@)
        && match ceiling {
            None => forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].key@ < target@,
            Some(position) => floor@ <= position@ && position@ < log_bytes@
                && exists<i: Int> 0 <= i && i < entries@.len() && target@ <= entries@[i].key@
                    && position == entries@[i].coordinate
                    && forall<j: Int> 0 <= j && j < i ==> entries@[j].key@ < target@,
        },
})]
pub(super) fn validated_index_bounds_lookup(
    entries: &[OffsetEntry],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) -> Result<(u32, Option<u32>), ()>;
    entries, i, previous, relative, position;
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].key@ <= max_relative@ && entries@[j].coordinate@ < log_bytes@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].key@ < entries@[k].key@ && entries@[j].coordinate@ < entries@[k].coordinate@)]
    ;
        restore_offset_index_entry_valid(previous, relative, position, max_relative, log_bytes);
    let floor = offset_index_lookup(entries, target);
    let ceiling = offset_index_position_at_or_after(entries, target);
    Ok((floor, ceiling))
}
