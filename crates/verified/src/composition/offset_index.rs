use {
    super::{
        offset_index_lookup, offset_index_position_at_or_after, restore_offset_index_entry_valid,
    },
    creusot_std::prelude::*,
};

open_logic! {
pub fn offset_archive_valid(entries: Seq<(u32, u32)>, max_relative: Int, log_bytes: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < entries.len()
            ==> entries[i].0@ <= max_relative && entries[i].1@ < log_bytes)
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < entries.len()
            ==> entries[i].0@ < entries[j].0@ && entries[i].1@ < entries[j].1@)
    }
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
        && ((floor@ == 0 && forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ > target@)
            || exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ <= target@
                && floor == entries@[i].1
                && forall<j: Int> i < j && j < entries@.len() ==> entries@[j].0@ > target@)
        && match ceiling {
            None => forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ < target@,
            Some(position) => floor@ <= position@ && position@ < log_bytes@
                && exists<i: Int> 0 <= i && i < entries@.len() && target@ <= entries@[i].0@
                    && position == entries@[i].1
                    && forall<j: Int> 0 <= j && j < i ==> entries@[j].0@ < target@,
        },
})]
pub(super) fn validated_index_bounds_lookup(
    entries: &[(u32, u32)],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) -> Result<(u32, Option<u32>), ()>;
    entries, i, previous, relative, position;
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].0@ <= max_relative@ && entries@[j].1@ < log_bytes@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ < entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    ;
        restore_offset_index_entry_valid(previous, relative, position, max_relative, log_bytes);
    let floor = offset_index_lookup(entries, target);
    let ceiling = offset_index_position_at_or_after(entries, target);
    Ok((floor, ceiling))
}
