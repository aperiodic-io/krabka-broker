use {
    super::{completion_offset, five_completions_above},
    creusot_std::prelude::*,
};

mod order;
mod witness;

/// An unchanged window contains every original source. Isolate the existential
/// witness from the executable selector's mutable state and epoch branches.
// cargo-mutants: #[cfg(creusot)] proof lemma; not compiled outside Creusot.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ == j)]
#[ensures(result)]
#[ensures(result ==> (forall<i: Int> 0 <= i && i < selected.len() ==>
    exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == i))]
pub(super) fn lemma_identity_window(selected: Seq<usize>) -> bool {
    proof_assert!(forall<i: Int> 0 <= i && i < selected.len() ==> selected[i]@ == i);
    true
}

/// A sorted insertion followed by a one-row prefix cut keeps the greatest
/// five distinct offsets. This isolates the inverse-index proof from the
/// mutable Vec and producer-epoch branches of the executable selector.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(ends.len() <= 5 && 0 <= position && position <= ends.len())]
#[requires(first == if ends.len() == 5 { 1 } else { 0 })]
#[requires(selected.len() == ends.len() + 1 - first)]
#[requires(order::insertion_ordered(ends, incoming, position))]
#[requires(forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ ==
    if first + j == position { ends.len() }
    else if first + j > position { first + j - 1 } else { first + j })]
#[ensures(result)]
#[ensures(result ==> selected.len() > 0 && selected.len() <= 5)]
#[ensures(result ==> (forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ <= ends.len()))]
#[ensures(result ==> (forall<i: Int, j: Int> 0 <= i && i < j && j < selected.len() ==>
    completion_offset(ends, incoming, selected[i]@) < completion_offset(ends, incoming, selected[j]@)))]
#[ensures(result ==> (forall<i: Int> 0 <= i && i < ends.len()
    && !(exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == i) ==>
        five_completions_above(ends, incoming, selected, ends[i]@)))]
#[ensures(result ==> (!(exists<j: Int> 0 <= j && j < selected.len()
    && completion_offset(ends, incoming, selected[j]@) == incoming@) ==>
        five_completions_above(ends, incoming, selected, incoming@)))]
pub(super) fn lemma_inserted_window(
    ends: Seq<i64>,
    incoming: i64,
    position: Int,
    first: Int,
    selected: Seq<usize>,
) -> bool {
    proof_assert!(position >= first ==> selected[position - first]@ == ends.len());
    proof_assert!(forall<i: Int> 0 <= i && i < ends.len()
        && !(exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == i) ==> {
        let covered = if i < position { first <= i } else { first <= i + 1 };
        if covered {
            let slot = witness::old_source_slot(selected, ends.len(), position, first, i);
            if selected[slot]@ == i {
                false
            } else {
                i == 0 && ends[i] == ends[0] && first == 1 && position > 0 && selected.len() == 5
            }
        } else {
            i == 0 && ends[i] == ends[0] && first == 1 && position > 0 && selected.len() == 5
        }
    });
    proof_assert!(forall<j: Int> 0 <= j && j < selected.len() ==>
        selected[j]@ == order::insertion_source(ends.len(), position, first + j));
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < selected.len() ==> {
        let checked = order::lemma_inserted_pair(ends, incoming, position, first + i, first + j);
        checked && completion_offset(ends, incoming, selected[i]@)
            < completion_offset(ends, incoming, selected[j]@)
    });
    proof_assert!(forall<j: Int> first == 1 && position > 0 && 0 <= j && j < selected.len() ==> {
        let checked = order::lemma_inserted_pair(ends, incoming, position, 0, first + j);
        checked && ends[0]@ < completion_offset(ends, incoming, selected[j]@)
    });
    true
}
