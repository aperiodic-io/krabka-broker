use {super::completion_offset, creusot_std::prelude::*};

open_logic! {
pub fn insertion_source(count: Int, position: Int, ordinal: Int) -> Int {
    pearlite! { if ordinal == position { count }
    else if ordinal > position { ordinal - 1 } else { ordinal } }
}
}

open_logic! {
pub(super) fn insertion_ordered(ends: Seq<i64>, incoming: i64, position: Int) -> bool {
    pearlite! {
        (crate::sequence::strictly_increasing(ends))
        && (forall<i: Int> 0 <= i && i < position ==> ends[i]@ < incoming@)
        && (forall<i: Int> position <= i && i < ends.len() ==> incoming@ < ends[i]@)
    }
}
}

/// Any two positions in a sorted insertion retain their strict offset order.
/// This pointwise fact avoids mixing index arithmetic with quantified Vec
/// coverage and the producer selector's epoch branches.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= position && position <= ends.len())]
#[requires(0 <= lower && lower < upper && upper <= ends.len())]
#[requires(insertion_ordered(ends, incoming, position))]
#[ensures(result)]
#[ensures(result ==> completion_offset(ends, incoming, insertion_source(ends.len(), position, lower))
    < completion_offset(ends, incoming, insertion_source(ends.len(), position, upper)))]
pub(super) fn lemma_inserted_pair(
    ends: Seq<i64>,
    incoming: i64,
    position: Int,
    lower: Int,
    upper: Int,
) -> bool {
    true
}
