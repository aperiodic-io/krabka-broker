//! Shared quantified laws for sequence order and uniqueness.
use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// No pair of distinct indexes contains equal values.
pub fn distinct<T>(values: Seq<T>) -> bool {
    pearlite! { pairwise(values, |(left, right): (T, T)| left != right) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Integer views increase strictly with their indexes.
pub fn strictly_increasing<T: View<ViewTy = Int>>(values: Seq<T>) -> bool {
    pearlite! { pairwise(values, |(left, right): (T, T)| left@ < right@) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Apply a relation to every pair of values in source-index order.
pub fn pairwise<T>(values: Seq<T>, relation: creusot_std::logic::Mapping<(T, T), bool>) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < j && j < values.len()
    ==> relation.get((values[i], values[j])) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The selected source-index sequence includes this original position.
pub(crate) fn contains_source_index(selected: Seq<usize>, index: Int) -> bool {
    pearlite! { exists<j: Int> 0 <= j && j < selected.len() && selected[j]@ == index }
}
