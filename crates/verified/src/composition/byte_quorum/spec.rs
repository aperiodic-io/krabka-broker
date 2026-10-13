use {
    super::{WalCopyBatch, WalCopyObservation},
    crate::composition::wal_copy::wal_copy_byte_count,
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] logical reference shape, absent from runtime.
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(super) fn reference_valid(source: Seq<WalCopyBatch>, floor: Int, end: Int) -> bool {
    pearlite! {
        0 <= floor && floor <= end
        && (if source.len() == 0 { floor == end } else {
            source[0].0@ <= floor && floor < source[0].0@ + source[0].1@ + 1
            && source[source.len() - 1].0@ + source[source.len() - 1].1@ + 1 == end
        })
        && (forall<i: Int, j: Int> 0 <= i && i < j && j < source.len() ==>
            source[i].0@ + source[i].1@ < source[j].0@)
        && forall<i: Int> 0 <= i && i < source.len() ==>
            source[i].0@ >= 0 && source[i].1@ >= 0
            && source[i].0@ + source[i].1@ + 1 <= end
            && source[i].2@.len() > 0
            && (i > 0 ==> source[i].0@ == source[i - 1].0@ + source[i - 1].1@ + 1)
    }
}

// cargo-mutants: #[cfg(creusot)] independent copy admission policy, absent from runtime.
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(super) fn copy_admitted(
    voters: Seq<u64>,
    claimed: u64,
    local: u64,
    epoch: i32,
    source: Seq<WalCopyBatch>,
    observation: WalCopyObservation,
) -> bool {
    pearlite! {
        observation.2 && observation.0 == Some(claimed)
        && voters.len() > 0 && voters[0] == local
        && (exists<i: Int> 0 <= i && i < voters.len() && voters[i] == claimed)
        && (observation.1@ < 0 || observation.1 == epoch)
        && observation.3@.len() <= source.len()
        && (forall<i: Int> 0 <= i && i < observation.3@.len() ==> observation.3@[i].0 == source[i].0 && observation.3@[i].1 == source[i].1 && observation.3@[i].2@ == source[i].2@)
        && (if observation.3@.len() <= source.len() {
            wal_copy_byte_count(source.subsequence(0, observation.3@.len()), observation.3@.len()) <= u64::MAX@
        } else { false })
    }
}
