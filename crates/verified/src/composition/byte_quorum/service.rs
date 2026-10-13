use {crate::consensus::count_ge_prefix, creusot_std::prelude::*};

// cargo-mutants: #[cfg(creusot)] induction over actual vote count, absent at runtime.
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= count && count <= reports.len())]
#[requires(forall<i: Int> 0 <= i && i < count ==> reports[i]@ >= threshold)]
#[ensures(count_ge_prefix(log_end, reports, threshold, count + 1, false) == count)]
#[variant(count)]
pub(super) fn lemma_complete_votes_count(
    log_end: Int,
    reports: Seq<i64>,
    threshold: Int,
    count: Int,
) {
    if count > 0 {
        lemma_complete_votes_count(log_end, reports, threshold, count - 1);
    }
}
