use creusot_std::prelude::*;

/// The sum of the first `limit` elements of `s`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(limit)]
pub fn sum_prefix(s: Seq<i64>, limit: Int) -> Int {
    pearlite! {
        if limit <= 0 {
            0
        } else {
            sum_prefix(s, limit - 1) + s[limit - 1]@
        }
    }
}

/// The sum of every element of `s`. This is the total voter count.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn sum_all(s: Seq<i64>) -> Int {
    pearlite! { sum_prefix(s, s.len()) }
}

/// One step of the `sum_prefix` recursion, as a fact about `limit - 1`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(1 <= limit && limit <= s.len())]
#[ensures(sum_prefix(s, limit) == sum_prefix(s, limit - 1) + s[limit - 1]@)]
pub fn lemma_sum_prefix_step(s: Seq<i64>, limit: Int) {}

/// A prefix sum is not negative when no element of that prefix is negative.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= s.len())]
#[requires(forall<k: Int> 0 <= k && k < limit ==> s[k]@ >= 0)]
#[ensures(sum_prefix(s, limit) >= 0)]
#[variant(limit)]
pub fn lemma_sum_prefix_nonnegative(s: Seq<i64>, limit: Int) {
    if limit > 0 {
        lemma_sum_prefix_nonnegative(s, limit - 1);
        lemma_sum_prefix_step(s, limit);
    }
}

/// Two sites never survive a site loss, whatever the split of the voters.
///
/// This is the reason a stretch cluster needs a third site, and it is the
/// claim [`quorum_survives_any_single_site_loss`]'s documentation makes about
/// two sites. One of the two sites holds at least half of the voters. The loss
/// of that site leaves half of the voters or less, and half is not a strict
/// majority. No other proof calls this lemma; it exists so that the two-site
/// claim is proved rather than only asserted.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[requires(voters_per_site@.len() == 2)]
#[requires(forall<k: Int> 0 <= k && k < 2 ==> voters_per_site@[k]@ >= 0)]
#[ensures(exists<k: Int> 0 <= k && k < voters_per_site@.len()
    && 2 * (sum_all(voters_per_site@) - voters_per_site@[k]@) <= sum_all(voters_per_site@))]
pub fn lemma_two_sites_never_survive(voters_per_site: &[i64]) {
    #[cfg(creusot)]
    proof_assert!({
        lemma_sum_prefix_nonnegative(voters_per_site@, voters_per_site@.len());
        sum_all(voters_per_site@) >= 0
    });
    proof_assert!(sum_prefix(voters_per_site@, 1) == voters_per_site@[0]@);
    proof_assert!(sum_all(voters_per_site@) == voters_per_site@[0]@ + voters_per_site@[1]@);
    proof_assert!(voters_per_site@[0]@ >= voters_per_site@[1]@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[0]@) <= sum_all(voters_per_site@));
    proof_assert!(voters_per_site@[1]@ >= voters_per_site@[0]@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[1]@) <= sum_all(voters_per_site@));
}

/// `true` if the surviving `KRaft` voters still form a strict majority after
/// the loss of any one site.
///
/// `voters_per_site[k]` is the count of `KRaft` voters in site `k`. The check
/// asks for every site `k` whether `2 * (total - voters_per_site[k]) > total`
/// holds, where `total` is the sum of the slice. A strict majority is what a
/// `KRaft` quorum needs to elect a leader and to commit a metadata record, so a
/// cluster that fails this check stops its metadata writes when it loses a
/// site.
///
/// Two sites never pass this check. Any split of the voters over two sites
/// leaves one site with half of them or more, and the loss of that site leaves
/// half or less. This is why a third site must hold at least one voter. A
/// data-bearing witness is the smallest form of that third site: it holds one
/// voter, so `[1, 1, 1]` passes the check, and it holds a replica as well, so
/// it also counts toward `min.insync.replicas`.
///
/// An empty slice returns `true`. There is no site to lose, so the condition
/// holds for every site of the empty set.
///
/// The preconditions bound the slice at 1024 sites and each site at 1024
/// voters. The total is then 1048576 at most, and `2 * total` cannot overflow.
#[requires(voters_per_site@.len() <= 1024)]
#[requires(forall<k: Int> 0 <= k && k < voters_per_site@.len()
    ==> 0 <= voters_per_site@[k]@ && voters_per_site@[k]@ <= 1024)]
#[ensures(result == (forall<k: Int> 0 <= k && k < voters_per_site@.len()
    ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[k]@) > sum_all(voters_per_site@)))]
#[must_use]
pub fn quorum_survives_any_single_site_loss(voters_per_site: &[i64]) -> bool {
    let n = voters_per_site.len();

    let mut total: i64 = 0;
    let mut i = 0;
    #[invariant(i@ <= n@)]
    #[invariant(total@ == sum_prefix(voters_per_site@, i@))]
    #[invariant(0 <= total@ && total@ <= i@ * 1024)]
    #[variant(n@ - i@)]
    while i < n {
        proof_assert!(sum_prefix(voters_per_site@, i@ + 1)
            == sum_prefix(voters_per_site@, i@) + voters_per_site@[i@]@);
        total += voters_per_site[i];
        i += 1;
    }

    let mut k = 0;
    let mut survives = true;
    #[invariant(k@ <= n@)]
    #[invariant(0 <= total@ && total@ <= 1024 * 1024)]
    #[invariant(total@ == sum_all(voters_per_site@))]
    #[invariant(survives == (forall<j: Int> 0 <= j && j < k@
        ==> 2 * (sum_all(voters_per_site@) - voters_per_site@[j]@) > sum_all(voters_per_site@)))]
    #[variant(n@ - k@)]
    while k < n {
        if 2 * (total - voters_per_site[k]) <= total {
            survives = false;
        }
        k += 1;
    }
    survives
}
