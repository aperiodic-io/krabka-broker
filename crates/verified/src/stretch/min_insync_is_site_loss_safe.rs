use creusot_std::prelude::*;

/// The replicas that round-robin placement puts on `site`: the placement
/// visits sites `0, 1, ..., sites - 1, 0, 1, ...` in turn, so replica `r`
/// lands on site `r % sites`, and this counts the `r < rf` that land on
/// `site`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(rf)]
pub fn round_robin_load(rf: Int, sites: Int, site: Int) -> Int {
    pearlite! {
        if rf <= 0 {
            0
        } else {
            round_robin_load(rf - 1, sites, site)
                + if (rf - 1) % sites == site { 1 } else { 0 }
        }
    }
}

/// Integer division by a positive divisor is monotone on nonnegative
/// numerators.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= low && low <= high && divisor >= 1)]
#[ensures(low / divisor <= high / divisor)]
pub fn lemma_div_monotone(low: Int, high: Int, divisor: Int) {
    // Both numerators split into quotient and remainder.
    proof_assert!(low == divisor * (low / divisor) + low % divisor && 0 <= low % divisor);
    proof_assert!(high == divisor * (high / divisor) + high % divisor && high % divisor < divisor);
    // A larger quotient for `low` would put `low` a whole divisor past `high`.
    proof_assert!(low / divisor > high / divisor
        ==> (low / divisor - high / divisor - 1) * divisor >= 0);
}

/// A numerator written as `divisor * quotient + remainder`, with the
/// remainder in `[0, divisor)`, divides to exactly that quotient.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(divisor >= 1 && quotient >= 0 && 0 <= remainder && remainder < divisor)]
#[ensures((divisor * quotient + remainder) / divisor == quotient)]
pub fn lemma_div_exact(divisor: Int, quotient: Int, remainder: Int) {
    // Why3's `Div_mult` peels the whole multiples off, and `Div_inf` makes the
    // remainder's own quotient 0.
    proof_assert!((divisor * quotient + remainder) / divisor == quotient + remainder / divisor);
    proof_assert!(remainder / divisor == 0);
}

#[krabka_macros::round_robin_site_lemma(1)]
/// One more replica raises the closed form `(rf - site + sites - 1) / sites`
/// by one exactly when that replica lands on `site`.
#[ensures((rf - site + sites - 1) / sites
    == (rf - 1 - site + sites - 1) / sites + if (rf - 1) % sites == site { 1 } else { 0 })]
pub fn lemma_round_robin_step(rf: Int, sites: Int, site: Int) {
    let q = (rf - 1) / sites;
    let r = (rf - 1) % sites;
    // Division of the nonnegative `rf - 1`: quotient `q`, remainder `r`.
    proof_assert!(rf - 1 == sites * q + r && 0 <= r && r < sites && q >= 0);
    // Each case writes both numerators as `sites * quotient + remainder` with
    // the remainder in `[0, sites)`, which fixes the quotient.
    if r == site {
        proof_assert!(rf - site + sites - 1 == sites * (q + 1) + 0);
        proof_assert!(rf - 1 - site + sites - 1 == sites * q + (sites - 1));
        lemma_div_exact(sites, q + 1, 0);
        lemma_div_exact(sites, q, sites - 1);
    } else if r > site {
        proof_assert!(rf - site + sites - 1 == sites * (q + 1) + (r - site));
        proof_assert!(rf - 1 - site + sites - 1 == sites * (q + 1) + (r - site - 1));
        lemma_div_exact(sites, q + 1, r - site);
        lemma_div_exact(sites, q + 1, r - site - 1);
    } else {
        proof_assert!(rf - site + sites - 1 == sites * q + (sites + r - site));
        proof_assert!(rf - 1 - site + sites - 1 == sites * q + (sites + r - site - 1));
        lemma_div_exact(sites, q, sites + r - site);
        lemma_div_exact(sites, q, sites + r - site - 1);
    }
}

#[krabka_macros::round_robin_site_lemma(0)]
/// The closed form of `round_robin_load`: site `site` holds
/// `(rf - site + sites - 1) / sites` replicas.
#[ensures(round_robin_load(rf, sites, site) == (rf - site + sites - 1) / sites)]
#[variant(rf)]
pub fn lemma_round_robin_load(rf: Int, sites: Int, site: Int) {
    if rf > 0 {
        lemma_round_robin_load(rf - 1, sites, site);
        lemma_round_robin_step(rf, sites, site);
    }
}

#[krabka_macros::round_robin_site_lemma(0)]
/// No site holds more than site 0 under round-robin placement, so site 0 is
/// a fullest site.
#[ensures(round_robin_load(rf, sites, site) <= round_robin_load(rf, sites, 0))]
pub fn lemma_site_load_at_most_first(rf: Int, sites: Int, site: Int) {
    // Both loads in closed form; site 0 has the largest numerator.
    lemma_round_robin_load(rf, sites, site);
    lemma_round_robin_load(rf, sites, 0);
    lemma_div_monotone(rf - site + sites - 1, rf + sites - 1, sites);
}

/// The replica count that survives the loss of any one site.
///
/// Under the round-robin placement, `round_robin_load`, site 0 is a fullest
/// site. The contract proves that the result is what the loss of site 0
/// leaves, and that the loss of any other site leaves at least as many, so it
/// is the fewest replicas any single site loss leaves. The body computes it as
/// `rf - ceil(rf / sites)`; the proof derives that ceiling from the model
/// rather than taking it as the definition.
///
/// An operator reads this number to pick `min.insync.replicas`. Three replicas
/// on three sites leave 2, because each site holds one replica. Three replicas
/// on two sites leave 1, because two of the three replicas share a site. One
/// site leaves 0, because the loss of that site is the loss of the partition.
///
/// The proof assumes both `rf` and `sites` are in `1..=1024`. Normal Rust
/// builds do not check that proof precondition.
///
/// # Panics
///
/// Panics when `sites` is zero. Arithmetic outside the proof bounds can panic
/// when overflow checks are enabled.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result@ == rf@ - round_robin_load(rf@, sites@, 0))]
#[ensures(forall<site: Int> 0 <= site && site < sites@
    ==> rf@ - round_robin_load(rf@, sites@, site) >= result@)]
#[ensures(result@ >= 0)]
#[ensures(result@ < rf@)]
#[ensures(sites@ == 1 ==> result@ == 0)]
#[must_use]
pub fn site_loss_survivors(rf: i64, sites: i64) -> i64 {
    // `(rf - 1) * (sites - 1) >= 0` gives `rf + sites - 1 <= rf * sites`, which
    // is what bounds the ceiling by `rf` and keeps the result at 0 or above.
    proof_assert!((rf@ - 1) * (sites@ - 1) >= 0);
    proof_assert!(rf@ + sites@ - 1 <= sites@ * rf@ + 0);
    proof_assert!({
        lemma_div_monotone(rf@ + sites@ - 1, sites@ * rf@ + 0, sites@);
        lemma_div_exact(sites@, rf@, 0);
        (rf@ + sites@ - 1) / sites@ <= rf@
    });
    // The placement model: site 0 holds the ceiling, and no site holds more.
    proof_assert!({
        lemma_round_robin_load(rf@, sites@, 0);
        round_robin_load(rf@, sites@, 0) == (rf@ + sites@ - 1) / sites@
    });
    proof_assert!(forall<site: Int> 0 <= site && site < sites@ ==> {
        lemma_site_load_at_most_first(rf@, sites@, site);
        round_robin_load(rf@, sites@, site) <= round_robin_load(rf@, sites@, 0)
    });
    rf - (rf + sites - 1) / sites
}

/// `true` if `min_insync` keeps `acks=all` writes durable and available through
/// the loss of any one site.
///
/// Two bounds define the safe range, and the contract states both against
/// every site of the round-robin placement, `round_robin_load`.
///
/// The **lower** bound is that no single site holds `min_insync` replicas.
/// `min.insync.replicas` is a count and not a placement: the leader accepts an
/// `acks=all` write as soon as that many in-sync replicas hold it, wherever
/// they sit. If one site can hold `min_insync` of them, then the in-sync set
/// can shrink to that one site, and the leader then acknowledges a write that
/// only that site holds. The loss of that site loses an acknowledged write,
/// and the surviving sites still hold the voter majority, so they elect a
/// leader that never saw it. The shrink needs no second site loss to get
/// there: one site down and one lagging replica is enough, and a witness on a
/// cheap link is the replica most likely to lag.
///
/// The **upper** bound is that the loss of any one site leaves at least
/// `min_insync` replicas, so the leader keeps accepting `acks=all` writes. The
/// fewest that a site loss leaves is [`site_loss_survivors`].
///
/// The lower bound implies `min_insync >= 2`, because one site always holds at
/// least one replica. It is therefore the whole of the durability condition,
/// and 2 is not stated separately.
///
/// For three replicas on three sites each site holds one, so the bounds are
/// `min_insync > 1` and `min_insync <= 2`: they meet, and 2 is the only safe
/// value. Four replicas on three sites have no safe value at all, because one
/// site holds two of them: the bounds become `min_insync > 2` and
/// `min_insync <= 2`. Three replicas on two sites have none either, for the
/// same reason. A witness site closes that gap. It holds a replica of its own,
/// so three replicas spread one per site over three sites again.
#[requires(rf@ >= 1 && rf@ <= 1024)]
#[requires(sites@ >= 1 && sites@ <= 1024)]
#[ensures(result == (
    (forall<site: Int> 0 <= site && site < sites@
        ==> round_robin_load(rf@, sites@, site) < min_insync@)
    && (forall<site: Int> 0 <= site && site < sites@
        ==> rf@ - round_robin_load(rf@, sites@, site) >= min_insync@)
))]
#[must_use]
pub fn min_insync_is_site_loss_safe(rf: i64, sites: i64, min_insync: i64) -> bool {
    let survivors = site_loss_survivors(rf, sites);
    // What the fullest site holds. `site_loss_survivors` is `rf` less that
    // count, so this recovers it without restating the ceiling.
    let largest_site = rf - survivors;
    min_insync > largest_site && min_insync <= survivors
}
