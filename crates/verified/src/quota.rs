//! Kafka quota-entity precedence and quota bucket debt accounting.

use {
    crate::throttle::{
        AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
    },
    creusot_std::prelude::*,
};

mod refill;
pub use refill::quota_refill;

mod debt_cap;
pub use debt_cap::quota_debt_cap;

mod request;
pub use request::quota_whole_request;

/// Credit micro-tokens to a quota bucket, repaying debt before filling its
/// balance. Both refill and refund use this transition; rate and clock
/// conversion and atomic state publication remain with the caller.
#[ensures(result.0@ <= burst@)]
#[ensures(result.1@ == debt@ - if debt@ <= credit@ { debt@ } else { credit@ })]
#[ensures(result.0@ == crate::throttle::capped(available@,
    credit@ - if debt@ <= credit@ { debt@ } else { credit@ }, burst@))]
#[ensures((available@ == 0 || debt@ == 0) ==> (result.0@ == 0 || result.1@ == 0))]
#[must_use]
pub fn quota_credit(available: u64, debt: u64, credit: u64, burst: u64) -> (u64, u64) {
    let repaid = credit.min(debt);
    let (_, new_available) = plan_consume(
        AvailableTokens(available),
        RefillTokens(credit - repaid),
        BurstCapacity(burst),
        RequestedTokens(0),
    );
    (new_available.0, debt - repaid)
}

/// Charge the effective micro-token request in full, retaining its unpaid
/// portion as debt up to `debt_cap`. A smaller cap intentionally forgets debt;
/// `u64::MAX` is the unbounded mode's representability limit.
#[ensures(result.0@ == if requested@ <= crate::throttle::capped(available@, 0, burst@) {
    crate::throttle::capped(available@, 0, burst@) - requested@
} else { 0 })]
#[ensures(result.1@ == crate::throttle::capped(debt@,
    requested@ - (crate::throttle::capped(available@, 0, burst@) - result.0@), debt_cap@))]
#[ensures(result.0@ <= burst@ && result.1@ <= debt_cap@)]
#[ensures((available@ == 0 || debt@ == 0) ==> (result.0@ == 0 || result.1@ == 0))]
#[must_use]
pub fn quota_charge(
    available: u64,
    debt: u64,
    requested: u64,
    burst: u64,
    debt_cap: u64,
) -> (u64, u64) {
    let (grant, new_available) = plan_consume(
        AvailableTokens(available),
        RefillTokens(0),
        BurstCapacity(burst),
        RequestedTokens(requested),
    );
    let new_debt = debt.saturating_add(requested - grant.0).min(debt_cap);
    (new_available.0, new_debt)
}

/// Selected user/client quota candidate, in Kafka's precedence order.
///
/// The order is `DefaultQuotaCallback`'s in Kafka's `ClientQuotaManager`:
/// every `user=U` level ranks above every `user=<default>` level, and both
/// rank above the `client-id`-only levels.
#[krabka_macros::model_type]
pub enum UserClientQuotaPrecedence {
    /// 1. `user=U, client-id=C`
    ExactPair,
    /// 2. `user=U, client-id=<default>`
    ExactUserDefaultClient,
    /// 3. `user=U`
    ExactUser,
    /// 4. `user=<default>, client-id=C`
    DefaultUserExactClient,
    /// 5. `user=<default>, client-id=<default>`
    DefaultPair,
    /// 6. `user=<default>`
    DefaultUser,
    /// 7. `client-id=C`
    ExactClient,
    /// 8. `client-id=<default>`
    DefaultClient,
    None,
}

/// Whether one canonical quota candidate exists in the metadata image.
#[krabka_macros::model_type]
pub enum QuotaCandidatePresence {
    Absent,
    Present,
}

/// Presence of each canonical user/client quota candidate, in Kafka's
/// precedence order.
#[krabka_macros::model_type]
pub struct UserClientQuotaFacts {
    pub exact_pair: QuotaCandidatePresence,
    pub exact_user_default_client: QuotaCandidatePresence,
    pub exact_user: QuotaCandidatePresence,
    pub default_user_exact_client: QuotaCandidatePresence,
    pub default_pair: QuotaCandidatePresence,
    pub default_user: QuotaCandidatePresence,
    pub exact_client: QuotaCandidatePresence,
    pub default_client: QuotaCandidatePresence,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn exact_user_quota_absent(facts: UserClientQuotaFacts) -> bool {
    pearlite! { facts.exact_pair == QuotaCandidatePresence::Absent
    && facts.exact_user_default_client == QuotaCandidatePresence::Absent
    && facts.exact_user == QuotaCandidatePresence::Absent }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn all_user_quota_absent(facts: UserClientQuotaFacts) -> bool {
    pearlite! { exact_user_quota_absent(facts)
    && facts.default_user_exact_client == QuotaCandidatePresence::Absent
    && facts.default_pair == QuotaCandidatePresence::Absent
    && facts.default_user == QuotaCandidatePresence::Absent }
}

/// Select Kafka's first present user/client quota candidate.
#[ensures((result == UserClientQuotaPrecedence::ExactPair)
    == (facts.exact_pair == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactUserDefaultClient)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_user_default_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactUser)
    == (facts.exact_pair == QuotaCandidatePresence::Absent
        && facts.exact_user_default_client == QuotaCandidatePresence::Absent
        && facts.exact_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultUserExactClient)
    == (exact_user_quota_absent(facts)
        && facts.default_user_exact_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultPair)
    == (exact_user_quota_absent(facts)
        && facts.default_user_exact_client == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultUser)
    == (exact_user_quota_absent(facts)
        && facts.default_user_exact_client == QuotaCandidatePresence::Absent
        && facts.default_pair == QuotaCandidatePresence::Absent
        && facts.default_user == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::ExactClient)
    == (all_user_quota_absent(facts)
        && facts.exact_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::DefaultClient)
    == (all_user_quota_absent(facts)
        && facts.exact_client == QuotaCandidatePresence::Absent
        && facts.default_client == QuotaCandidatePresence::Present))]
#[ensures((result == UserClientQuotaPrecedence::None) == !(
    facts.exact_pair == QuotaCandidatePresence::Present
        || facts.exact_user_default_client == QuotaCandidatePresence::Present
        || facts.exact_user == QuotaCandidatePresence::Present
        || facts.default_user_exact_client == QuotaCandidatePresence::Present
        || facts.default_pair == QuotaCandidatePresence::Present
        || facts.default_user == QuotaCandidatePresence::Present
        || facts.exact_client == QuotaCandidatePresence::Present
        || facts.default_client == QuotaCandidatePresence::Present))]
#[must_use]
pub fn user_client_quota_precedence(facts: UserClientQuotaFacts) -> UserClientQuotaPrecedence {
    if matches!(facts.exact_pair, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactPair
    } else if matches!(
        facts.exact_user_default_client,
        QuotaCandidatePresence::Present
    ) {
        UserClientQuotaPrecedence::ExactUserDefaultClient
    } else if matches!(facts.exact_user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactUser
    } else if matches!(
        facts.default_user_exact_client,
        QuotaCandidatePresence::Present
    ) {
        UserClientQuotaPrecedence::DefaultUserExactClient
    } else if matches!(facts.default_pair, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultPair
    } else if matches!(facts.default_user, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultUser
    } else if matches!(facts.exact_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::ExactClient
    } else if matches!(facts.default_client, QuotaCandidatePresence::Present) {
        UserClientQuotaPrecedence::DefaultClient
    } else {
        UserClientQuotaPrecedence::None
    }
}

/// Selected IP quota candidate.
#[krabka_macros::model_type]
pub enum IpQuotaPrecedence {
    Exact,
    Default,
    None,
}

/// Select an exact IP quota before the IP default.
#[ensures((result == IpQuotaPrecedence::Exact) == exact)]
#[ensures((result == IpQuotaPrecedence::Default) == (!exact && default))]
#[ensures((result == IpQuotaPrecedence::None) == (!exact && !default))]
#[must_use]
pub fn ip_quota_precedence(exact: bool, default: bool) -> IpQuotaPrecedence {
    if exact {
        IpQuotaPrecedence::Exact
    } else if default {
        IpQuotaPrecedence::Default
    } else {
        IpQuotaPrecedence::None
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Spendable credit is bounded by the burst and is absent while debt remains.
pub(crate) fn balance_within_burst(available: Int, debt: Int, burst: Int) -> bool {
    pearlite! { available <= burst && (available == 0 || debt == 0) }
}

#[cfg(test)]
mod tests {
    use super::{
        IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts, UserClientQuotaPrecedence,
        ip_quota_precedence, user_client_quota_precedence,
    };

    #[test]
    fn selectors_choose_the_first_present_candidate() {
        for mask in 0_u16..256 {
            let present = |index| mask & (1_u16 << index) != 0_u16;
            let candidate = |index| {
                if present(index) {
                    QuotaCandidatePresence::Present
                } else {
                    QuotaCandidatePresence::Absent
                }
            };
            let got = user_client_quota_precedence(UserClientQuotaFacts {
                exact_pair: candidate(0),
                exact_user_default_client: candidate(1),
                exact_user: candidate(2),
                default_user_exact_client: candidate(3),
                default_pair: candidate(4),
                default_user: candidate(5),
                exact_client: candidate(6),
                default_client: candidate(7),
            });
            let expected = match (0..8).find(|index| present(*index)) {
                Some(0) => UserClientQuotaPrecedence::ExactPair,
                Some(1) => UserClientQuotaPrecedence::ExactUserDefaultClient,
                Some(2) => UserClientQuotaPrecedence::ExactUser,
                Some(3) => UserClientQuotaPrecedence::DefaultUserExactClient,
                Some(4) => UserClientQuotaPrecedence::DefaultPair,
                Some(5) => UserClientQuotaPrecedence::DefaultUser,
                Some(6) => UserClientQuotaPrecedence::ExactClient,
                Some(7) => UserClientQuotaPrecedence::DefaultClient,
                Some(_) | None => UserClientQuotaPrecedence::None,
            };
            assert2::check!(got == expected, "mask {mask:#010b}");
        }
        assert2::check!(ip_quota_precedence(true, true) == IpQuotaPrecedence::Exact);
        assert2::check!(ip_quota_precedence(false, true) == IpQuotaPrecedence::Default);
        assert2::check!(ip_quota_precedence(false, false) == IpQuotaPrecedence::None);
    }
}
