//! Pure token-bucket consume arithmetic.

use creusot_std::prelude::*;
#[cfg(not(creusot))]
use derive_more::{Display, From, Into};

/// Tokens currently sitting in the bucket, available to grant.
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct AvailableTokens(pub u64);

/// Tokens accrued since the last refill, to be added to `available` this call.
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct RefillTokens(pub u64);

/// The burst cap: the maximum the bucket may hold after a refill.
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct BurstCapacity(pub u64);

/// Tokens the caller is asking to consume.
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct RequestedTokens(pub u64);

/// Tokens actually granted by a consume call (`<= requested`).
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct GrantedTokens(pub u64);

/// The bucket's new `available` count after a consume call commits.
#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct NewAvailable(pub u64);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// `min(available + refill, burst)` in unbounded integers.
///
/// This equals the executable `available.saturating_add(refill).min(burst)`
/// for every `u64` input. Without overflow the saturating sum is the exact
/// sum. With overflow the exact sum exceeds `u64::MAX`, which is at least
/// `burst`, so both sides are `burst`. [`plan_consume`] proves this equality
/// for its own inputs.
pub fn capped(available: Int, refill: Int, burst: Int) -> Int {
    if available + refill <= burst {
        available + refill
    } else {
        burst
    }
}

/// Caps a refill, grants at most the request, and returns the new balance.
#[ensures(result.0.0@ <= requested.0@)]
#[ensures(result.1.0@ <= burst.0@)]
#[ensures(result.0.0@ + result.1.0@ == capped(available.0@, refill.0@, burst.0@))]
#[ensures(result.0.0@ == if requested.0@ <= capped(available.0@, refill.0@, burst.0@) {
    requested.0@
} else {
    capped(available.0@, refill.0@, burst.0@)
})]
#[must_use]
pub fn plan_consume(
    available: AvailableTokens,
    refill: RefillTokens,
    burst: BurstCapacity,
    requested: RequestedTokens,
) -> (GrantedTokens, NewAvailable) {
    let total = available.0.saturating_add(refill.0).min(burst.0);
    proof_assert!(total@ == capped(available.0@, refill.0@, burst.0@));
    let grant = requested.0.min(total);
    (GrantedTokens(grant), NewAvailable(total - grant))
}

#[cfg(test)]
mod tests {
    use {super::*, proptest::prelude::*};

    #[test]
    fn plan_consume_grants_and_caps() {
        for ((available, refill, burst, requested), (grant, new_available)) in [
            ((100, 0, 1000, 50), (50, 50)),
            ((100, 0, 1000, 200), (100, 0)),
            ((900, 500, 1000, 200), (200, 800)),
            ((0, 0, 1000, 100), (0, 0)),
            ((u64::MAX, u64::MAX, 1000, 1000), (1000, 0)),
        ] {
            assert2::assert!(
                plan_consume(
                    AvailableTokens(available),
                    RefillTokens(refill),
                    BurstCapacity(burst),
                    RequestedTokens(requested)
                ) == (GrantedTokens(grant), NewAvailable(new_available))
            );
        }
    }

    proptest! {
        #[test]
        fn plan_consume_invariants(
            available in 0u64..=u64::MAX,
            refill in 0u64..=u64::MAX,
            burst in 0u64..1_000_000,
            requested in 0u64..=u64::MAX,
        ) {
            let (grant, new) = plan_consume(
                AvailableTokens(available),
                RefillTokens(refill),
                BurstCapacity(burst),
                RequestedTokens(requested),
            );
            let capped = available.saturating_add(refill).min(burst);
            prop_assert!(grant.0 <= requested);
            prop_assert!(grant.0 <= capped);
            prop_assert_eq!(new.0, capped - grant.0);
            prop_assert!(new.0 <= burst, "burst cap");
        }
    }
}
