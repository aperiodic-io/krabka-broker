use {super::quota_credit, creusot_std::prelude::*};

/// Credit elapsed nanoseconds at a micro-token rate, retaining the fractional
/// micro-token numerator. Repay debt before filling the bucket, and discard
/// the fraction when the bucket reaches its burst. The caller claims the
/// whole elapsed interval together with the returned state.
#[requires(fraction@ < 1_000_000_000)]
#[requires(available@ <= burst@ && (available@ == 0 || debt@ == 0))]
#[ensures(result.0@ <= burst@ && (result.0@ == 0 || result.1@ == 0))]
#[ensures(result.2@ < 1_000_000_000)]
#[ensures({
    let credit = (elapsed@ * rate@ + fraction@) / 1_000_000_000;
    result.1@ == if credit <= debt@ { debt@ - credit } else { 0 }
})]
#[ensures({
    let credit = (elapsed@ * rate@ + fraction@) / 1_000_000_000;
    let paid = if credit <= debt@ { credit } else { debt@ };
    result.0@ == crate::throttle::capped(available@, credit - paid, burst@)
})]
#[ensures(result.2@ == if result.0@ == burst@ && result.1@ == 0 { 0 }
    else { (elapsed@ * rate@ + fraction@) % 1_000_000_000 })]
#[ensures({
    let balance = (available@ - debt@) * 1_000_000_000 + fraction@ + elapsed@ * rate@;
    (result.0@ - result.1@) * 1_000_000_000 + result.2@
        == if balance <= burst@ * 1_000_000_000 { balance } else { burst@ * 1_000_000_000 }
})]
#[must_use]
pub fn quota_refill(
    available: u64,
    debt: u64,
    fraction: u64,
    elapsed: u64,
    rate: u64,
    burst: u64,
) -> (u64, u64, u64) {
    let numerator = u128::from(elapsed) * u128::from(rate) + u128::from(fraction);
    let credit = numerator / 1_000_000_000;
    // The explicit u64 bound keeps the narrowing visible to Clippy.
    let paid = credit.min(u128::from(debt)).min(18_446_744_073_709_551_615) as u64;
    let excess = (credit - u128::from(paid))
        .min(u128::from(burst))
        .min(18_446_744_073_709_551_615) as u64;
    let repaid = quota_credit(available, debt, paid, burst);
    let filled = quota_credit(repaid.0, repaid.1, excess, burst);
    let fraction = if filled.0 == burst && filled.1 == 0 {
        0
    } else {
        (numerator % 1_000_000_000) as u64
    };
    (filled.0, filled.1, fraction)
}
