use {
    super::{
        AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume, quota_charge,
        quota_credit,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] specification, absent from runtime tests.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn refilled_quota_balance(available: Int, debt: Int, refill: Int, burst: Int) -> Int {
    if available - debt + refill <= burst {
        available - debt + refill
    } else {
        burst
    }
}

/// A full charge/refund restores the refilled signed balance and therefore
/// the next consume budget, exactly when the charge's debt fits in storage.
/// Counts are effective micro-tokens; the host serializes the steps without
/// another refill, charge, reset, or refund between charge and refund.
#[requires(available@ <= burst@ && (available@ == 0 || debt@ == 0))]
#[ensures((result != None) ==
    (requested@ - refilled_quota_balance(available@, debt@, refill@, burst@) <= u64::MAX@))]
#[ensures(match result {
    None => true,
    Some((restored_available, restored_debt, grant)) =>
        restored_available@ <= burst@
        && (restored_available@ == 0 || restored_debt@ == 0)
        && restored_available@ - restored_debt@
            == refilled_quota_balance(available@, debt@, refill@, burst@)
        && grant@ == if probe@ <= restored_available@ { probe@ } else { restored_available@ },
})]
pub(super) fn quota_charge_refund_restores_consume_budget(
    available: u64,
    debt: u64,
    refill: u64,
    burst: u64,
    requested: u64,
    probe: u64,
) -> Option<(u64, u64, u64)> {
    let refilled = quota_credit(available, debt, refill, burst);
    refilled
        .1
        .checked_add(requested.saturating_sub(refilled.0))?;
    let charged = quota_charge(refilled.0, refilled.1, requested, burst, u64::MAX);
    let restored = quota_credit(charged.0, charged.1, requested, burst);
    let (grant, _) = plan_consume(
        AvailableTokens(restored.0),
        RefillTokens(0),
        BurstCapacity(burst),
        RequestedTokens(probe),
    );
    Some((restored.0, restored.1, grant.0))
}

/// A bounded charge cannot starve a coherent bucket after at least its cap
/// has been credited: debt is gone, and credit beyond the cap supplies a
/// guaranteed consume budget, up to the burst and the requested probe.
/// The host must derive this credit from actual elapsed time at the rate;
/// the floating-point wait/rate conversion and clock claim are not proved.
#[requires(initial.0@ <= burst@ && (initial.0@ == 0 || initial.1@ == 0))]
#[requires(repayment@ >= cap@)]
#[ensures(result.1@ == 0 && result.0@ <= burst@)]
#[ensures({
    let before = refilled_quota_balance(initial.0@, initial.1@, refill@, burst@);
    let charged = if before - requested@ >= -cap@ { before - requested@ } else { -cap@ };
    result.0@ == if charged + repayment@ <= burst@ { charged + repayment@ } else { burst@ }
})]
#[ensures(result.0@ >= if repayment@ - cap@ <= burst@ { repayment@ - cap@ } else { burst@ })]
#[ensures(result.2@ == if probe@ <= result.0@ { probe@ } else { result.0@ })]
#[ensures((probe@ <= burst@ && cap@ + probe@ <= repayment@) ==> result.2@ == probe@)]
pub(super) fn bounded_quota_debt_cannot_outlast_repayment(
    initial: (u64, u64), // available, debt
    refill: u64,
    burst: u64,
    requested: u64,
    cap: u64,
    repayment: u64,
    probe: u64,
) -> (u64, u64, u64) {
    let refilled = quota_credit(initial.0, initial.1, refill, burst);
    let charged = quota_charge(refilled.0, refilled.1, requested, burst, cap);
    let repaid = quota_credit(charged.0, charged.1, repayment, burst);
    let (grant, _) = plan_consume(
        AvailableTokens(repaid.0),
        RefillTokens(0),
        BurstCapacity(burst),
        RequestedTokens(probe),
    );
    (repaid.0, repaid.1, grant.0)
}
