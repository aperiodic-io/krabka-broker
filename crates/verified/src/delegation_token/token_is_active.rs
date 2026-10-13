use {
    super::{TokenCreateDecision, TokenDeadlines, TokenExpireDecision, TokenRenewDecision},
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn deadline_model(now_ms: i64, duration_ms: Int) -> Int {
    pearlite! {
        if now_ms@ + duration_ms > i64::MAX@ { i64::MAX@ } else { now_ms@ + duration_ms }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn bounded_period_model(requested_ms: i64, default_ms: i64) -> Int {
    pearlite! {
        if requested_ms@ > 0 && requested_ms@ < default_ms@ { requested_ms@ } else { default_ms@ }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn min_model(left: Int, right: Int) -> Int {
    pearlite! {
        if left < right { left } else { right }
    }
}

/// Kafka's `DelegationTokenControlManager.sum`: `now + duration`, saturated
/// at `i64::MAX` instead of wrapping.
#[requires(duration_ms@ >= 0)]
#[ensures(result@ == deadline_model(now_ms, duration_ms@))]
fn token_deadline(now_ms: i64, duration_ms: i64) -> i64 {
    now_ms.checked_add(duration_ms).unwrap_or(i64::MAX)
}

/// A requested period, bounded by the configured one.
///
/// Kafka's `createDelegationToken` and `renewDelegationToken` both use the
/// configured value for a request of 0 or less, and the smaller of the two
/// for a positive request.
#[requires(default_ms@ > 0)]
#[ensures(result@ == bounded_period_model(requested_ms, default_ms))]
#[ensures(result@ > 0)]
fn bounded_period(requested_ms: i64, default_ms: i64) -> i64 {
    if requested_ms > 0 {
        requested_ms.min(default_ms)
    } else {
        default_ms
    }
}

/// Derive both stored deadlines of a new token.
///
/// Matches `DelegationTokenControlManager.createDelegationToken` in Kafka
/// trunk: the lifetime is `delegation.token.max.lifetime.ms` (`ceiling_ms`)
/// for a request of 0 or less and the smaller of the two otherwise, the
/// maximum timestamp is `now` plus that lifetime, and the first expiry is
/// `now` plus `delegation.token.expiry.time.ms` (`default_renew_period_ms`),
/// capped at the maximum timestamp. Both sums saturate at `i64::MAX`. Kafka's
/// configuration validation rejects a period below 1, so a non-positive host
/// setting is `Invalid` here.
#[ensures((result == TokenCreateDecision::Invalid) ==
    (ceiling_ms@ <= 0 || default_renew_period_ms@ <= 0))]
#[ensures(match result {
    TokenCreateDecision::Invalid => true,
    TokenCreateDecision::Create(deadlines) =>
        deadlines.max_timestamp_ms@ ==
            deadline_model(now_ms, bounded_period_model(requested_ms, ceiling_ms))
            && deadlines.initial_expiry_ms@ == min_model(
                deadlines.max_timestamp_ms@,
                deadline_model(now_ms, default_renew_period_ms@),
            ),
})]
// Export arithmetic bounds independently of the private exact-value models.
#[ensures(match result {
    TokenCreateDecision::Invalid => true,
    TokenCreateDecision::Create(d) => now_ms@ <= d.initial_expiry_ms@
        && d.initial_expiry_ms@ <= d.max_timestamp_ms@
        && d.max_timestamp_ms@ <= now_ms@ + ceiling_ms@
        && (requested_ms@ > 0 ==> d.max_timestamp_ms@ <= now_ms@ + requested_ms@)
        && d.initial_expiry_ms@ <= now_ms@ + default_renew_period_ms@
        && (d.max_timestamp_ms@ == i64::MAX@ || d.max_timestamp_ms@ == now_ms@ + ceiling_ms@
            || (requested_ms@ > 0 && d.max_timestamp_ms@ == now_ms@ + requested_ms@))
        && (d.initial_expiry_ms@ == d.max_timestamp_ms@
            || d.initial_expiry_ms@ == now_ms@ + default_renew_period_ms@),
})]
#[must_use]
pub fn create_token_deadlines(
    now_ms: i64,
    requested_ms: i64,
    ceiling_ms: i64,
    default_renew_period_ms: i64,
) -> TokenCreateDecision {
    if ceiling_ms <= 0 || default_renew_period_ms <= 0 {
        return TokenCreateDecision::Invalid;
    }

    let max_timestamp_ms = token_deadline(now_ms, bounded_period(requested_ms, ceiling_ms));
    let renew_deadline_ms = token_deadline(now_ms, default_renew_period_ms);
    TokenCreateDecision::Create(TokenDeadlines {
        max_timestamp_ms,
        initial_expiry_ms: renew_deadline_ms.min(max_timestamp_ms),
    })
}

/// Derive a renewed expiry.
///
/// Matches `DelegationTokenControlManager.renewDelegationToken` in Kafka
/// trunk: a token whose expiry or maximum timestamp is strictly before `now`
/// is `Expired`. Otherwise the renew period is the configured
/// `delegation.token.expiry.time.ms` for a request of 0 or less and the
/// smaller of the two for a positive request, and the new expiry is `now`
/// plus that period, capped at the maximum timestamp. The new expiry replaces
/// the current one even when it is earlier. A non-positive configured period
/// is `Invalid`.
#[ensures((result == TokenRenewDecision::Expired) ==
    (current_expiry_ms@ < now_ms@ || max_timestamp_ms@ < now_ms@))]
#[ensures((result == TokenRenewDecision::Invalid) == (
    current_expiry_ms@ >= now_ms@
        && max_timestamp_ms@ >= now_ms@
        && default_renew_period_ms@ <= 0
))]
#[ensures(match result {
    TokenRenewDecision::Renew(expiry) =>
        expiry@ == min_model(
            max_timestamp_ms@,
            deadline_model(now_ms, bounded_period_model(requested_ms, default_renew_period_ms)),
        ),
    _ => true,
})]
// Export the interval independently of the private arithmetic models.
#[ensures(match result {
    TokenRenewDecision::Renew(expiry) => now_ms@ <= expiry@ && expiry@ <= max_timestamp_ms@
        && (now_ms@ < max_timestamp_ms@ ==> now_ms@ < expiry@),
    _ => true,
})]
#[must_use]
pub fn renew_token_expiry(
    now_ms: i64,
    requested_ms: i64,
    default_renew_period_ms: i64,
    current_expiry_ms: i64,
    max_timestamp_ms: i64,
) -> TokenRenewDecision {
    if current_expiry_ms < now_ms || max_timestamp_ms < now_ms {
        return TokenRenewDecision::Expired;
    }
    if default_renew_period_ms <= 0 {
        return TokenRenewDecision::Invalid;
    }

    let period = bounded_period(requested_ms, default_renew_period_ms);
    TokenRenewDecision::Renew(token_deadline(now_ms, period).min(max_timestamp_ms))
}

/// Select deletion or a bounded expiry update.
///
/// Matches `DelegationTokenControlManager.expireDelegationToken` in Kafka
/// trunk: every negative period deletes the token, whatever its deadlines.
/// Otherwise a token whose expiry or maximum timestamp is strictly before
/// `now` is `Expired`, and a live token gets `now + period`, saturated at
/// `i64::MAX` and capped at its maximum timestamp.
#[ensures((result == TokenExpireDecision::Delete) == (period_ms@ < 0))]
#[ensures((result == TokenExpireDecision::Expired) ==
    (period_ms@ >= 0
        && (current_expiry_ms@ < now_ms@ || max_timestamp_ms@ < now_ms@)))]
#[ensures(match result {
    TokenExpireDecision::Update(expiry) =>
        expiry@ == min_model(max_timestamp_ms@, deadline_model(now_ms, period_ms@)),
    _ => true,
})]
#[must_use]
pub fn expire_token_deadline(
    now_ms: i64,
    period_ms: i64,
    current_expiry_ms: i64,
    max_timestamp_ms: i64,
) -> TokenExpireDecision {
    if period_ms < 0 {
        return TokenExpireDecision::Delete;
    }
    if current_expiry_ms < now_ms || max_timestamp_ms < now_ms {
        return TokenExpireDecision::Expired;
    }

    TokenExpireDecision::Update(token_deadline(now_ms, period_ms).min(max_timestamp_ms))
}

/// Whether a stored delegation token may authenticate at `now_ms`.
#[ensures(result == (
    now_ms@ >= 0
        && expiry_timestamp_ms@ > now_ms@
        && max_timestamp_ms@ > now_ms@
        && expiry_timestamp_ms@ <= max_timestamp_ms@
))]
#[must_use]
pub fn token_is_active(now_ms: i64, expiry_timestamp_ms: i64, max_timestamp_ms: i64) -> bool {
    now_ms >= 0
        && expiry_timestamp_ms > now_ms
        && max_timestamp_ms > now_ms
        && expiry_timestamp_ms <= max_timestamp_ms
}
