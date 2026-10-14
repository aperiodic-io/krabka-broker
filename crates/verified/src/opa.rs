//! OPA authorization-cache admission and error policy.
//!
//! All deadlines here are milliseconds on the host's monotonic clock. The
//! kernels compare and add them; that the clock never steps backwards is a
//! host responsibility, discharged by reading a `MonotonicClock`.

use creusot_std::prelude::*;

/// Binary authorization result used at the OPA proof boundary.
#[krabka_macros::model_type]
pub enum OpaAuthorizationDecision {
    Allow,
    Deny,
}

/// Result of checking one exact-key cache entry.
#[krabka_macros::model_type]
pub enum OpaCacheAdmission {
    Miss,
    Hit(OpaAuthorizationDecision),
}

/// Result of computing a monotonic cache deadline.
#[krabka_macros::model_type]
pub enum OpaCacheExpiry {
    DoNotCache,
    CacheUntil { expires_at_ms: i128 },
}

/// Reuse an entry's decision only while its deadline is strictly in the
/// future.
///
/// The caller passes only an entry found under the complete authorization
/// key; the host's hash-map lookup on that key is what makes it an exact
/// match. The exact deadline is already stale.
#[ensures(match result {
    OpaCacheAdmission::Miss => expires_at_ms@ <= now_ms@,
    OpaCacheAdmission::Hit(decision) => expires_at_ms@ > now_ms@
        && decision == cached_decision,
})]
#[must_use]
pub fn opa_cache_admission(
    now_ms: i128,
    expires_at_ms: i128,
    cached_decision: OpaAuthorizationDecision,
) -> OpaCacheAdmission {
    if expires_at_ms > now_ms {
        OpaCacheAdmission::Hit(cached_decision)
    } else {
        OpaCacheAdmission::Miss
    }
}

/// Compute a deadline `ttl_ms` after `now_ms`.
///
/// A decision is cached exactly when the TTL is positive and the deadline is
/// representable: `DoNotCache` iff `ttl_ms <= 0` or `now_ms + ttl_ms`
/// overflows `i128`.
#[ensures(match result {
    OpaCacheExpiry::DoNotCache => ttl_ms@ <= 0 || now_ms@ + ttl_ms@ > i128::MAX@,
    OpaCacheExpiry::CacheUntil { expires_at_ms } => ttl_ms@ > 0
        && expires_at_ms@ == now_ms@ + ttl_ms@,
})]
#[must_use]
pub fn opa_cache_expiry(now_ms: i128, ttl_ms: i64) -> OpaCacheExpiry {
    if ttl_ms <= 0 {
        return OpaCacheExpiry::DoNotCache;
    }
    match now_ms.checked_add(i128::from(ttl_ms)) {
        Some(expires_at_ms) => OpaCacheExpiry::CacheUntil { expires_at_ms },
        None => OpaCacheExpiry::DoNotCache,
    }
}

/// Map the explicit outage policy without changing successful OPA decisions.
#[ensures((result == OpaAuthorizationDecision::Allow) == allow_on_error)]
#[must_use]
pub fn opa_error_decision(allow_on_error: bool) -> OpaAuthorizationDecision {
    if allow_on_error {
        OpaAuthorizationDecision::Allow
    } else {
        OpaAuthorizationDecision::Deny
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OpaAuthorizationDecision, OpaCacheAdmission, OpaCacheExpiry, opa_cache_admission,
        opa_cache_expiry, opa_error_decision,
    };

    #[yare::parameterized(
        allow_before_expiry = {
            9,
            10,
            OpaAuthorizationDecision::Allow,
            OpaCacheAdmission::Hit(OpaAuthorizationDecision::Allow),
        },
        deny_before_expiry = {
            9,
            10,
            OpaAuthorizationDecision::Deny,
            OpaCacheAdmission::Hit(OpaAuthorizationDecision::Deny),
        },
        exact_expiry = {
            10,
            10,
            OpaAuthorizationDecision::Allow,
            OpaCacheAdmission::Miss,
        },
        after_expiry = {
            11,
            10,
            OpaAuthorizationDecision::Deny,
            OpaCacheAdmission::Miss,
        },
    )]
    fn cache_requires_strict_freshness(
        now_ms: i128,
        expires_at_ms: i128,
        cached: OpaAuthorizationDecision,
        expected: OpaCacheAdmission,
    ) {
        assert2::check!(opa_cache_admission(now_ms, expires_at_ms, cached) == expected);
    }

    #[test]
    fn expiry_and_error_mapping_fail_safely() {
        assert2::check!(
            opa_cache_expiry(10, 5) == OpaCacheExpiry::CacheUntil { expires_at_ms: 15 }
        );
        assert2::check!(opa_cache_expiry(10, 0) == OpaCacheExpiry::DoNotCache);
        assert2::check!(opa_cache_expiry(10, -1) == OpaCacheExpiry::DoNotCache);
        assert2::check!(
            opa_cache_expiry(i128::MAX - 1, 1)
                == OpaCacheExpiry::CacheUntil {
                    expires_at_ms: i128::MAX
                }
        );
        assert2::check!(opa_cache_expiry(i128::MAX, 1) == OpaCacheExpiry::DoNotCache);
        assert2::check!(
            opa_error_decision(false) == OpaAuthorizationDecision::Deny
                && opa_error_decision(true) == OpaAuthorizationDecision::Allow
        );
    }
}
