use {
    crate::authz::{
        RequestAuthState, request_auth_admission, sasl_session_expiry, session_expired_for_request,
    },
    creusot_std::prelude::*,
};

/// PLAIN, regular SCRAM and GSSAPI have no credential expiry. A positive
/// listener cap still installs a finite, exact deadline, even when adding it
/// to the clock would overflow. Cryptographic success and the snapshot/clock
/// projection remain host obligations, including nondecreasing observations.
#[requires(0 <= now_ms@ && now_ms@ <= request_ms@ && cap_ms@ > 0)]
#[ensures(result.0@ == if now_ms@ + cap_ms@ > i64::MAX@ {
    i64::MAX@
} else { now_ms@ + cap_ms@ })]
#[ensures(result.1@ == result.0@ - now_ms@)]
#[ensures(result.2 == (api_key@ == 17 || api_key@ == 36 || request_ms@ < result.0@))]
pub(super) fn credential_free_session_cap_bounds_requests(
    now_ms: i64,
    cap_ms: i64,
    request_ms: i64,
    api_key: i16,
) -> (i64, i64, bool) {
    let (expiry, lifetime) = sasl_session_expiry(now_ms, None, Some(cap_ms));
    let expiry = expiry.unwrap();
    let admitted = request_auth_admission(RequestAuthState::Authenticated, api_key)
        && !session_expired_for_request(Some(expiry), api_key, request_ms);
    (expiry, lifetime, admitted)
}
