//! KIP-48: `RenewDelegationToken` (`api_key` 39).
//!
//! Matches Kafka trunk's `KafkaApis.handleRenewTokenRequest` and
//! `DelegationTokenControlManager.renewDelegationToken`, in their order:
//!
//! 1. `allowTokenRequests` refuses a caller that is not securely
//!    authenticated, or that authenticated with a delegation token, with
//!    `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) and an expiry of `-1`
//!    (`DelegationTokenManager.ERROR_TIMESTAMP`).
//! 2. The controller answers `DELEGATION_TOKEN_AUTH_DISABLED` (61) when no
//!    secret key is configured, `UNSUPPORTED_VERSION` (35) below the
//!    delegation-token `metadata.version`, `DELEGATION_TOKEN_NOT_FOUND` (62)
//!    for an unknown `hmac`, and `DELEGATION_TOKEN_EXPIRED` (66) for a token
//!    whose expiry or maximum timestamp is before now.
//! 3. `allowedToRenew`: only the owner or a listed renewer may renew. Anyone
//!    else, a super user included, gets `DELEGATION_TOKEN_OWNER_MISMATCH`
//!    (63).
//!
//! The new expiry is `now + min(delegation.token.expiry.time.ms,
//! renew_period_ms)` for a positive period and `now +
//! delegation.token.expiry.time.ms` otherwise, capped at the token's
//! `max_timestamp_ms`. It replaces the current expiry even when it is
//! earlier. The handler appends a fresh `V1DelegationToken` record with the
//! same `token_id`; the image semantics are replace.

use {
    crate::{
        codes::{
            DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_NOT_FOUND,
            DELEGATION_TOKEN_OWNER_MISMATCH, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, INVALID_REQUEST,
            UNSUPPORTED_VERSION,
        },
        features::{METADATA_VERSION, require_feature},
        handlers::ErrorCodeResponse as _,
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
        time_util::now_ms,
    },
    krabka_metadata::{DelegationToken, DelegationTokenRecord},
    krabka_protocol::owned::{
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    },
    krabka_raft::DelegationTokenMutation,
    krabka_security::{Principal, SecretBytes},
    krabka_verified::{
        TokenRenewDecision,
        delegation_token::{TokenApi, TokenApiAdmission},
        renew_token_expiry,
    },
};

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry the broker
/// answers with when it refuses the request before forwarding it.
const ERROR_TIMESTAMP: i64 = -1;

#[tracing::instrument(
    name = "handle_renew_delegation_token",
    level = "info",
    skip_all,
    fields(api = "RenewDelegationToken")
)]
pub(crate) async fn handle(
    req: &RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    default_renew_period_ms: i64,
    controller: &dyn MetadataSource,
) -> RenewDelegationTokenResponse {
    let (principal, token) = match admit_token_request(
        TokenApi::Renew,
        auth,
        secret_key,
        req.hmac.as_ref(),
        controller,
    ) {
        Ok(admitted) => admitted,
        Err((error_code, expiry_timestamp_ms)) => {
            return RenewDelegationTokenResponse {
                expiry_timestamp_ms,
                ..RenewDelegationTokenResponse::error(error_code)
            };
        }
    };
    let caller = principal.to_kafka();

    let decision = renew_token_expiry(
        now_ms(),
        req.renew_period_ms,
        default_renew_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    );
    if decision == TokenRenewDecision::Expired {
        return RenewDelegationTokenResponse::error(DELEGATION_TOKEN_EXPIRED);
    }
    if token.owner != caller && !token.renewers.contains(&caller) {
        return RenewDelegationTokenResponse::error(DELEGATION_TOKEN_OWNER_MISMATCH);
    }
    let TokenRenewDecision::Renew(new_expiry) = decision else {
        return RenewDelegationTokenResponse::error(INVALID_REQUEST);
    };

    let expected = token.to_record();
    let replacement = DelegationTokenRecord {
        expiry_timestamp_ms: new_expiry,
        ..expected.clone()
    };
    if let Err(e) = controller
        .submit_delegation_token_mutations(vec![DelegationTokenMutation::Renew {
            expected,
            replacement,
        }])
        .await
    {
        tracing::warn!(error = %e, "RenewDelegationToken: submit_change failed");
        return RenewDelegationTokenResponse::error(INVALID_REQUEST);
    }

    RenewDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

/// The checks `RenewDelegationToken` and `ExpireDelegationToken` share,
/// steps 1 and 2 of Kafka's order up to the `hmac` lookup.
///
/// Returns the authenticated principal and the token the `hmac` names, or the
/// refusal's error code and the expiry its response carries:
/// `ERROR_TIMESTAMP` for a broker-side refusal, the schema default of 0 for a
/// controller-side one.
pub(crate) fn admit_token_request<'a>(
    api: TokenApi,
    auth: &'a ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    hmac: &[u8],
    controller: &dyn MetadataSource,
) -> Result<(&'a Principal, DelegationToken), (i16, i64)> {
    let refuse = |code| Err((code, 0));
    if auth.token_api_admission(api) == TokenApiAdmission::Reject {
        return Err((DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, ERROR_TIMESTAMP));
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return Err((DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, ERROR_TIMESTAMP));
    };
    let Some(secret_key) = secret_key else {
        return refuse(DELEGATION_TOKEN_AUTH_DISABLED);
    };

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if require_feature(
        &image,
        METADATA_VERSION,
        krabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return refuse(UNSUPPORTED_VERSION);
    }
    let Some(token) = image
        .delegation_token_by_hmac(secret_key.as_bytes(), hmac)
        .cloned()
    else {
        return refuse(DELEGATION_TOKEN_NOT_FOUND);
    };
    Ok((principal, token))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::DELEGATION_TOKEN_EXPIRED,
            handlers::{
                delegation_token_test_support as token_fixtures,
                delegation_token_test_support::{
                    DAY_MS, ExpiryExpectation, MutationExecution, SeededToken, TokenExpirySetup,
                    TokenFixture, TokenMutationRefusalSetup, TokenPeriodMillis, authed,
                    in_mutation_window, mutation_credential_refusals, ordinary_ownership_refusals,
                    stored_expiry as stored_expiry_fixture,
                },
            },
            test_support::{KafkaErrorCode, UnixMillis},
        },
        assert2::assert,
        krabka_raft::ControllerHandle,
    };

    fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> i64 {
        stored_expiry_fixture(controller, token_id).expect("token remains")
    }

    /// Refusals come in Kafka's order, carry Kafka's expiry, and leave the
    /// stored expiry alone. A super user that is neither owner nor renewer
    /// gets `DELEGATION_TOKEN_OWNER_MISMATCH`, as `allowedToRenew` gives it,
    /// and an expired token answers `DELEGATION_TOKEN_EXPIRED` before the
    /// owner check.
    #[tokio::test]
    async fn refusals_follow_kafka_order() {
        token_fixtures::check_refusals()
            .cases(|live, expired| {
                mutation_credential_refusals(&live.hmac)
                    .chain([
                        TokenMutationRefusalSetup {
                            name: "expired token, foreign caller",
                            authentication: authed("eve"),
                            hmac: expired.hmac.clone(),
                            error: KafkaErrorCode(DELEGATION_TOKEN_EXPIRED),
                            ..Default::default()
                        },
                        TokenMutationRefusalSetup {
                            name: "expired token, owner",
                            hmac: expired.hmac.clone(),
                            error: KafkaErrorCode(DELEGATION_TOKEN_EXPIRED),
                            ..Default::default()
                        },
                    ])
                    .chain(ordinary_ownership_refusals(&live.hmac))
                    .collect::<Vec<_>>()
            })
            .mutation(async |controller, secret, case| {
                let resp = handle(
                    &RenewDelegationTokenRequest {
                        hmac: case.hmac.into(),
                        renew_period_ms: 1_000,
                        ..Default::default()
                    },
                    &case.authentication,
                    case.secret.get(secret),
                    DAY_MS,
                    controller,
                )
                .await;
                krabka_macros::token_refusal_result!(RenewDelegationTokenResponse, case, resp)
            })
            .verify(|controller, live, expired| {
                assert!(
                    stored_expiry(controller, "live") == live.expires_at
                        && stored_expiry(controller, "expired") == expired.expires_at
                );
            })
            .call()
            .await;
    }

    /// The owner or a renewer renews to `min(max, now + min(default,
    /// period))` for a positive period and `min(max, now + default)`
    /// otherwise, even when that is earlier than the current expiry.
    #[tokio::test]
    async fn renews_to_kafka_capped_expiry() {
        let TokenFixture {
            directory: _dir,
            controller,
            secret,
        } = TokenFixture::new().await;
        let hour: i64 = 60 * 60 * 1_000;
        let default_period = 2 * hour;

        // (case, caller, renew_period_ms, current expiry delta, max delta,
        //  expected expiry delta, where a delta of `None` means the max)
        let cases = [
            (
                "owner, shorter period",
                "alice",
                hour,
                hour,
                DAY_MS,
                Some(hour),
            ),
            (
                "renewer, shorter period",
                "bob",
                hour,
                hour,
                DAY_MS,
                Some(hour),
            ),
            (
                "period above the default is capped",
                "alice",
                10 * hour,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "-1 takes the default",
                "alice",
                -1,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "0 takes the default",
                "alice",
                0,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "-5 takes the default",
                "alice",
                -5,
                hour,
                DAY_MS,
                Some(default_period),
            ),
            (
                "shortens a later expiry",
                "alice",
                60_000,
                20 * hour,
                DAY_MS,
                Some(60_000),
            ),
            (
                "capped at the max timestamp",
                "alice",
                -1,
                60_000,
                30 * 60_000,
                None,
            ),
            (
                "period near i64::MAX",
                "alice",
                i64::MAX,
                60_000,
                30 * 60_000,
                None,
            ),
        ];
        for (index, (case, caller, period, expiry_delta, max_delta, expected_delta)) in
            cases.into_iter().enumerate()
        {
            let SeededToken {
                token_id,
                hmac,

                maximum: max_timestamp_ms,
            } = token_fixtures::seed_mutation_token()
                .controller(&controller)
                .index(index)
                .expiry_delta(TokenPeriodMillis(expiry_delta))
                .maximum_delta(TokenPeriodMillis(max_delta))
                .call()
                .await;

            let MutationExecution {
                window,
                response: resp,
            } = in_mutation_window(handle(
                &RenewDelegationTokenRequest {
                    hmac: hmac.into(),
                    renew_period_ms: period,
                    ..Default::default()
                },
                &authed(caller),
                Some(&secret),
                default_period,
                &*controller,
            ))
            .await;
            let expiry_setup = TokenExpirySetup {
                window,
                expectation: ExpiryExpectation::for_period(
                    expected_delta.map(TokenPeriodMillis),
                    UnixMillis(max_timestamp_ms),
                ),
            };
            expiry_setup.check(
                case,
                token_fixtures::TokenMutationOutcome::from_wire(
                    resp.error_code,
                    resp.expiry_timestamp_ms,
                ),
            );
            assert!(
                stored_expiry(&controller, &token_id) == resp.expiry_timestamp_ms,
                "{case}"
            );
        }
        controller.cancel().await;
    }
}
