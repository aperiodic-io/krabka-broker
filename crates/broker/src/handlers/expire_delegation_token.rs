//! KIP-48: `ExpireDelegationToken` (`api_key` 40).
//!
//! Matches Kafka trunk's `KafkaApis.handleExpireTokenRequest` and
//! `DelegationTokenControlManager.expireDelegationToken`, in their order:
//!
//! 1. `allowTokenRequests` refuses a caller that is not securely
//!    authenticated, or that authenticated with a delegation token, with
//!    `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) and an expiry of `-1`
//!    (`DelegationTokenManager.ERROR_TIMESTAMP`).
//! 2. The controller answers `DELEGATION_TOKEN_AUTH_DISABLED` (61) when no
//!    secret key is configured, `UNSUPPORTED_VERSION` (35) below the
//!    delegation-token `metadata.version`, and `DELEGATION_TOKEN_NOT_FOUND`
//!    (62) for an unknown `hmac`.
//! 3. `allowedToRenew`: only the owner or a listed renewer may expire the
//!    token. Anyone else, a super user included, gets
//!    `DELEGATION_TOKEN_OWNER_MISMATCH` (63).
//!
//! Then `expiry_time_period_ms` decides:
//!   - Below 0: the token is deleted, whatever its deadlines, and the response
//!     carries `expiry_timestamp_ms = now`.
//!   - Otherwise a token whose expiry or maximum timestamp is before now gets
//!     `DELEGATION_TOKEN_EXPIRED` (66), and a live one gets `now + period`,
//!     saturated at `i64::MAX` and capped at its `max_timestamp_ms`.

use krabka_metadata::DelegationTokenRecord;
use krabka_protocol::owned::{
    expire_delegation_token_request::ExpireDelegationTokenRequest,
    expire_delegation_token_response::ExpireDelegationTokenResponse,
};
use krabka_raft::DelegationTokenMutation;
use krabka_security::SecretBytes;
use krabka_verified::{TokenExpireDecision, delegation_token::TokenApi, expire_token_deadline};

use crate::{
    codes::{DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_OWNER_MISMATCH, INVALID_REQUEST},
    handlers::{ErrorCodeResponse as _, renew_delegation_token::admit_token_request},
    metadata_source::MetadataSource,
    network::auth::ConnectionAuth,
    time_util::now_ms,
};

#[tracing::instrument(
    name = "handle_expire_delegation_token",
    level = "info",
    skip_all,
    fields(api = "ExpireDelegationToken")
)]
pub(crate) async fn handle(
    req: &ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn MetadataSource,
) -> ExpireDelegationTokenResponse {
    let (principal, token) = match admit_token_request(
        TokenApi::Expire,
        auth,
        secret_key,
        req.hmac.as_ref(),
        controller,
    ) {
        Ok(admitted) => admitted,
        Err((error_code, expiry_timestamp_ms)) => {
            return ExpireDelegationTokenResponse {
                expiry_timestamp_ms,
                ..ExpireDelegationTokenResponse::error(error_code)
            };
        }
    };
    let caller = principal.to_kafka();

    if token.owner != caller && !token.renewers.contains(&caller) {
        return ExpireDelegationTokenResponse::error(DELEGATION_TOKEN_OWNER_MISMATCH);
    }

    let now = now_ms();
    let expected = token.to_record();
    let (mutation, new_expiry) = match expire_token_deadline(
        now,
        req.expiry_time_period_ms,
        token.expiry_timestamp_ms,
        token.max_timestamp_ms,
    ) {
        TokenExpireDecision::Expired => {
            return ExpireDelegationTokenResponse::error(DELEGATION_TOKEN_EXPIRED);
        }
        TokenExpireDecision::Delete => (DelegationTokenMutation::Delete { expected }, now),
        TokenExpireDecision::Update(new_expiry) => {
            let replacement = DelegationTokenRecord {
                expiry_timestamp_ms: new_expiry,
                ..expected.clone()
            };
            (
                DelegationTokenMutation::Expire {
                    expected,
                    replacement,
                },
                new_expiry,
            )
        }
    };
    if let Err(e) = controller
        .submit_delegation_token_mutations(vec![mutation])
        .await
    {
        tracing::warn!(error = %e, "ExpireDelegationToken: submit_change failed");
        return ExpireDelegationTokenResponse::error(INVALID_REQUEST);
    }

    ExpireDelegationTokenResponse {
        error_code: 0,
        expiry_timestamp_ms: new_expiry,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        codes::{DELEGATION_TOKEN_EXPIRED, DELEGATION_TOKEN_OWNER_MISMATCH},
        handlers::delegation_token_test_support::{
            DAY_MS, MutationSeedSetup, TokenExpiryRule, TokenExpirySetup, TokenMutationIndex,
            TokenMutationRefusalSetup, TokenPeriodMillis, TokenPersistence, authed,
            in_mutation_window, mutation_credential_refusals, ordinary_ownership_refusals,
            stored_expiry, test_controller,
        },
        test_support::{KafkaErrorCode, UnixMillis},
    };

    /// Refusals come in Kafka's order, carry Kafka's expiry, and leave the
    /// token alone. A caller that is neither owner nor renewer gets
    /// `DELEGATION_TOKEN_OWNER_MISMATCH`, a super user included and before
    /// the expiry check.
    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ExpiryCaseSetup<'a> {
        #[default("owner deletes")]
        name: &'a str,
        #[default("alice")]
        caller: &'a str,
        #[default(TokenPeriodMillis(-1))]
        period: TokenPeriodMillis,
        #[default(TokenPeriodMillis(60_000))]
        expiry_delta: TokenPeriodMillis,
        #[default(TokenPeriodMillis(DAY_MS))]
        maximum_delta: TokenPeriodMillis,
        #[default(TokenExpiryRule::RelativeToMutation(TokenPeriodMillis(0)))]
        rule: TokenExpiryRule,
        persistence: TokenPersistence,
    }

    #[tokio::test]
    async fn refusals_follow_kafka_order() {
        refusal_token_fixture!((_dir, controller, secret, live, expired));

        let cases = mutation_credential_refusals(&live.0)
            .into_iter()
            .chain(ordinary_ownership_refusals(&live.0))
            .chain([
                TokenMutationRefusalSetup {
                    name: "foreign caller, expired token",
                    authentication: authed("eve"),
                    hmac: expired.0.clone(),
                    period: TokenPeriodMillis(0),
                    error: KafkaErrorCode(DELEGATION_TOKEN_OWNER_MISMATCH),
                    ..Default::default()
                },
                TokenMutationRefusalSetup {
                    name: "owner, expired token",
                    hmac: expired.0.clone(),
                    period: TokenPeriodMillis(0),
                    error: KafkaErrorCode(DELEGATION_TOKEN_EXPIRED),
                    ..Default::default()
                },
                TokenMutationRefusalSetup {
                    name: "renewer, expired token",
                    authentication: authed("bob"),
                    hmac: expired.0.clone(),
                    period: TokenPeriodMillis(1_000),
                    error: KafkaErrorCode(DELEGATION_TOKEN_EXPIRED),
                    ..Default::default()
                },
            ]);
        for case in cases {
            let resp = handle(
                &ExpireDelegationTokenRequest {
                    hmac: case.hmac.into(),
                    expiry_time_period_ms: case.period.0,
                    ..Default::default()
                },
                &case.authentication,
                case.secret.get(&secret),
                &*controller,
            )
            .await;
            check_token_mutation_refusal!(resp, case, ExpireDelegationTokenResponse);
        }
        assert!(
            (
                stored_expiry(&controller, "live"),
                stored_expiry(&controller, "expired")
            ) == (Some(live.1), Some(expired.1))
        );
        controller.cancel().await;
    }

    /// The owner or a renewer deletes the token for a negative period, even an
    /// expired one, and answers `now`; a non-negative period sets
    /// `min(max, now + period)`.
    #[tokio::test]
    async fn expires_to_kafka_deadline() {
        token_fixture!(dir, controller, secret);

        let cases = [
            ExpiryCaseSetup::default(),
            ExpiryCaseSetup {
                name: "renewer deletes",
                caller: "bob",
                period: TokenPeriodMillis(-5),
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "owner deletes an expired token",
                expiry_delta: TokenPeriodMillis(-1_000),
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "zero expires now",
                period: TokenPeriodMillis(0),
                persistence: TokenPersistence::Stored,
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "renewer shortens",
                caller: "bob",
                period: TokenPeriodMillis(30_000),
                rule: TokenExpiryRule::RelativeToMutation(TokenPeriodMillis(30_000)),
                persistence: TokenPersistence::Stored,
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "owner lengthens",
                period: TokenPeriodMillis(120_000),
                rule: TokenExpiryRule::RelativeToMutation(TokenPeriodMillis(120_000)),
                persistence: TokenPersistence::Stored,
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "capped at the max timestamp",
                period: TokenPeriodMillis(2 * DAY_MS),
                rule: TokenExpiryRule::LifetimeCap,
                persistence: TokenPersistence::Stored,
                ..Default::default()
            },
            ExpiryCaseSetup {
                name: "period near i64::MAX saturates",
                period: TokenPeriodMillis(i64::MAX),
                rule: TokenExpiryRule::LifetimeCap,
                persistence: TokenPersistence::Stored,
                ..Default::default()
            },
        ];
        for (
            index,
            ExpiryCaseSetup {
                name: case,
                caller,
                period,
                expiry_delta,
                maximum_delta,
                rule,
                persistence,
            },
        ) in cases.into_iter().enumerate()
        {
            seeded_token_fixture!(
                (token_id, hmac, seeded_at, max_timestamp_ms),
                &controller,
                MutationSeedSetup {
                    index: TokenMutationIndex(index),
                    expiry_delta,
                    maximum_delta
                }
            );

            let (window, resp) = in_mutation_window(handle(
                &ExpireDelegationTokenRequest {
                    hmac: hmac.into(),
                    expiry_time_period_ms: period.0,
                    ..Default::default()
                },
                &authed(caller),
                Some(&secret),
                &*controller,
            ))
            .await;
            check_token_expiry!(
                case,
                resp,
                TokenExpirySetup {
                    window,
                    expectation: rule.with_cap(UnixMillis(max_timestamp_ms)),
                }
            );
            let stored = stored_expiry(&controller, &token_id);
            let want = match persistence {
                TokenPersistence::Deleted => None,
                TokenPersistence::Stored => Some(resp.expiry_timestamp_ms),
            };
            assert!(stored == want, "{case}");
        }
        controller.cancel().await;
    }
}
