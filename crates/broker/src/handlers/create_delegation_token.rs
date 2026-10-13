//! KIP-48: `CreateDelegationToken` (`api_key` 38).
//!
//! Matches Kafka trunk's `KafkaApis.handleCreateTokenRequest`,
//! `allowTokenRequests`, and
//! `DelegationTokenControlManager.createDelegationToken`, in their order:
//!
//! 0. An owner name with a null owner type cannot be built into a
//!    `KafkaPrincipal`. Kafka answers `UNKNOWN_SERVER_ERROR` (-1) with
//!    `ANONYMOUS` as owner and requester, from
//!    `CreateDelegationTokenRequest.getErrorResponse`.
//! 1. `allowTokenRequests` refuses a caller that is not securely
//!    authenticated, or that authenticated with a delegation token, with
//!    `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64). KIP-48 forbids a token
//!    minting a token.
//! 2. A token for another owner needs `CreateTokens` on
//!    `User:<owner>`, or `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65). A null
//!    or empty owner name means the requester, which needs no ACL. Any owner
//!    principal type is accepted.
//! 3. A renewer whose principal type is not `User` gives
//!    `INVALID_PRINCIPAL_TYPE` (67).
//! 4. The controller then answers `DELEGATION_TOKEN_AUTH_DISABLED` (61)
//!    when no secret key is configured and `UNSUPPORTED_VERSION` (35) below
//!    the delegation-token `metadata.version`.
//!
//! Every error response names the owner and the requester. The three
//! broker-side refusals also carry `-1` timestamps, as Kafka's
//! `CreateDelegationTokenResponse.prepareResponse` writes them.
//!
//! The HMAC-SHA-512 of `(secret_key, token_id)`, 64 bytes as in Kafka's
//! `DelegationTokenManager.createHmac` and computed by
//! `krabka_security::compute_token_hmac`, becomes the token's password
//! equivalent. The metadata record carries no HMAC, as Kafka's
//! `DelegationTokenRecord` has none: every use recomputes it from the secret
//! key. Clients re-authenticate with the `token_id` as the SCRAM username and
//! the HMAC bytes as the password.
//!
//! This file holds the request flow itself. Owner resolution and its
//! authorization live in `owner`, the deadline arithmetic in `lifetime`, and
//! the response shapes in `wire`.

use {
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        codes::{
            DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_AUTHORIZATION_FAILED,
            DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, INVALID_PRINCIPAL_TYPE, INVALID_REQUEST,
            UNKNOWN_SERVER_ERROR, UNSUPPORTED_VERSION,
        },
        features::{METADATA_VERSION, require_feature},
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
        time_util::now_ms,
    },
    krabka_metadata::{AclOperation, DelegationTokenRecord, MetadataRecord, ResourceType},
    krabka_protocol::owned::{
        create_delegation_token_request::CreateDelegationTokenRequest,
        create_delegation_token_response::CreateDelegationTokenResponse,
    },
    krabka_security::{KafkaPrincipal, SecretBytes, compute_token_hmac},
    krabka_verified::delegation_token::{TokenApi, TokenApiAdmission},
    std::net::SocketAddr,
};

/// What the KIP-373 `CreateTokens` check reads: the broker's authorizer and
/// the peer the request came from.
#[derive(Clone, Copy)]
pub(crate) struct TokenAcl<'a> {
    pub(crate) authorizer: &'a dyn Authorizer,
    pub(crate) peer: &'a SocketAddr,
}

mod lifetime;
mod owner;
mod wire;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    lifetime::{TokenCreateDecision, create_token_deadlines},
    owner::{may_create_for, resolve_owner},
    wire::{broker_refusal, controller_refusal, minted_response},
};

/// A relative span of milliseconds, such as a token lifetime or a renew
/// period. It is not an absolute epoch timestamp in milliseconds.
pub(crate) type DurationMs = i64;

/// Kafka's `KafkaPrincipal.USER_TYPE`, the only renewer type Kafka accepts.
const USER_PRINCIPAL_TYPE: &str = "User";

/// Kafka's `KafkaPrincipal.ANONYMOUS`, the requester of a connection that has
/// not authenticated.
fn anonymous_principal() -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: USER_PRINCIPAL_TYPE.to_string(),
        name: "ANONYMOUS".to_string(),
    }
}

#[tracing::instrument(
    name = "handle_create_delegation_token",
    level = "info",
    skip_all,
    fields(api = "CreateDelegationToken")
)]
pub(crate) async fn handle(
    req: &CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    max_lifetime_ms: DurationMs,
    default_renew_period_ms: DurationMs,
    controller: &dyn MetadataSource,
    acl: TokenAcl<'_>,
) -> CreateDelegationTokenResponse {
    let requester = auth
        .principal()
        .map_or_else(anonymous_principal, krabka_security::Principal::to_kafka);
    // Kafka resolves the owner before any check. When the constructor throws,
    // `KafkaApis.handle` answers through `getErrorResponse`, which names
    // ANONYMOUS as both principals.
    let Some(owner) = resolve_owner(req, &requester) else {
        let anonymous = anonymous_principal();
        return broker_refusal(UNKNOWN_SERVER_ERROR, &anonymous, &anonymous);
    };

    if auth.token_api_admission(TokenApi::Create) == TokenApiAdmission::Reject {
        return broker_refusal(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, &owner, &requester);
    }
    let image = controller.current_image();
    let authorize_create_tokens = |resource_name: &str| {
        auth.principal().is_some_and(|principal| {
            acl.authorizer.authorize(&*image, &AuthorizationRequest {
                principal,
                host: acl.peer,
                resource_type: ResourceType::User,
                resource_name,
                operation: AclOperation::CreateTokens,
            }) == AuthorizationResult::Allow
        })
    };
    if !may_create_for(&owner, &requester, authorize_create_tokens) {
        return broker_refusal(DELEGATION_TOKEN_AUTHORIZATION_FAILED, &owner, &requester);
    }
    if req
        .renewers
        .iter()
        .any(|renewer| renewer.principal_type != USER_PRINCIPAL_TYPE)
    {
        return broker_refusal(INVALID_PRINCIPAL_TYPE, &owner, &requester);
    }

    let Some(secret_key) = secret_key else {
        return controller_refusal(DELEGATION_TOKEN_AUTH_DISABLED, &owner, &requester);
    };
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if require_feature(
        &image,
        METADATA_VERSION,
        krabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return controller_refusal(UNSUPPORTED_VERSION, &owner, &requester);
    }

    let now = now_ms();
    let deadlines = match create_token_deadlines(
        now,
        req.max_lifetime_ms,
        max_lifetime_ms,
        default_renew_period_ms,
    ) {
        TokenCreateDecision::Create(deadlines) => deadlines,
        TokenCreateDecision::Invalid => {
            return controller_refusal(INVALID_REQUEST, &owner, &requester);
        }
    };
    let token_id = uuid::Uuid::new_v4().to_string();
    let hmac = compute_token_hmac(secret_key.as_bytes(), &token_id);

    let renewers: Vec<KafkaPrincipal> = req
        .renewers
        .iter()
        .map(|r| KafkaPrincipal {
            principal_type: r.principal_type.clone(),
            name: r.principal_name.clone(),
        })
        .collect();

    let record = DelegationTokenRecord {
        token_id: token_id.clone(),
        owner: owner.clone(),
        requester: requester.clone(),
        issue_timestamp_ms: now,
        expiry_timestamp_ms: deadlines.initial_expiry_ms,
        max_timestamp_ms: deadlines.max_timestamp_ms,
        renewers,
    };

    if let Err(e) = controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
        .await
    {
        tracing::warn!(error = %e, "CreateDelegationToken: submit_change failed");
        return controller_refusal(INVALID_REQUEST, &owner, &requester);
    }

    minted_response(&owner, &requester, now, &deadlines, token_id, hmac)
}
