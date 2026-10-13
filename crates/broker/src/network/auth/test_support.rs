//! Whole-struct assertions on the two `SaslAuthenticate` response shapes.
//!
//! The mechanism modules each check the same success and failure envelopes,
//! so the comparisons live here rather than once per test module.

use {
    crate::codes::SASL_AUTHENTICATION_FAILED, assert2::assert,
    krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
};

pub(super) fn assert_success_authenticate_response(
    resp: &SaslAuthenticateResponse,
    expected_auth_bytes: &[u8],
    expected_session_lifetime_ms: i64,
) {
    let expected = SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::copy_from_slice(expected_auth_bytes),
        session_lifetime_ms: expected_session_lifetime_ms,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(*resp == expected);
}

/// `expected_message` is `None` for a failure that goes out with Kafka's
/// generic text, which the dispatcher fills in.
pub(super) fn assert_failed_authenticate_response(
    resp: &SaslAuthenticateResponse,
    expected_message: Option<&str>,
) {
    let expected = SaslAuthenticateResponse {
        error_code: SASL_AUTHENTICATION_FAILED,
        error_message: expected_message.map(str::to_owned),
        auth_bytes: bytes::Bytes::new(),
        session_lifetime_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(*resp == expected);
}

/// Existing OAuth session while an in-band reauthentication exchange is pending.
pub(super) fn oauth_reauthenticating(name: &str, expires_at_ms: i64) -> super::ConnectionAuth {
    super::ConnectionAuth::Reauthenticating {
        previous: super::AuthenticatedSnapshot {
            principal: krabka_security::Principal {
                name: name.to_owned(),
                auth_method: krabka_security::AuthMethod::SaslOAuthBearer,
                groups: vec![],
            },
            mechanism: krabka_security::SaslMechanism::OAuthBearer,
            expires_at_ms: Some(expires_at_ms),
            authenticated_via_token: false,
        },
        exchange: super::state::SaslExchange::OAuthBearer,
        pending_token_expiry_ms: None,
    }
}

/// Construct a session without deriving its auth method from the mechanism.
pub(super) fn authenticated(
    name: &str,
    auth_method: krabka_security::AuthMethod,
    mechanism: krabka_security::SaslMechanism,
    expires_at_ms: Option<i64>,
    authenticated_via_token: bool,
) -> super::ConnectionAuth {
    super::ConnectionAuth::Authenticated {
        principal: krabka_security::Principal {
            name: name.to_owned(),
            auth_method,
            groups: vec![],
        },
        mechanism,
        expires_at_ms,
        authenticated_via_token,
    }
}

/// The RFC 4616 bytes with an empty authorization identity.
pub(crate) fn plain_request(
    user: &str,
    password: &str,
) -> krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
    let mut bytes = vec![0_u8];
    bytes.extend_from_slice(user.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(password.as_bytes());
    krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(bytes),
        ..Default::default()
    }
}
