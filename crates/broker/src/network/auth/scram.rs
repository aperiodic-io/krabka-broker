//! SASL/SCRAM (RFC 5802) over `SaslAuthenticate`, with KIP-48 delegation-token
//! authentication.
//!
//! The two RFC 5802 rounds and the token credential that KIP-48 defines are
//! one concern: the client-first `tokenauth` extension picks the token store
//! inside SCRAM round 1, and cannot be read apart from it.

use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::{
    Principal, SaslMechanism, ScramServerExchange, SecretBytes, compute_token_hmac,
};
use krabka_units::Time;
use krabka_verified::delegation_token::{ScramCredentialSource, scram_credential_source};

use super::{
    response::{fail_authenticate, fail_authenticate_with, sasl_ok},
    state::{ConnectionAuth, SaslExchange, session_expiry},
};
use crate::{metadata_source::MetadataSource, time_util::now_ms};

/// SCRAM-SHA-256 and SCRAM-SHA-512 `SaslAuthenticate` handler. It runs the two RFC 5802 rounds
/// over Kafka's `SaslAuthenticate` (`api_key` 36) wire envelope.
///
/// Round 1 (client-first):
///   - `auth_bytes` is the raw SCRAM client-first message,
///     `n,[a=<authzid>],n=<user>,r=<client-nonce>[,tokenauth=true]`. The
///     handler parses it as Kafka's `ScramSaslServer` does, looks up the
///     credential in the metadata image (the delegation-token store when
///     `tokenauth` is set, the SCRAM user store otherwise), and builds a
///     [`ScramServerExchange`]. The exchange consumes the same client-first
///     bytes and emits the server-first message (`r=…,s=…,i=…`), which
///     becomes the response `auth_bytes`. `auth` moves from
///     `Negotiating { exchange: ScramPending }` to
///     `Negotiating { exchange: Scram(server) }`, still unauthenticated.
///
/// Round 2 (client-final):
///   - `auth_bytes` is `c=biws,r=<combined-nonce>,p=<proof>`. The exchange
///     verifies the client proof and emits the server-final message
///     (`v=<server-signature>`). On success, `auth` moves to
///     `Authenticated { principal }`. On any error, the response carries
///     `error_code = 58` and the dispatcher closes the connection.
///
/// A SCRAM credential carries no lifetime of its own, so `max_reauth` — the
/// listener's `connections.max.reauth.ms` — is what bounds a regular SCRAM
/// session (KIP-368). A delegation-token session is bounded by the earlier of
/// the token expiry and that cap.
///
/// A delegation token's password is the base64 of its HMAC, which the
/// metadata image does not hold: it is recomputed under `secret_key`, the
/// broker's `delegation.token.secret.key`. Without one no token can
/// authenticate, as in Kafka, whose token cache stays empty.
pub fn handle_authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    controller: &dyn MetadataSource,
    secret_key: Option<&SecretBytes>,
    max_reauth: Option<Time>,
) -> SaslAuthenticateResponse {
    super::state::authenticate_with_reauth(auth, |auth| {
        // Round-1 case: still in `ScramPending` — build the exchange now that
        // we have the client-first bytes (and thus the username).
        if let ConnectionAuth::Negotiating {
            exchange: SaslExchange::ScramPending,
            mechanism,
            pending_token_expiry_ms: _,
        } = auth
        {
            let mech = *mechanism;
            let Some(client_first) = ClientFirst::parse(&req.auth_bytes) else {
                return fail_authenticate("malformed SCRAM client-first");
            };
            let Some(username) = decode_sasl_name(client_first.sasl_name) else {
                return fail_authenticate("SCRAM username has an invalid `=` escape");
            };

            // Kafka's `ScramSaslServer` picks the credential store from the
            // `tokenauth` extension alone: with it the name is a delegation-token
            // id and only the token cache is read, without it only the SCRAM user
            // store is. A token carries a credential for every SCRAM mechanism
            // (`DelegationTokenManager.prepareScramCredentials`).
            let image = controller.current_image();
            let token_requested = client_first.token_authenticated();
            let regular = if token_requested {
                None
            } else {
                image.scram_credential(&username, mech)
            };
            // Kafka keeps its token cache only when `delegation.token.secret.key`
            // is set, so a token is found only next to the key that recomputes
            // its password.
            let token = secret_key.filter(|_| token_requested).and_then(|secret| {
                image
                    .delegation_token_by_id(&username)
                    .map(|token| (token, secret))
            });
            let token_active = token.is_some_and(|(token, _)| {
                krabka_verified::token_is_active(
                    now_ms(),
                    token.expiry_timestamp_ms,
                    token.max_timestamp_ms,
                )
            });
            // The verified selector's `token_mechanism` input is whether the
            // token store may be read, which is the `tokenauth` extension.
            let (cred, principal, token_expiry_ms) = match scram_credential_source(
                regular.is_some(),
                token_requested,
                token.is_some(),
                token_active,
            ) {
                ScramCredentialSource::Regular => (
                    regular.expect("verified regular source exists").clone(),
                    Principal {
                        name: username.clone(),
                        auth_method: krabka_security::AuthMethod::from_sasl(mech),
                        groups: vec![],
                    },
                    None,
                ),
                ScramCredentialSource::DelegationToken => {
                    let (token, secret) = token.expect("verified token source exists");
                    let owner = Principal {
                        name: token.owner.name.clone(),
                        auth_method: krabka_security::AuthMethod::from_sasl(mech),
                        groups: vec![],
                    };
                    (
                        synthesize_token_scram_credential(token, secret, mech),
                        owner,
                        Some(token.expiry_timestamp_ms),
                    )
                }
                ScramCredentialSource::ExpiredDelegationToken => {
                    return fail_authenticate("delegation token expired");
                }
                ScramCredentialSource::Unknown => {
                    return fail_authenticate("unknown user");
                }
            };

            // Kafka checks the GS2 authorization id only once a credential is
            // found, and this refusal is a `SaslAuthenticationException`, so its
            // text reaches the client.
            if client_first
                .authorization_id
                .is_some_and(|authzid| authzid != username)
            {
                return fail_authenticate_with(AUTHORIZATION_ID_MISMATCH.to_owned());
            }

            // The exchange decodes the `n=` value and checks it against its
            // username, as `ScramSaslServer` does with `ScramFormatter.username`,
            // so it gets the decoded name.
            let server = ScramServerExchange::new_with_principal(username, cred, principal);
            // Feed the same client-first bytes; on success the exchange emits
            // the server-first message and yields the next phase.
            match server.step(&req.auth_bytes) {
                krabka_security::StepResult::Continue(bytes, next) => {
                    *auth = ConnectionAuth::Negotiating {
                        mechanism: mech,
                        exchange: SaslExchange::Scram(Box::new(next)),
                        // Side-channel — `Some` here is the
                        // unambiguous "this is a token-authed session"
                        // signal that the round-2 success arm consumes
                        // to set `Authenticated.authenticated_via_token`
                        // + `expires_at_ms`.
                        pending_token_expiry_ms: token_expiry_ms,
                    };
                    sasl_ok(bytes, 0)
                }
                // Done on the first round would be a server bug — SCRAM is
                // always two round trips for SHA-512. Treat as auth failure.
                krabka_security::StepResult::Done(_, _) => {
                    fail_authenticate("SCRAM server completed in one round")
                }
                krabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM step failed"),
            }
        } else if let ConnectionAuth::Negotiating {
            exchange: SaslExchange::Scram(_),
            ..
        } = auth
        {
            // Round 2: exchange already exists. `step` consumes the exchange, so
            // extract it by value (mirroring `handle_handshake`'s re-auth
            // snapshot swap) before stepping it with the client-final bytes; on
            // success extract the principal + server-final bytes and transition
            // to `Authenticated`.
            let ConnectionAuth::Negotiating {
                mechanism,
                exchange: SaslExchange::Scram(server),
                pending_token_expiry_ms,
            } = std::mem::replace(auth, ConnectionAuth::Anonymous)
            else {
                unreachable!("matched Negotiating{{Scram}} above");
            };
            match server.step(&req.auth_bytes) {
                krabka_security::StepResult::Continue(_, _) => {
                    // Two-round SCRAM-SHA-512: an extra `Continue` here is a bug.
                    fail_authenticate("SCRAM second round expected Done")
                }
                krabka_security::StepResult::Done(principal, bytes) => {
                    // When round-1 fell back to a delegation
                    // token, `pending_token_expiry_ms` is `Some(expiry)`
                    // — its presence is both the marker for
                    // `authenticated_via_token: true` and the value of
                    // `expires_at_ms` (the KIP-368 re-auth ceiling).
                    // For regular SCRAM, it's `None` and the
                    // session has no expiry.
                    let now = now_ms();
                    if pending_token_expiry_ms
                        .is_some_and(|e| !krabka_verified::token_is_active(now, e, e))
                    {
                        return fail_authenticate("delegation token expired");
                    }
                    let (expires_at_ms, session_lifetime_ms) =
                        session_expiry(now, pending_token_expiry_ms, max_reauth);
                    *auth = ConnectionAuth::Authenticated {
                        principal,
                        mechanism,
                        expires_at_ms,
                        authenticated_via_token: pending_token_expiry_ms.is_some(),
                    };
                    sasl_ok(bytes, session_lifetime_ms)
                }
                krabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM proof failed"),
            }
        } else {
            fail_authenticate("not in SCRAM negotiation")
        }
    })
}

/// The iteration count of a delegation token's SCRAM credential: Kafka's
/// `DelegationTokenManager.prepareScramCredentials` uses each mechanism's
/// `minIterations`, which is 4096 for both SCRAM mechanisms.
const TOKEN_SCRAM_ITERS: u32 = 4096;

/// Kafka's `ScramSaslServer` message when the GS2 header names an
/// authorization id other than the authenticating user.
const AUTHORIZATION_ID_MISMATCH: &str =
    "Authentication failed: Client requested an authorization id that is different from username";

/// Builds the SCRAM credential of a delegation token for `mechanism`.
///
/// The password is the base64 of the token HMAC under `secret_key`, the value
/// `CreateDelegationToken` returns and clients present as the SCRAM password.
/// The salt is the token id's bytes: the client reads the salt from the
/// server-first message, so a fixed salt works as well as Kafka's random one,
/// and deriving it keeps the credential out of the metadata image.
fn synthesize_token_scram_credential(
    token: &krabka_metadata::DelegationToken,
    secret_key: &SecretBytes,
    mechanism: SaslMechanism,
) -> krabka_security::ScramCredential {
    use base64::Engine;
    let password = base64::engine::general_purpose::STANDARD
        .encode(compute_token_hmac(secret_key.as_bytes(), &token.token_id));
    let salt = token.token_id.as_bytes().to_vec();
    krabka_security::scram::hash_scram_password_with_salt(
        password.as_bytes(),
        mechanism,
        TOKEN_SCRAM_ITERS,
        salt,
    )
}

/// A SCRAM client-first message, parsed as Kafka's
/// `ScramMessages.ClientFirstMessage` does:
/// `n,[a=<authzid>],[m=<reserved>,]n=<saslname>,r=<nonce>[,<key>=<value>]*`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientFirst<'a> {
    /// The GS2 `a=` value as sent, or `None` when the header has none.
    authorization_id: Option<&'a str>,
    /// The `n=` value, still `=2C`/`=3D` encoded.
    sasl_name: &'a str,
    /// The extensions after the nonce, in the order sent.
    extensions: Vec<(&'a str, &'a str)>,
}

impl<'a> ClientFirst<'a> {
    /// Parses `bytes`, or returns `None` where Kafka's pattern does not match.
    fn parse(bytes: &'a [u8]) -> Option<Self> {
        let message = std::str::from_utf8(bytes).ok()?;
        let rest = message.strip_prefix("n,")?;
        let (authorization_id, rest) = match rest.strip_prefix("a=") {
            Some(after) => {
                let (authzid, rest) = after.split_once(',')?;
                (Some(is_sasl_name(authzid).then_some(authzid)?), rest)
            }
            None => (None, rest.strip_prefix(',')?),
        };
        let rest = match rest.strip_prefix("m=") {
            Some(after) => {
                let (reserved, rest) = after.split_once(',')?;
                if !is_value(reserved) {
                    return None;
                }
                rest
            }
            None => rest,
        };
        let mut attributes = rest.split(',');
        let sasl_name = attributes.next()?.strip_prefix("n=")?;
        let nonce = attributes.next()?.strip_prefix("r=")?;
        if !is_sasl_name(sasl_name) || !is_printable(nonce) {
            return None;
        }
        let extensions = attributes
            .map(|extension| {
                let (key, value) = extension.split_once('=')?;
                (!key.is_empty() && key.bytes().all(|b| b.is_ascii_alphabetic()) && is_value(value))
                    .then_some((key, value))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            authorization_id,
            sasl_name,
            extensions,
        })
    }

    /// `ScramExtensions.tokenAuthenticated`: Java's `Boolean.parseBoolean` of
    /// the `tokenauth` extension, the last one sent winning as it does in
    /// Kafka's `HashMap`.
    fn token_authenticated(&self) -> bool {
        self.extensions
            .iter()
            .rev()
            .find(|(key, _)| *key == "tokenauth")
            .is_some_and(|(_, value)| value.eq_ignore_ascii_case("true"))
    }
}

/// Kafka's `SASLNAME`: one or more ASCII characters other than NUL, `=` and
/// `,`, or the escapes `=2C` and `=3D`.
fn is_sasl_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'=' if matches!(bytes.get(i + 1..i + 3), Some(b"2C" | b"3D")) => i += 3,
            b'=' | b',' | 0 | 0x80.. => return false,
            _ => i += 1,
        }
    }
    !bytes.is_empty()
}

/// Kafka's `VALUE`: one or more ASCII characters other than NUL and `,`.
fn is_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| (0x01..=0x7F).contains(&b) && b != b',')
}

/// Kafka's `PRINTABLE`: one or more of `0x21..=0x7E` other than `,`.
fn is_printable(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| (0x21..=0x7E).contains(&b) && b != b',')
}

/// `ScramFormatter.username`: turns `=2C` into `,` and `=3D` into `=`, and
/// refuses any other `=`.
fn decode_sasl_name(sasl_name: &str) -> Option<String> {
    let commas = sasl_name.replace("=2C", ",");
    if commas.replace("=3D", "").contains('=') {
        return None;
    }
    Some(commas.replace("=3D", "="))
}

#[cfg(test)]
mod tests;
