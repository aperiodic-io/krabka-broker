//! SCRAM handler tests: Kafka's client-first grammar, the `tokenauth`
//! credential-store selection for both mechanisms, `=2C`/`=3D` name decoding
//! and the GS2 authorization-id check.
//!
//! The exchanges run against a [`FakeMetadataSource`] seeded with SCRAM users
//! and delegation tokens, driven by [`KafkaScramClient`], which builds the
//! client messages the way Kafka's `ScramSaslClient` does, extensions and
//! authorization id included.

use {
    super::{ClientFirst, ScramExtension, decode_sasl_name},
    crate::{
        network::auth::{
            ConnectionAuth, handle_authenticate_scram, state::SaslExchange,
            test_support::assert_failed_authenticate_response,
        },
        test_support::FakeMetadataSource,
        time_util::now_ms,
    },
    assert2::{assert, check},
    base64::{Engine, engine::general_purpose::STANDARD as B64},
    krabka_metadata::{DelegationTokenRecord, MetadataRecord, ScramCredentialRecord},
    krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    },
    krabka_security::{
        AuthMethod, KafkaPrincipal, Principal, SaslMechanism, SecretBytes, compute_token_hmac,
        scram::hash_scram_password_with_salt,
    },
    pbkdf2::{
        hmac::{Hmac, KeyInit, Mac},
        sha2::{Digest, Sha256, Sha512},
    },
};

krabka_macros::scram_client_proof_fixture!(scram_client_proof);

const USER_PASSWORD: &str = "user-password";
/// The broker's `delegation.token.secret.key` in these tests.
const TOKEN_SECRET_KEY: &[u8] = b"token-secret-key";
const TOKEN_OWNER: &str = "owner";

fn token_secret() -> SecretBytes { SecretBytes::new(TOKEN_SECRET_KEY.to_vec()) }

/// A client and the client-first message it wrote.
struct FirstMessage {
    client: KafkaScramClient,
    message: Vec<u8>,
}

/// A SCRAM client that writes the messages Kafka's `ScramSaslClient` writes.
struct KafkaScramClient {
    mechanism: SaslMechanism,
    password: Vec<u8>,
    gs2_header: String,
    client_first_bare: String,
}

impl KafkaScramClient {
    /// Returns the client and its client-first message.
    fn first(
        mechanism: SaslMechanism,
        authorization_id: Option<&str>,
        sasl_name: &str,
        extensions: &str,
        password: &[u8],
    ) -> FirstMessage {
        let gs2_header = format!(
            "n,{},",
            authorization_id.map_or(String::new(), |a| format!("a={a}"))
        );
        let client_first_bare = format!("n={sasl_name},r=clientnonce{extensions}");
        let message = format!("{gs2_header}{client_first_bare}").into_bytes();
        let client = Self {
            mechanism,
            password: password.to_vec(),
            gs2_header,
            client_first_bare,
        };
        FirstMessage { client, message }
    }

    /// The client-final message for `server_first`.
    fn last(&self, server_first: &[u8]) -> Vec<u8> {
        let server_first = std::str::from_utf8(server_first).expect("server-first is UTF-8");
        let attribute = |name: &str| {
            server_first
                .split(',')
                .find_map(|a| a.strip_prefix(name))
                .expect("server-first attribute")
        };
        let nonce = attribute("r=");
        let salt = B64.decode(attribute("s=")).expect("salt is base64");
        let iterations: u32 = attribute("i=").parse().expect("iterations");
        let salted = krabka_security::scram::pbkdf2_salted(
            &self.password,
            self.mechanism,
            iterations,
            &salt,
        );
        let without_proof = format!("c={},r={nonce}", B64.encode(&self.gs2_header));
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_key = self.hmac(&salted, b"Client Key");
        let stored_key = self.hash(&client_key);
        let signature = self.hmac(&stored_key, auth_message.as_bytes());
        let proof = scram_client_proof(&client_key, &signature);
        format!("{without_proof},p={}", B64.encode(proof)).into_bytes()
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        if self.mechanism == SaslMechanism::ScramSha256 {
            let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        } else {
            let mut mac = <Hmac<Sha512>>::new_from_slice(key).expect("any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    }

    fn hash(&self, data: &[u8]) -> Vec<u8> {
        if self.mechanism == SaslMechanism::ScramSha256 {
            Sha256::digest(data).to_vec()
        } else {
            Sha512::digest(data).to_vec()
        }
    }
}

fn scram_user(user: &str, mechanism: SaslMechanism) -> MetadataRecord {
    let credential =
        hash_scram_password_with_salt(USER_PASSWORD.as_bytes(), mechanism, 4096, b"salt".to_vec());
    MetadataRecord::V1ScramCredential(ScramCredentialRecord {
        user: user.into(),
        mechanism,
        salt: credential.salt,
        stored_key: credential.stored_key,
        server_key: credential.server_key,
        iterations: credential.iterations,
    })
}

fn token(token_id: &str, expiry_timestamp_ms: i64) -> MetadataRecord {
    MetadataRecord::V1DelegationToken(DelegationTokenRecord {
        token_id: token_id.into(),
        owner: KafkaPrincipal {
            principal_type: "User".into(),
            name: TOKEN_OWNER.into(),
        },
        requester: KafkaPrincipal {
            principal_type: "User".into(),
            name: TOKEN_OWNER.into(),
        },
        issue_timestamp_ms: 0,
        expiry_timestamp_ms,
        max_timestamp_ms: expiry_timestamp_ms,
        renewers: vec![],
    })
}

/// A token's password: the base64 of its HMAC under `secret_key`, which
/// `CreateDelegationToken` hands to the client as raw HMAC bytes.
fn token_password_under(secret_key: &[u8], token_id: &str) -> Vec<u8> {
    B64.encode(compute_token_hmac(secret_key, token_id))
        .into_bytes()
}

fn token_password(token_id: &str) -> Vec<u8> { token_password_under(TOKEN_SECRET_KEY, token_id) }

fn authenticate(
    source: &FakeMetadataSource,
    secret_key: Option<&SecretBytes>,
    auth: &mut ConnectionAuth,
    auth_bytes: Vec<u8>,
) -> SaslAuthenticateResponse {
    handle_authenticate_scram(
        &SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(auth_bytes),
            ..Default::default()
        },
        auth,
        source,
        secret_key,
        None,
    )
}

/// What one full exchange came to.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Round 2 authenticated `principal`.
    Authenticated {
        principal: Principal,
        via_token: bool,
    },
    /// Round 1 failed with this client-facing message (`None` is Kafka's
    /// generic text, filled in at dispatch).
    FirstRoundFailed(Option<String>),
    /// Round 2 refused the proof.
    ProofRefused,
}

/// Drives both rounds, stopping at the first failure.
fn exchange(
    source: &FakeMetadataSource,
    secret_key: Option<&SecretBytes>,
    mechanism: SaslMechanism,
    authorization_id: Option<&str>,
    sasl_name: &str,
    extensions: &str,
    password: &[u8],
) -> Outcome {
    let mut auth = ConnectionAuth::Negotiating {
        mechanism,
        exchange: SaslExchange::ScramPending,
        pending_token_expiry_ms: None,
    };
    let FirstMessage {
        client,
        message: first,
    } = KafkaScramClient::first(mechanism, authorization_id, sasl_name, extensions, password);
    let round1 = authenticate(source, secret_key, &mut auth, first);
    if round1.error_code != 0 {
        assert_failed_authenticate_response(&round1, round1.error_message.as_deref());
        return Outcome::FirstRoundFailed(round1.error_message);
    }
    let round2 = authenticate(
        source,
        secret_key,
        &mut auth,
        client.last(&round1.auth_bytes),
    );
    if round2.error_code != 0 {
        assert_failed_authenticate_response(&round2, None);
        return Outcome::ProofRefused;
    }
    match auth {
        ConnectionAuth::Authenticated {
            principal,
            mechanism: authenticated_mechanism,
            authenticated_via_token,
            ..
        } => {
            assert!(authenticated_mechanism == mechanism);
            Outcome::Authenticated {
                principal,
                via_token: authenticated_via_token,
            }
        }
        other => panic!("round 2 succeeded without authenticating: {other:?}"),
    }
}

fn principal(name: &str, mechanism: SaslMechanism) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::from_sasl(mechanism),
        groups: vec![],
    }
}

/// One exchange row: case name, GS2 authorization id, SASL name, extensions,
/// password and the expected outcome.
struct ExchangeCase<'a> {
    case: &'a str,
    authorization_id: Option<&'a str>,
    sasl_name: &'a str,
    extensions: &'a str,
    password: &'a [u8],
    expected: Outcome,
}

/// Kafka's `ScramSaslServer` reads the token store if and only if the
/// client-first message carries `tokenauth=true`, for both mechanisms.
#[test]
fn tokenauth_extension_selects_the_credential_store() {
    const TOKEN_AUTH: &str = ",tokenauth=true";
    let expiry = now_ms() + 60_000;
    for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
        let source = FakeMetadataSource::builder()
            .records(&[
                scram_user("alice", mechanism),
                // A token whose id is also a SCRAM user name.
                scram_user("shared", mechanism),
                token("shared", expiry),
                token("tok", expiry),
                token("expired", now_ms() - 1),
                // `a,b=c` on the wire is `a=2Cb=3Dc`.
                scram_user("a,b=c", mechanism),
            ])
            .build();
        let user = USER_PASSWORD.as_bytes();
        let secret = token_secret();
        let token_pw = token_password("tok");
        let shared_pw = token_password("shared");
        let expired_pw = token_password("expired");
        let owner = Outcome::Authenticated {
            principal: principal(TOKEN_OWNER, mechanism),
            via_token: true,
        };
        let generic_failure = Outcome::FirstRoundFailed(None);
        // (case, authzid, sasl name, extensions, password, expected)
        let cases: [ExchangeCase<'_>; 15] = [
            ExchangeCase {
                case: "SCRAM user without extension",
                authorization_id: None,
                sasl_name: "alice",
                extensions: "",
                password: user,
                expected: Outcome::Authenticated {
                    principal: principal("alice", mechanism),
                    via_token: false,
                },
            },
            ExchangeCase {
                case: "token with tokenauth",
                authorization_id: None,
                sasl_name: "tok",
                extensions: TOKEN_AUTH,
                password: &token_pw,
                expected: owner,
            },
            ExchangeCase {
                case: "tokenauth parsed as Java parseBoolean",
                authorization_id: None,
                sasl_name: "tok",
                extensions: ",tokenauth=TRUE",
                password: &token_pw,
                expected: Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            },
            ExchangeCase {
                case: "token id without tokenauth",
                authorization_id: None,
                sasl_name: "tok",
                extensions: "",
                password: &token_pw,
                expected: generic_failure,
            },
            ExchangeCase {
                case: "tokenauth=false reads the user store",
                authorization_id: None,
                sasl_name: "tok",
                extensions: ",tokenauth=false",
                password: &token_pw,
                expected: Outcome::FirstRoundFailed(None),
            },
            ExchangeCase {
                case: "SCRAM user with tokenauth",
                authorization_id: None,
                sasl_name: "alice",
                extensions: TOKEN_AUTH,
                password: user,
                expected: Outcome::FirstRoundFailed(None),
            },
            ExchangeCase {
                case: "shared name with tokenauth uses the token",
                authorization_id: None,
                sasl_name: "shared",
                extensions: TOKEN_AUTH,
                password: &shared_pw,
                expected: Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            },
            ExchangeCase {
                case: "shared name with tokenauth refuses the user password",
                authorization_id: None,
                sasl_name: "shared",
                extensions: TOKEN_AUTH,
                password: user,
                expected: Outcome::ProofRefused,
            },
            ExchangeCase {
                case: "shared name without tokenauth uses the user",
                authorization_id: None,
                sasl_name: "shared",
                extensions: "",
                password: user,
                expected: Outcome::Authenticated {
                    principal: principal("shared", mechanism),
                    via_token: false,
                },
            },
            ExchangeCase {
                case: "expired token",
                authorization_id: None,
                sasl_name: "expired",
                extensions: TOKEN_AUTH,
                password: &expired_pw,
                expected: Outcome::FirstRoundFailed(None),
            },
            ExchangeCase {
                case: "escaped SASL name",
                authorization_id: None,
                sasl_name: "a=2Cb=3Dc",
                extensions: "",
                password: user,
                expected: Outcome::Authenticated {
                    principal: principal("a,b=c", mechanism),
                    via_token: false,
                },
            },
            ExchangeCase {
                case: "authorization id equal to the user",
                authorization_id: Some("alice"),
                sasl_name: "alice",
                extensions: "",
                password: user,
                expected: Outcome::Authenticated {
                    principal: principal("alice", mechanism),
                    via_token: false,
                },
            },
            // `ScramSaslServer` compares the authzid against the token id,
            // the SCRAM user name, not against the token's owner.
            ExchangeCase {
                case: "authorization id equal to the token id",
                authorization_id: Some("tok"),
                sasl_name: "tok",
                extensions: TOKEN_AUTH,
                password: &token_pw,
                expected: Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            },
            // Kafka compares the authzid as sent, still `=2C`/`=3D` encoded,
            // with the decoded user name, so an escaped name never matches.
            ExchangeCase {
                case: "escaped authorization id against the decoded user",
                authorization_id: Some("a=2Cb=3Dc"),
                sasl_name: "a=2Cb=3Dc",
                extensions: "",
                password: user,
                expected: Outcome::FirstRoundFailed(Some(
                    "Authentication failed: Client requested an authorization id that is \
                     different from username"
                        .into(),
                )),
            },
            ExchangeCase {
                case: "authorization id other than the user",
                authorization_id: Some("mallory"),
                sasl_name: "alice",
                extensions: "",
                password: user,
                expected: Outcome::FirstRoundFailed(Some(
                    "Authentication failed: Client requested an authorization id that is \
                     different from username"
                        .into(),
                )),
            },
        ];
        for ExchangeCase {
            case,
            authorization_id,
            sasl_name,
            extensions,
            password,
            expected,
        } in cases
        {
            check!(
                exchange(
                    &source,
                    Some(&secret),
                    mechanism,
                    authorization_id,
                    sasl_name,
                    extensions,
                    password
                ) == expected,
                "{mechanism:?}: {case}"
            );
        }
    }
}

/// A token session carries the token expiry from round 1 to round 2.
#[test]
fn token_round_one_threads_the_token_expiry() {
    let ActiveTokenSource { expiry, source } = active_token_source();
    let mut auth = ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::ScramSha512,
        exchange: SaslExchange::ScramPending,
        pending_token_expiry_ms: None,
    };
    let FirstMessage {
        client,
        message: first,
    } = KafkaScramClient::first(
        SaslMechanism::ScramSha512,
        None,
        "tok",
        ",tokenauth=true",
        &token_password("tok"),
    );
    let round1 = authenticate(&source, Some(&token_secret()), &mut auth, first);
    assert!(round1.error_code == 0);
    let ConnectionAuth::Negotiating {
        pending_token_expiry_ms,
        ..
    } = &auth
    else {
        panic!("round 1 must keep negotiating, got {auth:?}");
    };
    assert!(*pending_token_expiry_ms == Some(expiry));
    let before = now_ms();
    let round2 = authenticate(
        &source,
        Some(&token_secret()),
        &mut auth,
        client.last(&round1.auth_bytes),
    );
    let after = now_ms();
    assert!(round2.error_code == 0);
    assert!(matches!(&auth, ConnectionAuth::Authenticated {
        expires_at_ms: Some(at), authenticated_via_token: true, ..
    } if *at == expiry));
    assert!(
        expiry - after <= round2.session_lifetime_ms
            && round2.session_lifetime_ms <= expiry - before
    );
    assert!(!auth.expired_for_request(0, expiry - 1));
    assert!(auth.expired_for_request(0, expiry));
    assert!(!auth.expired_for_request(17, expiry));
    assert!(
        auth.token_api_admission(krabka_verified::delegation_token::TokenApi::Describe)
            == krabka_verified::delegation_token::TokenApiAdmission::Reject
    );
}

/// The image keeps no token HMAC, as Kafka's metadata does not: a token's
/// SCRAM password is recomputed from the broker's secret key, so with no key
/// configured no token authenticates, and a password computed under another
/// key fails the proof.
#[test]
fn token_password_is_recomputed_from_the_secret_key() {
    let ActiveTokenSource { source, .. } = active_token_source();
    let secret = token_secret();
    for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
        // (case, broker secret key, key the client's HMAC came from, expected)
        let cases = [
            (
                "matching key",
                Some(&secret),
                TOKEN_SECRET_KEY,
                Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            ),
            (
                "no secret key configured",
                None,
                TOKEN_SECRET_KEY,
                Outcome::FirstRoundFailed(None),
            ),
            (
                "password computed under another key",
                Some(&secret),
                b"another-secret-key".as_slice(),
                Outcome::ProofRefused,
            ),
        ];
        for (case, broker_key, client_key, expected) in cases {
            check!(
                exchange(
                    &source,
                    broker_key,
                    mechanism,
                    None,
                    "tok",
                    ",tokenauth=true",
                    &token_password_under(client_key, "tok"),
                ) == expected,
                "{mechanism:?}: {case}"
            );
        }
    }
}

/// `ClientFirst::parse` follows Kafka's `ClientFirstMessage` pattern.
#[test]
fn client_first_parses_as_kafka_does() {
    /// A client-first message and what parsing it gives.
    struct ParseCase<'a> {
        message: &'a str,
        expected: Option<ClientFirst<'a>>,
    }
    let parsed = |authorization_id, sasl_name, extensions| {
        Some(ClientFirst {
            authorization_id,
            sasl_name,
            extensions,
        })
    };
    let cases: [ParseCase<'_>; 11] = [
        ParseCase {
            message: "n,,n=alice,r=abc",
            expected: parsed(None, "alice", vec![]),
        },
        ParseCase {
            message: "n,a=alice,n=alice,r=abc",
            expected: parsed(Some("alice"), "alice", vec![]),
        },
        ParseCase {
            message: "n,,m=ext,n=alice,r=abc",
            expected: parsed(None, "alice", vec![]),
        },
        ParseCase {
            message: "n,,n=tok,r=abc,tokenauth=true,other=x",
            expected: parsed(None, "tok", vec![
                ScramExtension {
                    key: "tokenauth",
                    value: "true",
                },
                ScramExtension {
                    key: "other",
                    value: "x",
                },
            ]),
        },
        ParseCase {
            message: "n,,n=a=2Cb=3Dc,r=abc",
            expected: parsed(None, "a=2Cb=3Dc", vec![]),
        },
        // Only the `n` GS2 flag, a non-empty name and nonce, and alphabetic
        // extension keys match.
        ParseCase {
            message: "y,,n=alice,r=abc",
            expected: None,
        },
        ParseCase {
            message: "n,,n=,r=abc",
            expected: None,
        },
        ParseCase {
            message: "n,,n=alice,r=",
            expected: None,
        },
        ParseCase {
            message: "n,,n=a=41,r=abc",
            expected: None,
        },
        ParseCase {
            message: "n,,n=alice,r=abc,token_auth=true",
            expected: None,
        },
        ParseCase {
            message: "n,,r=abc,n=alice",
            expected: None,
        },
    ];
    for ParseCase { message, expected } in cases {
        check!(
            ClientFirst::parse(message.as_bytes()) == expected,
            "{message}"
        );
    }
}

/// `ScramFormatter.username`.
#[yare::parameterized(
    plain = { "alice", Some("alice") },
    comma_escape = { "a=2Cb", Some("a,b") },
    equals_escape = { "a=3Db", Some("a=b") },
    escaped_equals_followed_by_text = { "=3D2C", Some("=2C") },
    invalid_escape = { "a=41", None },
)]
fn sasl_name_decoding_matches_scram_formatter(sasl_name: &str, expected: Option<&str>) {
    check!(
        decode_sasl_name(sasl_name).as_deref() == expected,
        "{sasl_name}"
    );
}

/// A metadata source holding one active token, and the token's expiry.
struct ActiveTokenSource {
    expiry: i64,
    source: FakeMetadataSource,
}

fn active_token_source() -> ActiveTokenSource {
    let expiry = now_ms() + 60_000;
    let source = FakeMetadataSource::builder()
        .records(&[token("tok", expiry)])
        .build();
    ActiveTokenSource { expiry, source }
}
