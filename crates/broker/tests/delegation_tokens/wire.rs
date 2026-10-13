//! Raw-socket plumbing for the KIP-48 suite: the length-prefixed
//! request/response framing and the two SASL handshake drivers that every
//! delegation-token step runs over.
//!
//! The framing and the PLAIN driver are the shared [`kafka_wire`] helpers, and
//! the SCRAM driver has the same wire shape as the one in
//! `auth_handlers/scram.rs`. Both drivers differ from `auth_handlers` in one way
//! that matters here: each returns the still-open `TcpStream`, so a caller can
//! send admin RPCs on the session it just authenticated.

use {
    crate::{
        kafka_wire::{self, PlainAuthenticationSetup, exchange},
        rpc::send_create_delegation_token,
    },
    base64::Engine,
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    krabka_security::SaslMechanism,
    std::{io, net::SocketAddr},
    tokio::net::TcpStream,
};

// ─────────────────────────────────────────────────────────────────────────────
// Wire framing (length-prefixed request/response), bound to this suite's
// client id.
// ─────────────────────────────────────────────────────────────────────────────

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-deltok-test";

// One length-prefixed request/response exchange, bound to this suite.
crate::socket_round_trip_fixture!(pub(crate) round_trip, CLIENT_ID);

// ─────────────────────────────────────────────────────────────────────────────
// SASL handshake drivers. Both walk ApiVersions → SaslHandshake →
// SaslAuthenticate on a fresh TcpStream and return the still-open stream
// for follow-up requests.
// ─────────────────────────────────────────────────────────────────────────────

/// Connects and authenticates with SASL/PLAIN; see
/// [`kafka_wire::sasl_plain_authenticate`].
pub(crate) async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> io::Result<TcpStream> {
    kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (password).to_vec(),
    })
    .await
}

/// SCRAM-SHA-256 delegation-token driver. It has the same wire shape as
/// `auth_handlers/scram.rs::drive_sasl_scram_session`, but it returns the open
/// connection on success, which step (c) needs, and its client-first message
/// carries the `tokenauth=true` extension that Kafka's `ScramLoginModule`
/// sends for a token, so `username` is a token id and `password` its HMAC.
pub(crate) async fn sasl_scram_sha256_authenticate(
    addr: SocketAddr,
    username: &str,
    password: &str,
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let _av_resp: ApiVersionsResponse = exchange()
        .stream(&mut stream)
        .request(&ApiVersionsRequest::default())
        .client_id(CLIENT_ID)
        .call()
        .await?;

    let sh_resp: SaslHandshakeResponse = exchange()
        .stream(&mut stream)
        .request(&SaslHandshakeRequest {
            mechanism: "SCRAM-SHA-256".to_string(),
            ..Default::default()
        })
        .api_key(17)
        .version(1)
        .correlation(2)
        .client_id(CLIENT_ID)
        .call()
        .await?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake(SCRAM-SHA-256) failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let client = TokenScramClient::new(username, password);
    let client_first = client.client_first();

    let r1_resp = kafka_wire::sasl_authenticate_on()
        .stream(&mut stream)
        .auth_bytes(bytes::Bytes::from(client_first))
        .client_id(CLIENT_ID)
        .call()
        .await?;
    if r1_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 1 failed: code={} msg={:?}",
            r1_resp.error_code, r1_resp.error_message
        )));
    }

    let client_final = client.client_final(&r1_resp.auth_bytes)?;
    let r2_resp = kafka_wire::sasl_authenticate_on()
        .stream(&mut stream)
        .auth_bytes(bytes::Bytes::from(client_final))
        .client_id(CLIENT_ID)
        .correlation(4)
        .call()
        .await?;
    if r2_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 2 failed: code={} msg={:?}",
            r2_resp.error_code, r2_resp.error_message
        )));
    }

    Ok(stream)
}

/// A SCRAM-SHA-256 client that writes the messages Kafka's `ScramSaslClient`
/// writes for a delegation token: `krabka_security::ScramClientExchange` has no
/// way to add the `tokenauth=true` extension, which is part of the signed
/// client-first message.
struct TokenScramClient {
    password: Vec<u8>,
    client_first_bare: String,
}

impl TokenScramClient {
    fn new(token_id: &str, token_hmac: &str) -> Self {
        Self {
            password: token_hmac.as_bytes().to_vec(),
            client_first_bare: format!("n={token_id},r=tokenclientnonce,tokenauth=true"),
        }
    }

    fn client_first(&self) -> Vec<u8> { format!("n,,{}", self.client_first_bare).into_bytes() }

    fn client_final(&self, server_first: &[u8]) -> Result<Vec<u8>, io::Error> {
        use {
            base64::{Engine, engine::general_purpose::STANDARD as B64},
            pbkdf2::{
                hmac::{Hmac, KeyInit, Mac},
                sha2::{Digest, Sha256},
            },
        };

        let hmac = |key: &[u8], data: &[u8]| {
            let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC takes any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        };
        let server_first = std::str::from_utf8(server_first)
            .map_err(|e| io::Error::other(format!("server-first is not UTF-8: {e}")))?;
        let attribute = |name: &str| {
            server_first
                .split(',')
                .find_map(|a| a.strip_prefix(name))
                .ok_or_else(|| io::Error::other(format!("server-first lacks {name}")))
        };
        let nonce = attribute("r=")?;
        let salt = B64
            .decode(attribute("s=")?)
            .map_err(|e| io::Error::other(format!("server-first salt: {e}")))?;
        let iterations: u32 = attribute("i=")?
            .parse()
            .map_err(|e| io::Error::other(format!("server-first iterations: {e}")))?;
        let salted = krabka_security::scram::pbkdf2_salted(
            &self.password,
            SaslMechanism::ScramSha256,
            iterations,
            &salt,
        );
        let without_proof = format!("c={},r={nonce}", B64.encode(b"n,,"));
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_key = hmac(&salted, b"Client Key");
        let signature = hmac(&Sha256::digest(&client_key), auth_message.as_bytes());
        let proof = scram_client_proof(&client_key, &signature);
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }
}

pub(crate) async fn token_session(
    addr: SocketAddr,
    token_id: &str,
    hmac: &[u8],
) -> Result<TcpStream, String> {
    let password = base64::engine::general_purpose::STANDARD.encode(hmac);
    let mut session = sasl_scram_sha256_authenticate(addr, token_id, &password)
        .await
        .map_err(|e| format!("token SCRAM auth: {e}"))?;
    let create = send_create_delegation_token(
        &mut session,
        200,
        &krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        },
    )
    .await
    .map_err(|e| format!("CreateDelegationToken(token-auth): {e}"))?;
    assert2::assert!(
        create.error_code == crate::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); got {} — principal override may have regressed",
        create.error_code
    );
    Ok(session)
}

krabka_macros::scram_client_proof_fixture!(scram_client_proof);

/// Open the suite's super-user PLAIN session with its shared authentication diagnostic.
pub async fn admin_plain(addr: SocketAddr) -> Result<TcpStream, String> {
    sasl_plain_authenticate(addr, "admin", b"admin-pw")
        .await
        .map_err(|error| format!("admin PLAIN auth: {error}"))
}
