//! `AlterUserScramCredentials` (`api_key` 51, KIP-554) provisioning.
//!
//! A super-user upserts a SCRAM-SHA-256 or SCRAM-SHA-512 credential and the
//! named user then authenticates with it, while a non-super-user is refused
//! per row. The module also owns the KIP-554 wire constants, the
//! PLAIN-authenticated request drive, and the PBKDF2 salting that the
//! validation cases in `alter_scram_validation` reuse.

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use krabka_broker::Broker;
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::owned::{
    alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
    api_versions_request::ApiVersionsRequest, sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::SaslMechanism;
use tokio::net::TcpStream;

use crate::{
    harness::{
        admin_plain_password, alice_password, round_trip, scram_admin_fixture, wrong_scram_password,
    },
    kafka_wire::{
        SaslHandshakeSetup, decode_named, encode_named, plain_payload, sasl_handshake_on,
    },
    scram::drive_sasl_scram_session,
    support::{
        acl::use_simple_acl_authorizer,
        sasl::{ScramAlgorithm, ScramUpsertionSetup, sasl_temp_config, scram_upsertion},
        wire::{
            CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
            SocketExchangeSetup,
        },
    },
};

/// SCRAM mechanism byte on the `AlterUserScramCredentials` wire, from
/// KIP-554. `1` is `SCRAM-SHA-256` and `2` is `SCRAM-SHA-512`.
pub const WIRE_MECH_SCRAM_SHA_256: i8 = 1;
pub const WIRE_MECH_SCRAM_SHA_512: i8 = 2;
pub const KAFKA_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
pub const KAFKA_UNACCEPTABLE_CREDENTIAL: i16 = 93;
pub const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Happy path: a super-user authenticates over PLAIN, sends an
/// `AlterUserScramCredentials` upsertion for `alice`, and the broker stores
/// the credential.
///
/// The test then authenticates as `alice` over SCRAM-SHA-512. This proves
/// that the upsertion wrote a valid credential to the metadata image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision() {
    let (_log_dir, handle, addr) =
        scram_admin_fixture(vec![SaslMechanism::Plain, SaslMechanism::ScramSha512]).await;

    let req = alice_sha512_request();
    let resp = provision_as_admin(addr, req, "PLAIN auth + AUSCR upsertion").await;
    assert!(resp.results.len() == 1, "one result row per upsertion");
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Round-trip: now log in as `alice` over SCRAM, proving the upserted
    // credential actually reached the metadata image. Wait for the raft
    // commit to land the credential in the committed image, then auth.
    let result = post_upsertion_auth(&handle, addr, SaslMechanism::ScramSha512).await;
    handle.shutdown().await;
    result.expect("post-upsertion SCRAM auth must succeed");
}

/// Wire-mapping proof: `AlterUserScramCredentials` accepts `mechanism=1`,
/// which is SCRAM-SHA-256, and stores a credential.
///
/// The broker can later authenticate against that credential over SHA-256.
/// The test is a copy of `alter_scram_creds_super_user_can_provision`, but it
/// uses the SHA-256 wire byte and a 32-byte `salted_password` payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision_sha256() {
    let (_log_dir, handle, addr) =
        scram_admin_fixture(vec![SaslMechanism::Plain, SaslMechanism::ScramSha256]).await;

    let (salt, salted) = pbkdf2_salt_and_salted_sha256(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![scram_upsertion(ScramUpsertionSetup {
            mechanism: ScramAlgorithm::Sha256,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        })],
        ..Default::default()
    };
    let resp = provision_as_admin(addr, req, "PLAIN auth + AUSCR upsertion (SHA-256)").await;
    assert!(resp.results.len() == 1);
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Wait for the upserted credential to reach the committed metadata
    // image, then authenticate as `alice` over SHA-256 SCRAM.
    let result = post_upsertion_auth(&handle, addr, SaslMechanism::ScramSha256).await;
    handle.shutdown().await;
    result.expect("post-upsertion SHA-256 SCRAM auth must succeed");
}

/// A non-super-user authenticates and tries to upsert.
///
/// The broker accepts the request, because it is a valid SASL listener API.
/// But every per-user row reports `CLUSTER_AUTHORIZATION_FAILED` (31). The
/// broker makes no metadata change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_non_super_user_rejected() {
    let (_log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("bob".to_string(), wrong_scram_password());
    cfg.super_users = maplit::hashset! {"admin".to_string()};
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; the default `AllowAllAuthorizer`
    // would let alice through.
    use_simple_acl_authorizer(&mut cfg);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let req = alice_sha512_request();
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "bob",
        wrong_scram_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR (rejected)");
    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == 31, // CLUSTER_AUTHORIZATION_FAILED
        "non-super-user must get CLUSTER_AUTHORIZATION_FAILED, got {:?}",
        resp.results[0]
    );
}

/// Authenticate over SASL/PLAIN against `addr` as `user`/`password`, send one
/// `AlterUserScramCredentials v0` request, and decode the response.
///
/// The request uses `api_key` 51 and is flexible. Every T15 test case calls
/// this helper, so the SASL boilerplate stays in one place.
pub async fn drive_alter_user_scram_credentials_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: AlterUserScramCredentialsRequest,
) -> Result<AlterUserScramCredentialsResponse, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
    let av_req = ApiVersionsRequest::default();
    let av_body = encode_named(&av_req, 0, "ApiVersions")?;
    let _ = round_trip(&mut stream, SocketExchangeSetup::default(), &av_body).await?;

    // ── 2. SaslHandshake v1.
    sasl_handshake_on(
        &mut stream,
        SaslHandshakeSetup {
            client_id: "krabka-sasl-test",
            ..Default::default()
        },
    )
    .await?;

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let auth_body = encode_named(
        &SaslAuthenticateRequest {
            auth_bytes: plain_payload(user, password),
            ..Default::default()
        },
        2,
        "SaslAuthenticate",
    )?;
    let auth_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(3),
            header: RequestHeaderEncoding::Flexible,
        },
        &auth_body,
    )
    .await?;
    let auth_resp =
        decode_named::<SaslAuthenticateResponse>(&auth_resp_bytes, 2, "SaslAuthenticate")?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={}",
            auth_resp.error_code
        )));
    }

    // ── 4. AlterUserScramCredentials v0 (api_key 51, flexible from v0).
    let auscr_body = encode_named(&req, 0, "AUSCR")?;
    let auscr_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(51),
            correlation: WireCorrelationId(4),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &auscr_body,
    )
    .await?;
    decode_named::<AlterUserScramCredentialsResponse>(&auscr_resp_bytes, 0, "AUSCR")
}

/// Compute `(salt, salted_password)` for a SCRAM-SHA-512 wire upsertion.
///
/// The salt is a fixed 16-byte vector, which keeps the test deterministic.
/// The salted password is the 64-byte PBKDF2-HMAC-SHA-512 output that the
/// KIP-554 wire request carries.
pub fn pbkdf2_salt_and_salted(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 64]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 64] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, iterations);
    (salt, salted)
}

/// SHA-256 analog of [`pbkdf2_salt_and_salted`].
///
/// It produces the 32-byte PBKDF2-HMAC-SHA-256 output for the wire tests.
fn pbkdf2_salt_and_salted_sha256(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 32]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 32] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, iterations);
    (salt, salted)
}

/// The same deterministic Alice credential for the allow and deny provisioning paths.
fn alice_sha512_request() -> AlterUserScramCredentialsRequest {
    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    AlterUserScramCredentialsRequest {
        upsertions: vec![scram_upsertion(ScramUpsertionSetup {
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        })],
        ..Default::default()
    }
}

/// Send an upsertion as the fixture's PLAIN super-user.
pub async fn provision_as_admin(
    addr: SocketAddr,
    request: AlterUserScramCredentialsRequest,
    context: &str,
) -> AlterUserScramCredentialsResponse {
    drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        request,
    )
    .await
    .expect(context)
}

async fn post_upsertion_auth(
    broker: &krabka_broker::BrokerHandle,
    addr: SocketAddr,
    mechanism: SaslMechanism,
) -> io::Result<()> {
    broker
        .wait_for_image(|image| image.scram_credential("alice", mechanism).is_some())
        .await;
    drive_sasl_scram_session(addr, "alice", &alice_password(), mechanism).await
}
