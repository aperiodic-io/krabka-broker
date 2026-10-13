//! The audit trail a delegation-token mutation leaves behind.
//!
//! `CreateDelegationToken`, `RenewDelegationToken` and `ExpireDelegationToken`
//! each have to reach the audit topic as an OCSF `AdminOperation` record that
//! names the token they changed. Two of those three are only auditable at all
//! because the broker resolves the token id from the metadata image *before*
//! the mutation: renew and expire address a token by HMAC, neither response
//! echoes an id back, and an expire removes the record the lookup would read.
//!
//! The record names the id and never the HMAC. The HMAC is the token's
//! password equivalent, and the audit topic is a place an auditor reads.
//!
//! These cases need the `SASL_PLAINTEXT` fixture rather than the in-process
//! one the rest of the audit suite uses: the four delegation-token apis are
//! gated on `delegation_token_secret_key`, and they authorize against the
//! connection's own principal rather than against a request context.

use {
    crate::{
        cluster::start_broker,
        rpc::{
            send_create_delegation_token, send_expire_delegation_token, send_renew_delegation_token,
        },
        support,
        wire::sasl_plain_authenticate,
    },
    assert2::{assert, check},
    base64::Engine as _,
    krabka_broker::coordinator::AUDIT_TOPIC,
    krabka_protocol::owned::{
        create_delegation_token_request::CreateDelegationTokenRequest,
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        renew_delegation_token_request::RenewDelegationTokenRequest,
    },
};

/// The OCSF class for an API Activity record.
const API_ACTIVITY: i64 = 6003;
/// The OCSF `status_id` for a success.
const STATUS_SUCCESS: i64 = 1;

/// Does this record report `operation` succeeding on `token_id`, by `actor`?
fn names_token(record: &serde_json::Value, operation: &str, token_id: &str, actor: &str) -> bool {
    record["class_uid"] == API_ACTIVITY
        && record["api"]["operation"] == operation
        && record["status_id"] == STATUS_SUCCESS
        && record["actor"]["user"]["name"] == actor
        && record["resources"][0]["type"] == "DelegationToken"
        && record["resources"][0]["name"] == token_id
}

/// How many records name `operation`, whatever their outcome.
fn records_for(records: &[serde_json::Value], operation: &str) -> usize {
    records
        .iter()
        .filter(|j| j["class_uid"] == API_ACTIVITY && j["api"]["operation"] == operation)
        .count()
}

/// The token secret in the two textual encodings an audit record could leak it
/// in: base64, which is how a token holder types it as a SCRAM password, and
/// lowercase hex, which is how the audit chain renders raw bytes.
fn secret_spellings(hmac: &[u8]) -> Vec<String> {
    let hex = hmac.iter().fold(String::new(), |mut out, b| {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
        out
    });
    vec![base64::engine::general_purpose::STANDARD.encode(hmac), hex]
}

/// Mint, renew and expire one token over a SASL-authenticated connection, and
/// prove each mutation reached the audit topic naming that token's id.
///
/// The expire case is the load-bearing one. Its record can only carry the id
/// if the broker read the id off the metadata image before the delete, so a
/// lookup that moved after the mutation would leave this waiting on a record
/// that never arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delegation_token_mutations_reach_the_audit_topic() {
    let (handle, _dir, addr) = start_broker().await;
    handle.wait_until_partition_present(AUDIT_TOPIC, 0).await;

    let mut alice = sasl_plain_authenticate(addr, "alice", b"wonderland")
        .await
        .expect("alice PLAIN auth");

    let create = send_create_delegation_token(&mut alice, 100, &CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    })
    .await
    .expect("CreateDelegationToken");
    assert!(create.error_code == 0, "{create:?}");
    let token_id = create.token_id.clone();
    let hmac = create.hmac.clone();

    let renew = send_renew_delegation_token(&mut alice, 200, &RenewDelegationTokenRequest {
        hmac: hmac.clone(),
        renew_period_ms: 30 * 24 * 60 * 60 * 1_000,
        ..Default::default()
    })
    .await
    .expect("RenewDelegationToken");
    assert!(renew.error_code == 0, "{renew:?}");

    // A renew against an HMAC no token carries fails, and a failure is not an
    // admin mutation: the count assertion below is what catches an audit call
    // that drifted out from under the success guard.
    let missing = send_renew_delegation_token(&mut alice, 300, &RenewDelegationTokenRequest {
        hmac: bytes::Bytes::from_static(&[0x5a; 32]),
        renew_period_ms: 3_600_000,
        ..Default::default()
    })
    .await
    .expect("RenewDelegationToken (unknown hmac)");
    check!(
        missing.error_code != 0,
        "a renew against an unknown HMAC must fail"
    );

    let expire = send_expire_delegation_token(&mut alice, 400, &ExpireDelegationTokenRequest {
        hmac: hmac.clone(),
        expiry_time_period_ms: -1,
        ..Default::default()
    })
    .await
    .expect("ExpireDelegationToken");
    assert!(expire.error_code == 0, "{expire:?}");
    drop(alice);

    let client = support::sasl_client(&addr.to_string(), "alice", "wonderland").await;
    for operation in [
        "CreateDelegationToken",
        "RenewDelegationToken",
        "ExpireDelegationToken",
    ] {
        support::wait_for_audit_record(&client, operation, |j| {
            names_token(j, operation, &token_id, "alice")
        })
        .await;
    }

    let records = support::consume_audit_records(&client).await;
    check!(
        records_for(&records, "RenewDelegationToken") == 1,
        "a renew that failed must not be audited"
    );

    // Redaction: the audit topic carries the token's id and never the secret
    // that authenticates as it.
    let dump = serde_json::to_string(&records).expect("audit records serialize");
    for secret in secret_spellings(&hmac) {
        check!(
            !dump.contains(&secret),
            "the token HMAC must never reach the audit topic"
        );
    }

    handle.shutdown().await;
}
