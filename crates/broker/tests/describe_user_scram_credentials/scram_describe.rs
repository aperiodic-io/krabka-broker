//! The rows `DescribeUserScramCredentials` returns for an authorized caller: a
//! seeded credential, an unknown user, and a user named twice in one request.

use {
    crate::{
        scram_cluster::{
            admin_test_password, seed_scram_credential,
            start_single_broker_sasl_plaintext_with_users,
        },
        scram_driver::drive_describe_user_scram_credentials_sasl,
    },
    assert2::assert,
    krabka_security::SaslMechanism,
};

const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
const WIRE_MECH_SCRAM_SHA_512: i8 = 2;

/// Test 1: seed alice's SCRAM credential directly with
/// `submit_metadata_record_for_test`, describe with `users=None`, then assert
/// that mechanism=2 (SCRAM-SHA-512) appears in the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_all_users_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users("admin", &[(
        "admin",
        &admin_test_password(),
    )])
    .await;

    // Seed alice's SCRAM credential directly via metadata (bypasses the
    // AlterUserScramCredentials wire path — keeps this test focused on Describe).
    let rec = krabka_metadata::MetadataRecord::V1ScramCredential(
        krabka_metadata::ScramCredentialRecord {
            user: "alice".into(),
            mechanism: krabka_security::SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1, 2, 3, 4],
            server_key: vec![5; 64],
            stored_key: vec![6; 64],
        },
    );
    handle
        .submit_metadata_record_for_test(rec)
        .await
        .expect("seed alice ScramCredential");

    // Wait for the credential to become visible in the controller image.
    handle
        .wait_for_image(|img| !img.scram_credentials_for_user("alice").is_empty())
        .await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "admin", &admin_test_password(), None)
            .await;

    assert!(top_err == 0, "top-level error should be 0");

    let alice_row = per_user
        .iter()
        .find(|(u, _, _)| u == "alice")
        .expect("alice must appear in response");
    assert!(
        alice_row.1 == 0,
        "per-user error_code should be 0 for alice"
    );
    assert!(
        alice_row.2.iter().any(|(mech, _)| *mech == 2),
        "expected mechanism=2 (SCRAM-SHA-512) in credential_infos; got {:?}",
        alice_row.2,
    );
}

/// Test 2: describe the user `ghost`, which does not exist, then assert that
/// the per-user row carries `error_code = 91` (`RESOURCE_NOT_FOUND`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_user_returns_error() {
    let (_handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users("admin", &[(
        "admin",
        &admin_test_password(),
    )])
    .await;

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr,
        "admin",
        &admin_test_password(),
        Some(vec!["ghost".into()]),
    )
    .await;

    assert!(top_err == 0, "top-level error_code should be 0");

    let row = per_user
        .iter()
        .find(|(u, _, _)| u == "ghost")
        .expect("ghost must appear in response");
    assert!(
        row.1 == 91, /* RESOURCE_NOT_FOUND */
        "expected RESOURCE_NOT_FOUND (91) for unknown user ghost; got {}",
        row.1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_duplicate_requested_user_returns_single_duplicate_resource_row() {
    let admin_pass = format!(
        "admin-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos()
    );
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", admin_pass.as_str())])
            .await;
    seed_scram_credential(&handle, "alice", SaslMechanism::ScramSha512, 4096).await;
    seed_scram_credential(&handle, "bob", SaslMechanism::ScramSha512, 8192).await;

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr,
        "admin",
        admin_pass.as_str(),
        Some(vec!["alice".into(), "bob".into(), "alice".into()]),
    )
    .await;

    handle.shutdown().await;
    assert!(top_err == 0, "top-level error_code should be 0");
    assert!(
        per_user.len() == 2,
        "duplicate request users collapse: {per_user:?}"
    );

    let alice_rows: Vec<_> = per_user
        .iter()
        .filter(|(user, _, _)| user == "alice")
        .collect();
    assert!(
        alice_rows.len() == 1,
        "alice should appear once: {per_user:?}"
    );
    assert!(alice_rows[0].1 == KAFKA_DUPLICATE_RESOURCE);
    assert!(alice_rows[0].2.is_empty());

    let bob = per_user
        .iter()
        .find(|(user, _, _)| user == "bob")
        .expect("distinct users remain successful");
    assert!(bob.1 == 0);
    assert!(bob.2 == vec![(WIRE_MECH_SCRAM_SHA_512, 8192)]);
}
