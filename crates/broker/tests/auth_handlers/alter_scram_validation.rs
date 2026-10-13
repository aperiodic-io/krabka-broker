//! `AlterUserScramCredentials` (KIP-554) request validation.
//!
//! The cases here drive the per-row error codes: iteration counts below and
//! above the accepted range, an unknown mechanism byte, two rows for one
//! username, a deletion and an upsertion for the same username, and a
//! deletion whose target credential does not exist.

use {
    crate::{
        alter_scram::{
            KAFKA_DUPLICATE_RESOURCE, KAFKA_MAX_SCRAM_ITERATIONS, KAFKA_UNACCEPTABLE_CREDENTIAL,
            KAFKA_UNSUPPORTED_SASL_MECHANISM, SaltedPassword, WIRE_MECH_SCRAM_SHA_256,
            WIRE_MECH_SCRAM_SHA_512, drive_alter_user_scram_credentials_as_plain,
            pbkdf2_salt_and_salted, provision_as_admin,
        },
        harness::{
            ListeningBroker, RetainedBroker, alice_password, scram_admin_fixture, start_scram_admin,
        },
        support::{
            sasl::{ScramAlgorithm, ScramMechanismCode, scram_upsertion},
            sasl_plaintext_config,
        },
    },
    assert2::assert,
    krabka_broker::Broker,
    krabka_protocol::owned::alter_user_scram_credentials_request::{
        AlterUserScramCredentialsRequest, ScramCredentialDeletion,
    },
    krabka_security::SaslMechanism,
};

/// `iterations < 4096` gives `UNACCEPTABLE_CREDENTIAL`.
///
/// The test uses a super-user principal, so the only error path it exercises
/// is the parameter validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_low_iterations_rejected() {
    let RetainedBroker {
        log_dir: _log_dir,
        handle,
        addr,
    } = scram_admin_fixture(vec![SaslMechanism::Plain]).await;

    // 64-byte salted_password length is valid; only `iterations` violates.
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![scram_upsertion().iterations(1).call()],
        ..Default::default()
    };
    let resp = provision_as_admin(addr, req, "PLAIN auth + AUSCR (rejected)").await;
    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == KAFKA_UNACCEPTABLE_CREDENTIAL,
        "iterations < 4096 must get UNACCEPTABLE_CREDENTIAL, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_high_iterations_rejected_but_max_allowed() {
    let RetainedBroker {
        log_dir: _log_dir,
        handle,
        addr,
    } = scram_admin_fixture(vec![SaslMechanism::Plain]).await;
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![
            scram_upsertion()
                .name("too-high".into())
                .iterations(KAFKA_MAX_SCRAM_ITERATIONS + 1)
                .call(),
            scram_upsertion()
                .name("max".into())
                .iterations(KAFKA_MAX_SCRAM_ITERATIONS)
                .salt(bytes::Bytes::from(vec![1u8; 16]))
                .salted_password(bytes::Bytes::from(vec![1u8; 64]))
                .call(),
        ],
        ..Default::default()
    };

    let resp = provision_as_admin(addr, req, "PLAIN auth + AUSCR high iterations").await;

    handle.shutdown().await;
    assert!(resp.results.len() == 2, "one row per distinct username");
    let too_high = resp
        .results
        .iter()
        .find(|result| result.user == "too-high")
        .expect("too-high row");
    assert!(
        too_high.error_code == KAFKA_UNACCEPTABLE_CREDENTIAL,
        "iterations > 16384 must get UNACCEPTABLE_CREDENTIAL, got {:?}",
        too_high
    );
    let max = resp
        .results
        .iter()
        .find(|result| result.user == "max")
        .expect("max row");
    assert!(
        max.error_code == 0,
        "16384 iterations remains allowed: {max:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_unknown_mechanism_returns_unsupported_sasl_mechanism() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = sasl_plaintext_config(log_dir.path().to_path_buf());
    let admin_password = format!(
        "test-pass-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_password.clone());
    cfg.super_users = maplit::hashset! {"admin".to_string()};

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![
            scram_upsertion()
                .mechanism(ScramAlgorithm::Unrecognized(ScramMechanismCode(99)))
                .call(),
        ],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR unknown mechanism");

    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == KAFKA_UNSUPPORTED_SASL_MECHANISM,
        "unknown SCRAM mechanism must get UNSUPPORTED_SASL_MECHANISM, got {:?}",
        resp.results[0]
    );
}

/// Two upsertions for the same user in one request: Kafka's response is
/// per username, so the single row for that username gets
/// `DUPLICATE_RESOURCE` (92) even when the mechanisms differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_resource_rejected() {
    let RetainedBroker {
        log_dir: _log_dir,
        handle,
        addr,
    } = scram_admin_fixture(vec![SaslMechanism::Plain]).await;

    let SaltedPassword { salt, salted } = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let upsert = scram_upsertion()
        .salt(bytes::Bytes::from(salt))
        .salted_password(bytes::Bytes::from(salted.to_vec()))
        .call();
    let mut upsert_sha256 = upsert.clone();
    upsert_sha256.mechanism = WIRE_MECH_SCRAM_SHA_256;
    upsert_sha256.salted_password = bytes::Bytes::from(vec![7; 32]);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![upsert, upsert_sha256],
        ..Default::default()
    };
    let resp = provision_as_admin(addr, req, "PLAIN auth + AUSCR (duplicate)").await;
    handle.shutdown().await;
    assert!(resp.results.len() == 1, "one result row per username");
    assert!(
        resp.results[0].error_code == KAFKA_DUPLICATE_RESOURCE,
        "duplicate username must get DUPLICATE_RESOURCE, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_deletion_and_upsertion_rejected_per_user() {
    let RandomScramAdmin {
        log_dir: _log_dir,
        admin_password,
        handle,
        addr,
    } = random_scram_admin_fixture().await;
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                iterations: 4096,
                salt: vec![1; 16],
                server_key: vec![2; 64],
                stored_key: vec![3; 64],
            },
        ))
        .await
        .expect("seed alice SCRAM credential");
    handle
        .wait_for_image(|image| {
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        })
        .await;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        upsertions: vec![
            scram_upsertion()
                .mechanism(ScramAlgorithm::Sha256)
                .salt(bytes::Bytes::from(vec![4u8; 16]))
                .salted_password(bytes::Bytes::from(vec![5u8; 32]))
                .call(),
        ],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR duplicate deletion/upsertion");

    handle.shutdown().await;
    assert!(resp.results.len() == 1, "one result row per username");
    assert!(resp.results[0].user == "alice");
    assert!(
        resp.results[0].error_code == KAFKA_DUPLICATE_RESOURCE,
        "delete+upsert for same username must get DUPLICATE_RESOURCE, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_missing_deletion_returns_resource_not_found_91() {
    let RandomScramAdmin {
        log_dir: _log_dir,
        admin_password,
        handle,
        addr,
    } = random_scram_admin_fixture().await;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "ghost".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR missing deletion");

    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == 91,
        "missing deletion target must get RESOURCE_NOT_FOUND (91), got {:?}",
        resp.results[0]
    );
}

/// A SCRAM-admin broker with a random admin password, and the log directory
/// that backs it.
struct RandomScramAdmin {
    log_dir: tempfile::TempDir,
    admin_password: String,
    handle: krabka_broker::BrokerHandle,
    addr: std::net::SocketAddr,
}

async fn random_scram_admin_fixture() -> RandomScramAdmin {
    let log_dir = tempfile::tempdir().unwrap();
    let admin_password = uuid::Uuid::new_v4().to_string();
    let ListeningBroker { handle, addr } =
        start_scram_admin(log_dir.path(), &admin_password, vec![SaslMechanism::Plain]).await;
    RandomScramAdmin {
        log_dir,
        admin_password,
        handle,
        addr,
    }
}
