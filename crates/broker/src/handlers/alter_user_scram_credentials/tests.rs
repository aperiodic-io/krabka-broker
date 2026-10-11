//! Tests for the whole-request gates of the `AlterUserScramCredentials`
//! handler and for the submit of an accepted request.
//!
//! They cover the cluster authorization preamble, the `metadata.version`
//! feature gate, and the metadata image a successful request leaves behind.
//! Most of them drive a live broker, so they are kept out of the module root.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialDeletion;
use krabka_security::{SaslMechanism, scram::MIN_SCRAM_ITERATIONS};

use super::*;
use crate::{
    handlers::alter_user_scram_credentials::test_support::{
        deletion, expected_response, expected_result, low_metadata_version, start_broker,
        test_context, valid_upsertion,
    },
    test_support::{DenyAll, test_ctx, wait_for_controller_leader},
};

fn valid_request(user: &str) -> AlterUserScramCredentialsRequest {
    AlterUserScramCredentialsRequest {
        upsertions: vec![valid_upsertion(user)],
        ..Default::default()
    }
}

fn denied_user(
    user: &str,
) -> krabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult
{
    expected_result(
        user,
        codes::CLUSTER_AUTHORIZATION_FAILED,
        Some("Request AlterUserScramCredentials needs ALTER permission."),
    )
}

#[test]
fn scram_gate_permits_unknown_and_at_or_above_level() {
    use krabka_metadata::metadata_version::SCRAM_MIN_LEVEL;

    let cases = [
        // No finalized metadata.version — gate permits.
        (None, false),
        // Below SCRAM_MIN_LEVEL — gate rejects.
        (Some(10), true),
        // At SCRAM_MIN_LEVEL — gate permits.
        (Some(11), false),
    ];
    for (level, want_err) in cases {
        assert!(
            crate::handlers::test_support::metadata_version_gated(level, SCRAM_MIN_LEVEL)
                == want_err,
            "level {level:?}"
        );
    }
}

#[tokio::test]
async fn handle_denies_invalid_rows_before_scram_validation() {
    denied_invalid_rows(false).await;
}

#[tokio::test]
async fn handle_authorizes_and_persists_valid_upsertion() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "admin"),
        controller_leader
    );
    let req = valid_request("alice");

    let resp = answer(&broker, req, &ctx).await;

    let expected = expected_response(vec![expected_result("alice", 0, None)]);
    assert!(resp == expected);
    let image = broker.controller.current_image();
    assert!(
        image
            .scram_credential("alice", SaslMechanism::ScramSha256)
            .is_some()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_denies_valid_upsertion_without_cluster_alter() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        deny_all,
        context(ctx, "admin")
    );
    let req = valid_request("alice");

    let resp = answer(&broker, req, &ctx).await;

    let expected = expected_response(vec![denied_user("alice")]);
    assert!(resp == expected);
    let image = broker.controller.current_image();
    assert!(
        image
            .scram_credential("alice", SaslMechanism::ScramSha256)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unsupported_metadata_version_reports_every_requested_user() {
    check_unsupported_users(|| vec![valid_upsertion("bob")]).await;
}

#[tokio::test]
async fn handle_low_metadata_version_denied_request_reports_authorization_per_distinct_user() {
    denied_invalid_rows(true).await;
}

#[tokio::test]
async fn handle_low_metadata_version_authorized_request_deduplicates_unsupported_users() {
    check_unsupported_users(|| {
        vec![
            valid_upsertion("bob"),
            valid_upsertion("bob"),
            valid_upsertion("alice"),
        ]
    })
    .await;
}

async fn denied_invalid_rows(use_low_metadata_version: bool) {
    broker_fixture!((broker_handle, _dir, broker), deny_all);
    if use_low_metadata_version {
        wait_for_controller_leader(&broker).await;
        low_metadata_version(&broker).await;
    }
    test_ctx!(ctx, "admin");
    let mut invalid_upsertion = valid_upsertion("bob");
    invalid_upsertion.iterations = MIN_SCRAM_ITERATIONS - 1;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".into(),
            mechanism: 99,
            ..Default::default()
        }],
        upsertions: vec![invalid_upsertion, valid_upsertion("bob")],
        ..Default::default()
    };

    let resp = answer(&broker, req, &ctx).await;

    let expected = expected_response(vec![denied_user("alice"), denied_user("bob")]);
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

async fn check_unsupported_users(
    upsertions: impl FnOnce() -> Vec<
        krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialUpsertion,
    >,
) {
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    low_metadata_version(&broker).await;
    test_ctx!(ctx, "admin");
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![deletion("alice")],
        upsertions: upsertions(),
        ..Default::default()
    };

    let resp = answer(&broker, req, &ctx).await;

    let msg = "The current metadata.version does not support SCRAM";
    let expected = expected_response(vec![
        expected_result("alice", codes::UNSUPPORTED_VERSION, Some(msg)),
        expected_result("bob", codes::UNSUPPORTED_VERSION, Some(msg)),
    ]);
    assert!(resp == expected);
    broker_handle.shutdown().await;
}
