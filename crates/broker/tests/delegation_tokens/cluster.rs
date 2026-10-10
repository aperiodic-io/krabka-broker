//! Single-broker `SASL_PLAINTEXT` fixtures for the KIP-48 suite, and the
//! metadata-image waiters the tests use to observe a committed token record.
//!
//! Both fixtures enable PLAIN and SCRAM-SHA-256 on one listener and set the
//! master delegation-token key, because the token-fallback authentication
//! path needs SCRAM even though the human users authenticate with PLAIN.

use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerHandle};
use krabka_security::SaslMechanism;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boots a single-broker `SASL_PLAINTEXT` cluster set up for the full KIP-48
/// lifecycle:
///   - PLAIN credentials for `alice` and `bob`
///   - both PLAIN and SCRAM-SHA-256 enabled on the listener. PLAIN serves the
///     human-user handshakes, and SCRAM-SHA-256 serves the token-fallback
///     path.
///   - `delegation_token_secret_key = Some("e2e-master-key")`, which gates
///     the four delegation-token RPCs and the SCRAM token-fallback lookup
pub(crate) async fn start_broker() -> (BrokerHandle, TempDir, SocketAddr) {
    let (log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![
        SaslMechanism::Plain,
        SaslMechanism::ScramSha256,
    ]);
    cfg.plain_credentials
        .insert("alice".to_string(), "wonderland".to_string());
    cfg.plain_credentials
        .insert("bob".to_string(), "builder".to_string());
    // Inter-broker auth uses PLAIN as alice (the cluster only has one broker
    // so this is not exercised, but `BrokerConfig::validate` requires it
    // when the inter-broker listener is SASL).
    cfg.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: "alice".to_string(),
        password: "wonderland".to_string(),
    });
    // KIP-48 distinguishes the absolute ceiling (`max_lifetime_ms` →
    // `max_timestamp_ms`) from the initial renew window (`default_renew_period`
    // → `expiry_timestamp_ms`). With 7d ceiling + 24h renew period (both
    // the Kafka defaults), the create handler emits expiry = issue + 24h
    // and max = issue + 7d as separate values, so Renew can extend the
    // expiry well past its initial value (and the lifecycle test asserts
    // strict-monotonic extension below).
    crate::support::sasl::delegation_token_defaults(&mut cfg, b"e2e-master-key");

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Act-as variant. It boots a single-broker `SASL_PLAINTEXT` cluster with
/// caller-specified PLAIN credentials and a caller-specified set of
/// super-users.
///
/// `plain_creds` is `&[(username, password)]`. `super_users` is
/// `&[username]`. The names listed there go into `BrokerConfig.super_users`
/// and bypass ACL checks. In particular, per spec §1 they are the only
/// callers allowed to set `owner_principal_*` on `CreateDelegationToken`.
///
/// The protocol surface matches `start_broker`: PLAIN and SCRAM-SHA-256
/// enabled, the master delegation-token key set, a 7d ceiling, and a 24h
/// default renew period.
pub(crate) fn start_broker_with_super_users(
    plain_creds: &[(&str, &str)],
    super_users: &[&str],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let (log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![
        SaslMechanism::Plain,
        SaslMechanism::ScramSha256,
    ]);
    for (user, password) in plain_creds {
        cfg.plain_credentials
            .insert((*user).to_string(), (*password).to_string());
    }
    for user in super_users {
        cfg.super_users.insert((*user).to_string());
    }
    // Act-as is gated by KIP-373's `CreateTokens` ACL. With no ACL authorizer
    // Kafka allows every operation, so the gate is exercised against the ACL
    // authorizer, which grants the super users everything.
    cfg.authorizer = std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
        cfg.super_users.iter().cloned().collect(),
    ));
    // Inter-broker auth uses PLAIN as the first listed user. `BrokerConfig::
    // validate` requires inter-broker credentials when the inter-broker
    // listener is SASL, even though a single-broker cluster never opens an
    // inter-broker connection.
    let (ib_user, ib_pw) = plain_creds
        .first()
        .expect("must supply at least one PLAIN credential for inter-broker auth");
    cfg.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: (*ib_user).to_string(),
        password: (*ib_pw).to_string(),
    });
    crate::support::sasl::delegation_token_defaults(&mut cfg, b"act-as-master-key");

    Box::pin(crate::support::sasl::start_broker(cfg, log_dir))
}

// ─────────────────────────────────────────────────────────────────────────────
// Image-watch helpers. `submit_change` returns once the record is replicated
// to the controller's state machine, but the listener's `MetadataImage` is
// served through the same controller handle, so a tight poll converges
// within a few ms.
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) async fn wait_for_token(
    handle: &BrokerHandle,
    token_id: &str,
) -> krabka_metadata::DelegationToken {
    // Watch the committed metadata image (same controller handle
    // `controller_image_for_test` reads) until the V1DelegationToken record
    // materializes, then re-read it to return the applied token.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_some())
        .await;
    handle
        .controller_image_for_test()
        .delegation_token_by_id(token_id)
        .expect("token present in image after wait_for_image")
        .clone()
}

pub(crate) async fn wait_for_token_gone(handle: &BrokerHandle, token_id: &str) {
    // Watch the committed metadata image until the token's tombstone applies.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_none())
        .await;
}

/// The act-as cluster: an admin super user and an ordinary token owner.
pub(crate) async fn start_admin_alice() -> (BrokerHandle, TempDir, SocketAddr) {
    start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"]).await
}
