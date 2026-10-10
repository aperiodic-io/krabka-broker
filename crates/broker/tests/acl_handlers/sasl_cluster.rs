//! `BrokerConfig` builders for the ACL suite. Each one declares a single
//! `SASL_PLAINTEXT` listener with PLAIN credentials and installs the
//! enforcing `SimpleAclAuthorizer` in place of the permissive default, so
//! every test in the suite starts from a cluster that actually checks ACLs.

use krabka_broker::{BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer};
use krabka_security::SaslMechanism;

use crate::support::acl::TopicAclSetup;

/// Build a `BrokerConfig` with a single `SASL_PLAINTEXT` listener, PLAIN
/// enabled, and the given super-user. The non-super-user case still
/// declares a super-user so the cluster-Alter gate applies. It also
/// installs `SimpleAclAuthorizer` explicitly so the broker enforces ACLs.
/// The default is `AllowAllAuthorizer`, which would silently let
/// every test through.
pub fn sasl_plain_broker_config(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_user: Option<&str>,
) -> BrokerConfig {
    let super_users: Vec<&str> = super_user.into_iter().collect();
    sasl_plain_broker_config_multi_super(log_dir, creds, &super_users)
}

/// Like `sasl_plain_broker_config`, but it accepts multiple super-users. The
/// `multi_super_user_both_can_provision` test uses it to verify that
/// any principal in the `super_users` set can drive privileged admin APIs.
pub fn sasl_plain_broker_config_multi_super(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_users: &[&str],
) -> BrokerConfig {
    let mut cfg = crate::support::sasl::sasl_plaintext_mechanisms(
        log_dir.to_path_buf(),
        vec![SaslMechanism::Plain],
    );
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_users = super_users.iter().map(|s| (*s).to_string()).collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(with_controller_peer(
        &cfg.super_users,
    )));
    cfg
}

/// The authorizer's super-users: the configured ones and `ANONYMOUS`.
///
/// Clients reach this broker over SASL, but its own heartbeat reaches the
/// PLAINTEXT controller listener as `ANONYMOUS`. Like a Kafka inter-broker
/// principal it needs `ClusterAction`, or the broker never unfences and places
/// no replica.
fn with_controller_peer(
    super_users: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    super_users
        .iter()
        .cloned()
        .chain(std::iter::once("ANONYMOUS".to_string()))
        .collect()
}

/// Fresh ACL broker with the common admin and alice credentials.
pub async fn start_admin_alice() -> (BrokerHandle, tempfile::TempDir, std::net::SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );
    crate::support::sasl::start_broker(cfg, dir).await
}

/// A single foo partition with Alice's exact topic operation grant.
pub async fn start_alice_topic_grant(
    operation: krabka_metadata::AclOperation,
) -> (BrokerHandle, tempfile::TempDir, std::net::SocketAddr) {
    let (handle, dir, _) = start_admin_alice().await;
    let addr = handle.listen_addr();
    crate::acl_admin::create_topic_as_admin(addr, "foo", 1).await;
    handle
        .submit_metadata_record_for_test(crate::support::acl::topic_acl_record(TopicAclSetup {
            topic: "foo",
            operation,
            ..Default::default()
        }))
        .await
        .expect("seed Alice's topic operation ACL");
    (handle, dir, addr)
}

/// Provisioning fixture with the caller's exact credential set and the admin super-user.
pub async fn start_admin_with(
    creds: &[(&str, &str)],
) -> (tempfile::TempDir, BrokerHandle, std::net::SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(dir.path(), creds, Some("admin"));
    let handle = krabka_broker::Broker::start(cfg)
        .await
        .expect("broker must start");
    let addr = handle.listen_addr();
    (dir, handle, addr)
}

/// A real foo partition with only an irrelevant ACL: Alice's topic operations remain denied.
pub async fn start_denied_foo() -> (BrokerHandle, tempfile::TempDir, std::net::SocketAddr) {
    let (handle, directory, address) = start_admin_alice().await;
    crate::acl_admin::create_topic_as_admin(address, "foo", 1).await;
    crate::support::acl::seed_dummy_acl(&handle).await;
    (handle, directory, address)
}
