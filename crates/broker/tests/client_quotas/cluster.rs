//! Fixtures that stand up the cluster the client-quota tests run against.
//!
//! The module starts a single-broker SASL/PLAINTEXT cluster with a
//! `SimpleAclAuthorizer`, creates topics as the super user, and seeds the ACL
//! records a test needs before the authorizer lets alice produce or fetch.

use krabka_broker::BrokerHandle;

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers
// ─────────────────────────────────────────────────────────────────────────────
/// Creates a topic with SASL/PLAIN as admin. Asserts success.
pub use crate::kafka_wire::create_topic_as_admin;
pub use crate::support::sasl::start_sasl_plaintext_with_acl_users as start_single_broker_sasl_plaintext_with_users;

/// Waits until `handle` sees `(topic, partition)` in its image.
pub async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: seed a dummy ACL to disable the compat shim (allow-all when no ACLs)
// ─────────────────────────────────────────────────────────────────────────────

/// Seeds an ACL that allows alice to Write topic `topic`.
pub use crate::support::acl::seed_alice_write_acl;
pub use crate::support::acl::seed_compat_shim_disable_acl;

/// Wait for the exact user quota to reach the committed image.
pub async fn wait_alice_quota(handle: &BrokerHandle, key: &str, rate: f64) {
    handle
        .wait_for_image(|image| {
            let entity: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
            image
                .client_quotas()
                .get(&entity)
                .and_then(|configs| configs.get(key))
                == Some(&rate)
        })
        .await;
}
