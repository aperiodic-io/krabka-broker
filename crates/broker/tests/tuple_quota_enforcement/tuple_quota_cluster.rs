//! Cluster setup and metadata seeding for the tuple-quota test: the
//! single-broker SASL/PLAINTEXT boot, the admin `CreateTopics` call, and the
//! two ACL records the authorizer needs before alice may produce.
//!
//! The seeding is here rather than in the test body because one of the two ACLs
//! exists only to disable the compatibility shim, which allows every operation
//! while the image holds no ACL at all.

use krabka_broker::BrokerHandle;

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers (copied from client_quotas.rs)
// ─────────────────────────────────────────────────────────────────────────────
pub(crate) use crate::kafka_wire::create_configured_topic_sasl as create_topic_as_admin;
pub use crate::support::sasl::start_single_broker_sasl_plaintext_with_users;

/// Waits until `handle` sees `(topic, partition)` in its image.
pub(crate) async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: seed a dummy ACL to disable the compat shim (allow-all when no ACLs)
// ─────────────────────────────────────────────────────────────────────────────

/// Seeds an ACL that allows alice to Write to the topic `topic`.
pub use crate::support::acl::seed_alice_write_acl;
pub use crate::support::acl::seed_compat_shim_disable_acl;
