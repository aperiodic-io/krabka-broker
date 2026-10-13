//! The shape of the `__barrier_state` internal topic.
//!
//! The topic holds the group definitions, the injection-start records, and the
//! cuts. It is compacted, so the newest record of every key survives for the
//! life of the log. Old cuts leave through tombstones and not through a log
//! trim, because the group definitions share the same prefix.
//!
//! The first group definition creates the topic, with
//! `barrier_state_num_partitions` partitions and
//! `barrier_state_replication_factor` replicas
//! ([`crate::auto_topic_creation::AutoTopicCreation`]), as the other
//! coordinator topics are created.

use {crate::config_keys::CLEANUP_POLICY, std::collections::BTreeMap};

/// The topic configs `__barrier_state` is created with.
///
/// Compaction keeps the newest record of every key, which is what makes a
/// group definition and a retained cut survive without a bound on the log.
pub(crate) fn topic_configs() -> BTreeMap<String, String> {
    BTreeMap::from([(CLEANUP_POLICY.to_owned(), "compact".to_owned())])
}
