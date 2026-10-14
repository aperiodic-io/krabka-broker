//! The one answer to "does the broker own this topic".
//!
//! `Metadata` and `DescribeTopicPartitions` both fill the `is_internal` flag of
//! a topic row from [`is_internal_topic`], so a client sees the same answer
//! whichever RPC it asks with. The flag is what hides a topic from
//! `AdminClient.listTopics()`, from `kafka-topics --list --exclude-internal`,
//! and from a consumer that subscribes by pattern with the default
//! `exclude.internal.topics=true`. A broker-owned topic that answers `false` is
//! one a `.*` mirroring or audit sink silently starts consuming.
//!
//! # The set is the subsystems' own names
//!
//! Every entry of [`INTERNAL_TOPICS`] is the constant the owning subsystem
//! already declares for its topic, never a second spelling of the name. A new
//! internal topic is covered the day its constant lands, by naming that
//! constant here.
//!
//! The audit log is the one broker-owned topic whose name is not a constant:
//! `krabka.audit.topic` renames it, and every audit code path reads
//! [`BrokerConfig::audit_topic`] rather than a literal. So [`is_internal_topic`]
//! takes the config and compares against the name the broker is actually
//! auditing to, which is why it is not the pure static predicate Kafka's
//! `Topic.isInternal` can afford to be — Kafka has no rename knob.
//! [`validate_audit_topic_name`] keeps that name inside the `__` convention so
//! the freeze rule below stays in step with this one.
//!
//! # What Kafka reports as internal
//!
//! Kafka's own set is a fixed three names — `__consumer_offsets`,
//! `__transaction_state` and `__share_group_state` — and it does **not**
//! include `__remote_log_metadata`, whose tiered-storage records are
//! broker-owned in spirit but ordinary on the wire. That was settled against
//! the pinned images rather than the wiki, two ways that agree: the
//! `INTERNAL_TOPICS` set in `org.apache.kafka.common.internals.Topic` inside
//! each image's `kafka-clients` jar, and creating all four names on a running
//! broker and diffing `kafka-topics --list` against
//! `kafka-topics --list --exclude-internal`. `apache/kafka:4.3.1` and
//! `apache/kafka:4.0.0` report the three; `confluentinc/cp-kafka:7.5.0`
//! predates KIP-932 and reports the first two, leaving `__share_group_state`
//! ordinary because it has no share coordinator to own it. Krabka does have
//! one, so it follows the 4.x answer.
//!
//! The remaining entries are Krabka's own: the audit log, the barrier state
//! topic, and the diskless WAL index. Kafka has no name for them and so no
//! opinion about them, and each carries broker state that no application should
//! read as a data topic.

use crate::{
    BrokerError, barrier::STATE_TOPIC, config::BrokerConfig, coordinator::bootstrap::OFFSETS_TOPIC,
    diskless::index_log::DISKLESS_WAL_INDEX_TOPIC, share_coordinator::bootstrap::TOPIC,
    txn::bootstrap::TOPIC as TXN_TOPIC,
};

/// The broker-owned topics whose names are fixed, each named by the constant
/// its subsystem declares.
///
/// The audit log is deliberately absent: it is the one broker-owned topic an
/// operator can rename, so [`is_internal_topic`] reads its name out of the
/// config instead.
///
/// The constant is `pub(crate)` rather than private so that tests elsewhere in
/// the crate — the freeze registry's, in particular — can hold their own rules
/// against the whole set instead of against a copy of it.
pub(crate) const INTERNAL_TOPICS: [&str; 5] = [
    OFFSETS_TOPIC,
    TXN_TOPIC,
    TOPIC,
    STATE_TOPIC,
    DISKLESS_WAL_INDEX_TOPIC,
];

/// The name prefix that marks a broker-owned topic.
///
/// Every name in [`INTERNAL_TOPICS`] carries it, which
/// `every_internal_topic_carries_the_internal_prefix` holds them to, and
/// [`validate_audit_topic_name`] holds the configured audit topic to the same
/// rule. [`crate::freeze::scope_covers_internal_topic`] tests for the prefix
/// rather than for set membership, because a freeze has to be refused on a
/// scope that merely *reaches* an internal topic, not only on one that names
/// it.
pub(crate) const INTERNAL_TOPIC_PREFIX: &str = "__";

/// Whether Kafka clients should treat this topic as owned by the broker.
pub(crate) fn is_internal_topic(config: &BrokerConfig, name: &str) -> bool {
    INTERNAL_TOPICS.contains(&name) || name == config.audit_topic
}

/// The `client_id` Kafka's `KafkaApis.handleProduceRequest` compares against
/// to decide `internalTopicsAllowed`:
/// `internalTopicsAllowed = request.header.clientId == "__admin_client"`.
///
/// The group coordinator, the transaction coordinator and the share
/// coordinator never reach the Produce handler at all: they append to
/// `__consumer_offsets`, `__transaction_state` and `__share_group_state`
/// through the partition writer directly, the same way
/// `ReplicaManager.appendRecords`'s other internal callers do, and so does
/// the barrier coordinator for [`crate::barrier::STATE_TOPIC`]. This name is
/// therefore not a bypass any of those subsystems take; it exists only so a
/// client Produce request with this exact `client_id` (Kafka's own admin
/// tooling) is not refused by [`produce_internal_topics_allowed`].
pub(crate) const PRODUCE_ADMIN_CLIENT_ID: &str = "__admin_client";

/// The `client_id` prefix the broker's own diskless WAL index writer
/// connects with, one connection per broker
/// (`{DISKLESS_INDEX_WRITER_CLIENT_ID_PREFIX}{broker_id}-producer`; see
/// `broker::diskless_index::index_log_config`).
///
/// Unlike the coordinators above, the diskless WAL index flusher has no
/// direct-partition-writer path: it publishes its own compacted records to
/// [`INTERNAL_TOPICS`]'s `__diskless_wal_index` through a real
/// `krabka_client_producer::Producer` connection, the same way an ordinary
/// client's Produce reaches this broker. Without this exception the
/// flusher's own writes are refused by the gate meant for everyone *else*,
/// and the diskless flush loop can never advance.
pub(crate) const DISKLESS_INDEX_WRITER_CLIENT_ID_PREFIX: &str = "krabka-diskless-index-broker-";

/// Whether a Produce request naming `client_id` may append directly to an
/// internal topic. See [`PRODUCE_ADMIN_CLIENT_ID`] and
/// [`DISKLESS_INDEX_WRITER_CLIENT_ID_PREFIX`].
pub(crate) fn produce_internal_topics_allowed(client_id: &str) -> bool {
    client_id == PRODUCE_ADMIN_CLIENT_ID
        || client_id.starts_with(DISKLESS_INDEX_WRITER_CLIENT_ID_PREFIX)
}

/// Rejects a `krabka.audit.topic` that the `__` convention does not cover.
///
/// [`crate::freeze::scope_covers_internal_topic`] refuses a freeze by the
/// prefix rather than by membership of [`INTERNAL_TOPICS`], so an audit topic
/// named outside the convention would be internal to `Metadata` and freezable
/// at the same time — and an operator could stop the cluster auditing by
/// freezing writes to its own audit log. Requiring the prefix keeps the two
/// rules in step for the one broker-owned name that is not a constant.
///
/// # Errors
///
/// Returns `Err` when `name` fails Kafka's topic-name rules
/// ([`krabka_log::topic_name::validate_topic_name`]), because the broker
/// creates a partition directory from it, or when `name` does not start with
/// [`INTERNAL_TOPIC_PREFIX`].
pub(crate) fn validate_audit_topic_name(name: &str) -> Result<(), BrokerError> {
    if let Err(invalid) = krabka_log::topic_name::validate_topic_name(name) {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "audit_topic {name:?}: {invalid}"
        )));
    }
    if name.starts_with(INTERNAL_TOPIC_PREFIX) {
        return Ok(());
    }
    Err(BrokerError::InvalidRuntimeConfig(format!(
        "audit_topic {name:?} must start with {INTERNAL_TOPIC_PREFIX:?}: the audit log is an \
         internal topic, and a name outside that convention would be freezable"
    )))
}

#[cfg(test)]
mod tests {
    use {super::*, crate::config::DEFAULT_AUDIT_TOPIC, assert2::check, std::path::PathBuf};

    /// A config whose audit topic is `name`, for driving [`is_internal_topic`].
    fn config_auditing_to(name: &str) -> BrokerConfig {
        let mut config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
        config.audit_topic = name.to_string();
        config
    }

    /// Kafka's three names answer as Kafka answers, Krabka's own topics answer
    /// `true`, and nothing else does. `__remote_log_metadata` is the row that
    /// is easy to guess wrong: broker-owned in spirit, ordinary to Kafka.
    #[yare::parameterized(
        offsets = { "__consumer_offsets", true },
        txn_state = { "__transaction_state", true },
        share_state = { "__share_group_state", true },
        audit_log = { "__krabka_audit", true },
        barrier_state = { "__barrier_state", true },
        diskless_index = { "__diskless_wal_index", true },
        remote_log_metadata = { "__remote_log_metadata", false },
        raft_metadata = { "__cluster_metadata", false },
        ordinary = { "orders", false },
        single_underscore = { "_foo", false },
        user_prefix = { "__user_topic", false },
        internal_suffix = { "__consumer_offsets-2", false },
        internal_prefix = { "__consumer_offset", false },
    )]
    fn a_broker_owned_topic_is_internal_and_nothing_else_is(name: &str, expected: bool) {
        let config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
        check!(is_internal_topic(&config, name) == expected, "{name}");
    }

    /// An operator who renames the audit log gets the flag on the name the
    /// broker is auditing to, and not on the one it no longer writes.
    #[test]
    fn the_configured_audit_topic_is_the_internal_one() {
        let config = config_auditing_to("__house_audit");
        check!(is_internal_topic(&config, "__house_audit"));
        check!(!is_internal_topic(&config, "__krabka_audit"));
        // The fixed names are unaffected by the rename.
        check!(is_internal_topic(&config, "__consumer_offsets"));
    }

    /// The exact-name set and the `__` convention the freeze registry tests for
    /// have to agree, or a topic could be internal and freezable at once.
    #[test]
    fn every_internal_topic_carries_the_internal_prefix() {
        for name in INTERNAL_TOPICS {
            check!(name.starts_with(INTERNAL_TOPIC_PREFIX), "{name}");
        }
    }

    /// Two subsystems naming the same topic would make the set's length lie
    /// about how many topics the broker owns.
    #[test]
    fn no_two_subsystems_claim_the_same_topic_name() {
        let mut seen = INTERNAL_TOPICS.to_vec();
        seen.sort_unstable();
        seen.dedup();
        check!(seen.len() == INTERNAL_TOPICS.len());
    }

    // Only the exact admin client id Kafka's `KafkaApis` compares against,
    // or the broker's own diskless index writer, is let through; nothing
    // else, including a name that merely contains one of them, is.
    #[yare::parameterized(
        admin_client = { "__admin_client", true },
        ordinary_application = { "my-app", false },
        empty_client_id = { "", false },
        admin_client_suffix = { "__admin_client-2", false },
        diskless_writer_broker_one = { "krabka-diskless-index-broker-1-producer", true },
        diskless_writer_broker_42 = { "krabka-diskless-index-broker-42-producer", true },
        diskless_writer_prefix_in_name = { "not-krabka-diskless-index-broker-1-producer", false },
    )]
    fn only_the_admin_client_or_the_diskless_index_writer_may_produce_to_an_internal_topic(
        client_id: &str,
        expected: bool,
    ) {
        check!(
            produce_internal_topics_allowed(client_id) == expected,
            "{client_id}"
        );
    }

    /// The audit topic is the one internal name the prefix rule cannot take on
    /// trust, so validation is what puts it back inside the convention.
    #[yare::parameterized(
        no_prefix = { "audit" },
        single_underscore = { "_audit" },
        empty = { "" },
        parent_path = { "__../x" },
        slash = { "__a/b" },
    )]
    fn an_audit_topic_outside_the_convention_is_rejected(name: &str) {
        let result = validate_audit_topic_name(name);
        assert2::assert!(let Err(error) = result, "{name}");
        check!(error.to_string().contains("audit_topic"), "{name}");
    }

    /// Validation exists to keep the freeze rule and this one in step: any name
    /// it accepts is a name `set_freeze` refuses, under both pattern types.
    #[test]
    fn a_validated_audit_topic_is_never_freezable() {
        use krabka_metadata::PatternType;

        for name in [DEFAULT_AUDIT_TOPIC, "__house_audit"] {
            check!(validate_audit_topic_name(name).is_ok(), "{name}");
            check!(
                crate::freeze::scope_covers_internal_topic(PatternType::Literal, name),
                "literal {name}"
            );
            check!(
                crate::freeze::scope_covers_internal_topic(PatternType::Prefixed, name),
                "prefixed {name}"
            );
        }
    }
}
