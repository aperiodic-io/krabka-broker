//! The broker-scoped dynamic config keys: the controller-managed witness
//! role, broker fencing state and stretch-site role that `DescribeConfigs`
//! reports read-only, and the KIP-1075 deadline for remote `ListOffsets` work.

use std::time::Duration;

use super::parse::{check_range, parse_long};

/// Marks a node as a data-bearing witness. The broker publishes this key for
/// itself, in the same metadata batch that carries its registration record,
/// so the controller can read the role from the metadata image.
///
/// The key is controller-managed and read-only. `AlterConfigs` and
/// `IncrementalAlterConfigs` reject it with `INVALID_CONFIG`, and
/// `DescribeConfigs` returns it with `read_only` set. An operator reads it
/// with `kafka-configs --entity-type brokers --describe`.
///
/// A witness replicates partition data and votes in `KRaft`, so it counts
/// toward `min.insync.replicas`. It serves no client and leads no partition.
pub(crate) const BROKER_WITNESS: &str = "broker.witness";

/// Names the site that should hold partition leadership in a stretch
/// cluster. The controller leader publishes it as a cluster-default broker
/// config, so every node that later becomes controller reads the same value.
///
/// The value is a `broker.rack` value. Site-aware placement puts a broker
/// from this site first in the replica list. In Kafka the preferred leader
/// is `replicas[0]`, so that ordering is what pins leadership.
///
/// The key is controller-managed and read-only, like [`BROKER_WITNESS`].
pub(crate) const STRETCH_PREFERRED_LEADER_SITE: &str = "stretch.preferred.leader.site";

/// KIP-211: how long a committed offset outlives the group that owns it, in
/// whole minutes. The value is static for the life of the process, so
/// `DescribeConfigs` reports it read-only against `STATIC_BROKER_CONFIG`.
pub(crate) const OFFSETS_RETENTION_MINUTES: &str = "offsets.retention.minutes";

/// Cadence of the offset-retention sweep, in milliseconds. Static and
/// read-only for the same reason as [`OFFSETS_RETENTION_MINUTES`].
pub(crate) const OFFSETS_RETENTION_CHECK_INTERVAL_MS: &str = "offsets.retention.check.interval.ms";

/// How long a client connection may go without a complete frame before the
/// broker closes it. The process reads it from its static configuration at
/// startup, so `DescribeConfigs` reports it read-only, at
/// `STATIC_BROKER_CONFIG` when an operator named it and `DEFAULT_CONFIG` when
/// they did not.
///
/// A per-listener override is spelled
/// `listener.name.<name>.connections.max.idle.ms`, which is a key per listener
/// rather than a registry row. `DescribeConfigs` reports each override under
/// this row's type and documentation, because it is the same config under
/// Kafka's `ListenerName.configPrefix`.
pub(crate) const CONNECTIONS_MAX_IDLE_MS: &str = "connections.max.idle.ms";

/// KIP-368: how long an authenticated SASL session may live before the client
/// must re-authenticate in band. The process reads it from its static
/// configuration at startup, so `DescribeConfigs` reports it read-only, the
/// same way it reports [`CONNECTIONS_MAX_IDLE_MS`].
///
/// Kafka spells the per-listener override
/// `listener.name.<name>.<mechanism>.connections.max.reauth.ms`; krabka keys
/// its own override by listener alone, because the window is a property of
/// the listener rather than of the mechanism a client picks on it.
pub(crate) const CONNECTIONS_MAX_REAUTH_MS: &str = "connections.max.reauth.ms";

/// The value krabka writes for [`BROKER_WITNESS`] on a witness node.
pub(crate) const WITNESS_TRUE: &str = "true";

/// Broker-scoped config keys that only the controller writes. `AlterConfigs`
/// and `IncrementalAlterConfigs` must reject every key in this list, and
/// `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_BROKER_CONFIGS: [&str; 2] =
    [BROKER_WITNESS, STRETCH_PREFERRED_LEADER_SITE];

/// `true` when `key` is a broker config that only the controller writes.
pub(crate) fn is_controller_managed_broker_config(key: &str) -> bool {
    CONTROLLER_MANAGED_BROKER_CONFIGS.contains(&key)
}

/// Resolve [`BROKER_WITNESS`] for one node. A missing or unparseable value
/// resolves to `false`, so a cluster with no witness behaves as it did
/// before the role existed.
pub(crate) fn resolve_broker_witness(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> bool {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(BROKER_WITNESS))
        .map(String::as_str)
        == Some(WITNESS_TRUE)
}

/// Every registered node that carries the witness role.
///
/// The controller builds this set once for each scan and then excludes its
/// members from leader selection. Building it once keeps the scan a single
/// walk over the image rather than a lookup for each partition replica.
pub(crate) fn witness_node_ids(
    image: &krabka_metadata::MetadataImage,
) -> std::collections::HashSet<krabka_metadata::NodeId> {
    image
        .brokers()
        .filter(|broker| resolve_broker_witness(image, broker.node_id))
        .map(|broker| broker.node_id)
        .collect()
}

/// Resolve [`STRETCH_PREFERRED_LEADER_SITE`] from the cluster defaults.
/// `None` means the cluster pins leadership to no site.
pub(crate) fn resolve_preferred_leader_site(
    image: &krabka_metadata::MetadataImage,
) -> Option<&str> {
    image
        .default_broker_config()?
        .get(STRETCH_PREFERRED_LEADER_SITE)
        .map(String::as_str)
}

/// KIP-464: the partition count `CreateTopics` gives a topic that asks for
/// `num_partitions = -1`. Kafka's `ServerLogConfigs.NUM_PARTITIONS_CONFIG`,
/// default 1, validated `atLeast(1)`.
///
/// The key is static in Kafka: the controller reads it once, when it builds
/// its `ReplicationControlManager`. `DescribeConfigs` reports it read-only
/// from the node's own configuration, out of
/// [`crate::config::BrokerConfig::num_partitions`].
pub(crate) const NUM_PARTITIONS: &str = "num.partitions";

/// KIP-464: the replication factor `CreateTopics` gives a topic that asks for
/// `replication_factor = -1`. Kafka's
/// `ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG`, default 1.
///
/// Static in Kafka, as [`NUM_PARTITIONS`] is. `DescribeConfigs` reports it
/// read-only out of [`crate::config::BrokerConfig::default_replication_factor`].
pub(crate) const DEFAULT_REPLICATION_FACTOR: &str = "default.replication.factor";

/// Whether `DeleteTopics` may delete a topic. Kafka's
/// `ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG`, a boolean that defaults to
/// `true`. When it is `false`, `ControllerApis.deleteTopics` refuses every
/// request: `INVALID_REQUEST` below v3 and `TOPIC_DELETION_DISABLED` from v3.
///
/// Static in Kafka. `DescribeConfigs` reports it read-only out of
/// [`crate::config::BrokerConfig::delete_topic_enable`].
pub(crate) const DELETE_TOPIC_ENABLE: &str = "delete.topic.enable";

/// Whether `Metadata` may create a topic it is asked about and that does not
/// exist. Kafka's `ServerConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG`, a boolean
/// that defaults to `true`. `KafkaApis.handleTopicMetadataRequest` creates a
/// missing topic only when it is `true` and the request sets
/// `allow_auto_topic_creation`; when it is `false`, a missing topic answers
/// `UNKNOWN_TOPIC_OR_PARTITION` and no `Create` is checked.
///
/// Static in Kafka. `DescribeConfigs` reports it read-only out of
/// [`crate::config::BrokerConfig::auto_create_topics_enable`].
pub(crate) const AUTO_CREATE_TOPICS_ENABLE: &str = "auto.create.topics.enable";

/// The record time a segment may span before it rolls, for a topic that sets
/// no `segment.ms`. Kafka's `ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG`, a
/// long with no default that wins over `log.roll.hours`.
///
/// A node reads it from its `server.properties` keys into
/// [`crate::config::BrokerConfig::log_config`]. `DescribeConfigs` reports it
/// at `STATIC_BROKER_CONFIG` when the operator named it.
pub(crate) const LOG_ROLL_MS: &str = "log.roll.ms";

/// The listener that a broker sends inter-broker traffic on. Kafka's
/// `ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG`, a string with no
/// default. When it is not set, Kafka uses the listener that
/// `security.inter.broker.protocol` names.
///
/// A node reads it from the `inter_broker_listener_name` file key into
/// [`crate::config::BrokerConfig::inter_broker_listener_name`].
/// `DescribeConfigs` reports it at `STATIC_BROKER_CONFIG` when the operator
/// named it, and as null at `DEFAULT_CONFIG` when they did not.
pub(crate) const INTER_BROKER_LISTENER_NAME: &str = "inter.broker.listener.name";

/// KIP-98: how long a transactional id may sit in a terminal or idle state
/// before the transaction coordinator tombstones it out of
/// `__transaction_state`. Kafka defaults it to 604800000 ms (7 days).
///
/// The key is static in Kafka: `kafka-configs --alter` refuses it with
/// `Cannot update these configs dynamically`, and `DescribeConfigs` reports it
/// read-only from the node's own configuration. krabka reports it the same
/// way, out of [`crate::config::BrokerConfig::txn_id_expiration`].
pub(crate) const TRANSACTIONAL_ID_EXPIRATION_MS: &str = "transactional.id.expiration.ms";

/// KIP-98: how often the transactional-id expiry sweep runs. Kafka defaults it
/// to 3600000 ms (1 hour). Static and read-only, like
/// [`TRANSACTIONAL_ID_EXPIRATION_MS`].
pub(crate) const TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS: &str =
    "transaction.remove.expired.transaction.cleanup.interval.ms";

/// KIP-1075: server-side deadline for remote `ListOffsets` work when an older
/// request does not carry `timeout_ms`. Kafka exposes this as a dynamic broker
/// config and defaults it to 30 seconds.
pub(crate) const REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS: &str =
    "remote.list.offsets.request.timeout.ms";
pub(crate) const DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Parse KIP-1075's dynamic broker timeout: Kafka's `LONG` with
/// `atLeast(1)`.
pub(crate) fn parse_remote_list_offsets_timeout(value: &str) -> Result<Duration, String> {
    let key = REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS;
    let millis = check_range(key, parse_long(key, value)?, Some(1), None)?;
    Ok(Duration::from_millis(millis.unsigned_abs()))
}

/// Resolve the per-broker KIP-1075 timeout over the cluster default.
pub(crate) fn resolve_remote_list_offsets_timeout(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
) -> Duration {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        .or_else(|| {
            image
                .default_broker_config()
                .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        })
        .and_then(|value| parse_remote_list_offsets_timeout(value).ok())
        .unwrap_or(DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT)
}

#[cfg(test)]
mod tests {

    use assert2::assert;

    use super::{
        super::validation::{is_recognized, validate_topic_config},
        *,
    };

    /// Register `node_id` and, when `witness` is set, publish
    /// `broker.witness=true` for it. This is the path the broker takes at
    /// registration.
    fn register_node(
        img: &mut krabka_metadata::MetadataImage,
        node_id: u64,
        witness: Option<&str>,
    ) {
        use krabka_metadata::{BrokerConfigRecord, MetadataRecord, NodeId};
        img.apply(&MetadataRecord::V1BrokerRegistration(
            crate::test_support::broker_registration(krabka_raft::NodeId(node_id)),
        ));
        if let Some(value) = witness {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(node_id),
                config_name: BROKER_WITNESS.into(),
                config_value: Some(value.into()),
            }));
        }
    }

    #[test]
    fn resolve_broker_witness_reads_only_an_exact_true() {
        use krabka_metadata::NodeId;
        // (published value, expected role)
        let cases = [
            (None, false),
            (Some(WITNESS_TRUE), true),
            (Some("false"), false),
            (Some("TRUE"), false),
            (Some(""), false),
            (Some("yes"), false),
        ];
        for (value, want) in cases {
            let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
            register_node(&mut img, 1, value);
            assert!(
                resolve_broker_witness(&img, NodeId(1)) == want,
                "broker.witness={value:?}"
            );
        }
    }

    #[test]
    fn resolve_broker_witness_is_false_for_an_unregistered_node() {
        use krabka_metadata::NodeId;
        let img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(!resolve_broker_witness(&img, NodeId(7)));
    }

    #[test]
    fn resolve_broker_witness_does_not_read_the_cluster_default() {
        use krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
        };
        // The role is per node. A cluster default must not turn every broker
        // into a witness.
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        register_node(&mut img, 1, None);
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: BROKER_WITNESS.into(),
            config_value: Some(WITNESS_TRUE.into()),
        }));
        assert!(!resolve_broker_witness(&img, NodeId(1)));
    }

    #[test]
    fn witness_node_ids_collects_every_marked_node() {
        use std::collections::HashSet;

        use krabka_metadata::NodeId;
        let mut img = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(witness_node_ids(&img) == HashSet::new());
        register_node(&mut img, 1, None);
        register_node(&mut img, 2, Some(WITNESS_TRUE));
        register_node(&mut img, 3, Some("false"));
        register_node(&mut img, 4, Some(WITNESS_TRUE));
        assert!(witness_node_ids(&img) == maplit::hashset! {NodeId(2), NodeId(4)});
    }

    #[test]
    fn broker_witness_is_controller_managed_and_not_a_topic_config() {
        assert!(is_controller_managed_broker_config(BROKER_WITNESS));
        assert!(!is_recognized(BROKER_WITNESS));
        assert!(validate_topic_config(BROKER_WITNESS, WITNESS_TRUE).is_err());
    }
}
