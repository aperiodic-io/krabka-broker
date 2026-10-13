//! The `KafkaConfig` keys whose value this node runs with, and which
//! `[runtime]` field sets each.
//!
//! `DescribeConfigs` reports a key the operator named, or one this node runs
//! with a value other than Kafka's default, at `STATIC_BROKER_CONFIG` with
//! the value the node uses. Kafka answers from the `KafkaConfig` the process
//! parsed, so a group's `consumer.session.timeout.ms` and a named broker's
//! `group.consumer.session.timeout.ms` both say what the coordinator runs.
//! This table is where krabka reads those values from
//! [`crate::config::BrokerConfig`], so the coordinators, the broker resource
//! and the group resource cannot report different numbers for one setting.
//!
//! [`KafkaStaticKey::runtime_field`] is the `[runtime]` field the loader
//! watches to record provenance in
//! [`StaticConfigOrigins::supplied_kafka_keys`](super::StaticConfigOrigins).
//! A key with no `[runtime]` field is set by code that builds the
//! configuration, and reports at `STATIC_BROKER_CONFIG` once its value leaves
//! Kafka's default.

use {
    super::BrokerConfig,
    crate::{
        api_catalog::UnstableApiVersions,
        config_keys::{
            broker_dynamic::{TOPIC_DEFAULT_SYNONYMS, topic_broker_synonyms},
            group::KAFKA_GROUP_KEYS,
            in_topic_unit,
            kafka_broker::lookup,
        },
    },
    krabka_units::{
        ByteSize, Time,
        convert::{ByteSizeExt as _, TimeExt as _},
    },
    std::time::Duration,
};

/// One `KafkaConfig` key and how this node states its value.
pub(crate) struct KafkaStaticKey {
    /// The Kafka key.
    pub(crate) name: &'static str,
    /// The `[runtime]` field that sets it, when there is one.
    pub(crate) runtime_field: Option<&'static str>,
    /// The value this node runs with, as Kafka's `ConfigDef.convertToString`
    /// renders it.
    pub(crate) value: fn(&BrokerConfig) -> String,
}

fn ms(duration: Duration) -> String { duration.as_millis().to_string() }

fn time_ms(time: Time) -> String { time.millis_i64().to_string() }

fn size(bytes: ByteSize) -> String { bytes.bytes_u64().to_string() }

/// An `INT` key that krabka holds as a wider count: Kafka's parse refuses
/// anything past `Integer.MAX_VALUE`, and its own unlimited is that value.
fn int(count: usize) -> String { i32::try_from(count).unwrap_or(i32::MAX).to_string() }

macro_rules! keys {
    ($($name:literal, $field:expr, |$c:ident| $value:expr;)*) => {
        &[$(KafkaStaticKey {
            name: $name,
            runtime_field: $field,
            value: |$c| $value,
        }),*]
    };
}

/// Every key this node can state a running value of, other than the ones
/// [`crate::config::StaticConfigOrigins`] tracks with a flag of their own.
pub(crate) const KAFKA_STATIC_KEYS: &[KafkaStaticKey] = keys! {
    "group.consumer.session.timeout.ms", Some("consumer_group_session_timeout"),
        |c| ms(c.next_gen_consumer_group.session_timeout);
    "group.consumer.heartbeat.interval.ms", Some("consumer_group_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.heartbeat_interval);
    "group.consumer.assignment.interval.ms", None,
        |c| ms(c.next_gen_consumer_group.assignment_interval);
    "group.consumer.min.session.timeout.ms", Some("consumer_group_min_session_timeout"),
        |c| ms(c.next_gen_consumer_group.min_session_timeout);
    "group.consumer.max.session.timeout.ms", Some("consumer_group_max_session_timeout"),
        |c| ms(c.next_gen_consumer_group.max_session_timeout);
    "group.consumer.min.heartbeat.interval.ms", Some("consumer_group_min_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.min_heartbeat_interval);
    "fetch.max.bytes", Some("fetch_max_bytes"), |c| size(c.fetch_max_bytes);
    "group.consumer.max.heartbeat.interval.ms", Some("consumer_group_max_heartbeat_interval"),
        |c| ms(c.next_gen_consumer_group.max_heartbeat_interval);
    "group.consumer.max.size", Some("consumer_group_max_size"),
        |c| int(c.next_gen_consumer_group.max_size);
    "group.consumer.migration.policy", None,
        |c| c.next_gen_consumer_group.migration_policy.as_str().to_owned();
    "group.initial.rebalance.delay.ms", Some("classic_group_initial_rebalance_delay"),
        |c| time_ms(c.classic_group_initial_rebalance_delay);
    "group.min.session.timeout.ms", Some("classic_group_min_session_timeout"),
        |c| ms(c.next_gen_consumer_group.classic_min_session_timeout);
    "group.max.session.timeout.ms", Some("classic_group_max_session_timeout"),
        |c| ms(c.next_gen_consumer_group.classic_max_session_timeout);
    "group.max.size", Some("classic_group_max_size"),
        |c| int(c.next_gen_consumer_group.classic_max_size);
    "group.share.session.timeout.ms", Some("share_group_session_timeout"),
        |c| ms(c.share_group.session_timeout);
    "group.share.heartbeat.interval.ms", Some("share_group_heartbeat_interval"),
        |c| ms(c.share_group.heartbeat_interval);
    "group.share.assignment.interval.ms", None,
        |c| ms(c.share_group.assignment_interval);
    "group.share.min.session.timeout.ms", Some("share_group_min_session_timeout"),
        |c| ms(c.share_group.min_session_timeout);
    "group.share.max.session.timeout.ms", Some("share_group_max_session_timeout"),
        |c| ms(c.share_group.max_session_timeout);
    "group.share.min.heartbeat.interval.ms", Some("share_group_min_heartbeat_interval"),
        |c| ms(c.share_group.min_heartbeat_interval);
    "group.share.max.heartbeat.interval.ms", Some("share_group_max_heartbeat_interval"),
        |c| ms(c.share_group.max_heartbeat_interval);
    "group.share.max.size", Some("share_group_max_size"),
        |c| int(c.share_group.max_size);
    "group.share.record.lock.duration.ms", Some("share_group_record_lock_duration"),
        |c| ms(c.share_group.record_lock_duration);
    "group.share.min.record.lock.duration.ms", Some("share_group_min_record_lock_duration"),
        |c| ms(c.share_group.min_record_lock_duration);
    "group.share.max.record.lock.duration.ms", Some("share_group_max_record_lock_duration"),
        |c| ms(c.share_group.max_record_lock_duration);
    "group.share.delivery.count.limit", Some("share_group_delivery_count_limit"),
        |c| c.share_group.max_delivery_attempts.to_string();
    "group.share.min.delivery.count.limit", Some("share_group_min_delivery_count_limit"),
        |c| c.share_group.min_delivery_count_limit.to_string();
    "group.share.max.delivery.count.limit", Some("share_group_max_delivery_count_limit"),
        |c| c.share_group.max_delivery_count_limit.to_string();
    "group.share.partition.max.record.locks", Some("share_group_partition_max_record_locks"),
        |c| c.share_group.max_inflight_records.to_string();
    "group.share.min.partition.max.record.locks",
        Some("share_group_min_partition_max_record_locks"),
        |c| c.share_group.min_partition_max_record_locks.to_string();
    "group.share.max.partition.max.record.locks",
        Some("share_group_max_partition_max_record_locks"),
        |c| c.share_group.max_partition_max_record_locks.to_string();

    "group.streams.session.timeout.ms", Some("streams_group_session_timeout"),
        |c| ms(c.streams_group.session_timeout);
    "group.streams.heartbeat.interval.ms", Some("streams_group_heartbeat_interval"),
        |c| ms(c.streams_group.heartbeat_interval);
    "group.streams.assignment.interval.ms", None,
        |c| ms(c.streams_group.assignment_interval);
    "group.streams.initial.rebalance.delay.ms", None,
        |c| ms(c.streams_group.initial_rebalance_delay);
    "group.streams.min.session.timeout.ms", Some("streams_group_min_session_timeout"),
        |c| ms(c.streams_group.min_session_timeout);
    "group.streams.max.session.timeout.ms", Some("streams_group_max_session_timeout"),
        |c| ms(c.streams_group.max_session_timeout);
    "group.streams.min.heartbeat.interval.ms", Some("streams_group_min_heartbeat_interval"),
        |c| ms(c.streams_group.min_heartbeat_interval);
    "group.streams.max.heartbeat.interval.ms", Some("streams_group_max_heartbeat_interval"),
        |c| ms(c.streams_group.max_heartbeat_interval);
    "group.streams.max.size", Some("streams_group_max_size"),
        |c| int(c.streams_group.max_size);
    "group.streams.num.standby.replicas", Some("streams_group_num_standby_replicas"),
        |c| c.streams_group.num_standby_replicas.to_string();
    "group.streams.num.warmup.replicas", Some("streams_group_num_warmup_replicas"),
        |c| c.streams_group.num_warmup_replicas.to_string();
    "group.streams.acceptable.recovery.lag", Some("streams_group_acceptable_recovery_lag"),
        |c| c.streams_group.acceptable_recovery_lag.to_string();
    "group.streams.task.offset.interval.ms", Some("streams_group_task_offset_interval"),
        |c| ms(c.streams_group.task_offset_interval);
    "group.streams.rack.aware.assignment.tags",
        Some("streams_group_rack_aware_assignment_tags"),
        |c| c.streams_group.rack_aware_assignment_tags.join(",");
    "group.streams.assignors", Some("streams_group_assignor"),
        |c| c.streams_group.assignor.config_name().to_owned();

    "share.coordinator.load.buffer.size", Some("share_coordinator_load_buffer_size"),
        |c| size(c.share_coordinator_load_buffer_size);
    "share.coordinator.state.topic.num.partitions", Some("share_state_num_partitions"),
        |c| c.share_coordinator.state_topic_num_partitions.to_string();
    "share.coordinator.state.topic.replication.factor", Some("share_state_replication_factor"),
        |c| c.share_coordinator.state_topic_replication_factor.to_string();
    "share.coordinator.state.topic.segment.bytes", Some("share_state_segment_bytes"),
        |c| size(c.share_coordinator.state_topic_segment_bytes);
    "share.coordinator.state.topic.min.isr", Some("share_state_min_isr"),
        |c| c.share_coordinator.state_topic_min_isr.to_string();
    "share.coordinator.snapshot.update.records.per.snapshot",
        Some("share_snapshot_update_records_per_snapshot"),
        |c| c.share_coordinator.snapshot_update_records_per_snapshot.to_string();
    "share.coordinator.write.timeout.ms", Some("share_coordinator_write_timeout"),
        |c| ms(c.share_coordinator.write_timeout);
    "share.coordinator.state.topic.prune.interval.ms", Some("share_state_prune_interval"),
        |c| ms(c.share_coordinator.state_topic_prune_interval);
    "share.coordinator.cold.partition.snapshot.interval.ms",
        Some("share_cold_partition_snapshot_interval"),
        |c| ms(c.share_coordinator.cold_partition_snapshot_interval);
    "share.coordinator.state.topic.compression.codec", Some("share_state_compression_codec"),
        |c| c.share_coordinator.state_topic_compression_codec.as_attribute_bits().to_string();
    "share.coordinator.threads", Some("share_coordinator_threads"),
        |c| c.share_coordinator.threads.to_string();
    "share.coordinator.append.linger.ms", Some("share_coordinator_append_linger_ms"),
        |c| c.share_coordinator.append_linger.map_or_else(|| "-1".to_owned(), ms);
    "share.coordinator.cached.buffer.max.bytes",
        Some("share_coordinator_cached_buffer_max_bytes"),
        |c| size(c.share_coordinator.cached_buffer_max_bytes);

    "offsets.topic.num.partitions", Some("offsets_topic_num_partitions"),
        |c| c.offsets_topic_num_partitions.to_string();
    "offsets.topic.replication.factor", Some("offsets_topic_replication_factor"),
        |c| c.offsets_topic_replication_factor.to_string();
    "offsets.topic.segment.bytes", Some("offsets_topic_segment_bytes"),
        |c| size(c.offsets_topic_segment_bytes);
    "transaction.state.log.num.partitions", Some("transaction_state_num_partitions"),
        |c| c.transaction_state_num_partitions.to_string();
    "transaction.state.log.replication.factor", Some("transaction_state_replication_factor"),
        |c| c.transaction_state_replication_factor.to_string();
    "transaction.state.log.segment.bytes", Some("transaction_state_segment_bytes"),
        |c| size(c.transaction_state_segment_bytes);
    "transaction.state.log.min.isr", Some("transaction_state_min_isr"),
        |c| c.transaction_state_min_isr.to_string();
    "transaction.max.timeout.ms", Some("transaction_max_timeout"),
        |c| time_ms(c.transaction_max_timeout);
    "transaction.partition.verification.enable",
        Some("transaction_partition_verification_enable"),
        |c| c.transaction_partition_verification_enable.to_string();
    "transaction.abort.timed.out.transaction.cleanup.interval.ms",
        Some("txn_abort_cleanup_interval"),
        |c| time_ms(c.txn_abort_cleanup_interval);

    "socket.request.max.bytes", Some("socket_request_max"),
        |c| size(c.socket_request_max);
    "socket.send.buffer.bytes", Some("socket_send_buffer"),
        |c| size(c.socket_send_buffer);
    "socket.receive.buffer.bytes", Some("socket_receive_buffer"),
        |c| size(c.socket_receive_buffer);
    "queued.max.requests", Some("queued_max_requests"),
        |c| int(c.queued_max_requests);
    "queued.max.request.bytes", Some("queued_max_request_bytes"),
        |c| c.queued_max_request_bytes.map_or_else(|| "-1".to_owned(), size);
    "max.request.partition.size.limit", Some("max_request_partition_size_limit"),
        |c| c.max_request_partition_size_limit.to_string();
    "max.connections", Some("max_connections"),
        |c| int(c.max_connections);
    "max.connections.per.ip", Some("max_connections_per_ip"),
        |c| int(c.max_connections_per_ip);
    "max.incremental.fetch.session.cache.slots",
        Some("max_incremental_fetch_session_cache_slots"),
        |c| int(c.max_incremental_fetch_session_cache_slots);
    "producer.id.expiration.ms", Some("producer_id_expiration"),
        |c| time_ms(c.producer_id_expiration);
    "producer.id.expiration.check.interval.ms", Some("producer_id_expiration_scan_interval"),
        |c| time_ms(c.producer_id_expiration_scan_interval);

    "num.replica.fetchers", Some("replica_fetchers"),
        |c| int(c.replication.fetchers);
    "replica.lag.time.max.ms", Some("replica_lag_time_max"),
        |c| time_ms(c.replica_lag_time_max);
    "leader.imbalance.check.interval.seconds", Some("leader_imbalance_check_interval"),
        |c| (c.leader_imbalance_check_interval.millis_i64() / 1_000).to_string();
    "controller.quorum.fetch.timeout.ms", Some("controller_election_timeout"),
        |c| time_ms(c.controller_election_timeout);
    "metadata.log.max.record.bytes.between.snapshots",
        Some("metadata_max_bytes_between_snapshots"),
        |c| size(c.metadata_max_bytes_between_snapshots);
    "metadata.log.max.snapshot.interval.ms", Some("metadata_max_snapshot_interval"),
        |c| time_ms(c.metadata_max_snapshot_interval);
    "log.roll.jitter.ms", None, |c| time_ms(c.log_config.segment_jitter);
    "log.segment.delete.delay.ms", None, |c| time_ms(c.log_config.file_delete_delay);
    "log.flush.interval.messages", None, |c| c.log_config.flush_messages.map_or_else(|| i64::MAX.to_string(), |n| n.to_string());
    "log.flush.interval.ms", None, |c| c.log_config.flush_interval.map_or_else(|| i64::MAX.to_string(), time_ms);
    "compression.gzip.level", None, |c| c.log_config.compression_gzip_level.to_string();
    "compression.lz4.level", None, |c| c.log_config.compression_lz4_level.to_string();
    "compression.zstd.level", None, |c| c.log_config.compression_zstd_level.to_string();

    "metadata.log.segment.bytes", Some("metadata_log_segment_bytes"),
        |c| size(c.metadata_log.segment_size);
    "metadata.log.segment.ms", Some("metadata_log_segment_roll_interval"),
        |c| time_ms(c.metadata_log.segment_roll_interval);
    "metadata.max.retention.bytes", Some("metadata_max_retention_bytes"),
        |c| c.metadata_log.max_retention_size.map_or_else(|| "-1".to_owned(), size);
    "metadata.max.retention.ms", Some("metadata_max_retention"),
        |c| c.metadata_log.max_retention.map_or_else(|| "-1".to_owned(), time_ms);
    "metadata.max.idle.interval.ms", Some("metadata_max_idle_interval"),
        |c| time_ms(c.metadata_log.max_idle_interval);
    "broker.heartbeat.interval.ms", Some("heartbeat_interval"),
        |c| time_ms(c.heartbeat_interval);
    "broker.session.timeout.ms", Some("heartbeat_timeout"),
        |c| time_ms(c.heartbeat_timeout);

    "remote.log.storage.system.enable", None,
        |c| c.remote_storage_backend.is_some().to_string();
    "remote.log.manager.task.interval.ms", Some("remote_log_manager_interval"),
        |c| time_ms(c.remote_log_manager_interval);
    "remote.log.index.file.cache.total.size.bytes", None,
        |c| size(c.remote_index_cache_size);
    "remote.log.manager.copier.thread.pool.size", None,
        |c| int(c.remote_copier_threads);
    "remote.log.manager.expiration.thread.pool.size", None,
        |c| int(c.remote_expiration_threads);
    "remote.log.reader.threads", None,
        |c| int(c.remote_reader_threads);
    "remote.log.reader.max.pending.tasks", None,
        |c| int(c.remote_reader_max_pending_tasks);

    "delegation.token.max.lifetime.ms", Some("delegation_token_max_lifetime"),
        |c| time_ms(c.delegation_token_max_lifetime);
    "delegation.token.expiry.check.interval.ms", Some("delegation_token_expiry_check_interval"),
        |c| time_ms(c.delegation_token_expiry_check_interval);
    "delegation.token.expiry.time.ms", Some("delegation_token_default_renew_period"),
        |c| time_ms(c.delegation_token_default_renew_period);
};

impl KafkaStaticKey {
    /// Kafka's effective default, following topic broker synonyms with unit
    /// conversion when the direct row has no default, or the group default
    /// for a group synonym that Kafka 4.3.1 does not define.
    pub(crate) fn kafka_default(&self) -> Option<String> {
        lookup(self.name)
            .and_then(|row| row.default)
            .or_else(|| {
                KAFKA_GROUP_KEYS
                    .iter()
                    .find(|group_key| group_key.broker_synonym == Some(self.name))
                    .and_then(|group_key| group_key.default)
            })
            .map(str::to_owned)
            .or_else(|| {
                let (_, topic) = TOPIC_DEFAULT_SYNONYMS
                    .iter()
                    .find(|(broker, _)| *broker == self.name)?;
                topic_broker_synonyms(topic)
                    .iter()
                    .find_map(|synonym| in_topic_unit(topic, synonym.name, synonym.default?))
            })
    }

    /// The value `config` runs the key with, when it is a static setting: the
    /// operator named the key, or the node runs another value than Kafka's
    /// default. `None` is a key `DescribeConfigs` reports at its default.
    pub(crate) fn static_value(&self, config: &BrokerConfig) -> Option<String> {
        // Kafka 4.3.1 has no `group.streams.assignors` broker key. Krabka's
        // Streams assignor setting is part of trunk's GroupConfig surface and
        // is only visible when Kafka's unstable API versions are enabled.
        if self.name == "group.streams.assignors"
            && config.features.unstable_api_versions == UnstableApiVersions::Disabled
        {
            return None;
        }
        let value = (self.value)(config);
        let supplied = config
            .static_config_origins
            .supplied_kafka_keys
            .contains(self.name);
        (supplied || self.kafka_default().as_deref() != Some(value.as_str())).then_some(value)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{api_catalog::UnstableApiVersions, file_config::RuntimeFileConfig},
        assert2::check,
    };

    /// The keys whose value a default `BrokerConfig` runs differently from
    /// Kafka 4.3.1's default: the 1 MiB socket buffers that the network layer
    /// is tuned with, the 5 s follower fetch watchdog of the metadata quorum,
    /// and the 3 s broker heartbeat. The Streams assignor is trunk-only and
    /// has its own unstable-mode test below.
    const KRABKA_OWN_DEFAULTS: &[(&str, &str)] = &[
        ("socket.send.buffer.bytes", "1048576"),
        ("socket.receive.buffer.bytes", "1048576"),
        ("controller.quorum.fetch.timeout.ms", "5000"),
        ("broker.heartbeat.interval.ms", "3000"),
    ];

    /// Every key is one Kafka defines, or a group synonym that names a default,
    /// or the streams assignor list, which Kafka 4.3.1 has no default for.
    #[test]
    fn every_key_has_a_kafka_default() {
        let without: Vec<&str> = KAFKA_STATIC_KEYS
            .iter()
            .filter(|key| key.kafka_default().is_none())
            .map(|key| key.name)
            .collect();

        check!(without == vec!["group.streams.assignors"]);
    }

    #[test]
    fn no_key_is_listed_twice() {
        let mut names: Vec<&str> = KAFKA_STATIC_KEYS.iter().map(|key| key.name).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();

        check!(names.len() == count);
    }

    /// A `[runtime]` field named here has to exist, or a key the operator
    /// names would never be recorded.
    #[test]
    fn every_runtime_field_is_one_the_table_has() {
        let serde_json::Value::Object(fields) = serde_json::to_value(RuntimeFileConfig::default())
            .expect("serialize the runtime table")
        else {
            panic!("the runtime table serializes as an object");
        };
        let missing: Vec<&str> = KAFKA_STATIC_KEYS
            .iter()
            .filter_map(|key| key.runtime_field)
            .filter(|field| !fields.contains_key(*field))
            .collect();

        check!(missing.is_empty(), "{missing:?}");
    }

    /// A node that named nothing states a key only where krabka's own default
    /// is not Kafka's, which is where it has to say what it runs.
    #[test]
    fn a_default_config_states_only_the_defaults_krabka_changed() {
        let config = BrokerConfig::default();
        let stated: Vec<(&str, String)> = KAFKA_STATIC_KEYS
            .iter()
            .filter_map(|key| key.static_value(&config).map(|value| (key.name, value)))
            .collect();
        let expected: Vec<(&str, String)> = KRABKA_OWN_DEFAULTS
            .iter()
            .map(|(name, value)| (*name, (*value).to_owned()))
            .collect();

        check!(stated == expected);
    }

    #[test]
    fn streams_assignors_is_disclosed_only_with_unstable_api_versions() {
        let mut config = BrokerConfig::default();
        let key = KAFKA_STATIC_KEYS
            .iter()
            .find(|key| key.name == "group.streams.assignors")
            .expect("Streams assignor key");

        check!(key.static_value(&config).is_none());
        config.features.unstable_api_versions = UnstableApiVersions::Enabled;
        check!(key.static_value(&config).as_deref() == Some("auto"));
    }

    /// The five `MetadataLogConfig` keys state the value the metadata log
    /// runs with, once it leaves Kafka's default. A retention limit that is
    /// not set states Kafka's `-1`, which is how Kafka writes "no limit".
    #[test]
    fn the_metadata_log_keys_state_what_the_metadata_log_runs_with() {
        use krabka_units::{bytes, hours, mebibytes, millis};

        const METADATA_LOG_KEYS: [&str; 5] = [
            "metadata.log.segment.bytes",
            "metadata.log.segment.ms",
            "metadata.max.retention.bytes",
            "metadata.max.retention.ms",
            "metadata.max.idle.interval.ms",
        ];
        let stated = |metadata_log: krabka_raft::MetadataLogConfig| -> Vec<(&str, String)> {
            let config = BrokerConfig {
                metadata_log,
                ..BrokerConfig::default()
            };
            KAFKA_STATIC_KEYS
                .iter()
                .filter(|key| METADATA_LOG_KEYS.contains(&key.name))
                .filter_map(|key| key.static_value(&config).map(|value| (key.name, value)))
                .collect()
        };
        let cases = [
            (
                "Kafka's defaults",
                krabka_raft::MetadataLogConfig::default(),
                vec![],
            ),
            (
                "the no-op records off, as the test profile runs",
                krabka_raft::MetadataLogConfig {
                    max_idle_interval: millis(0),
                    ..krabka_raft::MetadataLogConfig::default()
                },
                vec![("metadata.max.idle.interval.ms", "0")],
            ),
            (
                "no retention limit",
                krabka_raft::MetadataLogConfig {
                    max_retention_size: None,
                    max_retention: None,
                    ..krabka_raft::MetadataLogConfig::default()
                },
                vec![
                    ("metadata.max.retention.bytes", "-1"),
                    ("metadata.max.retention.ms", "-1"),
                ],
            ),
            (
                "every key changed",
                krabka_raft::MetadataLogConfig {
                    segment_size: mebibytes(16),
                    segment_roll_interval: hours(1),
                    max_retention_size: Some(bytes(0)),
                    max_retention: Some(millis(0)),
                    max_idle_interval: millis(250),
                },
                vec![
                    ("metadata.log.segment.bytes", "16777216"),
                    ("metadata.log.segment.ms", "3600000"),
                    ("metadata.max.retention.bytes", "0"),
                    ("metadata.max.retention.ms", "0"),
                    ("metadata.max.idle.interval.ms", "250"),
                ],
            ),
        ];

        for (label, metadata_log, expected) in cases {
            let expected: Vec<(&str, String)> = expected
                .into_iter()
                .map(|(name, value)| (name, value.to_owned()))
                .collect();
            check!(stated(metadata_log) == expected, "{label}");
        }
    }
}
