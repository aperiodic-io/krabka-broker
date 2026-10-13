//! The share settings of one group on the share-partition leader: each
//! per-group `share.*` override from the group config in the metadata image,
//! with the broker setting as the default.
//!
//! This is Kafka's `ShareGroupConfigProvider`. `SharePartition` asks it for
//! the record lock duration, the delivery count limit, the record lock limit
//! and whether `Renew` is allowed, and `KafkaApis` asks it for the isolation
//! level and for the lock duration that `AcquisitionLockTimeoutMs` reports.
//! An override that does not parse falls back to the default; the config
//! RPCs validate a value before they store it.

use {
    crate::{
        coordinator::unified::{config::clamp_to_range, share::config::ShareGroupConfig},
        features::share_dlq_supported,
    },
    krabka_metadata::MetadataImage,
    std::time::Duration,
};

/// Kafka's `GroupConfig.SHARE_RECORD_LOCK_DURATION_MS_CONFIG`.
const KEY_RECORD_LOCK_DURATION_MS: &str = "share.record.lock.duration.ms";
/// Kafka's `GroupConfig.SHARE_DELIVERY_COUNT_LIMIT_CONFIG`.
const KEY_DELIVERY_COUNT_LIMIT: &str = "share.delivery.count.limit";
/// Kafka's `GroupConfig.SHARE_PARTITION_MAX_RECORD_LOCKS_CONFIG`.
const KEY_PARTITION_MAX_RECORD_LOCKS: &str = "share.partition.max.record.locks";
/// Kafka's `GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG`.
const KEY_ISOLATION_LEVEL: &str = "share.isolation.level";
/// Kafka's `GroupConfig.SHARE_RENEW_ACKNOWLEDGE_ENABLE_CONFIG`.
const KEY_RENEW_ACKNOWLEDGE_ENABLE: &str = "share.renew.acknowledge.enable";
/// Kafka's `GroupConfig.ERRORS_DEADLETTERQUEUE_TOPIC_NAME_CONFIG`.
pub(crate) const KEY_DLQ_TOPIC_NAME: &str = "errors.deadletterqueue.topic.name";
/// Kafka's `GroupConfig.ERRORS_DEADLETTERQUEUE_COPY_RECORD_ENABLE_CONFIG`.
pub(crate) const KEY_DLQ_COPY_RECORD_ENABLE: &str = "errors.deadletterqueue.copy.record.enable";

/// The resolved share settings of one group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupShareSettings {
    /// How long an acquired record stays locked to its member.
    pub(crate) record_lock_duration: Duration,
    /// The delivery count at which a record is archived.
    pub(crate) delivery_count_limit: i16,
    /// The most records a share partition holds in flight.
    pub(crate) max_record_locks: i32,
    /// The group reads only committed transactional data.
    pub(crate) read_committed: bool,
    /// The group allows `Renew` acknowledgements.
    pub(crate) renew_acknowledge_enabled: bool,
    /// The group has a dead-letter queue (KIP-1191): the finalized
    /// `share.version` is 2 or more, and the group names a topic in
    /// `errors.deadletterqueue.topic.name`. Kafka's
    /// `SharePartition.isDLQEnabledForGroup`.
    pub(crate) dlq_enabled: bool,
}

impl GroupShareSettings {
    /// Resolves the settings of `group`: each override in the group config
    /// of `image`, or the broker setting of `defaults`.
    ///
    /// An override is capped to the broker's `group.share.min.*` and
    /// `group.share.max.*` bounds of `defaults`, as Kafka's
    /// `GroupConfig.evaluate` caps a stored group config. The config RPCs
    /// refuse a value outside them when they store it, so one is outside only
    /// when the bounds moved since.
    ///
    /// Kafka has no broker-level share isolation key. A group without a
    /// `share.isolation.level` reads `read_uncommitted`, Kafka's
    /// `GroupConfig.SHARE_ISOLATION_LEVEL_DEFAULT`.
    #[must_use]
    pub(crate) fn resolve(image: &MetadataImage, group: &str, defaults: &ShareGroupConfig) -> Self {
        let overrides = image.group_config(group);
        let value = |key: &str| overrides.and_then(|configs| configs.get(key));
        // Kafka parses each of the three as an `INT`, and a value that does not
        // parse is ignored.
        let int = |key: &str| value(key).and_then(|text| text.trim().parse::<i32>().ok());
        Self {
            record_lock_duration: int(KEY_RECORD_LOCK_DURATION_MS).map_or(
                defaults.record_lock_duration,
                |millis| {
                    clamp_to_range(
                        Duration::from_millis(u64::try_from(millis).unwrap_or(0)),
                        defaults.min_record_lock_duration,
                        defaults.max_record_lock_duration,
                    )
                },
            ),
            delivery_count_limit: int(KEY_DELIVERY_COUNT_LIMIT).map_or(
                defaults.max_delivery_attempts,
                |limit| {
                    clamp_to_range(
                        limit,
                        i32::from(defaults.min_delivery_count_limit),
                        i32::from(defaults.max_delivery_count_limit),
                    )
                    .try_into()
                    .unwrap_or(defaults.max_delivery_count_limit)
                },
            ),
            max_record_locks: int(KEY_PARTITION_MAX_RECORD_LOCKS).map_or(
                defaults.max_inflight_records,
                |locks| {
                    clamp_to_range(
                        locks,
                        defaults.min_partition_max_record_locks,
                        defaults.max_partition_max_record_locks,
                    )
                },
            ),
            read_committed: value(KEY_ISOLATION_LEVEL)
                .is_some_and(|level| level.trim().eq_ignore_ascii_case("read_committed")),
            renew_acknowledge_enabled: value(KEY_RENEW_ACKNOWLEDGE_ENABLE)
                .is_none_or(|enabled| !enabled.trim().eq_ignore_ascii_case("false")),
            dlq_enabled: share_dlq_supported(image)
                && value(KEY_DLQ_TOPIC_NAME).is_some_and(|topic| !topic.trim().is_empty()),
        }
    }

    /// The lock duration in milliseconds, as `AcquisitionLockTimeoutMs`
    /// carries it.
    #[must_use]
    pub(crate) fn record_lock_duration_ms(&self) -> i32 {
        i32::try_from(self.record_lock_duration.as_millis()).unwrap_or(i32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{features::SHARE_VERSION, test_support::string_pairs},
        assert2::assert,
        krabka_metadata::{GroupConfigRecord, MetadataRecord},
    };

    fn image(configs: &[(&str, &str)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "g".into(),
            configs: string_pairs(configs),
        }));
        image
    }

    fn check_settings_rows(
        defaults: &ShareGroupConfig,
        rows: Vec<(&[(&str, &str)], GroupShareSettings)>,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (configs, settings) in rows {
            actual.push(GroupShareSettings::resolve(&image(configs), "g", defaults));
            expected.push(settings);
        }
        assert!(actual == expected);
    }

    #[test]
    fn each_override_replaces_its_broker_default() {
        let defaults = ShareGroupConfig::default();
        let broker = GroupShareSettings {
            record_lock_duration: defaults.record_lock_duration,
            delivery_count_limit: defaults.max_delivery_attempts,
            max_record_locks: defaults.max_inflight_records,
            read_committed: false,
            renew_acknowledge_enabled: true,
            dlq_enabled: false,
        };
        let rows: Vec<(&[(&str, &str)], GroupShareSettings)> = vec![
            (&[], broker),
            (
                &[(KEY_RECORD_LOCK_DURATION_MS, "60000")],
                GroupShareSettings {
                    record_lock_duration: Duration::from_mins(1),
                    ..broker
                },
            ),
            (&[(KEY_DELIVERY_COUNT_LIMIT, "2")], GroupShareSettings {
                delivery_count_limit: 2,
                ..broker
            }),
            (
                &[(KEY_PARTITION_MAX_RECORD_LOCKS, "500")],
                GroupShareSettings {
                    max_record_locks: 500,
                    ..broker
                },
            ),
            (
                &[(KEY_ISOLATION_LEVEL, "read_committed")],
                GroupShareSettings {
                    read_committed: true,
                    ..broker
                },
            ),
            (
                &[(KEY_RENEW_ACKNOWLEDGE_ENABLE, "FALSE")],
                GroupShareSettings {
                    renew_acknowledge_enabled: false,
                    ..broker
                },
            ),
            (&[(KEY_DELIVERY_COUNT_LIMIT, "many")], broker),
        ];
        check_settings_rows(&defaults, rows);
    }

    /// Kafka's `GroupConfig.evaluate` caps a stored value to the broker's
    /// bounds: the record lock duration, the delivery count limit and the
    /// record lock limit each to their `group.share.min.*` and
    /// `group.share.max.*`. A negative value is below every bound.
    #[test]
    fn an_override_outside_the_broker_bounds_is_capped() {
        let defaults = ShareGroupConfig::default();
        let broker = GroupShareSettings::resolve(&image(&[]), "g", &defaults);
        let rows: Vec<(&[(&str, &str)], GroupShareSettings)> = vec![
            (&[(KEY_RECORD_LOCK_DURATION_MS, "1")], GroupShareSettings {
                record_lock_duration: defaults.min_record_lock_duration,
                ..broker
            }),
            (&[(KEY_RECORD_LOCK_DURATION_MS, "-7")], GroupShareSettings {
                record_lock_duration: defaults.min_record_lock_duration,
                ..broker
            }),
            (
                &[(KEY_RECORD_LOCK_DURATION_MS, "3600000")],
                GroupShareSettings {
                    record_lock_duration: defaults.max_record_lock_duration,
                    ..broker
                },
            ),
            (&[(KEY_DELIVERY_COUNT_LIMIT, "1")], GroupShareSettings {
                delivery_count_limit: defaults.min_delivery_count_limit,
                ..broker
            }),
            (
                &[(KEY_DELIVERY_COUNT_LIMIT, "100000")],
                GroupShareSettings {
                    delivery_count_limit: defaults.max_delivery_count_limit,
                    ..broker
                },
            ),
            (
                &[(KEY_PARTITION_MAX_RECORD_LOCKS, "10")],
                GroupShareSettings {
                    max_record_locks: defaults.min_partition_max_record_locks,
                    ..broker
                },
            ),
            (
                &[(KEY_PARTITION_MAX_RECORD_LOCKS, "1000000")],
                GroupShareSettings {
                    max_record_locks: defaults.max_partition_max_record_locks,
                    ..broker
                },
            ),
        ];
        check_settings_rows(&defaults, rows);
    }

    /// Kafka's `isDLQEnabledForGroup`: the group has a queue when the finalized
    /// `share.version` is 2 or more and the group names a topic. A blank name,
    /// no name, or a lower level leaves it off.
    #[test]
    fn a_group_has_a_dead_letter_queue_from_share_version_two_with_a_topic() {
        use krabka_metadata::FeatureLevelRecord;

        // (finalized share.version, topic name, dead-letter queue on)
        let cases = [
            (2, Some("dlq.g"), true),
            (2, Some(" dlq.g "), true),
            (2, Some(""), false),
            (2, Some("  "), false),
            (2, None, false),
            (1, Some("dlq.g"), false),
            (0, Some("dlq.g"), false),
        ];
        let defaults = ShareGroupConfig::default();
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (level, topic, on) in cases {
            let configs: Vec<(&str, &str)> = topic
                .map(|name| (KEY_DLQ_TOPIC_NAME, name))
                .into_iter()
                .collect();
            let mut image = image(&configs);
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: SHARE_VERSION.into(),
                level,
            }));
            actual.push(GroupShareSettings::resolve(&image, "g", &defaults).dlq_enabled);
            expected.push(on);
        }
        assert!(actual == expected);
    }
}
