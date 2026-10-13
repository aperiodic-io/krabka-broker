//! KIP-932 share-group membership configuration.
use {crate::coordinator::unified::config::DEFAULT_ASSIGNMENT_INTERVAL, std::time::Duration};

/// Kafka's `GroupConfig.SHARE_SESSION_TIMEOUT_MS_CONFIG`.
const KEY_SHARE_SESSION_TIMEOUT_MS: &str = "share.session.timeout.ms";
/// Kafka's `GroupConfig.SHARE_HEARTBEAT_INTERVAL_MS_CONFIG`.
const KEY_SHARE_HEARTBEAT_INTERVAL_MS: &str = "share.heartbeat.interval.ms";
/// Kafka's `GroupConfig.SHARE_ASSIGNMENT_INTERVAL_MS_CONFIG`.
const KEY_SHARE_ASSIGNMENT_INTERVAL_MS: &str = "share.assignment.interval.ms";

crate::coordinator::unified::config::membership_config_type! {
    /// The broker share-group settings: the share keys of Kafka's
    /// `GroupCoordinatorConfig` and of its `ShareGroupConfig`.
    ///
    /// Kafka has no broker share isolation key. A group reads with its own
    /// `share.isolation.level`, which defaults to `read_uncommitted`.
    ///
    /// Each `min_*` and `max_*` pair bounds the broker value beside it and the
    /// matching per-group override. The `[runtime]` applier checks Kafka's
    /// ranges and the order within each triple at startup.
    #[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
    pub struct ShareGroupConfig;
    prefix {}
    /// Kafka's `group.share.session.timeout.ms`.
    session_timeout;
    /// Kafka's `group.share.heartbeat.interval.ms`.
    heartbeat_interval;
    before_bounds {
        /// Kafka's `group.share.assignment.interval.ms`: the least time between
        /// two target assignments of a group. Zero does not wait.
        #[default(DEFAULT_ASSIGNMENT_INTERVAL)]
        pub assignment_interval: Duration,
    }
    bounds {
        /// Kafka's `group.share.min.session.timeout.ms`.
        min_session_timeout;
        /// Kafka's `group.share.max.session.timeout.ms`.
        max_session_timeout;
        /// Kafka's `group.share.min.heartbeat.interval.ms`.
        min_heartbeat_interval;
        /// Kafka's `group.share.max.heartbeat.interval.ms`.
        max_heartbeat_interval;
    }
    suffix {
        /// Kafka's `group.share.max.size`.
        #[default(200)]
        pub max_size: usize,
        /// Kafka's `group.share.record.lock.duration.ms`.
        #[default(Duration::from_secs(30))]
        pub record_lock_duration: Duration,
        /// Kafka's `group.share.min.record.lock.duration.ms`.
        #[default(Duration::from_secs(15))]
        pub min_record_lock_duration: Duration,
        /// Kafka's `group.share.max.record.lock.duration.ms`.
        #[default(Duration::from_mins(1))]
        pub max_record_lock_duration: Duration,
        /// Kafka's `group.share.delivery.count.limit`: the delivery count at
        /// which a record is archived.
        #[default(5)]
        pub max_delivery_attempts: i16,
        /// Kafka's `group.share.min.delivery.count.limit`.
        #[default(2)]
        pub min_delivery_count_limit: i16,
        /// Kafka's `group.share.max.delivery.count.limit`.
        #[default(10)]
        pub max_delivery_count_limit: i16,
        /// Kafka's `group.share.partition.max.record.locks`: the most records a
        /// share partition holds in flight.
        #[default(2000)]
        pub max_inflight_records: i32,
        /// Kafka's `group.share.min.partition.max.record.locks`.
        #[default(100)]
        pub min_partition_max_record_locks: i32,
        /// Kafka's `group.share.max.partition.max.record.locks`.
        #[default(4000)]
        pub max_partition_max_record_locks: i32,
        #[default(Duration::from_secs(15))]
        pub backlog_poll_interval: Duration,
        #[default(64)]
        pub actor_mailbox_capacity: usize,
        /// Kafka's internal `group.share.initialize.retry.interval.ms`: how long a
        /// partition may stay initializing before the group asks the persister to
        /// initialize it again.
        #[default(Duration::from_secs(30))]
        pub initialize_retry_interval: Duration,
    }
}

impl ShareGroupConfig {
    crate::coordinator::unified::config::membership_config_methods! {
    /// The membership settings a share group runs with: each `share.*`
    /// override in the group's stored config over the broker value, clamped to
    /// the broker's `group.share.min.*` and `group.share.max.*` bounds.
    ///
    /// This is Kafka's `GroupMetadataManager.shareGroupSessionTimeoutMs`,
    /// `shareGroupHeartbeatIntervalMs` and `shareGroupAssignmentIntervalMs`:
    /// `GroupConfigManager.groupConfig` over `GroupCoordinatorConfig`, with the
    /// stored config evaluated against the bounds (`GroupConfig.evaluate`). A
    /// group with no override borrows the broker value.
        KEY_SHARE_SESSION_TIMEOUT_MS, KEY_SHARE_HEARTBEAT_INTERVAL_MS, KEY_SHARE_ASSIGNMENT_INTERVAL_MS
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::config::MAX_ASSIGNMENT_INTERVAL, test_support::string_pairs,
        },
        assert2::assert,
        std::{borrow::Cow, collections::BTreeMap},
    };

    /// The defaults of the share keys of Kafka 4.3.1's
    /// `GroupCoordinatorConfig` and of its `ShareGroupConfig`.
    #[test]
    fn defaults_are_kafkas() {
        let expected = ShareGroupConfig {
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            assignment_interval: Duration::from_secs(1),
            min_session_timeout: Duration::from_secs(45),
            max_session_timeout: Duration::from_mins(1),
            min_heartbeat_interval: Duration::from_secs(5),
            max_heartbeat_interval: Duration::from_secs(15),
            max_size: 200,
            record_lock_duration: Duration::from_secs(30),
            min_record_lock_duration: Duration::from_secs(15),
            max_record_lock_duration: Duration::from_mins(1),
            max_delivery_attempts: 5,
            min_delivery_count_limit: 2,
            max_delivery_count_limit: 10,
            max_inflight_records: 2000,
            min_partition_max_record_locks: 100,
            max_partition_max_record_locks: 4000,
            backlog_poll_interval: Duration::from_secs(15),
            actor_mailbox_capacity: 64,
            initialize_retry_interval: Duration::from_secs(30),
        };
        assert!(ShareGroupConfig::default() == expected);
    }

    /// Kafka's `shareGroupSessionTimeoutMs`, `shareGroupHeartbeatIntervalMs`
    /// and `shareGroupAssignmentIntervalMs`: a `share.*` override replaces the
    /// broker value, another coordinator's key and a value that does not parse
    /// leave it.
    #[test]
    fn for_group_applies_each_share_override() {
        let broker = ShareGroupConfig::default();
        let with = |entries: &[(&str, &str)]| {
            let overrides: BTreeMap<String, String> = string_pairs(entries);
            broker.for_group(Some(&overrides)).into_owned()
        };
        let rows: Vec<(&[(&str, &str)], ShareGroupConfig)> = vec![
            (&[], broker.clone()),
            (&[("share.session.timeout.ms", "50000")], ShareGroupConfig {
                session_timeout: Duration::from_secs(50),
                ..broker.clone()
            }),
            (
                &[("share.heartbeat.interval.ms", "7000")],
                ShareGroupConfig {
                    heartbeat_interval: Duration::from_secs(7),
                    ..broker.clone()
                },
            ),
            (&[("share.assignment.interval.ms", "0")], ShareGroupConfig {
                assignment_interval: Duration::ZERO,
                ..broker.clone()
            }),
            (
                &[
                    ("consumer.session.timeout.ms", "50000"),
                    ("share.heartbeat.interval.ms", "many"),
                ],
                broker.clone(),
            ),
            // `GroupConfig.evaluate` caps a value to the broker's bounds.
            (&[("share.session.timeout.ms", "1000")], ShareGroupConfig {
                session_timeout: broker.min_session_timeout,
                ..broker.clone()
            }),
            (
                &[("share.session.timeout.ms", "3600000")],
                ShareGroupConfig {
                    session_timeout: broker.max_session_timeout,
                    ..broker.clone()
                },
            ),
            (&[("share.heartbeat.interval.ms", "0")], ShareGroupConfig {
                heartbeat_interval: broker.min_heartbeat_interval,
                ..broker.clone()
            }),
            (
                &[("share.heartbeat.interval.ms", "60000")],
                ShareGroupConfig {
                    heartbeat_interval: broker.max_heartbeat_interval,
                    ..broker.clone()
                },
            ),
            (
                &[("share.assignment.interval.ms", "3600000")],
                ShareGroupConfig {
                    assignment_interval: MAX_ASSIGNMENT_INTERVAL,
                    ..broker.clone()
                },
            ),
        ];
        let (actual, expected): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .map(|(entries, config)| (with(entries), config))
            .unzip();
        assert!(actual == expected);
        assert!(matches!(broker.for_group(None), Cow::Borrowed(_)));
    }
}
