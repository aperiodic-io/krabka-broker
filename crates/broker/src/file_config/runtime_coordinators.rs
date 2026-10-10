//! The `[runtime]` appliers for the group coordinators.
//!
//! `apply_coordinators` covers the classic and consumer group protocol
//! timings, and `apply_share_group` and `apply_streams_group` cover the KIP-932
//! share groups and the KIP-1071 streams groups. All three write the same
//! coordinator layer, and all three validate the durations Kafka bounds.

use std::{ops::RangeInclusive, time::Duration};

use krabka_units::{Time, convert::TimeExt as _};

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        invalid_runtime_value, nonnegative_time, positive_time, positive_usize,
        whole_millis_i32_time,
    },
};
use crate::coordinator::unified::share::config::ShareGroupConfig;

impl RuntimeFileConfig {
    pub(super) fn apply_coordinators(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! {
            runtime => cfg;
            positive_time: coordinator_session_expiry_tick, coordinator_shutdown_ack_timeout;
            duration: consumer_group_session_timeout => next_gen_consumer_group.session_timeout,
                consumer_group_heartbeat_interval => next_gen_consumer_group.heartbeat_interval,
                consumer_group_min_session_timeout => next_gen_consumer_group.min_session_timeout,
                consumer_group_max_session_timeout => next_gen_consumer_group.max_session_timeout,
                consumer_group_min_heartbeat_interval
                    => next_gen_consumer_group.min_heartbeat_interval,
                consumer_group_max_heartbeat_interval
                    => next_gen_consumer_group.max_heartbeat_interval;
            positive_usize: consumer_group_max_size => next_gen_consumer_group.max_size;
            // Zero is Kafka's own development setting for
            // `group.initial.rebalance.delay.ms`, so this one is not held to
            // `positive_time`.
            nonnegative_time: classic_group_initial_rebalance_delay;
            duration: classic_group_min_session_timeout
                    => next_gen_consumer_group.classic_min_session_timeout,
                classic_group_max_session_timeout
                    => next_gen_consumer_group.classic_max_session_timeout;
            positive_usize: classic_group_max_size => next_gen_consumer_group.classic_max_size;
            positive_time: sync_group_follower_wait;
        }
        Ok(())
    }

    /// Applies the share-group keys with the ranges of Kafka's
    /// `GroupCoordinatorConfig` and `ShareGroupConfig` `ConfigDef`, then
    /// checks the order within each value, minimum and maximum triple over
    /// the resulting config, as their constructors `require` it.
    ///
    /// The order check runs over `cfg` rather than over this table alone, so
    /// a command-line overlay applied after the file is checked against the
    /// file's values.
    pub(super) fn apply_share_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        let share = &mut *cfg.share_group;
        set_runtime! {
            runtime => share;
            int_millis: share_group_session_timeout => session_timeout in AT_LEAST_ONE_MS,
                share_group_heartbeat_interval => heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_min_session_timeout => min_session_timeout in AT_LEAST_ONE_MS,
                share_group_max_session_timeout => max_session_timeout in AT_LEAST_ONE_MS,
                share_group_min_heartbeat_interval => min_heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_max_heartbeat_interval => max_heartbeat_interval in AT_LEAST_ONE_MS,
                share_group_record_lock_duration => record_lock_duration in 1_000..=3_600_000,
                share_group_min_record_lock_duration
                    => min_record_lock_duration in 1_000..=30_000,
                share_group_max_record_lock_duration
                    => max_record_lock_duration in 30_000..=3_600_000;
            in_range: share_group_max_size => max_size in 1..=1_000,
                share_group_delivery_count_limit => max_delivery_attempts in 2..=10,
                share_group_min_delivery_count_limit => min_delivery_count_limit in 2..=5,
                share_group_max_delivery_count_limit => max_delivery_count_limit in 5..=25,
                share_group_partition_max_record_locks => max_inflight_records in 100..=10_000,
                share_group_min_partition_max_record_locks
                    => min_partition_max_record_locks in 100..=2_000,
                share_group_max_partition_max_record_locks
                    => max_partition_max_record_locks in 2_000..=10_000;
        }
        validate_share_group_order(share)?;
        set_runtime! {
            runtime => share;
            duration: share_group_backlog_poll_interval => backlog_poll_interval;
        }
        Ok(())
    }

    pub(super) fn apply_streams_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! {
            runtime => cfg;
            plain: streams_group_enable => streams_group.enable;
            duration: streams_group_session_timeout => streams_group.session_timeout,
                streams_group_heartbeat_interval => streams_group.heartbeat_interval,
                streams_group_min_session_timeout => streams_group.min_session_timeout,
                streams_group_max_session_timeout => streams_group.max_session_timeout,
                streams_group_min_heartbeat_interval => streams_group.min_heartbeat_interval,
                streams_group_max_heartbeat_interval => streams_group.max_heartbeat_interval;
            positive_usize: streams_group_max_size => streams_group.max_size;
        }
        if let Some(value) = runtime.streams_group_num_standby_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_standby_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_standby_replicas = value;
        }
        if let Some(entries) = runtime.streams_group_rack_aware_assignment_tags.take() {
            use crate::coordinator::unified::streams::config::parse_broker_rack_aware_assignment_tags;
            cfg.streams_group.rack_aware_assignment_tags =
                parse_broker_rack_aware_assignment_tags(&entries).map_err(|message| {
                    invalid_runtime_value("streams_group_rack_aware_assignment_tags", message)
                })?;
        }
        if let Some(value) = runtime.streams_group_num_warmup_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_warmup_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_warmup_replicas = value;
        }
        if let Some(value) = runtime.streams_group_acceptable_recovery_lag {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_acceptable_recovery_lag",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.acceptable_recovery_lag = value;
        }
        set_runtime! {
            runtime => cfg;
            duration: streams_group_task_offset_interval => streams_group.task_offset_interval;
        }
        if let Some(value) = runtime.streams_group_assignor.take() {
            use crate::coordinator::unified::streams::config::StreamsAssignorKind;
            cfg.streams_group.assignor =
                StreamsAssignorKind::from_config_name(&value).ok_or_else(|| {
                    invalid_runtime_value(
                        "streams_group_assignor",
                        "expected `auto`, `sticky`, or `highly-available`",
                    )
                })?;
        }

        if let Some(value) = runtime.inter_broker_server_name.take() {
            cfg.inter_broker_server_name = value;
        }
        Ok(())
    }
}

/// Kafka's `atLeast(1)` over an `INT` millisecond key.
const AT_LEAST_ONE_MS: RangeInclusive<i64> = 1..=2_147_483_647;

/// A whole number of milliseconds within `range`: Kafka's `between` or
/// `atLeast` over an `INT` key.
fn int_millis(
    name: &str,
    value: Time,
    range: RangeInclusive<i64>,
) -> Result<Duration, FileConfigError> {
    let value = whole_millis_i32_time(name, value)?;
    if range.contains(&value.millis_i64()) {
        Ok(value.to_std())
    } else {
        Err(invalid_runtime_value(
            name,
            format!("must be within {}ms..={}ms", range.start(), range.end()),
        ))
    }
}

/// `value` within `range`: Kafka's `between` over an `INT` key.
fn in_range<T: PartialOrd + std::fmt::Display>(
    name: &str,
    value: T,
    range: RangeInclusive<T>,
) -> Result<T, FileConfigError> {
    if range.contains(&value) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            format!("must be within {}..={}", range.start(), range.end()),
        ))
    }
}

/// The `require` checks of Kafka's `GroupCoordinatorConfig` and
/// `ShareGroupConfig` constructors over the share keys, in Kafka's order and
/// with Kafka's messages, each naming the `[runtime]` field for the Kafka key.
fn validate_share_group_order(share: &ShareGroupConfig) -> Result<(), FileConfigError> {
    const AT_LEAST: &str = "must be greater than or equal to";
    const AT_MOST: &str = "must be less than or equal to";
    // (whether the check holds, the field it names, the relation, the field
    // it compares against)
    let checks = [
        (
            share.max_heartbeat_interval >= share.min_heartbeat_interval,
            "share_group_max_heartbeat_interval",
            AT_LEAST,
            "share_group_min_heartbeat_interval",
        ),
        (
            share.heartbeat_interval >= share.min_heartbeat_interval,
            "share_group_heartbeat_interval",
            AT_LEAST,
            "share_group_min_heartbeat_interval",
        ),
        (
            share.heartbeat_interval <= share.max_heartbeat_interval,
            "share_group_heartbeat_interval",
            AT_MOST,
            "share_group_max_heartbeat_interval",
        ),
        (
            share.max_session_timeout >= share.min_session_timeout,
            "share_group_max_session_timeout",
            AT_LEAST,
            "share_group_min_session_timeout",
        ),
        (
            share.session_timeout >= share.min_session_timeout,
            "share_group_session_timeout",
            AT_LEAST,
            "share_group_min_session_timeout",
        ),
        (
            share.session_timeout <= share.max_session_timeout,
            "share_group_session_timeout",
            AT_MOST,
            "share_group_max_session_timeout",
        ),
        (
            share.heartbeat_interval < share.session_timeout,
            "share_group_heartbeat_interval",
            "must be less than",
            "share_group_session_timeout",
        ),
        (
            share.max_delivery_count_limit >= share.max_delivery_attempts,
            "share_group_max_delivery_count_limit",
            AT_LEAST,
            "share_group_delivery_count_limit",
        ),
        (
            share.max_delivery_attempts >= share.min_delivery_count_limit,
            "share_group_delivery_count_limit",
            AT_LEAST,
            "share_group_min_delivery_count_limit",
        ),
        (
            share.max_partition_max_record_locks >= share.max_inflight_records,
            "share_group_max_partition_max_record_locks",
            AT_LEAST,
            "share_group_partition_max_record_locks",
        ),
        (
            share.max_inflight_records >= share.min_partition_max_record_locks,
            "share_group_partition_max_record_locks",
            AT_LEAST,
            "share_group_min_partition_max_record_locks",
        ),
        (
            share.record_lock_duration >= share.min_record_lock_duration,
            "share_group_record_lock_duration",
            AT_LEAST,
            "share_group_min_record_lock_duration",
        ),
        (
            share.max_record_lock_duration >= share.record_lock_duration,
            "share_group_max_record_lock_duration",
            AT_LEAST,
            "share_group_record_lock_duration",
        ),
    ];
    match checks.into_iter().find(|(holds, ..)| !holds) {
        Some((_, name, relation, other)) => {
            Err(invalid_runtime_value(name, format!("{relation} {other}")))
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    fn applied_runtime(body: &str) -> Result<crate::config::BrokerConfig, String> {
        let file: crate::file_config::FileConfig =
            toml::from_str(&format!("[runtime]\n{body}\n")).expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).map_err(|error| error.to_string())?;
        Ok(cfg)
    }

    use assert2::assert;

    use super::*;

    fn with(change: fn(&mut ShareGroupConfig)) -> Result<ShareGroupConfig, String> {
        let mut config = ShareGroupConfig::default();
        change(&mut config);
        if config == ShareGroupConfig::default() {
            return Err("the row changes nothing".to_owned());
        }
        Ok(config)
    }

    fn refused(message: &str) -> Result<ShareGroupConfig, String> {
        Err(format!("invalid config: {message}"))
    }

    /// Kafka 4.3.1's `ShareGroupConfig` and `GroupCoordinatorConfig` ranges
    /// and `require` checks over the share keys: each `[runtime]` body gives
    /// the applied share config or the startup error.
    #[test]
    fn share_group_keys_follow_kafkas_ranges_and_order() {
        let rows: Vec<(&str, &str, Result<ShareGroupConfig, String>)> = vec![
            ("defaults", "", Ok(ShareGroupConfig::default())),
            (
                "record lock limit at its floor",
                "share_group_partition_max_record_locks = 100",
                with(|c| c.max_inflight_records = 100),
            ),
            (
                "record lock limit below 100",
                "share_group_partition_max_record_locks = 99",
                refused("share_group_partition_max_record_locks: must be within 100..=10000"),
            ),
            (
                "record lock limit above 10000",
                "share_group_partition_max_record_locks = 10001",
                refused("share_group_partition_max_record_locks: must be within 100..=10000"),
            ),
            (
                "record lock limit above its maximum",
                "share_group_partition_max_record_locks = 5000",
                refused(
                    "share_group_max_partition_max_record_locks: must be greater than or equal \
                     to share_group_partition_max_record_locks",
                ),
            ),
            (
                "record lock limit raised with its maximum",
                "share_group_partition_max_record_locks = 5000\n\
                 share_group_max_partition_max_record_locks = 5000",
                with(|c| {
                    c.max_inflight_records = 5000;
                    c.max_partition_max_record_locks = 5000;
                }),
            ),
            (
                "record lock limit below its minimum",
                "share_group_partition_max_record_locks = 200\n\
                 share_group_min_partition_max_record_locks = 300",
                refused(
                    "share_group_partition_max_record_locks: must be greater than or equal to \
                     share_group_min_partition_max_record_locks",
                ),
            ),
            (
                "delivery count limit of 1",
                "share_group_delivery_count_limit = 1",
                refused("share_group_delivery_count_limit: must be within 2..=10"),
            ),
            (
                "delivery count limit of 11",
                "share_group_delivery_count_limit = 11",
                refused("share_group_delivery_count_limit: must be within 2..=10"),
            ),
            (
                "delivery count limit above its maximum",
                "share_group_delivery_count_limit = 8\nshare_group_max_delivery_count_limit = 6",
                refused(
                    "share_group_max_delivery_count_limit: must be greater than or equal to \
                     share_group_delivery_count_limit",
                ),
            ),
            (
                "maximum delivery count limit of 25",
                "share_group_max_delivery_count_limit = 25",
                with(|c| c.max_delivery_count_limit = 25),
            ),
            (
                "record lock duration below 1s",
                "share_group_record_lock_duration = \"999ms\"",
                refused("share_group_record_lock_duration: must be within 1000ms..=3600000ms"),
            ),
            (
                "record lock duration above its maximum",
                "share_group_record_lock_duration = \"90s\"",
                refused(
                    "share_group_max_record_lock_duration: must be greater than or equal to \
                     share_group_record_lock_duration",
                ),
            ),
            (
                "record lock duration below its minimum",
                "share_group_record_lock_duration = \"10s\"",
                refused(
                    "share_group_record_lock_duration: must be greater than or equal to \
                     share_group_min_record_lock_duration",
                ),
            ),
            (
                "record lock duration and its minimum lowered",
                "share_group_record_lock_duration = \"10s\"\n\
                 share_group_min_record_lock_duration = \"5s\"",
                with(|c| {
                    c.record_lock_duration = Duration::from_secs(10);
                    c.min_record_lock_duration = Duration::from_secs(5);
                }),
            ),
            (
                "minimum record lock duration above 30s",
                "share_group_min_record_lock_duration = \"31s\"",
                refused("share_group_min_record_lock_duration: must be within 1000ms..=30000ms"),
            ),
            (
                "session timeout above its maximum",
                "share_group_session_timeout = \"61s\"",
                refused(
                    "share_group_session_timeout: must be less than or equal to \
                     share_group_max_session_timeout",
                ),
            ),
            (
                "session bounds widened",
                "share_group_min_session_timeout = \"10s\"\n\
                 share_group_max_session_timeout = \"2min\"\n\
                 share_group_session_timeout = \"90s\"",
                with(|c| {
                    c.min_session_timeout = Duration::from_secs(10);
                    c.max_session_timeout = Duration::from_mins(2);
                    c.session_timeout = Duration::from_secs(90);
                }),
            ),
            (
                "heartbeat interval below its minimum",
                "share_group_heartbeat_interval = \"4s\"",
                refused(
                    "share_group_heartbeat_interval: must be greater than or equal to \
                     share_group_min_heartbeat_interval",
                ),
            ),
            (
                "heartbeat bounds inverted",
                "share_group_min_heartbeat_interval = \"20s\"",
                refused(
                    "share_group_max_heartbeat_interval: must be greater than or equal to \
                     share_group_min_heartbeat_interval",
                ),
            ),
            (
                "heartbeat interval not below the session timeout",
                "share_group_min_session_timeout = \"10s\"\n\
                 share_group_session_timeout = \"15s\"\n\
                 share_group_heartbeat_interval = \"15s\"",
                refused(
                    "share_group_heartbeat_interval: must be less than \
                     share_group_session_timeout",
                ),
            ),
            (
                "share group of 1001 members",
                "share_group_max_size = 1001",
                refused("share_group_max_size: must be within 1..=1000"),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (row, body, want) in rows {
            let applied = applied_runtime(body).map(|cfg| (*cfg.share_group).clone());
            actual.push((row, applied));
            expected.push((row, want));
        }
        assert!(actual == expected);
    }

    /// Kafka's `group.streams.min.session.timeout.ms` and its three siblings
    /// move the bounds, and the session timeout and the heartbeat interval
    /// must stay inside them, as `GroupCoordinatorConfig` requires.
    #[test]
    fn streams_group_bounds_move_and_hold_the_timings() {
        type Timings = (Duration, Duration, Duration, Duration, Duration, Duration);
        let secs = Duration::from_secs;
        let rows: [(&str, &str, Result<Timings, &str>); 4] = [
            (
                "the defaults",
                "",
                Ok((secs(45), secs(45), secs(60), secs(5), secs(5), secs(15))),
            ),
            (
                "session timeout lowered with its minimum",
                "streams_group_min_session_timeout = \"10s\"\n\
                 streams_group_session_timeout = \"10s\"",
                Ok((secs(10), secs(10), secs(60), secs(5), secs(5), secs(15))),
            ),
            (
                "heartbeat bounds widened",
                "streams_group_min_heartbeat_interval = \"1s\"\n\
                 streams_group_max_heartbeat_interval = \"30s\"\n\
                 streams_group_heartbeat_interval = \"20s\"",
                Ok((secs(45), secs(45), secs(60), secs(20), secs(1), secs(30))),
            ),
            (
                "session timeout lowered alone",
                "streams_group_session_timeout = \"10s\"",
                Err(
                    "invalid config: invalid runtime configuration: streams group session \
                     timeout is outside its bounds",
                ),
            ),
        ];
        for (row, body, want) in rows {
            let applied = applied_runtime(body).map(|cfg| {
                let streams = &cfg.streams_group;
                (
                    streams.session_timeout,
                    streams.min_session_timeout,
                    streams.max_session_timeout,
                    streams.heartbeat_interval,
                    streams.min_heartbeat_interval,
                    streams.max_heartbeat_interval,
                )
            });
            assert!(applied == want.map_err(str::to_owned), "{row}");
        }
    }

    /// Kafka trunk's `group.streams.rack.aware.assignment.tags`: the TOML
    /// entries stand for the comma-joined Kafka value, an empty entry fails
    /// `ValidList` and a repeated one fails `GroupCoordinatorConfig`, with
    /// the empty check first as in Kafka.
    #[test]
    fn streams_group_rack_aware_assignment_tags_follow_kafka() {
        const EMPTY: &str = "invalid config: streams_group_rack_aware_assignment_tags: \
                             Configuration 'group.streams.rack.aware.assignment.tags' values \
                             must not be empty.";
        const DUPLICATE: &str = "invalid config: streams_group_rack_aware_assignment_tags: \
                                 group.streams.rack.aware.assignment.tags must not contain \
                                 duplicate tag keys.";
        let rows: [(&str, Result<Vec<&str>, &str>); 8] = [
            ("[]", Ok(vec![])),
            ("[\"\"]", Ok(vec![])),
            ("[\"zone\"]", Ok(vec!["zone"])),
            ("[\" zone \", \"rack\"]", Ok(vec!["zone", "rack"])),
            ("[\"zone,rack\"]", Ok(vec!["zone", "rack"])),
            ("[\"zone\", \"\"]", Err(EMPTY)),
            ("[\"zone\", \"rack\", \"zone\"]", Err(DUPLICATE)),
            ("[\"zone\", \"zone\", \"\"]", Err(EMPTY)),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (value, want) in rows {
            let file: crate::file_config::FileConfig = toml::from_str(&format!(
                "[runtime]\nstreams_group_rack_aware_assignment_tags = {value}\n"
            ))
            .expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let applied = file
                .apply_to(&mut cfg)
                .map(|()| cfg.streams_group.rack_aware_assignment_tags.clone())
                .map_err(|error| error.to_string());
            actual.push((value, applied));
            expected.push((
                value,
                want.map(|tags| tags.into_iter().map(str::to_owned).collect())
                    .map_err(str::to_owned),
            ));
        }
        assert!(actual == expected);
    }
}
