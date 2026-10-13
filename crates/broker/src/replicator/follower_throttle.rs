//! KIP-73 follower-side fetch throttling.
//!
//! The module decides whether a fetch round may run at all, and with how large
//! a `partition_max_bytes` budget, by reading the topic's
//! `follower.replication.throttled.replicas` list and drawing from the
//! broker-wide follower-in token bucket.
//!
//! A follower that has caught up is never held back: Kafka's
//! `RemoteLeaderEndPoint.shouldFollowerThrottle` throttles a replica only while
//! it is not in sync, and a replica is in sync when its lag is zero
//! (`PartitionFetchState.isReplicaInSync`), to avoid ISR thrashing. What such a
//! follower fetches still counts against the quota
//! (`ReplicaFetcherThread.processPartitionData` records every throttled
//! partition), so [`record_replicated`] charges it as it arrives.

use {
    super::Config,
    crate::throttle::{REPLICATION_QUOTA_WINDOW, TopicThrottle},
    krabka_units::{
        ByteRate, ByteSize,
        convert::{ByteRateExt, ByteSizeExt},
    },
    std::sync::atomic::{
        AtomicI64, AtomicU64,
        Ordering::{Acquire, Relaxed, Release},
    },
};

/// How far a followed partition trails the leader's high watermark, as the
/// `lag` of Kafka's `PartitionFetchState`.
///
/// The lag is unknown until a response has been applied, and an unknown lag is
/// not in sync. An empty response leaves a known lag as it was, because Kafka
/// recomputes it only when the response carried bytes.
#[derive(Debug, krabka_macros::FieldDefaults)]
pub(crate) struct ReplicaLag {
    /// The lag in offsets, never below zero, or [`Self::UNKNOWN`].
    #[default(AtomicI64::new(Self::UNKNOWN))]
    lag: AtomicI64,
    /// The bytes appended since the lag was last computed.
    #[default(AtomicU64::new(0))]
    appended: AtomicU64,
}

impl ReplicaLag {
    const UNKNOWN: i64 = -1;

    /// Whether the partition is in sync: its lag is known and is zero.
    fn in_sync(&self) -> bool { self.lag.load(Acquire) == 0 }

    fn note_appended(&self, bytes: u64) { self.appended.fetch_add(bytes, Relaxed); }

    /// Recomputes the lag from the leader's high watermark and this replica's
    /// log end, once a response row has been applied: `max(0, hw - leo)`. Like
    /// Kafka it does so only when the row appended bytes or the lag was
    /// unknown.
    pub(super) fn update(&self, leader_high_watermark: i64, log_end_offset: i64) {
        let appended = self.appended.swap(0, Relaxed);
        if appended > 0 || self.lag.load(Acquire) == Self::UNKNOWN {
            self.lag.store(
                leader_high_watermark.saturating_sub(log_end_offset).max(0),
                Release,
            );
        }
    }
}

/// Whether the `follower.replication.throttled.replicas` list of the topic
/// throttles this partition on this broker.
fn follower_partition_throttled(cfg: &Config) -> bool {
    let image = cfg.controller.current_image();
    TopicThrottle::for_topic(&image, &cfg.topic, cfg.node_id)
        .follower
        .contains(cfg.partition.get())
}

/// Accounts `bytes` of one appended batch: the replication-in metric, the lag
/// bookkeeping, and, for a throttled partition that is in sync, the follower
/// quota.
///
/// A partition that is not in sync drew its budget when the round was planned
/// ([`follower_partition_fetch_cap`]), so it is not charged twice.
pub(super) fn record_replicated(cfg: &Config, bytes: u64) {
    cfg.metrics
        .record_replication_in(&cfg.topic, cfg.partition.get(), bytes);
    cfg.lag.note_appended(bytes);
    if cfg.lag.in_sync()
        && cfg.throttle_state.follower_in.byte_rate() != <ByteRate as ByteRateExt>::ZERO
        && follower_partition_throttled(cfg)
    {
        cfg.throttle_state
            .follower_in
            .record_bounded(bytes, REPLICATION_QUOTA_WINDOW);
        cfg.metrics.record_replication_throttled_in(bytes);
    }
}

/// Whether this round may fetch, and with how large a per-partition budget.
///
/// This enum is not `Eq`. The budget is a [`ByteSize`], and its `f64` storage
/// is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum FetchThrottleDecision {
    Fetch(ByteSize),
    Sleep,
}

pub(super) fn follower_partition_fetch_cap(cfg: &Config) -> FetchThrottleDecision {
    // Kafka throttles a follower only while it is not in sync, so the cheap
    // checks come before the image lookup.
    if cfg.throttle_state.follower_in.byte_rate() == <ByteRate as ByteRateExt>::ZERO
        || cfg.lag.in_sync()
        || !follower_partition_throttled(cfg)
    {
        return FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max);
    }

    // The bucket seam counts raw bytes, so the budget crosses into `u64` here
    // and back on the granted amount. `try_consume` never grants more than it
    // was asked for, so the result is bounded by the configured maximum.
    let granted = cfg
        .throttle_state
        .follower_in
        .try_consume(cfg.connection.replication.fetch_max.bytes_u64());
    // KIP-73: the measured follower-side throttled-replication rate, Kafka's
    // `kafka.server:type=FollowerReplication,name=byte-rate`.
    cfg.metrics.record_replication_throttled_in(granted);
    if granted == 0 {
        // Nothing was left to grant, so this partition sits the round out
        // entirely rather than fetching a capped amount. That refusal is what
        // this counter records; there is no delay to observe.
        cfg.metrics.record_replication_throttle_sleep();
        FetchThrottleDecision::Sleep
    } else {
        FetchThrottleDecision::Fetch(ByteSize::from_bytes(granted))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            replicator::test_support::{
                ConfigWithDir, LEADER_ID, NODE_ID, image_with_follower_throttle, image_with_leader,
                test_config,
            },
            throttle::MICROS_PER_TOKEN,
        },
        assert2::{assert, check},
        krabka_units::{bytes, bytes_per_sec},
    };

    #[test]
    fn follower_partition_fetch_cap_ignores_unthrottled_partitions() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1234), bytes(0));

        assert!(
            follower_partition_fetch_cap(&cfg)
                == FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_ignores_zero_rate_throttle() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));

        assert!(
            follower_partition_fetch_cap(&cfg)
                == FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_sleeps_when_throttled_bucket_is_empty() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Sleep);
    }

    #[test]
    fn follower_partition_fetch_cap_uses_granted_bucket_size() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1234), bytes(1234));

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(bytes(1234)));
    }

    /// The follower list names the destination replica, which is the broker
    /// that reads it, so a list that names another broker throttles nothing
    /// here, and one that names this broker throttles the partition (#1210).
    #[test]
    fn follower_partition_fetch_cap_reads_only_the_entries_naming_this_broker() {
        // (list, whether the partition is throttled on the follower, broker 2)
        for (list, throttled) in [
            (format!("0:{}", NODE_ID.0), true),
            (format!("0:{}", LEADER_ID.0), false),
            (format!("1:{}", NODE_ID.0), false),
        ] {
            let (cfg, _log_dir) = test_config(image_with_follower_throttle(&list));
            cfg.throttle_state
                .follower_in
                .set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));

            let want = if throttled {
                FetchThrottleDecision::Sleep
            } else {
                FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max)
            };
            check!(follower_partition_fetch_cap(&cfg) == want, "{list}");
        }
    }

    /// A follower that has caught up to the leader's high watermark is never
    /// throttled, as Kafka's `shouldFollowerThrottle` requires the replica not
    /// to be in sync (#1211). A lag that is unknown, or above zero, is not in
    /// sync, and an empty response leaves a known lag as it was.
    #[test]
    fn a_caught_up_follower_is_never_throttled() {
        // (label, (leader hw, log end, bytes appended) of each response, whether the
        // next round is throttled)
        let cases = [
            ("no response yet: the lag is unknown", vec![], true),
            (
                "a response that leaves it behind the high watermark",
                vec![(10, 4, 100)],
                true,
            ),
            (
                "a response that reaches the high watermark",
                vec![(10, 10, 100)],
                false,
            ),
            (
                "a log end past the high watermark is no lag either",
                vec![(10, 12, 100)],
                false,
            ),
            (
                "an empty response keeps the lag it had: caught up",
                vec![(10, 10, 100), (20, 10, 0)],
                false,
            ),
            (
                "an empty response keeps the lag it had: behind",
                vec![(10, 4, 100), (10, 10, 0)],
                true,
            ),
            (
                "a response with bytes recomputes it: behind again",
                vec![(10, 10, 100), (30, 20, 50)],
                true,
            ),
        ];
        for (label, responses, throttled) in cases {
            let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
            cfg.throttle_state
                .follower_in
                .set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));
            for (leader_hw, log_end, appended) in responses {
                cfg.lag.note_appended(appended);
                cfg.lag.update(leader_hw, log_end);
            }

            let want = if throttled {
                FetchThrottleDecision::Sleep
            } else {
                FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max)
            };
            check!(follower_partition_fetch_cap(&cfg) == want, "{label}");
        }
    }

    fn throttled_follower_at(leader_hw: i64, follower_offset: i64) -> ConfigWithDir {
        let (cfg, log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1000), bytes(1000));
        cfg.lag.note_appended(1);
        cfg.lag.update(leader_hw, follower_offset);
        ConfigWithDir { cfg, dir: log_dir }
    }

    /// What a caught-up follower fetches still counts against the follower
    /// quota, so that total replication stays within it (#1211): the bytes are
    /// recorded as they arrive, and the debt leaves the follower that is not
    /// in sync with nothing to draw.
    #[test]
    fn a_caught_up_follower_still_charges_the_quota() {
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 10);

        // Caught up: the round is not throttled, and the batch that arrives
        // is recorded in full, past the 1000 bytes the bucket holds.
        let fetching = follower_partition_fetch_cap(&cfg);
        record_replicated(&cfg, 4_000);
        // Behind again: the bucket is in debt, so there is no budget.
        cfg.lag.note_appended(1);
        cfg.lag.update(20, 10);
        let behind = follower_partition_fetch_cap(&cfg);

        check!(
            (fetching, behind)
                == (
                    FetchThrottleDecision::Fetch(cfg.connection.replication.fetch_max),
                    FetchThrottleDecision::Sleep
                )
        );
    }

    /// The debt a caught-up follower's bytes leave is kept for one
    /// replication quota window at most, as Kafka's quota forgets a sample
    /// that leaves its window. A burst of `100_000` bytes on a `1_000` B/s quota
    /// is 99 s of debt unbounded, and a lagging partition of the same broker
    /// would sit out all of it; bounded, it sits out 11 s at most.
    #[test]
    fn a_caught_up_follower_keeps_one_window_of_debt_at_most() {
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 10);

        record_replicated(&cfg, 100_000);

        // The refill between the charge and the read repays a few micro-tokens.
        let debt_tokens = cfg.throttle_state.follower_in.record(0) / MICROS_PER_TOKEN;
        check!((10_900..=11_000).contains(&debt_tokens), "{debt_tokens}");
    }

    /// A partition that is not in sync drew its budget when the round was
    /// planned, so its bytes are not charged a second time as they arrive.
    #[test]
    fn a_follower_that_is_behind_is_not_charged_twice() {
        let ConfigWithDir { cfg, dir: _log_dir } = throttled_follower_at(10, 4);

        record_replicated(&cfg, 4_000);

        check!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(bytes(1000)));
    }
}
