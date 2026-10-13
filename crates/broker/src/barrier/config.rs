//! Tunables of the barrier coordinator.
//!
//! The broker configuration owns these values. The coordinator takes them as
//! one struct, so a test can build a coordinator without a `BrokerConfig`.

use krabka_units::{ByteSize, Time, mebibytes, millis, minutes, secs};

/// How the coordinator sizes its internal topic, its retries, and its
/// scheduler.
///
/// The struct is [`PartialEq`] but not [`Eq`], because [`Time`] and
/// [`ByteSize`] are backed by a float.
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub(crate) struct BarrierConfig {
    /// Partition count of `__barrier_state`. It fixes the group-to-partition
    /// map, so a change moves every group.
    #[default(50)]
    pub(crate) state_topic_num_partitions: i32,
    /// How much of a state partition the coordinator reads per call during
    /// recovery.
    #[default(mebibytes(1))]
    pub(crate) recovery_read_max: ByteSize,
    /// How long one injection retries the partitions that carry no marker
    /// yet. The coordinator publishes a partial cut when this time runs out.
    #[default(minutes(1))]
    pub(crate) injection_timeout: Time,
    /// The first wait between two fan-out attempts.
    #[default(millis(100))]
    pub(crate) retry_backoff: Time,
    /// The largest wait between two fan-out attempts.
    #[default(secs(5))]
    pub(crate) retry_backoff_max: Time,
    /// How often the scheduler looks for a group that is due.
    #[default(secs(1))]
    pub(crate) scheduler_tick: Time,
    /// How many cuts a group keeps when the caller names no value.
    #[default(32)]
    pub(crate) default_retained_cuts: i32,
    /// Maximum number of cuts a barrier group may retain.
    #[default(100)]
    pub(crate) max_retained_cuts: i32,
    /// Maximum number of barrier groups the coordinator accepts.
    #[default(100)]
    pub(crate) max_groups: usize,
    /// Maximum number of topics in one barrier group.
    #[default(100)]
    pub(crate) max_topics_per_group: usize,
    /// Shortest periodic injection interval a barrier group may ask for.
    #[default(secs(1))]
    pub(crate) min_injection_interval: Time,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_units::convert::TimeExt as _};

    #[test]
    fn the_defaults_are_the_documented_values() {
        let expected = BarrierConfig {
            state_topic_num_partitions: 50,
            recovery_read_max: mebibytes(1),
            injection_timeout: minutes(1),
            retry_backoff: millis(100),
            retry_backoff_max: secs(5),
            scheduler_tick: secs(1),
            default_retained_cuts: 32,
            max_retained_cuts: 100,
            max_groups: 100,
            max_topics_per_group: 100,
            min_injection_interval: secs(1),
        };
        assert!(BarrierConfig::default() == expected);
    }

    #[test]
    fn the_first_backoff_is_below_the_largest_one() {
        let config = BarrierConfig::default();
        assert!(config.retry_backoff.millis_i64() < config.retry_backoff_max.millis_i64());
        assert!(config.retry_backoff_max.millis_i64() < config.injection_timeout.millis_i64());
    }
}
