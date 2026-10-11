//! Flush policy for the diskless WAL flusher: the tick cadence, the per-object
//! size budget, the trim safety lag, and the index-projection stall timeout,
//! in one `FlushConfig` that the broker derives from its own `BrokerConfig`.

use std::time::Duration;

use krabka_units::{ByteSize, convert::TimeExt as _};

use crate::config::{
    BrokerConfig, DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL, DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
    DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT, DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
};

#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub(crate) struct FlushConfig {
    #[default(DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL.to_std())]
    pub(crate) interval: Duration,
    #[default(DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE)]
    pub(crate) max_size: ByteSize,
    #[default(Some(DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG))]
    pub(crate) trim_safety_lag: Option<i64>,
    /// How long the flusher tolerates the index projection standing still:
    /// both waiting for its own published record to come back, and waiting
    /// for the startup replay to advance. It bounds a lack of *progress*, not
    /// total elapsed time, so a large index-topic backlog does not trip it.
    #[default(DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT.to_std())]
    pub(crate) index_projection_timeout: Duration,
}

impl FlushConfig {
    pub(crate) fn from_broker(config: &BrokerConfig) -> Self {
        Self {
            interval: config.diskless_wal_flush_interval.to_std(),
            max_size: config.diskless_wal_flush_max_size,
            trim_safety_lag: Some(config.diskless_wal_trim_safety_lag),
            index_projection_timeout: config.diskless_wal_index_projection_timeout.to_std(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::config::BrokerConfig;

    #[tokio::test]
    async fn default_config_enables_safe_trim_lag() {
        let config = FlushConfig::default();
        assert!(config.trim_safety_lag == Some(DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG));
        assert!(
            config.index_projection_timeout
                == DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT.to_std()
        );
    }

    #[test]
    fn broker_config_controls_every_flusher_policy() {
        let broker = BrokerConfig {
            diskless_wal_flush_interval: krabka_units::millis(125),
            diskless_wal_flush_max_size: krabka_units::mebibytes(4),
            diskless_wal_trim_safety_lag: 0,
            diskless_wal_index_projection_timeout: krabka_units::secs(3),
            ..BrokerConfig::default()
        };

        let config = FlushConfig::from_broker(&broker);

        assert!(config.interval == Duration::from_millis(125));
        assert!(config.max_size == krabka_units::mebibytes(4));
        assert!(config.trim_safety_lag == Some(0));
        assert!(config.index_projection_timeout == Duration::from_secs(3));
    }
}
