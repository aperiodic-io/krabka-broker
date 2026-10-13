//! The dedup settings of a topic and the broker-wide resources every
//! partition index shares.

use {
    crate::{
        config::BrokerConfig, config_keys::DedupTopicConfig, error::BrokerError,
        metrics::BrokerMetrics,
    },
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::DedupBudget,
    std::sync::Arc,
};

/// What a topic's `dedup.window.ms` and `dedup.key.header` ask for.
///
/// A topic with dedup off has no `DedupSettings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupSettings {
    /// The window in milliseconds, at least 1.
    pub window_ms: i64,
    /// The header whose value is the record identity, or `None` for the
    /// record key.
    pub key_header: Option<Arc<str>>,
}

impl From<DedupTopicConfig> for DedupSettings {
    fn from(config: DedupTopicConfig) -> Self {
        Self {
            window_ms: config.window_ms,
            key_header: config.key_header.map(Arc::from),
        }
    }
}

/// The resources that every partition index on this broker shares.
///
/// The broker builds one at startup. Each [`DedupConfig`] holds it by `Arc`,
/// so a writer that receives a config also receives the budget its index
/// charges and the metrics it reports to.
#[derive(Debug)]
pub struct DedupRuntime {
    budget: Arc<DedupBudget>,
    max_entries_per_partition: usize,
    policy: RecordDecompressionPolicy,
    metrics: BrokerMetrics,
}

impl DedupRuntime {
    /// Build the runtime from the broker's settings.
    ///
    /// # Errors
    ///
    /// Returns the error of [`BrokerConfig::record_decompression_policy`] when
    /// the decompression limits are invalid.
    pub fn from_config(config: &BrokerConfig, metrics: BrokerMetrics) -> Result<Self, BrokerError> {
        Ok(Self::new(
            config.dedup_max_entries_per_partition,
            config.dedup_max_entries,
            config.record_decompression_policy()?,
            metrics,
        ))
    }

    /// A runtime with the default limits and a private metrics registry.
    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self::new(
            krabka_dedup::DedupWindowFilter::DEFAULT_MAX_ENTRIES,
            16_000_000,
            RecordDecompressionPolicy::default(),
            BrokerMetrics::new(),
        )
    }

    #[must_use]
    pub fn new(
        max_entries_per_partition: usize,
        max_entries: usize,
        policy: RecordDecompressionPolicy,
        metrics: BrokerMetrics,
    ) -> Self {
        metrics.set_dedup_broker_index(0, max_entries);
        Self {
            budget: Arc::new(DedupBudget::new(max_entries)),
            max_entries_per_partition,
            policy,
            metrics,
        }
    }

    /// The entry count that all indexes on this broker share.
    #[must_use]
    pub fn budget(&self) -> &Arc<DedupBudget> { &self.budget }

    #[must_use]
    pub fn max_entries_per_partition(&self) -> usize { self.max_entries_per_partition }

    /// The limits that a writer applies when it decompresses a batch to read
    /// its identities: the same limits as the produce path.
    #[must_use]
    pub fn policy(&self) -> RecordDecompressionPolicy { self.policy }

    #[must_use]
    pub fn metrics(&self) -> &BrokerMetrics { &self.metrics }
}

/// A topic's dedup settings, with the broker resources an index needs.
#[derive(Debug, Clone)]
pub struct DedupConfig {
    pub settings: DedupSettings,
    pub runtime: Arc<DedupRuntime>,
}

impl DedupConfig {
    /// The config of a topic, or `None` when the topic has dedup off.
    #[must_use]
    pub fn resolve(topic: Option<DedupTopicConfig>, runtime: &Arc<DedupRuntime>) -> Option<Self> {
        topic.map(|topic| Self {
            settings: topic.into(),
            runtime: Arc::clone(runtime),
        })
    }
}
