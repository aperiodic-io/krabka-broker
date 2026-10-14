//! The dedup settings of a topic and the broker-wide resources every
//! partition index shares.

use {
    super::partition::PartitionDedup,
    crate::{
        config::BrokerConfig, config_keys::DedupTopicConfig, error::BrokerError,
        metrics::BrokerMetrics, time_util::now_ms,
    },
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::DedupBudget,
    std::{
        fmt,
        sync::{Arc, Mutex, OnceLock, PoisonError, Weak},
        time::Duration,
    },
    tokio::{task::JoinHandle, time::MissedTickBehavior},
    tokio_util::sync::CancellationToken,
};

/// How often the ticker lets time pass for every partition index.
///
/// A busy partition expires entries as it inserts new ones; the ticker is for
/// the idle ones, whose entries would otherwise hold their share of the
/// broker-wide budget forever. Each tick visits a bounded number of entries,
/// so a second is often enough to drain an idle million-entry index within
/// minutes of its window passing, and rare enough to cost nothing.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

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
    /// Every partition index on this broker that has had dedup on, for the
    /// ticker. A writer that ends drops its index, and the next tick forgets
    /// the dead entry.
    indexes: Mutex<Vec<Weak<Mutex<PartitionDedup>>>>,
    /// Reads a topic's dedup config from the broker's newest metadata image.
    resolver: OnceLock<TopicResolver>,
}

/// Reads a topic's dedup config from the broker's newest metadata image.
pub(crate) struct TopicResolver(Box<ResolveTopic>);

/// The function a [`TopicResolver`] holds.
type ResolveTopic = dyn Fn(&str) -> Option<DedupTopicConfig> + Send + Sync;

impl fmt::Debug for TopicResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("TopicResolver") }
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
            indexes: Mutex::new(Vec::new()),
            resolver: OnceLock::new(),
        }
    }

    /// Install the reader of a topic's current dedup config. The broker calls
    /// this once at startup, with its metadata source.
    pub(crate) fn set_resolver(
        &self,
        resolve: impl Fn(&str) -> Option<DedupTopicConfig> + Send + Sync + 'static,
    ) {
        let _ = self.resolver.set(TopicResolver(Box::new(resolve)));
    }

    /// The dedup config `topic` has in the broker's newest metadata image.
    /// Without a resolver (a writer-level test), `otherwise`.
    pub(crate) fn newest_config(
        self: &Arc<Self>,
        topic: &str,
        otherwise: &DedupConfig,
    ) -> Option<DedupConfig> {
        match self.resolver.get() {
            Some(resolver) => DedupConfig::resolve((resolver.0)(topic), self),
            None => Some(otherwise.clone()),
        }
    }

    /// Hand a partition index to the ticker. A writer calls this once, the
    /// first time its index turns on.
    pub(crate) fn track(&self, index: &Arc<Mutex<PartitionDedup>>) {
        self.indexes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::downgrade(index));
    }

    /// One tick for every live index. An index its writer holds is skipped:
    /// a writer that is appending is not idle, and it expires entries as it
    /// inserts.
    pub(crate) fn tick_all(&self, now_ms: i64) {
        let indexes: Vec<_> = {
            let mut indexes = self.indexes.lock().unwrap_or_else(PoisonError::into_inner);
            indexes.retain(|index| index.strong_count() > 0);
            indexes.clone()
        };
        for index in indexes.iter().filter_map(Weak::upgrade) {
            if let Ok(mut index) = index.try_lock() {
                index.tick(now_ms);
            }
        }
    }

    /// Run [`Self::tick_all`] every [`TICK_INTERVAL`] until `shutdown`, on
    /// the blocking pool, so the ticks never hold up an async worker.
    pub(crate) fn spawn_ticker(self: &Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let runtime = Arc::clone(&runtime);
                        let _ = tokio::task::spawn_blocking(move || runtime.tick_all(now_ms())).await;
                    }
                }
            }
        })
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
