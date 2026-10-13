//! The dedup index of one partition replica, as its writer keeps it.

use {
    super::{
        config::{DedupConfig, DedupRuntime},
        identity::{BatchSource, identities_of, indexes_batch},
        rebuild::rebuild_from_log,
        rewrite::{Rewritten, drop_records},
    },
    crate::{metrics::DedupCounterDelta, partition::ProduceData, partition_writer::lock_log},
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::{Classification, DedupCounters, DedupRecord, DedupWindowFilter, Undo},
    krabka_log::{Log, Offset},
    std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
    tracing::warn,
};

#[cfg(test)]
mod tests;

/// The identities that the produce handler read from a client batch, and the
/// topic config it read them under.
#[derive(Debug, Clone)]
pub(crate) struct DedupJob {
    pub(crate) config: DedupConfig,
    /// One entry for each record of the batch, in record order.
    pub(crate) records: Vec<DedupRecord>,
}

/// How the index must follow one batch that the writer is about to append.
#[derive(Debug)]
pub(crate) enum PendingAppend {
    /// Dedup is off, or the index is stale and the append does not touch it.
    Ignored,
    /// The leader classified the batch, and the index already holds its
    /// admitted identities. A failed append gives them back.
    Classified { undo: Undo, log_end: Offset },
    /// The batch arrived without identities, for example a marker or a batch
    /// that the broker built itself. The index reads its records after the
    /// append succeeds.
    Observed {
        batch: ObservedBatch,
        log_end: Offset,
    },
}

/// The identities of a batch, read before the batch moved into the log, that
/// the index adds once the append succeeds.
///
/// It holds one digest and timestamp per record, not a copy of the batch: a
/// follower reads every batch it replicates through one of these, so a copy
/// would cost an allocation per record and per header on the replication
/// path.
#[derive(Debug, Default)]
pub(crate) struct ObservedBatch {
    records: Vec<DedupRecord>,
}

/// The records a leader appends for `data`, as an index reads them.
pub(crate) fn produce_source(data: &ProduceData) -> BatchSource<'_> {
    match data {
        ProduceData::Verbatim(batch) if !batch.is_transactional => BatchSource::Wire(&batch.bytes),
        ProduceData::Owned(batch) if indexes_batch(batch.attributes) => BatchSource::Owned(batch),
        _ => BatchSource::Empty,
    }
}

/// What [`PartitionDedup::classify`] hands back to the writer.
#[derive(Debug)]
pub(crate) struct ClassifiedAppend {
    /// The batch to append: the client's batch, or the batch without its
    /// duplicates.
    pub(crate) data: ProduceData,
    pub(crate) pending: PendingAppend,
}

/// The index of one partition replica and how much of the log it covers.
///
/// The partition writer owns it and changes it only while it changes the log,
/// so the index and the log move together. The index is current when it holds
/// every record of the last window of the log as it stands now:
/// `covered_through` then equals the log end offset. The writer brings a stale
/// index up to date from the log before the next append, and every append
/// that it then makes keeps it current.
#[derive(Debug)]
pub(crate) struct PartitionDedup {
    topic: Arc<str>,
    partition: i32,
    active: Option<ActiveIndex>,
    /// Whether the broker's idle ticker holds this index.
    tracked: bool,
}

#[derive(Debug)]
struct ActiveIndex {
    config: DedupConfig,
    filter: DedupWindowFilter,
    /// The log end offset the index covers, or `None` when it is stale.
    covered_through: Option<Offset>,
    reported: DedupCounters,
    rebuilds: u64,
    reported_rebuilds: u64,
    /// When this index last warned that it admits records unindexed.
    saturation_warned_at: Option<Instant>,
}

/// How often one partition may warn that its index admits records unindexed.
const SATURATION_WARNING_INTERVAL: Duration = Duration::from_secs(300);

impl ActiveIndex {
    fn new(config: DedupConfig) -> Self {
        let runtime = &config.runtime;
        let filter = DedupWindowFilter::new(
            config.settings.window_ms,
            runtime.max_entries_per_partition(),
            Some(Arc::clone(runtime.budget())),
        );
        Self {
            reported: filter.counters(),
            config,
            filter,
            covered_through: None,
            rebuilds: 0,
            reported_rebuilds: 0,
            saturation_warned_at: None,
        }
    }

    /// Warn, at most once per [`SATURATION_WARNING_INTERVAL`], that records
    /// were appended without being indexed. Their duplicates go undetected,
    /// and raising either limit needs a broker restart, so an operator has to
    /// hear about it rather than find it in a counter.
    fn warn_if_saturated(&mut self, delta: &DedupCounterDelta, topic: &str, partition: i32) {
        if delta.records_unindexed == 0
            || self
                .saturation_warned_at
                .is_some_and(|at| at.elapsed() < SATURATION_WARNING_INTERVAL)
        {
            return;
        }
        self.saturation_warned_at = Some(Instant::now());
        let budget = self.config.runtime.budget();
        warn!(
            topic,
            partition,
            index_entries = self.filter.len(),
            max_entries_per_partition = self.filter.max_entries(),
            broker_entries = budget.entries(),
            max_entries = budget.max_entries(),
            unindexed = delta.records_unindexed,
            "dedup: index is full, so new identities are appended without deduplication; \
             raise dedup_max_entries_per_partition or dedup_max_entries"
        );
    }

    fn policy(&self) -> RecordDecompressionPolicy { self.config.runtime.policy() }

    fn is_current(&self, log_end: Offset) -> bool { self.covered_through == Some(log_end) }

    fn stale(&mut self) {
        self.filter.clear();
        self.covered_through = None;
    }

    /// Read the identities of `source` under this index's rule.
    fn identities(&self, source: BatchSource<'_>) -> ObservedBatch {
        ObservedBatch {
            records: identities_of(
                source,
                self.config.settings.key_header.as_deref(),
                self.policy(),
            ),
        }
    }

    /// Add the identities of an appended batch.
    ///
    /// `stamp` replaces each record's timestamp when the log stamped the batch
    /// with its append time.
    fn observe(&mut self, batch: &ObservedBatch, stamp: Option<i64>, now_ms: i64) {
        for record in &batch.records {
            if let Some(digest) = record.identity {
                self.filter
                    .populate(digest, stamp.unwrap_or(record.timestamp_ms), now_ms);
            }
        }
    }

    /// The counters that changed since the last report.
    fn take_delta(&mut self) -> DedupCounterDelta {
        let now = self.filter.counters();
        let then = self.reported;
        let delta = DedupCounterDelta {
            records_dropped: now.dropped_records - then.dropped_records,
            records_unindexed: now.unindexed_records - then.unindexed_records,
            records_expired_on_arrival: now.expired_on_arrival_records
                - then.expired_on_arrival_records,
            records_skew_clamped: now.skew_clamped_records - then.skew_clamped_records,
            capacity_evicted_entries: now.capacity_evicted_entries - then.capacity_evicted_entries,
            batches_rebuilt: now.rebuilt_batches - then.rebuilt_batches,
            index_rebuilds: self.rebuilds - self.reported_rebuilds,
        };
        self.reported = now;
        self.reported_rebuilds = self.rebuilds;
        delta
    }
}

impl PartitionDedup {
    pub(crate) fn new(topic: Arc<str>, partition: i32) -> Self {
        Self {
            topic,
            partition,
            active: None,
            tracked: false,
        }
    }

    /// A new index for the writer of `topic`'s `partition`, shared with the
    /// blocking tasks that append for it.
    pub(crate) fn shared(topic: &str, partition: i32) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new(Arc::from(topic), partition)))
    }

    pub(crate) fn is_enabled(&self) -> bool { self.active.is_some() }

    /// Apply a topic's dedup config, or `None` to turn dedup off.
    ///
    /// The index is a function of the log and the config together. A config
    /// that changes the identity rule, or widens the window, makes the index
    /// stale, so the writer rebuilds it from the log before the next append.
    /// The rebuilt index then holds what the new rule finds in the new
    /// window, which a cleared index would not. A narrower window only
    /// shortens how long an entry can still match, which the index applies
    /// in place.
    pub(crate) fn configure(&mut self, config: Option<DedupConfig>) {
        let Some(config) = config else {
            self.disable();
            return;
        };
        let Some(active) = &mut self.active else {
            self.active = Some(ActiveIndex::new(config));
            self.report_metrics();
            return;
        };
        let old = &active.config.settings;
        let new = &config.settings;
        if old == new {
            return;
        }
        if old.key_header != new.key_header || new.window_ms > old.window_ms {
            active.stale();
        }
        active.filter.set_window(new.window_ms);
        active.config = config;
        self.report_metrics();
    }

    /// Apply the config the replica supervisor sent.
    ///
    /// Returns the broker runtime the first time the index is on after a
    /// push, so the writer can hand the index to the idle ticker; see
    /// [`DedupRuntime::track`].
    pub(crate) fn apply_pushed(
        &mut self,
        config: Option<DedupConfig>,
    ) -> Option<Arc<DedupRuntime>> {
        self.configure(config);
        let active = self.active.as_ref().filter(|_| !self.tracked)?;
        self.tracked = true;
        Some(Arc::clone(&active.config.runtime))
    }

    /// Settle a disagreement between a produce job's config and the index's.
    ///
    /// The produce handler reads the topic config from the newest metadata
    /// image it has, and the replica supervisor pushes it to this partition a
    /// little later. So a job can carry a newer config than the index has:
    /// right after a topic is created or an operator changed it. A job can
    /// also carry an older one: it was prepared from an image read before the
    /// change. The two look the same from here, so neither copy is trusted:
    /// the index takes the config the broker's newest image holds now, which
    /// is never older than the job's. A job prepared before dedup was turned
    /// off therefore cannot turn it back on, and a job prepared after a change
    /// does not wait for the supervisor.
    ///
    /// Without a resolver (a writer-level test), the job's config is taken.
    pub(crate) fn adopt(&mut self, job: Option<&DedupJob>) {
        let Some(job) = job else { return };
        let same = self
            .active
            .as_ref()
            .is_some_and(|active| active.config.settings == job.config.settings);
        if same {
            return;
        }
        let current = job.config.runtime.newest_config(&self.topic, &job.config);
        self.configure(current);
    }

    /// Mark the index stale after the log lost records at its end.
    pub(crate) fn invalidate(&mut self) {
        if let Some(active) = &mut self.active {
            active.stale();
        }
    }

    /// Rebuild a stale index from the last window of `log`.
    ///
    /// The writer calls this before an append and without the log lock: the
    /// rebuild takes the lock for each read.
    pub(crate) fn make_current(&mut self, log: &Mutex<Log>, now_ms: i64) {
        let Some(active) = &mut self.active else {
            return;
        };
        let log_end = lock_log(log).log_end_offset();
        if active.is_current(log_end) {
            return;
        }
        active.filter.clear();
        let key_header = active.config.settings.key_header.clone();
        let outcome = rebuild_from_log(&mut active.filter, key_header.as_deref(), log, now_ms);
        if let Some(error) = &outcome.error {
            warn!(
                topic = &*self.topic,
                partition = self.partition,
                %error,
                "dedup: index rebuild stopped at a log read error; the index holds the \
                 identities read before it"
            );
        }
        active.covered_through = Some(outcome.covered_through);
        active.rebuilds += 1;
    }

    /// Decide which records of a leader's produce batch to append.
    ///
    /// The index classifies the batch only when it is current at `log_end`
    /// and the job carries one identity for each record under the index's own
    /// rule. Otherwise the batch appends unchanged, and the index reads it
    /// afterwards if it is current.
    ///
    /// `append_time_ms` is the broker clock on a `LogAppendTime` topic, which
    /// replaces every record's timestamp.
    pub(crate) fn classify(
        &mut self,
        data: ProduceData,
        job: Option<DedupJob>,
        log_end: Offset,
        append_time_ms: Option<i64>,
        now_ms: i64,
    ) -> ClassifiedAppend {
        let Some(active) = self
            .active
            .as_mut()
            .filter(|active| active.is_current(log_end))
        else {
            return ClassifiedAppend {
                data,
                pending: PendingAppend::Ignored,
            };
        };
        let records = job
            .filter(|job| job.config.settings == active.config.settings)
            .map(|job| job.records)
            .filter(|records| {
                usize::try_from(data.record_count()).is_ok_and(|count| count == records.len())
            });
        let Some(mut records) = records else {
            return ClassifiedAppend {
                pending: PendingAppend::Observed {
                    batch: active.identities(produce_source(&data)),
                    log_end,
                },
                data,
            };
        };
        if let Some(stamp) = append_time_ms {
            for record in &mut records {
                record.timestamp_ms = stamp;
            }
        }
        let Classification {
            dropped,
            kept,
            undo,
        } = active.filter.classify(&records, now_ms);
        let data = if kept == records.len() {
            data
        } else {
            match drop_records(data, &dropped, active.policy()) {
                Rewritten::Batch(data) => data,
                Rewritten::Undecodable(data, error) => {
                    // The produce handler decoded these bytes already, so this
                    // does not occur. Append the batch whole rather than lose
                    // it.
                    active.filter.revert(&undo);
                    warn!(
                        topic = &*self.topic,
                        partition = self.partition,
                        %error,
                        "dedup: could not rebuild a batch without its duplicates; appending it \
                         whole"
                    );
                    return ClassifiedAppend {
                        pending: PendingAppend::Observed {
                            batch: active.identities(produce_source(&data)),
                            log_end,
                        },
                        data,
                    };
                }
            }
        };
        ClassifiedAppend {
            data,
            pending: PendingAppend::Classified { undo, log_end },
        }
    }

    /// Bring the index in line with the result of a leader's append.
    ///
    /// `log_end` is the log end offset after the append. `stamp` is the
    /// append time the log wrote into the batch on a `LogAppendTime` topic.
    /// [`Self::classify`] had to decide before the append, with its own
    /// reading of the clock; the index moves the identities it admitted to
    /// the log's stamp, which is what a follower and a rebuild read.
    pub(crate) fn finish(
        &mut self,
        pending: PendingAppend,
        appended: bool,
        log_end: Offset,
        stamp: Option<i64>,
        now_ms: i64,
    ) {
        let Some(active) = &mut self.active else {
            return;
        };
        match pending {
            PendingAppend::Ignored => {
                if appended {
                    active.covered_through = None;
                }
            }
            PendingAppend::Classified {
                undo,
                log_end: before,
            } => {
                if !appended {
                    active.filter.revert(&undo);
                } else if active.is_current(before) {
                    if let Some(stamp) = stamp {
                        for entry in undo.entries() {
                            active.filter.populate(entry.identity, stamp, now_ms);
                        }
                    }
                    active.covered_through = Some(log_end);
                }
            }
            PendingAppend::Observed {
                batch,
                log_end: before,
            } => {
                if appended && active.is_current(before) {
                    active.observe(&batch, stamp, now_ms);
                    active.covered_through = Some(log_end);
                }
            }
        }
    }

    /// Prepare to follow a batch that a follower is about to append from its
    /// leader: bring a stale index up to date, then read the batch's
    /// identities while the batch is still at hand. `None` when the
    /// partition does not deduplicate.
    pub(crate) fn identities_for_follow(
        &mut self,
        log: &Mutex<Log>,
        source: BatchSource<'_>,
        now_ms: i64,
    ) -> Option<ObservedBatch> {
        self.active.as_ref()?;
        self.make_current(log, now_ms);
        self.active.as_ref().map(|active| active.identities(source))
    }

    /// Follow a batch that a follower appended from its leader.
    ///
    /// `before` and `after` are the log end offsets around the append. The
    /// follower reads the batch with the rules the leader classified it with,
    /// so a promoted follower holds the index that the leader held.
    pub(crate) fn follow(
        &mut self,
        batch: &ObservedBatch,
        before: Offset,
        after: Offset,
        now_ms: i64,
    ) {
        let Some(active) = &mut self.active else {
            return;
        };
        if active.is_current(before) {
            active.observe(batch, None, now_ms);
            active.covered_through = Some(after);
        } else {
            active.covered_through = None;
        }
    }

    /// Let time pass for an index that may be idle: expire what the broker
    /// clock has moved past, a bounded number of entries per call, so an
    /// idle partition gives its entries back to the broker-wide budget. The
    /// broker's dedup ticker calls this about once a second.
    pub(crate) fn tick(&mut self, now_ms: i64) {
        let Some(active) = &mut self.active else {
            return;
        };
        active.filter.tick(now_ms);
        self.report_metrics();
    }

    /// Report the counters that changed since the last report, and the
    /// index sizes.
    pub(crate) fn report_metrics(&mut self) {
        let Some(active) = &mut self.active else {
            return;
        };
        let delta = active.take_delta();
        active.warn_if_saturated(&delta, &self.topic, self.partition);
        let size = IndexSize {
            entries: active.filter.len(),
            heap_bytes: active.filter.heap_bytes(),
        };
        report(&self.topic, self.partition, &active.config, delta, size);
    }

    /// Turn dedup off: report what is left to report, then free the index and
    /// its share of the broker-wide budget.
    fn disable(&mut self) {
        let Some(mut active) = self.active.take() else {
            return;
        };
        let delta = active.take_delta();
        let config = active.config.clone();
        drop(active);
        report(
            &self.topic,
            self.partition,
            &config,
            delta,
            IndexSize::default(),
        );
    }
}

/// The size of one partition's index, as its gauges report it.
#[derive(Debug, Default, Clone, Copy)]
struct IndexSize {
    entries: usize,
    heap_bytes: usize,
}

impl Drop for PartitionDedup {
    /// A partition writer ends when its topic is deleted or the replica moves
    /// away. Dropping the index gives its entries back to the broker-wide
    /// budget; publish the budget's new size so the broker gauge does not wait
    /// for another partition's append. The partition's own series are left
    /// alone: the path that removed the partition evicts them, and setting
    /// one here would bring it back.
    fn drop(&mut self) {
        let Some(active) = self.active.take() else {
            return;
        };
        let runtime = Arc::clone(&active.config.runtime);
        drop(active);
        let budget = runtime.budget();
        runtime
            .metrics()
            .set_dedup_broker_index(budget.entries(), budget.max_entries());
    }
}

fn report(
    topic: &Arc<str>,
    partition: i32,
    config: &DedupConfig,
    delta: DedupCounterDelta,
    size: IndexSize,
) {
    let runtime = &config.runtime;
    let metrics = runtime.metrics();
    metrics.record_dedup_partition(topic, partition, delta, size.entries);
    metrics.set_dedup_index_heap_bytes(topic, partition, size.heap_bytes);
    metrics.set_dedup_broker_index(runtime.budget().entries(), runtime.budget().max_entries());
}
