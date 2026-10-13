//! The dedup index of one partition replica, as its writer keeps it.

use {
    super::{
        config::DedupConfig,
        identity::{RecordIdentity, indexes_batch, record_identity, visit_owned_batch},
        rebuild::rebuild_from_log,
        rewrite::drop_records,
    },
    crate::{metrics::DedupCounterDelta, partition::ProduceData},
    bytes::Bytes,
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::{DedupCounters, DedupRecord, DedupWindowFilter, Undo},
    krabka_log::{Log, Offset},
    krabka_protocol::records::{
        Attributes, RecordBatch, RecordBorrowed, TimestampType, validate_one_v2_batch,
    },
    std::sync::{Arc, Mutex, PoisonError},
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

/// The records of a batch that the index reads after its append.
#[derive(Debug)]
pub(crate) enum ObservedBatch {
    /// Nothing to read: a control or a transactional batch.
    Empty,
    /// One wire batch.
    Bytes(Bytes),
    /// A decoded batch.
    Owned(RecordBatch),
}

impl ObservedBatch {
    /// The records a leader appends for `data`.
    fn of_produce(data: &ProduceData) -> Self {
        match data {
            ProduceData::Verbatim(batch) if !batch.is_transactional => {
                Self::Bytes(batch.bytes.clone())
            }
            ProduceData::Owned(batch) if indexes_batch(batch.attributes) => {
                Self::Owned(batch.clone())
            }
            _ => Self::Empty,
        }
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
}

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
        }
    }

    fn policy(&self) -> RecordDecompressionPolicy { self.config.runtime.policy() }

    fn is_current(&self, log_end: Offset) -> bool { self.covered_through == Some(log_end) }

    fn stale(&mut self) {
        self.filter.clear();
        self.covered_through = None;
    }

    /// Add the identities of an appended batch.
    ///
    /// `append_time_ms` replaces each record's timestamp when the log stamps
    /// the batch with its append time as it appends it.
    fn observe(&mut self, batch: &ObservedBatch, append_time_ms: Option<i64>, now_ms: i64) {
        let policy = self.policy();
        let key_header = self.config.settings.key_header.clone();
        let key_header = key_header.as_deref();
        let filter = &mut self.filter;
        let populate = |identity: RecordIdentity| {
            if let RecordIdentity::Indexed(record) = identity
                && let Some(digest) = record.identity
            {
                filter.populate(digest, record.timestamp_ms, now_ms);
            }
        };
        match batch {
            ObservedBatch::Empty => {}
            ObservedBatch::Owned(batch) => {
                visit_owned_batch(batch, key_header, append_time_ms, populate);
            }
            ObservedBatch::Bytes(bytes) => {
                visit_wire_batch(bytes, key_header, append_time_ms, policy, populate);
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

/// Call `visit` with the identity of every record of one wire batch.
///
/// A batch that does not validate is skipped. The log accepted these bytes,
/// so this happens only to a batch the index cannot read, and an index that
/// lacks some identities is narrower, never wrong.
fn visit_wire_batch(
    bytes: &[u8],
    key_header: Option<&str>,
    append_time_ms: Option<i64>,
    policy: RecordDecompressionPolicy,
    mut visit: impl FnMut(RecordIdentity),
) {
    let Ok(validated) = validate_one_v2_batch(bytes) else {
        return;
    };
    let attributes = Attributes(validated.header.attributes.get());
    if !indexes_batch(attributes) {
        return;
    }
    let base_timestamp = validated.header.base_timestamp.get();
    let max_timestamp = validated.header.max_timestamp.get();
    let timestamp_of =
        |record: &RecordBorrowed<'_>| match (append_time_ms, attributes.timestamp_type()) {
            (Some(stamp), _) => stamp,
            (None, TimestampType::LogAppendTime) => max_timestamp,
            (None, TimestampType::CreateTime) => {
                base_timestamp.saturating_add(record.timestamp_delta)
            }
        };
    let _ = validated.validate_records_with(policy, |record| {
        visit(record_identity(
            record.key,
            record
                .headers
                .iter()
                .map(|header| (header.key, header.value)),
            key_header,
            timestamp_of(record),
        ));
    });
}

impl PartitionDedup {
    pub(crate) fn new(topic: Arc<str>, partition: i32) -> Self {
        Self {
            topic,
            partition,
            active: None,
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

    /// Adopt the config that a produce job carries when it differs from the
    /// current one.
    ///
    /// The produce handler reads the topic config from the newest metadata
    /// image it has, and the replica supervisor pushes the same config a
    /// little later. Adopting it here means that the first produce to a new
    /// topic is deduplicated even before that push arrives.
    pub(crate) fn adopt(&mut self, job: Option<&DedupJob>) {
        let Some(job) = job else { return };
        let same = self
            .active
            .as_ref()
            .is_some_and(|active| active.config.settings == job.config.settings);
        if !same {
            self.configure(Some(job.config.clone()));
        }
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
        let log_end = log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .log_end_offset();
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
                    batch: ObservedBatch::of_produce(&data),
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
        let classification = active.filter.classify(&records, now_ms);
        let classified = |data| ClassifiedAppend {
            data,
            pending: PendingAppend::Classified {
                undo: classification.undo.clone(),
                log_end,
            },
        };
        if classification.kept == records.len() {
            return classified(data);
        }
        match drop_records(&data, &classification.dropped, active.policy()) {
            Ok(Some(batch)) => classified(ProduceData::Owned(batch)),
            Ok(None) => classified(data),
            Err(error) => {
                // The produce handler decoded these bytes already, so this
                // does not occur. Append the batch whole rather than lose it.
                active.filter.revert(&classification.undo);
                warn!(
                    topic = &*self.topic,
                    partition = self.partition,
                    %error,
                    "dedup: could not rebuild a batch without its duplicates; appending it whole"
                );
                ClassifiedAppend {
                    pending: PendingAppend::Observed {
                        batch: ObservedBatch::of_produce(&data),
                        log_end,
                    },
                    data,
                }
            }
        }
    }

    /// Bring the index in line with the result of a leader's append.
    ///
    /// `log_end` is the log end offset after the append, and `append_time_ms`
    /// is the value [`Self::classify`] received.
    pub(crate) fn finish(
        &mut self,
        pending: PendingAppend,
        appended: bool,
        log_end: Offset,
        append_time_ms: Option<i64>,
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
                    active.covered_through = Some(log_end);
                }
            }
            PendingAppend::Observed {
                batch,
                log_end: before,
            } => {
                if appended && active.is_current(before) {
                    active.observe(&batch, append_time_ms, now_ms);
                    active.covered_through = Some(log_end);
                }
            }
        }
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

    /// Report the counters that changed since the last report, and the
    /// index sizes.
    pub(crate) fn report_metrics(&mut self) {
        let Some(active) = &mut self.active else {
            return;
        };
        let delta = active.take_delta();
        let size = active.filter.len();
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
        report(&self.topic, self.partition, &config, delta, 0);
    }
}

fn report(
    topic: &Arc<str>,
    partition: i32,
    config: &DedupConfig,
    delta: DedupCounterDelta,
    index_entries: usize,
) {
    let runtime = &config.runtime;
    let metrics = runtime.metrics();
    metrics.record_dedup_partition(topic, partition, delta, index_entries);
    metrics.set_dedup_broker_index(runtime.budget().entries(), runtime.budget().max_entries());
}
