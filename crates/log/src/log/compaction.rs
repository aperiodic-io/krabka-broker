//! One compaction pass over the sealed segment list, and the broker-side
//! inputs that pass depends on.
//!
//! The pass rewrites the consumed sealed segments into one or more new ones,
//! grouped so no output segment exceeds `segment.bytes` (Kafka's
//! `Cleaner.groupSegmentsBySize`), and never touches the active segment, so
//! the log-end offset a producer sees does not move.

use {
    super::Log,
    crate::{
        compact::{
            CleanedTransactionMetadata, CleaningRound, RewriteOutput, RewriteRetention,
            atomic_swap, atomic_swap_retiring, build_offset_map, rewrite_segments,
        },
        error::LogError,
        retention,
        segment::Segment,
        txn_index::TxnIndex,
    },
    krabka_ids::Offset,
    krabka_units::prelude::{
        ByteSize, ByteSizeExt as _, Ratio, RatioExt as _, Time, TimeExt as _, fraction,
    },
    tracing::instrument,
};

/// Inputs to one [`Log::compact`] pass that depend on broker-side state.
///
/// These inputs are the wall clock that computes the KIP-534 delete horizons,
/// and the last stable offset that bounds what a pass may rewrite. The pass
/// reads the active producers from the producer state of the log itself, as
/// Kafka's `Cleaner.cleanSegments` does.
#[derive(Debug, Clone)]
pub struct CompactionContext {
    /// Wall clock for this pass. It drives delete-horizon stamps and expiry.
    pub now: std::time::SystemTime,
    /// The replica's last stable offset: the first offset that still belongs
    /// to an open transaction, or that a complete transaction's marker has
    /// not yet reached the high watermark for.
    ///
    /// A pass consumes only sealed segments that end at or below it, which is
    /// the bound Kafka's `LogCleanerManager.cleanableOffsets` takes: the
    /// minimum of `log.lastStableOffset()`, the active segment's base offset
    /// and the segment the minimum compaction lag allows. The high watermark
    /// alone is not enough, because the records between the LSO and the high
    /// watermark belong to transactions with no marker yet; rewriting them
    /// early would leave the output segment's aborted-transaction index
    /// unable to learn about a marker that arrives later, and a
    /// `read_committed` consumer could then see a record from a transaction
    /// that later aborts.
    pub last_stable_offset: Offset,
}

/// What a partition looks like to the broker's cleaner before it decides
/// whether a compaction pass is worth running.
///
/// Kafka's `LogCleanerManager.cleanableOffsets` splits a log into three parts
/// and this mirrors it: the clean prefix below the first dirty offset, the
/// cleanable range a pass would rewrite, and an uncleanable tail that is
/// neither. The ratio Kafka tests is the cleanable range over the clean
/// prefix plus that range, so the uncleanable tail is left out of both halves.
///
/// krabka keeps no checkpoint file, because [`Log::compact`] rewrites the
/// sealed segments it is given into one or more size-bounded outputs: the
/// leading sealed segments are therefore the previous pass's output and count
/// as clean, and every sealed segment after them arrived since. A log that
/// has never been compacted reports no clean prefix at all, which is the
/// conservative direction: the ratio it reports is never larger than the true
/// one.
///
/// The uncleanable tail is Kafka's, too. The active segment is always in it,
/// and so is every sealed segment from the first one whose largest timestamp
/// is younger than `min.compaction.lag.ms` onwards. That is what the min lag
/// means in Kafka: it withholds the young tail from the pass, it does not
/// postpone the pass. A topic taking writes more often than its min lag still
/// has its older segments cleaned on schedule.
///
/// The high watermark bounds the tail as well, which is the third term of
/// Kafka's `LogCleanerManager.cleanableOffsets` minimum beside the active
/// segment's base offset and the min lag. A record above the watermark is not
/// committed yet: a leader election can still take it away, and the
/// truncation that removes it would leave this replica missing the older
/// record that compaction dropped in its favour. Bounding the pass at the
/// watermark is what makes compaction a safe local operation on every
/// replica, leader or follower.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CompactionCandidacy {
    /// Bytes the previous compaction pass already deduplicated.
    clean_bytes: ByteSize,
    /// Bytes in the cleanable range, which one pass would deduplicate.
    cleanable_bytes: ByteSize,
    /// Timestamp of the oldest record in the dirty region, which is what
    /// `max.compaction.lag.ms` bounds. `None` when nothing is dirty.
    ///
    /// Kafka reads this over the whole dirty region rather than over the
    /// cleanable range, so a segment held back by the min lag does not hide
    /// an older one from the max lag.
    oldest_dirty_timestamp_ms: Option<i64>,
}

impl CompactionCandidacy {
    /// Kafka's `LogToClean.cleanableRatio`: cleanable bytes over the clean
    /// prefix plus them. Zero for an empty log, which no cleaner should spend
    /// a pass on.
    fn cleanable_ratio(self) -> Ratio {
        let total = self.clean_bytes + self.cleanable_bytes;
        if total.bytes_f64() <= 0.0 {
            return Ratio::ZERO;
        }
        fraction(self.cleanable_bytes.bytes_f64() / total.bytes_f64())
    }
}

/// The three `LogConfig` values the decision reads, so it is a pure function
/// of the log's shape and the topic's configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CompactionSchedule {
    min_lag: Time,
    max_lag: Option<Time>,
    min_ratio: Ratio,
}

/// How many of the dirty sealed segments a pass may clean, given each one's
/// largest timestamp in offset order.
///
/// This is Kafka's `findFirstUncleanableSegment`: with a positive
/// `min.compaction.lag.ms`, the first segment whose largest timestamp is
/// within the lag ends the cleanable range, and every segment from there on is
/// uncleanable. A zero lag withholds nothing. A segment carrying no batch
/// reports [`i64::MIN`], which no lag can hold back.
fn cleanable_prefix(largest_timestamps_ms: &[i64], now_ms: i64, min_lag_ms: i64) -> usize {
    if min_lag_ms <= 0 {
        return largest_timestamps_ms.len();
    }
    let youngest_cleanable_ms = now_ms.saturating_sub(min_lag_ms);
    largest_timestamps_ms
        .iter()
        .position(|timestamp| *timestamp > youngest_cleanable_ms)
        .unwrap_or(largest_timestamps_ms.len())
}

/// Kafka's cleanable test, over one partition's [`CompactionCandidacy`].
///
/// This is `grabFilthiestCompactedLog`'s filter: a log with nothing cleanable
/// is never due, one whose dirty region has outlived `max.compaction.lag.ms`
/// is due regardless of its ratio, and otherwise it is due once the cleanable
/// range is a larger share of the log than `min.cleanable.dirty.ratio`.
fn compaction_is_due(
    candidacy: CompactionCandidacy,
    schedule: CompactionSchedule,
    now_ms: i64,
) -> bool {
    if candidacy.cleanable_bytes.bytes_f64() <= 0.0 {
        return false;
    }
    if let (Some(max_lag), Some(oldest)) = (schedule.max_lag, candidacy.oldest_dirty_timestamp_ms)
        && now_ms.saturating_sub(oldest) > max_lag.millis_i64_trunc()
    {
        return true;
    }
    candidacy.cleanable_ratio().as_f64() > schedule.min_ratio.as_f64()
}

/// One output group of a compaction pass: the base offsets of the segments it
/// consumes and the rewrite that replaces them.
struct GroupRewrite {
    group_bases: Vec<Offset>,
    rewrite: RewriteOutput,
}

impl Log {
    /// Whether a compaction pass over this partition is due, as Kafka's
    /// `LogCleanerManager` decides it: the cleanable range has to be a large
    /// enough share of the log (`min.cleanable.dirty.ratio`), unless the dirty
    /// region has been dirty so long that `max.compaction.lag.ms` forces a
    /// pass regardless. `min.compaction.lag.ms` withholds the young tail from
    /// that range rather than holding the pass back.
    ///
    /// The cleanup policy itself is not read here. That is the caller's test,
    /// because a partition the policy excludes is one the broker's cleaner
    /// skips before it opens the log at all.
    ///
    /// # Panics
    /// Panics if the configuration lock is poisoned.
    #[must_use]
    pub fn compaction_due(&self, now: std::time::SystemTime, last_stable_offset: Offset) -> bool {
        let (min_lag, max_lag, min_ratio) = {
            let cfg = self.config.read().unwrap();
            (
                cfg.min_compaction_lag,
                cfg.max_compaction_lag,
                cfg.min_cleanable_dirty_ratio,
            )
        };
        let now_ms = retention::now_ms(now);
        compaction_is_due(
            self.compaction_candidacy(min_lag.millis_i64_trunc(), now_ms, last_stable_offset),
            CompactionSchedule {
                min_lag,
                max_lag,
                min_ratio,
            },
            now_ms,
        )
    }

    /// The clean/cleanable split and dirty-region age [`Self::compaction_due`]
    /// reads. See [`CompactionCandidacy`] for what counts as which and why.
    fn compaction_candidacy(
        &self,
        min_lag_ms: i64,
        now_ms: i64,
        last_stable_offset: Offset,
    ) -> CompactionCandidacy {
        // Kafka's clean prefix is what a previous pass produced, which it
        // reads from `cleaner-offset-checkpoint`. A log nothing has cleaned
        // has no clean prefix at all: its whole sealed region is dirty, which
        // is what makes a single sealed segment cleanable on the first pass.
        //
        // Clamped to the segments actually on hand: retention or truncation
        // can remove a previous pass's output out from under this count, and
        // a clean prefix wider than the sealed list itself is nonsensical.
        let clean_segments = self.clean_prefix_segments.min(self.segments.len());
        let clean_bytes = self.segments[..clean_segments]
            .iter()
            .fold(ByteSize::ZERO, |total, segment| total + segment.size());
        let dirty: Vec<&Segment> = self.segments[clean_segments..].iter().collect();
        let cleanable = self
            .cleanable_sealed_count(min_lag_ms, now_ms, last_stable_offset)
            .saturating_sub(clean_segments);
        let cleanable_bytes = dirty[..cleanable]
            .iter()
            .fold(ByteSize::ZERO, |total, segment| total + segment.size());
        let oldest_dirty_timestamp_ms = dirty
            .iter()
            .filter_map(|segment| {
                segment
                    .offset_for_timestamp(i64::MIN)
                    .map(|(_, timestamp)| timestamp)
            })
            .min();
        CompactionCandidacy {
            clean_bytes,
            cleanable_bytes,
            oldest_dirty_timestamp_ms,
        }
    }

    /// How many sealed segments one pass may consume, counting the clean
    /// prefix: the clean prefix plus the dirty segments neither Kafka's
    /// `min.compaction.lag.ms` nor the high watermark withholds.
    ///
    /// The lag applies to every dirty segment, the oldest one included, as
    /// Kafka's `findFirstUncleanableSegment` tests each dirty segment from the
    /// first dirty offset. A log nothing has cleaned, or one that has been
    /// reopened, has no clean prefix, so its oldest sealed segment is dirty
    /// and the lag holds it back while its records are still young. The clean
    /// prefix is a previous pass's output, which the lag already released.
    ///
    /// Zero for a log with no sealed segment at all, and zero for one whose
    /// oldest sealed segment still reaches above `last_stable_offset`.
    fn cleanable_sealed_count(
        &self,
        min_lag_ms: i64,
        now_ms: i64,
        last_stable_offset: Offset,
    ) -> usize {
        let clean_segments = self.clean_prefix_segments.min(self.segments.len());
        let dirty_timestamps: Vec<i64> = self.segments[clean_segments..]
            .iter()
            .map(Segment::max_timestamp)
            .collect();
        let by_lag = clean_segments + cleanable_prefix(&dirty_timestamps, now_ms, min_lag_ms);
        by_lag.min(self.sealed_segments_below(last_stable_offset))
    }

    /// How many of the leading sealed segments end at or below `bound`.
    ///
    /// A sealed segment ends where the next one begins, and the last one ends
    /// where the active segment does. Counting whole segments is what the
    /// rewrite consumes, so a segment straddling the bound is withheld
    /// entirely rather than split.
    fn sealed_segments_below(&self, bound: Offset) -> usize {
        self.segments
            .iter()
            .enumerate()
            .position(|(index, _)| self.sealed_segment_end(index) > bound)
            .unwrap_or(self.segments.len())
    }

    /// Kafka's `Cleaner.groupSegmentsBySize`: split a run of segment sizes
    /// into contiguous groups, none heavier than `segment_bytes`. Each group
    /// becomes one output segment of a compaction pass, so a long-lived
    /// compacted log never ends up with one sealed segment far larger than
    /// the configured limit. The grouping only combines segments; a single
    /// segment already at or over the limit still gets a group of its own,
    /// the same as Kafka never splitting one segment's records across two
    /// output segments.
    ///
    /// Returns the length of each group, in input order; the lengths sum to
    /// `sizes.len()`. An empty input returns no groups.
    fn group_segments_by_size(sizes: &[ByteSize], segment_bytes: ByteSize) -> Vec<usize> {
        let mut groups = Vec::new();
        let mut start = 0usize;
        while start < sizes.len() {
            let mut group_len = 1usize;
            let mut total = sizes[start];
            while start + group_len < sizes.len() {
                let candidate = total + sizes[start + group_len];
                if candidate > segment_bytes {
                    break;
                }
                total = candidate;
                group_len += 1;
            }
            groups.push(group_len);
            start += group_len;
        }
        groups
    }

    /// The first offset past sealed segment `index`.
    fn sealed_segment_end(&self, index: usize) -> Offset {
        self.segments.get(index + 1).map_or_else(
            || {
                self.active
                    .as_ref()
                    .map_or_else(|| self.log_end_offset(), Segment::base_offset)
            },
            Segment::base_offset,
        )
    }

    /// Run one compaction pass over the cleanable sealed segments.
    ///
    /// The pass never touches the active segment, and it stops short of the
    /// sealed segments `min.compaction.lag.ms` or `ctx.last_stable_offset`
    /// withholds -- the same range [`Self::compaction_due`] measures, so the
    /// decision and the pass agree on what one pass would do. The output is
    /// one or more new sealed segments, grouped so none exceeds
    /// `segment.bytes`, starting at the lowest input base offset; together
    /// they replace the sealed segments this pass consumed. Any withheld
    /// sealed segments stay where they are and become cleanable once their
    /// records outlive the lag.
    ///
    /// `ctx` carries the wall clock, which drives the KIP-534 delete-horizon
    /// computation, and the last stable offset. The pass keeps the last record
    /// of each producer in the producer state of this log, and the last batch
    /// of the pass, as an empty batch (`RETAIN_EMPTY`) when compaction removes
    /// all of their records. The last record of a producer is its last data
    /// batch, or a marker at its epoch when it has no data batch at that epoch.
    /// This is Kafka's `Cleaner.cleanSegments`, which reads
    /// `UnifiedLog.lastRecordsOfActiveProducers` on every replica. A leader
    /// and a follower update that state for each batch that they append, so
    /// they keep the same batches.
    #[instrument(
        level = "info",
        skip_all,
        fields(sealed_segments = self.segments.len()),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn compact(&mut self, ctx: &CompactionContext) -> Result<(), LogError> {
        self.rollover_flusher.check()?;
        if self.segments.is_empty() {
            return Ok(());
        }

        let (index_interval, delete_retention, min_lag, segment_bytes, record_limit) = {
            let cfg_guard = self.config.read().unwrap();
            if !cfg_guard.cleanup_policy.contains_compact() {
                return Ok(());
            }
            (
                cfg_guard.index_interval,
                cfg_guard.delete_retention,
                cfg_guard.min_compaction_lag,
                cfg_guard.segment_size,
                cfg_guard.max_decompressed_record,
            )
        };

        let now_ms = retention::now_ms(ctx.now);
        let consumed =
            self.cleanable_sealed_count(min_lag.millis_i64_trunc(), now_ms, ctx.last_stable_offset);
        if consumed == 0 {
            // Everything sealed is withheld — by the min lag, by the last
            // stable offset, or by both — so this pass has nothing to rewrite.
            return Ok(());
        }
        // Borrow sealed segments to run map + rewrite (`rewrite_segments`
        // reads each input segment's `.log` by path rather than through the
        // segment's own handle, so this borrow does not itself hold anything
        // open across the swap below).
        //
        // The offset map is built once over the whole consumed range:
        // whether a record is the newest for its key is a fact about the full
        // dirty region, not about whichever output group a record lands in.
        // The map skips the batches of aborted transactions, so it needs the
        // aborted transactions of the range, wherever their abort marker sits.
        // The rewrite itself then runs once per size-bounded group, so no
        // output segment grows past `segment.bytes`, and every group's
        // `.swap` files are written before any of them is promoted. One
        // transaction tracker walks all the groups in order, as a transaction
        // can span two of them.
        //
        // Kafka's `Cleaner.cleanSegments` reads the last record of each
        // active producer from the producer state of the log, once for the
        // groups of one pass.
        self.rollover_flusher.finish()?;
        let active_producers = self.last_records_of_active_producers();
        let mut rewrites: Vec<GroupRewrite> = Vec::new();
        {
            let sealed_refs: Vec<&Segment> = self.segments[..consumed].iter().collect();
            let consumed_end = self.sealed_segment_end(consumed - 1);
            let offset_map = build_offset_map(
                &sealed_refs,
                self.aborted_in_range(sealed_refs[0].base_offset(), consumed_end),
                record_limit,
            )?;
            let mut txn_meta = CleanedTransactionMetadata::default();
            let sizes: Vec<ByteSize> = sealed_refs.iter().map(|segment| segment.size()).collect();
            let groups = Self::group_segments_by_size(&sizes, segment_bytes);
            rewrites.reserve_exact(groups.len());

            let mut start = 0usize;
            for group_len in groups {
                let group_refs = &sealed_refs[start..start + group_len];
                let group_bases: Vec<Offset> = group_refs
                    .iter()
                    .map(|segment| segment.base_offset())
                    .collect();
                txn_meta.add_aborted_transactions(self.aborted_in_range(
                    group_refs[0].base_offset(),
                    self.sealed_segment_end(start + group_len - 1),
                ));
                let rewrite = rewrite_segments(
                    &*self.io,
                    &self.dir,
                    group_refs,
                    &offset_map,
                    &mut txn_meta,
                    RewriteRetention {
                        now_ms,
                        delete_retention,
                    },
                    CleaningRound {
                        active_producers: &active_producers,
                        // The round rewrites the offset map's whole range, so
                        // it ends where the last consumed segment does.
                        upper_bound: consumed_end,
                        max_decompressed_record: record_limit,
                    },
                )?;
                rewrites.push(GroupRewrite {
                    group_bases,
                    rewrite,
                });
                start += group_len;
            }
            // `sealed_refs` (and the borrow of `self.segments` it holds)
            // ends here, before the consumed segments are dropped below.
        }

        // Compaction rewrites sealed segments, so any record the last
        // activation walk read may not survive it. Nothing on disk has changed
        // until here: the offset map and every rewrite above write only
        // `.cleaned` files and can refuse the pass (a record above
        // `max.decompressed.message.bytes`), and a refused pass that reset the
        // schedule would send every later sweep of the partition through a full
        // delivery walk for nothing.
        let compacted_from = self.log_start_offset();
        self.invalidate_delivery_schedule(compacted_from);

        // Take ownership of the consumed segments out of `self.segments` and
        // drop them now, closing their `.log`/`.index`/`.timeindex` file
        // handles before any `atomic_swap` runs its delete/rename steps.
        // `compact::swap` requires this: on Windows, `remove_file` and
        // `rename` both fail while another handle on the same file is still
        // open, so promoting even the first output group while a consumed
        // segment's handle is still alive would fail every time.
        let consumed_segments: Vec<Segment> = self.segments.drain(..consumed).collect();
        drop(consumed_segments);

        let mut new_segments: Vec<Segment> = Vec::with_capacity(rewrites.len());
        for GroupRewrite {
            group_bases,
            rewrite,
        } in &rewrites
        {
            if self.config.read().unwrap().file_delete_delay <= krabka_units::Time::ZERO {
                atomic_swap(&*self.io, &self.dir, group_bases, rewrite)?;
            } else {
                let io = self.io.clone();
                let dir = self.dir.clone();
                let now = std::time::SystemTime::now();
                atomic_swap_retiring(&*io, &dir, group_bases, rewrite, |base| {
                    self.retire_files(base, now, &["log", "index", "timeindex", "txnindex"])
                })?;
            }

            // Validation scans the new log from byte zero, rebuilds both
            // sparse indexes, and derives exact offset and timestamp
            // frontiers before the segment is sealed.
            let mut new_seg = Segment::open_active_with_index_interval(
                &self.dir,
                rewrite.new_base_offset,
                true,
                index_interval,
            )?;
            new_seg.set_io(self.io.clone());
            new_seg.seal()?;
            let txn_index = TxnIndex::open(new_seg.txn_index_path())?;
            for base in group_bases {
                self.sealed_txn_indexes.remove(base);
            }
            self.sealed_txn_indexes
                .insert(rewrite.new_base_offset, txn_index);
            new_segments.push(new_seg);
        }

        // Every segment just inserted was rewritten from the full offset map
        // over this pass's consumed range, so all of them -- not only the
        // first -- are this pass's output and belong to the clean prefix.
        self.clean_prefix_segments = new_segments.len();
        self.segments.splice(0..0, new_segments);
        Ok(())
    }
}

#[cfg(test)]
mod producer_tests;
#[cfg(test)]
mod replication_tests;
#[cfg(test)]
mod transaction_tests;

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            CleanupPolicy,
            config::{DeliveryPolicy, LogConfig},
            log::test_support::{
                KeyedRecord, TempLog, append_keyed_samples, compact_test_log, compacting_segments,
                compaction_ctx, keyed_batch, set_segment_size, tiny_segments,
            },
            name,
            test_support::KeyValue,
        },
        bytes::BytesMut,
        krabka_units::prelude::{bytes, mebibytes},
        std::io::Write as _,
        tempfile::tempdir,
    };

    fn read_values(log: &Log) -> Vec<Vec<u8>> {
        log.read(Offset(0), mebibytes(1))
            .unwrap()
            .batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .map(|record| record.value.as_deref().unwrap().to_vec())
            .collect()
    }

    /// A high watermark past every offset these logs hold, for the tests
    /// whose subject is the ratio, the lag or the rewrite rather than the
    /// watermark bound.
    const UNBOUNDED_HW: Offset = Offset(i64::MAX);

    /// Kafka's cleanable test, over the shapes a partition can be in. The
    /// clock is fixed at `10_000` ms so a "dirty since" timestamp reads as an
    /// age directly.
    #[test]
    fn the_cleanable_test_reads_the_ratio_and_the_max_lag() {
        const NOW_MS: i64 = 10_000;
        let candidacy = |clean: u32, cleanable: u32, oldest: i64| CompactionCandidacy {
            clean_bytes: bytes(clean),
            cleanable_bytes: bytes(cleanable),
            oldest_dirty_timestamp_ms: Some(oldest),
        };
        let schedule = |min_lag_ms: i64, max_lag_ms: Option<i64>, ratio: f64| CompactionSchedule {
            min_lag: Time::from_millis(min_lag_ms),
            max_lag: max_lag_ms.map(Time::from_millis),
            min_ratio: fraction(ratio),
        };
        let cases = [
            (
                "nothing cleanable",
                candidacy(100, 0, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "cleanable two thirds of the log",
                candidacy(100, 200, 0),
                schedule(0, None, 0.5),
                true,
            ),
            (
                // Kafka's filter is `cleanableRatio > minCleanableRatio`, so
                // a log exactly at the threshold is not yet worth a pass.
                "cleanable exactly half the log",
                candidacy(100, 100, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "cleanable a quarter of the log is below the ratio",
                candidacy(300, 100, 0),
                schedule(0, None, 0.5),
                false,
            ),
            (
                "below the ratio but past the max lag",
                candidacy(300, 100, 0),
                schedule(0, Some(5_000), 0.5),
                true,
            ),
            (
                "below the ratio and inside the max lag",
                candidacy(300, 100, 9_000),
                schedule(0, Some(5_000), 0.5),
                false,
            ),
            (
                // The min lag withholds segments from the cleanable range; it
                // never holds back a pass over what is left, so a log with a
                // cleanable range above the ratio is due whatever the lag is.
                "above the ratio under a long min lag",
                candidacy(100, 200, 9_900),
                schedule(60_000, None, 0.5),
                true,
            ),
            (
                // Everything dirty is inside the min lag, so the cleanable
                // range is empty and even the max lag has nothing to clean.
                "the whole dirty region is withheld by the min lag",
                candidacy(100, 0, 9_900),
                schedule(60_000, Some(1), 0.5),
                false,
            ),
        ];
        for (label, candidacy, schedule, expected) in cases {
            assert2::check!(
                compaction_is_due(candidacy, schedule, NOW_MS) == expected,
                "{label}"
            );
        }
    }

    /// The min lag's own rule: it ends the cleanable range at the first
    /// segment whose largest timestamp is younger than the lag, and withholds
    /// nothing when it is zero.
    #[test]
    fn the_min_lag_ends_the_cleanable_range_at_the_first_young_segment() {
        const NOW_MS: i64 = 10_000;
        // Four dirty segments, the last two written inside a 1s lag.
        let timestamps = [1_000, 5_000, 9_500, 9_800];
        let cases = [
            ("no lag withholds nothing", 0, 4),
            ("a lag shorter than the whole log", 1_000, 2),
            ("a lag that covers every segment", 60_000, 0),
            // The boundary is Kafka's `largestTimestamp > now - minLag`, so a
            // segment exactly at the edge is still cleanable.
            ("a segment exactly at the edge", 5_000, 2),
            ("an empty log has no cleanable segment", 1_000, 0),
        ];
        for (label, min_lag_ms, expected) in cases {
            let stamps: &[i64] = if label.starts_with("an empty log") {
                &[]
            } else {
                &timestamps
            };
            assert2::check!(
                cleanable_prefix(stamps, NOW_MS, min_lag_ms) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_pass_leaves_a_log_the_cleaner_no_longer_owes_one() {
        // Twelve distinct keys survive the pass, so the segment it produces is
        // the bulk of the log and the ratio falls under Kafka's 0.5 default.
        let TempLog { dir: _dir, mut log } = compact_test_log();
        append_keyed_samples(&mut log, 12, |i| format!("k{i}"));
        assert2::check!(log.compaction_due(std::time::SystemTime::now(), UNBOUNDED_HW));

        // Grouping shares `segment.bytes` with the roll that packed these
        // one-record segments, so widen it before the pass: the fixture
        // still needs many small sealed segments going in, but this test is
        // about the ratio the pass leaves behind, not the grouping cap.
        set_segment_size(&mut log, mebibytes(1));
        log.compact(&compaction_ctx()).unwrap();
        assert2::check!(!log.compaction_due(std::time::SystemTime::now(), UNBOUNDED_HW));
    }

    /// One sealed segment per batch, each stamped `1_000 * i` milliseconds
    /// after the epoch, all under one key. The last append lands in the active
    /// segment.
    fn log_stamped_a_second_apart(dir: &std::path::Path, cfg: LogConfig, batches: i64) -> Log {
        let mut log = Log::open(dir, cfg).unwrap();
        for i in 0..batches {
            let value = format!("v{i}");
            let mut batch = keyed_batch(i, &[KeyedRecord {
                offset_delta: 0,
                key: b"key",
                value: value.as_bytes(),
            }]);
            batch.base_timestamp = i * 1_000;
            batch.max_timestamp = i * 1_000;
            log.append(&mut batch).unwrap();
        }
        log
    }

    /// A `SystemTime` `millis` after the epoch, which is the clock the
    /// stamped-segment tests reason in.
    fn at_epoch_millis(millis: u64) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(millis)
    }

    fn compact_at(log: &mut Log, millis: u64, stable: Offset) -> Result<(), LogError> {
        log.compact(&CompactionContext {
            now: at_epoch_millis(millis),
            last_stable_offset: stable,
        })
    }

    /// Kafka's `min.compaction.lag.ms` withholds the young tail from the
    /// cleanable range; it does not postpone the pass. A topic that takes a
    /// write more often than its lag would otherwise never be compacted at
    /// all, because its newest record is always too young.
    #[test]
    fn a_topic_written_to_faster_than_its_min_lag_is_still_due() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            // Longer than the second that separates two appends, and short
            // enough to leave the older segments cleanable.
            min_compaction_lag: Time::from_millis(1_500),
            ..tiny_segments()
        };
        let log = log_stamped_a_second_apart(dir.path(), cfg, 6);

        assert2::check!(log.compaction_due(at_epoch_millis(6_000), UNBOUNDED_HW));
    }

    /// Kafka's `Cleaner.groupSegmentsBySize`: a run of segment sizes splits
    /// into contiguous groups none heavier than `segment.bytes`, combining
    /// where it can and never splitting a single segment across groups.
    #[test]
    fn group_segments_by_size_caps_every_group_at_segment_bytes() {
        /// The segment sizes, `segment.bytes`, and the expected group lengths.
        struct GroupCase {
            sizes: &'static [u32],
            segment_bytes: u32,
            expected: &'static [usize],
        }
        let cases = [
            GroupCase {
                sizes: &[],
                segment_bytes: 100,
                expected: &[],
            },
            GroupCase {
                sizes: &[10],
                segment_bytes: 100,
                expected: &[1],
            },
            // Every size fits in one group together.
            GroupCase {
                sizes: &[10, 10, 10],
                segment_bytes: 100,
                expected: &[3],
            },
            // Nothing combines: each already at or over the limit.
            GroupCase {
                sizes: &[60, 60, 60],
                segment_bytes: 50,
                expected: &[1, 1, 1],
            },
            // A mix: the first two combine, the third starts a new group
            // with the fourth, and the fifth (alone, over the limit) gets
            // its own group.
            GroupCase {
                sizes: &[30, 30, 30, 30, 200],
                segment_bytes: 70,
                expected: &[2, 2, 1],
            },
        ];
        for GroupCase {
            sizes,
            segment_bytes,
            expected,
        } in cases
        {
            let sizes: Vec<ByteSize> = sizes.iter().map(|&n| bytes(n)).collect();
            let groups = Log::group_segments_by_size(&sizes, bytes(segment_bytes));
            assert2::check!(
                groups == expected,
                "sizes={sizes:?} segment_bytes={segment_bytes}"
            );
            assert2::check!(
                groups.iter().sum::<usize>() == sizes.len(),
                "group lengths must account for every input segment"
            );
        }
    }

    /// A log nothing has cleaned yet has no clean prefix: its one sealed
    /// segment is dirty, and the pass it is owed happens.
    ///
    /// This is Kafka's `cleaner-offset-checkpoint` read on a fresh log — the
    /// dirty region is `[logStart, firstUncleanable)`, not `[secondSegment,
    /// firstUncleanable)`. Treating the first sealed segment as clean before
    /// any pass has run makes a partition with a single sealed segment report
    /// a cleanable ratio of zero, so it is never due and a duplicate key
    /// survives however long the log sits there. The `compact,delete`
    /// end-to-end case in `jvm_acceptance_cli` is what caught it.
    #[test]
    fn a_never_cleaned_log_has_no_clean_prefix() {
        let dir = tempdir().unwrap();
        let cfg = compacting_segments();
        // Two appends: one sealed segment and the active one.
        let mut log = log_stamped_a_second_apart(dir.path(), cfg, 2);

        assert2::check!(log.segments.len() == 1, "one sealed segment");
        assert2::check!(
            log.compaction_due(at_epoch_millis(6_000), UNBOUNDED_HW),
            "a single dirty sealed segment is cleanable"
        );

        compact_at(&mut log, 6_000, UNBOUNDED_HW).unwrap();

        // The pass output is now the clean prefix, so the same log is not due
        // again until something else is produced.
        assert2::check!(
            !log.compaction_due(at_epoch_millis(6_000), UNBOUNDED_HW),
            "a log whose only sealed segment is the last pass's output is settled"
        );
    }

    /// Kafka's `findFirstUncleanableSegment` tests every dirty segment against
    /// `min.compaction.lag.ms`, the oldest one included. A log nothing has
    /// cleaned, or one that was just reopened, has no clean prefix, so its
    /// oldest sealed segment is dirty and the lag holds it back until its
    /// records are old enough. Consumers that rely on seeing every update
    /// inside the lag window would otherwise miss intermediate values.
    #[test]
    fn the_min_lag_holds_back_the_oldest_segment_of_a_never_cleaned_log() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            min_compaction_lag: Time::from_millis(3_000),
            ..tiny_segments()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        // The first segment holds two records under one key, written at
        // timestamp 0. A second batch seals it.
        let mut first = keyed_batch(0, &[
            KeyedRecord {
                offset_delta: 0,
                key: b"key",
                value: b"old",
            },
            KeyedRecord {
                offset_delta: 1,
                key: b"key",
                value: b"new",
            },
        ]);
        let mut second = keyed_batch(0, &[KeyedRecord {
            offset_delta: 0,
            key: b"other",
            value: b"v",
        }]);
        second.base_timestamp = 1_000;
        second.max_timestamp = 1_000;
        log.append(&mut first).unwrap();
        log.append(&mut second).unwrap();
        assert2::assert!(log.segments.len() == 1, "one sealed segment");
        let compact_at = |log: &mut Log, millis: u64| {
            compact_at(log, millis, UNBOUNDED_HW).unwrap();
        };

        // Every record is younger than the lag: nothing is due, and a pass
        // leaves the intermediate value alone.
        assert2::check!(!log.compaction_due(at_epoch_millis(2_500), UNBOUNDED_HW));
        compact_at(&mut log, 2_500);
        assert2::check!(read_values(&log) == vec![b"old".to_vec(), b"new".to_vec(), b"v".to_vec()]);

        // Once the segment's newest record has outlived the lag, the pass
        // deduplicates it.
        assert2::check!(log.compaction_due(at_epoch_millis(3_500), UNBOUNDED_HW));
        compact_at(&mut log, 3_500);
        assert2::check!(read_values(&log) == vec![b"new".to_vec(), b"v".to_vec()]);
    }

    /// The withheld tail is withheld from the pass as well, so a record inside
    /// the min lag is not deduplicated early: the pass rewrites the segments
    /// below the lag and leaves the rest where they are.
    #[test]
    fn a_pass_leaves_the_segments_the_min_lag_withholds() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            min_compaction_lag: Time::from_millis(2_500),
            ..tiny_segments()
        };
        // Sealed segments carry timestamps 0..=4_000 and the active one 5_000.
        // At 6_000 the lag withholds everything stamped after 3_500, so the
        // pass consumes the four segments stamped 0..=3_000.
        let mut log = log_stamped_a_second_apart(dir.path(), cfg, 6);
        // Grouping shares `segment.bytes` with the roll that produced these
        // one-record segments, so widen it before the pass: the fixture
        // still needs many small sealed segments going in, but this test is
        // about the min-lag withhold, not the grouping cap, so the output
        // should be free to collapse them into one.
        set_segment_size(&mut log, mebibytes(1));
        compact_at(&mut log, 6_000, UNBOUNDED_HW).unwrap();

        assert2::check!(log.segments.len() == 2);
        let values = read_values(&log);
        assert2::check!(values == vec![b"v3".to_vec(), b"v4".to_vec(), b"v5".to_vec()]);
    }

    /// Kafka's `cleanableOffsets` bounds a pass at the last stable offset: an
    /// uncommitted record can still be truncated away by a leader election,
    /// and a pass that dropped an older record in its favour would leave this
    /// replica short of one the leader kept.
    ///
    /// Six one-batch segments under one key, so every sealed segment is
    /// cleanable on the lag and the ratio, and only the last stable offset
    /// holds the pass back.
    #[test]
    fn the_last_stable_offset_bounds_the_cleanable_range() {
        let dir = tempdir().unwrap();
        let cfg = compacting_segments();
        let mut log = log_stamped_a_second_apart(dir.path(), cfg, 6);
        // Five sealed segments (one batch each) and the active sixth.
        assert2::assert!(log.segments.len() == 5);

        // Nothing is committed: no sealed segment ends at or below zero, so
        // there is nothing to be due for and a pass rewrites nothing.
        assert2::check!(!log.compaction_due(at_epoch_millis(6_000), Offset(0)));
        compact_at(&mut log, 6_000, Offset(0)).unwrap();
        assert2::check!(log.segments.len() == 5, "no segment was consumed");

        // Committing the first three segments makes exactly those cleanable:
        // the pass collapses them into one and leaves the rest alone. Widen
        // `segment.bytes` first, for the same reason as the min-lag test
        // above: grouping shares the config with the roll that produced
        // these one-record segments, and this test is about the last-stable-
        // offset bound, not the grouping cap.
        assert2::check!(log.compaction_due(at_epoch_millis(6_000), Offset(3)));
        set_segment_size(&mut log, mebibytes(1));
        compact_at(&mut log, 6_000, Offset(3)).unwrap();
        assert2::check!(log.segments.len() == 3);
        let values = read_values(&log);
        assert2::check!(
            values
                == vec![
                    b"v2".to_vec(),
                    b"v3".to_vec(),
                    b"v4".to_vec(),
                    b"v5".to_vec()
                ],
            "only the committed prefix was deduplicated",
        );
    }

    /// A watermark inside a sealed segment withholds that whole segment: the
    /// rewrite consumes segments, not record ranges, so a straddling segment
    /// cannot be half-cleaned.
    #[test]
    fn a_watermark_inside_a_segment_withholds_that_segment() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            // Two batches per sealed segment.
            segment_size: bytes(200),
            ..Default::default()
        };
        let mut log = log_stamped_a_second_apart(dir.path(), cfg, 6);
        let first_end = log.segments[1].base_offset();
        assert2::assert!(first_end > Offset(1), "the fixture packed a segment");

        // One offset short of the first segment's end: it straddles the
        // watermark, so nothing is cleanable at all.
        compact_at(&mut log, 6_000, first_end - 1).unwrap();
        let sealed_before = log.segments.len();
        assert2::check!(!log.compaction_due(at_epoch_millis(6_000), first_end - 1));

        // At its end offset the segment is committed in full and cleanable.
        // Grouping shares `segment.bytes` with the roll that packed this
        // segment, so widen it before the pass: this test is about the
        // watermark straddle, not the grouping cap, and the straddling
        // segment must stay untouched either way.
        assert2::check!(log.compaction_due(at_epoch_millis(6_000), first_end));
        set_segment_size(&mut log, mebibytes(1));
        compact_at(&mut log, 6_000, first_end).unwrap();
        assert2::check!(log.segments.len() == sealed_before);
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let kept: usize = out.batches.iter().map(|batch| batch.records.len()).sum();
        assert2::check!(
            kept == 5,
            "the first segment's duplicate went, nothing else"
        );
    }

    #[test]
    fn a_ratio_of_one_holds_a_partly_dirty_log_back_until_the_max_lag() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            min_cleanable_dirty_ratio: fraction(1.0),
            ..tiny_segments()
        };
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..3 {
            let value = format!("v{i}");
            let mut batch = keyed_batch(i, &[KeyedRecord {
                offset_delta: 0,
                key: b"key",
                value: value.as_bytes(),
            }]);
            log.append(&mut batch).unwrap();
        }
        assert2::check!(!log.compaction_due(std::time::SystemTime::now(), UNBOUNDED_HW));

        // The batches carry timestamp 0, so any max lag has long elapsed and
        // the pass is owed however clean the ratio says the log is.
        log.set_config(LogConfig {
            max_compaction_lag: Some(Time::from_millis(1)),
            ..cfg
        });
        assert2::check!(log.compaction_due(std::time::SystemTime::now(), UNBOUNDED_HW));
    }

    fn assert_corrupt_suffix_preserves_originals(suffix: &[u8]) {
        let TempLog { dir, mut log } = compact_test_log();
        for i in 0..3 {
            let value = format!("v{i}");
            let mut batch = keyed_batch(0, &[KeyedRecord {
                offset_delta: 0,
                key: b"key",
                value: value.as_bytes(),
            }]);
            log.append(&mut batch).unwrap();
        }
        assert2::assert!(!log.segments.is_empty());

        let corrupt_path = name::log_path(dir.path(), log.segments[0].base_offset().0);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&corrupt_path)
            .unwrap()
            .write_all(suffix)
            .unwrap();

        let originals: Vec<_> = log
            .segments
            .iter()
            .map(|segment| {
                let path = name::log_path(dir.path(), segment.base_offset().0);
                (path.clone(), std::fs::read(path).unwrap())
            })
            .collect();
        let bases_before: Vec<_> = log.segments.iter().map(Segment::base_offset).collect();

        let error = log.compact(&compaction_ctx()).unwrap_err();
        assert2::assert!(matches!(error, LogError::Records(_) | LogError::Corrupt(_)));
        assert2::assert!(
            log.segments
                .iter()
                .map(Segment::base_offset)
                .collect::<Vec<_>>()
                == bases_before
        );
        for (path, bytes) in originals {
            assert2::assert!(std::fs::read(path).unwrap() == bytes);
        }
    }

    #[test]
    fn compact_rejects_truncated_suffix_without_replacing_originals() {
        assert_corrupt_suffix_preserves_originals(&[0; 16]);
    }

    #[test]
    fn compact_rejects_crc_corrupt_suffix_without_replacing_originals() {
        let batch = keyed_batch(0, &[KeyedRecord {
            offset_delta: 0,
            key: b"corrupt",
            value: b"suffix",
        }]);
        let mut encoded = BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut encoded).unwrap();
        // The stored CRC occupies bytes 17..21. Changing it preserves framing
        // while making the complete suffix fail integrity validation.
        encoded[17] ^= 1;
        assert_corrupt_suffix_preserves_originals(&encoded);
    }

    /// Kafka trunk's cleaner reads every record of a compressed batch it
    /// indexes, and a record above `max.decompressed.message.bytes` throws
    /// `InvalidRecordException`, which `LogCleaner` turns into an uncleanable
    /// partition. The record here is superseded by a later write, and Kafka
    /// refuses it all the same, because the offset map reads it before it knows
    /// that. The refused pass leaves the log as it found it.
    fn append_compressed_versions(log: &mut Log, codec: krabka_compression::CompressionType) {
        let large = [b'x'; 1_000];
        for (key, value) in [
            (b"k1".as_slice(), large.as_slice()),
            (b"k1", b"new"),
            (b"tail", b"t"),
        ] {
            let mut batch = keyed_batch(0, &[KeyedRecord {
                offset_delta: 0,
                key,
                value,
            }]);
            batch.attributes = batch.attributes.with_compression(codec);
            log.append(&mut batch).unwrap();
        }
    }

    fn append_versions(log: &mut Log) {
        for i in 0..3 {
            let value = format!("v{i}");
            log.append(&mut keyed_batch(0, &[KeyedRecord {
                offset_delta: 0,
                key: b"k1",
                value: value.as_bytes(),
            }]))
            .unwrap();
        }
    }

    /// Three sealed versions of k1 plus an active record outside the pass.
    /// A segment's base offset and the bytes of its `.log` file.
    #[derive(Debug, PartialEq, Eq)]
    struct SegmentFile {
        base: i64,
        contents: Vec<u8>,
    }

    fn versioned_log(dir: &std::path::Path, active: KeyValue<'_>) -> Log {
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            segment_size: bytes(256), // force rolls
            ..Default::default()
        };
        let mut log = Log::open(dir, cfg).unwrap();
        append_versions(&mut log);
        log.append(&mut keyed_batch(0, &[KeyedRecord {
            offset_delta: 0,
            key: active.key,
            value: active.value,
        }]))
        .unwrap();
        log
    }

    fn sealed_sizes(log: &Log) -> Vec<ByteSize> { log.segments.iter().map(Segment::size).collect() }

    fn append_distinct_keys(log: &mut Log) { append_keyed_samples(log, 6, |i| format!("k{i}")); }

    #[yare::parameterized(
        compressed_oversized = {
            "compressed, oversized",
            krabka_compression::CompressionType::Gzip,
            Some(bytes(100)),
            true,
        },
        compressed_without_limit = {
            "compressed, no limit",
            krabka_compression::CompressionType::Gzip,
            None,
            false,
        },
        uncompressed_oversized = {
            "uncompressed, oversized",
            krabka_compression::CompressionType::None,
            Some(bytes(100)),
            false,
        },
    )]
    fn compact_refuses_a_compressed_record_above_the_limit_and_leaves_the_log_alone(
        name: &'static str,
        codec: krabka_compression::CompressionType,
        limit: Option<ByteSize>,
        refuses: bool,
    ) {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            max_decompressed_record: limit,
            ..tiny_segments()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        append_compressed_versions(&mut log, codec);
        let files = |log: &Log| -> Vec<SegmentFile> {
            log.segments
                .iter()
                .map(|segment| {
                    let base = segment.base_offset().0;
                    SegmentFile {
                        base,
                        contents: std::fs::read(name::log_path(dir.path(), base)).unwrap(),
                    }
                })
                .collect()
        };
        let before = files(&log);

        let result = log.compact(&compaction_ctx());

        if refuses {
            let error = result.unwrap_err();
            assert2::check!(
                matches!(error, LogError::RecordTooLarge { limit: 100, .. }),
                "{name}: {error}"
            );
            assert2::check!(files(&log) == before, "{name}");
        } else {
            result.unwrap();
            let out = log.read(Offset(0), mebibytes(1)).unwrap();
            let k1: Vec<&[u8]> = out
                .batches
                .iter()
                .flat_map(|batch| batch.records.iter())
                .filter(|record| record.key.as_deref() == Some(b"k1".as_slice()))
                .filter_map(|record| record.value.as_deref())
                .collect();
            assert2::check!(k1 == [b"new".as_slice()], "{name}");
        }
    }

    /// A pass rewrites records, so it resets what the last delivery walk learned
    /// about them, and it does that only once it will change the log. A pass a
    /// record above `max.decompressed.message.bytes` refuses touches nothing on
    /// disk, and a refusal that dropped the watermark would send every later
    /// sweep of the partition through a full delivery walk.
    #[yare::parameterized(
        refused = { "refused", Some(bytes(100)), true },
        rewritten = { "rewritten", None, false },
    )]
    fn only_a_pass_that_rewrites_resets_the_delivery_watermark(
        name: &'static str,
        limit: Option<ByteSize>,
        refuses: bool,
    ) {
        use krabka_compression::CompressionType;

        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            delivery_policy: DeliveryPolicy::Scheduled,
            max_decompressed_record: limit,
            ..tiny_segments()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        append_compressed_versions(&mut log, CompressionType::Gzip);
        // Every batch is stamped 0, long past, so the walk reads them all.
        log.advance_delivery_watermark(1_000_000);
        assert2::assert!(log.delivery_watermark() == log.log_end_offset(), "{name}");

        let result = log.compact(&compaction_ctx());

        assert2::check!(result.is_err() == refuses, "{name}");
        let expected = if refuses {
            log.log_end_offset()
        } else {
            log.log_start_offset()
        };
        assert2::check!(log.delivery_watermark() == expected, "{name}");
    }

    #[test]
    fn compact_no_op_when_only_one_segment() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: CleanupPolicy::Compact,
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        let mut b = keyed_batch(0, &[KeyedRecord {
            offset_delta: 0,
            key: b"k1",
            value: b"v1",
        }]);
        log.append(&mut b).unwrap();
        // Only the active segment exists; sealed list is empty.
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == 1);
    }

    #[test]
    fn compact_dedupes_sealed_segments_keeps_active_intact() {
        let dir = tempdir().unwrap();
        // Write 3 sealed segments, each with one record under "k1", then a
        // fresh active segment outside the compaction pass.
        let mut log = versioned_log(dir.path(), KeyValue {
            key: b"active-key",
            value: b"active-value",
        });

        let active_leo_before = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == active_leo_before);

        // After compaction: read everything, assert only the newest k1 plus
        // the active "active-key" survive.
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let all_records: Vec<_> = out.batches.iter().flat_map(|b| b.records.iter()).collect();
        let keys: Vec<&[u8]> = all_records
            .iter()
            .map(|r| r.key.as_ref().unwrap().as_ref())
            .collect();
        assert2::assert!(keys.contains(&b"k1".as_ref()));
        assert2::assert!(keys.contains(&b"active-key".as_ref()));
    }

    /// Compaction must run and must not do nothing. Three sealed segments
    /// each carry a record under the SAME key "k1". After `compact`, exactly
    /// ONE k1 record must remain, the newest one, "v2". The sealed segment
    /// list must collapse to a single rewritten segment. A compaction that
    /// returned `Ok(())` at once would leave all three k1 records and three
    /// sealed segments.
    #[test]
    fn compact_actually_dedupes_reducing_record_count() {
        let TempLog { dir: _dir, mut log } = compact_test_log();

        // Three sealed segments, each one record under "k1" (v0, v1, v2).
        append_versions(&mut log);
        // A final append lands in a fresh active segment (untouched by compact).
        let mut tail = keyed_batch(0, &[KeyedRecord {
            offset_delta: 0,
            key: b"tail",
            value: b"t",
        }]);
        log.append(&mut tail).unwrap();

        // Sanity: before compaction there are >= 2 sealed segments holding the
        // three k1 versions.
        assert2::assert!(log.segments.len() >= 2);

        // Grouping shares `segment.bytes` with the roll that packed these
        // one-record segments, so widen it before the pass: this test is
        // about dedup collapsing the sealed segments, not the grouping cap.
        set_segment_size(&mut log, mebibytes(1));
        log.compact(&compaction_ctx()).unwrap();

        // Sealed segments collapse to exactly one rewritten segment.
        assert2::assert!(log.segments.len() == 1);

        // Exactly one surviving k1 record, and it is the newest value "v2".
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let k1_values: Vec<&[u8]> = out
            .batches
            .iter()
            .flat_map(|b| b.records.iter())
            .filter(|r| r.key.as_deref() == Some(b"k1".as_ref()))
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert2::assert!(k1_values == vec![b"v2".as_ref()]);
    }

    #[test]
    fn compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let mut log = versioned_log(dir.path(), KeyValue {
            key: b"active",
            value: b"x",
        });
        log.compact(&compaction_ctx()).unwrap();
        let leo1 = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        let leo2 = log.log_end_offset();
        assert2::assert!(leo1 == leo2);
    }

    /// A pass that splits into more than one output segment
    /// (`Log::group_segments_by_size`) must mark every output clean, not only
    /// the first: `clean_prefix_segments` tracks the count of leading clean
    /// segments rather than a single boolean, so the whole output run counts
    /// toward the clean prefix `compaction_candidacy` reads.
    ///
    /// Before this fix, `compacted_once` was a `bool`, so
    /// `compaction_candidacy` treated only the first output segment as clean
    /// and the rest as freshly dirty -- a log made entirely of one pass's own
    /// outputs would still (wrongly) report itself due for another pass.
    #[test]
    fn a_multi_group_pass_treats_every_output_as_clean() {
        let TempLog { dir: _dir, mut log } = compact_test_log();
        // Six distinct keys: nothing is superseded, so the pass only
        // regroups records into size-bounded outputs, it never shrinks them.
        append_distinct_keys(&mut log);
        // Five sealed segments (one record each) and the active sixth.
        assert2::assert!(log.segments.len() == 5);

        let sealed_bytes: Vec<ByteSize> = sealed_sizes(&log);
        // A cap that fits exactly the first two sealed segments together, so
        // the pass is forced to produce more than one output group.
        let group_cap = sealed_bytes[0] + sealed_bytes[1];
        assert2::assert!(
            Log::group_segments_by_size(&sealed_bytes, group_cap).len() >= 2,
            "the fixture must actually exercise more than one output group"
        );

        // `segment.bytes` at compact time is what `Log::compact` reads for
        // the grouping cap; widen it to exactly `group_cap`.
        set_segment_size(&mut log, group_cap);
        log.compact(&compaction_ctx()).unwrap();

        let output_segments = log.segments.len();
        assert2::assert!(
            output_segments >= 2,
            "the pass must have split into more than one output segment"
        );
        assert2::assert!(
            log.clean_prefix_segments == output_segments,
            "every rewritten output segment must count toward the clean prefix"
        );
        assert2::check!(
            !log.compaction_due(std::time::SystemTime::now(), UNBOUNDED_HW),
            "a log made entirely of this pass's own outputs owes no further pass"
        );
    }

    /// [`compact::swap`] requires every consumed segment's file handle
    /// closed before it deletes or renames the underlying files -- on
    /// Windows, `remove_file` and `rename` both fail while another handle on
    /// the same file is open. `Log::compact` meets that precondition by
    /// draining the consumed segments out of `self.segments` (dropping their
    /// `Segment`s, and with them the `Arc<File>` handles inside) before any
    /// `atomic_swap` call runs, for every output group in a multi-group pass,
    /// not only the first.
    ///
    /// This test cannot reproduce the Windows failure itself on this
    /// platform (removing or renaming a file out from under an open handle
    /// is not an error on Linux), so it instead pins the invariant a
    /// Windows run depends on: once a multi-group pass returns, none of the
    /// original consumed segments' files remain, only every output's own
    /// files. That is only possible if the consumed segments were dropped
    /// (and their handles closed) before the swap ran, since a leftover open
    /// handle guarding a still-existing original file is exactly what the
    /// precondition rules out.
    #[test]
    fn a_multi_group_pass_drops_every_original_segment_file_before_swap() {
        let TempLog { dir, mut log } = compact_test_log();
        append_distinct_keys(&mut log);
        let original_bases: Vec<i64> = log.segments.iter().map(|s| s.base_offset().0).collect();
        assert2::assert!(original_bases.len() == 5);

        let sealed_bytes: Vec<ByteSize> = sealed_sizes(&log);
        let group_cap = sealed_bytes[0] + sealed_bytes[1];
        set_segment_size(&mut log, group_cap);
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(
            log.segments.len() >= 2,
            "the fixture exercises multiple groups"
        );

        // Every original consumed segment's `.log`/`.index`/`.timeindex`
        // files are gone, except where an output group's own base offset
        // happens to equal one it consumed (the lowest offset in that
        // group): `atomic_swap` only removes the others once their
        // `Segment` handle is closed, so their absence here is only
        // possible because the drain-before-swap ordering held for every
        // group.
        let new_bases: Vec<i64> = log.segments.iter().map(|s| s.base_offset().0).collect();
        for original_base in original_bases {
            if new_bases.contains(&original_base) {
                continue;
            }
            assert2::assert!(!name::log_path(dir.path(), original_base).exists());
            assert2::assert!(!name::index_path(dir.path(), original_base).exists());
            assert2::assert!(!name::timeindex_path(dir.path(), original_base).exists());
        }
        // Every output segment's own files exist, and no `.swap` scratch
        // file was left behind.
        for base in &new_bases {
            assert2::assert!(name::log_path(dir.path(), *base).exists());
        }
        let leftover_swaps = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().ends_with(".swap"));
        assert2::assert!(!leftover_swaps, "no .swap scratch file must remain");
    }
}
