//! Periodic maintenance: the time- and size-based retention sweep, Kafka's
//! `UnifiedLog.deleteOldSegments`.
//!
//! The segment roll itself is not here. Kafka rolls inside the append, in
//! [`Log::should_roll_for_incoming`], against the incoming batch's own size
//! and timestamp; a wall-clock roll on this sweep would roll an idle
//! partition's active segment, which Kafka never does.
//!
//! Retention may delete every segment, the active one included, and then
//! rolls first so the log keeps a fresh empty segment. It never deletes an
//! empty newest segment, and never evicts a segment that still holds a record
//! whose delivery time has not arrived.

use {
    super::Log,
    crate::{error::LogError, producer_snapshot, retention, segment::Segment},
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSizeExt, TimeExt as _},
    krabka_verified::retention::{LocalRetentionSegment, local_retention_prefix},
    std::{collections::HashSet, time::SystemTime},
    tracing::instrument,
};

impl Log {
    /// Periodic maintenance: apply time- and size-based retention when `cleanup.policy` holds `delete`, and delete
    /// segments below the log start offset under every policy. The passes
    /// follow Kafka's `UnifiedLog.deleteOldSegments`: size retention deletes
    /// an oldest segment only while the bytes over `retention.bytes` still
    /// cover the whole segment, and time retention ages a segment by Kafka's
    /// `largestTimestamp()` -- its newest record timestamp, or its `.log`
    /// file's modification time when no record carries one. The walk covers
    /// the active segment too; when every segment goes, the active one is
    /// rolled first, as Kafka's `deleteSegments` does, and the fresh empty
    /// segment stays.
    ///
    /// Like Kafka's `deletableSegments`, the walk stops at the first segment
    /// the partition's `high_watermark` has not passed: a segment whose upper
    /// bound (the next segment's base, or the log end for the active one) is
    /// above it still holds records followers may not have replicated.
    #[instrument(
        level = "debug",
        skip_all,
        fields(evicted = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn tick(&mut self, now: SystemTime, high_watermark: Offset) -> Result<(), LogError> {
        self.rollover_flusher.check()?;
        self.flush_if_due(std::time::Instant::now())?;
        self.reap_deleted_files(now)?;
        // Tiered topics' segment lifecycle is owned by the RemoteLogManager.
        if self.config.read().unwrap().remote_storage_enable {
            return Ok(());
        }
        // Refresh the visibility watermark before retention reads it, so a
        // partition that no consumer has fetched from still gets an accurate
        // floor. On a topic that delivers immediately this returns the log
        // end and does no I/O.
        let visible_floor = self
            .advance_delivery_watermark(retention::now_ms(now))
            .watermark;

        let now_ms = retention::now_ms(now);
        let cfg_guard = self.config.read().unwrap();
        // Kafka's `UnifiedLog.deleteOldSegments`: time and size retention run
        // only when `cleanup.policy` holds `delete`. A compact-only log keeps
        // every key however old it is, and loses a segment only when the whole
        // segment is below the log start offset.
        let retention_applies = cfg_guard.cleanup_policy.contains_delete();
        // Kafka's `deleteRetentionMsBreachedSegments` ages a segment by its
        // `largestTimestamp()` and deletes it once `now - anchor >
        // retention.ms`. Truncating, not rounding: a sub-millisecond window
        // must not round up into deleting a segment a millisecond early.
        let time_cutoff = cfg_guard
            .retention
            .filter(|_| retention_applies)
            .map(|window| now_ms.saturating_sub(window.millis_i64_trunc()));
        // Kafka's `deleteRetentionSizeBreachedSegments` runs only when the log
        // is at least its budget, and a log exactly at its budget still runs
        // it with a debt of zero. The active segment counts toward the size.
        let size_debt = cfg_guard
            .retention_size
            .filter(|_| retention_applies)
            .and_then(|budget| self.size().bytes_u64().checked_sub(budget.bytes_u64()));
        drop(cfg_guard);

        // Kafka's `deletableSegments` walks every segment, the active one
        // last, and `deleteOldSegments` runs three passes over them: the
        // log-start breach, `retention.bytes`, then `retention.ms`. The kernel
        // folds the breach into the time flag, which deletes the same prefix
        // (see `local_retention_model`).
        //
        // Kafka's `deleteLogStartOffsetBreachedSegments`: a segment whose next
        // segment starts at or below the log start offset holds no record at
        // or above it. Every policy deletes it. The active segment has no next
        // segment, so the breach never reaches it.
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        let segments: Vec<&Segment> = self.segments.iter().chain(self.active.as_ref()).collect();
        let next_bases = segments
            .iter()
            .skip(1)
            .map(|segment| Some(segment.base_offset()))
            .chain(std::iter::once(None));
        // On an immediate topic the floor is the log end, so no segment is
        // blocked. On a scheduled topic the first waiting segment stops the
        // prefix; later segments are never skipped around it.
        let facts: Vec<LocalRetentionSegment> = segments
            .iter()
            .zip(next_bases)
            .map(|(segment, next_base)| LocalRetentionSegment {
                // Kafka's `deletableSegments` requires `highWatermark >=
                // upperBoundOffset`, the next segment's base or the log end.
                blocked: segment.last_offset() >= visible_floor
                    || next_base.unwrap_or(log_end) > high_watermark,
                expired: next_base.is_some_and(|next_base| next_base <= log_start)
                    || time_cutoff.is_some_and(|cutoff| self.largest_timestamp(segment) < cutoff),
                size: segment.size().bytes_u64(),
            })
            .collect();
        let mut evict_len = local_retention_prefix(&facts, size_debt);
        if self.active.is_none() {
            // Nothing to roll into: the newest sealed segment stays.
            evict_len = evict_len.min(self.segments.len().saturating_sub(1));
        } else if evict_len == facts.len() {
            // Kafka's `deleteSegments`: a log must always keep a segment, so
            // when every segment goes, the active one is rolled first and the
            // fresh empty segment the roll opens is the one that stays. The
            // kernel never selects an empty newest segment, so the roll always
            // seals records and never recreates what it is about to delete.
            self.roll_active_segment()?;
        }
        let to_evict: Vec<Offset> = self
            .segments
            .iter()
            .take(evict_len)
            .map(Segment::base_offset)
            .collect();
        if !to_evict.is_empty() {
            self.rollover_flusher.finish()?;
        }

        // Unlink first, and forget only what actually left the disk. A failed
        // unlink otherwise drops the segment from `self.segments` -- and from
        // `Log::size`, and from the partition's disk gauge -- while its bytes
        // stay on the filesystem with nothing left to retry them. Eviction is
        // a prefix, so stopping at the first failure keeps it one.
        let mut deleted: HashSet<Offset> = HashSet::with_capacity(to_evict.len());
        let mut failure: Option<LogError> = None;
        for base in to_evict {
            if let Err(error) = self.retire_segment(base, now) {
                failure = Some(error);
                break;
            }
            deleted.insert(base);
            // Kafka's `deleteSegments` → `deleteProducerSnapshots`: the
            // snapshot at a deleted segment's base goes with it.
            if let Err(error) = producer_snapshot::remove_at(&self.dir, base) {
                failure = Some(error);
                break;
            }
        }
        tracing::Span::current().record("evicted", deleted.len());
        self.segments
            .retain(|s| !deleted.contains(&s.base_offset()));
        self.sealed_txn_indexes
            .retain(|base, _| !deleted.contains(base));
        self.stamp_indexes.retain(|base, _| !deleted.contains(base));
        if let Some(error) = failure {
            // The floor still follows whatever did come off disk, so the log
            // start is refreshed before the failure is reported.
            self.set_log_start_offset(self.first_local_offset())?;
            return Err(error);
        }
        // Ordinary retention deletes the records outright: nothing holds them
        // any more, so the global floor follows the files off disk (Kafka's
        // `deleteSegments` → `maybeIncrementLogStartOffset`). This is the
        // opposite of the tiered eviction in `delete_local_segments_through`,
        // which leaves the floor behind because the remote tier still answers
        // for those offsets. The early return above keeps a tiered topic out
        // of this path entirely.
        self.set_log_start_offset(self.first_local_offset())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::LogConfig,
            log::test_support::{
                TempLog, append_samples, install_stamps, rolled_log, rolling_test_log,
                sample_batch, test_log, tiny_segments,
            },
        },
        assert2::check,
        krabka_units::prelude::{ByteSize, kibibytes, millis, secs},
        tempfile::tempdir,
    };

    /// Retention may take the whole log, but it never leaves the log without a
    /// segment, however far past the budget the log is.
    ///
    /// A log with no segments has nowhere to append and no offset to report.
    /// Kafka's `deleteSegments` rolls before it deletes every segment, so the
    /// fresh empty segment that roll opens is the one that stays, and the log
    /// end and start both land on it.
    #[test]
    fn retention_never_evicts_the_last_segment() {
        let dir = tempdir().unwrap();
        // Roll often, and keep nothing: everything is evictable.
        let config = LogConfig {
            segment_size: kibibytes(1),
            retention_size: Some(ByteSize::ZERO),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        append_samples(&mut log, 40, 4);
        check!(!log.segments.is_empty(), "the appends should have rolled");

        let end = log.log_end_offset();
        log.tick(SystemTime::now(), log.log_end_offset())
            .expect("tick");
        check!(log.segments.is_empty(), "every sealed segment is gone");
        check!(
            log.active.as_ref().map(Segment::base_offset) == Some(end),
            "an empty active segment stays at the log end"
        );
        check!(log.log_start_offset() == end);
        // And it is still usable afterwards.
        let mut batch = sample_batch(1);
        check!(
            log.append(&mut batch).is_ok(),
            "the log still accepts appends"
        );
    }

    /// Kafka's `UnifiedLog.deleteOldSegments`: `cleanup.policy` decides which
    /// passes run. Each log has three one-record segments. Offset 0 is older
    /// than `retention.ms`, and offsets 1 and 2 are fresh. The base offsets
    /// that survive the tick are compared as a whole.
    #[test]
    fn tick_applies_time_and_size_retention_only_when_the_policy_deletes() {
        use crate::config::CleanupPolicy;

        /// The policy, the retention byte budget, the log start offset set
        /// before the tick, and the base offsets that survive it.
        struct Case {
            cleanup_policy: CleanupPolicy,
            retention_size: Option<ByteSize>,
            log_start: Offset,
            expected: Vec<Offset>,
        }

        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let all = vec![Offset(0), Offset(1), Offset(2)];
        let cases = [
            Case {
                cleanup_policy: CleanupPolicy::Delete,
                retention_size: None,
                log_start: Offset(0),
                expected: vec![Offset(1), Offset(2)],
            },
            Case {
                cleanup_policy: CleanupPolicy::Compact,
                retention_size: None,
                log_start: Offset(0),
                expected: all.clone(),
            },
            Case {
                cleanup_policy: CleanupPolicy::CompactAndDelete,
                retention_size: None,
                log_start: Offset(0),
                expected: vec![Offset(1), Offset(2)],
            },
            // Size retention with a budget of nothing deletes every segment,
            // the active one behind a roll, but only under a policy that
            // deletes.
            Case {
                cleanup_policy: CleanupPolicy::Delete,
                retention_size: Some(ByteSize::ZERO),
                log_start: Offset(0),
                expected: vec![],
            },
            Case {
                cleanup_policy: CleanupPolicy::Compact,
                retention_size: Some(ByteSize::ZERO),
                log_start: Offset(0),
                expected: all.clone(),
            },
            Case {
                cleanup_policy: CleanupPolicy::CompactAndDelete,
                retention_size: Some(ByteSize::ZERO),
                log_start: Offset(0),
                expected: vec![],
            },
            // A segment wholly below the log start offset goes under every
            // policy, and a segment the start offset only reaches stays.
            Case {
                cleanup_policy: CleanupPolicy::Compact,
                retention_size: None,
                log_start: Offset(1),
                expected: vec![Offset(1), Offset(2)],
            },
            Case {
                cleanup_policy: CleanupPolicy::Compact,
                retention_size: None,
                log_start: Offset(2),
                expected: vec![Offset(2)],
            },
        ];

        for Case {
            cleanup_policy,
            retention_size,
            log_start,
            expected,
        } in cases
        {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig {
                retention: Some(secs(10)),
                retention_size,
                cleanup_policy,
                ..tiny_segments()
            })
            .unwrap();
            for timestamp in [1_000, 95_000, 95_000] {
                let mut batch = sample_batch(1);
                batch.base_timestamp = timestamp;
                batch.max_timestamp = timestamp;
                log.append(&mut batch).unwrap();
            }
            log.set_log_start_offset(log_start).unwrap();

            log.tick(now, log.log_end_offset()).unwrap();

            let surviving: Vec<Offset> = log
                .segments
                .iter()
                .chain(log.active.as_ref())
                .filter(|segment| segment.size() > ByteSize::ZERO)
                .map(Segment::base_offset)
                .collect();
            check!(
                surviving == expected,
                "{cleanup_policy:?} {retention_size:?} start={log_start:?}"
            );
        }
    }

    #[test]
    fn tick_with_no_retention_is_noop() {
        let TempLog { dir: _dir, mut log } = test_log();
        let mut b1 = sample_batch(2);
        let mut b2 = sample_batch(3);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now(), log.log_end_offset()).unwrap();
        assert2::assert!(log.log_end_offset() == before);
    }

    /// Kafka's `deletableSegments` walks the active segment too: a lone
    /// active segment past `retention.ms` goes behind a roll, and the log
    /// keeps an empty segment at its end with the start moved up to it. An
    /// empty active segment is never deleted, so a second tick changes
    /// nothing.
    #[test]
    fn tick_deletes_a_breached_active_segment_behind_a_roll() {
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention: Some(secs(1)),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        log.append(&mut sample_batch(2)).unwrap();
        let now = SystemTime::now() + Duration::from_hours(30 * 24);

        for _ in 0..2 {
            log.tick(now, log.log_end_offset()).unwrap();
            check!(log.segments.is_empty());
            check!(log.active.as_ref().map(Segment::base_offset) == Some(Offset(2)));
            check!(log.log_start_offset() == Offset(2));
            check!(log.log_end_offset() == Offset(2));
        }
    }

    /// Kafka's `LogSegment.largestTimestamp()`: a segment whose records carry
    /// no timestamp (`-1`) is aged by its `.log` file's modification time, so
    /// it stays while the file is young and goes once the file is older than
    /// `retention.ms`.
    #[test]
    fn a_segment_without_record_timestamps_is_aged_by_its_file() {
        use std::time::Duration;
        for (name, age, deleted) in [
            ("a young file stays", Duration::ZERO, false),
            ("an old file goes", Duration::from_hours(2), true),
        ] {
            let dir = tempdir().unwrap();
            let mut log = Log::open(dir.path(), LogConfig {
                retention: Some(secs(60 * 60)),
                ..tiny_segments()
            })
            .unwrap();
            let mut untimed = sample_batch(1);
            untimed.max_timestamp = -1;
            log.append(&mut untimed).unwrap();
            log.append(&mut sample_batch(1)).unwrap();
            let now = SystemTime::now() + age;
            // Keep the newest segment inside the window whatever `now` is.
            let mut fresh = sample_batch(1);
            fresh.max_timestamp = retention::now_ms(now);
            log.append(&mut fresh).unwrap();

            log.tick(now, log.log_end_offset()).unwrap();

            let first = log.segments.first().map(Segment::base_offset);
            check!((first != Some(Offset(0))) == deleted, "{name}: {first:?}");
        }
    }

    /// Kafka's `deletableSegments` deletes a segment only once the high
    /// watermark has passed its upper bound: the next segment's base, or the
    /// log end for the active segment.
    #[test]
    fn retention_stops_at_the_high_watermark() {
        // `(what, high watermark, log start after the tick)`. Three
        // one-record segments at 0, 1 and 2, all past retention.ms.
        let rows = [
            ("nothing is replicated", Offset(0), Offset(0)),
            ("the first segment is replicated", Offset(1), Offset(1)),
            ("the sealed segments are replicated", Offset(2), Offset(2)),
            ("the whole log is replicated", Offset(3), Offset(3)),
        ];
        for (what, high_watermark, expected_start) in rows {
            let dir = tempdir().unwrap();
            let config = LogConfig {
                retention: Some(secs(1)),
                ..tiny_segments()
            };
            let mut log = Log::open(dir.path(), config).unwrap();
            for _ in 0..3 {
                let mut batch = sample_batch(1);
                batch.base_timestamp = 1_000;
                batch.max_timestamp = 1_000;
                log.append(&mut batch).unwrap();
            }

            log.tick(
                SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60),
                high_watermark,
            )
            .unwrap();

            assert2::check!(log.log_start_offset() == expected_start, "{what}");
        }
    }

    #[test]
    fn tick_removes_only_retained_away_segment_stamp_indexes() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig {
            retention: Some(millis(1)),
            ..tiny_segments()
        })
        .unwrap();
        install_stamps(&mut log, 10, 1);
        for timestamp in [0, 0, 1_000] {
            let mut batch = sample_batch(1);
            batch.max_timestamp = timestamp;
            log.append(&mut batch).unwrap();
        }

        log.tick(
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1),
            log.log_end_offset(),
        )
        .unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
        check!(!log.sealed_txn_indexes.contains_key(&Offset(0)));
        check!(!log.sealed_txn_indexes.contains_key(&Offset(1)));
    }

    /// Kafka's `deleteRetentionSizeBreachedSegments` deletes an oldest segment
    /// only while the size over budget still covers the whole segment
    /// (`diff - segment.size() >= 0`), and the active segment counts toward
    /// the log size. Each log holds four sealed segments and an active one,
    /// all of one size `s`; the budget is the total minus the debt below.
    #[test]
    fn tick_size_retention_deletes_only_segments_the_debt_covers() {
        /// A case name, the size debt in half-segments, and the sealed
        /// segments left.
        struct DebtCase {
            name: &'static str,
            debt_halves: Option<u64>,
            expected_sealed: usize,
        }
        let cases = [
            DebtCase {
                name: "under budget",
                debt_halves: None,
                expected_sealed: 4,
            },
            DebtCase {
                name: "exactly at budget",
                debt_halves: Some(0),
                expected_sealed: 4,
            },
            DebtCase {
                name: "half a segment over",
                debt_halves: Some(1),
                expected_sealed: 4,
            },
            DebtCase {
                name: "one segment over",
                debt_halves: Some(2),
                expected_sealed: 3,
            },
            DebtCase {
                name: "one and a half segments over",
                debt_halves: Some(3),
                expected_sealed: 3,
            },
            DebtCase {
                name: "two segments over",
                debt_halves: Some(4),
                expected_sealed: 2,
            },
            DebtCase {
                name: "the whole log over, the active segment behind a roll",
                debt_halves: Some(10),
                expected_sealed: 0,
            },
        ];
        for DebtCase {
            name,
            debt_halves,
            expected_sealed,
        } in cases
        {
            let dir = tempdir().unwrap();
            let mut log = rolling_test_log(dir.path());
            append_samples(&mut log, 5, 1);
            let segment = log.segments[0].size().bytes_u64();
            check!(
                log.segments
                    .iter()
                    .chain(log.active.as_ref())
                    .all(|s| s.size().bytes_u64() == segment),
                "{name}: the fixture needs equal segments"
            );
            let total = log.size().bytes_u64();
            let budget = debt_halves.map_or(total + 1, |halves| {
                total.saturating_sub(halves * segment / 2)
            });
            let mut config = log.config_snapshot();
            config.retention_size = Some(ByteSize::from_bytes(budget));
            log.set_config(config);

            log.tick(SystemTime::UNIX_EPOCH, log.log_end_offset())
                .unwrap();

            check!(log.segments.len() == expected_sealed, "{name}");
        }
    }

    #[test]
    fn tick_skips_retention_when_remote_storage_enable_is_true() {
        use std::time::Duration;
        let far_future = SystemTime::now() + Duration::from_hours(365 * 24);

        // Tiered topic: tick must not delete any segment, however old.
        // The remote-log manager owns local eviction.
        let dir_tiered = tempdir().unwrap();
        let mut tiered = rolled_log(dir_tiered.path(), &LogConfig {
            remote_storage_enable: true,
            retention: Some(millis(1)),
            ..LogConfig::default()
        });
        let sealed_before = tiered.tierable_segments().len();
        assert2::assert!(sealed_before > 0);
        tiered.tick(far_future, Offset(i64::MAX)).unwrap();
        assert2::assert!(tiered.tierable_segments().len() == sealed_before);

        // Non-tiered baseline: tick should still evict aggressively.
        let dir_plain = tempdir().unwrap();
        let mut plain = rolled_log(dir_plain.path(), &LogConfig {
            remote_storage_enable: false,
            retention: Some(millis(1)),
            ..LogConfig::default()
        });
        assert2::assert!(!plain.tierable_segments().is_empty());
        plain.tick(far_future, Offset(i64::MAX)).unwrap();
        // Non-tiered path: every segment is past retention, so every sealed
        // segment is evicted.
        assert2::assert!(plain.tierable_segments().len() == 0);
    }
}
