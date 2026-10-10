//! Timestamp-to-offset lookups: the `ListOffsets` queries that search by
//! record time rather than by offset.
//!
//! Each search reads sealed segments oldest-first and then the active
//! segment, and ties resolve to the earliest offset as KIP-734 requires.

use std::time::SystemTime;

use krabka_ids::Offset;
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

use super::Log;
use crate::{error::LogError, name, retention, segment::Segment};

impl Log {
    /// Legacy `ListOffsets` v0 segment boundaries at or before `timestamp`,
    /// newest first and capped by `max_num_offsets`.
    ///
    /// Version 0 predates record timestamps. Its timestamp lookup uses each
    /// segment file's modification time and returns segment base offsets (plus
    /// the log end for a non-empty active segment), matching Kafka's
    /// `legacyFetchOffsetsBefore` behavior.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when a segment's modification time cannot be read.
    pub fn legacy_offsets_before(
        &self,
        timestamp: i64,
        max_num_offsets: usize,
    ) -> Result<Vec<Offset>, LogError> {
        let segments: Vec<&Segment> = self.segments.iter().chain(self.active.as_ref()).collect();
        let log_start = self.log_start_offset();
        let mut offset_times = Vec::with_capacity(segments.len() + 1);
        for segment in &segments {
            let modified = std::fs::metadata(name::log_path(&self.dir, segment.base_offset().0))?
                .modified()?;
            offset_times.push((
                segment.base_offset().max(log_start),
                retention::now_ms(modified),
            ));
        }
        if segments
            .last()
            .is_some_and(|segment| segment.size() > krabka_units::ByteSize::ZERO)
        {
            offset_times.push((self.log_end_offset(), retention::now_ms(SystemTime::now())));
        }

        let start = match timestamp {
            -1 => offset_times.len().checked_sub(1),
            -2 => (!offset_times.is_empty()).then_some(0),
            _ => offset_times
                .iter()
                .rposition(|(_, modified)| *modified <= timestamp),
        };
        let Some(start) = start else {
            return Ok(Vec::new());
        };
        Ok(offset_times[..=start]
            .iter()
            .rev()
            .take(max_num_offsets)
            .map(|(offset, _)| *offset)
            .collect())
    }

    /// Earliest local `(offset, record_timestamp)` whose record timestamp is
    /// `>= target_ts`, excluding records below the logical log start.
    ///
    /// The search reads sealed segments oldest-first and then the active
    /// segment. The first segment whose `max_timestamp >= target_ts` holds
    /// the answer. The per-segment helper does the index lookup and the
    /// forward scan. The result is `None` when no local record qualifies,
    /// including the case of an empty log.
    ///
    /// This is the lookup the log's own bookkeeping uses, and it applies no
    /// `max.decompressed.message.bytes`. A `ListOffsets` answer goes through
    /// [`Self::offset_for_timestamp_checked`], which does.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        // With no limit the scan has nothing to refuse.
        self.scan_offset_for_timestamp(target_ts, scan_window, None)
            .ok()
            .flatten()
    }

    /// [`Self::offset_for_timestamp`] under the log's
    /// [`max_decompressed_record`](crate::LogConfig::max_decompressed_record),
    /// as Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp` answers it.
    ///
    /// Kafka's scan decompresses a compressed batch whose max timestamp reaches
    /// the target, up to the record it returns, and fails on a record above the
    /// limit. A lookup that never reads the oversized record is unaffected.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for such a record.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn offset_for_timestamp_checked(
        &self,
        target_ts: i64,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let (scan_window, limit) = self.timestamp_scan_settings();
        self.scan_offset_for_timestamp(target_ts, scan_window, limit)
    }

    /// The scan window and the record limit of the log's current configuration.
    fn timestamp_scan_settings(&self) -> (ByteSize, Option<ByteSize>) {
        let config = self.config.read().unwrap();
        (config.timestamp_scan_window, config.max_decompressed_record)
    }

    fn scan_offset_for_timestamp(
        &self,
        target_ts: i64,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let minimum = self.log_start_offset();
        for seg in &self.segments {
            // A sealed segment restores its maximum on open, so the unknown
            // sentinel survives only where the segment holds no readable
            // batch. There is nothing in such a segment to find.
            if seg.last_offset() >= minimum
                && seg.max_timestamp() >= target_ts
                && let Some(hit) = seg.offset_for_timestamp_with_window_from(
                    target_ts,
                    scan_window,
                    limit,
                    minimum,
                )?
            {
                return Ok(Some(hit));
            }
        }
        if let Some(active) = &self.active
            && active.max_timestamp() >= target_ts
        {
            return active.offset_for_timestamp_with_window_from(
                target_ts,
                scan_window,
                limit,
                minimum,
            );
        }
        Ok(None)
    }

    /// Offset and timestamp of the record that carries the partition's
    /// largest timestamp (KIP-734 `MAX_TIMESTAMP`), under the log's
    /// [`max_decompressed_record`](crate::LogConfig::max_decompressed_record),
    /// as Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp(MAX_TIMESTAMP)`
    /// answers it: Kafka decompresses only the batch that holds the log's
    /// maximum, up to the record it returns.
    ///
    /// The scan reads sealed segments and then the active segment. Ties
    /// resolve to the earliest offset: the first segment wins, and the first
    /// record within it wins. The result is `None` when the log holds no
    /// records.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::RecordTooLarge`] for a record of that batch above the
    /// limit.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    pub fn max_timestamp_offset_and_ts(&self) -> Result<Option<(Offset, i64)>, LogError> {
        let (scan_window, limit) = self.timestamp_scan_settings();
        self.scan_max_timestamp(scan_window, limit)
    }

    fn scan_max_timestamp(
        &self,
        scan_window: ByteSize,
        limit: Option<ByteSize>,
    ) -> Result<Option<(Offset, i64)>, LogError> {
        let mut best: Option<(i64, Offset, &Segment)> = None; // (timestamp, offset, segment)
        let candidates = self.segments.iter().chain(self.active.as_ref());
        for seg in candidates {
            // Every segment is measured without the limit: Kafka picks the
            // winner from the segments' max timestamps alone, and reads only
            // the winner's batch.
            if let Some((offset, ts)) =
                seg.offset_of_max_timestamp_with_window(scan_window, None)?
                && best.is_none_or(|(best_ts, _, _)| ts > best_ts)
            {
                best = Some((ts, offset, seg));
            }
        }
        if let Some((_, _, winner)) = best
            && limit.is_some()
        {
            winner.offset_of_max_timestamp_with_window(scan_window, limit)?;
        }
        Ok(best.map(|(ts, offset, _)| (offset, ts)))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, kibibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::LogConfig,
        log::test_support::{test_log, tiny_segments, ts_batch},
        segment::Segment,
    };

    /// The largest timestamp in the log wins, and the first segment holding it
    /// keeps the answer when several do.
    ///
    /// KIP-734 asks for the offset of the maximum timestamp, so a tie has to
    /// resolve to one offset -- and taking the later one hands back a record
    /// that is not the first with that timestamp.
    #[test]
    fn the_max_timestamp_offset_comes_from_the_first_segment_holding_it() {
        let dir = tempdir().unwrap();
        let log = crate::log::test_support::rolled_sample_log(dir.path());

        let (offset, ts) = log
            .max_timestamp_offset_and_ts()
            .expect("no limit is set")
            .expect("a log with records has a maximum");
        // sample_batch stamps every record at the same timestamp, so the
        // maximum is shared and the earliest offset carrying it is the answer.
        let earliest = log
            .segments
            .iter()
            .chain(log.active.as_ref())
            .filter_map(Segment::offset_of_max_timestamp)
            .filter(|(_, seg_ts)| *seg_ts == ts)
            .map(|(seg_offset, _)| seg_offset)
            .min()
            .expect("some segment holds the maximum");
        check!(
            offset == earliest,
            "got {offset:?}, earliest is {earliest:?}"
        );
    }

    #[test]
    fn log_offset_for_timestamp_across_segments() {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        let mut log = Log::open(dir.path(), config).unwrap();
        // offsets 0..=4 with timestamps 100,200,300,400,500.
        for (_name, i, ts) in [
            ("first", 0, 100),
            ("second", 1, 200),
            ("third", 2, 300),
            ("fourth", 3, 400),
            ("fifth", 4, 500),
        ] {
            let mut b = ts_batch(ts);
            assert2::assert!(log.append(&mut b).unwrap().0 == Offset(i));
        }
        for (name, ts, want) in [
            // before-first → offset 0.
            ("before first", 50, Some((Offset(0), 100))),
            // exact match on a sealed segment.
            ("exact sealed", 300, Some((Offset(2), 300))),
            // between records → next record up.
            ("between records", 350, Some((Offset(3), 400))),
            // landing on the active segment's record.
            ("active record", 500, Some((Offset(4), 500))),
            // after-last → None.
            ("after last", 600, None),
        ] {
            check!(log.offset_for_timestamp(ts) == want, "case {name}: ts={ts}");
        }
        log.close();
        drop(dir);
    }

    #[test]
    fn reopened_log_scans_sealed_segments_with_unknown_max_timestamp() {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        {
            let mut log = Log::open(dir.path(), config.clone()).unwrap();
            for timestamp in [100, 200, 300] {
                log.append(&mut ts_batch(timestamp)).unwrap();
            }
            log.close();
        }

        let log = Log::open(dir.path(), config).unwrap();
        assert2::assert!(log.offset_for_timestamp(150) == Some((Offset(1), 200)));
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == Some((Offset(2), 300)));
        log.close();
    }

    #[test]
    fn configured_io_policy_reaches_reads_and_timestamp_scans() {
        let (_dir, mut log) = crate::log::test_support::configured_test_log(LogConfig {
            read_buffer_cap: bytes(1),
            timestamp_scan_window: bytes(1),
            ..LogConfig::default()
        });
        let mut batch = ts_batch(100);
        log.append(&mut batch).unwrap();

        assert2::assert!(
            !log.read_raw(Offset(0), Offset(1), kibibytes(1))
                .unwrap()
                .bytes
                .is_empty()
        );
        assert2::assert!(log.offset_for_timestamp(100) == Some((Offset(0), 100)));
    }

    #[test]
    fn log_offset_for_timestamp_empty_log_is_none() {
        let (dir, log) = test_log();
        assert2::assert!(log.offset_for_timestamp(0) == None);
        log.close();
        drop(dir);
    }

    /// An empty log has no maximum: Kafka's `UnifiedLog.fetchOffsetByTimestamp`
    /// answers `MAX_TIMESTAMP` with an empty result rather than the log start.
    #[test]
    fn log_max_timestamp_of_an_empty_log_is_none() {
        let (dir, log) = test_log();
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_max_timestamp_offset_and_ts_returns_pair() {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        let mut log = Log::open(dir.path(), config).unwrap();
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        // Max timestamp 300 lives at offset 1.
        assert2::assert!(log.max_timestamp_offset_and_ts().unwrap() == Some((Offset(1), 300)));
        log.close();
        drop(dir);
    }

    #[test]
    fn legacy_offsets_before_semantics() {
        let (_dir, mut log) = test_log();
        assert2::assert!(log.legacy_offsets_before(-1, 10).unwrap() == vec![Offset(0)]);
        assert2::assert!(log.legacy_offsets_before(-2, 10).unwrap() == vec![Offset(0)]);
        assert2::assert!(log.legacy_offsets_before(0, 10).unwrap().is_empty());

        let mut b = ts_batch(100);
        log.append(&mut b).unwrap();

        // -1 (latest): log end offset then segment base
        let latest = log.legacy_offsets_before(-1, 10).unwrap();
        assert2::assert!(latest == vec![Offset(1), Offset(0)]);

        // -2 (earliest): segment base only
        let earliest = log.legacy_offsets_before(-2, 10).unwrap();
        assert2::assert!(earliest == vec![Offset(0)]);

        // 0 timestamp: earlier than file mtime, empty
        assert2::assert!(log.legacy_offsets_before(0, 10).unwrap().is_empty());

        // Future timestamp: both offsets returned, capped by max_num_offsets
        let all = log.legacy_offsets_before(i64::MAX, 10).unwrap();
        assert2::assert!(all == vec![Offset(1), Offset(0)]);
        let capped = log.legacy_offsets_before(i64::MAX, 1).unwrap();
        assert2::assert!(capped == vec![Offset(1)]);
    }

    /// A gzip batch of one record stamped `ts` with a `value_len`-byte value.
    fn gzip_batch(ts: i64, value_len: usize) -> krabka_protocol::records::RecordBatch {
        let mut batch = ts_batch(ts);
        crate::log::test_support::gzip_value(&mut batch, value_len);
        batch
    }

    /// What a checked lookup answers: the record it found, or `Err` when it
    /// refused one as too large.
    fn answer(
        result: Result<Option<(Offset, i64)>, LogError>,
    ) -> Result<Option<(Offset, i64)>, ()> {
        match result {
            Ok(found) => Ok(found),
            Err(LogError::RecordTooLarge { limit: 100, .. }) => Err(()),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    /// Kafka trunk's `UnifiedLog.fetchOffsetByTimestamp` holds a compressed
    /// batch it decompresses to `max.decompressed.message.bytes`. Three
    /// one-record segments, the outer two oversized: timestamps 100 (oversized),
    /// 300 (small), 200 (oversized).
    #[test]
    fn the_checked_lookups_hold_a_decompressed_batch_to_the_record_limit() {
        let (_dir, mut log) = crate::log::test_support::configured_test_log(LogConfig {
            max_decompressed_record: Some(bytes(100)),
            ..tiny_segments()
        });
        for (ts, value_len) in [(100, 1_000), (300, 10), (200, 1_000)] {
            log.append(&mut gzip_batch(ts, value_len)).unwrap();
        }
        check!(log.segments.len() == 2, "the third batch is the active one");

        for (name, target, expected) in [
            // The first segment holding a timestamp that reaches the target
            // is the one Kafka reads, and its match is oversized.
            ("the match is oversized", 50, Err(())),
            // Segment 0 tops out at 100, so Kafka skips it undecompressed.
            (
                "an older oversized segment is skipped",
                150,
                Ok(Some((Offset(1), 300))),
            ),
            (
                "a match in the small batch",
                300,
                Ok(Some((Offset(1), 300))),
            ),
            ("nothing that late", 301, Ok(None)),
        ] {
            check!(
                answer(log.offset_for_timestamp_checked(target)) == expected,
                "{name}: {target}"
            );
        }
        // The maximum is in the small segment, and Kafka reads only that one,
        // whatever the other segments hold.
        check!(answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(1), 300))));

        // The log's own bookkeeping lookups take no limit.
        check!(log.offset_for_timestamp(50) == Some((Offset(0), 100)));

        // Once the oversized batch holds the maximum, the lookup refuses it.
        log.append(&mut gzip_batch(400, 1_000)).unwrap();
        check!(answer(log.max_timestamp_offset_and_ts()) == Err(()));

        // Raising the limit, as the operator does, answers it again; so does
        // no limit at all.
        for limit in [Some(bytes(1_008)), None] {
            let mut config = log.config_snapshot();
            config.max_decompressed_record = limit;
            log.set_config(config);
            check!(
                answer(log.offset_for_timestamp_checked(50)) == Ok(Some((Offset(0), 100))),
                "{limit:?}"
            );
            check!(
                answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(3), 400))),
                "{limit:?}"
            );
        }
        log.close();
    }

    /// A limit changes nothing for a partition that keeps its records
    /// uncompressed: Kafka reads them in place.
    #[test]
    fn the_checked_lookups_never_hold_an_uncompressed_batch_to_the_limit() {
        let (_dir, mut log) = crate::log::test_support::configured_test_log(LogConfig {
            max_decompressed_record: Some(bytes(100)),
            ..LogConfig::default()
        });
        let mut batch = gzip_batch(100, 1_000);
        batch.attributes = batch
            .attributes
            .with_compression(krabka_compression::CompressionType::None);
        log.append(&mut batch).unwrap();

        check!(answer(log.offset_for_timestamp_checked(100)) == Ok(Some((Offset(0), 100))));
        check!(answer(log.max_timestamp_offset_and_ts()) == Ok(Some((Offset(0), 100))));
        log.close();
    }
}
