//! `KraftLog`: the replicated metadata log behind the `LogView` seam.
//! It is a thin facade over `krabka_log::Log` that adds high-watermark
//! tracking, committed-read filtering for KIP-595 `Fetch`, and divergence
//! lookup. The controller uses it as the metadata log.

use std::path::{Path, PathBuf};

use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::{Log, LogConfig, RawRead};
use krabka_protocol::{Decode as _, records::RecordBatch};
use krabka_units::prelude::ByteSize;

use crate::{
    config::MetadataLogConfig,
    error::{PersistedFormatError, RaftError},
    kraft::types::{Epoch, LogOffsetMetadata, LogView},
};

pub struct KraftLog {
    log: Log,
    /// Highest committed offset. This is consensus state, and krabka-log does
    /// not track it.
    hwm: Offset,
    hwm_path: PathBuf,
    /// The first I/O error a write to the metadata log directory returned.
    /// It stays set: Kafka treats a failed metadata log directory as fatal
    /// (KIP-858), and the engine stops the controller over it.
    failure: Option<String>,
}

/// Read budget [`KraftLog::timestamp_below`] starts from. It only ever needs
/// one batch, and `read_decoded` grows the window until one fits, so this is
/// sized to make that the common case rather than to bound the result.
const TIMESTAMP_READ_WINDOW: ByteSize = krabka_units::prelude::kibibytes(64);

/// The file the committed offset survives a restart in. Kafka keeps no such
/// file, so the name is krabka's own. It sits beside the KIP-630 snapshots in
/// the metadata partition directory, so it must not end in `.checkpoint`: a
/// snapshot scan, krabka's or Kafka's, and a `*.checkpoint` glob must not take
/// it for a snapshot.
const HIGH_WATERMARK_FILE: &str = "high-watermark";

/// The file a new high watermark is written to before it is renamed over
/// [`HIGH_WATERMARK_FILE`], so a reader never sees a torn write.
const HIGH_WATERMARK_TMP_FILE: &str = "high-watermark.tmp";

/// The version of the [`HIGH_WATERMARK_FILE`] layout: a text file whose first
/// line is this number and whose second line is the committed offset in
/// decimal, each ended by a newline, as Kafka's checkpoint files put their
/// version first. It is part of the 1.x on-disk contract: a later 1.x build
/// reads this layout, and a layout change takes a new number.
pub(crate) const HIGH_WATERMARK_FILE_VERSION: i16 = 0;

/// The text of the [`HIGH_WATERMARK_FILE`] that records `hwm`.
fn encode_high_watermark(hwm: Offset) -> String {
    format!("{HIGH_WATERMARK_FILE_VERSION}\n{}\n", hwm.0)
}

/// Reads the text of a [`HIGH_WATERMARK_FILE`].
///
/// A single decimal line with no version line is the layout a build before 1.0
/// wrote, and is [`PersistedFormatError::MissingVersion`]. A version line
/// other than [`HIGH_WATERMARK_FILE_VERSION`] is
/// [`PersistedFormatError::UnsupportedVersion`]. Anything else that is not the
/// current layout is [`PersistedFormatError::Malformed`].
fn decode_high_watermark(text: &str) -> Result<Offset, PersistedFormatError> {
    let malformed = || PersistedFormatError::Malformed(format!("{text:?}"));
    let Some((version, rest)) = text.split_once('\n') else {
        return Err(if text.trim().parse::<i64>().is_ok() {
            PersistedFormatError::MissingVersion
        } else {
            malformed()
        });
    };
    let version = version.parse::<i64>().map_err(|_| malformed())?;
    if version != i64::from(HIGH_WATERMARK_FILE_VERSION) {
        return Err(PersistedFormatError::UnsupportedVersion {
            found: version,
            min: i64::from(HIGH_WATERMARK_FILE_VERSION),
            max: i64::from(HIGH_WATERMARK_FILE_VERSION),
        });
    }
    rest.strip_suffix('\n')
        .and_then(|offset| offset.parse::<i64>().ok())
        .filter(|offset| *offset >= 0)
        .map(Offset)
        .ok_or_else(malformed)
}

/// The high watermark [`KraftLog::open`] starts from: the one `hwm_path`
/// records, or `log_start` when there is none.
///
/// Kafka keeps no high-watermark file. A restarted Kafka replica knows no
/// committed offset until the leader's `Fetch` responses tell it, or, as the
/// leader, until it commits a record of its own epoch. Krabka's file only
/// saves the replay of the committed prefix on restart, and a high watermark
/// below the true one is safe: the controller waits for the quorum to raise it
/// again. So the file is a cache of state the quorum recomputes. A file that
/// cannot be read, or that does not parse, is dropped with a warning and the
/// log start is used, as Kafka starts with no high watermark at all.
///
/// A version marker this build does not read is not damage to a cache: it is
/// a later build's file, or a 0.x data directory that the 1.x contract does
/// not cover. Both are a hard error, so a node never runs on a format it
/// cannot read.
fn recover_high_watermark(hwm_path: &Path, log_start: Offset) -> Result<Offset, RaftError> {
    let bytes = match std::fs::read(hwm_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(log_start),
        Err(error) => {
            tracing::warn!(?error, path = %hwm_path.display(), "kraft: high watermark unreadable; starting from the log start");
            return Ok(log_start);
        }
    };
    let decoded = std::str::from_utf8(&bytes)
        .map_err(|error| PersistedFormatError::Malformed(error.to_string()))
        .and_then(decode_high_watermark);
    match decoded {
        Ok(hwm) => Ok(hwm),
        Err(PersistedFormatError::Malformed(reason)) => {
            tracing::warn!(%reason, path = %hwm_path.display(), "kraft: high watermark does not parse; starting from the log start");
            Ok(log_start)
        }
        Err(problem) => Err(RaftError::PersistedFormat {
            artifact: HIGH_WATERMARK_FILE,
            path: hwm_path.to_path_buf(),
            problem,
        }),
    }
}

/// The `krabka_log` configuration of the metadata log, as Kafka's
/// `KafkaRaftLog.createLog` builds it: the segments roll at
/// `metadata.log.segment.bytes` and `metadata.log.segment.ms`, and time and
/// size retention are off. Only a snapshot moves the log start, so only the
/// metadata log's own cleaning deletes a segment.
fn metadata_log_config(config: &MetadataLogConfig) -> LogConfig {
    LogConfig {
        segment_size: config.segment_size,
        segment_roll_interval: config.segment_roll_interval,
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    }
}

impl KraftLog {
    /// Opens or creates the metadata log in `dir`, the metadata partition
    /// directory `__cluster_metadata-0`. The segments roll as `config` says.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log directory cannot be created or the
    /// underlying `krabka_log::Log` fails to open, and
    /// [`RaftError::PersistedFormat`] if the high-watermark file has a
    /// version this build does not read or has no version marker.
    pub fn open(dir: impl AsRef<Path>, config: &MetadataLogConfig) -> Result<Self, RaftError> {
        let log_dir = dir.as_ref();
        let hwm_path = log_dir.join(HIGH_WATERMARK_FILE);
        std::fs::create_dir_all(log_dir).map_err(krabka_log::LogError::Io)?;
        // `krabka_log::Log` checkpoints its own log start, so a prune that
        // advanced inside the active segment is already restored here.
        let log = Log::open(log_dir, metadata_log_config(config))?;
        let hwm = recover_high_watermark(&hwm_path, log.log_start_offset())?
            .max(log.log_start_offset())
            .min(log.log_end_offset());
        Ok(Self {
            log,
            hwm,
            hwm_path,
            failure: None,
        })
    }

    /// The first I/O error a write to the metadata log directory returned, or
    /// `None` while every write has succeeded.
    #[must_use]
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// Records the error of a failed write when it is an I/O error, and
    /// passes `result` on.
    fn checked<T>(&mut self, result: Result<T, krabka_log::LogError>) -> Result<T, RaftError> {
        if let Err(krabka_log::LogError::Io(error)) = &result {
            self.note_failure(error);
        }
        Ok(result?)
    }

    fn note_failure(&mut self, error: &std::io::Error) {
        if self.failure.is_none() {
            self.failure = Some(error.to_string());
        }
    }

    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        self.log.log_start_offset()
    }
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        self.log.log_end_offset()
    }
    #[must_use]
    pub fn hwm(&self) -> Offset {
        self.hwm
    }

    /// The size of every segment's `.log` file together, Kafka's
    /// `UnifiedLog.size`, which the metadata log's size retention weighs.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        self.log.size()
    }

    /// Leader path: appends a batch stamped with `append_timestamp_ms`.
    /// krabka-log assigns the offset and records the batch's
    /// `partition_leader_epoch`. Returns the assigned base offset.
    ///
    /// The stamp is the batch's create-time, as Kafka's `BatchAccumulator`
    /// stamps every batch the raft client appends with the current time. It is
    /// what [`Self::last_committed_timestamp_ms`] reads back for a snapshot
    /// header, and what a follower replicates verbatim through
    /// [`Self::append_at`].
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails.
    pub fn append(
        &mut self,
        batch: &mut RecordBatch,
        append_timestamp_ms: i64,
    ) -> Result<Offset, RaftError> {
        // Every record in an engine-built batch carries `timestamp_delta` 0,
        // so one stamp is the create-time of all of them.
        batch.base_timestamp = append_timestamp_ms;
        batch.max_timestamp = append_timestamp_ms;
        // The metadata log is always `CreateTime`, so the log stamps nothing
        // and the second element of the append result is always `None`.
        let appended = self.log.append(batch);
        let (base_offset, _log_append_time_ms) = self.checked(appended)?;
        Ok(base_offset)
    }

    /// Follower path: appends a batch at the leader-assigned `offset`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails, for example when
    /// `offset` does not equal the current log end offset.
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: Offset) -> Result<(), RaftError> {
        let appended = self.log.append_at(batch, offset);
        self.checked(appended)?;
        Ok(())
    }

    /// Decoded read from `offset`. The tests and the replication apply path
    /// use it.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying read fails.
    pub fn read_decoded(
        &self,
        offset: Offset,
        max_size: ByteSize,
    ) -> Result<Vec<RecordBatch>, RaftError> {
        let log_end = self.log.log_end_offset();
        if offset >= log_end {
            return Ok(Vec::new());
        }
        let mut window = max_size;
        loop {
            let raw = self.log.read_raw(offset, log_end, window)?;
            let mut bytes = raw.bytes.as_ref();
            let mut batches = Vec::new();
            while !bytes.is_empty() {
                batches.push(
                    RecordBatch::decode(&mut bytes)
                        .map_err(|error| RaftError::ChangeRejected(error.to_string()))?,
                );
            }
            if !batches.is_empty() {
                return Ok(batches);
            }
            // A sparse index can floor `offset` to an earlier batch. If the
            // configured window ends before the first requested batch, grow
            // only until one complete requested batch fits so reads always
            // make progress without unbounding the returned batch run.
            window *= 2.0;
        }
    }

    /// The append timestamp of the last record below `end_offset`: the
    /// `max_timestamp` of the batch that contains `end_offset - 1`.
    ///
    /// KIP-630 stamps this into `SnapshotHeaderRecord`'s
    /// `last_contained_log_timestamp`, where Kafka supplies the append time of
    /// the last batch the snapshot contains. `None` when no such record is
    /// readable here: the boundary is at or below the log start, because
    /// everything under it was pruned or arrived inside an installed snapshot,
    /// or it is beyond the log end.
    #[must_use]
    pub fn timestamp_below(&self, end_offset: Offset) -> Option<i64> {
        let last = Offset(end_offset.0.checked_sub(1)?);
        if last < self.log.log_start_offset() || last >= self.log.log_end_offset() {
            return None;
        }
        let batches = self.read_decoded(last, TIMESTAMP_READ_WINDOW).ok()?;
        // A sparse index floors the read to a batch boundary at or before
        // `last`, and the window can carry batches past it, so the containing
        // batch is the last one that still starts at or below `last`.
        batches
            .iter()
            .take_while(|batch| batch.base_offset <= last.0)
            .last()
            .map(|batch| batch.max_timestamp)
    }

    /// The append timestamp of the last committed record (`hwm - 1`), for the
    /// header of a snapshot taken at the high watermark.
    #[must_use]
    pub fn last_committed_timestamp_ms(&self) -> Option<i64> {
        self.timestamp_below(self.hwm)
    }

    /// Serves KIP-595 `Fetch`: verbatim batch bytes in
    /// `[offset, min(hwm, log_end))`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying raw read fails.
    pub fn read_committed(&self, offset: Offset, max_size: ByteSize) -> Result<RawRead, RaftError> {
        let limit = self.hwm.min(self.log.log_end_offset());
        Ok(self.log.read_raw(offset, limit, max_size)?)
    }

    /// Advances the high watermark. The move is monotonic, and it never goes
    /// past the log end.
    pub fn advance_hwm(&mut self, new_hwm: Offset) {
        let log_end = self.log.log_end_offset();
        let next = Offset(krabka_verified::raft::advance_high_watermark(
            self.hwm.0, new_hwm.0, log_end.0,
        ));
        if next > self.hwm {
            self.hwm = next;
            self.persist_hwm();
        }
        assert2::assert!(self.hwm <= log_end);
    }

    /// Truncates the log so that no record at offset `>= offset` remains, and
    /// clamps the HWM down.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying truncation fails.
    pub fn truncate_to(&mut self, offset: Offset) -> Result<(), RaftError> {
        let truncated = self.log.truncate_to(offset);
        self.checked(truncated)?;
        // A cut inside a batch can leave the physical end below the request.
        self.hwm = Offset(krabka_verified::truncation_frontier(
            self.hwm.0,
            self.log.log_end_offset().0,
        ));
        self.persist_hwm();
        Ok(())
    }

    /// Prunes the committed prefix below `end_offset`: it advances the
    /// log-start pointer and trims the now-dead segments. This is a no-op when
    /// `end_offset` is at or below the current log start. The leader calls it
    /// after it writes a snapshot.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying log operations fail.
    pub fn prune_to(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        if end_offset <= self.log.log_start_offset() {
            return Ok(());
        }
        // `trim_to_offset` deletes every sealed segment wholly below the new
        // start and then checkpoints the start itself. Setting the start
        // first would make the trim see nothing left to do, and the segments
        // would stay on disk.
        let trimmed = self.log.trim_to_offset(end_offset);
        self.checked(trimmed)?;
        Ok(())
    }

    /// Replaces the log with an empty log that starts at `end_offset`, which
    /// drops every segment, and sets the high watermark to `end_offset`. A
    /// follower calls it when it installs a fetched snapshot whose `end_offset`
    /// is ahead of its own log.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying reset fails.
    pub fn install_snapshot(&mut self, end_offset: Offset) -> Result<(), RaftError> {
        let reset = self.log.reset_to(end_offset);
        self.checked(reset)?;
        self.hwm = end_offset;
        self.persist_hwm();
        Ok(())
    }

    /// Writes the high watermark to a temporary file and renames it over the
    /// high-watermark file, so a crash leaves the old value or the new one.
    /// Neither write is synced: the file is a cache (see
    /// [`recover_high_watermark`]), and an advance that a crash loses only
    /// lowers the value a restart begins from.
    fn persist_hwm(&mut self) {
        let tmp = self.hwm_path.with_file_name(HIGH_WATERMARK_TMP_FILE);
        let written = std::fs::write(&tmp, encode_high_watermark(self.hwm))
            .and_then(|()| std::fs::rename(&tmp, &self.hwm_path));
        if let Err(error) = written {
            tracing::error!(?error, path = %self.hwm_path.display(), "kraft: persist high watermark failed");
            self.note_failure(&error);
        }
    }
}

/// Kafka's `KafkaRaftLog.endOffsetForEpoch` over a leader epoch checkpoint.
///
/// The checkpoint answers the floor epoch and its end, as
/// `LeaderEpochFileCache.endOffsetFor` does. An epoch newer than every entry
/// has no floor there, and Kafka then answers the log end with the latest
/// epoch, which never equals the requested one. The WAL replica log shares
/// this lookup.
#[must_use]
pub fn end_offset_for_epoch_in(
    checkpoint: &krabka_log::LeaderEpochCheckpoint,
    log_end: Offset,
    epoch: Epoch,
) -> LogOffsetMetadata {
    let latest = u32::try_from(checkpoint.latest_epoch().unwrap_or(LeaderEpoch(0)).0).unwrap_or(0);
    let past_every_epoch = LogOffsetMetadata {
        offset: log_end.0,
        epoch: latest,
    };
    let Ok(requested) = i32::try_from(epoch) else {
        return past_every_epoch;
    };
    let (found, end) = checkpoint.epoch_and_offset_for(LeaderEpoch(requested), log_end);
    match u32::try_from(found.0) {
        Ok(found) => LogOffsetMetadata {
            offset: end.0,
            epoch: found,
        },
        Err(_) => past_every_epoch,
    }
}

impl LogView for KraftLog {
    // `LogView` is defined by the pure `krabka-kraft-core` consensus engine and
    // speaks raw `i64` offsets; unwrap the `krabka-log` `Offset`s with `.0` at
    // this boundary so the core sees the integers it expects.
    fn end_offset(&self) -> i64 {
        self.log.log_end_offset().0
    }
    fn last_epoch(&self) -> Epoch {
        // The log seam speaks `krabka_ids::LeaderEpoch(i32)`; the core's
        // consensus `Epoch` is a `u32`. krabka-log epochs are non-negative
        // (0 for an empty log), so unwrap the newtype and convert to `u32`.
        let latest: LeaderEpoch = self
            .log
            .epoch_checkpoint()
            .latest_epoch()
            .unwrap_or(LeaderEpoch(0));
        u32::try_from(latest.0).unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        end_offset_for_epoch_in(
            self.log.epoch_checkpoint(),
            self.log.log_end_offset(),
            epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::prelude::mebibytes;

    use super::*;

    /// Read budget the log tests use. It is larger than any batch they append,
    /// so a read returns everything written.
    const TEST_READ_BUDGET: ByteSize = mebibytes(1);

    #[derive(Clone, Copy)]
    struct KraftSeedCount(usize);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct KraftLogSeedSetup {
        #[default(KraftSeedCount(5))]
        records: KraftSeedCount,
    }

    fn append_seeded_records(log: &mut KraftLog, setup: KraftLogSeedSetup) {
        for _ in 0..setup.records.0 {
            log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        }
    }

    fn seeded_log(setup: KraftLogSeedSetup) -> (KraftLog, tempfile::TempDir) {
        let (mut log, dir) = open_tmp();
        append_seeded_records(&mut log, setup);
        (log, dir)
    }

    fn open_tmp() -> (KraftLog, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path(), &MetadataLogConfig::default()).expect("open");
        (log, dir)
    }

    krabka_macros::epoch_record_batch_fixture!(batch);

    #[test]
    fn opens_empty_at_offset_zero() {
        let (log, _dir) = open_tmp();
        check!(
            (
                log.log_start_offset().0,
                log.log_end_offset().0,
                log.hwm().0,
            ) == (0, 0, 0)
        );
    }

    #[test]
    fn append_assigns_sequential_offsets_and_reads_back() {
        let (mut log, _dir) = open_tmp();
        let off0 = log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        let off1 = log.append(&mut batch(0, 1, b"b"), 0).unwrap();
        assert2::assert!((off0, off1, log.log_end_offset()) == (Offset(0), Offset(1), Offset(2)));
        // read back decoded
        let out = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();
        assert2::assert!(
            out.iter()
                .map(|batch| batch.partition_leader_epoch)
                .collect::<Vec<_>>()
                == vec![1, 1]
        );
    }

    #[test]
    fn append_stamps_the_batch_create_time_the_snapshot_header_reads_back() {
        let (mut log, _dir) = open_tmp();
        // Two batches with distinct create-times, committed one at a time: the
        // KIP-630 header timestamp names the last batch a snapshot *contains*,
        // so it follows the high watermark rather than the log end.
        let older = 1_700_000_000_000;
        let newer = 1_700_000_111_222;
        log.append(&mut batch(0, 1, b"a"), older).unwrap();
        log.append(&mut batch(0, 1, b"b"), newer).unwrap();

        log.advance_hwm(Offset(1));
        let at_first = log.last_committed_timestamp_ms();
        log.advance_hwm(Offset(2));

        assert2::assert!(
            (
                at_first,
                log.last_committed_timestamp_ms(),
                log.timestamp_below(Offset(1)),
                log.read_decoded(Offset(0), TEST_READ_BUDGET)
                    .unwrap()
                    .iter()
                    .map(|batch| (batch.base_timestamp, batch.max_timestamp))
                    .collect::<Vec<_>>(),
            ) == (
                Some(older),
                Some(newer),
                Some(older),
                vec![(older, older), (newer, newer)],
            )
        );
    }

    #[test]
    fn no_contained_record_has_no_timestamp() {
        let (mut log, _dir) = open_tmp();
        // Nothing committed yet: an empty log, and a log whose records are all
        // still uncommitted, both have no last contained record to name.
        let empty = log.last_committed_timestamp_ms();
        log.append(&mut batch(0, 1, b"a"), 1_700_000_000_000)
            .unwrap();
        let uncommitted = log.last_committed_timestamp_ms();

        // Committed, then pruned away: the boundary is now the log start, and
        // the batch that carried the stamp is gone with the prefix.
        log.advance_hwm(Offset(1));
        log.prune_to(Offset(1)).unwrap();

        assert2::assert!(
            (
                empty,
                uncommitted,
                log.last_committed_timestamp_ms(),
                log.timestamp_below(Offset(0)),
                log.timestamp_below(Offset(9)),
            ) == (None, None, None, None, None)
        );
    }

    #[test]
    fn append_return_matches_assigned_base_and_advances_public_end_offset() {
        let (mut log, _dir) = open_tmp();

        let first = log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        let second = log.append(&mut batch(0, 1, b"b"), 0).unwrap();
        let decoded = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();

        check!(
            (
                first.0,
                second.0,
                decoded
                    .iter()
                    .map(|batch| batch.base_offset)
                    .collect::<Vec<_>>(),
                log.log_end_offset().0,
                LogView::end_offset(&log),
            ) == (0, 1, vec![first.0, second.0], 2, 2)
        );
    }

    #[test]
    fn public_hwm_accessor_tracks_committed_offset_after_advance_and_snapshot() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup {
            records: KraftSeedCount(3),
        });
        log.advance_hwm(Offset(2));
        assert2::assert!(log.hwm() == 2);

        log.install_snapshot(Offset(9)).unwrap();
        check!(
            (
                log.hwm().0,
                log.log_start_offset().0,
                log.log_end_offset().0
            ) == (9, 9, 9)
        );
    }

    #[test]
    fn append_at_preserves_leader_offset() {
        let (mut log, _dir) = open_tmp();
        // follower applies a leader-assigned batch at offset 0
        log.append_at(&mut batch(0, 2, b"x"), Offset(0)).unwrap();
        assert2::assert!(log.log_end_offset().0 == 1);
        assert2::assert!(
            log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap()[0].partition_leader_epoch == 2
        );
    }

    #[test]
    fn logview_reports_end_offset_and_last_epoch() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"a"), 0).unwrap();
        log.append(&mut batch(0, 3, b"b"), 0).unwrap(); // epoch jumps to 3
        assert2::assert!(LogView::end_offset(&log) == 2);
        assert2::assert!(LogView::last_epoch(&log) == 3);
    }

    #[test]
    fn logview_end_offset_for_epoch_follows_kafka() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 2, b"a"), 0).unwrap(); // epoch 2 @ [0,1)
        log.append(&mut batch(0, 4, b"b"), 0).unwrap(); // epoch 4 @ [1,2)
        let at = |offset, epoch| LogOffsetMetadata { offset, epoch };
        for (case, epoch, want) in [
            ("an epoch older than the log", 1, at(0, 1)),
            ("a completed prior epoch", 2, at(1, 2)),
            ("an epoch between two the log holds", 3, at(1, 2)),
            ("the current epoch", 4, at(2, 4)),
            ("an epoch newer than the log", 9, at(2, 4)),
        ] {
            check!(LogView::end_offset_for_epoch(&log, epoch) == want, "{case}");
        }
    }

    #[test]
    fn empty_log_last_epoch_is_zero() {
        let (log, _dir) = open_tmp();
        assert2::assert!(LogView::last_epoch(&log) == 0);
    }

    #[test]
    fn read_committed_never_returns_bytes_past_hwm() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup::default()); // offsets 0..5
        log.advance_hwm(Offset(3));
        let r = log.read_committed(Offset(0), TEST_READ_BUDGET).unwrap();
        // bytes contain only batches with base_offset < 3 (offsets 0,1,2)
        let decoded = log.read_decoded(Offset(0), TEST_READ_BUDGET).unwrap();
        let committed: Vec<_> = decoded.into_iter().filter(|b| b.base_offset < 3).collect();
        check!(committed.len() == 3);
        // total committed bytes equals the size of the first 3 batches
        check!((r.start_offset.0, r.bytes.is_empty()) == (0, false));
    }

    #[test]
    fn advance_hwm_is_monotonic_and_clamped_to_log_end() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"x"), 0).unwrap(); // log_end = 1
        log.advance_hwm(Offset(5)); // clamp to log_end
        assert2::assert!(log.hwm() == 1);
        log.advance_hwm(Offset(0)); // never regress
        assert2::assert!(log.hwm() == 1);
    }

    #[test]
    fn prune_to_advances_log_start_and_is_noop_when_behind() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup::default());
        log.advance_hwm(log.log_end_offset());
        assert2::assert!(log.log_start_offset() == 0);
        log.prune_to(Offset(3)).unwrap();
        assert2::assert!(log.log_start_offset() == 3);
        log.prune_to(Offset(2)).unwrap(); // <= current start: no-op
        assert2::assert!(log.log_start_offset() == 3);
    }

    #[test]
    fn timestamp_below_returns_none_for_out_of_bounds() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup::default());
        log.advance_hwm(log.log_end_offset());
        log.prune_to(Offset(3)).unwrap();

        assert2::assert!(log.timestamp_below(Offset(3)) == None);
        assert2::assert!(log.timestamp_below(Offset(2)) == None);
        assert2::assert!(log.timestamp_below(Offset(10)) == None);
        assert2::assert!(log.timestamp_below(Offset(4)).is_some());
        assert2::assert!(log.timestamp_below(Offset(5)).is_some());
    }

    #[test]
    fn prune_inside_the_active_segment_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let mut log = KraftLog::open(dir.path(), &MetadataLogConfig::default()).expect("open");
            append_seeded_records(&mut log, KraftLogSeedSetup::default());
            log.advance_hwm(log.log_end_offset());
            // Every record is in one segment, so no segment name records the
            // prune: `krabka_log::Log`'s checkpoint is what carries it.
            log.prune_to(Offset(3)).unwrap();
        }

        let log = KraftLog::open(dir.path(), &MetadataLogConfig::default()).expect("reopen");

        check!((log.log_start_offset().0, log.log_end_offset().0) == (3, 5));
    }

    #[test]
    fn install_snapshot_resets_log_to_empty_at_offset() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup {
            records: KraftSeedCount(4),
        });
        log.install_snapshot(Offset(100)).unwrap();
        check!(
            (
                log.log_start_offset().0,
                log.log_end_offset().0,
                log.hwm().0,
            ) == (100, 100, 100)
        );
        let base = log.append(&mut batch(0, 1, b"x"), 0).unwrap();
        assert2::assert!(base == 100);
    }

    #[test]
    fn truncate_to_drops_log_end_and_hwm() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup::default());
        log.advance_hwm(Offset(5));
        log.truncate_to(Offset(2)).unwrap();
        assert2::assert!(log.log_end_offset().0 == 2);
        assert2::assert!(log.hwm().0 == 2);
    }

    #[test]
    fn truncation_inside_a_batch_clamps_hwm_to_the_retained_end() {
        for hwm in [0, 1, 2, 3] {
            let (mut log, dir) = open_tmp();
            log.append(&mut batch(0, 1, b"retained"), 0).unwrap();
            let mut tail = batch(0, 1, b"discarded");
            tail.last_offset_delta = 1;
            let mut second = tail.records[0].clone();
            second.offset_delta = 1;
            tail.records.push(second);
            log.append(&mut tail, 0).unwrap();
            log.advance_hwm(Offset(hwm));
            log.truncate_to(Offset(2)).unwrap();
            assert2::assert!(log.log_end_offset() == Offset(1));
            assert2::assert!(log.hwm() == Offset(hwm.min(1)));
            log.advance_hwm(Offset(2));
            assert2::assert!(log.hwm() == Offset(1));
            drop(log);
            let reopened = KraftLog::open(dir.path(), &MetadataLogConfig::default()).unwrap();
            assert2::assert!(reopened.log_end_offset() == Offset(1));
            assert2::assert!(reopened.hwm() == Offset(1));
        }
    }

    /// Kafka's `KafkaRaftLog.truncateTo` hands the cut to
    /// `UnifiedLog.truncateTo`, which never refuses a target below the log
    /// start: it truncates and moves the start down onto the target.
    #[test]
    fn truncate_below_log_start_lowers_the_log_start() {
        let (mut log, _dir) = seeded_log(KraftLogSeedSetup {
            records: KraftSeedCount(4),
        });
        log.prune_to(Offset(2)).unwrap();

        log.truncate_to(Offset(1)).unwrap();
        check!(
            (
                log.log_start_offset().0,
                log.log_end_offset().0,
                log.hwm().0
            ) == (1, 1, 0)
        );
    }

    /// The `.log` files in `dir`, by name, oldest first.
    fn segment_files(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("list the partition directory")
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect();
        names.sort();
        names
    }

    /// The metadata log rolls as `metadata.log.segment.bytes` and
    /// `metadata.log.segment.ms` say, on the append that would cross either
    /// limit, as Kafka's `UnifiedLog.maybeRoll` does.
    #[test]
    fn segments_roll_by_the_metadata_log_limits() {
        // (what, segment size, roll interval, append timestamps, segments)
        let cases = [
            (
                "inside both limits",
                mebibytes(1),
                krabka_units::prelude::secs(10),
                vec![0, 5_000, 10_000],
                vec!["00000000000000000000.log"],
            ),
            (
                "the size limit",
                krabka_units::prelude::bytes(1),
                krabka_units::prelude::secs(10),
                vec![0, 1, 2],
                vec![
                    "00000000000000000000.log",
                    "00000000000000000001.log",
                    "00000000000000000002.log",
                ],
            ),
            (
                "the roll interval",
                mebibytes(1),
                krabka_units::prelude::secs(10),
                vec![0, 10_000, 10_001],
                vec!["00000000000000000000.log", "00000000000000000002.log"],
            ),
        ];
        for (what, segment_size, segment_roll_interval, timestamps, segments) in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let config = MetadataLogConfig {
                segment_size,
                segment_roll_interval,
                ..MetadataLogConfig::default()
            };
            let mut log = KraftLog::open(dir.path(), &config).expect("open");
            for timestamp in timestamps {
                log.append(&mut batch(0, 1, b"x"), timestamp)
                    .expect("append");
            }
            check!(segment_files(dir.path()) == segments, "{what}");
        }
    }

    /// A prune deletes every segment wholly below the new log start, as
    /// Kafka's `deleteBeforeSnapshot` does, and keeps the one the new start
    /// falls in.
    #[test]
    fn a_prune_deletes_the_segments_wholly_below_the_new_log_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = MetadataLogConfig {
            segment_size: krabka_units::prelude::bytes(1),
            ..MetadataLogConfig::default()
        };
        let mut log = KraftLog::open(dir.path(), &config).expect("open");
        for _ in 0..4 {
            log.append(&mut batch(0, 1, b"x"), 0).expect("append");
        }
        log.advance_hwm(Offset(4));

        log.prune_to(Offset(2)).expect("prune");

        check!(
            (log.log_start_offset(), segment_files(dir.path()))
                == (
                    Offset(2),
                    vec![
                        "00000000000000000002.log".to_owned(),
                        "00000000000000000003.log".to_owned(),
                    ]
                )
        );
    }

    /// The exact text of a version-0 high-watermark file that records offset
    /// 3. A change to it is a change to the 1.x on-disk contract.
    const HIGH_WATERMARK_V0_FIXTURE: &[u8] = b"0\n3\n";

    /// A log with four committed one-record batches, its high watermark at 3.
    fn log_with_hwm_three() -> (KraftLog, tempfile::TempDir) {
        let (mut log, dir) = open_tmp();
        for _ in 0..4 {
            log.append(&mut batch(0, 1, b"x"), 0).expect("append");
        }
        log.advance_hwm(Offset(3));
        (log, dir)
    }

    #[test]
    fn the_high_watermark_file_matches_the_version_zero_fixture() {
        let (log, dir) = log_with_hwm_three();
        drop(log);

        let written = std::fs::read(dir.path().join(HIGH_WATERMARK_FILE)).expect("read");
        check!(written == HIGH_WATERMARK_V0_FIXTURE);
        check!(!dir.path().join(HIGH_WATERMARK_TMP_FILE).exists());
        check!(decode_high_watermark("0\n3\n") == Ok(Offset(3)));
    }

    #[test]
    fn decode_high_watermark_refuses_every_other_layout() {
        let unsupported = |found| PersistedFormatError::UnsupportedVersion {
            found,
            min: 0,
            max: 0,
        };
        let malformed = |text: &str| PersistedFormatError::Malformed(format!("{text:?}"));
        let cases = [
            ("0.x layout", "3", PersistedFormatError::MissingVersion),
            (
                "0.x layout, padded",
                " 3 ",
                PersistedFormatError::MissingVersion,
            ),
            ("future version", "1\n3\n", unsupported(1)),
            ("negative version", "-1\n3\n", unsupported(-1)),
            ("empty", "", malformed("")),
            ("version only", "0\n", malformed("0\n")),
            ("no final newline", "0\n3", malformed("0\n3")),
            ("trailing line", "0\n3\n4\n", malformed("0\n3\n4\n")),
            ("negative offset", "0\n-3\n", malformed("0\n-3\n")),
            ("not a number", "0\nx\n", malformed("0\nx\n")),
            ("version not a number", "v\n3\n", malformed("v\n3\n")),
        ];
        for (case, text, want) in cases {
            check!(decode_high_watermark(text) == Err(want), "{case}");
        }
    }

    /// A version marker this build does not read stops the open; a file that
    /// is merely damaged is a cache miss, and the log start is used.
    #[test]
    fn open_refuses_an_unknown_high_watermark_version_and_drops_a_damaged_one() {
        let cases: [(&str, &[u8], Result<Offset, PersistedFormatError>); 5] = [
            ("current version", HIGH_WATERMARK_V0_FIXTURE, Ok(Offset(3))),
            (
                "0.x layout",
                b"3",
                Err(PersistedFormatError::MissingVersion),
            ),
            (
                "future version",
                b"7\n3\n",
                Err(PersistedFormatError::UnsupportedVersion {
                    found: 7,
                    min: 0,
                    max: 0,
                }),
            ),
            ("empty after a crash", b"", Ok(Offset(0))),
            ("not utf-8", b"0\n\xff\n", Ok(Offset(0))),
        ];
        for (case, contents, want) in cases {
            let (log, dir) = log_with_hwm_three();
            drop(log);
            let path = dir.path().join(HIGH_WATERMARK_FILE);
            std::fs::write(&path, contents).expect("write the high-watermark file");

            let opened = KraftLog::open(dir.path(), &MetadataLogConfig::default());

            let got = match opened {
                Ok(log) => Ok(log.hwm()),
                Err(RaftError::PersistedFormat {
                    artifact,
                    path: reported,
                    problem,
                }) => {
                    check!(
                        (artifact, reported) == (HIGH_WATERMARK_FILE, path.clone()),
                        "{case}"
                    );
                    Err(problem)
                }
                Err(other) => panic!("{case}: unexpected error {other}"),
            };
            check!(got == want, "{case}");
        }
    }
}
