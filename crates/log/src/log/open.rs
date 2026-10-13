//! Opening a log directory and rebuilding the producer, transaction, and
//! stamp state that no sidecar file holds.
//!
//! Recovery restores the newest valid producer snapshot and then replays
//! the log tail that snapshot does not cover, so a reopened log reaches
//! exactly the state the append path would have left.

use {
    super::{
        Log,
        control::{
            ControlBatchKind, TransactionMarkerFlags, batch_control_marker_coordinator_epoch,
            control_batch_kind, transaction_marker_flags,
        },
    },
    crate::{
        config::LogConfig,
        error::LogError,
        io::FileIo,
        leader_epoch_checkpoint::LeaderEpochCheckpoint,
        log_start_offset_checkpoint, name,
        producer_snapshot::{self, LoadedSnapshot, NUM_BATCHES_TO_RETAIN, ProducerSnapshotEntry},
        recovery::{deleted_orphan_recover, swap_orphan_recover},
        segment::Segment,
        stamp_index::OffsetRange,
        txn_index::TxnIndex,
    },
    krabka_ids::{Offset, ProducerId},
    krabka_protocol::records::RecordBatch,
    krabka_verified::{ProducerReloadRange, increment_sequence},
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        fs,
        path::Path,
    },
    tracing::instrument,
};

/// The last offset of a replayed batch and the offset the replay cursor
/// advances to after it.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RecoveredBatchOffsets {
    pub(super) last: Offset,
    pub(super) advanced_to: Offset,
}

/// The sequence and offset a producer's entry ends at after one data batch.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ProducerTail {
    pub(super) last_sequence: i32,
    pub(super) last_offset: Offset,
}

/// One data batch a producer appended, as the producer state records it.
#[derive(Debug, Clone, Copy)]
pub(super) struct DataProducerAppend {
    pub(super) producer_id: ProducerId,
    pub(super) producer_epoch: i16,
    pub(super) base_sequence: i32,
    pub(super) last_offset_delta: i32,
    pub(super) base_offset: Offset,
    pub(super) timestamp: i64,
    pub(super) is_transactional: bool,
}

impl Log {
    /// Open or create a `Log` at `dir`.
    ///
    /// This method finds existing segments by `.log` filename and marks all
    /// but the latest as sealed. If the directory is empty, it creates a
    /// fresh active segment at offset 0.
    #[instrument(
        level = "info",
        skip_all,
        fields(
            dir = %dir.as_ref().display(),
            segments = tracing::field::Empty,
            log_end = tracing::field::Empty,
        ),
        err,
    )]
    // cargo-mutants: the only mutant here is the `segments.len() + 1` in the `span.record`
    // call, a tracing-span diagnostic field with no behavioral effect. The
    // sibling `seal_at(next_base - 1)` recovery arithmetic is separately pinned
    // by `reopen_seals_recovered_segments_at_next_base_minus_one`.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Heal any orphaned compaction `.swap` files before
        // we scan the directory for segments.
        swap_orphan_recover(&dir)?;
        // …and reclaim what an interrupted segment deletion left: `.deleted`
        // tombstones, and sidecars whose `.log` the scan below can no longer
        // see. Both are invisible to every later pass otherwise.
        deleted_orphan_recover(&FileIo, &dir)?;

        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let Ok(file_name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 names: ignore (unlikely)
            };
            if let Ok(base) = name::parse_log_filename(&file_name) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort_unstable();
        base_offsets.dedup();
        if !krabka_verified::local_recovery_segment_chain(&base_offsets) {
            return Err(LogError::Corrupt(
                "log segment bases are not a nonnegative ordered chain".into(),
            ));
        }

        let mut segments: Vec<Segment> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            if i + 1 < base_offsets.len() {
                let mut seg = Segment::open_unscanned(&dir, Offset(*base))?;
                // The no-scan loader leaves
                // `last_offset = base - 1`. A sealed segment's true last offset
                // is one below the next segment's base; set it so `read_raw`
                // (which skips a segment whose `last_offset() < fetch_offset`)
                // doesn't skip this recovered segment and serve a later base
                // offset — which after a restart manufactures an offset gap that
                // strands a follower fetching from a low offset.
                let last = krabka_verified::local_recovery_sealed_last(*base, base_offsets[i + 1])
                    .ok_or_else(|| LogError::Corrupt("invalid sealed segment boundary".into()))?;
                seg.seal_at(Offset(last));
                // The no-scan loader also leaves `max_timestamp` unknown. Without
                // this restore, retention would age every reopened segment by
                // its file's modification time -- Kafka's `largestTimestamp()`
                // fallback for a segment with no record timestamp -- instead
                // of by its newest record.
                seg.restore_max_timestamp()?;
                segments.push(seg);
            } else {
                active = Some(Segment::open_active_with_index_interval(
                    &dir,
                    Offset(*base),
                    config.validate_on_open,
                    config.index_interval,
                )?);
            }
        }

        let (active, dir_sync_needed) = match active {
            // We cannot know whether the process that created this segment
            // fsynced the parent directory before crashing. Conservatively
            // require one directory fsync on the next explicit `sync()` so a
            // diskless WAL ack never relies only on file data durability.
            Some(s) => (s, true),
            None => (Segment::create(&dir, Offset(0))?, true),
        };

        let sealed_txn_indexes = segments
            .iter()
            .map(|segment| {
                Ok((
                    segment.base_offset(),
                    TxnIndex::open(segment.txn_index_path())?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LogError>>()?;
        let active_txn_index = TxnIndex::open(active.txn_index_path())?;
        let mut epoch_checkpoint =
            LeaderEpochCheckpoint::open(active.leader_epoch_checkpoint_path())?;
        // LSO starts at log_end_offset(); computed before moving `active`.
        let lso = active.last_offset() + 1;
        epoch_checkpoint.truncate_from_end(lso)?;

        let config = std::sync::Arc::new(std::sync::RwLock::new(config));

        let span = tracing::Span::current();
        span.record("segments", segments.len() + 1);
        span.record("log_end", lso.0);

        // The segment names witness only where the surviving files begin. The
        // checkpoint below carries the rest, and is what turns this into a
        // floor somebody deleted up to.
        let start_offset = segments
            .first()
            .map_or_else(|| active.base_offset(), Segment::base_offset);

        let mut log = Self {
            dir,
            config,
            io: std::sync::Arc::new(FileIo),
            segments,
            active: Some(active),
            dir_sync_needed,
            rollover_flusher: super::rollover_flush::Flusher::default(),
            unflushed_messages: 0,
            last_flush: std::time::Instant::now(),
            roll_jitter: None,
            pending_deletes: Vec::new(),
            start_offset,
            // Derived from the files on disk, not deleted up to by anyone.
            // The checkpoint restore below is what may set it: see
            // `Log::established_log_start`.
            start_offset_established: false,
            // Nothing has cleaned this log yet, so every sealed segment is
            // dirty until the first pass says otherwise.
            clean_prefix_segments: 0,
            lso,
            pending: HashMap::new(),
            verification_states: HashMap::new(),
            unreplicated: BTreeMap::new(),
            pending_stamp_ranges: HashMap::new(),
            coordinator_epochs: HashMap::new(),
            producer_state: HashMap::new(),
            earlier_batches: HashMap::new(),
            active_txn_index,
            sealed_txn_indexes,
            stamp_source: None,
            stamp_indexes: BTreeMap::new(),
            epoch_checkpoint,
            reconciled_frontier: Offset(0),
            delivery_watermark: Offset(0),
            delivery_pending_ms: None,
        };
        // Restore a log start that the segment names cannot express: a trim
        // that landed inside a segment left its records on disk, and only the
        // checkpoint says they are gone.
        if let Some(checkpointed) = log_start_offset_checkpoint::read(&log.dir)? {
            // Cap at the log end and nothing else. Above it, the whole
            // surviving log is below a start that was acknowledged, so every
            // record left is one the trim removed and the log start is the log
            // end -- a crash between a trim and the fsync of the records it
            // trimmed past lands there.
            //
            // There is deliberately no floor at the segment-derived start. On
            // a tiered partition (KIP-405) the global floor belongs *below*
            // the oldest local segment, in the band the archive serves, and
            // raising the checkpoint to meet the surviving files would hide
            // every offset the remote tier still holds -- and then write that
            // loss back to disk. `Log::local_log_start_offset` is the floor
            // that follows the files.
            let effective = checkpointed.min(log.log_end_offset());
            log.start_offset = effective;
            // A checkpoint exists, so somebody deleted up to this floor: it is
            // one a reader may be refused against and remote retention may
            // delete against.
            log.start_offset_established = true;
            if effective != checkpointed {
                // Persist what was resolved instead of leaving the
                // out-of-range value on disk. It is inert against this log,
                // but appends move the log end, and the next open would find
                // the stale value back in range and hide live records.
                log_start_offset_checkpoint::write(&*log.io, &log.dir, effective)?;
            }
            // Kafka's `LogLoader.load` → `truncateFromStart(logStartOffsetCheckpoint)`:
            // a crash can land between a start increment and the epoch-cache
            // rewrite that followed it, so the reload trims the cache again.
            log.epoch_checkpoint.truncate_from_start(effective)?;
        }
        // Kafka's `LogLoader.load` first drops every snapshot no segment
        // accounts for, then reloads against the log start it just restored,
        // so producer state is rebuilt only after the checkpoint is applied.
        let segment_bases: Vec<i64> = log
            .segments
            .iter()
            .chain(log.active.iter())
            .map(|segment| segment.base_offset().0)
            .collect();
        producer_snapshot::remove_strays(&log.dir, &segment_bases)?;
        log.rebuild_producer_and_transaction_state()?;
        // Recovery needs no durable watermark: the schedule is in the records,
        // so the first advance rebuilds it from the log start.
        log.delivery_watermark = log.log_start_offset();
        Ok(log)
    }

    /// The range a producer-state reload up to `log_end` runs against.
    ///
    /// The log start is the one Kafka's `LogLoader.load` hands
    /// `truncateAndReload`: on a tiered partition (KIP-405) the checkpointed
    /// floor, read as 0 when nobody established one, and otherwise the
    /// greater of that floor and the oldest local segment's base.
    pub(super) fn producer_reload_range(&self, log_end: Offset) -> ProducerReloadRange {
        let local_start = self.local_log_start_offset();
        ProducerReloadRange {
            log_start: krabka_verified::producer_snapshot_reload_log_start(
                self.config.read().unwrap().remote_storage_enable,
                self.established_log_start().map(|start| start.0),
                local_start.0,
            ),
            local_start: local_start.0,
            log_end: log_end.0,
        }
    }

    /// Rebuild producer state the way Kafka's
    /// `UnifiedLog.rebuildProducerState` does: reload the newest snapshot in
    /// `(log start, log end]`, deleting every snapshot outside that range,
    /// then replay the log tail the snapshot does not cover.
    ///
    /// A producer whose last batch lies below the log start is kept only when
    /// a surviving snapshot carries it, exactly as in Kafka. Missing boundary
    /// snapshots are created during the replay so every sealed segment can be
    /// copied to remote storage with its matching producer state, and a
    /// snapshot is taken at the log end once the replay is done.
    pub(super) fn rebuild_producer_and_transaction_state(&mut self) -> Result<(), LogError> {
        self.clear_producer_and_transaction_state();
        let end = self.log_end_offset();
        let range = self.producer_reload_range(end);
        let snapshot = producer_snapshot::reload(&self.dir, range)?;
        let snapshot_offset = snapshot.as_ref().map(|loaded| loaded.offset.0);
        if let Some(LoadedSnapshot { state, .. }) = snapshot {
            self.producer_state = state;
            for (&producer_id, entry) in &self.producer_state {
                if let Some(first_offset) = entry.current_txn_first_offset {
                    self.pending.insert(producer_id, first_offset);
                }
                if entry.coordinator_epoch >= 0 {
                    self.coordinator_epochs
                        .insert(producer_id, entry.coordinator_epoch);
                }
            }
        }
        let mut next = krabka_verified::producer_snapshot_replay_start(range, snapshot_offset)
            .map(Offset)
            .ok_or_else(|| LogError::Corrupt("invalid producer snapshot replay frontier".into()))?;

        let mut boundaries: BTreeSet<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(self.active.iter().map(Segment::base_offset))
            .collect();
        let mut boundaries = boundaries.split_off(&next);
        let _ = boundaries.remove(&next);
        let mut first_read = true;
        while next < end {
            let read = self.read(next, krabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery made no progress at offset {next}"
                )));
            }
            let mut advanced_to = next;
            for (index, batch) in read.batches.iter().enumerate() {
                let cursor = Self::replay_cursor(first_read && index == 0, advanced_to, batch);
                advanced_to = Self::recovered_batch_offsets(cursor, end, batch)?.advanced_to;
                self.apply_recovered_batch_state(batch)?;
                let covered: Vec<_> = boundaries.range(..=advanced_to).copied().collect();
                for boundary in covered {
                    producer_snapshot::write(&*self.io, &self.dir, boundary, &self.producer_state)?;
                    let _ = boundaries.remove(&boundary);
                }
            }
            if advanced_to <= next {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery did not advance past offset {next}"
                )));
            }
            next = advanced_to;
            first_read = false;
        }
        // Kafka's `rebuildProducerState` ends with `updateMapEndOffset(lastOffset)`
        // and `takeSnapshot()`, which writes a snapshot at the log end unless
        // the map end is no further than the last snapshot taken. The next
        // reload then starts from here instead of replaying the tail again.
        // `write` keeps a snapshot already at `end`, which a replay boundary
        // or the loaded snapshot put there with this same state.
        if end.0 > range.log_start {
            producer_snapshot::write(&*self.io, &self.dir, end, &self.producer_state)?;
        }
        self.rebuild_pending_stamp_ranges()?;
        self.refresh_lso()?;
        Ok(())
    }

    /// The cursor a replayed batch must start at or after.
    ///
    /// The replay can start inside a batch, where a trim left the log start.
    /// Kafka's `LogSegment.read(startOffset, ..)` then begins with the batch
    /// that holds `startOffset`, so the first batch of a replay may start
    /// below the cursor. Every later batch must start at or after it.
    fn replay_cursor(first: bool, cursor: Offset, batch: &RecordBatch) -> Offset {
        if first {
            cursor.min(Offset(batch.base_offset))
        } else {
            cursor
        }
    }

    fn apply_recovered_batch_state(&mut self, batch: &RecordBatch) -> Result<(), LogError> {
        if control_batch_kind(batch) == Some(ControlBatchKind::Barrier) {
            // The append path keeps no producer state and no transaction state
            // for a barrier marker. Recovery reaches the same result.
            return Ok(());
        }
        let producer_id = ProducerId(batch.producer_id);
        if producer_id.get() < 0 {
            return Ok(());
        }
        self.update_owned_producer_entry(batch)?;
        if batch.attributes.is_control_batch() {
            let TransactionMarkerFlags {
                is_abort,
                is_commit,
            } = transaction_marker_flags(batch)?;
            if krabka_verified::transaction_marker_closes(
                is_abort,
                is_commit,
                self.pending.contains_key(&producer_id),
            ) && let Some(first_offset) = self.pending.remove(&producer_id)
            {
                self.unreplicated.insert(
                    first_offset,
                    Offset(batch.base_offset + i64::from(batch.last_offset_delta)),
                );
            }
            if (is_abort || is_commit)
                && let Some(epoch) = batch_control_marker_coordinator_epoch(batch)?
            {
                self.coordinator_epochs.insert(producer_id, epoch);
            }
        } else if batch.attributes.is_transactional() {
            self.pending
                .entry(producer_id)
                .or_insert(Offset(batch.base_offset));
        }
        Ok(())
    }

    fn rebuild_pending_stamp_ranges(&mut self) -> Result<(), LogError> {
        self.pending_stamp_ranges.clear();
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut next = self.local_log_start_offset();
        let end = self.log_end_offset();
        let mut is_first_read = true;
        while next < end {
            let slice = self.read(next, krabka_units::mebibytes(1))?;
            if slice.batches.is_empty() {
                let msg = format!("transaction-stamp recovery made no progress at offset {next}");
                return Err(LogError::Corrupt(msg));
            }
            for (index, batch) in slice.batches.iter().enumerate() {
                let producer_id = ProducerId(batch.producer_id);
                let cursor = Self::replay_cursor(is_first_read && index == 0, next, batch);
                let RecoveredBatchOffsets { last, advanced_to } =
                    Self::recovered_batch_offsets(cursor, end, batch)?;
                match control_batch_kind(batch) {
                    // A barrier marker closes no transaction, so it clears no
                    // stamp range. The append path reaches the same result.
                    Some(ControlBatchKind::Barrier) => {}
                    Some(ControlBatchKind::Transaction) => {
                        self.pending_stamp_ranges.remove(&producer_id);
                    }
                    None => {
                        if batch.attributes.is_transactional() && producer_id.get() >= 0 {
                            self.pending_stamp_ranges
                                .entry(producer_id)
                                .or_default()
                                .push(OffsetRange {
                                    first: Offset(batch.base_offset),
                                    last,
                                });
                        }
                    }
                }
                next = advanced_to;
            }
            is_first_read = false;
        }
        Ok(())
    }

    fn recovered_batch_offsets(
        current: Offset,
        end: Offset,
        batch: &RecordBatch,
    ) -> Result<RecoveredBatchOffsets, LogError> {
        match krabka_verified::replay_batch_cursor_decision(
            current.0,
            end.0,
            Some((batch.base_offset, batch.last_offset_delta)),
        ) {
            krabka_verified::ReplayCursorDecision::Advance(next) => Ok(RecoveredBatchOffsets {
                last: Offset(next - 1),
                advanced_to: Offset(next),
            }),
            krabka_verified::ReplayCursorDecision::Stop => Err(LogError::Corrupt(format!(
                "log recovery rejected batch at offset {current} before end {end}"
            ))),
        }
    }

    pub(super) fn update_owned_producer_entry(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), LogError> {
        let producer_id = ProducerId(batch.producer_id);
        if producer_id.get() < 0 {
            return Ok(());
        }
        if batch.attributes.is_control_batch() {
            let entry = self
                .producer_state
                .entry(producer_id)
                .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, batch.producer_epoch));
            if entry.producer_epoch != batch.producer_epoch {
                // Kafka clears the retained data-batch metadata when an end
                // marker advances the producer epoch (transaction version 2).
                self.earlier_batches.remove(&producer_id);
                entry.last_sequence = -1;
                entry.last_offset = Offset(-1);
                entry.offset_delta = 0;
            }
            entry.producer_epoch = batch.producer_epoch;
            entry.timestamp = batch.max_timestamp;
            let TransactionMarkerFlags {
                is_abort,
                is_commit,
            } = transaction_marker_flags(batch)?;
            if is_abort || is_commit {
                entry.current_txn_first_offset = None;
            }
            if (is_abort || is_commit)
                && let Some(epoch) = batch_control_marker_coordinator_epoch(batch)?
            {
                entry.coordinator_epoch = epoch;
            }
            return Ok(());
        }
        self.update_data_producer_entry(DataProducerAppend {
            producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            last_offset_delta: batch.last_offset_delta,
            base_offset: Offset(batch.base_offset),
            timestamp: batch.max_timestamp,
            is_transactional: batch.attributes.is_transactional(),
        })
    }

    pub(super) fn update_data_producer_entry(
        &mut self,
        append: DataProducerAppend,
    ) -> Result<(), LogError> {
        let DataProducerAppend {
            producer_id,
            producer_epoch,
            base_sequence,
            last_offset_delta,
            base_offset,
            timestamp,
            is_transactional,
        } = append;
        let Some(ProducerTail {
            last_sequence,
            last_offset,
        }) = Self::data_producer_tail(producer_id, base_sequence, last_offset_delta, base_offset)?
        else {
            return Ok(());
        };
        let entry = self
            .producer_state
            .entry(producer_id)
            .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, producer_epoch));
        // Kafka's `ProducerStateEntry.addBatch`: a new epoch clears the
        // retained batches, and the batch that was last joins the earlier
        // ones, the oldest leaving at capacity.
        if entry.producer_epoch != producer_epoch {
            self.earlier_batches.remove(&producer_id);
        } else if let Some(previous) = entry.last_batch() {
            let earlier = self.earlier_batches.entry(producer_id).or_default();
            earlier.push_back(previous);
            if earlier.len() >= NUM_BATCHES_TO_RETAIN {
                earlier.pop_front();
            }
        }
        entry.producer_epoch = producer_epoch;
        entry.last_sequence = last_sequence;
        entry.last_offset = last_offset;
        entry.offset_delta = last_offset_delta;
        entry.timestamp = timestamp;
        if is_transactional && entry.current_txn_first_offset.is_none() {
            entry.current_txn_first_offset = Some(base_offset);
        }
        Ok(())
    }

    pub(super) fn data_producer_tail(
        producer_id: ProducerId,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: Offset,
    ) -> Result<Option<ProducerTail>, LogError> {
        if producer_id.get() < 0 || base_sequence < 0 {
            return Ok(None);
        }
        if last_offset_delta < 0 {
            return Err(LogError::InvalidArgument(format!(
                "negative producer offset delta for producer {producer_id}"
            )));
        }
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let last_offset = base_offset
            .0
            .checked_add(i64::from(last_offset_delta))
            .map(Offset)
            .ok_or_else(|| {
                LogError::InvalidArgument(format!(
                    "producer offset overflow for producer {producer_id}"
                ))
            })?;
        Ok(Some(ProducerTail {
            last_sequence,
            last_offset,
        }))
    }
}

#[cfg(test)]
mod tests;
