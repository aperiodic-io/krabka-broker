//! The two append paths: the encoding one and the verbatim passthrough.
//!
//! Both write to the active `.log` file and update the same segment state and
//! sparse indexes, so the shared side effects stay in one file. The verbatim
//! path additionally has to leave the producer's CRC-covered bytes untouched.

use {
    super::{
        Segment,
        io::{write_all, write_all_vectored},
        lifecycle::WriteSnapshot,
    },
    crate::{VerbatimBatch, error::LogError},
    bytes::Bytes,
    krabka_ids::{LeaderEpoch, Offset},
    krabka_protocol::records::{HEADER_LEN, RecordBatch, patch_base_offset_and_leader_epoch},
    krabka_units::prelude::{ByteSize, ByteSizeExt},
    std::io::IoSlice,
    tracing::instrument,
};

/// One batch's encoded bytes, as an append path has them.
#[derive(Clone, Copy)]
enum BatchBytes<'a> {
    /// The encoding path's single buffer.
    Whole(&'a Bytes),
    /// The verbatim path's patched header and the producer's untouched body.
    Split(&'a [u8], &'a [u8]),
}

impl BatchBytes<'_> {
    fn len(self) -> usize {
        match self {
            Self::Whole(bytes) => bytes.len(),
            Self::Split(header, body) => header.len() + body.len(),
        }
    }
}

impl Segment {
    fn append_coordinates(
        &self,
        base: i64,
        delta: i32,
        len: usize,
    ) -> Result<(i64, u64), LogError> {
        let (last, _) = krabka_verified::local_append_coordinates(base, base, delta)
            .ok_or_else(|| LogError::InvalidArgument("invalid batch offset interval".into()))?;
        let len = u64::try_from(len)
            .map_err(|_| LogError::InvalidArgument("encoded batch length overflow".into()))?;
        let size = self
            .log_size
            .checked_add(len)
            .ok_or_else(|| LogError::InvalidArgument("segment byte length overflow".into()))?;
        Ok((last, size))
    }

    /// Append a record batch and return the byte position where the batch
    /// starts.
    ///
    /// Side effects:
    /// - Updates `log_size`, `max_timestamp`, and `last_offset`.
    /// - Adds sparse index entries when the byte count since the last entry
    ///   exceeds `index_interval`, which Kafka's `LogSegment.append` does not
    ///   do for the first batch either.
    #[instrument(
        level = "debug",
        skip(self, batch),
        fields(
            base_offset = self.base_offset.0,
            batch_base = batch.base_offset,
            bytes = batch.encoded_len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(
        &mut self,
        batch: &RecordBatch,
        index_interval: ByteSize,
    ) -> Result<u64, LogError> {
        self.ensure_writable()?;

        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf)?;
        let bytes = buf.freeze();

        self.append_encoded(batch, index_interval, &bytes)
    }

    /// Write an owned batch encoded at the caller's chosen compression level.
    pub(crate) fn append_encoded(
        &mut self,
        batch: &RecordBatch,
        index_interval: ByteSize,
        bytes: &Bytes,
    ) -> Result<u64, LogError> {
        self.ensure_writable()?;
        // Owned appends retain the single-buffer write path and its I/O seam.
        self.write_batch(
            batch.base_offset,
            batch.last_offset_delta,
            batch.max_timestamp,
            index_interval,
            BatchBytes::Whole(bytes),
        )
    }

    /// Append a batch **verbatim** and write the producer's exact wire bytes.
    ///
    /// This method does not decode, re-encode, recompress, or recompute the
    /// CRC. `bytes` is the producer's verbatim v2 batch, which the caller has
    /// already CRC-validated through the borrowed header-only path. This
    /// method patches only `base_offset`, bytes 0..8, and
    /// `partition_leader_epoch`, bytes 12..16, into a writable copy, then
    /// writes those bytes. Both fields sit outside the CRC-covered region. The
    /// stored CRC stays byte-identical to the producer's CRC, because no
    /// CRC-covered byte changes.
    ///
    /// `base_offset`, `last_offset_delta`, `max_timestamp`, and
    /// `leader_epoch` come from the caller's borrowed header read. This method
    /// updates the segment side effects, `log_size`, `last_offset`,
    /// `max_timestamp`, and the sparse index, exactly as [`Segment::append`]
    /// does.
    ///
    /// It returns the byte position where the batch starts.
    #[instrument(
        level = "debug",
        skip(self, bytes),
        fields(
            seg_base_offset = self.base_offset.0,
            base_offset = base_offset.0,
            bytes_len = bytes.len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_verbatim(
        &mut self,
        bytes: &[u8],
        base_offset: Offset,
        last_offset_delta: i32,
        max_timestamp: i64,
        leader_epoch: LeaderEpoch,
        index_interval: ByteSize,
    ) -> Result<u64, LogError> {
        self.ensure_writable()?;
        if bytes.len() < HEADER_LEN {
            return Err(LogError::Corrupt(
                "verbatim batch shorter than v2 header".into(),
            ));
        }

        // Patch base_offset + partition_leader_epoch in a copy of *just* the
        // fixed-size header — both fields live below byte 16, well under the
        // CRC-covered region (byte 21), so the producer's CRC stays valid (no
        // recompute). The batch BODY is written straight from the input slice
        // with no copy: the previous `bytes.to_vec()` was a full-payload memcpy
        // on the produce hot path (100 KiB+ per batch for large messages), the
        // dominant remaining produce-side cost.
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&bytes[..HEADER_LEN]);
        // The protocol patcher writes the raw KIP-320 wire `int32`; unwrap here.
        patch_base_offset_and_leader_epoch(&mut header, base_offset.0, leader_epoch.0);

        self.write_batch(
            base_offset.0,
            last_offset_delta,
            max_timestamp,
            index_interval,
            BatchBytes::Split(&header, &bytes[HEADER_LEN..]),
        )
    }

    /// Write one batch's bytes at the end of the `.log` file.
    ///
    /// Through the page cache, the buffered handle's cursor is kept at
    /// `log_size` by open, recovery and truncate, so the hot append path
    /// needs no `lseek` before a write, and a split batch goes out in one
    /// `writev` without a copy. Through `O_DIRECT`, the writer copies the
    /// batch behind the partial last block and writes whole blocks, and the
    /// batch joins the bytes kept in memory for reads.
    fn write_bytes(&mut self, batch: BatchBytes<'_>, chunk_ends: &[usize]) -> std::io::Result<()> {
        let Some(mode) = self.direct.as_mut() else {
            return match batch {
                BatchBytes::Whole(bytes) => write_all(&*self.io, &self.log_file, bytes),
                BatchBytes::Split(header, body) => {
                    write_all_vectored(&*self.io, &self.log_file, &mut [
                        IoSlice::new(header),
                        IoSlice::new(body),
                    ])
                }
            };
        };
        let io = &*self.io;
        let (parts, cached): (&[&[u8]], Bytes) = match batch {
            BatchBytes::Whole(bytes) => (&[bytes], bytes.clone()),
            BatchBytes::Split(header, body) => {
                (&[header, body], Bytes::from([header, body].concat()))
            }
        };
        mode.writer.append(parts, |file, buf, offset| {
            io.write_direct(file, buf, offset)
        })?;
        if chunk_ends.is_empty() {
            mode.cache.push(cached);
        } else {
            mode.cache.push_group(&cached, chunk_ends);
        }
        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }
        Ok(())
    }

    fn write_batch(
        &mut self,
        base: i64,
        delta: i32,
        max_timestamp: i64,
        index_interval: ByteSize,
        batch: BatchBytes<'_>,
    ) -> Result<u64, LogError> {
        let (last_offset, new_log_size) = self.append_coordinates(base, delta, batch.len())?;
        let position = self.log_size;
        let previous = self.write_snapshot();
        let index_entry = self.index_entry_for(position, last_offset, index_interval)?;
        if let Err(error) = self.write_bytes(batch, &[]) {
            self.rollback_failed_write(position, previous)?;
            return Err(error.into());
        }
        self.log_size = new_log_size;
        self.finish_append(position, previous, index_entry, last_offset, max_timestamp)
    }

    /// Write a validated group once, then build each batch's sparse indexes.
    /// Any failure restores the segment to the start of the group.
    pub(crate) fn append_verbatim_group(
        &mut self,
        batches: &[(&VerbatimBatch, Offset)],
        index_interval: ByteSize,
    ) -> Result<(), LogError> {
        self.ensure_writable()?;
        let size = batches.iter().map(|(batch, _)| batch.bytes.len()).sum();
        let mut bytes = bytes::BytesMut::with_capacity(size);
        let mut chunk_ends = Vec::with_capacity(batches.len());
        for &(batch, base) in batches {
            let mut header = [0; HEADER_LEN];
            header.copy_from_slice(&batch.bytes[..HEADER_LEN]);
            patch_base_offset_and_leader_epoch(&mut header, base.0, batch.leader_epoch.0);
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&batch.bytes[HEADER_LEN..]);
            chunk_ends.push(bytes.len());
        }
        let bytes = bytes.freeze();
        let start = self.log_size;
        let snapshot = self.write_snapshot();
        let result = (|| {
            self.write_bytes(BatchBytes::Whole(&bytes), &chunk_ends)?;
            for &(batch, base) in batches {
                let position = self.log_size;
                let (last, size) =
                    self.append_coordinates(base.0, batch.last_offset_delta, batch.bytes.len())?;
                let entry = self.index_entry_for(position, last, index_interval)?;
                self.log_size = size;
                self.record_batch(last, batch.max_timestamp);
                if let Some(entry) = entry {
                    self.append_index_entries(entry)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.rollback_failed_write(start, snapshot)?;
        }
        result
    }

    /// Publish the completed write and roll back its bytes if an index write fails.
    fn finish_append(
        &mut self,
        position: u64,
        previous: WriteSnapshot,
        index_entry: Option<(u32, u32)>,
        last_offset: i64,
        max_timestamp: i64,
    ) -> Result<u64, LogError> {
        self.record_batch(last_offset, max_timestamp);
        if let Some(entry) = index_entry
            && let Err(error) = self.append_index_entries(entry)
        {
            self.rollback_failed_write(position, previous)?;
            return Err(error);
        }
        tracing::Span::current().record("position", position);
        Ok(position)
    }

    /// Whether the batch about to be written at `position` gets a sparse-index
    /// point, and if so its offset-index entry: the batch's **last** offset,
    /// relative to the segment base, with the position where the batch starts.
    /// Kafka indexes a batch by its last offset (`LogSegment.append` writes
    /// `offsetIndex().append(batchLastOffset, physicalPosition)`), which is
    /// what `kafka-dump-log` checks against the batch at that position.
    ///
    /// The conversions can fail, so they run before anything is written.
    fn index_entry_for(
        &self,
        position: u64,
        last_offset: i64,
        index_interval: ByteSize,
    ) -> Result<Option<(u32, u32)>, LogError> {
        // Kafka's `bytesSinceLastIndexEntry > indexIntervalBytes`: the bytes
        // written since the last entry, counted from that entry's batch (or
        // from the segment start while there is none), must exceed the
        // interval. So the first batch of a segment is never indexed, and a
        // batch exactly one interval past the last entry is not either.
        let last_indexed = self
            .offset_index
            .last_entry()
            .map_or(0, |(_, last_pos)| u64::from(last_pos));
        let should_index = position.saturating_sub(last_indexed) > index_interval.bytes_u64();
        if !should_index {
            return Ok(None);
        }
        let rel = u32::try_from(last_offset - self.base_offset.0)
            .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
        let pos = u32::try_from(position)
            .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
        Ok(Some((rel, pos)))
    }

    /// Account for a batch just written: it moves the last offset, and it
    /// becomes the running maximum timestamp's batch when its timestamp is
    /// strictly higher, Kafka's `maxTimestampAndOffsetSoFar`.
    fn record_batch(&mut self, last_offset: i64, max_timestamp: i64) {
        self.last_offset = Offset(last_offset);
        if max_timestamp > self.max_timestamp {
            self.max_timestamp = max_timestamp;
            self.max_timestamp_offset = Offset(last_offset);
        }
    }

    /// Write one sparse-index point. The time index gets the running maximum
    /// timestamp with the last offset of the batch that set it, and only when
    /// that timestamp is newer than its last entry, as Kafka's
    /// `timeIndex().maybeAppend(maxTimestampSoFar(), shallowOffsetOfMaxTimestampSoFar())`.
    fn append_index_entries(&mut self, (rel, pos): (u32, u32)) -> Result<(), LogError> {
        self.offset_index.append(rel, pos)?;
        self.append_running_max_time_entry()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::segment::test_support::{
            DENSE_INDEX, FIRST_LARGE_TIMESTAMP_BATCH, NO_LIMIT, ONE_RECORD_BATCH, RecordTimestamp,
            SECOND_LARGE_TIMESTAMP_BATCH, SECOND_SINGLE_RECORD_BATCH, SampleBatchSetup,
            SeededSegmentSetup, sample_batch, seeded_fixture, segment_at, test_batch_at,
            test_segment,
        },
        krabka_units::prelude::kibibytes,
    };

    #[test]
    fn append_then_read_back() {
        let (_dir, mut seg) = test_segment();
        let b1 = sample_batch(FIRST_LARGE_TIMESTAMP_BATCH);
        let b2 = sample_batch(SECOND_LARGE_TIMESTAMP_BATCH);
        seg.append(&b1, kibibytes(4)).unwrap();
        seg.append(&b2, kibibytes(4)).unwrap();
        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(read == vec![b1, b2]);
    }

    #[test]
    fn append_rejects_offset_successor_overflow_before_writing() {
        let (dir, mut seg) = segment_at(Offset(i64::MAX));
        let batch = sample_batch(SampleBatchSetup {
            offset: Offset(i64::MAX),
            timestamp: RecordTimestamp(100),
            ..Default::default()
        });

        let error = seg.append(&batch, DENSE_INDEX).unwrap_err();

        assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        assert2::assert!(seg.size().bytes_u64() == 0);
        assert2::assert!(
            std::fs::metadata(crate::name::log_path(dir.path(), i64::MAX))
                .unwrap()
                .len()
                == 0
        );
    }

    #[test]
    fn append_after_truncate_writes_at_new_eof() {
        let (_dir, mut seg) = seeded_fixture(SeededSegmentSetup {
            batches: &[ONE_RECORD_BATCH],
            ..Default::default()
        });
        let expected_position = seg.size().bytes_u64();
        seg.append(&sample_batch(SECOND_SINGLE_RECORD_BATCH), DENSE_INDEX)
            .unwrap();

        seg.truncate_to_relative(1).unwrap();
        let position = seg
            .append(
                &sample_batch(SampleBatchSetup {
                    offset: Offset(1),
                    timestamp: RecordTimestamp(300),
                    ..Default::default()
                }),
                DENSE_INDEX,
            )
            .unwrap();

        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(position == expected_position);
        assert2::assert!(seg.last_offset() == Offset(1));
        assert2::assert!(
            read == vec![
                sample_batch(crate::segment::test_support::ONE_RECORD_BATCH),
                sample_batch(crate::segment::test_support::SampleBatchSetup {
                    offset: Offset(1),
                    timestamp: crate::segment::test_support::RecordTimestamp(300),
                    ..Default::default()
                })
            ]
        );
    }

    #[test]
    fn append_to_sealed_segment_errors() {
        let (_dir, mut seg) = test_segment();
        seg.seal().unwrap();
        assert2::assert!(seg.is_sealed());
        let err = seg
            .append(&sample_batch(SampleBatchSetup::default()), kibibytes(4))
            .unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
    }

    #[test]
    fn append_verbatim_is_byte_exact_except_offset_and_epoch() {
        let (dir, mut seg) = test_segment();
        // Build a batch as a "producer" would, with its own base_offset and
        // leader epoch, then encode to verbatim wire bytes.
        let mut producer = test_batch_at(Offset(0));
        producer.base_offset = 999; // producer-supplied (to be overwritten)
        producer.partition_leader_epoch = -1; // producer-supplied
        producer.last_offset_delta = 0;
        producer.max_timestamp = 1_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        // Append verbatim with an assigned base_offset and a stamped epoch.
        let assigned_base = Offset(0);
        let stamped_epoch = 7i32;
        seg.append_verbatim(
            &wire,
            assigned_base,
            0,
            1_000,
            LeaderEpoch(stamped_epoch),
            DENSE_INDEX,
        )
        .unwrap();
        assert2::assert!(seg.last_offset() == 0);

        // Read back the raw .log bytes.
        let mut on_disk = Vec::new();
        seg.read_log_range(0, &mut on_disk, usize::MAX).unwrap();
        let mut expected_wire = wire.to_vec();
        expected_wire[0..8].copy_from_slice(&assigned_base.0.to_be_bytes());
        expected_wire[12..16].copy_from_slice(&stamped_epoch.to_be_bytes());
        assert2::assert!(on_disk == expected_wire);

        // And it decodes (CRC still valid).
        let mut cur: &[u8] = &on_disk;
        let decoded = krabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        assert2::assert!(decoded.base_offset == assigned_base.0);
        assert2::assert!(decoded.partition_leader_epoch == stamped_epoch);
        drop(dir);
    }

    #[test]
    fn append_verbatim_updates_index_and_last_offset() {
        let (dir, mut seg) = test_segment();
        let mut producer = test_batch_at(Offset(0));
        producer.last_offset_delta = 2; // spans 3 offsets
        producer.max_timestamp = 5_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        seg.append_verbatim(&wire, Offset(0), 2, 5_000, LeaderEpoch(0), DENSE_INDEX)
            .unwrap();
        // Reading at offset 2 (inside the batch) returns the batch.
        let read = seg.read(Offset(2), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(seg.max_timestamp() == 5_000);
        assert2::assert!(read == vec![producer]);
        drop(dir);
    }

    #[test]
    fn append_verbatim_to_sealed_segment_errors() {
        let (dir, mut seg) = test_segment();
        seg.seal().unwrap();
        let mut wire = bytes::BytesMut::new();
        test_batch_at(Offset(0)).encode(&mut wire).unwrap();
        let err = seg
            .append_verbatim(&wire.freeze(), Offset(0), 0, 0, LeaderEpoch(0), DENSE_INDEX)
            .unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
        drop(dir);
    }
}
