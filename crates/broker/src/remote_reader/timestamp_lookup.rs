//! The `ListOffsets`-by-timestamp scan over the remote tier.
//!
//! A remote segment carries a sparse time index, so the index alone answers
//! only which relative offset the scan may start from. This module walks the
//! candidate segments in offset order, converts that floor into a byte
//! position through the offset index, and decodes records from there until it
//! finds the first one at or after the requested timestamp.

use {
    super::RemoteReader,
    krabka_remote_storage::{
        IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
        TimestampMs, TopicIdPartition, first_record_at_or_after_timestamp, parse_offset_index,
        parse_time_index, position_for_relative_offset, relative_offset_floor_for_timestamp,
    },
};

impl RemoteReader {
    /// Returns the smallest absolute offset and its record timestamp where the
    /// timestamp is `>= target_timestamp` and offset is `>= minimum_offset`,
    /// across the finished remote segments.
    /// The sparse time index supplies a scan floor; the exact answer comes from
    /// decoding records from the corresponding offset-index position.
    ///
    /// `max_record_body` is the topic's Kafka trunk `max.decompressed.message.bytes`,
    /// or `None` for no limit. A compressed record the scan has to read that is
    /// above it fails the lookup with [`RemoteStorageError::RecordTooLarge`],
    /// as `RemoteLogManager.findOffsetByTimestamp` fails with
    /// `InvalidRecordException` and does not go on to the next segment.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: TimestampMs,
        minimum_offset: LogOffset,
        max_record_body: Option<usize>,
    ) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
        let mut listed = self.list_remote_log_segments_blocking(tp).await?;
        listed.retain(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished);
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);

        for metadata in listed
            .into_iter()
            // `-1` is the persisted unknown-max sentinel for a sealed segment
            // opened without a tail scan. It must remain scan-eligible for a
            // positive timestamp lookup after broker restart.
            .filter(|md| {
                md.end_offset() >= minimum_offset
                    && (md.max_timestamp_ms() == -1 || md.max_timestamp_ms() >= target_timestamp)
            })
        {
            let (time_index_bytes, offset_index_bytes) = tokio::try_join!(
                self.fetch_index_blocking(metadata.clone(), IndexType::Timestamp),
                self.fetch_index_blocking(metadata.clone(), IndexType::Offset),
            )?;
            let scan_rel = relative_offset_floor_for_timestamp(
                parse_time_index(&time_index_bytes)?,
                target_timestamp,
            );
            let start_position =
                position_for_relative_offset(parse_offset_index(&offset_index_bytes)?, scan_rel);
            // ponytail: one tail read keeps the scan exact; switch to bounded,
            // batch-aligned windows only if remote segment profiling requires it.
            let data = self
                .fetch_log_blocking(metadata.clone(), start_position, None)
                .await?;
            // The time index names the last offset of the batch that set each
            // running maximum, and the offset index turns that into the
            // position of a batch at or before it. The records that follow
            // that position are all candidates, the ones before the named
            // offset inside that batch included: Kafka's
            // `RemoteLogManager.lookupTimestamp` filters on the log start
            // offset only, never on the time index's offset.
            if let Some(found) = first_record_at_or_after_timestamp(
                &data,
                metadata.start_offset().max(minimum_offset),
                target_timestamp,
                max_record_body,
            )? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::remote_reader::test_support::{
            append_time_remote_segment_reader, compressed_remote_segment_reader,
            sparse_remote_segment_reader, sparse_remote_segment_reader_with_max_timestamp, tp,
            unordered_timestamps_remote_segment_reader,
        },
        assert2::assert,
    };

    #[tokio::test]
    async fn timestamp_lookup_excludes_records_before_the_logical_floor() {
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log
        );
        let end = log.tierable_segments().last().unwrap().last_offset.0 + 1;
        for minimum in [0, 1, 3, end - 1, end, i64::MAX] {
            let expected = (minimum < end).then_some((minimum, 0));
            assert!(
                reader
                    .offset_for_timestamp(&tp(), 0, minimum, None)
                    .await
                    .unwrap()
                    == expected
            );
        }
    }

    #[tokio::test]
    async fn timestamp_lookup_answers_in_append_time() {
        let (reader, _dir) = append_time_remote_segment_reader();
        for (floor, target, expected) in [
            (0, 1_500, Some((10, 1_700))),
            (11, 1_500, Some((11, 1_700))),
            (0, 1_800, Some((14, 2_400))),
            (15, 2_400, Some((15, 2_400))),
            (0, 2_401, None),
        ] {
            assert!(
                reader
                    .offset_for_timestamp(&tp(), target, floor, None)
                    .await
                    .unwrap()
                    == expected,
                "floor {floor} target {target}"
            );
        }
    }

    #[tokio::test]
    async fn offset_for_timestamp_locates_remote_segment() {
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log, exports
        );
        // The segment metadata copies `max_timestamp` from the export; the
        // log's batch builder leaves base_timestamp at 0 by default, so
        // every batch's max_timestamp is 0 — so segments' max_timestamps are
        // all 0. Target a timestamp <= 0 to match the first segment.
        let target_ts = 0_i64;
        let got = reader
            .offset_for_timestamp(&tp(), target_ts, 0, None)
            .await
            .unwrap()
            .expect("first segment matches ts=0");
        // The first finished segment is the lowest-base one.
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        assert!(got == (expected, 0));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_before_sparse_ceiling() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 1_500, 0, None)
            .await
            .unwrap()
            .expect("timestamp 1500 has a remote match");

        assert!(got == (12, 1_600));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_exact_indexed_record_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_000, 0, None)
            .await
            .unwrap()
            .expect("timestamp 2000 has an exact record match");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_segment_with_unknown_max_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader_with_max_timestamp(-1);

        let got = reader
            .offset_for_timestamp(&tp(), 2_000, 0, None)
            .await
            .unwrap()
            .expect("the unknown max sentinel must not suppress an exact remote scan");

        assert!(got == (14, 2_000));
    }

    /// The time index names the last offset of the batch that set a running
    /// maximum. The newest record can sit earlier in that batch, and the scan
    /// must still find it: Kafka's `RemoteLogManager.lookupTimestamp` filters
    /// on the log start offset, never on the index entry's offset.
    #[tokio::test]
    async fn offset_for_timestamp_finds_the_newest_record_before_its_batchs_last_offset() {
        let (reader, _remote_dir) = unordered_timestamps_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_400, 0, None)
            .await
            .unwrap()
            .expect("the newest record is the first of the second batch");

        assert!(got == (14, 2_400));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_none_when_past_last() {
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, _log
        );
        // All segments have max_ts=0 by construction (see test above); any
        // strictly-positive target is past every remote segment.
        let got = reader
            .offset_for_timestamp(&tp(), 1, 0, None)
            .await
            .unwrap();
        assert!(got == None);
    }

    /// Kafka trunk's `max.decompressed.message.bytes` on the remote scan:
    /// `RemoteLogManager.lookupTimestamp` decompresses a batch only when its max
    /// timestamp reaches the target, and fails on a record above the limit
    /// rather than moving on to another segment. A record of the second batch
    /// is a `1_007`-byte body, and one of the first is a few dozen.
    #[tokio::test]
    async fn offset_for_timestamp_refuses_a_record_above_the_limit_that_it_reads() {
        use krabka_remote_storage::RemoteStorageError;

        let (reader, _remote_dir) = compressed_remote_segment_reader(1_000);
        for (name, target, limit, want) in [
            // The match is in the first batch, so the second is never read.
            (
                "found before the oversized batch",
                1_500,
                Some(100),
                Ok(Some((12, 1_600))),
            ),
            // The first batch tops out at 1_700, so it is skipped undecompressed
            // and the second batch's first record is the oversized match.
            ("the match is oversized", 1_800, Some(100), Err(1_007)),
            ("no limit", 1_800, None, Ok(Some((14, 2_000)))),
            (
                "a limit above the record",
                1_800,
                Some(1_007),
                Ok(Some((14, 2_000))),
            ),
        ] {
            let got = match reader.offset_for_timestamp(&tp(), target, 0, limit).await {
                Ok(found) => Ok(found),
                Err(RemoteStorageError::RecordTooLarge { size, limit: seen }) => {
                    assert!(Some(seen) == limit, "{name}");
                    Err(size)
                }
                Err(other) => panic!("{name}: unexpected error: {other}"),
            };
            assert!(got == want, "{name}");
        }
    }
}
