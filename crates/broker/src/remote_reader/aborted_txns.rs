//! Reading remote transaction indexes for a Fetch window.
//!
//! A `Fetch` under `read_committed` needs the aborted transactions that
//! overlap the offsets it returns. Abort markers can live in later segments,
//! so this module scans the finished remote tail's `.txnindex` objects and
//! keeps entries overlapping the requested range. A segment owning no abort
//! marker has no index object; those missing optional objects are skipped.

use {
    super::{AbortedTxnEntry, RemoteReader},
    krabka_ids::LeaderEpoch,
    krabka_remote_storage::{
        IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
        TopicIdPartition, parse_txn_index, txn_overlaps,
    },
};

impl RemoteReader {
    /// Returns the aborted transactions that overlap the inclusive offset
    /// range `[from_offset, to_offset]`. An abort marker can be in a later
    /// finished segment than its data, so all later indexes are considered.
    /// Returns an empty `Vec` if no finished segment covers the offset or no
    /// available transaction index contains an overlapping entry. Missing
    /// optional indexes (`SegmentNotFound` from `fetch_index`) are skipped.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        from_offset: LogOffset,
        to_offset: LogOffset,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        let Some(metadata) =
            self.rlmm
                .remote_log_segment_metadata(tp, leader_epoch, from_offset)?
        else {
            return Ok(Vec::new());
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(Vec::new());
        }

        let mut segments = self.list_remote_log_segments_blocking(tp).await?;
        segments.retain(|segment| {
            segment.state() == RemoteLogSegmentState::CopySegmentFinished
                && segment.end_offset() >= from_offset
        });
        segments.sort_by_key(RemoteLogSegmentMetadata::start_offset);
        let mut aborts = Vec::new();
        // ponytail: scan the finished tail for later abort markers; use trusted
        // LSO hints to stop earlier if cold-Fetch profiling requires it.
        for segment in segments {
            let bytes = match self
                .fetch_index_blocking(segment, IndexType::Transaction)
                .await
            {
                Ok(bytes) => bytes,
                // The index is optional when this segment owns no abort marker.
                Err(RemoteStorageError::SegmentNotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            aborts.extend(
                parse_txn_index(&bytes)?
                    .iter()
                    .filter(|entry| txn_overlaps(entry, from_offset, to_offset))
                    .map(|entry| AbortedTxnEntry {
                        start_offset: entry.start_offset.get(),
                        last_offset: entry.last_offset.get(),
                        producer_id: entry.producer_id.get(),
                    }),
            );
        }
        Ok(aborts)
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::remote_reader::test_support::{empty_reader, populated_reader_with_abort, tp},
        assert2::assert,
    };

    #[tokio::test]
    async fn aborted_transactions_returns_copied_abort() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log, abort) = populated_reader_with_abort(log_dir.path(), remote_dir.path());
        let (start, last, pid) = abort;

        // Query the first segment's offset range → the abort overlaps.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), start, last)
            .await
            .expect("ok");
        let expected = vec![AbortedTxnEntry {
            start_offset: start,
            last_offset: last,
            producer_id: pid,
        }];
        assert!(got == expected, "the copied abort is returned");
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_segment_has_no_txnindex() {
        // The default harness writes no `.txnindex` for any segment.
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log, exports
        );
        let seg = &exports[0];

        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), seg.base_offset.0, seg.last_offset.0)
            .await
            .expect("ok");
        assert!(
            got.is_empty(),
            "segment with no .txnindex yields an empty list, not an error"
        );
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_no_segment() {
        let (_remote_dir, reader) = empty_reader();
        // RLMM is empty → no covering segment → empty list, not an error.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), 0, 100)
            .await
            .expect("ok");
        assert!(got.is_empty());
    }
}
