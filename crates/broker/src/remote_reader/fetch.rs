//! Assembly of one `Fetch` answer from the remote tier.
//!
//! This module resolves the finished remote segment that covers a requested
//! offset, turns that offset into a byte position through the segment's
//! offset index, and reads back the capped byte range that holds the batch.
//! The segment resolution carries the defensive fallback that keeps a read
//! answerable when the epoch-indexed lookup in the `RLMM` misses.

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_protocol::records::{HEADER_LEN, RecordBatch, RecordBatchHeader};
use krabka_remote_storage::{
    IndexType, LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
    TopicIdPartition, end_position_for, first_batch_at_or_after, parse_offset_index,
    position_for_relative_offset,
};
use krabka_verified::remote_read_relative_offset;
use zerocopy::FromBytes as _;

use super::RemoteReader;

impl RemoteReader {
    /// Finds the finished segment in the RLMM that covers
    /// `(leader_epoch, offset)`, fetches its offset index, positions into the
    /// `.log` data, and returns the first batch whose last offset is
    /// `>= offset`. It returns `None` when no finished segment covers the
    /// requested offset.
    ///
    /// `max_bytes` caps the byte range that this method fetches from the
    /// remote tier. The caller's `partition_max_bytes` from the Fetch request
    /// arrives here.
    pub(crate) async fn fetch_batch(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>, RemoteStorageError> {
        let Some(read) = self
            .read_segment_at(tp, leader_epoch, offset, max_bytes)
            .await?
        else {
            return Ok(None);
        };
        Ok(first_batch_at_or_after(&read.data, offset))
    }

    /// Kafka's `RemoteLogManager.read`: the whole batches of the finished
    /// segment that covers `(leader_epoch, offset)`, from the batch that holds
    /// `offset` on, as the segment stores them.
    ///
    /// A share fetch and a dead-letter copy answer with the log's own bytes,
    /// so they read the tier through this and not through
    /// [`Self::fetch_batch`], which decodes. The read stays inside one
    /// segment and caps its bytes at `max_bytes` past the batch that holds
    /// `offset`, as [`Self::fetch_batch`] does. The first batch comes back
    /// whole even when it is larger than `max_bytes`, which is Kafka's
    /// `minOneMessage`. It returns `None` when no finished segment holds a
    /// batch at or after `offset`.
    pub(crate) async fn fetch_raw(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<Option<Bytes>, RemoteStorageError> {
        let Some(read) = self
            .read_segment_at(tp, leader_epoch, offset, max_bytes)
            .await?
        else {
            return Ok(None);
        };
        match whole_batches_from(&read.data, offset) {
            RawScan::Batches(range) => Ok(Some(Bytes::from(read.data).slice(range))),
            RawScan::Missing => Ok(None),
            RawScan::Truncated { at, len } => {
                // The cap cut the first batch the caller wants, so read that
                // batch alone. When the read stopped before its length field,
                // read the field first.
                let start = read
                    .start_position
                    .saturating_add(u32::try_from(at).unwrap_or(u32::MAX));
                let len = if let Some(len) = len {
                    len
                } else {
                    let field_end = start.saturating_add(field_len() - 1);
                    let prefix = self
                        .fetch_log_blocking(read.metadata.clone(), start, Some(field_end))
                        .await?;
                    let Some(len) = batch_len(&prefix) else {
                        return Ok(None);
                    };
                    len
                };
                let end = start
                    .saturating_add(u32::try_from(len).unwrap_or(u32::MAX))
                    .saturating_sub(1);
                let data = self
                    .fetch_log_blocking(read.metadata, start, Some(end))
                    .await?;
                Ok(match whole_batches_from(&data, offset) {
                    RawScan::Batches(range) => Some(Bytes::from(data).slice(range)),
                    RawScan::Missing | RawScan::Truncated { .. } => None,
                })
            }
        }
    }

    /// Resolves the finished segment that covers `(leader_epoch, offset)` and
    /// reads the byte range that holds the batch at `offset` and `max_bytes`
    /// past it. `None` when no finished segment covers the offset.
    async fn read_segment_at(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<Option<SegmentRead>, RemoteStorageError> {
        // Primary lookup: epoch-indexed fast path.  The caller resolves
        // `leader_epoch` from the local leader-epoch checkpoint via
        // `epoch_for_offset`, so this is the epoch that *owned* the requested
        // offset at copy time.  The RLMM indexes a segment under every epoch
        // in its `segment_leader_epochs` map, so this reliably hits after a
        // clean failover.
        let primary = self
            .rlmm
            .remote_log_segment_metadata(tp, leader_epoch, offset)?;

        // Defensive fallback: the epoch-indexed primary lookup can still miss
        // in rare edge cases (e.g. the local leader-epoch checkpoint is empty
        // on a fresh replica, or an unclean election produced a gap in the
        // checkpoint that `epoch_for_offset` cannot bridge).  When the primary
        // misses, scan `list_remote_log_segments` for finished segments that
        // cover `offset` and prefer the one whose `segment_leader_epochs` map
        // contains the passed epoch (same lineage). No lineage-unmatched
        // segment is a safe fallback under log divergence, so that case is a
        // miss rather than a read from a different history.
        let primary = primary.and_then(|metadata| {
            relative_offset(&metadata, leader_epoch, offset).map(|relative| (metadata, relative))
        });
        let (metadata, target_rel) = if let Some(selected) = primary {
            selected
        } else {
            let candidates = self.rlmm.list_remote_log_segments(tp)?;
            let Some(selected) = candidates
                .into_iter()
                .filter_map(|metadata| {
                    relative_offset(&metadata, leader_epoch, offset)
                        .map(|relative| (metadata, relative))
                })
                .max_by_key(|(metadata, _)| metadata.start_offset())
            else {
                return Ok(None);
            };
            selected
        };

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Offset)
            .await?;
        let entries = parse_offset_index(&index_bytes)?;
        let start_position = position_for_relative_offset(entries, target_rel);

        // Cap the read so the broker doesn't pull an entire segment when the
        // Fetch asked for one batch. Always pull at least one full batch worth
        // of bytes — the segment's `size` is the safe ceiling.
        let segment_size =
            u32::try_from(metadata.segment_size_in_bytes().max(0)).unwrap_or(u32::MAX);
        // An offset-index entry holds the *last* offset of its batch, as Kafka
        // writes it, so `start_position` is a batch that ends at or below
        // `offset`. The batch that covers `offset` starts no later than the
        // next indexed batch (or than the end of the segment, past the last
        // entry), so widen the cap by that span: the caller's budget then
        // counts from the covering batch, as it does when Kafka reads the
        // stream on from the index position.
        let next_entry = entries.partition_point(|entry| entry.relative_offset.get() <= target_rel);
        let skip_span = entries
            .get(next_entry)
            .map_or(segment_size, |entry| entry.position.get())
            .saturating_sub(start_position);
        // A `max_bytes` of zero means "no cap".
        let capped_bytes = if max_bytes == 0 {
            0
        } else {
            max_bytes.saturating_add(usize::try_from(skip_span).unwrap_or(usize::MAX))
        };
        let end_position = end_position_for(start_position, segment_size, capped_bytes);

        let data = self
            .fetch_log_blocking(metadata.clone(), start_position, end_position)
            .await?;
        Ok(Some(SegmentRead {
            metadata,
            start_position,
            data,
        }))
    }
}

/// The bytes one read took from a remote segment, and where they start in it.
struct SegmentRead {
    metadata: RemoteLogSegmentMetadata,
    start_position: u32,
    data: Vec<u8>,
}

/// Where the whole batches of a read that end at or after an offset sit.
#[derive(Debug, PartialEq, Eq)]
enum RawScan {
    /// The read holds them in this byte range, the first one whole.
    Batches(std::ops::Range<usize>),
    /// The first one starts at byte `at`, but the read holds only part of it.
    /// `len` is its whole length when the read reached its length field.
    Truncated { at: usize, len: Option<usize> },
    /// The read holds no batch that ends at or after the offset.
    Missing,
}

/// The bytes in front of a v2 batch's `batch_length` count: the base offset
/// (8) and the length itself (4).
const LOG_OVERHEAD: usize = 12;

/// [`LOG_OVERHEAD`] as a byte position: how far a read must reach to hold a
/// batch's length field.
fn field_len() -> u32 {
    u32::try_from(LOG_OVERHEAD).unwrap_or(u32::MAX)
}

/// The whole length of the batch that `bytes` starts with, once `bytes` holds
/// its length field. `None` when it does not, or when the length is too small
/// for a v2 batch.
fn batch_len(bytes: &[u8]) -> Option<usize> {
    bytes
        .get(8..LOG_OVERHEAD)
        .and_then(|field| field.try_into().ok())
        .map(i32::from_be_bytes)
        .and_then(|length| usize::try_from(length).ok())
        .map(|length| length + LOG_OVERHEAD)
        .filter(|len| *len >= HEADER_LEN)
}

/// Walks the batch headers of `data` and finds the whole batches whose last
/// offset is at or after `offset`. It decodes no record.
///
/// The read's cap always reaches past every batch in front of the one that
/// holds `offset`, so a batch that the read cut short before the first wanted
/// one counts as that wanted batch, and a malformed length ends the walk.
fn whole_batches_from(data: &[u8], offset: LogOffset) -> RawScan {
    let mut at = 0_usize;
    let mut first: Option<usize> = None;
    let found = |first: Option<usize>, at: usize| first.map(|first| RawScan::Batches(first..at));
    while at < data.len() {
        let rest = &data[at..];
        if rest.len() < LOG_OVERHEAD {
            return found(first, at).unwrap_or(RawScan::Truncated { at, len: None });
        }
        let Some(len) = batch_len(rest) else {
            return found(first, at).unwrap_or(RawScan::Missing);
        };
        let whole = rest
            .get(..len)
            .and_then(|batch| batch.get(..HEADER_LEN))
            .and_then(|raw| RecordBatchHeader::ref_from_bytes(raw).ok());
        let Some(header) = whole else {
            return found(first, at).unwrap_or(RawScan::Truncated { at, len: Some(len) });
        };
        let last = header
            .base_offset
            .get()
            .saturating_add(i64::from(header.last_offset_delta.get()));
        if last >= offset && first.is_none() {
            first = Some(at);
        }
        at += len;
    }
    found(first, at).unwrap_or(RawScan::Missing)
}

fn relative_offset(
    metadata: &RemoteLogSegmentMetadata,
    leader_epoch: LeaderEpoch,
    requested_offset: LogOffset,
) -> Option<u32> {
    let (epoch_start, next_epoch_start) = metadata.epoch_bounds(leader_epoch);
    remote_read_relative_offset(
        metadata.start_offset(),
        metadata.end_offset(),
        requested_offset,
        metadata.state() == RemoteLogSegmentState::CopySegmentFinished,
        epoch_start,
        next_epoch_start,
    )
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_remote_storage::RemoteLogSegmentMetadata;
    use uuid::Uuid;

    use super::*;
    use crate::{
        remote_log_manager::test_support::local_backends,
        remote_reader::test_support::{
            caching_sparse_remote_segment_reader, empty_reader, not_ready_reader,
            sparse_fixture_batch_bytes, sparse_fixture_second_batch_len,
            sparse_remote_segment_reader, tp,
        },
    };

    // Kafka's `RemoteLogManager.read` returns the segment's own bytes from the
    // batch that holds the offset on, up to the budget, and always the first
    // batch whole (`minOneMessage`). The sparse segment holds offsets 10 to 13,
    // then 14 to 16. Each case is `(offset, budget, the batches returned)`.
    #[tokio::test]
    async fn fetch_raw_returns_the_segment_bytes_from_the_batch_that_holds_the_offset() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();
        let [first, second] = sparse_fixture_batch_bytes();
        let both = Bytes::from([first.clone(), second.clone()].concat());
        let cases = [
            (10, 4096, Some(both.clone())),
            (12, 4096, Some(both)),
            (14, 4096, Some(second.clone())),
            (16, sparse_fixture_second_batch_len(), Some(second.clone())),
            (10, 1, Some(first)),
            (14, 1, Some(second)),
            (17, 4096, None),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (offset, budget, want) in cases {
            let got = reader
                .fetch_raw(&tp(), LeaderEpoch(0), offset, budget)
                .await
                .expect("the tier answers");
            actual.push((offset, budget, got));
            expected.push((offset, budget, want));
        }
        assert!(actual == expected);
    }

    // The header walk under the raw read: which whole batches a read holds
    // from an offset on, and what to read again when the cap cut the first
    // of them. Each case is `(label, the bytes read, offset, scan)`.
    #[test]
    fn the_raw_scan_keeps_whole_batches_from_the_offset() {
        let [first, second] = sparse_fixture_batch_bytes();
        let both = [first.to_vec(), second.to_vec()].concat();
        let (one, two) = (first.len(), both.len());
        let cases: [(&str, Vec<u8>, i64, RawScan); 7] = [
            ("both batches", both.clone(), 10, RawScan::Batches(0..two)),
            (
                "from the second",
                both.clone(),
                14,
                RawScan::Batches(one..two),
            ),
            ("past the read", both.clone(), 17, RawScan::Missing),
            (
                "a cut second batch ends the run",
                both[..two - 1].to_vec(),
                10,
                RawScan::Batches(0..one),
            ),
            (
                "a cut first batch is read again whole",
                both[..one - 1].to_vec(),
                10,
                RawScan::Truncated {
                    at: 0,
                    len: Some(one),
                },
            ),
            (
                "a cut wanted batch after a skipped one",
                both[..one + 20].to_vec(),
                14,
                RawScan::Truncated {
                    at: one,
                    len: Some(two - one),
                },
            ),
            (
                "a cut before the length field",
                both[..one + 4].to_vec(),
                14,
                RawScan::Truncated { at: one, len: None },
            ),
        ];
        for (label, data, offset, want) in cases {
            check!(whole_batches_from(&data, offset) == want, "{label}");
        }
    }

    /// KIP-405's `RemoteIndexCache`: a consumer walking one cold segment
    /// downloads its `.index` once, not once per `Fetch`. Before the cache,
    /// every batch a consumer read from the tier pulled the whole offset index
    /// again, which is two or three object GETs per batch on a topic whose
    /// segments are all remote.
    #[tokio::test]
    async fn two_fetches_of_one_segment_download_its_offset_index_once() {
        let (reader, _dirs, index_fetches) = caching_sparse_remote_segment_reader();

        for offset in [10, 12] {
            reader
                .fetch_batch(&tp(), LeaderEpoch(0), offset, 4096)
                .await
                .expect("ok")
                .expect("both offsets are in the synthetic remote segment");
        }

        assert!(
            index_fetches.load(std::sync::atomic::Ordering::Relaxed) == 1,
            "the second fetch must read the cached index, not download it again"
        );
        let stats = reader.index_cache.stats();
        assert!(stats.hits == 1 && stats.misses == 1, "{stats:?}");
    }

    /// The same segment's index is fetched again once the cache is told the
    /// segment is going away, which is what keeps a deleted segment's bytes
    /// from holding the budget against live ones.
    #[tokio::test]
    async fn dropping_a_segment_from_the_cache_makes_the_next_read_download_again() {
        let (reader, _dirs, index_fetches) = caching_sparse_remote_segment_reader();
        let segment_id = reader
            .rlmm
            .list_remote_log_segments(&tp())
            .expect("list")
            .first()
            .expect("one segment")
            .remote_log_segment_id()
            .id;

        reader
            .fetch_batch(&tp(), LeaderEpoch(0), 10, 4096)
            .await
            .expect("ok")
            .expect("a batch");
        reader.index_cache.remove_segment(segment_id);
        reader
            .fetch_batch(&tp(), LeaderEpoch(0), 12, 4096)
            .await
            .expect("ok")
            .expect("a batch");

        assert!(index_fetches.load(std::sync::atomic::Ordering::Relaxed) == 2);
        assert!(reader.index_cache.stats().hits == 0);
    }

    #[tokio::test]
    async fn fetch_batch_finds_segment_and_returns_first_batch() {
        // Pick an offset inside the second sealed segment. Each batch covers
        // two records, so base_offset=2 lives in segment[1] (base=2).
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log, exports
        );
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[1].base_offset.0;

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), target_offset, 4096)
            .await
            .expect("ok")
            .expect("found a batch");
        // The batch returned should start at or before target_offset and end
        // at or after it.
        let last = got.base_offset + i64::from(got.last_offset_delta);
        assert!(
            got.base_offset <= target_offset && last >= target_offset,
            "batch [{},{}] doesn't cover target {target_offset}",
            got.base_offset,
            last
        );
    }

    #[tokio::test]
    async fn fetch_batch_uses_offset_relative_to_remote_segment_start() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 12, 4096)
            .await
            .expect("ok")
            .expect("offset 12 is in the synthetic remote segment");

        assert!(
            got.base_offset == 10,
            "relative offset 2 should read the first batch, not jump to {}",
            got.base_offset
        );
    }

    /// Kafka's offset index holds each batch's *last* offset, so the position
    /// it gives for an offset in the second batch is the first batch, which
    /// ends below it. A budget of exactly the second batch's size must still
    /// return that batch: the read counts its budget from the covering batch,
    /// not from the indexed one before it.
    #[tokio::test]
    async fn fetch_batch_budget_counts_from_the_covering_batch() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 14, sparse_fixture_second_batch_len())
            .await
            .expect("ok")
            .expect("offset 14 starts the second batch");

        assert!(got.base_offset == 14);
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_when_segment_not_in_rlmm() {
        let (_remote_dir, reader) = empty_reader();
        // RLMM is empty → no segment for `tp` at epoch 0.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_for_in_progress_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let (rsm, rlmm) = local_backends(remote_dir.path());
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id,
            0,
            99,
            100,
            1,
            100,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0_i64},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        let reader = RemoteReader::new(rsm, rlmm);
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 50, 4096)
            .await
            .unwrap();
        assert!(
            got.is_none(),
            "started (not finished) segment must be invisible"
        );
    }

    #[tokio::test]
    async fn fetch_batch_propagates_not_ready() {
        let (_remote_dir, reader) = not_ready_reader();
        let err = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[test]
    fn relative_offset_respects_epoch_subrange_boundary() {
        let metadata = RemoteLogSegmentMetadata::new(
            krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4()),
            0,
            99,
            0,
            1,
            0,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentFinished,
                maplit::btreemap! {
                    LeaderEpoch(0) => 0,
                    LeaderEpoch(1) => 50,
                },
            ),
        )
        .unwrap();

        for (epoch, offset, expected) in [
            (LeaderEpoch(0), 49, Some(49)),
            (LeaderEpoch(0), 50, None),
            (LeaderEpoch(1), 49, None),
            (LeaderEpoch(1), 50, Some(50)),
        ] {
            assert!(
                relative_offset(&metadata, epoch, offset) == expected,
                "epoch={epoch:?} offset={offset}"
            );
        }
    }

    /// The broker tiers segments under the leader epoch that was active at
    /// copy time. In normal operation `fetch_batch` receives the owning epoch,
    /// which the caller resolves from the leader-epoch checkpoint, and the
    /// epoch-indexed primary lookup hits.
    ///
    /// This test exercises the defensive fallback with an epoch that is not in
    /// the segment's lineage, as an empty or stale checkpoint could supply.
    /// Serving that segment could cross divergent histories, so the fallback
    /// must fail closed.
    #[tokio::test]
    async fn fallback_rejects_segment_from_the_wrong_leader_epoch() {
        // `populated_reader` registers all segments under epoch 0 (the epoch
        // present in the tierable-segment export, defaulted to 0 when the log
        // was written without an explicit epoch).
        // Pick an offset inside the first sealed segment.
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log, exports
        );
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[0].base_offset.0;

        // Query with epoch 1 — the RLMM epoch-indexed primary path returns
        // None because the segment's `segment_leader_epochs` only contains
        // epoch 0. The fallback must not cross that lineage boundary.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(1), target_offset, 4096)
            .await
            .expect("ok");

        assert!(got.is_none(), "wrong-epoch remote data must fail closed");
    }
}
