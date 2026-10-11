//! The on-disk KIP-630 metadata checkpoints: the `FetchSnapshot` answer that
//! the engine gives a broker listener, the byte window of the newest
//! `<end_offset>-<epoch>.checkpoint`, the read of that file, and the
//! manual snapshot trigger. The checkpoint file layout is read straight from
//! disk here rather than through the engine, so it is kept apart from the rest
//! of the handle.

use super::ControllerHandle;
use crate::{
    error::RaftError,
    kraft::{
        controller::checkpoint::{latest_checkpoint_id, load_checkpoint_by_id},
        transport::Inbound,
    },
    snapshot::SnapshotReader,
};

/// A contiguous byte window of the latest metadata `.checkpoint`, returned by
/// [`ControllerHandle::read_snapshot_range`].
#[derive(Debug, PartialEq)]
pub struct SnapshotSlice {
    pub end_offset: i64,
    pub epoch: i32,
    pub total_size: i64,
    pub bytes: bytes::Bytes,
}

/// Outcome of [`ControllerHandle::read_snapshot_range`].
pub enum SnapshotRange {
    /// No `.checkpoint` exists yet.
    NoSnapshot,
    /// `position` is at or past the snapshot's end byte, which Kafka's
    /// `handleFetchSnapshotRequest` answers with `POSITION_OUT_OF_RANGE`.
    OutOfRange,
    /// The requested byte window.
    Slice(SnapshotSlice),
}

impl ControllerHandle {
    /// Read up to `max_bytes` of the latest metadata snapshot starting at
    /// `position`. Reads the engine's `.checkpoint` artifacts directly (the
    /// engine writes a bare KIP-630 checkpoint, no `.meta` sidecar).
    ///
    /// `position` and `max_bytes` stay the raw KIP-595 `FetchSnapshot` `int64`
    /// and `int32`: both are byte offsets into an on-disk checkpoint that the
    /// broker's handler forwards straight off the wire, so there is no domain
    /// layer between the decode and the slice index for a quantity to occupy.
    #[must_use]
    pub fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        let Some((id, bytes)) = load_latest_checkpoint(&self.data_dir) else {
            return SnapshotRange::NoSnapshot;
        };
        let Some(pos) = usize::try_from(position)
            .ok()
            .filter(|pos| *pos < bytes.len())
        else {
            return SnapshotRange::OutOfRange;
        };
        let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
        let slice = SnapshotReader::byte_range(&bytes, pos, max);
        SnapshotRange::Slice(SnapshotSlice {
            end_offset: id.0,
            epoch: id.1,
            total_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            bytes: bytes::Bytes::copy_from_slice(slice),
        })
    }

    /// Answer one KIP-630 `FetchSnapshot` request through the engine, as the
    /// controller listener answers it.
    ///
    /// `body` is the request body at `version`, and the answer is the response
    /// body at that version. The engine follows Kafka's
    /// `KafkaRaftClient.handleFetchSnapshotRequest`: it serves exactly the
    /// snapshot the request names, and refuses another topic or partition, a
    /// stale or unknown `CurrentLeaderEpoch`, another cluster id, and a
    /// position outside the snapshot. It also fills `CurrentLeader` and the
    /// leader's endpoint. A broker listener that answered from the checkpoint
    /// directly would answer the same request differently.
    ///
    /// # Errors
    /// Returns [`RaftError::Shutdown`] if the engine task is gone, or if it
    /// dropped the request because the body does not decode.
    pub async fn fetch_snapshot(
        &self,
        version: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.engine
            .deliver(Inbound::FetchSnapshot {
                req: body,
                version,
                reply,
            })
            .await?;
        answer.await.map_err(|_| RaftError::Shutdown)
    }

    /// Manually trigger a metadata snapshot (KIP-630 checkpoint) on this node.
    ///
    /// # Errors
    /// Returns [`RaftError`] if serialization or the file write fails.
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        self.engine.trigger_snapshot().await
    }
}

/// The highest `(end_offset, epoch)` `.checkpoint` in `dir` plus its raw
/// bytes. Matches the bare-checkpoint format the engine writes (no `.meta`
/// sidecar).
pub(super) fn load_latest_checkpoint(dir: &std::path::Path) -> Option<((i64, i32), Vec<u8>)> {
    let id = latest_checkpoint_id(dir)?;
    let bytes = load_checkpoint_by_id(dir, id.0, id.1)?;
    Some((id, bytes))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{controller::test_support::joining_controller, metadata_partition_dir};

    #[test]
    fn load_latest_checkpoint_picks_highest_offset_then_epoch() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000001-0000000009.checkpoint"),
            b"old-offset",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000002-0000000001.checkpoint"),
            b"old-epoch",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join("00000000000000000002-0000000003.checkpoint"),
            b"best",
        )
        .unwrap();
        std::fs::write(dir.path().join("9-9.txt"), b"ignored suffix").unwrap();
        std::fs::write(
            dir.path().join("not-a-checkpoint.checkpoint"),
            b"ignored name",
        )
        .unwrap();

        let latest = load_latest_checkpoint(dir.path()).expect("checkpoint");
        assert2::assert!(latest == ((2, 3), b"best".to_vec()));
    }

    #[tokio::test]
    async fn read_snapshot_range_rejects_the_end_and_everything_past_it() {
        let (dir, ctrl) = joining_controller("join start").await;
        let checkpoint_dir = metadata_partition_dir(dir.path());
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(
            checkpoint_dir.join("00000000000000000010-0000000004.checkpoint"),
            b"abc",
        )
        .unwrap();

        // Kafka's `handleFetchSnapshotRequest` refuses `position < 0` and
        // `position >= size`, so the end of the snapshot is not a valid position.
        let read = |position| match ctrl.read_snapshot_range(position, 10) {
            SnapshotRange::Slice(slice) => Some(slice),
            SnapshotRange::OutOfRange => None,
            SnapshotRange::NoSnapshot => panic!("the checkpoint was written"),
        };
        let window = |bytes: &'static [u8]| SnapshotSlice {
            end_offset: 10,
            epoch: 4,
            total_size: 3,
            bytes: bytes::Bytes::from_static(bytes),
        };
        for (position, expected) in [
            (0, Some(window(b"abc"))),
            (2, Some(window(b"c"))),
            (3, None),
            (4, None),
            (-1, None),
        ] {
            assert2::assert!(read(position) == expected, "position {position}");
        }
        ctrl.shutdown().await;
    }
}
