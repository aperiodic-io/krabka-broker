//! The one fixture the checkpoint tests share: a temporary directory holding an
//! as-yet unwritten `leader-epoch-checkpoint` path.

use {
    super::{EpochEntry, LeaderEpochCheckpoint},
    krabka_ids::{LeaderEpoch, Offset},
    std::path::PathBuf,
    tempfile::TempDir,
};

pub(super) fn fresh() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader-epoch-checkpoint");
    (dir, path)
}

/// An on-disk checkpoint containing the explicit epoch/start pairs of a test.
pub(super) fn checkpoint(entries: &[(i32, i64)]) -> (TempDir, LeaderEpochCheckpoint) {
    let (dir, path) = fresh();
    let mut checkpoint = LeaderEpochCheckpoint::open(path).unwrap();
    for &(epoch, start) in entries {
        checkpoint
            .append(LeaderEpoch(epoch), Offset(start))
            .unwrap();
    }
    (dir, checkpoint)
}

pub(super) fn entry(epoch: i32, start: i64) -> EpochEntry {
    EpochEntry {
        epoch: LeaderEpoch(epoch),
        start_offset: Offset(start),
    }
}

pub(super) fn check_persisted(
    checkpoint: &LeaderEpochCheckpoint,
    expected: &[EpochEntry],
    name: &str,
) {
    assert2::check!(checkpoint.entries() == expected, "{name}");
    let reopened = LeaderEpochCheckpoint::open(checkpoint.path.clone()).unwrap();
    assert2::check!(reopened.entries() == expected, "{name} (reopened)");
}

pub(super) fn checkpoint_entries(entries: &[EpochEntry]) -> (TempDir, LeaderEpochCheckpoint) {
    let (dir, mut checkpoint) = checkpoint(&[]);
    for entry in entries {
        checkpoint.append(entry.epoch, entry.start_offset).unwrap();
    }
    (dir, checkpoint)
}
