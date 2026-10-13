//! The one fixture the checkpoint tests share: a temporary directory holding an
//! as-yet unwritten `leader-epoch-checkpoint` path.

use {
    super::{EpochEntry, LeaderEpochCheckpoint},
    crate::test_support::TempPath,
    krabka_ids::{LeaderEpoch, Offset},
    tempfile::TempDir,
};

/// A checkpoint and the temporary directory that holds its file.
pub(super) struct TempCheckpoint {
    pub(super) dir: TempDir,
    pub(super) checkpoint: LeaderEpochCheckpoint,
}

pub(super) fn fresh() -> TempPath {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader-epoch-checkpoint");
    TempPath { dir, path }
}

/// An on-disk checkpoint containing the explicit epoch/start entries of a test.
pub(super) fn checkpoint(entries: &[EpochEntry]) -> TempCheckpoint {
    let TempPath { dir, path } = fresh();
    let mut checkpoint = LeaderEpochCheckpoint::open(path).unwrap();
    for entry in entries {
        checkpoint.append(entry.epoch, entry.start_offset).unwrap();
    }
    TempCheckpoint { dir, checkpoint }
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
