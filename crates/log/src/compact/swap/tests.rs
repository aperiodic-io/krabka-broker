//! Crash-point tests for [`atomic_swap`].
//!
//! Each case crashes the swap at one I/O operation, then loses any subset of
//! the directory operations since the last directory `fsync` -- the names a
//! real crash may or may not have made durable -- and reopens the directory
//! through recovery. Whatever the crash point and whatever was lost, the
//! directory must hold either every pre-compaction segment or the compacted
//! one, and the compacted one once the swap has committed.

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::Mutex,
};

use assert2::check;
use krabka_ids::Offset;

use super::atomic_swap;
use crate::{
    compact::{
        CleanedTransactionMetadata, RewriteOutput, RewriteRetention, build_offset_map,
        rewrite_segments,
        test_support::{RETENTION, make_record, round_over, write_sealed_segment},
    },
    error::LogError,
    io::{FileIo, IoTarget, LogIo},
    name,
    recovery::{
        deleted_orphan_recover,
        swap::{cleaned_path, recover_swaps, swap_path},
    },
    test_support::{Files, directory_files},
};

/// A directory operation a crash may lose, with what undoing it restores.
#[derive(Debug)]
enum Pending {
    Rename {
        from: PathBuf,
        to: PathBuf,
        displaced: Option<Vec<u8>>,
    },
    Remove {
        path: PathBuf,
        contents: Vec<u8>,
    },
}

#[derive(Debug, Default)]
struct CrashState {
    /// Operations performed so far, including the one that crashed.
    ops: Vec<String>,
    /// Directory operations since the last directory `fsync`.
    pending: Vec<Pending>,
}

/// A disk that dies at operation `crash_at`: that operation and every one
/// after it fail, and nothing they would have done happens.
#[derive(Debug)]
struct CrashingIo {
    crash_at: usize,
    state: Mutex<CrashState>,
}

impl CrashingIo {
    fn new(crash_at: usize) -> Self {
        Self {
            crash_at,
            state: Mutex::new(CrashState::default()),
        }
    }

    /// Record `op`, and fail it if the disk has died.
    fn step(&self, state: &mut CrashState, op: String) -> std::io::Result<()> {
        state.ops.push(op);
        if state.ops.len() > self.crash_at {
            Err(std::io::Error::other("crashed"))
        } else {
            Ok(())
        }
    }

    fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }

    fn ops(&self) -> Vec<String> {
        self.state.lock().unwrap().ops.clone()
    }

    /// Undo the pending directory operations whose bit is set in `lost`, the
    /// newest first.
    fn lose(&self, lost: u32) {
        let pending = std::mem::take(&mut self.state.lock().unwrap().pending);
        for (index, op) in pending.into_iter().enumerate().rev() {
            if lost & (1 << index) == 0 {
                continue;
            }
            match op {
                Pending::Rename {
                    from,
                    to,
                    displaced,
                } => {
                    std::fs::rename(&to, &from).unwrap();
                    if let Some(contents) = displaced {
                        std::fs::write(&to, contents).unwrap();
                    }
                }
                Pending::Remove { path, contents } => std::fs::write(path, contents).unwrap(),
            }
        }
    }
}

/// Whether `file`'s extension is one of `extensions`.
fn has_extension(file: &str, extensions: &[&str]) -> bool {
    Path::new(file)
        .extension()
        .is_some_and(|ext| extensions.iter().any(|wanted| ext == *wanted))
}

fn file_name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().into_owned()
}

impl LogIo for CrashingIo {
    fn sync_file(&self, _target: IoTarget, file: &File) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.step(&mut state, "sync_file".to_owned())?;
        file.sync_data()
    }

    fn sync_dir(&self, _dir: &Path) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.step(&mut state, "sync_dir".to_owned())?;
        state.pending.clear();
        Ok(())
    }

    fn rename(&self, _target: IoTarget, from: &Path, to: &Path) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        let op = format!("rename {} {}", file_name(from), file_name(to));
        self.step(&mut state, op)?;
        let displaced = std::fs::read(to).ok();
        std::fs::rename(from, to)?;
        state.pending.push(Pending::Rename {
            from: from.to_path_buf(),
            to: to.to_path_buf(),
            displaced,
        });
        Ok(())
    }

    fn remove_file(&self, _target: IoTarget, path: &Path) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.step(&mut state, format!("remove {}", file_name(path)))?;
        let contents = std::fs::read(path)?;
        std::fs::remove_file(path)?;
        state.pending.push(Pending::Remove {
            path: path.to_path_buf(),
            contents,
        });
        Ok(())
    }
}

/// A compactable directory: sealed segments at 0 and 10 that compaction
/// consumes, each with a stale `.txnindex`, and a segment at 20 it must not
/// touch. Returns the survivor as `.cleaned` files, the way the rewrite leaves
/// it, with a synthetic survivor `.txnindex` when `survivor_txnindex` is set.
fn compactable(dir: &Path, survivor_txnindex: bool) -> RewriteOutput {
    let rewrite = {
        let first = write_sealed_segment(
            dir,
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v1")),
            ],
        );
        let second = write_sealed_segment(
            dir,
            10,
            vec![
                make_record(0, Some(b"k1"), Some(b"v2")),
                make_record(1, Some(b"k3"), Some(b"v1")),
            ],
        );
        drop(write_sealed_segment(
            dir,
            20,
            vec![make_record(0, Some(b"k1"), Some(b"v3"))],
        ));
        let segments = vec![&first, &second];
        let map = build_offset_map(&segments, vec![], None).unwrap();
        let mut txn = CleanedTransactionMetadata::default();
        rewrite_segments(
            &FileIo,
            dir,
            &segments,
            &map,
            &mut txn,
            RewriteRetention {
                now_ms: 0,
                delete_retention: RETENTION,
            },
            round_over(&segments, &HashMap::new()),
        )
        .unwrap()
    };
    std::fs::write(name::txnindex_path(dir, 0), b"stale 0").unwrap();
    std::fs::write(name::txnindex_path(dir, 10), b"stale 10").unwrap();

    let to_cleaned = |from: &Path, ext: &str| {
        let to = cleaned_path(dir, 0, ext);
        if from != to {
            std::fs::rename(from, &to).unwrap();
        }
        to
    };
    let txnindex_swap = survivor_txnindex.then(|| {
        let path = cleaned_path(dir, 0, "txnindex");
        std::fs::write(&path, b"survivor").unwrap();
        path
    });
    RewriteOutput {
        log_swap: to_cleaned(&rewrite.log_swap, "log"),
        index_swap: to_cleaned(&rewrite.index_swap, "index"),
        timeindex_swap: to_cleaned(&rewrite.timeindex_swap, "timeindex"),
        new_base_offset: rewrite.new_base_offset,
        new_last_offset: rewrite.new_last_offset,
        txnindex_swap,
    }
}

/// What `Log::open` does to the directory before it reads a segment.
fn reopen(dir: &Path) -> Result<(), LogError> {
    recover_swaps(&FileIo, dir)?;
    deleted_orphan_recover(&FileIo, dir)
}

const CONSUMED: [Offset; 2] = [Offset(0), Offset(10)];

/// The directory before compaction, and after a clean compaction, both as
/// `Log::open` leaves them; and the operations a clean swap performs.
fn references(survivor_txnindex: bool) -> (Files, Files, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    compactable(dir.path(), survivor_txnindex);
    let before: Files = directory_files(dir.path())
        .into_iter()
        .filter(|(file, _)| !file.ends_with(".cleaned"))
        .collect();

    let dir = tempfile::tempdir().unwrap();
    let rewrite = compactable(dir.path(), survivor_txnindex);
    let io = CrashingIo::new(usize::MAX);
    atomic_swap(&io, dir.path(), &CONSUMED, &rewrite).unwrap();
    reopen(dir.path()).unwrap();
    (before, directory_files(dir.path()), io.ops())
}

#[test]
fn a_clean_swap_replaces_the_consumed_segments_with_the_survivor() {
    for survivor_txnindex in [false, true] {
        let (before, after, _) = references(survivor_txnindex);
        let segments = |listing: &Files| -> Vec<String> {
            listing
                .keys()
                .filter(|file| has_extension(file, &["log"]))
                .cloned()
                .collect()
        };
        check!(
            segments(&before)
                == [
                    "00000000000000000000.log",
                    "00000000000000000010.log",
                    "00000000000000000020.log"
                ]
        );
        check!(segments(&after) == ["00000000000000000000.log", "00000000000000000020.log"]);
        check!(after.get("00000000000000000020.log") == before.get("00000000000000000020.log"));
        let survivor_txn = after
            .get("00000000000000000000.txnindex")
            .map(Vec::as_slice);
        let expected: Option<&[u8]> = survivor_txnindex.then_some(b"survivor");
        check!(
            survivor_txn == expected,
            "stale .txnindex must never survive"
        );
        check!(
            !after
                .keys()
                .any(|file| has_extension(file, &["swap", "cleaned"]))
        );
    }
}

/// Crash at every operation, lose every subset of the unsynced directory
/// operations, and reopen: the log is always whole, and once the swap has
/// committed it is always the compacted one.
#[test]
fn every_crash_point_recovers_to_the_old_or_the_new_segments() {
    for survivor_txnindex in [false, true] {
        let (before, after, ops) = references(survivor_txnindex);
        // The first unlink of a consumed segment comes after the commit's
        // directory fsync; a crash from there on must complete the swap.
        let committed_from = ops
            .iter()
            .position(|op| op == "remove 00000000000000000000.log")
            .unwrap();
        for (crash_at, op) in ops.iter().enumerate() {
            let mut lost = 0u32;
            loop {
                let dir = tempfile::tempdir().unwrap();
                let rewrite = compactable(dir.path(), survivor_txnindex);
                let io = CrashingIo::new(crash_at);
                check!(atomic_swap(&io, dir.path(), &CONSUMED, &rewrite).is_err());
                let pending = io.pending_len();
                io.lose(lost);
                reopen(dir.path()).unwrap();

                let recovered = directory_files(dir.path());
                let case = format!(
                    "txnindex {survivor_txnindex}, crash at {crash_at} ({op}), lost {lost:b}"
                );
                if crash_at > committed_from {
                    check!(recovered == after, "{case}");
                } else {
                    check!(recovered == before || recovered == after, "{case}");
                }

                lost += 1;
                if lost >= 1 << pending {
                    break;
                }
            }
        }
    }
}

/// The finding this protocol exists for: the survivor's own base is still on
/// disk but a segment the swap consumed is already gone. Discarding the swap
/// here would lose that segment's records.
#[test]
fn a_committed_swap_is_completed_even_when_its_base_segment_survives() {
    let (_, after, _) = references(false);
    let dir = tempfile::tempdir().unwrap();
    let rewrite = compactable(dir.path(), false);
    // Commit by hand, then lose only the consumed segment at 10.
    for (from, ext) in [
        (&rewrite.index_swap, "index"),
        (&rewrite.timeindex_swap, "timeindex"),
        (&rewrite.log_swap, "log"),
    ] {
        std::fs::rename(from, swap_path(dir.path(), 0, ext)).unwrap();
    }
    for path in [
        name::log_path(dir.path(), 10),
        name::index_path(dir.path(), 10),
        name::timeindex_path(dir.path(), 10),
        name::txnindex_path(dir.path(), 10),
    ] {
        std::fs::remove_file(path).unwrap();
    }
    check!(name::log_path(dir.path(), 0).exists());

    reopen(dir.path()).unwrap();

    check!(directory_files(dir.path()) == after);
}

/// A failed unlink of a consumed segment fails the swap instead of being
/// ignored, and the reopened directory is the compacted one.
#[test]
fn a_failed_unlink_fails_the_swap_and_recovery_completes_it() {
    #[derive(Debug)]
    struct FailRemove;
    impl LogIo for FailRemove {
        fn remove_file(&self, _target: IoTarget, path: &Path) -> std::io::Result<()> {
            if path == name::log_path(path.parent().unwrap(), 10) {
                return Err(std::io::ErrorKind::PermissionDenied.into());
            }
            std::fs::remove_file(path)
        }
    }

    let (_, after, _) = references(false);
    let dir = tempfile::tempdir().unwrap();
    let rewrite = compactable(dir.path(), false);

    let error = atomic_swap(&FailRemove, dir.path(), &CONSUMED, &rewrite).unwrap_err();
    check!(let LogError::Io(_) = error);

    reopen(dir.path()).unwrap();
    check!(directory_files(dir.path()) == after);
}
