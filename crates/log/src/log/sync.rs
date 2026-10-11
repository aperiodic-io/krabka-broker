//! Explicit flush and `fsync` of the segment files and of the log
//! directory.
//!
//! A newly created segment name is durable only once the parent directory
//! is synced, so the log tracks that debt and pays it here rather than at
//! every site that creates a segment.

#[cfg(unix)]
use std::path::Path;

use krabka_ids::Offset;

use super::Log;
#[cfg(unix)]
use crate::io::LogIo;
use crate::{error::LogError, segment::Segment};

#[cfg(test)]
pub(crate) mod sync_observer {
    use std::cell::RefCell;
    #[cfg(unix)]
    use std::path::PathBuf;

    use krabka_ids::Offset;

    #[cfg(unix)]
    thread_local! {
        static DIR_SYNCS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
    }

    thread_local! {
        static SEGMENT_FLUSHES: RefCell<Vec<Offset>> = const { RefCell::new(Vec::new()) };
    }

    #[cfg(unix)]
    pub(super) fn take_dir_syncs() -> Vec<PathBuf> {
        DIR_SYNCS.take()
    }

    pub(crate) fn take_segment_flushes() -> Vec<Offset> {
        SEGMENT_FLUSHES.take()
    }

    #[cfg(unix)]
    pub(super) fn record_dir_sync(dir: PathBuf) {
        DIR_SYNCS.with_borrow_mut(|synced| synced.push(dir));
    }

    pub(super) fn record_segment_flush(base: Offset) {
        SEGMENT_FLUSHES.with_borrow_mut(|flushed| flushed.push(base));
    }
}

impl Log {
    /// Drain rollover work and `fsync` every segment to stable storage, independent of
    /// [`crate::LogConfig::flush_on_append`].
    ///
    /// This method also fsyncs the log directory after it creates a new
    /// segment file. The segment therefore stays reachable after a crash on
    /// filesystems that require a parent-directory fsync.
    ///
    /// # Errors
    /// Returns a [`LogError`] if the underlying segment or directory flush fails.
    pub fn sync(&mut self) -> Result<(), LogError> {
        self.rollover_flusher.finish()?;
        for segment in &mut self.segments {
            Self::segment_flush(segment)?;
        }
        self.active_segment_flush()?;
        if self.dir_sync_needed {
            // Rust's standard directory-open path is supported on Unix, where
            // syncing the parent makes newly-created segment names durable. On
            // Windows the platform provides no equivalent through `std`; the
            // segment, offset-index, and time-index handles above have still
            // been flushed with `sync_data`.
            #[cfg(unix)]
            Self::sync_log_dir(&*self.io, &self.dir)?;
            self.dir_sync_needed = false;
        }
        self.unflushed_messages = 0;
        self.last_flush = std::time::Instant::now();
        Ok(())
    }

    #[cfg(unix)]
    fn sync_log_dir(io: &dyn LogIo, dir: &Path) -> Result<(), LogError> {
        io.sync_dir(dir)?;
        #[cfg(test)]
        sync_observer::record_dir_sync(dir.to_path_buf());
        Ok(())
    }

    pub(super) fn active_segment_flush(&mut self) -> Result<(), LogError> {
        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        Self::segment_flush(active)
    }

    pub(super) fn rollback_failed_append(&mut self, base_offset: Offset) -> Result<(), LogError> {
        // Use the full log truncation path so rollback restores bytes, the
        // file cursor, every sidecar, producer/transaction state, cached
        // visibility frontiers, and the leader-epoch checkpoint together.
        self.truncate_to(base_offset)?;
        self.active_segment_flush()
    }

    fn segment_flush(segment: &mut Segment) -> Result<(), LogError> {
        #[cfg(test)]
        sync_observer::record_segment_flush(segment.base_offset());
        segment.flush()
    }
}

#[cfg(test)]
mod tests {
    use krabka_ids::Offset;

    use super::*;
    use crate::{
        log::test_support::{rolling_test_log, sample_batch},
        test_support::open_log,
    };

    #[test]
    fn sync_persists_appended_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = open_log(dir.path());
            log.append(&mut sample_batch(3)).unwrap();
            log.sync().unwrap(); // fsync without relying on flush_on_append
        }
        // Reopen from disk: the synced records are present.
        let log = open_log(dir.path());
        assert2::assert!(log.log_end_offset() == Offset(3));
    }

    #[cfg(unix)]
    #[test]
    fn sync_fsyncs_parent_dir_after_segment_lifecycle_events() {
        enum Case {
            InitialCreation,
            ReopenBeforePriorSync,
            Rollover,
        }

        for case in [
            Case::InitialCreation,
            Case::ReopenBeforePriorSync,
            Case::Rollover,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = match case {
                Case::InitialCreation => open_log(dir.path()),
                Case::ReopenBeforePriorSync => {
                    use crate::test_support::open_log;

                    drop(open_log(dir.path()));
                    open_log(dir.path())
                }
                Case::Rollover => {
                    let mut log = rolling_test_log(dir.path());
                    log.append(&mut sample_batch(1)).unwrap();
                    log.sync().unwrap();
                    log.append(&mut sample_batch(1)).unwrap();
                    log
                }
            };
            sync_observer::take_dir_syncs();

            log.sync().unwrap();

            assert2::assert!(sync_observer::take_dir_syncs() == vec![dir.path().to_path_buf()]);
        }
    }

    #[test]
    fn sync_flushes_sealed_and_active_segments_after_rollover() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = rolling_test_log(dir.path());
        log.append(&mut sample_batch(1)).unwrap();
        log.sync().unwrap();
        sync_observer::take_segment_flushes();

        log.append(&mut sample_batch(1)).unwrap();
        log.sync().unwrap();

        // The rollover worker flushed offset 0 before its snapshot. This
        // thread's explicit sync then flushes sealed and active segments.
        assert2::assert!(sync_observer::take_segment_flushes() == vec![Offset(0), Offset(1)]);
    }
}
