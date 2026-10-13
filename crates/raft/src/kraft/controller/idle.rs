//! KIP-835: the leader appends an empty `NoOpRecord` every
//! `metadata.max.idle.interval.ms`, as Kafka's `QuorumController` does with
//! its `writeNoOpRecord` periodic task.
//!
//! The records change no metadata. They keep the log moving while the cluster
//! is otherwise idle: the high watermark advances, a follower or broker can
//! tell that its view is current, and the active segment receives the appends
//! that roll it by `metadata.log.segment.ms`. Every replay skips them.

use {
    super::{
        Engine, KraftController,
        offsets::{leader_alone_is_majority, validate_append_result},
        records::{metadata_record_batch, noop_record_value},
    },
    crate::error::RaftError,
    krabka_units::prelude::TimeExt as _,
    tokio::time::Instant,
};

impl Engine {
    /// Arm the no-op timer while this node leads and the interval is not
    /// zero, and disarm it otherwise. A timer that is already armed keeps its
    /// deadline.
    pub fn reconcile_noop_timer(&mut self) {
        let interval = self.metadata_log.max_idle_interval.to_std();
        if !self.core.role().is_leader() || interval.is_zero() {
            self.noop_at = None;
        } else if self.noop_at.is_none() {
            self.noop_at = Some(Instant::now() + interval);
        }
    }

    /// The no-op timer fired: append one `NoOpRecord` and arm the timer for
    /// the next one.
    pub fn on_noop_timer(&mut self) {
        self.noop_at = None;
        if self.core.role().is_leader()
            && let Err(error) = self.append_noop()
        {
            tracing::warn!(?error, "kraft: append of a KIP-835 no-op record failed");
        }
        self.reconcile_noop_timer();
    }

    /// Append one empty `NoOpRecord` at the current leader epoch. A single
    /// voter commits it at once; a larger quorum commits it when the
    /// followers fetch it.
    ///
    /// # Errors
    /// Returns the [`RaftError`] of the batch encoding or the log append.
    pub fn append_noop(&mut self) -> Result<(), RaftError> {
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = metadata_record_batch(leader_epoch, &[noop_record_value()?])?;
        let expected_base = self.log.log_end_offset();
        let base = self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())?;
        validate_append_result("no-op", expected_base, base, self.log.log_end_offset())?;
        if leader_alone_is_majority(self.core.quorum_state().majority(), self.core.is_voter()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
        Ok(())
    }
}
