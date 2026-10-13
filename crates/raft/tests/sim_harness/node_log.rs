//! The pluggable per-node log the harness drives. The trait is its own file
//! because it is the seam that lets one scheduler run over both the in-memory
//! fake and a real on-disk `KraftLog`.

use krabka_raft::kraft::{
    event::LogEnd,
    types::{Epoch, LogView},
};

/// The log operations the harness does on a node's behalf. The trait abstracts
/// them, so the same scheduler drives both the in-memory fake and the real
/// on-disk `KraftLog`. Implementors are also [`LogView`]s, which are the
/// queries the core needs.
pub trait SimNodeLog: LogView {
    /// Appends `count` data records produced in `epoch`. These are the leader's
    /// own appends and the new-leader `LeaderChange` control record.
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize);

    /// Truncates the log so that exactly `offset` records remain.
    fn truncate_to(&mut self, offset: i64);

    /// Advances the log's own high-watermark bookkeeping to `hwm`, which is the
    /// consensus HWM the core has just computed. For the in-memory fake this is
    /// a no-op, because the harness mirrors the HWM separately. The real
    /// `KraftLog` uses it to gate committed reads. Default: no-op.
    fn advance_hwm(&mut self, hwm: i64) { let _ = hwm; }

    /// Replicates from `leader` into `self` and brings `self` byte-for-byte in
    /// line with the leader's log.
    ///
    /// The method first truncates any diverging or conflicting suffix that
    /// `self` holds and the leader does not. It then copies the suffix the
    /// follower is missing. The copy is epoch-faithful. The harness calls this
    /// method only when the leader is the genuine leader and neither endpoint is
    /// partitioned.
    fn replicate_from(&mut self, leader: &Self);

    /// The number of records in the log, which is its end offset as a `usize`.
    /// The convergence fingerprint uses it.
    fn record_count(&self) -> usize;

    /// The log tip as carried in Vote/Fetch requests.
    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}
