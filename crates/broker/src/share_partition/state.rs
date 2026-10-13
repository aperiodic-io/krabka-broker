//! In-memory share-partition acquisition state machine (KIP-932).
//!
//! This is the pure core that the share-partition leader drives. It owns no
//! I/O. It tracks the per-offset delivery state over the live window
//! `[start_offset, end_offset)`, that is SPSO to SPEO, as a list of contiguous
//! `InFlightBatch` runs. It answers `acquire`, `acknowledge`, and
//! `expire_locks` queries. Bytes, logs, locks, and persistence live elsewhere.
//!
//! Delivery-state codes match Kafka's on-the-wire values: `DS_AVAILABLE=0`,
//! `DS_ACQUIRED=1`, `DS_ACKNOWLEDGED=2`, `DS_ARCHIVING=3` and
//! `DS_ARCHIVED=4`. `Acquired` is transient, and the machine persists it back
//! as `Available(0)`. `Deferred` is KFC-1's refinement of `Available` and
//! persists as `Available(0)` too, so scheduled delivery adds no code to the
//! wire encoding. `Archiving` is KIP-1191's first phase of the dead-letter
//! queue: a record that a `Reject` or the delivery limit would archive waits
//! there, not terminal, until its dead-letter record is written.
//!
//! This file holds the types and the constructor. The transitions sit in one
//! module per concern: `acquire` grows the window and hands records out,
//! `acknowledge` applies a consumer's verdict, `locks` runs the acquisition
//! lock lifetime, `deferral` implements KFC-1 scheduled delivery, `window`
//! keeps the run list in shape, and `persistence` maps to and from the share
//! coordinator's records.

use {krabka_log::Offset, std::time::Instant};

mod acknowledge;
mod acquire;
mod deferral;
mod dlq;
mod locks;
mod persistence;
mod window;

#[cfg(test)]
pub(crate) mod test_support;

/// Saturating `i64 -> i32` conversion for record counts. Offset ranges do not
/// overflow `i32` in practice, but the counter type is `i32` to match the
/// persister.
fn clamp_i32(n: i64) -> i32 { i32::try_from(n).unwrap_or(i32::MAX) }

pub const DS_AVAILABLE: i8 = 0;
pub const DS_ACQUIRED: i8 = 1;
pub const DS_ACKNOWLEDGED: i8 = 2;
pub const DS_ARCHIVING: i8 = 3;
pub const DS_ARCHIVED: i8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordState {
    Available,
    Acquired,
    Acknowledged,
    /// KIP-1191: on its way to `Archived` through the dead-letter queue. It is
    /// not terminal, so the SPSO waits behind it until the queue write is
    /// done.
    Archiving,
    Archived,
    /// KFC-1: `Available`, but the record's delivery time has not arrived.
    ///
    /// It has no delivery-state code of its own. The share coordinator stores
    /// it as `Available`, so a coordinator that reloads after a leader change
    /// re-derives the deferral against its own clock instead of inheriting the
    /// old leader's reading of a clock it cannot check.
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AckType {
    Gap,
    Accept,
    Release,
    Reject,
}

impl AckType {
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Gap),
            1 => Some(Self::Accept),
            2 => Some(Self::Release),
            3 => Some(Self::Reject),
            _ => None,
        }
    }
}

pub use self::{
    acquire::AcquireShape,
    dlq::{DlqCause, DlqRange},
};

/// A contiguous run of offsets acquired by a single `acquire` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredRange {
    pub first: Offset,
    pub last: Offset,
    pub delivery_count: i16,
}

/// One contiguous run of offsets `[first_offset, last_offset]` with the same
/// delivery state and delivery count. The lock fields have a meaning only
/// while `state == RecordState::Acquired`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InFlightBatch {
    first_offset: Offset,
    last_offset: Offset,
    state: RecordState,
    delivery_count: i16,
    acquired_by: Option<String>,
    lock_deadline: Option<Instant>,
}

impl InFlightBatch {
    fn len(&self) -> i64 { self.last_offset.0 - self.first_offset.0 + 1 }

    fn acquired_within(&self, first: Offset, last: Offset) -> bool {
        self.first_offset >= first
            && self.last_offset <= last
            && self.state == RecordState::Acquired
    }

    /// Complete an archive with the saturating accounting used by broker
    /// archives and dead-letter completion. Acknowledgements count separately.
    fn archive_terminal(&mut self, complete_count: &mut i32) {
        *complete_count = complete_count.saturating_add(clamp_i32(self.len()));
        self.state = RecordState::Archived;
    }

    /// Kafka's `SharePartition.isStateTerminal`: Acknowledged or Archived.
    fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            RecordState::Acknowledged | RecordState::Archived
        )
    }
}

/// The in-memory acquisition state for one share partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AcquisitionState {
    /// Share-partition start offset (SPSO): the lowest offset that is not yet
    /// terminally acknowledged or archived.
    pub start_offset: Offset,
    /// Share-partition end offset (SPEO): one past the highest materialized
    /// offset. It equals `start_offset` when the window is empty.
    pub end_offset: Offset,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub dirty: bool,
    /// Number of terminal records, Acknowledged or Archived, in the in-flight
    /// window at or above the SPSO: Kafka's
    /// `SharePartition.deliveryCompleteCount`, and the persister's
    /// `delivery_complete_count`. It is not a cumulative counter. It grows
    /// when a record in the window becomes terminal and shrinks when the SPSO
    /// moves past terminal records, so `end - start - count` is the lag that
    /// `DescribeShareGroupOffsets` reports.
    ///
    /// The window never keeps a record below the SPSO, so none of the special
    /// cases Kafka has for records below a start offset that the log start
    /// moved past (`numInFlightRecordsInBatch`) arise here.
    delivery_complete_count: i32,
    batches: Vec<InFlightBatch>,
    /// KIP-1191: whether a record that a `Reject` or the delivery limit would
    /// archive waits in `Archiving` for the dead-letter queue first. It
    /// follows the group's config, so the owner of the state sets it with
    /// [`AcquisitionState::set_dlq_enabled`] before each operation.
    dlq_enabled: bool,
    /// The `Archiving` runs whose dead-letter write has not started.
    pending_dlq: Vec<DlqRange>,
}

impl AcquisitionState {
    #[must_use]
    pub fn new(start_offset: Offset) -> Self {
        Self {
            start_offset,
            end_offset: start_offset,
            state_epoch: 0,
            leader_epoch: 0,
            dirty: false,
            delivery_complete_count: 0,
            batches: Vec::new(),
            dlq_enabled: false,
            pending_dlq: Vec::new(),
        }
    }
}

#[cfg(test)]
impl AcquisitionState {
    /// Test-only: the state of every offset in the window, in offset order.
    pub(crate) fn record_states(&self) -> Vec<(i64, RecordState)> {
        self.batches
            .iter()
            .flat_map(|b| (b.first_offset.0..=b.last_offset.0).map(move |offset| (offset, b.state)))
            .collect()
    }
}

#[cfg(test)]
#[path = "state_model.rs"]
mod state_model;
