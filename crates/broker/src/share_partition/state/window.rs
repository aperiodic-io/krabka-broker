//! Geometry of the in-flight batch list that backs the acquisition window.
//!
//! This module holds the operations that reshape the run list without deciding
//! a delivery outcome. It splits a run at an offset boundary, merges neighbours
//! that agree on state, count, and lock, advances the share-partition start
//! offset over a terminal prefix, and answers whether one member holds a whole
//! offset range. The concern-specific modules call these helpers; none of them
//! is part of the public surface of `AcquisitionState`.

use {
    super::{
        AcquisitionState, InFlightBatch, RecordState, clamp_i32,
        dlq::{ArchiveSink, DlqCause},
    },
    crate::codes::{INVALID_RECORD_STATE, INVALID_REQUEST},
    krabka_log::Offset,
};

/// Gives an acquired run back: Kafka's `InFlightState.tryUpdateState` to
/// `AVAILABLE`, which archives the run instead when its delivery count has
/// reached `max_attempts`. The owner and the lock go either way.
///
/// It returns whether the run was archived. An archived run has moved from a
/// non-terminal state to a terminal one inside the window, so its offsets are
/// added to `delivery_complete_count`, as Kafka's `releaseAcquiredRecords` and
/// `releaseAcquisitionLockOnTimeout` add them. With a dead-letter queue the
/// run waits in `Archiving` instead, and counts when the queue is done with it.
pub(super) fn give_back(
    batch: &mut InFlightBatch,
    max_attempts: i16,
    archive: &mut ArchiveSink<'_>,
) -> bool {
    if batch.delivery_count >= max_attempts {
        archive.archive(batch, DlqCause::DeliveryCountExceeded);
        true
    } else {
        batch.acquired_by = None;
        batch.lock_deadline = None;
        batch.state = RecordState::Available;
        false
    }
}

impl AcquisitionState {
    /// Archive eligible runs, retaining the caller's range predicate and
    /// choice of whether to clear acquisition. Dirty marking and SPSO movement
    /// stay with the operation, including its behavior when no run changes.
    pub(super) fn archive_matching(
        &mut self,
        mut eligible: impl FnMut(&InFlightBatch) -> bool,
        clear_acquisition: bool,
    ) -> bool {
        let mut changed = false;
        for batch in &mut self.batches {
            if eligible(batch) {
                batch.archive_terminal(&mut self.delivery_complete_count);
                if clear_acquisition {
                    batch.acquired_by = None;
                    batch.lock_deadline = None;
                }
                changed = true;
            }
        }
        changed
    }

    /// Split before archiving, then apply the caller's unchanged-run coalescing policy.
    pub(super) fn archive_range(
        &mut self,
        first: Offset,
        last: Offset,
        eligible: impl FnMut(&InFlightBatch) -> bool,
        clear_acquisition: bool,
        coalesce_unchanged: bool,
    ) {
        if !self.split_valid_range(first, last) {
            return;
        }
        if self.archive_matching(eligible, clear_acquisition) {
            self.dirty = true;
            self.advance_spso();
        } else if coalesce_unchanged {
            self.coalesce();
        }
    }

    /// Carve an inclusive nonempty range without changing its delivery state.
    pub(super) fn split_valid_range(&mut self, first: Offset, last: Offset) -> bool {
        if first > last {
            return false;
        }
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        true
    }

    /// One past the last offset that was ever handed out: the end of the last
    /// run that is not a never-delivered `Available` or `Deferred` run.
    ///
    /// Kafka's `SharePartition` caches a batch only once it is acquired, so
    /// this is where its cached state ends. The window can run further,
    /// because materialization adds records before any member takes them.
    fn in_flight_end(&self) -> Option<Offset> {
        self.batches
            .iter()
            .rev()
            .find(|b| {
                !(matches!(b.state, RecordState::Available | RecordState::Deferred)
                    && b.delivery_count == 0)
            })
            .map(|b| b.last_offset + 1)
    }

    /// The part of `[first, last]` that an acknowledgement applies to, as
    /// Kafka's `SharePartition.acknowledge` and
    /// `fetchSubMapForAcknowledgementBatch` find it.
    ///
    /// A range that ends below the SPSO is already done and yields `None`. A
    /// range that starts below it is cut at the SPSO.
    ///
    /// # Errors
    ///
    /// `INVALID_RECORD_STATE` when nothing was ever handed out, and
    /// `INVALID_REQUEST` when the range runs past the last offset that was.
    pub(crate) fn ack_bounds(
        &self,
        first: Offset,
        last: Offset,
    ) -> Result<Option<(Offset, Offset)>, i16> {
        if last < self.start_offset {
            return Ok(None);
        }
        let end = self.in_flight_end().ok_or(INVALID_RECORD_STATE)?;
        if last >= end {
            return Err(INVALID_REQUEST);
        }
        Ok(Some((first.max(self.start_offset), last)))
    }

    /// Validates an acquired acknowledgement range and carves out its boundaries.
    ///
    /// # Errors
    /// Returns the acknowledgement range error or `INVALID_RECORD_STATE` for another owner.
    pub(super) fn split_acquired_range(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
    ) -> Result<Option<(Offset, Offset)>, i16> {
        if first > last {
            return Err(INVALID_RECORD_STATE);
        }
        let Some((first, last)) = self.ack_bounds(first, last)? else {
            return Ok(None);
        };
        if !self.range_acquired_by(member, first, last) {
            return Err(INVALID_RECORD_STATE);
        }
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        Ok(Some((first, last)))
    }

    /// True if and only if `member` currently holds every offset in
    /// `[first, last]` as Acquired.
    pub(super) fn range_acquired_by(&self, member: &str, first: Offset, last: Offset) -> bool {
        let mut cursor = first;
        for b in &self.batches {
            if b.last_offset < first || b.first_offset > last {
                continue;
            }
            // The covered batches must be contiguous from `first`.
            if b.first_offset > cursor {
                return false;
            }
            if b.state != RecordState::Acquired || b.acquired_by.as_deref() != Some(member) {
                return false;
            }
            cursor = b.last_offset + 1;
            if cursor > last {
                break;
            }
        }
        cursor > last
    }

    /// Splits the batch at index `i` so that `split` becomes the first offset
    /// of a new trailing batch. It does nothing when `split` is at a
    /// boundary.
    pub(super) fn split_at(&mut self, i: usize, split: Offset) {
        let b = &self.batches[i];
        if split <= b.first_offset || split > b.last_offset {
            return;
        }
        let tail = InFlightBatch {
            first_offset: split,
            last_offset: b.last_offset,
            state: b.state,
            delivery_count: b.delivery_count,
            acquired_by: b.acquired_by.clone(),
            lock_deadline: b.lock_deadline,
        };
        self.batches[i].last_offset = split - 1;
        self.batches.insert(i + 1, tail);
    }

    /// Splits whichever batch holds the boundary, so that `offset` becomes a
    /// batch `first_offset`. It does nothing when `offset` already lands on a
    /// boundary, and when `offset` is outside the window.
    pub(super) fn split_at_offset(&mut self, offset: Offset) {
        if let Some(i) = self.batches.iter().position(|b| {
            b.first_offset
                .0
                .checked_add(1)
                .is_some_and(|first_split_offset| {
                    (first_split_offset..=b.last_offset.0).contains(&offset.0)
                })
        }) {
            self.split_at(i, offset);
        }
    }

    /// Advances the SPSO over any terminal prefix, that is Acknowledged or
    /// Archived, and drops those batches. It then merges adjacent same-state
    /// neighbors.
    ///
    /// The dropped records leave the in-flight window, so they leave
    /// `delivery_complete_count` too, as Kafka's
    /// `maybeUpdateCachedStateAndOffsets` subtracts the terminal records that
    /// `findLastOffsetAcknowledgedAndMetadata` counts below the new start
    /// offset.
    pub(super) fn advance_spso(&mut self) {
        while let Some(b) = self.batches.first() {
            if b.first_offset == self.start_offset && b.is_terminal() {
                self.start_offset = b.last_offset + 1;
                self.delivery_complete_count = self
                    .delivery_complete_count
                    .saturating_sub(clamp_i32(b.len()));
                self.batches.remove(0);
            } else {
                break;
            }
        }
        if self.end_offset < self.start_offset {
            self.end_offset = self.start_offset;
        }
        self.coalesce();
    }

    /// Merges adjacent batches that have the same delivery state, delivery
    /// count, and acquisition, that is the same owner and deadline. This keeps
    /// the batch list compact.
    pub(super) fn coalesce(&mut self) {
        let mut i = 0;
        while i + 1 < self.batches.len() {
            let mergeable = {
                let a = &self.batches[i];
                let b = &self.batches[i + 1];
                a.last_offset + 1 == b.first_offset
                    && a.state == b.state
                    && a.delivery_count == b.delivery_count
                    && a.acquired_by == b.acquired_by
                    && a.lock_deadline == b.lock_deadline
            };
            if mergeable {
                let next_last = self.batches[i + 1].last_offset;
                self.batches[i].last_offset = next_last;
                self.batches.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }
}
