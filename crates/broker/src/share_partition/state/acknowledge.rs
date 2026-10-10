//! Terminal disposition of an acquired offset range.
//!
//! This module holds `acknowledge`, the step where a share consumer reports
//! what it did with the records it holds. `Accept` completes them, `Release`
//! puts them back for redelivery, and `Reject` and `Gap` archive them. It is
//! separate from acquisition because it is the only place that moves a record
//! into a terminal state on a consumer's word, and so the only place the
//! delivery-complete accounting grows outside an internal archive.

use krabka_log::Offset;

use super::{AckType, AcquisitionState, RecordState, clamp_i32, dlq::DlqCause, window::give_back};

impl AcquisitionState {
    /// Acknowledges the offset range `[first, last]` that `member` acquired
    /// earlier.
    ///
    /// A range below the SPSO is already done, and a range that starts below
    /// it is cut there, as `ack_bounds` says. `member`
    /// must currently hold the rest of the range as `Acquired`. The method
    /// splits the range into its own batches at the boundaries, then applies
    /// the acknowledgement. `Accept` gives Acknowledged. `Release` gives
    /// Available, clears the lock and the owner, and keeps `delivery_count`
    /// for redelivery, unless the count has reached `max_attempts`: then the
    /// records are archived at once, as Kafka's `InFlightState.tryUpdateState`
    /// does. `Reject` and `Gap` give Archived. With a dead-letter queue
    /// ([`AcquisitionState::set_dlq_enabled`]), a `Reject` and a `Release` at
    /// the limit give `Archiving` instead. The method then advances the
    /// SPSO over any new terminal prefix and marks the state dirty.
    ///
    /// # Errors
    ///
    /// `INVALID_REQUEST` for a range past the records ever handed out, and
    /// `INVALID_RECORD_STATE` for a range that `member` does not hold.
    pub fn acknowledge(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        ack: AckType,
        max_attempts: i16,
    ) -> Result<(), i16> {
        let Some((first, last)) = self.split_acquired_range(member, first, last)? else {
            return Ok(());
        };
        let (batches, mut archive) = self.runs_and_sink();
        for b in batches {
            if !b.acquired_within(first, last) {
                continue;
            }
            let n = clamp_i32(b.len());
            match ack {
                AckType::Accept => {
                    b.state = RecordState::Acknowledged;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    *archive.delivery_complete_count += n;
                }
                AckType::Release => {
                    // delivery_count retained: next acquire redelivers at +1.
                    give_back(b, max_attempts, &mut archive);
                }
                // Kafka's `recordStateWithDlq`: only a `Reject` goes through
                // the dead-letter queue. A gap has no record to send.
                AckType::Reject => archive.archive(b, DlqCause::ClientReject),
                AckType::Gap => {
                    b.state = RecordState::Archived;
                    b.acquired_by = None;
                    b.lock_deadline = None;
                    *archive.delivery_complete_count += n;
                }
            }
        }
        self.dirty = true;
        self.advance_spso();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::share_partition::state::{
        AcquiredRange,
        test_support::{LOCK, acquired_state, t0},
    };

    #[test]
    fn acquire_then_accept_advances_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100); // [0,4] Available
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(4),
                delivery_count: 1
            }]
        );
        s.acknowledge("m1", Offset(0), Offset(4), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 5);
    }

    #[test]
    fn release_redelivers_with_incremented_count() {
        let mut s = acquired_state(krabka_log::Offset(3));
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, 5)
            .unwrap();
        let acq2 = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq2[0].delivery_count == 2);
        // Released records stay in the window; SPSO did not advance.
        assert!(s.start_offset == 0);
    }

    #[test]
    fn partial_acknowledge_splits_a_batch() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100); // [0,9] Available
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq.len() == 1);
        // Accept only [0,3]; [4,9] remain Acquired.
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 4);
        // The remaining acquired range can still be acknowledged.
        s.acknowledge("m1", Offset(4), Offset(9), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 10);
    }

    #[test]
    fn reject_archives_and_advances_spso() {
        let mut s = acquired_state(krabka_log::Offset(3));
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Reject, 5)
            .unwrap();
        assert!(s.start_offset == 3); // archived prefix dropped
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq.is_empty()); // nothing left
    }

    #[test]
    fn gap_archives() {
        let mut s = acquired_state(krabka_log::Offset(2));
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Gap, 5)
            .unwrap();
        assert!(s.start_offset == 2);
        let (_start, dcc, batches) = s.to_persist_batches();
        assert!(batches.is_empty()); // archived prefix dropped from window
        // Both offsets became terminal, then left the window with the SPSO.
        assert!(dcc == 0);
    }

    #[test]
    fn acknowledge_wrong_member_is_invalid_record_state() {
        let mut s = acquired_state(krabka_log::Offset(3));
        let err = s.acknowledge("m2", Offset(0), Offset(2), AckType::Accept, 5);
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));
    }
}
