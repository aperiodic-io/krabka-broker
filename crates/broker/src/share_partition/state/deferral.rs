//! KFC-1 scheduled delivery: holding a record back until its delivery time.
//!
//! `Deferred` is a refinement of `Available` that acquisition steps over, so a
//! share group reaches the due records sitting behind a waiting one. This
//! module holds the only two transitions in and out of it, `defer_internal` and
//! `promote_deferred`, and the `deferred_records` count the `ShareFetch`
//! handler uses to bound materialization. The state is derived from a clock
//! reading rather than owned by the machine, which is why nothing here marks
//! the state dirty and why it persists as `Available`.

use krabka_log::Offset;

use super::{AcquisitionState, InFlightBatch, RecordState};

impl AcquisitionState {
    /// Holds back an offset range whose KFC-1 delivery time has not arrived,
    /// so acquisition steps over it and reaches the due records behind it.
    ///
    /// Only an `Available` record defers. A record already handed out, or
    /// already terminal, is past the point where the schedule can hold it
    /// back, and a lock or an acknowledgement outranks a delivery time.
    ///
    /// This marks nothing dirty. `Deferred` persists as `Available`, so the
    /// projection the share coordinator writes is byte-identical either way,
    /// and a derived mark must never cost a coordinator write.
    pub fn defer_internal(&mut self, first: Offset, last: Offset) {
        if first > last {
            return;
        }
        self.split_at_offset(first);
        self.split_at_offset(last + 1);
        for batch in &mut self.batches {
            if batch.last_offset < first || batch.first_offset > last {
                continue;
            }
            if batch.state == RecordState::Available {
                batch.state = RecordState::Deferred;
            }
        }
        // Unconditional: a split that retagged nothing must not leave the
        // window fragmented.
        self.coalesce();
    }

    /// Returns every `Deferred` record to `Available`.
    ///
    /// This is the only route out of `Deferred`. The `ShareFetch` handler
    /// calls it at the start of each acquire pass and then re-derives the
    /// deferral from the log and the clock, so a deferral is never a cached
    /// decision that a later clock reading would overturn, and a batch becomes
    /// acquirable on the first pass after it activates.
    pub fn promote_deferred(&mut self) {
        for batch in &mut self.batches {
            if batch.state == RecordState::Deferred {
                batch.state = RecordState::Available;
            }
        }
        self.coalesce();
    }

    /// Count of offsets in the live window that the schedule holds back.
    ///
    /// The `ShareFetch` handler reads it to bound how far past a deferred run
    /// it materializes, so one far-future batch at the head of the window
    /// cannot pull the whole log into the window behind it.
    #[must_use]
    pub fn deferred_records(&self) -> i64 {
        self.batches
            .iter()
            .filter(|b| b.state == RecordState::Deferred)
            .map(InFlightBatch::len)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;

    use super::*;
    use crate::{
        share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, StateBatchSetup, state_batch,
        },
        share_partition::state::{
            AckType, AcquiredRange,
            test_support::{LOCK, t0},
        },
    };

    #[test]
    fn only_available_records_defer_and_promotion_is_the_way_back() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(6), 100); // [0,5] Available
        let _ = s.acquire("m1", 2, krabka_log::Offset(i64::MAX), t0(), LOCK, 5); // [0,1] Acquired
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Accept, 5)
            .unwrap(); // SPSO -> 2
        let _ = s.acquire("m1", 1, krabka_log::Offset(i64::MAX), t0(), LOCK, 5); // [2,2] Acquired
        s.archive_internal(Offset(3), Offset(3));

        s.defer_internal(Offset(2), Offset(5));

        // Only [4,5] was Available, so only [4,5] deferred.
        check!(s.deferred_records() == 2);
        check!(
            s.acquire("m2", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5)
                .is_empty()
        );
        // The acquired record is still m1's, which proves defer left it alone.
        s.acknowledge("m1", Offset(2), Offset(2), AckType::Accept, 5)
            .unwrap();

        s.promote_deferred();

        check!(s.deferred_records() == 0);
        check!(
            s.acquire("m2", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5)
                == vec![AcquiredRange {
                    first: Offset(4),
                    last: Offset(5),
                    delivery_count: 1,
                }]
        );
    }

    #[test]
    fn a_deferred_head_holds_the_spso_but_not_materialization() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 2); // [0,1]
        s.defer_internal(Offset(0), Offset(1));

        // A deferred run is not Available, so the window still grows.
        s.materialize(Offset(10), 2); // [2,3]

        check!(s.end_offset == 4);
        check!(
            s.acquire("m1", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5)
                == vec![AcquiredRange {
                    first: Offset(2),
                    last: Offset(3),
                    delivery_count: 1,
                }]
        );
        // Head-of-line order does not apply to a share group, but the deferred
        // head is not terminal, so the SPSO stays behind it.
        check!(s.start_offset == 0);
        check!(s.deferred_records() == 2);
    }

    #[test]
    fn deferral_persists_as_available_and_survives_a_reload() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        s.defer_internal(Offset(2), Offset(3));

        let (start, dcc, batches) = s.to_persist_batches();
        check!(start == 0);
        check!(dcc == 0);
        check!(
            batches
                == vec![
                    state_batch(StateBatchSetup {
                        bounds: Offset(0)..=Offset(1),
                        attempts: DeliveryAttemptCount(0),
                        ..Default::default()
                    }),
                    state_batch(StateBatchSetup {
                        bounds: Offset(2)..=Offset(3),
                        attempts: DeliveryAttemptCount(0),
                        ..Default::default()
                    }),
                ]
        );

        // The next leader reads back Available over the whole window and
        // re-derives the deferral against its own clock. No offset is lost.
        let mut reloaded = AcquisitionState::new(Offset(0));
        reloaded.load_from(start, 0, 0, &batches);
        check!(reloaded.start_offset == 0);
        check!(reloaded.end_offset == 4);
        check!(reloaded.deferred_records() == 0);
        check!(
            reloaded.acquire("m1", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5)
                == vec![AcquiredRange {
                    first: Offset(0),
                    last: Offset(3),
                    delivery_count: 1,
                }]
        );
    }

    #[test]
    fn a_deferred_record_is_not_archived_as_a_poison_pill() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(2), 100);
        // Burn both delivery attempts, so the record is at the archive limit.
        for _ in 0..2 {
            let _ = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 2);
            s.expire_locks(t0() + Duration::from_secs(31), i16::MAX);
        }
        s.defer_internal(Offset(0), Offset(1));

        check!(
            s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 2)
                .is_empty()
        );
        // Still deferred, not archived: nobody made a third attempt.
        check!(s.deferred_records() == 2);
        check!(s.start_offset == 0);
        check!(s.delivery_complete_count() == 0);
    }
}
