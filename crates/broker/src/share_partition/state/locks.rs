//! The lifetime of an acquisition lock: renewal, expiry, and member release.
//!
//! A record handed to a share consumer stays out of circulation only while its
//! lock holds. This module holds the three ways that ends: `renew` pushes the
//! deadline out, `expire_locks` sweeps the deadlines that have passed, and
//! `release_member` gives back everything one member holds when its session
//! closes. All three return records to `Available` and keep `delivery_count`,
//! so the next hand-out counts as a redelivery.

use {
    super::{AcquisitionState, InFlightBatch, RecordState, window::give_back},
    krabka_log::Offset,
    std::time::{Duration, Instant},
};

impl AcquisitionState {
    /// Renews the acquisition lock on the range `[first, last]` that `member`
    /// holds. It resets each covered Acquired batch's `lock_deadline` to
    /// `now + lock_dur`. This is the KIP-932 RENEW acknowledgement.
    ///
    /// `member` must currently hold the whole range as `Acquired`. If it does
    /// not, this method returns `Err(INVALID_RECORD_STATE)`. The state, the
    /// owner, and `delivery_count` do not change. Only the deadline moves. The
    /// method does NOT advance the SPSO, because a renew keeps records in
    /// flight. It marks the state dirty.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn renew(
        &mut self,
        member: &str,
        first: Offset,
        last: Offset,
        now: Instant,
        lock_dur: Duration,
    ) -> Result<(), i16> {
        let Some((first, last)) = self.split_acquired_range(member, first, last)? else {
            return Ok(());
        };
        let new_deadline = now + lock_dur;
        for b in &mut self.batches {
            if !b.acquired_within(first, last) {
                continue;
            }
            b.lock_deadline = Some(new_deadline);
        }
        self.dirty = true;
        Ok(())
    }

    /// Returns any Acquired batch whose lock has expired to Available. It
    /// clears the lock and the owner but keeps `delivery_count`, so the next
    /// acquire counts as a redelivery. A batch whose count has reached
    /// `max_attempts` is archived at once instead, as Kafka's
    /// `releaseAcquisitionLockOnTimeout` does. It marks the state dirty when
    /// something changed.
    pub fn expire_locks(&mut self, now: Instant, max_attempts: i16) {
        self.release_matching(max_attempts, |batch| {
            batch.state == RecordState::Acquired
                && batch.lock_deadline.is_some_and(|deadline| now >= deadline)
        });
    }

    /// Releases every record currently acquired by `member` back to
    /// `Available`. The delivery count is retained for the next delivery, and
    /// a record whose count has reached `max_attempts` is archived instead,
    /// as Kafka's `SharePartition.releaseAcquiredRecords` does.
    ///
    /// Session close and connection disconnect call this method so records do
    /// not remain locked until their timeout after the consumer is gone.
    pub fn release_member(&mut self, member: &str, max_attempts: i16) {
        self.release_matching(max_attempts, |batch| {
            batch.state == RecordState::Acquired && batch.acquired_by.as_deref() == Some(member)
        });
    }

    fn release_matching(
        &mut self,
        max_attempts: i16,
        mut matches: impl FnMut(&InFlightBatch) -> bool,
    ) {
        let mut changed = false;
        let (batches, mut archive) = self.runs_and_sink();
        for batch in batches {
            if matches(batch) {
                give_back(batch, max_attempts, &mut archive);
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.advance_spso();
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_partition::state::{
            AckType, AcquiredRange,
            test_support::{LOCK, t0},
        },
        assert2::assert,
    };

    #[test]
    fn session_release_makes_only_members_records_available() {
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(4), 100);
        let _ = state.acquire("m1", 2, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        let _ = state.acquire("m2", 2, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);

        state.release_member("m1", 5);

        let reacquired = state.acquire("m3", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(reacquired.len() == 1);
        assert!(reacquired[0].first == Offset(0));
        assert!(reacquired[0].last == Offset(1));
        assert!(reacquired[0].delivery_count == 2);
    }

    #[test]
    fn expire_locks_reverts_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let _ = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        // Before expiry: re-acquire finds nothing (all Acquired).
        let none = s.acquire("m2", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(none.is_empty());
        s.expire_locks(t0() + Duration::from_secs(31), 5);
        // Now another member can acquire; redelivery bumps the count.
        let acq = s.acquire(
            "m2",
            10,
            krabka_log::Offset(i64::MAX),
            t0() + Duration::from_secs(31),
            LOCK,
            5,
        );
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(0),
                last: Offset(3),
                delivery_count: 2
            }]
        );
        assert!(s.start_offset == 0);
    }

    #[test]
    fn renew_extends_lock_keeping_acquired() {
        // F1: acquire with a short lock, renew with a longer one. An
        // `expire_locks` at the ORIGINAL deadline must not release the records.
        let t0 = Instant::now();
        let short = Duration::from_secs(10);
        let long = Duration::from_mins(1);

        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(4), 100);
        let acq = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0, short, 5);
        assert!(acq.len() == 1);
        let original_deadline = t0 + short;

        // Renew extends the lock well past the original deadline.
        s.renew("m1", Offset(0), Offset(3), t0, long).unwrap();
        assert!(s.dirty);

        // Sweeping at the original deadline must NOT release the renewed lock.
        s.expire_locks(original_deadline, 5);
        // Still Acquired by m1 -> a different member acquires nothing.
        let none = s.acquire(
            "m2",
            10,
            krabka_log::Offset(i64::MAX),
            original_deadline,
            short,
            5,
        );
        assert!(none.is_empty());
        // And m1 can still acknowledge it (proves it stayed Acquired by m1).
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
            .unwrap();
        assert!(s.start_offset == 4);
    }

    #[test]
    fn renew_on_unacquired_range_is_invalid_record_state() {
        let t0 = Instant::now();
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(3), 100);
        let _ = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0, LOCK, 5);
        // Wrong member.
        let err = s.renew("m2", Offset(0), Offset(2), t0, LOCK);
        assert!(err == Err(crate::codes::INVALID_RECORD_STATE));

        // Non-Acquired range: release [0,2] back to Available, then renew fails.
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Release, 5)
            .unwrap();
        let err2 = s.renew("m1", Offset(0), Offset(2), t0, LOCK);
        assert!(err2 == Err(crate::codes::INVALID_RECORD_STATE));
    }
}
