//! Growth of the acquisition window and the hand-out of records to a member.
//!
//! This module holds the three steps that put records in front of a share
//! consumer: `materialize` extends the live window with newly produced records,
//! `archive_internal` takes broker-internal offsets such as transaction control
//! batches out of it, and `acquire` walks the window and locks the available
//! runs for one member. The poison-pill rule, which archives a record that has
//! reached the delivery-attempt limit instead of giving it out again, lives
//! here because `acquire` is the only place that counts an attempt.

use {
    super::{AcquiredRange, AcquisitionState, InFlightBatch, RecordState, dlq::DlqCause},
    krabka_log::Offset,
    std::time::{Duration, Instant},
};

/// The smallest delivery count at which Kafka throttles a run:
/// `SharePartition.MINIMUM_THROTTLE_RECORDS_DELIVERY_LIMIT`.
const MINIMUM_THROTTLE_LIMIT: i16 = 2;

/// Kafka's `SharePartition.throttleRecordsDeliveryLimit`:
/// `max(2, ceil(max_attempts / 2))`.
fn throttle_limit(max_attempts: i16) -> i16 {
    MINIMUM_THROTTLE_LIMIT.max(max_attempts / 2 + max_attempts % 2)
}

/// How much one [`AcquisitionState::acquire_shaped`] pass may take.
#[derive(Debug, Clone, Copy)]
pub struct AcquireShape<'a> {
    /// The records the pass may take: a hard limit in `record_limit` mode and
    /// a soft one in `batch_optimized` mode.
    pub max_records: i32,
    /// The last offset of each log batch that the read returned, ascending.
    /// `Some` selects `batch_optimized` mode.
    pub batch_ends: Option<&'a [Offset]>,
}

impl AcquisitionState {
    /// Extends the live window with newly produced records.
    ///
    /// When no `Available` record remains in the window and the log has
    /// advanced past `end_offset`, this method appends one `Available` batch
    /// that spans `[end_offset, min(hwm-1, end_offset + room - 1)]` and
    /// advances `end_offset`. `room` is what is left of `max_record_locks`,
    /// the group's `share.partition.max.record.locks`, once the records that
    /// are in flight are counted: Kafka's `SharePartition.numInFlightRecords`
    /// is `endOffset - startOffset + 1`, and `canAcquireRecords` and
    /// `lastOffsetAndMaxRecordsToAcquire` stop the partition from taking
    /// records once it reaches the limit. With no room, the window does not
    /// grow until an acknowledgement, a release or a lock timeout moves the
    /// SPSO.
    ///
    /// A `Deferred` record is not `Available`, so a window that holds only
    /// records the schedule has not released yet does not block this method.
    /// That is what lets a share group reach a due record sitting behind a
    /// waiting one. The caller bounds how far it goes, because a deferred run
    /// is not in flight and so does not spend the in-flight budget itself.
    pub fn materialize(&mut self, hwm: Offset, max_record_locks: i32) {
        if self.has_available() || self.end_offset >= hwm {
            return;
        }
        let room = i64::from(max_record_locks.max(1)) - self.in_flight_records();
        if room <= 0 {
            return;
        }
        let last = (hwm - 1).min(self.end_offset + room - 1);
        self.batches.push(InFlightBatch {
            first_offset: self.end_offset,
            last_offset: last,
            state: RecordState::Available,
            delivery_count: 0,
            acquired_by: None,
            lock_deadline: None,
        });
        self.end_offset = last + 1;
        self.coalesce();
    }

    /// The records that count against the record lock limit: the whole window
    /// `[start_offset, end_offset)` except the runs that the schedule holds
    /// back, which no member holds a lock on.
    fn in_flight_records(&self) -> i64 {
        self.end_offset.0 - self.start_offset.0 - self.deferred_records()
    }

    /// True when the window holds an `Available` record.
    #[must_use]
    pub fn has_available(&self) -> bool {
        self.batches
            .iter()
            .any(|b| b.state == RecordState::Available)
    }

    /// Archives an internal log offset range so it can never be delivered to
    /// a share consumer.
    ///
    /// Transaction control batches occupy offsets in the partition log but
    /// are broker metadata, not user records. The `ShareFetch` handler calls
    /// this after materialization for every control-batch range in the live
    /// window. Under `read_committed` it also calls it for the data batches
    /// of aborted transactions.
    pub fn archive_internal(&mut self, first: Offset, last: Offset) {
        self.archive_range(
            first,
            last,
            |batch| {
                batch.last_offset >= first && batch.first_offset <= last && !batch.is_terminal()
            },
            true,
            false,
        );
    }

    /// Kafka's `SharePartition.updateCacheAndOffsets`, called when the log
    /// start offset has moved past the SPSO: by retention, `DeleteRecords`, or
    /// a `share.auto.offset.reset=earliest` initialization that is old.
    ///
    /// Archives every `Available` or `Deferred` run below `log_start`, exactly
    /// as `archiveAvailableRecordsOnLsoMovement` does, and then advances the
    /// SPSO. An `Acquired` run below `log_start` is not force-archived: it
    /// stays locked until its lock naturally expires, so the walk stops there
    /// instead of jumping past it. When nothing in the window blocks the
    /// advance, the SPSO (and the SPEO, if the window had not grown that far)
    /// jump straight to `log_start`, matching Kafka's unconditional
    /// `startOffset = newLogStartOffset` assignment for the un-cached region.
    ///
    /// Does nothing when `log_start` is at or below the current SPSO.
    pub fn advance_past_log_start(&mut self, log_start: Offset) {
        if log_start <= self.start_offset {
            return;
        }
        self.split_at_offset(log_start);
        let mut changed = false;
        for batch in &mut self.batches {
            if batch.first_offset >= log_start {
                break;
            }
            if matches!(batch.state, RecordState::Available | RecordState::Deferred) {
                batch.archive_terminal(&mut self.delivery_complete_count);
                batch.acquired_by = None;
                batch.lock_deadline = None;
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
        }
        self.advance_spso();
        // Nothing left in the window blocks the rest of the advance (either no
        // batch ever tracked that region, or every batch there was archived
        // above and `advance_spso` already dropped it): jump the SPSO the rest
        // of the way to `log_start`, and the SPEO with it if it had not
        // reached there yet.
        let blocked = self
            .batches
            .first()
            .is_some_and(|b| b.first_offset < log_start);
        if !blocked && self.start_offset < log_start {
            self.start_offset = log_start;
            self.dirty = true;
        }
        if self.end_offset < self.start_offset {
            self.end_offset = self.start_offset;
        }
    }

    /// Archives every `Available` run that has reached `max_attempts`
    /// deliveries, and advances the SPSO past a terminal prefix.
    ///
    /// The handler runs this before it materializes and reads. An exhausted
    /// run is `Available`, so it would stop `materialize` from growing the
    /// window, and the read starts past it, so `acquire` would never meet it
    /// and archive it.
    pub fn archive_exhausted(&mut self, max_attempts: i16) {
        let mut changed = false;
        let (batches, mut archive) = self.runs_and_sink();
        for batch in batches {
            if batch.state == RecordState::Available && batch.delivery_count >= max_attempts {
                archive.archive(batch, DlqCause::DeliveryCountExceeded);
                changed = true;
            }
        }
        if changed {
            self.dirty = true;
            self.advance_spso();
        }
    }

    /// The first offset that `acquire` can hand out: the start of the first
    /// `Available` run whose delivery count is under `max_attempts`.
    ///
    /// The handler starts its log read here, so the bytes it reads begin with
    /// a record it can acquire. It returns `None` when no such run exists.
    #[must_use]
    pub fn first_acquirable_offset(&self, max_attempts: i16) -> Option<Offset> {
        self.batches
            .iter()
            .find(|b| b.state == RecordState::Available && b.delivery_count < max_attempts)
            .map(|b| b.first_offset)
    }

    /// Acquires up to `max_records` Available records for `member`. It walks
    /// the window from `start_offset`.
    ///
    /// An Available batch whose `delivery_count >= max_attempts` is a bad
    /// record. This method archives it, does not give it out, and advances the
    /// SPSO past it. An Available batch under the limit moves to Acquired. The
    /// method splits the batch when it would exceed `max_records`, sets
    /// `acquired_by` and `lock_deadline`, adds 1 to `delivery_count`, and adds
    /// the batch to the returned ranges. The walk stops once it has acquired
    /// `max_records`.
    ///
    /// A `Deferred` batch is not `Available`, so the walk steps over it as it
    /// steps over an acquired or a terminal one, and it keeps its
    /// `delivery_count`. A record the schedule holds back is therefore never
    /// archived as a poison pill for an attempt nobody made.
    ///
    /// The walk acquires no offset past `last_offset`. The handler reads the
    /// log first and passes the last offset of the bytes that the read
    /// returned, so every acquired offset has its record in the response, as
    /// Kafka's `SharePartition.acquire` bounds the acquisition by the last
    /// fetched batch. An `Available` run that crosses `last_offset` is split,
    /// and its tail stays `Available`.
    pub fn acquire(
        &mut self,
        member: &str,
        max_records: i32,
        last_offset: Offset,
        now: Instant,
        lock_dur: Duration,
        max_attempts: i16,
    ) -> Vec<AcquiredRange> {
        self.acquire_shaped(
            member,
            AcquireShape {
                max_records,
                batch_ends: None,
            },
            last_offset,
            (now, lock_dur),
            max_attempts,
        )
    }

    /// [`AcquisitionState::acquire`] with Kafka's acquire mode.
    ///
    /// With `batch_ends`, the `batch_optimized` mode: `max_records` is a soft
    /// limit, and a run that it cuts is cut at the end of the log batch that
    /// holds the cut, so a whole batch goes out, as Kafka's
    /// `lastOffsetFromBatchWithRequestOffset` does. Without it, the
    /// `record_limit` mode: at most `max_records` records.
    ///
    /// Either way, Kafka's delivery throttle applies to a run whose delivery
    /// count has reached `max(2, ceil(max_attempts / 2))`
    /// (`SharePartition.throttleRecordsDeliveryLimit`): it goes out only as
    /// the first thing of a pass, a part of it whose size halves with each
    /// further delivery, and a single record on its last attempt, so one bad
    /// record does not take a whole batch to the delivery limit. Nothing is
    /// acquired after it.
    pub fn acquire_shaped(
        &mut self,
        member: &str,
        shape: AcquireShape<'_>,
        last_offset: Offset,
        (now, lock_dur): (Instant, Duration),
        max_attempts: i16,
    ) -> Vec<AcquiredRange> {
        let throttle_limit = throttle_limit(max_attempts);
        let mut acquired = Vec::new();
        let mut remaining = i64::from(shape.max_records.max(0));
        let mut i = 0;
        let mut any_change = false;
        while i < self.batches.len() {
            if remaining <= 0 {
                break;
            }
            if self.batches[i].first_offset > last_offset {
                break;
            }
            if self.batches[i].state != RecordState::Available {
                i += 1;
                continue;
            }
            // Poison pill: archive without handing out.
            if self.batches[i].delivery_count >= max_attempts {
                let (batches, mut archive) = self.runs_and_sink();
                archive.archive(&mut batches[i], DlqCause::DeliveryCountExceeded);
                any_change = true;
                i += 1;
                continue;
            }
            let first = self.batches[i].first_offset;
            let delivery_count = self.batches[i].delivery_count;
            let throttled = delivery_count >= throttle_limit;
            if throttled && !acquired.is_empty() {
                break;
            }
            let take = if throttled {
                let halvings = u32::try_from(delivery_count - throttle_limit + 1).unwrap_or(63);
                let portion = (self.batches[i].len() >> halvings.min(63)).max(1);
                let alone = max_attempts > 2 && delivery_count == max_attempts - 1;
                if alone { 1 } else { portion.min(remaining) }
            } else {
                remaining
            };
            // Split where the budget ends, or at the end of the log batch that
            // holds that point, and never past the last offset of the read.
            let mut split_at = first + take;
            if !throttled && let Some(ends) = shape.batch_ends {
                let cut = split_at - 1;
                if let Some(end) = ends.iter().copied().find(|end| *end >= cut) {
                    split_at = end + 1;
                }
            }
            let split_at = split_at.min(Offset(last_offset.0.saturating_add(1)));
            self.split_at(i, split_at);
            let b = &mut self.batches[i];
            b.state = RecordState::Acquired;
            b.delivery_count += 1;
            b.acquired_by = Some(member.to_string());
            b.lock_deadline = Some(now + lock_dur);
            acquired.push(AcquiredRange {
                first: b.first_offset,
                last: b.last_offset,
                delivery_count: b.delivery_count,
            });
            remaining -= b.len();
            any_change = true;
            i += 1;
            if throttled {
                break;
            }
        }
        if any_change {
            self.dirty = true;
            self.advance_spso();
        }
        acquired
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_partition::state::{
            AckType,
            test_support::{LOCK, t0},
        },
        assert2::{assert, check},
    };

    #[test]
    fn delivery_limit_archives_poison_pill() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(1), 100);
        for _ in 0..2 {
            // max_attempts = 2
            let _ = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 2);
            s.expire_locks(t0() + Duration::from_secs(31), 2);
        }
        let acq = s.acquire(
            "m1",
            10,
            krabka_log::Offset(i64::MAX),
            t0() + Duration::from_secs(62),
            LOCK,
            2,
        );
        assert!(acq.is_empty()); // archived, not redelivered
        assert!(s.start_offset == 1); // SPSO advanced past the poison pill
    }

    #[test]
    fn internal_offsets_are_archived_before_acquire() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100);
        s.archive_internal(Offset(2), Offset(2));

        let acquired = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acquired
                == vec![
                    AcquiredRange {
                        first: Offset(0),
                        last: Offset(1),
                        delivery_count: 1,
                    },
                    AcquiredRange {
                        first: Offset(3),
                        last: Offset(4),
                        delivery_count: 1,
                    },
                ]
        );
        check!(s.delivery_complete_count() == 1);
    }

    #[test]
    fn materialize_respects_max_inflight() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(100), 10); // hwm far ahead, but cap at 10 in flight
        assert!(s.end_offset == 10);
        let acq = s.acquire("m1", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq[0].first == 0);
        assert!(acq[0].last == 9);
    }

    /// Kafka's `SharePartition.numInFlightRecords` counts the window from the
    /// SPSO to the SPEO, and a partition at the limit takes no new records
    /// until an acknowledgement moves the SPSO.
    #[test]
    fn materialize_stops_at_the_record_lock_limit_until_the_spso_moves() {
        let mut s = AcquisitionState::new(Offset(0));
        let mut window_ends = Vec::new();

        s.materialize(Offset(100), 10);
        window_ends.push(s.end_offset.0);
        let _ = s.acquire("m1", 100, Offset(i64::MAX), t0(), LOCK, 5);
        // Every record is locked and the log has more: the window stays put,
        // however many times the member fetches again without acknowledging.
        for _ in 0..3 {
            s.materialize(Offset(100), 10);
            window_ends.push(s.end_offset.0);
        }
        // Releasing a record keeps it in the window: still full.
        s.acknowledge("m1", Offset(3), Offset(3), AckType::Release, 5)
            .unwrap();
        let _ = s.acquire("m1", 100, Offset(i64::MAX), t0(), LOCK, 5);
        s.materialize(Offset(100), 10);
        window_ends.push(s.end_offset.0);
        // An acknowledgement of the prefix moves the SPSO and makes room for
        // exactly the records it freed.
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
            .unwrap();
        s.materialize(Offset(100), 10);
        window_ends.push(s.end_offset.0);

        assert!(window_ends == vec![10, 10, 10, 10, 10, 14]);
        assert!(s.start_offset == Offset(4));
    }

    #[test]
    fn a_deferred_run_does_not_spend_the_record_lock_limit() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(100), 10);
        s.defer_internal(Offset(0), Offset(9));

        s.materialize(Offset(100), 10);

        assert!(s.end_offset == Offset(20));
    }

    #[test]
    fn acquire_stops_at_the_last_offset_of_the_read() {
        // (max_records, last_offset, acquired, first offset left Available)
        let cases = [
            (100, Offset(9), (Offset(0), Offset(9)), None),
            (100, Offset(3), (Offset(0), Offset(3)), Some(Offset(4))),
            (2, Offset(3), (Offset(0), Offset(1)), Some(Offset(2))),
        ];
        for (max_records, last_offset, (first, last), available) in cases {
            let mut s = AcquisitionState::new(Offset(0));
            s.materialize(Offset(10), 100);

            let acquired = s.acquire("m1", max_records, last_offset, t0(), LOCK, 5);

            assert!(
                (acquired, s.first_acquirable_offset(5))
                    == (
                        vec![AcquiredRange {
                            first,
                            last,
                            delivery_count: 1,
                        }],
                        available,
                    )
            );
        }
    }

    /// (name, window setup, what's below the new log start, new log start,
    /// expected SPSO, expected SPEO, expected offsets still `Available`).
    #[test]
    fn advance_past_log_start_archives_available_and_keeps_acquired_locked() {
        type Setup = fn(&mut AcquisitionState);
        // (name, setup, materialize the window first, new log start,
        // expected SPSO, expected SPEO, expected `Available` offsets).
        type Case = (&'static str, Setup, bool, i64, i64, i64, Vec<i64>);
        let no_window: Setup = |_s| {};
        let acquire_below: Setup = |s| {
            let _ = s.acquire("m1", 3, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        };
        let cases: [Case; 4] = [
            (
                "nothing in flight, window never materialized",
                no_window,
                false,
                3,
                3,
                3,
                vec![],
            ),
            (
                "available records below the new start are archived",
                no_window,
                true,
                3,
                3,
                10,
                vec![3, 4, 5, 6, 7, 8, 9],
            ),
            (
                "acquired records below the new start stay locked",
                acquire_below,
                true,
                3,
                0,
                10,
                vec![3, 4, 5, 6, 7, 8, 9],
            ),
            (
                "log start equal to the SPSO changes nothing",
                no_window,
                true,
                0,
                0,
                10,
                vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, setup, materialize_window, log_start, exp_start, exp_end, exp_available) in cases
        {
            let mut s = AcquisitionState::new(Offset(0));
            if materialize_window {
                s.materialize(Offset(10), 100);
            }
            setup(&mut s);

            s.advance_past_log_start(Offset(log_start));

            let available: Vec<i64> = s
                .record_states()
                .into_iter()
                .filter(|(_, state)| *state == RecordState::Available)
                .map(|(offset, _)| offset)
                .collect();
            actual.push((name, s.start_offset.0, s.end_offset.0, available));
            expected.push((name, exp_start, exp_end, exp_available));
        }
        assert!(actual == expected);
    }

    /// A second, healthy member's acquired records below the new log start are
    /// untouched, and a fresh acquire above the SPSO still hands out records.
    #[test]
    fn advance_past_log_start_leaves_other_members_acquired_records_alone() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let acquired = s.acquire("m1", 3, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acquired
                == vec![AcquiredRange {
                    first: Offset(0),
                    last: Offset(2),
                    delivery_count: 1,
                }]
        );

        s.advance_past_log_start(Offset(3));

        // The Acquired run is untouched and still owned by m1: the SPSO stops
        // at it instead of jumping past it or force-archiving it.
        assert!(s.start_offset == Offset(0));
        assert!(s.range_acquired_by("m1", Offset(0), Offset(2)));
        // Records past the lock are still there for a fresh acquire.
        let acquired2 = s.acquire("m2", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acquired2
                == vec![AcquiredRange {
                    first: Offset(3),
                    last: Offset(9),
                    delivery_count: 1,
                }]
        );
    }

    #[test]
    fn acquire_splits_at_max_records() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let acq = s.acquire("m1", 4, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq.len() == 1);
        assert!(acq[0].first == 0 && acq[0].last == 3);
        // The remaining [4,9] is still Available.
        let acq2 = s.acquire("m2", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(acq2[0].first == 4 && acq2[0].last == 9);
    }

    /// Kafka's delivery throttle and acquire modes, over one ten-record run.
    #[test]
    fn acquire_follows_the_throttle_and_the_acquire_mode() {
        // (delivery count of the run, max attempts, batch ends, max records,
        // acquired ranges)
        type Row = (i16, i16, Option<&'static [Offset]>, i32, Vec<(i64, i64)>);
        let rows: Vec<Row> = vec![
            // record_limit cuts at the budget.
            (0, 5, None, 3, vec![(0, 2)]),
            // batch_optimized rounds the cut up to the end of its log batch.
            (0, 5, Some(&[Offset(4), Offset(9)]), 3, vec![(0, 4)]),
            // Under the throttle limit of 3 the whole run goes out.
            (2, 5, None, 10, vec![(0, 9)]),
            // At the limit, half of the run.
            (3, 5, None, 10, vec![(0, 4)]),
            // On the last attempt, one record alone.
            (4, 5, None, 10, vec![(0, 0)]),
            // A limit of 2 throttles nothing before archiving.
            (1, 2, None, 10, vec![(0, 9)]),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (delivery_count, max_attempts, batch_ends, max_records, want) in rows {
            let mut s = AcquisitionState::new(Offset(0));
            s.materialize(Offset(10), 100);
            s.batches[0].delivery_count = delivery_count;
            let got: Vec<_> = s
                .acquire_shaped(
                    "m",
                    AcquireShape {
                        max_records,
                        batch_ends,
                    },
                    Offset(i64::MAX),
                    (t0(), LOCK),
                    max_attempts,
                )
                .into_iter()
                .map(|range| (range.first.0, range.last.0))
                .collect();
            actual.push((delivery_count, max_attempts, max_records, got));
            expected.push((delivery_count, max_attempts, max_records, want));
        }
        assert!(actual == expected);
    }

    /// A throttled run goes out only as the first thing of a pass.
    #[test]
    fn a_throttled_run_waits_behind_records_already_acquired() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        s.split_at(0, Offset(5));
        s.batches[1].delivery_count = 3;
        let first = s.acquire("m", 100, Offset(i64::MAX), t0(), LOCK, 5);
        let second = s.acquire("m", 100, Offset(i64::MAX), t0(), LOCK, 5);
        check!(
            first
                == vec![AcquiredRange {
                    first: Offset(0),
                    last: Offset(4),
                    delivery_count: 1,
                }]
        );
        check!(
            second
                == vec![AcquiredRange {
                    first: Offset(5),
                    last: Offset(6),
                    delivery_count: 4,
                }]
        );
    }
}
