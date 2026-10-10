//! KIP-1191's two phases of the dead-letter queue, as the acquisition machine
//! sees them.
//!
//! Phase 1 is a transition. When the group has a dead-letter queue
//! ([`AcquisitionState::set_dlq_enabled`]), a record that a `Reject` or the
//! delivery limit would archive goes to `Archiving` instead, and the machine
//! notes the run in `pending_dlq`. `Archiving` is not terminal, and it
//! persists as delivery state 3, so a leader that takes over after a crash
//! finds the run and writes its dead-letter record again. Kafka's
//! `InFlightState.tryUpdateState` and `SharePartition.recordStateWithDlq` make
//! the same choice.
//!
//! Phase 2 is [`AcquisitionState::finish_archiving`]. The owner writes the
//! dead-letter record for each run that [`AcquisitionState::take_pending_dlq`]
//! hands out, and then archives the run whether or not that write worked, as
//! `SharePartition.initiateDLQAndArchive` does: a record that cannot be
//! dead-lettered must not hold the SPSO for ever.

use krabka_log::Offset;

use super::{AcquisitionState, InFlightBatch, RecordState};

/// Why a record goes to the dead-letter queue: Kafka's
/// `ShareGroupDLQManager.CLIENT_REJECT` and `DELIVERY_COUNT_EXCEEDED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqCause {
    /// A member acknowledged the record with `Reject`.
    ClientReject,
    /// The record used up the group's delivery count limit.
    DeliveryCountExceeded,
}

impl DlqCause {
    /// The text of the `__dlq.errors.message` header: the message of Kafka's
    /// `ShareGroupDLQThrowable`.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::ClientReject => "Offset rejected by client.",
            Self::DeliveryCountExceeded => "Offset delivery count exceeded the threshold.",
        }
    }

    /// The cause of a run that was restored as `Archiving`. The cause is not
    /// persisted, so, as Kafka's `maybeResumeDlqArchiving` does, a run whose
    /// count reached the limit is `DeliveryCountExceeded`, and any other run
    /// is `ClientReject`.
    #[must_use]
    pub fn inferred(delivery_count: i16, max_attempts: i16) -> Self {
        if delivery_count >= max_attempts {
            Self::DeliveryCountExceeded
        } else {
            Self::ClientReject
        }
    }
}

/// A run of `Archiving` records whose dead-letter write has not started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DlqRange {
    pub first: Offset,
    pub last: Offset,
    pub delivery_count: i16,
    /// `None` for a run restored from the persister, whose cause is not
    /// stored: see [`DlqCause::inferred`].
    pub cause: Option<DlqCause>,
}

/// The parts of an [`AcquisitionState`] that archiving a run touches, borrowed
/// apart from `batches` so a loop over the runs can use them.
pub(super) struct ArchiveSink<'a> {
    pub(super) dlq_enabled: bool,
    pub(super) delivery_complete_count: &'a mut i32,
    pub(super) pending_dlq: &'a mut Vec<DlqRange>,
}

impl ArchiveSink<'_> {
    /// Archives `batch` for `cause`, and clears its owner and lock.
    ///
    /// With a dead-letter queue the run goes to `Archiving` and joins
    /// `pending_dlq`. Without one it is `Archived` at once, and its records
    /// count as terminal, as they always have.
    pub(super) fn archive(&mut self, batch: &mut InFlightBatch, cause: DlqCause) {
        batch.acquired_by = None;
        batch.lock_deadline = None;
        if self.dlq_enabled {
            batch.state = RecordState::Archiving;
            self.pending_dlq.push(DlqRange {
                first: batch.first_offset,
                last: batch.last_offset,
                delivery_count: batch.delivery_count,
                cause: Some(cause),
            });
        } else {
            batch.archive_terminal(self.delivery_complete_count);
        }
    }
}

impl AcquisitionState {
    /// The runs of the window, and the sink that archives them, borrowed
    /// apart so a loop over the runs can archive.
    pub(super) fn runs_and_sink(&mut self) -> (&mut [InFlightBatch], ArchiveSink<'_>) {
        (
            &mut self.batches,
            ArchiveSink {
                dlq_enabled: self.dlq_enabled,
                delivery_complete_count: &mut self.delivery_complete_count,
                pending_dlq: &mut self.pending_dlq,
            },
        )
    }

    /// Sets whether the group has a dead-letter queue: Kafka's
    /// `SharePartition.isDLQEnabledForGroup`, which is the finalized
    /// `share.version` at 2 or more with a topic named by
    /// `errors.deadletterqueue.topic.name`.
    ///
    /// The setting is read afresh on each operation, so the owner sets it
    /// after it takes the state and before it applies anything. It does not
    /// touch a run that is `Archiving` already.
    pub fn set_dlq_enabled(&mut self, enabled: bool) {
        self.dlq_enabled = enabled;
    }

    /// Hands out the `Archiving` runs whose dead-letter write has not
    /// started, and forgets them. The caller writes each run to the queue and
    /// then calls [`Self::finish_archiving`].
    pub fn take_pending_dlq(&mut self) -> Vec<DlqRange> {
        std::mem::take(&mut self.pending_dlq)
    }

    /// The second phase: moves every `Archiving` record in `[first, last]` to
    /// `Archived`, counts it as terminal, marks the state dirty and advances
    /// the SPSO over the new terminal prefix.
    ///
    /// A record of the range that is no longer `Archiving` stays as it is.
    pub fn finish_archiving(&mut self, first: Offset, last: Offset) {
        self.archive_range(
            first,
            last,
            |batch| {
                batch.state == RecordState::Archiving
                    && batch.first_offset >= first
                    && batch.last_offset <= last
            },
            false,
            true,
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, FixtureDeliveryState, StateBatchSetup, state_batch as batch,
        },
        share_partition::state::{
            AckType, AcquiredRange,
            test_support::{
                AcquiredWindowSetup, DeadLetterQueue, DlqRangeSetup, LOCK, acquire_window,
                dlq_range as range, t0,
            },
        },
        test_support::RecordCount,
    };

    fn state_with_dlq(end: Offset) -> AcquisitionState {
        let mut s = AcquisitionState::new(Offset(0));
        acquire_window(
            &mut s,
            AcquiredWindowSetup {
                end,
                record_limit: RecordCount(100),
                dead_letter_queue: DeadLetterQueue::Enabled,
            },
        );
        s
    }

    fn state_with_rejected_prefix() -> AcquisitionState {
        let mut s = state_with_dlq(Offset(4));
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
            .unwrap();
        s
    }

    /// Kafka's `recordStateWithDlq`: with a queue only `Reject` waits in
    /// `Archiving`. `Accept` and `Gap` keep their states, and the run that a
    /// `Reject` moves is not terminal, so it neither counts nor moves the
    /// SPSO.
    #[test]
    fn a_reject_with_a_queue_waits_in_archiving() {
        let mut s = state_with_rejected_prefix();
        s.acknowledge("m1", Offset(2), Offset(2), AckType::Gap, 5)
            .unwrap();
        s.acknowledge("m1", Offset(3), Offset(3), AckType::Accept, 5)
            .unwrap();

        assert!(
            (
                s.start_offset,
                s.delivery_complete_count(),
                s.take_pending_dlq(),
                s.record_states(),
            ) == (
                Offset(0),
                2,
                vec![range(DlqRangeSetup {
                    last: Offset(1),
                    cause: Some(DlqCause::ClientReject),
                    ..Default::default()
                })],
                vec![
                    (0, RecordState::Archiving),
                    (1, RecordState::Archiving),
                    (2, RecordState::Archived),
                    (3, RecordState::Acknowledged),
                ],
            )
        );
    }

    /// Without a queue a `Reject` archives at once, as it always has, and no
    /// run waits for the queue.
    #[test]
    fn a_reject_without_a_queue_archives_at_once() {
        let mut s = state_with_dlq(Offset(2));
        s.set_dlq_enabled(false);
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
            .unwrap();

        assert!((s.start_offset, s.take_pending_dlq()) == (Offset(2), Vec::new()));
    }

    /// Kafka's `InFlightState.tryUpdateState`: a record released, expired or
    /// given back at the delivery limit is archived through the queue, with
    /// the delivery-count cause. Under the limit it is available again.
    #[test]
    fn the_delivery_limit_sends_a_record_to_the_queue() {
        // (name, how the lock ends)
        type End = fn(&mut AcquisitionState);
        let cases: [(&str, End); 3] = [
            ("a release", |s| {
                s.acknowledge("m1", Offset(0), Offset(1), AckType::Release, 1)
                    .unwrap();
            }),
            ("a lock timeout", |s| s.expire_locks(t0() + LOCK * 2, 1)),
            ("a released member", |s| s.release_member("m1", 1)),
        ];
        for (name, end) in cases {
            let mut s = AcquisitionState::new(Offset(0));
            s.set_dlq_enabled(true);
            s.materialize(Offset(2), 100);
            let _ = s.acquire("m1", 10, Offset(i64::MAX), t0(), LOCK, 1);
            end(&mut s);

            assert!(
                (
                    s.take_pending_dlq(),
                    s.record_states(),
                    s.delivery_complete_count(),
                ) == (
                    vec![range(DlqRangeSetup {
                        last: Offset(1),
                        cause: Some(DlqCause::DeliveryCountExceeded),
                        ..Default::default()
                    })],
                    vec![(0, RecordState::Archiving), (1, RecordState::Archiving)],
                    0,
                ),
                "{name}"
            );
        }
    }

    /// The exhausted `Available` run that `acquire` archives as a poison pill
    /// goes through the queue too.
    #[test]
    fn a_poison_pill_goes_through_the_queue() {
        let mut s = AcquisitionState::new(Offset(0));
        s.set_dlq_enabled(true);
        s.materialize(Offset(1), 100);
        let _ = s.acquire("m1", 10, Offset(i64::MAX), t0(), LOCK, 2);
        s.expire_locks(t0() + LOCK * 2, 5);
        let _ = s.take_pending_dlq();

        let acquired = s.acquire("m1", 10, Offset(i64::MAX), t0() + LOCK * 3, LOCK, 1);

        assert!(
            (acquired, s.take_pending_dlq(), s.record_states())
                == (
                    Vec::<AcquiredRange>::new(),
                    vec![range(DlqRangeSetup {
                        cause: Some(DlqCause::DeliveryCountExceeded),
                        ..Default::default()
                    })],
                    vec![(0, RecordState::Archiving)],
                )
        );
    }

    /// Phase 2: the run becomes `Archived`, counts as terminal and lets the
    /// SPSO move on, and a state that is not `Archiving` is left alone.
    #[test]
    fn finishing_archives_the_run_and_moves_the_spso() {
        let mut s = state_with_rejected_prefix();
        s.acknowledge("m1", Offset(2), Offset(2), AckType::Accept, 5)
            .unwrap();
        s.dirty = false;

        s.finish_archiving(Offset(0), Offset(1));

        assert!(
            (s.start_offset, s.dirty, s.record_states())
                == (Offset(3), true, vec![(3, RecordState::Acquired)])
        );
    }

    /// Only the offsets asked for are archived: the rest of an `Archiving`
    /// run keeps waiting, and nothing changes for a range with no such
    /// record.
    #[test]
    fn finishing_a_part_of_a_run_leaves_the_rest_archiving() {
        let mut s = state_with_dlq(Offset(3));
        s.acknowledge("m1", Offset(0), Offset(2), AckType::Reject, 5)
            .unwrap();
        s.dirty = false;

        s.finish_archiving(Offset(1), Offset(1));
        let partial = (s.start_offset, s.record_states());
        s.dirty = false;
        s.finish_archiving(Offset(7), Offset(9));

        assert!(
            (partial, s.dirty)
                == (
                    (
                        Offset(0),
                        vec![
                            (0, RecordState::Archiving),
                            (1, RecordState::Archived),
                            (2, RecordState::Archiving),
                        ],
                    ),
                    false,
                )
        );
    }

    /// An `Archiving` record is persisted as delivery state 3, and a leader
    /// that loads it finds the run to resume, with no cause: the cause
    /// follows the delivery count.
    #[test]
    fn archiving_persists_as_state_three_and_reloads_pending() {
        let mut s = state_with_dlq(Offset(3));
        s.acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
            .unwrap();
        let (start, _, batches) = s.to_persist_batches();
        assert!(
            batches
                == vec![
                    batch(StateBatchSetup {
                        bounds: Offset(0)..=Offset(1),
                        delivery: FixtureDeliveryState::Archiving,
                        ..Default::default()
                    }),
                    batch(StateBatchSetup {
                        bounds: Offset(2)..=Offset(2),
                        delivery: FixtureDeliveryState::Available,
                        ..Default::default()
                    }),
                ]
        );

        let mut reloaded = AcquisitionState::new(Offset(0));
        reloaded.load_from(start, 1, 1, &batches);
        let pending = reloaded.take_pending_dlq();
        reloaded.finish_archiving(Offset(0), Offset(1));
        let (spso, _, after) = reloaded.to_persist_batches();

        assert!(
            (pending, spso, after)
                == (
                    vec![range(DlqRangeSetup {
                        last: Offset(1),
                        ..Default::default()
                    })],
                    Offset(2),
                    vec![batch(StateBatchSetup {
                        bounds: Offset(2)..=Offset(2),
                        delivery: FixtureDeliveryState::Available,
                        ..Default::default()
                    })],
                )
        );
    }

    /// The persister can never move the SPSO past an `Archiving` run: it is
    /// not terminal, so a state that is read back with one keeps the SPSO in
    /// front of it.
    #[test]
    fn an_archiving_run_holds_the_spso() {
        let mut s = AcquisitionState::new(Offset(0));
        s.load_from(
            Offset(0),
            1,
            1,
            &[batch(StateBatchSetup {
                bounds: Offset(0)..=Offset(0),
                delivery: FixtureDeliveryState::Archiving,
                attempts: DeliveryAttemptCount(5),
            })],
        );

        assert!((s.start_offset, s.delivery_complete_count()) == (Offset(0), 0));
    }
}
