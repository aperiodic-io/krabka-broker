//! The bridge between the live window and the share coordinator's records.
//!
//! This module holds the projection of the in-flight batch list into
//! `StateBatch` values that the share coordinator writes, and the reload that
//! rebuilds the machine from them. It also holds the two counters a caller
//! reads out of the machine, `delivery_complete_count` and
//! `count_acquired_batches`. Both directions of the mapping are stated once,
//! here, so a change to the persisted delivery-state codes touches one file.

use krabka_log::Offset;

use super::{
    AcquisitionState, DS_ACKNOWLEDGED, DS_ARCHIVED, DS_ARCHIVING, DS_AVAILABLE, InFlightBatch,
    RecordState, clamp_i32, dlq::DlqRange,
};
use crate::share_coordinator::persistence::StateBatch;

impl AcquisitionState {
    /// Projects the live window into persistable batches.
    ///
    /// It returns `(start_offset, delivery_complete_count, batches)` for
    /// `[start_offset, end_offset)`. It persists a transient `Acquired` record
    /// as `Available(0)`, so a leader that crashes and reloads offers the
    /// record again. It emits Acknowledged and Archived batches with their
    /// terminal codes.
    ///
    /// A `Deferred` record persists as `Available(0)` for the same reason: it
    /// is derived from a clock reading, and the next leader must re-derive it
    /// from its own clock. The `__share_group_state` encoding therefore does
    /// not change by one byte on a scheduled topic.
    #[must_use]
    pub fn to_persist_batches(&self) -> (Offset, i32, Vec<StateBatch>) {
        let mut out = Vec::with_capacity(self.batches.len());
        for b in &self.batches {
            let delivery_state = match b.state {
                RecordState::Available | RecordState::Acquired | RecordState::Deferred => {
                    DS_AVAILABLE
                }
                RecordState::Acknowledged => DS_ACKNOWLEDGED,
                RecordState::Archiving => DS_ARCHIVING,
                RecordState::Archived => DS_ARCHIVED,
            };
            out.push(StateBatch {
                first_offset: b.first_offset,
                last_offset: b.last_offset,
                delivery_state,
                delivery_count: b.delivery_count,
            });
        }
        (self.start_offset, self.delivery_complete_count, out)
    }

    /// Number of terminal records, Acknowledged or Archived, in the window at
    /// or above the SPSO. This is the persister's `delivery_complete_count`.
    /// Only the state-machine tests read this method today. The value also
    /// leaves through [`Self::to_persist_batches`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn delivery_complete_count(&self) -> i32 {
        self.delivery_complete_count
    }

    /// Number of in-flight batches currently in `Acquired` state. Test-only.
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn count_acquired_batches(&self) -> i32 {
        i32::try_from(
            self.batches
                .iter()
                .filter(|b| b.state == RecordState::Acquired)
                .count(),
        )
        .unwrap_or(i32::MAX)
    }

    /// Rebuilds the machine from persisted state.
    ///
    /// It restores the SPSO to `start_offset`. It rebuilds the batches, and
    /// maps a persisted `Acquired(1)` to `Available`, because a lock does not
    /// survive a leader change. It sets `end_offset` to `max(last_offset)+1`,
    /// or to `start_offset` when the batch list is empty.
    ///
    /// The delivery complete count is not read back. As Kafka's
    /// `SharePartition.maybeInitialize` does, it is the number of records in
    /// the Acknowledged and Archived batches, and the SPSO then moves past a
    /// terminal prefix (`maybeUpdateCachedStateAndOffsets`), which takes those
    /// records out of the count again. That move is not written back until
    /// the next change, as in Kafka.
    pub fn load_from(
        &mut self,
        start_offset: Offset,
        state_epoch: i32,
        leader_epoch: i32,
        batches: &[StateBatch],
    ) {
        self.start_offset = start_offset;
        self.state_epoch = state_epoch;
        self.leader_epoch = leader_epoch;
        self.dirty = false;
        self.batches = batches
            .iter()
            .map(|sb| {
                // Persisted Acquired(1) maps to Available: locks don't survive a
                // leader change, so re-offer those records.
                let state = match sb.delivery_state {
                    DS_ACKNOWLEDGED => RecordState::Acknowledged,
                    DS_ARCHIVING => RecordState::Archiving,
                    DS_ARCHIVED => RecordState::Archived,
                    _ => RecordState::Available,
                };
                InFlightBatch {
                    first_offset: sb.first_offset,
                    last_offset: sb.last_offset,
                    state,
                    delivery_count: sb.delivery_count,
                    acquired_by: None,
                    lock_deadline: None,
                }
            })
            .collect();
        // A run that was `Archiving` when the last leader stopped never got
        // its dead-letter record acknowledged, so this leader writes it again,
        // as Kafka's `maybeResumeDlqArchiving` does.
        self.pending_dlq = self
            .batches
            .iter()
            .filter(|b| b.state == RecordState::Archiving)
            .map(|b| DlqRange {
                first: b.first_offset,
                last: b.last_offset,
                delivery_count: b.delivery_count,
                cause: None,
            })
            .collect();
        self.end_offset = self
            .batches
            .iter()
            .map(|b| b.last_offset + 1)
            .max()
            .unwrap_or(start_offset)
            .max(start_offset);
        self.delivery_complete_count = self
            .batches
            .iter()
            .filter(|b| b.is_terminal())
            .fold(0, |count: i32, b| count.saturating_add(clamp_i32(b.len())));
        self.advance_spso();
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{
        share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, FixtureDeliveryState, StateBatchSetup, state_batch as persisted,
        },
        share_partition::state::{
            AckType, AcquiredRange,
            test_support::{LOCK, t0},
        },
    };

    #[test]
    fn to_persist_batches_maps_acquired_to_available() {
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(5), 100);
        let _ = s.acquire("m1", 10, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        let (start, dcc, batches) = s.to_persist_batches();
        check!(start == 0);
        check!(dcc == 0); // nothing terminal yet
        // Acquired persists as Available(0) but retains its delivery_count.
        check!(
            batches
                == vec![persisted(StateBatchSetup {
                    bounds: Offset(0)..=Offset(4),
                    ..Default::default()
                })]
        );
    }

    #[test]
    fn load_from_round_trip() {
        // Build a state, acquire part of it, persist, reload into a fresh one.
        let mut s = AcquisitionState::new(Offset(0));
        s.materialize(Offset(10), 100);
        let _ = s.acquire("m1", 4, krabka_log::Offset(i64::MAX), t0(), LOCK, 5); // [0,3] Acquired, [4,9] Available
        s.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
            .unwrap(); // SPSO -> 4
        let (start, _dcc, batches) = s.to_persist_batches();
        assert!(start == 4);

        let mut reloaded = AcquisitionState::new(Offset(0));
        reloaded.load_from(start, 7, 3, &batches);
        check!(reloaded.start_offset == 4);
        check!(reloaded.end_offset == 10);
        check!(reloaded.state_epoch == 7);
        check!(reloaded.leader_epoch == 3);
        check!(!reloaded.dirty);
        // The remaining records are Available again and re-acquirable.
        let acq = reloaded.acquire("m2", 100, krabka_log::Offset(i64::MAX), t0(), LOCK, 5);
        assert!(
            acq == vec![AcquiredRange {
                first: Offset(4),
                last: Offset(9),
                delivery_count: 1
            }]
        );
    }

    /// Five records, `[0]` acquired alone and `[1,4]` acquired together by
    /// `m1`, so a terminal run at `[2,3]` sits behind a record that still
    /// holds the SPSO at 0.
    fn five_acquired_in_two_runs(s: &mut AcquisitionState) {
        s.materialize(Offset(5), 100);
        let _ = s.acquire("m1", 1, Offset(i64::MAX), t0(), LOCK, 5);
        let _ = s.acquire("m1", 10, Offset(i64::MAX), t0(), LOCK, 5);
    }

    /// Kafka's `SharePartition.deliveryCompleteCount`: the number of
    /// Acknowledged and Archived records in the window at or above the SPSO.
    /// A record adds to it when it becomes terminal, and leaves it when the
    /// SPSO moves past it.
    #[test]
    fn delivery_complete_count_counts_the_terminal_records_in_the_window() {
        type Setup = fn(&mut AcquisitionState);
        // (name, setup, expected (SPSO, SPEO, delivery complete count))
        let cases: [(&str, Setup, (i64, i64, i32)); 13] = [
            (
                "accept counts the acknowledged records",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(2), Offset(3), AckType::Accept, 5)
                        .unwrap();
                },
                (0, 5, 2),
            ),
            (
                "reject counts the archived records",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(2), Offset(3), AckType::Reject, 5)
                        .unwrap();
                },
                (0, 5, 2),
            ),
            (
                "release is not terminal and does not count",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(2), Offset(3), AckType::Release, 5)
                        .unwrap();
                },
                (0, 5, 0),
            ),
            (
                "release at the delivery limit archives and counts",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(2), Offset(3), AckType::Release, 1)
                        .unwrap();
                },
                (0, 5, 2),
            ),
            (
                "a lock that expires at the delivery limit archives and counts",
                |s| {
                    s.materialize(Offset(5), 100);
                    let _ = s.acquire("m1", 1, Offset(i64::MAX), t0(), LOCK, 5);
                    s.renew("m1", Offset(0), Offset(0), t0(), LOCK * 100)
                        .unwrap();
                    let _ = s.acquire("m1", 10, Offset(i64::MAX), t0(), LOCK, 5);
                    s.expire_locks(t0() + LOCK * 2, 1);
                },
                (0, 5, 4),
            ),
            (
                "acquire archives an available run at the delivery limit and counts it",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(1), Offset(4), AckType::Release, 5)
                        .unwrap();
                    let _ = s.acquire("m2", 10, Offset(i64::MAX), t0(), LOCK, 1);
                },
                (0, 5, 4),
            ),
            (
                "an archived internal offset counts",
                |s| {
                    s.materialize(Offset(5), 100);
                    s.archive_internal(Offset(2), Offset(2));
                },
                (0, 5, 1),
            ),
            (
                "the SPSO moving past terminal records takes them out",
                |s| {
                    s.materialize(Offset(3), 100);
                    let _ = s.acquire("m1", 10, Offset(i64::MAX), t0(), LOCK, 5);
                    s.acknowledge("m1", Offset(0), Offset(2), AckType::Accept, 5)
                        .unwrap();
                },
                (3, 3, 0),
            ),
            (
                "a partial SPSO move keeps the terminal records above it",
                |s| {
                    five_acquired_in_two_runs(s);
                    s.acknowledge("m1", Offset(2), Offset(3), AckType::Accept, 5)
                        .unwrap();
                    s.acknowledge("m1", Offset(0), Offset(0), AckType::Accept, 5)
                        .unwrap();
                },
                (1, 5, 2),
            ),
            (
                "the log start moving past the window archives without counting",
                |s| {
                    s.materialize(Offset(5), 100);
                    s.advance_past_log_start(Offset(3));
                },
                (3, 5, 0),
            ),
            (
                "initialization counts the persisted terminal batches",
                |s| {
                    s.load_from(
                        Offset(2),
                        1,
                        0,
                        &[
                            persisted(StateBatchSetup {
                                bounds: Offset(2)..=Offset(3),
                                ..Default::default()
                            }),
                            persisted(StateBatchSetup {
                                bounds: Offset(4)..=Offset(5),
                                delivery: FixtureDeliveryState::Acknowledged,
                                ..Default::default()
                            }),
                            persisted(StateBatchSetup {
                                bounds: Offset(6)..=Offset(6),
                                delivery: FixtureDeliveryState::Archived,
                                attempts: DeliveryAttemptCount(2),
                            }),
                        ],
                    );
                },
                (2, 7, 3),
            ),
            (
                "initialization moves the SPSO past a terminal prefix",
                |s| {
                    s.load_from(
                        Offset(0),
                        1,
                        0,
                        &[
                            persisted(StateBatchSetup {
                                bounds: Offset(0)..=Offset(1),
                                delivery: FixtureDeliveryState::Acknowledged,
                                ..Default::default()
                            }),
                            persisted(StateBatchSetup {
                                bounds: Offset(2)..=Offset(3),
                                ..Default::default()
                            }),
                            persisted(StateBatchSetup {
                                bounds: Offset(4)..=Offset(4),
                                delivery: FixtureDeliveryState::Archived,
                                ..Default::default()
                            }),
                        ],
                    );
                },
                (2, 5, 1),
            ),
            (
                "initialization of an all-terminal window empties it",
                |s| {
                    s.load_from(
                        Offset(0),
                        1,
                        0,
                        &[persisted(StateBatchSetup {
                            bounds: Offset(0)..=Offset(3),
                            delivery: FixtureDeliveryState::Acknowledged,
                            ..Default::default()
                        })],
                    );
                },
                (4, 4, 0),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, setup, want) in cases {
            let mut s = AcquisitionState::new(Offset(0));
            setup(&mut s);
            let (start, dcc, _) = s.to_persist_batches();
            actual.push((name, (start.0, s.end_offset.0, dcc)));
            expected.push((name, want));
        }
        assert!(actual == expected);
    }
}
