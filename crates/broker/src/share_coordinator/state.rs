//! In-memory per-`(group, topicId, partition)` share-delivery state.
//!
//! The broker builds this state as Kafka's `ShareCoordinatorShard.replay`
//! does: a `ShareSnapshot` replaces the state, and a `ShareUpdate` merges
//! into it (`ShareCoordinatorShard.merge`). The live write path appends a
//! record and then applies it through the same functions, so a replay and the
//! live state never differ.
//!
//! Batches merge through [`combine_state_batches`], a port of Kafka's
//! `PersisterStateBatchCombiner`.

use {
    crate::share_coordinator::persistence::{ShareSnapshotValue, ShareUpdateValue, StateBatch},
    krabka_log::Offset,
    std::{cmp::Reverse, collections::BTreeMap},
};

/// The state of one share key, with the bookkeeping that Kafka's shard keeps
/// beside it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharePartitionState {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
    pub snapshot_epoch: i32,
    /// The time the state was created, from the latest snapshot.
    pub create_timestamp: i64,
    /// The time the latest snapshot was written. The cold-partition snapshot
    /// compares it with its interval.
    pub write_timestamp: i64,
    /// The log offset of the latest snapshot record of the key.
    pub last_snapshot_offset: Offset,
    /// Kafka's `snapshotUpdateCount`: the updates since the count last
    /// reached the snapshot threshold.
    pub updates_since_snapshot: u32,
    /// Kafka's `leaderEpochMap`: the highest leader epoch any record of the
    /// key carried. Reads and writes are fenced against it.
    pub fence_leader_epoch: i32,
    /// Kafka's `stateEpochMap`: the highest state epoch any snapshot of the
    /// key carried. Initializes and writes are fenced against it.
    pub fence_state_epoch: i32,
}

impl SharePartitionState {
    /// The state of a key whose first record is the snapshot `v` at `offset`.
    #[must_use]
    pub fn from_snapshot(v: &ShareSnapshotValue, offset: Offset) -> Self {
        let mut state = Self {
            fence_leader_epoch: v.leader_epoch,
            fence_state_epoch: v.state_epoch,
            ..Self::default()
        };
        state.apply_snapshot(v, offset, u32::MAX);
        state
    }

    /// The state of a key whose first record is the update `v`, as Kafka's
    /// `ShareGroupOffset.fromRecord(ShareUpdateValue)` builds it: state epoch
    /// 0 and no timestamps.
    #[must_use]
    pub fn from_update(v: &ShareUpdateValue) -> Self {
        Self {
            state_epoch: 0,
            leader_epoch: v.leader_epoch,
            start_offset: v.start_offset,
            delivery_complete_count: v.delivery_complete_count,
            state_batches: v.state_batches.clone(),
            snapshot_epoch: v.snapshot_epoch,
            updates_since_snapshot: 1,
            fence_leader_epoch: v.leader_epoch,
            ..Self::default()
        }
    }

    /// Replaces the state with the snapshot `v` at `offset`, as Kafka's
    /// `handleShareSnapshot` does. The update count restarts only when it
    /// has reached `updates_per_snapshot`.
    pub fn apply_snapshot(
        &mut self,
        v: &ShareSnapshotValue,
        offset: Offset,
        updates_per_snapshot: u32,
    ) {
        self.fence_leader_epoch = self.fence_leader_epoch.max(v.leader_epoch);
        self.fence_state_epoch = self.fence_state_epoch.max(v.state_epoch);
        self.snapshot_epoch = v.snapshot_epoch;
        self.state_epoch = v.state_epoch;
        self.leader_epoch = v.leader_epoch;
        self.start_offset = v.start_offset;
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches.clone_from(&v.state_batches);
        self.create_timestamp = v.create_timestamp;
        self.write_timestamp = v.write_timestamp;
        self.last_snapshot_offset = offset;
        if self.updates_since_snapshot >= updates_per_snapshot {
            self.updates_since_snapshot = 0;
        }
    }

    /// Merges the update `v`, as Kafka's `ShareCoordinatorShard.merge` does.
    ///
    /// A start offset or leader epoch of `-1` keeps the stored value. The
    /// batches combine with the stored ones and are clipped at the new start
    /// offset. The snapshot epoch, state epoch and timestamps stay.
    pub fn apply_update(&mut self, v: &ShareUpdateValue) {
        self.fence_leader_epoch = self.fence_leader_epoch.max(v.leader_epoch);
        if v.start_offset.0 != -1 {
            self.start_offset = v.start_offset;
        }
        if v.leader_epoch != -1 {
            self.leader_epoch = v.leader_epoch;
        }
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches =
            combine_state_batches(&self.state_batches, &v.state_batches, self.start_offset);
        self.updates_since_snapshot = self.updates_since_snapshot.saturating_add(1);
    }

    /// The snapshot of this state with `snapshot_epoch` and `write_timestamp`
    /// replaced, as Kafka's cold-partition snapshot writes it.
    #[must_use]
    pub fn to_snapshot(&self, snapshot_epoch: i32, write_timestamp: i64) -> ShareSnapshotValue {
        ShareSnapshotValue {
            snapshot_epoch,
            state_epoch: self.state_epoch,
            leader_epoch: self.leader_epoch,
            start_offset: self.start_offset,
            delivery_complete_count: self.delivery_complete_count,
            create_timestamp: self.create_timestamp,
            write_timestamp,
            state_batches: self.state_batches.clone(),
        }
    }
}

/// Combines `so_far` with `new_batches` into the shortest non-overlapping
/// cover of their union, clipped at `start_offset`.
///
/// This is Kafka's `PersisterStateBatchCombiner.combineStateBatches`. Where
/// batches overlap, the one with the higher delivery count wins, then the one
/// with the higher delivery state. Adjacent ranges with the same state and
/// count merge. A `start_offset` of `-1` clips nothing.
#[must_use]
pub fn combine_state_batches(
    so_far: &[StateBatch],
    new_batches: &[StateBatch],
    start_offset: Offset,
) -> Vec<StateBatch> {
    let pruned: Vec<StateBatch> = so_far
        .iter()
        .chain(new_batches)
        .filter(|b| start_offset.0 == -1 || b.last_offset >= start_offset)
        .map(|b| {
            if start_offset.0 == -1 || b.first_offset >= start_offset {
                b.clone()
            } else {
                StateBatch {
                    first_offset: start_offset,
                    ..b.clone()
                }
            }
        })
        .collect();
    if pruned.len() <= 1 {
        return pruned;
    }
    sweep_merge(&pruned)
}

/// The priority of a batch: the higher delivery count wins, then the higher
/// delivery state. `Reverse` makes the winner the first key of a map.
type Priority = Reverse<BatchPriority>;

/// A batch's delivery count and delivery state, in the order that decides
/// which batch wins an overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BatchPriority {
    delivery_count: i16,
    delivery_state: i8,
}

/// A batch boundary of the sweep: an offset where a batch of some priority
/// begins or, one past its last offset, ends.
#[derive(Debug, Clone, Copy)]
struct SweepEvent {
    offset: i64,
    is_begin: bool,
    priority: Priority,
}

/// Kafka's event-driven sweep over the pruned batches.
fn sweep_merge(batches: &[StateBatch]) -> Vec<StateBatch> {
    // `false` sorts before `true`, so an END comes before a BEGIN at the same
    // offset.
    let mut events: Vec<SweepEvent> = batches
        .iter()
        .flat_map(|b| {
            let priority = Reverse(BatchPriority {
                delivery_count: b.delivery_count,
                delivery_state: b.delivery_state,
            });
            [
                SweepEvent {
                    offset: b.first_offset.0,
                    is_begin: true,
                    priority,
                },
                SweepEvent {
                    offset: b.last_offset.0.saturating_add(1),
                    is_begin: false,
                    priority,
                },
            ]
        })
        .collect();
    events.sort_by_key(|event| (event.offset, event.is_begin));

    let mut active: BTreeMap<Priority, usize> = BTreeMap::new();
    let mut out: Vec<StateBatch> = Vec::new();
    let mut open_from = -1;
    let mut open_winner: Option<Priority> = None;
    let mut i = 0;
    while i < events.len() {
        let offset = events[i].offset;
        if let Some(winner) = open_winner
            && offset > open_from
        {
            append_coalesced(&mut out, open_from, offset - 1, winner);
        }
        while i < events.len() && events[i].offset == offset {
            let SweepEvent {
                is_begin, priority, ..
            } = events[i];
            i += 1;
            if is_begin {
                *active.entry(priority).or_insert(0) += 1;
            } else if let Some(count) = active.get_mut(&priority) {
                *count -= 1;
                if *count == 0 {
                    active.remove(&priority);
                }
            }
        }
        open_winner = active.keys().next().copied();
        open_from = offset;
    }
    out
}

fn append_coalesced(out: &mut Vec<StateBatch>, from: i64, to: i64, winner: Priority) {
    let Reverse(BatchPriority {
        delivery_count,
        delivery_state,
    }) = winner;
    if let Some(tail) = out.last_mut()
        && tail.last_offset.0 + 1 == from
        && tail.delivery_state == delivery_state
        && tail.delivery_count == delivery_count
    {
        tail.last_offset = Offset(to);
        return;
    }
    out.push(StateBatch {
        first_offset: Offset(from),
        last_offset: Offset(to),
        delivery_state,
        delivery_count,
    });
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, FixtureDeliveryState, state_batch as batch,
        },
        assert2::{assert, check},
    };

    struct CombinerRow {
        name: &'static str,
        so_far: Vec<StateBatch>,
        new: Vec<StateBatch>,
        start: Offset,
        expected: Vec<StateBatch>,
    }

    #[bon::builder]
    fn acknowledged_batch(start: i64, end: i64) -> StateBatch {
        batch()
            .bounds(Offset(start)..=Offset(end))
            .delivery(FixtureDeliveryState::Acknowledged)
            .call()
    }

    struct CombinerBatches {
        available_first_ten: StateBatch,
        available_at_100: StateBatch,
        acknowledged_first_ten: StateBatch,
    }

    impl Default for CombinerBatches {
        fn default() -> Self {
            Self {
                available_first_ten: batch().call(),
                available_at_100: batch().bounds(Offset(100)..=Offset(109)).call(),
                acknowledged_first_ten: acknowledged_batch().start(0).end(9).call(),
            }
        }
    }

    fn initial_combiner_rows(batches: &CombinerBatches) -> Vec<CombinerRow> {
        vec![
            CombinerRow {
                name: "both empty",
                so_far: vec![],
                new: vec![],
                start: Offset(0),
                expected: vec![],
            },
            CombinerRow {
                name: "one batch passes through",
                so_far: vec![],
                new: vec![batches.available_at_100.clone()],
                start: Offset(-1),
                expected: vec![batches.available_at_100.clone()],
            },
            CombinerRow {
                name: "disjoint batches stay apart and sort",
                so_far: vec![batch().bounds(Offset(110)..=Offset(119)).call()],
                new: vec![acknowledged_batch().start(100).end(104).call()],
                start: Offset(-1),
                expected: vec![
                    acknowledged_batch().start(100).end(104).call(),
                    batch().bounds(Offset(110)..=Offset(119)).call(),
                ],
            },
            CombinerRow {
                name: "adjacent equal batches coalesce",
                so_far: vec![batch().bounds(Offset(100)..=Offset(104)).call()],
                new: vec![batch().bounds(Offset(105)..=Offset(109)).call()],
                start: Offset(-1),
                expected: vec![batches.available_at_100.clone()],
            },
            CombinerRow {
                name: "the higher delivery count wins an overlap",
                so_far: vec![batches.available_at_100.clone()],
                new: vec![
                    batch()
                        .bounds(Offset(103)..=Offset(105))
                        .attempts(DeliveryAttemptCount(2))
                        .call(),
                ],
                start: Offset(-1),
                expected: vec![
                    batch().bounds(Offset(100)..=Offset(102)).call(),
                    batch()
                        .bounds(Offset(103)..=Offset(105))
                        .attempts(DeliveryAttemptCount(2))
                        .call(),
                    batch().bounds(Offset(106)..=Offset(109)).call(),
                ],
            },
            CombinerRow {
                name: "the higher state wins at an equal count",
                so_far: vec![acknowledged_batch().start(100).end(109).call()],
                new: vec![batches.available_at_100.clone()],
                start: Offset(-1),
                expected: vec![acknowledged_batch().start(100).end(109).call()],
            },
            CombinerRow {
                name: "the start offset drops and clips",
                so_far: vec![
                    batch().bounds(Offset(90)..=Offset(99)).call(),
                    batches.available_at_100.clone(),
                ],
                new: vec![acknowledged_batch().start(95).end(104).call()],
                start: Offset(103),
                expected: vec![
                    acknowledged_batch().start(103).end(104).call(),
                    batch().bounds(Offset(105)..=Offset(109)).call(),
                ],
            },
        ]
    }

    fn regression_combiner_rows(batches: &CombinerBatches) -> Vec<CombinerRow> {
        vec![
            // The rows of issue #935.
            CombinerRow {
                name: "a later batch splits the stored one",
                so_far: vec![batches.available_first_ten.clone()],
                new: vec![acknowledged_batch().start(5).end(9).call()],
                start: Offset(0),
                expected: vec![
                    batch().bounds(Offset(0)..=Offset(4)).call(),
                    acknowledged_batch().start(5).end(9).call(),
                ],
            },
            CombinerRow {
                name: "a lower delivery count loses",
                so_far: vec![batch().attempts(DeliveryAttemptCount(2)).call()],
                new: vec![batches.acknowledged_first_ten.clone()],
                start: Offset(0),
                expected: vec![batch().attempts(DeliveryAttemptCount(2)).call()],
            },
            CombinerRow {
                name: "a higher state wins over the stored batch",
                so_far: vec![batches.available_first_ten.clone()],
                new: vec![batches.acknowledged_first_ten.clone()],
                start: Offset(0),
                expected: vec![batches.acknowledged_first_ten.clone()],
            },
            CombinerRow {
                name: "a lone batch is clipped at the start offset",
                so_far: vec![batches.available_first_ten.clone()],
                new: vec![],
                start: Offset(5),
                expected: vec![batch().bounds(Offset(5)..=Offset(9)).call()],
            },
            CombinerRow {
                name: "adjacent equal batches coalesce at start offset 0",
                so_far: vec![acknowledged_batch().start(0).end(4).call()],
                new: vec![acknowledged_batch().start(5).end(9).call()],
                start: Offset(0),
                expected: vec![batches.acknowledged_first_ten.clone()],
            },
            CombinerRow {
                name: "an inner batch splits the stored one in three",
                so_far: vec![batches.available_first_ten.clone()],
                new: vec![
                    batch()
                        .bounds(Offset(3)..=Offset(5))
                        .delivery(FixtureDeliveryState::Archived)
                        .attempts(DeliveryAttemptCount(3))
                        .call(),
                ],
                start: Offset(-1),
                expected: vec![
                    batch().bounds(Offset(0)..=Offset(2)).call(),
                    batch()
                        .bounds(Offset(3)..=Offset(5))
                        .delivery(FixtureDeliveryState::Archived)
                        .attempts(DeliveryAttemptCount(3))
                        .call(),
                    batch().bounds(Offset(6)..=Offset(9)).call(),
                ],
            },
            CombinerRow {
                name: "a batch wholly below the start offset is dropped",
                so_far: vec![],
                new: vec![
                    batches.available_first_ten.clone(),
                    batch().bounds(Offset(20)..=Offset(29)).call(),
                ],
                start: Offset(20),
                expected: vec![batch().bounds(Offset(20)..=Offset(29)).call()],
            },
        ]
    }

    /// Cases of Kafka's `PersisterStateBatchCombinerTest`, including issue #935.
    #[test]
    fn combine_matches_kafka_combiner() {
        let batches = CombinerBatches::default();
        for CombinerRow {
            name,
            so_far,
            new,
            start,
            expected,
        } in initial_combiner_rows(&batches)
            .into_iter()
            .chain(regression_combiner_rows(&batches))
        {
            check!(
                combine_state_batches(&so_far, &new, start) == expected,
                "{name}"
            );
        }
    }

    fn snapshot(start: i64, batches: Vec<StateBatch>) -> ShareSnapshotValue {
        ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 2,
            leader_epoch: 3,
            start_offset: Offset(start),
            delivery_complete_count: 0,
            create_timestamp: 10,
            write_timestamp: 20,
            state_batches: batches,
        }
    }

    #[test]
    fn snapshot_then_update_merges_as_kafka() {
        let mut s = SharePartitionState::from_snapshot(
            &snapshot(0, vec![
                batch().call(),
                batch().bounds(Offset(10)..=Offset(19)).call(),
                batch().bounds(Offset(20)..=Offset(29)).call(),
            ]),
            Offset(7),
        );
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch().bounds(Offset(30)..=Offset(39)).call()],
        });

        let expected = SharePartitionState {
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch().bounds(Offset(20)..=Offset(39)).call()],
            snapshot_epoch: 1,
            create_timestamp: 10,
            write_timestamp: 20,
            last_snapshot_offset: Offset(7),
            updates_since_snapshot: 1,
            fence_leader_epoch: 4,
            fence_state_epoch: 2,
        };
        assert!(s == expected);
    }

    /// An update with `-1` keeps the stored start offset and leader epoch.
    #[test]
    fn update_with_minus_one_keeps_stored_values() {
        let mut s = SharePartitionState::from_snapshot(&snapshot(50, vec![]), Offset(0));
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: -1,
            start_offset: Offset(-1),
            delivery_complete_count: 3,
            state_batches: vec![batch().bounds(Offset(40)..=Offset(55)).call()],
        });
        check!(s.start_offset == Offset(50));
        check!(s.leader_epoch == 3);
        check!(s.state_batches == vec![batch().bounds(Offset(50)..=Offset(55)).call()]);
    }

    /// The update count restarts on a snapshot only once it has reached the
    /// threshold, and the fence epochs never go down.
    #[test]
    fn snapshot_resets_count_at_threshold_and_keeps_fences() {
        let rows = [(1, 2, 1), (2, 2, 0), (3, 2, 0)];
        for (updates, threshold, expected) in rows {
            let mut s = SharePartitionState {
                updates_since_snapshot: updates,
                fence_leader_epoch: 9,
                fence_state_epoch: 9,
                ..SharePartitionState::default()
            };
            s.apply_snapshot(&snapshot(0, vec![]), Offset(1), threshold);
            check!(
                s.updates_since_snapshot == expected,
                "{updates}/{threshold}"
            );
            check!((s.fence_leader_epoch, s.fence_state_epoch) == (9, 9));
        }
    }
}
