//! The second phase of the dead-letter queue on the leader (KIP-1191): write
//! the records of an `Archiving` run to the queue, then archive the run.
//!
//! The first phase is a transition of the acquisition machine, and it is durable
//! before this module hears of it: [`SharePartitionLeaderManager::persist_if_dirty`]
//! hands over the runs that the machine has noted only once the coordinator has
//! stored them as `Archiving`. A leader that stops before the second phase
//! ends leaves those runs in the coordinator, and the next leader that loads
//! the partition resumes them, so a record is written at least once.
//!
//! The second phase is Kafka's `SharePartition.initiateDLQAndArchive`: it
//! archives the run whether or not the write worked, because a record that
//! cannot be dead-lettered must not hold the SPSO for ever. A failure is logged
//! as an error.
//!
//! A consumer that rejects at a high rate leaves many runs `Archiving` at
//! once, and each run holds the SPSO until its write ends. Kafka's
//! `ShareGroupDLQStateManager` coalesces the produce requests for one
//! destination, and sends them from one thread. Here the writer does the same
//! (`share_partition::dlq`): the rounds of every write that waits for
//! one leader go out in as few produce requests as `max.message.bytes` allows,
//! and each write ends, and archives its run, when its own records are written.
//! The runs that one dispatch hands over are first joined where a single write
//! can carry them, which saves a read of the source records for each run, they
//! share one task, and at most [`MAX_CONCURRENT_DEAD_LETTER_WRITES`] writes run
//! at once across every partition the broker leads, so that is the most runs
//! that share a request.

use std::sync::Arc;

use futures_util::future::join_all;
use krabka_log::Offset;
use tokio::sync::Mutex;

use super::{LeaderKey, SharePartitionLeaderManager};
use crate::share_partition::{
    dlq::DlqRequest,
    group_settings::GroupShareSettings,
    state::{AcquisitionState, DlqCause, DlqRange},
};

/// The dead-letter writes that run at once on one broker.
pub(super) const MAX_CONCURRENT_DEAD_LETTER_WRITES: usize = 8;

/// Joins the runs that one write can carry: each run that begins where the one
/// before it ends, with the same delivery count and cause, so that every
/// record of the result has the headers its own run would have given it.
fn coalesce(mut ranges: Vec<DlqRange>) -> Vec<DlqRange> {
    ranges.sort_by_key(|range| range.first);
    let mut joined: Vec<DlqRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match joined.last_mut() {
            Some(previous)
                if previous.last + 1 == range.first
                    && previous.delivery_count == range.delivery_count
                    && previous.cause == range.cause =>
            {
                previous.last = range.last;
            }
            _ => joined.push(range),
        }
    }
    joined
}

impl SharePartitionLeaderManager {
    /// Starts the second phase for `ranges`, in one task that writes them
    /// together, and returns at once. A slow write holds up only itself, since
    /// the writes of the task run side by side, up to the limit of the broker.
    pub(super) fn dispatch_dead_letters(
        &self,
        key: &LeaderKey,
        cell: &Arc<Mutex<AcquisitionState>>,
        ranges: Vec<DlqRange>,
    ) {
        let ranges = coalesce(ranges);
        if ranges.is_empty() {
            return;
        }
        let Some(manager) = self.me.upgrade() else {
            return;
        };
        let (key, cell) = (key.clone(), Arc::clone(cell));
        tokio::spawn(async move {
            join_all(
                ranges
                    .into_iter()
                    .map(|range| manager.dead_letter(&key, &cell, range)),
            )
            .await;
        });
    }

    /// Writes `range` to the queue, then archives it and makes that durable.
    async fn dead_letter(
        &self,
        (group, topic_id, partition): &LeaderKey,
        cell: &Arc<Mutex<AcquisitionState>>,
        range: DlqRange,
    ) {
        // The cause of a run that came back from the coordinator is not
        // stored, so it follows the group's delivery count limit.
        let limit =
            GroupShareSettings::resolve(&self.controller.current_image(), group, &self.config)
                .delivery_count_limit;
        let request = DlqRequest {
            group: group.clone(),
            topic_id: *topic_id,
            source_partition: *partition,
            first: range.first,
            last: range.last,
            delivery_count: range.delivery_count,
            cause: range
                .cause
                .unwrap_or_else(|| DlqCause::inferred(range.delivery_count, limit)),
        };
        let (Offset(first), Offset(last)) = (range.first, range.last);
        let written = {
            // The semaphore is never closed, so a permit is always granted.
            let _permit = self.dead_letter_writes.acquire().await;
            self.dlq.write(request).await
        };
        if let Err(error) = written {
            tracing::error!(
                group,
                %topic_id,
                partition,
                first,
                last,
                %error,
                "Failed to write to DLQ, proceeding to ARCHIVED regardless."
            );
        }
        let mut state = cell.lock().await;
        state.finish_archiving(range.first, range.last);
        // Best-effort: a failed write keeps the state dirty, so a later write
        // carries the archived run to the coordinator. The state stays in
        // memory until then, as in Kafka.
        let _ = self
            .persist_if_dirty(group, *topic_id, *partition, Some(cell), &mut state)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use async_trait::async_trait;
    use krabka_log::Offset;

    use super::{MAX_CONCURRENT_DEAD_LETTER_WRITES, coalesce};
    use crate::{
        codes,
        share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, FixtureDeliveryState, StateBatchSetup, state_batch,
        },
        share_partition::{
            dlq::{DlqError, DlqRequest, DlqSink, test_support::RecordingDlq},
            manager::test_support::manager_with_dlq,
            state::{
                AckType, AcquisitionState, DlqCause, RecordState,
                test_support::{
                    AcquiredWindowSetup, DeadLetterQueue, DeliveryCount, DlqRangeSetup,
                    acquire_window, dlq_range as range,
                },
            },
        },
        test_support::RecordCount,
    };

    /// A sink that keeps each write in flight for a few polls, and remembers
    /// how many were in flight at once and what it was asked to write.
    #[derive(Default)]
    struct GaugeDlq {
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        written: std::sync::Mutex<Vec<DlqRequest>>,
    }

    #[async_trait]
    impl DlqSink for GaugeDlq {
        async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.in_flight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(now, SeqCst);
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            self.in_flight.fetch_sub(1, SeqCst);
            self.written.lock().expect("written lock").push(request);
            Ok(())
        }
    }

    /// Records `0..records`, all acquired by `m1`, in a cell that has a
    /// dead-letter queue.
    async fn acquired_cell(
        mgr: &Arc<super::SharePartitionLeaderManager>,
        tid: uuid::Uuid,
        records: RecordCount,
    ) -> Arc<tokio::sync::Mutex<AcquisitionState>> {
        let cell = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        {
            let mut state = cell.lock().await;
            acquire_window(
                &mut state,
                AcquiredWindowSetup {
                    end: Offset(i64::from(records.0)),
                    record_limit: RecordCount(100),
                    dead_letter_queue: DeadLetterQueue::Enabled,
                },
            );
        }
        cell
    }

    /// A sink, manager and acquired cell with no additional retained ownership.
    async fn acquired_manager<D: DlqSink + Default + 'static>(
        records: RecordCount,
    ) -> (
        Arc<D>,
        Arc<super::SharePartitionLeaderManager>,
        uuid::Uuid,
        Arc<tokio::sync::Mutex<AcquisitionState>>,
    ) {
        let dlq = Arc::new(D::default());
        let mgr = manager_with_dlq(dlq.clone());
        let tid = uuid::Uuid::from_bytes([61; 16]);
        let cell = acquired_cell(&mgr, tid, records).await;
        (dlq, mgr, tid, cell)
    }

    /// Dispatches the durable ranges, then waits for `done` or fails the test.
    async fn dispatch_and_wait(
        mgr: &super::SharePartitionLeaderManager,
        key: &super::LeaderKey,
        cell: &Arc<tokio::sync::Mutex<AcquisitionState>>,
        ranges: Vec<crate::share_partition::state::DlqRange>,
        done: impl Fn(&[(i64, RecordState)]) -> bool,
    ) -> Vec<(i64, RecordState)> {
        mgr.dispatch_dead_letters(key, cell, ranges);
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let states = cell.lock().await.record_states();
                if done(&states) {
                    return states;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the dead-letter write finished")
    }

    #[derive(krabka_macros::FieldDefaults)]
    struct ExpectedDlqRequestSetup {
        #[default(Offset(0)..=Offset(1))]
        bounds: std::ops::RangeInclusive<Offset>,
        #[default(DeliveryCount(1))]
        delivery_count: DeliveryCount,
        #[default(DlqCause::ClientReject)]
        cause: DlqCause,
    }

    fn request(setup: ExpectedDlqRequestSetup) -> DlqRequest {
        let ExpectedDlqRequestSetup {
            bounds,
            delivery_count,
            cause,
        } = setup;
        let (first, last) = bounds.into_inner();
        DlqRequest {
            group: "g1".into(),
            topic_id: uuid::Uuid::from_bytes([61; 16]),
            source_partition: 0,
            first,
            last,
            delivery_count: delivery_count.0,
            cause,
        }
    }

    /// A write that cannot reach the coordinator keeps the run in `Archiving`
    /// in memory: the dead-letter write starts only after the run is durable,
    /// and a failed write of `Archiving` rolls the reject back.
    #[tokio::test(start_paused = true)]
    async fn the_write_starts_only_once_archiving_is_durable() {
        let (dlq, mgr, tid, cell) = acquired_manager::<RecordingDlq>(RecordCount(2)).await;

        // The test manager's persister cannot write, so the reject rolls back.
        let mut state = cell.lock().await;
        let code = mgr
            .apply_durably("g1", tid, 0, &cell, &mut state, |st| {
                st.acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
                    .err()
                    .unwrap_or(codes::NONE)
            })
            .await;
        let states = state.record_states();
        drop(state);
        tokio::task::yield_now().await;

        assert!(
            (code, states, dlq.requests())
                == (
                    codes::COORDINATOR_NOT_AVAILABLE,
                    vec![(0, RecordState::Acquired), (1, RecordState::Acquired)],
                    Vec::<DlqRequest>::new(),
                )
        );
    }

    /// Kafka's `initiateDLQAndArchive`: each run is written to the queue with
    /// its cause and delivery count, then archived and the SPSO moves on.
    #[tokio::test(start_paused = true)]
    async fn a_dispatched_run_is_written_then_archived() {
        let (dlq, mgr, tid, cell) = acquired_manager::<RecordingDlq>(RecordCount(3)).await;
        let key = ("g1".to_owned(), tid, 0);
        let ranges = {
            let mut state = cell.lock().await;
            state
                .acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
                .unwrap();
            state.take_pending_dlq()
        };

        let states = dispatch_and_wait(&mgr, &key, &cell, ranges, |states| states.len() == 1).await;

        assert!(
            (states, dlq.requests(), cell.lock().await.start_offset)
                == (
                    vec![(2, RecordState::Acquired)],
                    vec![request(ExpectedDlqRequestSetup::default())],
                    Offset(2),
                )
        );
    }

    /// Only neighbours join, and only when a record of the result would get
    /// the same delivery count and cause header from its own run: a gap, a
    /// different count, a different cause, or a cause that was not stored,
    /// each keeps two runs apart. The input need not be in order.
    #[test]
    fn coalesce_joins_only_neighbours_with_one_count_and_cause() {
        let reject = Some(DlqCause::ClientReject);
        let exceeded = Some(DlqCause::DeliveryCountExceeded);
        let rejected_offsets_0_0_delivery_1 = range(DlqRangeSetup {
            cause: reject,
            ..Default::default()
        });
        let rejected_offsets_0_2_delivery_1 = range(DlqRangeSetup {
            last: Offset(2),
            cause: reject,
            ..Default::default()
        });
        let rejected_offsets_2_2_delivery_1 = range(DlqRangeSetup {
            first: Offset(2),
            last: Offset(2),
            cause: reject,
            ..Default::default()
        });
        let rejected_offsets_1_1_delivery_2 = range(DlqRangeSetup {
            first: Offset(1),
            last: Offset(1),
            delivery_count: DeliveryCount(2),
            cause: reject,
        });
        let rejected_offsets_0_0_delivery_5 = range(DlqRangeSetup {
            delivery_count: DeliveryCount(5),
            cause: reject,
            ..Default::default()
        });
        let exceeded_offsets_1_1_delivery_5 = range(DlqRangeSetup {
            first: Offset(1),
            last: Offset(1),
            delivery_count: DeliveryCount(5),
            cause: exceeded,
        });
        let exceeded_offsets_0_0_delivery_5 = range(DlqRangeSetup {
            delivery_count: DeliveryCount(5),
            cause: exceeded,
            ..Default::default()
        });
        let restored_offsets_1_1_delivery_5 = range(DlqRangeSetup {
            first: Offset(1),
            last: Offset(1),
            delivery_count: DeliveryCount(5),
            ..Default::default()
        });
        let cases = [
            (vec![], vec![]),
            (
                vec![
                    rejected_offsets_0_0_delivery_1,
                    range(DlqRangeSetup {
                        first: Offset(1),
                        last: Offset(2),
                        cause: reject,
                        ..Default::default()
                    }),
                ],
                vec![rejected_offsets_0_2_delivery_1],
            ),
            (
                vec![
                    range(DlqRangeSetup {
                        first: Offset(3),
                        last: Offset(3),
                        cause: reject,
                        ..Default::default()
                    }),
                    rejected_offsets_0_2_delivery_1,
                ],
                vec![range(DlqRangeSetup {
                    last: Offset(3),
                    cause: reject,
                    ..Default::default()
                })],
            ),
            (
                vec![
                    rejected_offsets_0_0_delivery_1,
                    range(DlqRangeSetup {
                        first: Offset(1),
                        last: Offset(1),
                        cause: reject,
                        ..Default::default()
                    }),
                    rejected_offsets_2_2_delivery_1,
                ],
                vec![rejected_offsets_0_2_delivery_1],
            ),
            (
                vec![
                    rejected_offsets_0_0_delivery_1,
                    rejected_offsets_2_2_delivery_1,
                ],
                vec![
                    rejected_offsets_0_0_delivery_1,
                    rejected_offsets_2_2_delivery_1,
                ],
            ),
            (
                vec![
                    rejected_offsets_0_0_delivery_1,
                    rejected_offsets_1_1_delivery_2,
                ],
                vec![
                    rejected_offsets_0_0_delivery_1,
                    rejected_offsets_1_1_delivery_2,
                ],
            ),
            (
                vec![
                    rejected_offsets_0_0_delivery_5,
                    exceeded_offsets_1_1_delivery_5,
                ],
                vec![
                    rejected_offsets_0_0_delivery_5,
                    exceeded_offsets_1_1_delivery_5,
                ],
            ),
            (
                vec![
                    exceeded_offsets_0_0_delivery_5,
                    restored_offsets_1_1_delivery_5,
                ],
                vec![
                    exceeded_offsets_0_0_delivery_5,
                    restored_offsets_1_1_delivery_5,
                ],
            ),
        ];

        for (input, expected) in cases {
            assert!(coalesce(input.clone()) == expected, "{input:?}");
        }
    }

    /// Neighbouring runs that a consumer rejected one after the other are one
    /// write, and one dead-letter record for each offset still comes out.
    #[tokio::test(start_paused = true)]
    async fn neighbouring_rejects_are_written_together_and_archived() {
        let (dlq, mgr, tid, cell) = acquired_manager::<RecordingDlq>(RecordCount(4)).await;
        let key = ("g1".to_owned(), tid, 0);
        let ranges = {
            let mut state = cell.lock().await;
            for offset in 0..3 {
                state
                    .acknowledge("m1", Offset(offset), Offset(offset), AckType::Reject, 5)
                    .unwrap();
            }
            state.take_pending_dlq()
        };

        let states = dispatch_and_wait(&mgr, &key, &cell, ranges, |states| states.len() == 1).await;

        assert!(
            (states, dlq.requests())
                == (
                    vec![(3, RecordState::Acquired)],
                    vec![request(ExpectedDlqRequestSetup {
                        bounds: Offset(0)..=Offset(2),
                        ..Default::default()
                    })],
                )
        );
    }

    /// Many runs that cannot be joined are written side by side, but no more
    /// than the broker's limit at once, so a consumer that rejects at a high
    /// rate cannot open a write for each run while the queue is slow.
    #[tokio::test(start_paused = true)]
    async fn writes_run_side_by_side_up_to_the_broker_limit() {
        let (dlq, mgr, tid, cell) = acquired_manager::<GaugeDlq>(RecordCount(40)).await;
        let key = ("g1".to_owned(), tid, 0);
        // Every other record, so no two runs are neighbours.
        let ranges = {
            let mut state = cell.lock().await;
            for offset in (0..40).step_by(2) {
                state
                    .acknowledge("m1", Offset(offset), Offset(offset), AckType::Reject, 5)
                    .unwrap();
            }
            state.take_pending_dlq()
        };
        assert!(ranges.len() == 20);

        mgr.dispatch_dead_letters(&key, &cell, ranges);
        // The writes end before each run is archived and persisted, and the
        // test manager's persister takes its time to fail, so the writes are
        // what to wait for.
        for _ in 0..10_000 {
            if dlq.written.lock().expect("written lock").len() == 20 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(
            (
                dlq.peak.load(std::sync::atomic::Ordering::SeqCst),
                dlq.written.lock().expect("written lock").len(),
            ) == (MAX_CONCURRENT_DEAD_LETTER_WRITES, 20)
        );
    }

    /// A run that came back from the coordinator has no stored cause: it is
    /// dead-lettered for the delivery count when the count reached the limit,
    /// and for a reject when it did not. A failed write archives the run
    /// regardless.
    #[tokio::test(start_paused = true)]
    async fn a_failed_write_still_archives_and_a_restored_cause_is_inferred() {
        let dlq = Arc::new(RecordingDlq::failing(DlqError::Write("boom".into())));
        let mgr = manager_with_dlq(dlq.clone());
        let tid = uuid::Uuid::from_bytes([61; 16]);
        let cell = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        let key = ("g1".to_owned(), tid, 0);
        let ranges = {
            let mut state = cell.lock().await;
            state.load_from(
                Offset(0),
                1,
                1,
                &[
                    state_batch(StateBatchSetup {
                        bounds: Offset(0)..=Offset(0),
                        delivery: FixtureDeliveryState::Archiving,
                        attempts: DeliveryAttemptCount(5),
                    }),
                    state_batch(StateBatchSetup {
                        bounds: Offset(1)..=Offset(1),
                        delivery: FixtureDeliveryState::Archiving,
                        attempts: DeliveryAttemptCount(2),
                    }),
                ],
            );
            state.take_pending_dlq()
        };

        let states = dispatch_and_wait(&mgr, &key, &cell, ranges, <[_]>::is_empty).await;
        let mut written = dlq.requests();
        written.sort_by_key(|request| request.first.0);

        assert!(
            (states, written)
                == (
                    Vec::new(),
                    vec![
                        request(ExpectedDlqRequestSetup {
                            bounds: Offset(0)..=Offset(0),
                            delivery_count: DeliveryCount(5),
                            cause: DlqCause::DeliveryCountExceeded
                        }),
                        request(ExpectedDlqRequestSetup {
                            bounds: Offset(1)..=Offset(1),
                            delivery_count: DeliveryCount(2),
                            ..Default::default()
                        }),
                    ],
                )
        );
    }
}
