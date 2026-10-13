//! The delivery-visibility state that one partition carries.
//!
//! Three things live here, and every one of them is a cache or a signal over
//! the log. None of them is durable state, because the schedule is the records
//! themselves: a restart or a leader change derives the same answer again.

use {
    crate::{
        delivery::{PartitionDelivery, waker::DeliveryWaker},
        time_util,
    },
    arc_swap::ArcSwapOption,
    krabka_ids::Offset,
    krabka_log::{DeliveryPolicy, Log},
    qubit_clock::{StdWallClock, WallClock},
    std::sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicI64, Ordering},
    },
    tokio::sync::Notify,
};

/// The delivery watermark mirror, the long-poll wake, and the scheduler poke of
/// one partition.
///
/// Cheap to clone: every field is an `Arc`. The partition owns one, and the
/// partition's writer actor owns a clone of the same handles, so the writer can
/// refresh the mirror after an append without reaching back through the
/// [`Partition`](crate::partition::Partition).
#[derive(Clone)]
pub(crate) struct DeliveryHandles {
    /// Delivery watermark as the last [`Self::publish`] left it.
    ///
    /// It is a cache for readers that must not take the log mutex, such as the
    /// metric sweep. It is never a substitute for the recompute a fetch does
    /// under that mutex, which is what makes a fetch correct.
    watermark: Arc<AtomicI64>,

    /// Fires when the watermark moved forward, so a long poll parked at the old
    /// value wakes and fetches the batch that just came due. A long-poll Fetch
    /// parks on this beside the partition's `append_notify`.
    pub(crate) advance_notify: Arc<Notify>,

    /// The broker-wide scheduler's poke, installed on the first sweep that sees
    /// this partition and empty until then. A partition that no scheduler has
    /// adopted yet still delivers on time; it waits for the scheduler's next
    /// sweep instead of re-arming it.
    rearm: Arc<ArcSwapOption<DeliveryWaker>>,

    /// Clock the writer reads when it refreshes the mirror after an append.
    /// The scheduler passes its own reading in, so this is only for the paths
    /// that have no clock of their own.
    clock: Arc<dyn WallClock>,
}

impl DeliveryHandles {
    /// Handles that read the system clock.
    pub(crate) fn new() -> Self { Self::with_clock(Arc::new(StdWallClock::new())) }

    /// Handles that read `clock`. A test passes a wall clock it pins or drives
    /// itself -- a [`FixedWallClock`](qubit_clock::FixedWallClock), or the
    /// [`ManualWallClock`](qubit_clock::ManualWallClock) that also feeds the
    /// scheduler -- so an append and the scheduler agree on one timeline.
    pub(crate) fn with_clock(clock: Arc<dyn WallClock>) -> Self {
        Self {
            watermark: Arc::new(AtomicI64::new(0)),
            advance_notify: Arc::new(Notify::new()),
            rearm: Arc::new(ArcSwapOption::empty()),
            clock,
        }
    }

    /// Delivery watermark as the last publish left it, with no lock and no I/O.
    pub(crate) fn watermark(&self) -> Offset { Offset(self.watermark.load(Ordering::Acquire)) }

    /// Recompute `log`'s delivery watermark against `now_ms`, publish it to the
    /// mirror, and wake a long poll when it moved forward.
    ///
    /// Returns `None` on a topic that delivers immediately. Such a topic has no
    /// schedule to track, so it never enters the scheduler's heap and never
    /// reports a metric series. The mirror is still refreshed, because
    /// `Log::advance_delivery_watermark` answers the log end offset for it
    /// before it reads a single batch header.
    pub(crate) fn publish(&self, log: &Mutex<Log>, now_ms: i64) -> Option<PartitionDelivery> {
        let (advance, scheduled, log_end) = {
            // Recover a poisoned guard rather than kill the caller. The log data
            // stays consistent enough to keep deriving a watermark from it, and
            // the alternative takes the partition's delivery off the air.
            let mut guard = log.lock().unwrap_or_else(PoisonError::into_inner);
            let scheduled = guard.config_snapshot().delivery_policy == DeliveryPolicy::Scheduled;
            let advance = guard.advance_delivery_watermark(now_ms);
            let log_end = guard.log_end_offset();
            (advance, scheduled, log_end)
        };

        let previous = self.watermark.swap(advance.watermark.0, Ordering::AcqRel);
        if advance.watermark.0 > previous {
            self.advance_notify.notify_waiters();
        }

        scheduled.then(|| PartitionDelivery {
            watermark: advance.watermark,
            pending: log_end.0 - advance.watermark.0,
            next_deadline_ms: advance.next_deadline_ms,
        })
    }

    /// [`Self::publish`] against this handle's own clock, for a caller that has
    /// none. The partition writer uses it after an append.
    pub(crate) fn publish_now(&self, log: &Mutex<Log>) -> Option<PartitionDelivery> {
        self.publish(log, time_util::epoch_millis(self.clock.now()))
    }

    /// This partition's clock reading, for a caller that already holds the log
    /// mutex and so cannot call [`Self::publish_now`] without deadlocking.
    ///
    /// A fetch is that caller. It recomputes the watermark under the guard it
    /// already has, and it must take its reading from the same clock the
    /// writer and the scheduler read, or a test that drives a manual timeline
    /// across an activation boundary cannot reach the fetch path at all.
    pub(crate) fn now_ms(&self) -> i64 { time_util::epoch_millis(self.clock.now()) }

    /// Install the scheduler's poke, so an append to this partition can re-arm
    /// the task. Idempotent: re-installing the same waker stores nothing.
    pub(crate) fn adopt(&self, waker: &Arc<DeliveryWaker>) {
        let installed = self.rearm.load();
        if installed
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, waker))
        {
            return;
        }
        self.rearm.store(Some(Arc::clone(waker)));
    }

    /// Re-arm the scheduler when `deadline_ms` falls before the instant it wakes
    /// on by itself. Reports whether it woke the task.
    pub(crate) fn wake_scheduler(&self, deadline_ms: i64) -> bool {
        self.rearm
            .load()
            .as_ref()
            .is_some_and(|waker| waker.wake_for(deadline_ms))
    }
}

impl Default for DeliveryHandles {
    fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for DeliveryHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliveryHandles")
            .field("watermark", &self.watermark())
            .field("adopted", &self.rearm.load().is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::delivery::test_support::{BOUND_MS, NOW_MS, batch_at, wall_at},
        assert2::check,
        krabka_log::LogConfig,
        qubit_clock::{FixedWallClock, ManualMonotonicClock},
        tempfile::tempdir,
    };

    /// A log, and the directory that holds it.
    struct LogInDir {
        dir: tempfile::TempDir,
        log: Mutex<Log>,
    }

    fn log_of(policy: DeliveryPolicy, activations: &[i64]) -> LogInDir {
        let dir = tempdir().expect("log root");
        let config = LogConfig {
            delivery_policy: policy,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).expect("open the log");
        for activation_ms in activations {
            log.append(&mut batch_at(*activation_ms))
                .expect("append a batch");
        }
        LogInDir {
            dir,
            log: Mutex::new(log),
        }
    }

    /// Handles pinned at `now_ms`, for a test that only reads the clock.
    fn handles_at(now_ms: i64) -> DeliveryHandles {
        DeliveryHandles::with_clock(Arc::new(FixedWallClock::new(wall_at(now_ms))))
    }

    /// Handles on a manual timeline that starts at `now_ms`, for the one test
    /// that has to carry the clock across an activation boundary. The clock it
    /// returns moves the wall reading it handed the handles.
    fn advancing_handles_at(now_ms: i64) -> AdvancingHandles {
        let clock = ManualMonotonicClock::new_shared();
        let handles = DeliveryHandles::with_clock(clock.new_wall_clock(wall_at(now_ms)));
        AdvancingHandles { clock, handles }
    }

    /// Handles, and the manual clock that moves them.
    struct AdvancingHandles {
        clock: Arc<ManualMonotonicClock>,
        handles: DeliveryHandles,
    }

    #[test]
    fn an_immediate_topic_reports_no_schedule_but_still_mirrors_the_log_end() {
        let LogInDir { dir: _dir, log } = log_of(DeliveryPolicy::Immediate, &[NOW_MS + 3_600_000]);
        let handles = handles_at(NOW_MS);

        check!(handles.publish_now(&log).is_none());
        check!(handles.watermark() == Offset(2));
    }

    #[test]
    fn a_scheduled_topic_reports_the_pending_count_and_the_next_deadline() {
        let LogInDir { dir: _dir, log } = log_of(DeliveryPolicy::Scheduled, &[
            NOW_MS - 60_000,
            NOW_MS + 10_000,
        ]);
        let handles = handles_at(NOW_MS);

        check!(
            handles.publish_now(&log)
                == Some(PartitionDelivery {
                    watermark: Offset(2),
                    pending: 2,
                    next_deadline_ms: Some(NOW_MS + 10_000 + BOUND_MS),
                })
        );
        check!(handles.watermark() == Offset(2));
    }

    #[tokio::test]
    async fn a_publish_that_moves_the_watermark_wakes_a_parked_reader() {
        let LogInDir { dir: _dir, log } = log_of(DeliveryPolicy::Scheduled, &[NOW_MS + 10_000]);
        let AdvancingHandles { clock, handles } = advancing_handles_at(NOW_MS);
        handles.publish_now(&log);
        check!(handles.watermark() == Offset(0));

        let parked = handles.advance_notify.notified();
        tokio::pin!(parked);
        // Register the waiter now: `notify_waiters` wakes only what is already
        // registered, and a `Notified` registers on its first poll.
        parked.as_mut().enable();
        clock
            .advance(std::time::Duration::from_millis(
                u64::try_from(10_000 + BOUND_MS).expect("positive"),
            ))
            .expect("manual time moves forward");

        check!(handles.publish_now(&log).is_some());
        check!(handles.watermark() == Offset(2));
        parked.await;
    }

    #[test]
    fn a_partition_no_scheduler_adopted_cannot_rearm_one() {
        let handles = handles_at(NOW_MS);
        check!(!handles.wake_scheduler(NOW_MS));

        let waker = Arc::new(DeliveryWaker::new());
        handles.adopt(&waker);
        // Idempotent: the second adopt keeps the same waker in place.
        handles.adopt(&waker);
        waker.arm(NOW_MS + 1_000);

        check!(handles.wake_scheduler(NOW_MS + 200));
        check!(!handles.wake_scheduler(NOW_MS + 5_000));
    }
}
