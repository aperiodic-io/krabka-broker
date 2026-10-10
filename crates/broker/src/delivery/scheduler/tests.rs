use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use krabka_ids::{Offset, PartitionIndex};
use krabka_log::DeliveryPolicy;
use krabka_metadata::NodeId;
use krabka_units::{millis, secs};
use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer, WallClock};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    delivery::{
        PartitionDelivery,
        metrics::NoDeliveryMetrics,
        test_support::{
            BOUND_MS, NOW_MS, RecordingMetrics, register, scheduled_partition, wait_parked,
            wait_until, wall_at,
        },
    },
    test_support::{BrokenTimer, TimerFailure},
};

const THIS_BROKER: u64 = 7;

fn key(topic: &str) -> PartitionKey {
    (topic.to_owned(), PartitionIndex(0))
}

#[test]
fn the_heap_answers_the_earliest_deadline_still_recorded() {
    let mut heap = DeadlineHeap::default();
    heap.set(key("a"), 300);
    heap.set(key("b"), 100);
    heap.set(key("c"), 200);

    check!(heap.earliest() == Some(100));
    check!(heap.deadline(&key("b")) == Some(100));

    heap.forget(&key("b"));
    check!(heap.earliest() == Some(200));
    check!(heap.deadline(&key("b")).is_none());
}

#[test]
fn a_later_deadline_for_the_same_partition_supersedes_the_earlier_one() {
    let mut heap = DeadlineHeap::default();
    heap.set(key("a"), 100);
    heap.set(key("a"), 900);

    check!(heap.earliest() == Some(900));
}

#[test]
fn the_heap_drops_partitions_that_are_no_longer_live() {
    let mut heap = DeadlineHeap::default();
    heap.set(key("kept"), 100);
    heap.set(key("gone"), 50);

    let live: HashSet<PartitionKey> = std::iter::once(key("kept")).collect();
    heap.retain(&live);

    check!(heap.earliest() == Some(100));
    check!(heap.deadline(&key("gone")).is_none());
}

#[test]
fn superseded_entries_do_not_grow_the_heap_without_bound() {
    let mut heap = DeadlineHeap::default();
    for deadline_ms in 0..200 {
        heap.set(key("a"), deadline_ms);
    }

    check!(heap.heap.len() <= 20);
    check!(heap.earliest() == Some(199));
}

#[test]
fn the_sleep_is_capped_by_the_idle_bound_and_floored_by_the_minimum() {
    let config = DeliveryConfig {
        idle_sleep: secs(1),
        min_sleep: millis(10),
        ..DeliveryConfig::default()
    };
    // (deadline, expected sleep in milliseconds)
    let cases = [
        (None, 1_000),
        // Further out than the idle bound: capped.
        (Some(NOW_MS + 60_000), 1_000),
        // Inside the bound: slept exactly.
        (Some(NOW_MS + 400), 400),
        // Already due, and one in the past: floored, never zero.
        (Some(NOW_MS), 10),
        (Some(NOW_MS - 5_000), 10),
    ];
    for (deadline_ms, expected_ms) in cases {
        let wait = sleep_for(deadline_ms, NOW_MS, &config);
        check!(
            wait == Duration::from_millis(expected_ms),
            "deadline {deadline_ms:?}"
        );
    }
}

/// A scheduler wired to a manual timeline, with the registry and the recorder
/// a test asserts on.
///
/// `timeline` is the whole of time here: the wall reading the scheduler and
/// every partition take, and the cadence its ticker parks on, both come out of
/// it, so advancing it moves the two together.
struct Harness {
    registry: Arc<PartitionRegistry>,
    metrics: Arc<RecordingMetrics>,
    waker: Arc<DeliveryWaker>,
    shutdown: CancellationToken,
    timeline: Arc<ManualMonotonicClock>,
    clock: Arc<dyn WallClock>,
}

impl Harness {
    fn new() -> Self {
        let timeline = ManualMonotonicClock::new_shared();
        Self {
            registry: Arc::new(PartitionRegistry::new()),
            metrics: Arc::new(RecordingMetrics::default()),
            waker: Arc::new(DeliveryWaker::new()),
            shutdown: CancellationToken::new(),
            clock: timeline.new_wall_clock(wall_at(NOW_MS)),
            timeline,
        }
    }

    fn with_partition(
        setup: crate::delivery::test_support::ScheduleSetup<'_>,
    ) -> (tempfile::TempDir, Self, Arc<crate::partition::Partition>) {
        let dir = tempfile::tempdir().expect("log root");
        let harness = Self::new();
        let partition = harness.scheduled(&dir, setup);
        (dir, harness, partition)
    }

    fn scheduled(
        &self,
        dir: &tempfile::TempDir,
        setup: crate::delivery::test_support::ScheduleSetup<'_>,
    ) -> Arc<crate::partition::Partition> {
        let partition = scheduled_partition(dir, &self.clock, setup);
        register(&self.registry, &partition);
        partition
    }

    fn spawn(&self) -> tokio::task::JoinHandle<()> {
        self.spawn_on(self.timeline.new_timer())
    }

    /// Spawns the scheduler on `timer` rather than on the harness timeline, so
    /// that a test can hand the loop a ticker that gives out.
    fn spawn_on(&self, timer: Arc<dyn Timer>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(run(
            Arc::clone(&self.registry),
            NodeId(THIS_BROKER),
            DeliveryConfig {
                idle_sleep: secs(1),
                min_sleep: millis(1),
                clock: Arc::clone(&self.clock),
                timer,
            },
            Arc::clone(&self.metrics) as Arc<dyn DeliveryMetrics>,
            Arc::clone(&self.waker),
            self.shutdown.clone(),
        ))
    }
}

#[tokio::test]
async fn a_batch_that_is_not_due_holds_the_watermark_and_then_releases_it() {
    // One batch that is already active, then one that comes due in 10s.
    let (_dir, harness, partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            activations: vec![
                crate::test_support::UnixMillis(NOW_MS - 60_000),
                crate::test_support::UnixMillis(NOW_MS + 10_000),
            ],
            ..Default::default()
        });

    let task = harness.spawn();
    check!(wait_parked(&harness.timeline, 1).await);

    // The first two records are visible; the pending batch holds the rest.
    check!(partition.delivery_watermark() == Offset(2));
    let advance_notified = partition.delivery.advance_notify.notified();
    tokio::pin!(advance_notified);
    // Register the waiter now. `notify_waiters` wakes only what is already
    // registered, and a `Notified` registers on its first poll.
    advance_notified.as_mut().enable();

    // Cross the activation boundary: the batch is due once the declared clock
    // bound has also elapsed.
    harness
        .timeline
        .advance(Duration::from_millis(
            u64::try_from(10_000 + BOUND_MS).expect("positive"),
        ))
        .expect("manual time moves forward");
    // Wait on the watermark itself, not on the park count. A fired timer stays
    // registered on the timeline until the woken task drops it, so a second
    // `wait_parked` here can be satisfied by the park the advance was meant to
    // end and return before the sweep has run.
    let watching = Arc::clone(&partition);
    check!(wait_until(move || watching.delivery_watermark() == Offset(4)).await);
    // A consumer parked at the old watermark was woken rather than left to
    // time out.
    advance_notified.await;

    let lateness = harness.metrics.lateness_ms();
    check!(lateness.len() == 1, "{lateness:?}");
    check!(
        lateness[0] == 0,
        "the manual timeline lands exactly on the deadline"
    );

    harness.shutdown.cancel();
    task.await.expect("the scheduler task exits");
}

#[tokio::test]
async fn a_topic_that_delivers_immediately_reports_nothing() {
    let (_dir, harness, immediate) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "immediate",
            policy: DeliveryPolicy::Immediate,
            activations: vec![crate::test_support::UnixMillis(NOW_MS + 10_000)],
            ..Default::default()
        });

    let task = harness.spawn();
    check!(wait_parked(&harness.timeline, 1).await);

    // Everything durable is visible, and the partition creates no series.
    check!(immediate.delivery_watermark() == Offset(2));
    check!(harness.metrics.watermarks().is_empty());
    check!(harness.metrics.wakeups() >= 1);

    harness.shutdown.cancel();
    task.await.expect("the scheduler task exits");
}

#[tokio::test]
async fn a_partition_this_broker_does_not_lead_is_left_alone() {
    let (_dir, harness, followed) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "followed",
            activations: vec![crate::test_support::UnixMillis(NOW_MS - 60_000)],
            leader: krabka_ids::NodeId(THIS_BROKER + 1),
            ..Default::default()
        });

    let task = harness.spawn();
    check!(wait_parked(&harness.timeline, 1).await);

    check!(harness.metrics.watermarks().is_empty());
    // The scheduler never adopts a partition it does not lead, so an append
    // there cannot re-arm it.
    check!(!followed.delivery.wake_scheduler(NOW_MS));

    harness.shutdown.cancel();
    task.await.expect("the scheduler task exits");
}

#[tokio::test]
async fn the_scheduler_adopts_a_leader_partition_so_a_produce_can_rearm_it() {
    let (_dir, harness, partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "adopted",
            activations: vec![crate::test_support::UnixMillis(NOW_MS - 60_000)],
            ..Default::default()
        });

    let task = harness.spawn();
    check!(wait_parked(&harness.timeline, 1).await);

    // Nothing waits, so the task sleeps on its idle bound and any nearer
    // deadline re-arms it.
    check!(harness.waker.wakes_at_ms() == NOW_MS + 1_000);
    let swept = harness.metrics.watermarks().len();
    check!(partition.delivery.wake_scheduler(NOW_MS + 200));

    // The poke drives a sweep of its own, without the manual timeline moving.
    let metrics = Arc::clone(&harness.metrics);
    check!(wait_until(move || metrics.watermarks().len() > swept).await);

    harness.shutdown.cancel();
    task.await.expect("the scheduler task exits");
}

fn reported_sweep(
    harness: &Harness,
    metrics: &dyn DeliveryMetrics,
) -> (DeadlineHeap, Arc<DeliveryWaker>) {
    let mut heap = DeadlineHeap::default();
    let waker = Arc::new(DeliveryWaker::new());
    sweep(
        (harness.registry.as_ref(), NodeId(THIS_BROKER)),
        (NOW_MS, true),
        &mut heap,
        (metrics, &waker),
    );
    (heap, waker)
}

#[tokio::test]
async fn the_sweep_reports_the_watermark_and_the_pending_count() {
    let (_dir, harness, _partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "reported",
            activations: vec![
                crate::test_support::UnixMillis(NOW_MS - 60_000),
                crate::test_support::UnixMillis(NOW_MS + 10_000),
            ],
            ..Default::default()
        });

    let (mut heap, _waker) = reported_sweep(&harness, harness.metrics.as_ref());

    let watermarks = harness.metrics.watermarks();
    assert!(let [_] = watermarks.as_slice(), "{watermarks:?}");
    let (topic, partition, delivery) = &watermarks[0];
    check!(topic == "reported");
    check!(*partition == PartitionIndex(0));
    check!(
        *delivery
            == PartitionDelivery {
                watermark: Offset(2),
                pending: 2,
                next_deadline_ms: Some(NOW_MS + 10_000 + BOUND_MS),
            }
    );
    check!(heap.earliest() == Some(NOW_MS + 10_000 + BOUND_MS));
}

#[tokio::test]
async fn a_sweep_with_no_metrics_of_its_own_still_advances_the_watermark() {
    let (_dir, harness, partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "quiet",
            activations: vec![crate::test_support::UnixMillis(NOW_MS - 60_000)],
            ..Default::default()
        });

    let (mut heap, _waker) = reported_sweep(&harness, &NoDeliveryMetrics);

    check!(partition.delivery_watermark() == Offset(2));
    check!(heap.earliest().is_none());
}

async fn check_dead_start(
    harness: &Harness,
    timer: &BrokenTimer,
    task: tokio::task::JoinHandle<()>,
) {
    task.await.expect("the scheduler task exits");
    check!(harness.metrics.wakeups() == 0);
    check!(timer.registrations() == 1);
}

#[tokio::test]
async fn the_scheduler_stops_without_sweeping_when_the_first_deadline_is_refused() {
    let (_dir, harness, _partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "unarmable",
            activations: vec![crate::test_support::UnixMillis(NOW_MS + 10_000)],
            ..Default::default()
        });

    let timer = BrokenTimer::dead(TimerFailure::Registration);
    let task = harness.spawn_on(timer.injectable());

    // Nothing cancels the shutdown token, so the refused start-up deadline is
    // the only thing that can end the task — and it ends it before the first
    // sweep, so the loop reports no wake at all.
    check_dead_start(&harness, &timer, task).await;
}

#[tokio::test]
async fn the_scheduler_stops_when_the_first_deadline_is_armed_but_never_completes() {
    let harness = Harness::new();
    let timer = BrokenTimer::dead(TimerFailure::Completion);
    let task = harness.spawn_on(timer.injectable());

    // The registration is accepted, so the loop reaches its select; the
    // deadline then fails, which is the other way a ticker goes away.
    check_dead_start(&harness, &timer, task).await;
}

#[tokio::test]
async fn the_scheduler_sweeps_once_and_stops_when_the_next_sleep_cannot_be_armed() {
    let (_dir, harness, partition) =
        Harness::with_partition(crate::delivery::test_support::ScheduleSetup {
            topic: "unrearmable",
            activations: vec![crate::test_support::UnixMillis(NOW_MS - 60_000)],
            ..Default::default()
        });

    let timer = BrokenTimer::dead_after(1, TimerFailure::Registration);
    let task = harness.spawn_on(timer.injectable());

    // The start-up deadline is honoured, so the loop takes exactly one sweep
    // and publishes the batch that is already due. The sleep it arms afterwards
    // is refused, and the task stops rather than retrying it: two registrations
    // in all, not a climbing count.
    task.await.expect("the scheduler task exits");
    check!(harness.metrics.wakeups() == 1);
    check!(partition.delivery_watermark() == Offset(2));
    check!(timer.registrations() == 2);
}
