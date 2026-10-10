//! Tests for the ticker itself: that it sweeps once at startup, re-arms on
//! the injected interval, returns when the shutdown token is cancelled, and
//! gives up rather than spinning when its ticker dies.

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_units::secs;
use qubit_clock::Timer;

use super::*;
use crate::{
    cleaner::test_support::{compactable_partition, record_count},
    test_support::{BrokenTimer, TimerFailure},
};

#[tokio::test]
async fn run_ticks_until_shutdown() {
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

    let (dir, registry) = crate::test_support::sweep_registry_fixture();
    let partition = compactable_partition(
        &dir,
        crate::cleaner::test_support::CompactionSetup {
            topic: "run-compact",
            ..Default::default()
        },
    )
    .await;
    let before = record_count(&partition);
    registry.insert(
        "run-compact".into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );

    // Drive the sweep cadence on a manual timeline instead of wall-clock time.
    let interval = secs(30);
    let clock = ManualMonotonicClock::new_shared();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        Arc::clone(&registry),
        CleanerConfig {
            interval,
            timer: clock.new_timer(),
            metadata: None,
        },
        shutdown.clone(),
        BrokerMetrics::new(),
    ));

    // The immediate t=0 tick runs a compaction sweep, then the loop re-arms
    // on a deadline one `interval` out. Block (bounded real time, hang-guard
    // only) until the clock holds one waiter — the cleaner's parked interval
    // timer, which registers strictly after the first sweep's `tick_all`
    // returns, so the compaction is fully applied by then.
    // `wait_for_waiters` runs on a blocking thread so it never stalls the
    // current-thread runtime that must drive the cleaner task and the
    // partition writer actor to completion.
    let parked = crate::test_support::park_manual_timer(&clock).await;
    assert!(
        parked,
        "cleaner should park on the interval timer after the first sweep"
    );
    assert!(
        record_count(&partition) < before,
        "immediate first sweep should compact the eligible partition"
    );

    // Advance one interval to fire a second sweep, then confirm the loop
    // re-parks — proving it keeps ticking on the injected cadence with no
    // wall-clock time (the second sweep is idempotent, so the log stays
    // compacted rather than shrinking further).
    clock
        .advance(interval.to_std())
        .expect("manual time moves forward");
    let parked_again = crate::test_support::park_manual_timer(&clock).await;
    assert!(
        parked_again,
        "cleaner should re-park on the interval timer after the second sweep"
    );
    assert!(record_count(&partition) < before, "log stays compacted");

    shutdown.cancel();
    task.await.expect("cleaner task exits");
}

/// Spawns the cleaner over one compactable partition on `timer`, and returns
/// the task, the partition, and its pre-sweep record count.
///
/// The shutdown token handed to the task is dropped uncancelled, so the only
/// way the returned task can finish is the timer giving out.
async fn spawn_on(
    dir: &tempfile::TempDir,
    topic: &str,
    timer: Arc<dyn Timer>,
) -> (
    tokio::task::JoinHandle<()>,
    Arc<crate::partition::Partition>,
    usize,
) {
    let registry = Arc::new(PartitionRegistry::new());
    let (partition, before) =
        super::test_support::register_compactable(dir, &registry, topic).await;
    let task = tokio::spawn(run(
        registry,
        CleanerConfig {
            interval: secs(30),
            timer,
            metadata: None,
        },
        CancellationToken::new(),
        BrokerMetrics::new(),
    ));
    (task, partition, before)
}

/// Both initial timer failures end the task before it can compact the partition.
async fn check_initial_timer_failure(topic: &str, failure: TimerFailure) {
    let dir = tempfile::tempdir().expect("log root");
    let timer = BrokenTimer::dead(failure);
    let (task, partition, before) = spawn_on(&dir, topic, timer.injectable()).await;
    task.await.expect("cleaner task exits");
    check!(record_count(&partition) == before);
    check!(timer.registrations() == 1);
}

#[tokio::test]
async fn run_stops_without_sweeping_when_the_first_deadline_is_refused() {
    // Nobody cancels the token; refusal of the first deadline ends the task.
    check_initial_timer_failure("unarmable", TimerFailure::Registration).await;
}

#[tokio::test]
async fn run_stops_when_the_first_deadline_is_armed_but_never_completes() {
    // Registration succeeds, then the failed deadline ends the select loop.
    check_initial_timer_failure("unfired", TimerFailure::Completion).await;
}

#[tokio::test]
async fn run_sweeps_once_and_stops_when_the_interval_cannot_be_re_armed() {
    let dir = tempfile::tempdir().expect("log root");
    let timer = BrokenTimer::dead_after(1, TimerFailure::Registration);
    let (task, partition, before) = spawn_on(&dir, "unrearmable", timer.injectable()).await;

    // The start-up deadline is honoured, so the first sweep compacts the
    // partition. The interval the loop re-arms afterwards is refused, and the
    // task stops rather than retrying it: two registrations in all, not a
    // climbing count.
    task.await.expect("cleaner task exits");
    check!(record_count(&partition) < before);
    check!(timer.registrations() == 2);
}
