//! Tests for the ticker itself: that it holds off one whole interval before
//! its first sweep, then sweeps and re-arms on that interval, returns when the
//! shutdown token is cancelled, and gives up rather than spinning when its
//! ticker dies.

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_metadata::NodeId;
use krabka_units::secs;

use super::*;
use crate::{
    log_retention::test_support::{expired_partition, segment_files},
    test_support::{BrokenTimer, TimerFailure, park_manual_timer as park},
};

/// Block until the sweep has counted `want` clean passes, or give up.
///
/// The counter is incremented at the end of `tick_all`, so a pass's evictions
/// are all on disk once it moves. Bounded real time, hang-guard only: the
/// cadence itself is the manual clock's, not this poll's.
async fn swept(metrics: &BrokerMetrics, want: u64) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if metrics.log_retention_runs_total.get() >= want {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_ticks_until_shutdown() {
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _};

    let (dir, registry) = crate::test_support::sweep_registry_fixture();
    let partition = expired_partition(
        &dir,
        crate::test_support::CommittedPartitionSetup {
            topic: "run-retain",
            leader: NodeId(7),
            ..Default::default()
        },
    )
    .await;
    let before = segment_files(&dir, "run-retain");
    registry.insert(
        "run-retain".into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );

    // Drive the sweep cadence on a manual timeline instead of wall-clock time.
    let interval = secs(300);
    let clock = ManualMonotonicClock::new_shared();
    let shutdown = CancellationToken::new();
    let metrics = BrokerMetrics::new();
    let task = tokio::spawn(run(
        Arc::clone(&registry),
        LogRetentionConfig {
            interval,
            timer: clock.new_timer(),
            metadata: None,
        },
        shutdown.clone(),
        metrics.clone(),
    ));

    // Nothing has swept yet: the first deadline is a whole `interval` out, so
    // a broker whose partitions still carry the default `LogConfig` does not
    // trim on the strength of it.
    assert!(park(&clock).await, "the sweep parks before its first pass");
    assert!(
        metrics.log_retention_runs_total.get() == 0,
        "no pass may run before the first interval elapses"
    );
    assert!(
        segment_files(&dir, "run-retain") == before,
        "and nothing may leave the disk before it"
    );

    // Advance one interval: the first sweep fires and evicts.
    clock
        .advance(interval.to_std())
        .expect("manual time moves forward");
    assert!(swept(&metrics, 1).await, "the first sweep should complete");
    let after_first = segment_files(&dir, "run-retain");
    assert!(
        after_first.len() < before.len(),
        "the first sweep should evict the expired segments"
    );

    // And again -- proving it re-arms and keeps ticking on the injected
    // cadence with no wall-clock time. The second sweep has nothing left to
    // evict, so the listing stays where the first one left it.
    assert!(
        park(&clock).await,
        "the sweep re-parks after its first pass"
    );
    clock
        .advance(interval.to_std())
        .expect("manual time moves forward");
    assert!(swept(&metrics, 2).await, "the second sweep should complete");
    assert!(segment_files(&dir, "run-retain") == after_first);

    shutdown.cancel();
    task.await.expect("retention task exits");
}

#[tokio::test]
async fn run_stops_without_sweeping_when_the_first_deadline_is_refused() {
    let (dir, registry) = crate::test_support::sweep_registry_fixture();
    let partition = expired_partition(
        &dir,
        crate::test_support::CommittedPartitionSetup {
            topic: "unarmable",
            leader: NodeId(7),
            ..Default::default()
        },
    )
    .await;
    let before = segment_files(&dir, "unarmable");
    registry.insert(
        "unarmable".into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );
    let timer = BrokenTimer::dead(TimerFailure::Registration);

    // Nobody cancels the token, so the task can only end by giving up on its
    // ticker -- and it gives up before it can sweep at all, so the expired
    // segments are left exactly where they were.
    let task = tokio::spawn(run(
        registry,
        LogRetentionConfig {
            interval: secs(300),
            timer: timer.injectable(),
            metadata: None,
        },
        CancellationToken::new(),
        BrokerMetrics::new(),
    ));
    task.await.expect("retention task exits");

    check!(segment_files(&dir, "unarmable") == before);
    check!(timer.registrations() == 1);
}
