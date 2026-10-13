//! The broker-wide deliver-at-time scheduler.
//!
//! One task per broker, not one per partition. It holds a min-heap of the next
//! activation deadline of every scheduled partition this broker leads, sleeps
//! until the earliest of them, advances the partitions that came due, and
//! re-arms.
//!
//! The task exists for liveness only. A fetch recomputes the watermark under
//! the log mutex, so a consumer that keeps polling sees every batch on time
//! whatever this task does. What the task adds is the wake for a consumer that
//! is *parked* in a long poll with no fetch in flight to do that recompute.
//! See the [module documentation](crate::delivery).
//!
//! A partition whose next batch is not due yet costs nothing on a sweep: its
//! deadline sits in the heap and the loop skips it. A partition with nothing
//! waiting is advanced on every sweep, which is one uncontended mutex and no
//! I/O. A partition whose topic delivers immediately leaves the heap on the
//! sweep that finds it and reports no metric series.

use {
    crate::{
        delivery::{DeliveryWaker, config::DeliveryConfig, metrics::DeliveryMetrics},
        partition_registry::PartitionRegistry,
        time_util,
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::NodeId,
    krabka_units::{Time, convert::TimeExt as _},
    std::{
        cmp::Reverse,
        collections::{BinaryHeap, HashMap, HashSet},
        sync::{Arc, atomic::Ordering},
        time::Duration,
    },
    tokio_util::sync::CancellationToken,
    tracing::debug,
};

/// Names this cadence loop in the error a failed timer registration logs.
const TASK: &str = "delivery scheduler";

/// How the scheduler names a partition in its own bookkeeping.
type PartitionKey = (String, PartitionIndex);

/// Min-heap of activation deadlines, keyed on the epoch-millisecond instant at
/// which a partition's first waiting batch becomes visible.
///
/// The heap holds superseded entries rather than paying to remove them, and
/// `deadlines` is what makes an entry authoritative. [`Self::set`] rebuilds the
/// heap once the stale entries outnumber the live ones, so the memory stays
/// proportional to the partitions this broker leads.
#[derive(Default)]
struct DeadlineHeap {
    deadlines: HashMap<PartitionKey, i64>,
    heap: BinaryHeap<Reverse<(i64, PartitionKey)>>,
}

impl DeadlineHeap {
    /// The deadline recorded for `key`, or `None` when nothing waits on it.
    fn deadline(&self, key: &PartitionKey) -> Option<i64> { self.deadlines.get(key).copied() }

    /// Record that `key`'s first waiting batch becomes visible at `deadline_ms`.
    fn set(&mut self, key: PartitionKey, deadline_ms: i64) {
        self.deadlines.insert(key.clone(), deadline_ms);
        self.heap.push(Reverse((deadline_ms, key)));
        if self.heap.len() > self.deadlines.len() * 2 + 16 {
            self.rebuild();
        }
    }

    /// Drop `key`, because nothing waits on it any more.
    fn forget(&mut self, key: &PartitionKey) { self.deadlines.remove(key); }

    /// Drop every partition that is not in `live`, because this broker no
    /// longer leads it or the registry no longer holds it.
    fn retain(&mut self, live: &HashSet<PartitionKey>) {
        if self.deadlines.keys().all(|key| live.contains(key)) {
            return;
        }
        self.deadlines.retain(|key, _| live.contains(key));
        self.rebuild();
    }

    /// The earliest deadline still recorded, or `None` when nothing waits.
    fn earliest(&mut self) -> Option<i64> {
        loop {
            let Reverse((deadline_ms, key)) = self.heap.peek()?;
            if self.deadlines.get(key) == Some(deadline_ms) {
                return Some(*deadline_ms);
            }
            self.heap.pop();
        }
    }

    fn rebuild(&mut self) {
        self.heap = self
            .deadlines
            .iter()
            .map(|(key, deadline_ms)| Reverse((*deadline_ms, key.clone())))
            .collect();
    }
}

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    node_id: NodeId,
    config: DeliveryConfig,
    metrics: Arc<dyn DeliveryMetrics>,
    waker: Arc<DeliveryWaker>,
    shutdown: CancellationToken,
) {
    let mut heap = DeadlineHeap::default();
    let timer = Arc::clone(&config.timer);
    // A zero-length first sleep makes the loop sweep at startup, the way
    // `tokio::time::interval` fires at t=0. Every later sleep is armed after the
    // sweep finishes, so a slow sweep never triggers a catch-up burst.
    let Some(mut tick) = time_util::arm(&*timer, Duration::ZERO, TASK) else {
        return;
    };
    // The first sweep adopts every partition, so it advances all of them.
    let mut advance_all = true;
    loop {
        tokio::select! {
            outcome = &mut tick => {
                if !time_util::fired(outcome, TASK) {
                    return;
                }
            }
            () = waker.woken() => advance_all = true,
            () = shutdown.cancelled() => {
                debug!("delivery scheduler shutting down");
                return;
            }
        }

        metrics.scheduler_woke();
        sweep(
            (&partitions, node_id),
            (time_util::epoch_millis(config.clock.now()), advance_all),
            &mut heap,
            (metrics.as_ref(), &waker),
        );
        advance_all = false;

        let now_ms = time_util::epoch_millis(config.clock.now());
        let wait = sleep_for(heap.earliest(), now_ms, &config);
        // Publish the wake instant before the sleep is armed. A produce that
        // pokes in between compares against the value that is about to hold,
        // and the notification permit outlives the gap either way.
        waker.arm(now_ms.saturating_add(time_util::duration_millis(wait)));
        let Some(next) = time_util::arm(&*timer, wait, TASK) else {
            return;
        };
        tick = next;
    }
}

/// Advance the partitions that came due, and refresh the heap.
///
/// `advance_all` forces every scheduled partition through a recompute, rather
/// than only the ones the heap says are due. The first sweep sets it so the
/// heap gets populated, and a produce that re-armed the task sets it because
/// the task does not know which partition asked for the earlier deadline.
fn sweep(
    registry: (&PartitionRegistry, NodeId),
    clock: (i64, bool),
    heap: &mut DeadlineHeap,
    reporting: (&dyn DeliveryMetrics, &Arc<DeliveryWaker>),
) {
    let (partitions, node_id) = registry;
    let (now_ms, advance_all) = clock;
    let (metrics, waker) = reporting;

    let mut live: HashSet<PartitionKey> = HashSet::new();
    for partition in partitions.arcs() {
        let key = (partition.topic.clone(), partition.index);
        if partition.current_leader.load(Ordering::Relaxed) != node_id {
            heap.forget(&key);
            continue;
        }
        live.insert(key.clone());

        let waiting = heap.deadline(&key);
        if !advance_all && waiting.is_some_and(|deadline_ms| deadline_ms > now_ms) {
            continue;
        }

        let previous = partition.delivery_watermark();
        let Some(delivery) = partition.advance_delivery_watermark(now_ms) else {
            // The topic delivers immediately. It has no schedule to track, and
            // it never gets the poke, so an append there re-arms nothing.
            heap.forget(&key);
            continue;
        };
        partition.adopt_delivery_waker(waker);

        // The batch this partition was waiting on is visible now, so the wait
        // is over and its length is what an operator wants to see.
        if let Some(deadline_ms) = waiting
            && deadline_ms <= now_ms
            && delivery.watermark > previous
        {
            metrics.activation_late(lateness(now_ms - deadline_ms));
        }
        metrics.watermark_advanced(&partition.topic, partition.index, delivery);

        match delivery.next_deadline_ms {
            Some(next_ms) => heap.set(key, next_ms),
            None => heap.forget(&key),
        }
    }
    heap.retain(&live);
}

/// How long to sleep before the next sweep.
///
/// The idle bound caps every sleep, so a partition that entered the registry
/// since the last sweep waits at most that long to be found. The floor keeps a
/// deadline that is already in the past from spinning the task.
fn sleep_for(deadline_ms: Option<i64>, now_ms: i64, config: &DeliveryConfig) -> Duration {
    let idle = config.idle_sleep.to_std();
    let wait = deadline_ms.map_or(idle, |deadline_ms| {
        let remaining = deadline_ms.saturating_sub(now_ms).max(0);
        Duration::from_millis(u64::try_from(remaining).unwrap_or(u64::MAX)).min(idle)
    });
    wait.max(config.min_sleep.to_std())
}

/// A non-negative millisecond count as a [`Time`].
fn lateness(delta_ms: i64) -> Time {
    Time::from_std(Duration::from_millis(u64::try_from(delta_ms).unwrap_or(0)))
}

#[cfg(test)]
mod tests;
