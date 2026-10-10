//! Test doubles and fixtures that the delivery unit tests share.
//!
//! The helpers build a real [`Partition`] over a real [`Log`], because the
//! scheduler's whole job is to read a log's schedule. Nothing here spawns a
//! writer actor: the scheduler never sends the partition a message, so a
//! closed writer channel is enough.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::{DeliveryPolicy, Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::{Time, convert::TimeExt as _};
use qubit_clock::{ManualMonotonicClock, WallClock};
use tokio::sync::{Notify, mpsc};

use crate::{
    delivery::{DeliveryHandles, PartitionDelivery, metrics::DeliveryMetrics},
    partition::{
        Partition, WriterMessage, empty_marker_materialization, initial_replication_target,
    },
    partition_registry::PartitionRegistry,
};

/// A fixed clock reading, so a schedule in a test is exact rather than nearly
/// right.
pub(crate) const NOW_MS: i64 = 1_700_000_000_000;

/// The default clock-confidence bound, in milliseconds.
pub(crate) const BOUND_MS: i64 = 250;

/// The wall-clock instant that `epoch_ms` names.
///
/// It is the inverse of [`crate::time_util::epoch_millis`] over the constants
/// these tests are written in. 0.13 of qubit-clock re-exports no date type, so
/// a fixture that starts from a millisecond literal such as [`NOW_MS`] has to
/// build the [`SystemTime`] itself.
///
/// # Panics
///
/// Panics if `epoch_ms` is negative. Every constant here names an instant well
/// after the epoch.
pub(crate) fn wall_at(epoch_ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(epoch_ms).expect("a post-epoch instant"))
}

/// A two-record batch whose activation time is `activation_ms`.
pub(crate) fn batch_at(activation_ms: i64) -> RecordBatch {
    let mut batch = RecordBatch {
        base_timestamp: activation_ms,
        max_timestamp: activation_ms,
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    for delta in 0..2 {
        batch.records.push(Record {
            offset_delta: delta,
            key: Some(Bytes::from(format!("k{delta}"))),
            value: Some(Bytes::from(vec![b'v'; 64])),
            ..Record::default()
        });
    }
    batch
}

/// A partition over a log that holds one batch per entry of `activations`,
/// registered under `topic` with this broker as its leader.
///
/// `policy` decides whether the topic schedules delivery. The partition's
/// [`DeliveryHandles`] read `clock`, so an append and the scheduler agree on
/// one timeline.
#[derive(Clone)]
pub(crate) struct ScheduleSetup<'a> {
    pub topic: &'a str,
    pub policy: DeliveryPolicy,
    pub activations: Vec<crate::test_support::UnixMillis>,
    pub leader: krabka_ids::NodeId,
}

impl Default for ScheduleSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "scheduled",
            policy: DeliveryPolicy::Scheduled,
            activations: vec![],
            leader: krabka_ids::NodeId(7),
        }
    }
}

pub(crate) fn scheduled_partition(
    dir: &tempfile::TempDir,
    clock: &Arc<dyn WallClock>,
    setup: ScheduleSetup<'_>,
) -> Arc<Partition> {
    let ScheduleSetup {
        topic,
        policy,
        activations,
        leader,
    } = setup;
    let config = LogConfig {
        delivery_policy: policy,
        ..LogConfig::default()
    };
    let batches = activations
        .iter()
        .map(|activation_ms| batch_at(activation_ms.0));
    partition_with_batches(
        dir,
        clock,
        DeliveryPartitionSetup {
            topic,
            config,
            batches: batches.collect(),
            leader,
        },
    )
}

/// A partition over a log with `config` that holds `batches`, registered
/// under `topic` with this broker as its leader. The high watermark is the
/// log end offset.
pub(crate) struct DeliveryPartitionSetup<'a> {
    pub topic: &'a str,
    pub config: LogConfig,
    pub batches: Vec<RecordBatch>,
    pub leader: krabka_ids::NodeId,
}

impl Default for DeliveryPartitionSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            config: LogConfig::default(),
            batches: vec![],
            leader: krabka_ids::NodeId(7),
        }
    }
}

pub(crate) fn partition_with_batches(
    dir: &tempfile::TempDir,
    clock: &Arc<dyn WallClock>,
    setup: DeliveryPartitionSetup<'_>,
) -> Arc<Partition> {
    let DeliveryPartitionSetup {
        topic,
        config,
        batches,
        leader,
    } = setup;
    let partition_dir = crate::log_dir::partition_dir(dir.path(), topic, 0);
    std::fs::create_dir_all(&partition_dir).expect("create the partition directory");
    let mut log = Log::open(&partition_dir, config).expect("open the log");
    for mut batch in batches {
        log.append(&mut batch).expect("append a batch");
    }

    // A single-replica leader has acknowledged every record it holds the moment
    // its append returns, so seed the high watermark at the log end offset. A
    // `ListOffsets` answer for a client is bounded by that watermark, and a
    // partition left at the zero `ReplicaState::new` gives would report an
    // empty log to every sentinel that reads record data.
    let mut replica_state = crate::replica_state::ReplicaState::new();
    replica_state.hw = log.log_end_offset();

    let (tx, rx) = mpsc::channel::<WriterMessage>(1);
    // The scheduler sends the partition nothing, so no writer actor is needed.
    // Keeping the receiver alive stops the sender from reporting a dead writer.
    let writer = tokio::spawn(async move {
        let mut rx = rx;
        while rx.recv().await.is_some() {}
    });
    let log = Arc::new(Mutex::new(log));
    let delivery = DeliveryHandles::with_clock(Arc::clone(clock));
    delivery.publish_now(&log);
    Arc::new(Partition {
        topic: topic.to_owned(),
        index: PartitionIndex(0),
        log_dir: Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        log,
        writer_tx: tx,
        marker_materialization: empty_marker_materialization(),
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(tokio::sync::Mutex::new(replica_state)),
        hw_advance_notify: Arc::new(Notify::new()),
        delivery,
        current_leader: Arc::new(AtomicU64::new(leader.0)),
        current_leader_epoch: Arc::new(AtomicI32::new(0)),
        replication_target: initial_replication_target(None),
        diskless: false,
        writer_handle: Arc::new(Mutex::new(Some(writer))),
    })
}

/// Register `partition` under its own topic name.
pub(crate) fn register(registry: &PartitionRegistry, partition: &Arc<Partition>) {
    registry.insert(
        partition.topic.as_str().into(),
        partition.index,
        Arc::clone(partition),
    );
}

/// What one [`DeliveryMetrics`] call reported.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MetricEvent {
    Watermark(String, PartitionIndex, PartitionDelivery),
    Late(i64),
    Wakeup,
}

/// A [`DeliveryMetrics`] that keeps every call, so a test asserts on what the
/// scheduler reported instead of on a live registry.
#[derive(Default)]
pub(crate) struct RecordingMetrics {
    events: Mutex<Vec<MetricEvent>>,
}

impl RecordingMetrics {
    pub(crate) fn events(&self) -> Vec<MetricEvent> {
        self.events
            .lock()
            .expect("the event lock is not poisoned")
            .clone()
    }

    pub(crate) fn watermarks(&self) -> Vec<(String, PartitionIndex, PartitionDelivery)> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                MetricEvent::Watermark(topic, partition, delivery) => {
                    Some((topic, partition, delivery))
                }
                _ => None,
            })
            .collect()
    }

    pub(crate) fn wakeups(&self) -> usize {
        self.events()
            .iter()
            .filter(|event| **event == MetricEvent::Wakeup)
            .count()
    }

    pub(crate) fn lateness_ms(&self) -> Vec<i64> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                MetricEvent::Late(millis) => Some(millis),
                _ => None,
            })
            .collect()
    }

    fn push(&self, event: MetricEvent) {
        self.events
            .lock()
            .expect("the event lock is not poisoned")
            .push(event);
    }
}

impl DeliveryMetrics for RecordingMetrics {
    fn watermark_advanced(
        &self,
        topic: &str,
        partition: PartitionIndex,
        delivery: PartitionDelivery,
    ) {
        self.push(MetricEvent::Watermark(
            topic.to_owned(),
            partition,
            delivery,
        ));
    }

    fn activation_late(&self, lateness: Time) {
        self.push(MetricEvent::Late(lateness.millis_i64()));
    }

    fn scheduler_woke(&self) {
        self.push(MetricEvent::Wakeup);
    }
}

/// Poll `done` until it holds, yielding between tries so a task on the same
/// current-thread runtime gets to run. Reports whether it held.
pub(crate) async fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..1_000 {
        if done() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

/// Wait, on `clock`'s manual timeline, until `count` timers are parked on it.
///
/// The wait runs on a blocking thread, so it never stalls the current-thread
/// runtime that has to drive the scheduler task to its next park. The
/// five-second bound is a hang guard against a task that never parks, not a
/// timeout a passing test ever reaches.
pub(crate) async fn wait_parked(clock: &Arc<ManualMonotonicClock>, count: usize) -> bool {
    let clock = Arc::clone(clock);
    tokio::task::spawn_blocking(move || clock.wait_for_waiters(count, Duration::from_secs(5)))
        .await
        .expect("the timeline wait finishes")
}
