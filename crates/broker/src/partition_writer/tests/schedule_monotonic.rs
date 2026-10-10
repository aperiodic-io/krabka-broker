//! Writer-loop tests for KFC-1 `delivery.schedule.monotonic`, which the log
//! enforces under the lock that writes the batch.
//!
//! The writer batches up to `max_produce_group` queued jobs into one
//! `append_produce_batch` call, so a rule checked anywhere above the writer is
//! a rule two jobs of one group can walk straight past. These tests drive that
//! exact shape: two produces, one group, descending delivery times.

use assert2::check;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;

use super::*;
use crate::{
    codes,
    delivery::test_support::{NOW_MS, batch_at},
};

/// A scheduled, monotonic log.
fn monotonic_config() -> LogConfig {
    LogConfig {
        delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
        schedule_order: krabka_log::ScheduleOrder::Monotonic,
        ..LogConfig::default()
    }
}

/// Spawn the writer over `log` and hand back its sender.
///
/// Every argument but the log is the default the other writer-loop tests use;
/// nothing in this file reads a watermark, a notification or a producer state.
fn spawn_writer(
    dir: &std::path::Path,
    log: &Arc<Mutex<Log>>,
    rx: mpsc::Receiver<WriterMessage>,
) -> tokio::task::JoinHandle<()> {
    crate::partition_writer::test_support::spawn_writer(
        dir,
        log.clone(),
        rx,
        WriterOptions {
            topic: "scheduled".to_string(),
            ..Default::default()
        },
    )
}

async fn queued_descending_batches(
    tx: &mpsc::Sender<WriterMessage>,
) -> (
    crate::partition_writer::test_support::ProduceAck,
    crate::partition_writer::test_support::ProduceAck,
) {
    let later = queue_batch(tx, batch_at(NOW_MS + 60_000)).await;
    let earlier = queue_batch(tx, batch_at(NOW_MS)).await;
    (later, earlier)
}

/// Two produces whose delivery times descend, queued before the writer runs so
/// that its group drain takes both into one append call.
///
/// The later batch is admitted and the earlier one is refused with the error
/// the broker maps to `INVALID_TIMESTAMP` (32). The log holds exactly the one
/// batch that was admitted: two records, one per record of `batch_at`.
#[tokio::test]
async fn a_backwards_delivery_time_in_one_writer_group_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), monotonic_config()).expect("open the scheduled log"),
    ));

    // Both jobs are on the queue before the writer starts, so its first
    // `recv` and the `try_recv` behind it drain them into one group.
    let (tx, rx) = mpsc::channel(2);
    let (later, earlier) = queued_descending_batches(&tx).await;

    let writer = spawn_writer(dir.path(), &log, rx);

    let later = later.await.expect("ack the later batch");
    check!(later.expect("the later batch is admitted").base_offset == Offset(0));

    let earlier = earlier
        .await
        .expect("ack the earlier batch")
        .expect_err("a batch that runs the schedule backwards is refused");
    check!(matches!(
        &earlier,
        crate::error::BrokerError::Log(krabka_log::LogError::ScheduleRunsBackwards {
            delivery_ms
        }) if *delivery_ms == NOW_MS
    ));
    check!(codes::from_broker_error(&earlier) == codes::INVALID_TIMESTAMP);

    // The refusal appended nothing: the log holds the admitted batch alone.
    check!(log.lock().unwrap().log_end_offset() == Offset(2));

    drop(tx);
    writer.await.expect("writer join");
}

/// The same two produces on a scheduled topic that did not ask for the
/// setting. Both are admitted, because KFC-1 leaves a backwards schedule legal
/// by default and only reports it when an operator asks.
#[tokio::test]
async fn a_backwards_delivery_time_is_admitted_without_the_setting() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(
            dir.path(),
            LogConfig {
                schedule_order: krabka_log::ScheduleOrder::Unordered,
                ..monotonic_config()
            },
        )
        .expect("open the scheduled log"),
    ));

    let (tx, rx) = mpsc::channel(2);
    let (later, earlier) = queued_descending_batches(&tx).await;

    let writer = spawn_writer(dir.path(), &log, rx);

    check!(
        later
            .await
            .expect("ack the later batch")
            .expect("the later batch is admitted")
            .base_offset
            == Offset(0)
    );
    check!(
        earlier
            .await
            .expect("ack the earlier batch")
            .expect("the earlier batch is admitted too")
            .base_offset
            == Offset(2)
    );
    check!(log.lock().unwrap().log_end_offset() == Offset(4));

    drop(tx);
    writer.await.expect("writer join");
}
