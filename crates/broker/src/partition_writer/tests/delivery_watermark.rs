//! Writer-loop tests for the delivery-watermark refresh that runs after
//! every message, on both a scheduled and an immediate topic.

use assert2::check;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;

use super::*;
use crate::{
    partition_writer::test_support::sample_batch,
    test_support::{EmptyPayloadBatchSetup, RecordCount},
};

#[tokio::test]
async fn a_produce_to_a_scheduled_topic_refreshes_the_mirror_and_rearms_the_scheduler() {
    use crate::delivery::{
        DeliveryWaker,
        test_support::{BOUND_MS, NOW_MS, batch_at, wait_until, wall_at},
    };

    let dir = tempdir().expect("tempdir");
    let config = LogConfig {
        delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
        ..LogConfig::default()
    };
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), config).expect("open scheduled log"),
    ));
    let timeline = qubit_clock::ManualMonotonicClock::new_shared();
    let delivery = DeliveryHandles::with_clock(timeline.new_wall_clock(wall_at(NOW_MS)));
    // The partition is adopted, and the scheduler sleeps for a full second.
    let waker = Arc::new(DeliveryWaker::new());
    waker.arm(NOW_MS + 1_000);
    delivery.adopt(&waker);

    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            topic: "scheduled".to_string(),
            delivery: delivery.clone(),
            ..Default::default()
        },
    );

    // A batch that is already active moves the watermark to the log end.
    let ack_rx = queue_batch(&tx, batch_at(NOW_MS - 60_000)).await;
    ack_rx.await.expect("ack").expect("append ok");
    // The writer publishes the watermark after it acks, so poll for it.
    check!(wait_until(|| delivery.watermark() == Offset(2)).await);

    // A batch that comes due inside the scheduler's sleep re-arms it.
    let woken = waker.woken();
    tokio::pin!(woken);
    let ack_rx = queue_batch(&tx, batch_at(NOW_MS + 200)).await;
    ack_rx.await.expect("ack").expect("append ok");

    // The pending batch holds the watermark where it was.
    tokio::time::timeout(std::time::Duration::from_secs(1), woken)
        .await
        .expect("the produce did not re-arm the delivery scheduler");
    check!(delivery.watermark() == Offset(2));
    check!(log.lock().unwrap().log_end_offset() == Offset(4));

    // Past the activation instant, the writer's own refresh releases it.
    timeline
        .advance(std::time::Duration::from_millis(
            u64::try_from(200 + BOUND_MS).expect("positive"),
        ))
        .expect("manual time moves forward");
    let ack_rx = queue_batch(&tx, batch_at(NOW_MS - 60_000)).await;
    ack_rx.await.expect("ack").expect("append ok");
    check!(wait_until(|| delivery.watermark() == Offset(6)).await);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn a_produce_to_an_immediate_topic_keeps_the_mirror_at_the_log_end() {
    let dir = tempdir().expect("tempdir");
    let log = open_default_log(dir.path());
    let delivery = DeliveryHandles::new();
    let (tx, rx) = mpsc::channel(1);
    let writer = spawn_writer(
        dir.path(),
        log.clone(),
        rx,
        WriterOptions {
            topic: "immediate".to_string(),
            delivery: delivery.clone(),
            ..Default::default()
        },
    );

    let ack_rx = queue_batch(
        &tx,
        sample_batch(EmptyPayloadBatchSetup {
            count: RecordCount(3),
        }),
    )
    .await;
    ack_rx.await.expect("ack").expect("append ok");

    check!(crate::delivery::test_support::wait_until(|| delivery.watermark() == Offset(3)).await);

    drop(tx);
    writer.await.expect("writer join");
}
