//! KFC-10 deduplication on a KFC-1 scheduled topic.
//!
//! On `delivery.mode=scheduled` a batch's `max_timestamp` is its delivery
//! time, and `delivery.schedule.monotonic=true` refuses a batch whose delivery
//! time is below the partition's highest. A partial-drop rewrite recomputes
//! `max_timestamp` from the records it keeps, so dropping the record that
//! carried a batch's delivery time silently moves that batch's delivery time
//! earlier than the producer set.

use {
    crate::{
        Target,
        support::{
            dedup::{Rec, create_dedup_topic, keyed_batch},
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            start,
        },
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::{primitives::uuid::Uuid, records::RecordsPayload},
};

/// The `max_timestamp` of every stored batch of a partition, in offset order.
async fn stored_delivery_times(client: &Client, target: Target<'_>) -> Vec<i64> {
    let at_zero = fetch_partition()
        .partition(target.partition)
        .offset(0)
        .call();
    let request = single_partition_fetch()
        .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
        .topic_id(target.topic_id)
        .topic(target.topic.to_owned())
        .partition(at_zero)
        .call();
    let response = client.send(request).await.expect("Fetch");
    let row = &response.responses[0].partitions[0];
    assert!(row.error_code == 0);
    let as_v2 = row.records.as_ref().and_then(RecordsPayload::as_v2);
    as_v2
        .map(|batches| batches.iter().map(|batch| batch.max_timestamp).collect())
        .unwrap_or_default()
}

/// A dedup topic that is also scheduled and monotonic.
async fn scheduled_dedup_topic(client: &Client, broker: &krabka_broker::BrokerHandle) -> Uuid {
    create_dedup_topic()
        .broker(broker)
        .client(client)
        .topic("scheduled")
        .window_ms(3_600_000)
        .extra_configs(&[
            ("delivery.mode", "scheduled"),
            ("delivery.schedule.monotonic", "true"),
            // Admit delivery times well into the future so the batches below
            // are not refused for their delay or their timestamp window.
            ("delivery.max.delay.ms", "-1"),
            ("message.timestamp.after.max.ms", "86400000"),
        ])
        .call()
        .await
}

/// A batch whose first record is due soonest and whose last record is due
/// latest: its `max_timestamp`, the batch's delivery time, is the last
/// record's. `early_key` is the earliest-due record's key.
fn ascending_batch(early_key: &str, late_key: &str) -> krabka_protocol::records::RecordBatch {
    // `aged` dates a record before now; a negative age dates it ahead. The
    // late record is due an hour from now, the early one half an hour.
    keyed_batch()
        .records(&[
            Rec::key(early_key).aged(-1_800_000),
            Rec::key(late_key).aged(-3_600_000),
        ])
        .call()
}

/// Dropping the latest-due record of a batch as a duplicate rewrites the
/// batch. A rewrite that recomputed `max_timestamp` from the records it keeps
/// would lower the batch's delivery time to the earliest kept record's, so
/// records the producer scheduled for later would become due earlier, and on
/// a monotonic partition the lowered delivery time would fall below the
/// partition's high-water and the log would reject the batch with
/// `INVALID_TIMESTAMP` (32). The rewrite must keep the producer's
/// `max_timestamp`, so the batch appends and every stored batch keeps the
/// delivery time the producer set.
///
/// Target: coordinator finding 6. This passes once the rewrite preserves
/// `max_timestamp`; before that fix the second produce was rejected (32).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_drop_keeps_a_scheduled_batchs_delivery_time() {
    let p = start().await;
    let topic_id = scheduled_dedup_topic(&p.client, &p.broker).await;
    let target = Target {
        topic: "scheduled",
        topic_id,
        partition: 0,
    };

    // Seed the identity that the next batch's latest-due record duplicates.
    let seed = target
        .send(&p.client, ascending_batch("early-0", "late"))
        .await;
    assert!(seed.error_code == 0);
    // A second batch whose latest-due record ("late") duplicates the seed, so
    // the leader drops it and rewrites the batch around the earlier-due
    // "early-1". The rewrite recomputes `max_timestamp` down to "early-1"'s
    // delivery time, which is below the partition's monotonic high-water, so
    // the monotonic gate in the log rejects the rewritten batch with
    // INVALID_TIMESTAMP (32). Without dedup the producer's own batch, whose
    // delivery time equals the high-water, would append.
    let partial = target
        .send(&p.client, ascending_batch("early-1", "late"))
        .await;
    assert!(
        partial.error_code == 0,
        "the dedup rewrite lowered the batch's delivery time and the monotonic gate rejected it: \
         error_code {}",
        partial.error_code
    );

    let delivery_times = stored_delivery_times(&p.client, target).await;
    // Every stored batch must still advertise the hour-ahead delivery time the
    // producer set; the rewrite must not have lowered the second batch to the
    // half-hour-ahead time of its surviving record. (Reached only once the
    // rejection above is fixed.)
    assert!(
        delivery_times.iter().all(|&t| delivery_times[0] == t),
        "delivery times moved: {delivery_times:?}"
    );
    p.broker.shutdown().await;
}
