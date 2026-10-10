//! Tests for the KFC-1 scheduled-delivery gate, including one that drives a
//! real scheduled partition end to end through the per-partition pipeline.

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::{bytes, millis};

use super::*;
use crate::{
    config_keys::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE_IMMEDIATE, DELIVERY_SCHEDULE_MONOTONIC},
    handlers::produce::{
        framing::PartitionPayload,
        pipeline::{PartitionInput, process_partition},
        test_support::{encode_batch, image_with_overrides as image_with_delivery},
    },
};

// ── KFC-1 scheduled delivery ─────────────────────────────────────
//
// On a topic with `delivery.mode=scheduled` a batch's `max_timestamp` is
// the time it becomes visible to a consumer. The produce path rejects two
// kinds of batch with `INVALID_TIMESTAMP` (32), and does nothing at all for
// a topic that delivers immediately.

// The fixed clock reading the pure delivery-gate cases run against, so a
// schedule in a test is exact rather than nearly right.
const SCHEDULE_NOW_MS: i64 = 1_700_000_000_000;

// The topic-config overrides one delivery-gate table row applies.
type DeliveryOverrides = &'static [(&'static str, &'static str)];

// A one-record batch that asks to be delivered at `delivery_ms`.
fn batch_delivered_at(delivery_ms: i64) -> RecordBatch {
    RecordBatch {
        base_timestamp: delivery_ms,
        max_timestamp: delivery_ms,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn only_a_scheduled_topic_resolves_a_delivery_gate() {
    let cases: [(DeliveryOverrides, Option<DeliveryGate>, &str); 6] = [
        (&[], None, "no topic config at all"),
        (
            &[(DELIVERY_MODE, DELIVERY_MODE_IMMEDIATE)],
            None,
            "the explicit default",
        ),
        (
            // A mode that is not `scheduled` keeps Kafka's behavior, even
            // when the other two keys are set.
            &[
                (DELIVERY_MODE, "later"),
                (DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ],
            None,
            "a corrupt mode alongside the other keys",
        ),
        (
            &[(DELIVERY_MODE, DELIVERY_MODE_SCHEDULED)],
            Some(DeliveryGate {
                max_delay: Some(millis(604_800_000)),
            }),
            "scheduled, with both other keys defaulted",
        ),
        (
            &[
                (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
                (DELIVERY_MAX_DELAY_MS, "-1"),
                (DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ],
            Some(DeliveryGate { max_delay: None }),
            "scheduled, with the unbounded sentinel",
        ),
        (
            &[
                (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
                (DELIVERY_MAX_DELAY_MS, "90000"),
            ],
            Some(DeliveryGate {
                max_delay: Some(millis(90_000)),
            }),
            "scheduled, with an explicit bound",
        ),
    ];

    for (overrides, want, label) in cases {
        let image = image_with_delivery("t", overrides);
        check!(resolve_delivery_gate(&image, "t") == want, "case: {label}");
    }
}

#[test]
fn the_delivery_gate_bounds_only_how_far_ahead_a_batch_is_scheduled() {
    // `delivery.max.delay.ms` is the one bound this gate still holds: it
    // compares the batch against the broker's clock and reads no log state.
    // `delivery.schedule.monotonic` is the log's, under the lock that writes
    // the batch, and `crates/log/src/log/append/tests.rs` covers it there.
    let cases = [
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 59_999,
            false,
            "inside the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 60_000,
            false,
            "exactly at the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 60_001,
            true,
            "one millisecond past the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS - 86_400_000,
            false,
            "a day in the past is not a delay",
        ),
        (
            Some(<Time as TimeExt>::ZERO),
            SCHEDULE_NOW_MS,
            false,
            "a zero bound still takes the present instant",
        ),
        (
            Some(<Time as TimeExt>::ZERO),
            SCHEDULE_NOW_MS + 1,
            true,
            "a zero bound rejects the next millisecond",
        ),
        (
            None,
            i64::MAX,
            false,
            "the -1 sentinel removes the bound entirely",
        ),
    ];

    for (max_delay, delivery_ms, want, label) in cases {
        let gate = DeliveryGate { max_delay };
        check!(
            gate.rejects(delivery_ms, SCHEDULE_NOW_MS) == want,
            "case: {label}"
        );
    }
}

// Drive `process_partition` against a real scheduled partition: both
// rejections, then the batch that fits the schedule, which must reach the
// log as the producer's own bytes.
//
// The gate reads the broker's clock, so the delivery times here are
// relative to that reading and sit far from either boundary.
#[tokio::test]
async fn a_scheduled_partition_rejects_and_appends_by_delivery_time() {
    use krabka_protocol::owned::produce_response::PartitionProduceResponse;

    let dir = tempfile::tempdir().unwrap();
    let image = Arc::new(image_with_delivery(
        "sched",
        &[
            (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
            (DELIVERY_MAX_DELAY_MS, "3600000"),
            (DELIVERY_SCHEDULE_MONOTONIC, "true"),
        ],
    ));
    let delivery = resolve_delivery_gate(&image, "sched");
    let fixture =
        crate::handlers::produce::test_support::PipelineFixture::new(krabka_ids::NodeId(1));

    let part = fixture
        .partition_with_config(
            dir.path(),
            &image,
            crate::handlers::produce::test_support::PipelinePartitionSetup {
                topic: "sched",
                log_config: krabka_log::LogConfig {
                    delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
                    // The log enforces `delivery.schedule.monotonic`, so its
                    // configuration must match the topic's delivery settings.
                    schedule_order: krabka_log::ScheduleOrder::Monotonic,
                    ..krabka_log::LogConfig::default()
                },
            },
        )
        .await;

    // Seed offset 0 with a batch that comes due in ten minutes, so the
    // partition already carries a schedule to run backwards from.
    let now_ms = part.delivery.now_ms();
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch_delivered_at(now_ms + 600_000))
        .expect("seed the partition schedule");
    fixture
        .partitions
        .insert("sched".into(), krabka_ids::PartitionIndex(0), part);

    // The accepted case appends, so it comes last.
    let accepted_delivery_ms = now_ms + 900_000;
    let cases = [
        (
            now_ms + 300_000,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TIMESTAMP,
                base_offset: -1,
                ..Default::default()
            },
            "before the delivery time the partition already holds",
        ),
        (
            now_ms + 7_200_000,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TIMESTAMP,
                base_offset: -1,
                ..Default::default()
            },
            "further ahead than delivery.max.delay.ms",
        ),
        (
            accepted_delivery_ms,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::NONE,
                base_offset: 1,
                // An accepted row carries the partition's real log start
                // offset. Nothing has trimmed this one, so it is 0 and not
                // the -1 the two refusals above keep from `Default`.
                log_start_offset: 0,
                ..Default::default()
            },
            "after the schedule and inside the bound",
        ),
    ];

    for (delivery_ms, want, label) in cases {
        let resp = process_partition(
            PartitionInput {
                timestamps: crate::handlers::produce::topic_settings::TimestampPolicy::default(),
                delivery,
                ..crate::handlers::produce::test_support::pipeline_input(
                    "sched",
                    PartitionPayload::Slice(encode_batch(&batch_delivered_at(delivery_ms))),
                )
            },
            fixture.services(&image),
        )
        .await
        .expect("process partition")
        .expect_done();
        check!(resp == want, "case: {label}");
    }

    // The accepted batch took the verbatim path: the log holds the
    // producer's own bytes, with only `base_offset` (v2 header bytes 0..8)
    // and `partition_leader_epoch` (bytes 12..16) stamped. Both sit ahead
    // of the CRC's coverage, which is what lets the writer patch them
    // without re-encoding. A scheduled topic must keep that passthrough.
    let accepted_wire = encode_batch(&batch_delivered_at(accepted_delivery_ms));
    let mut want_bytes = accepted_wire.to_vec();
    want_bytes[0..8].copy_from_slice(&1_i64.to_be_bytes());
    want_bytes[12..16].copy_from_slice(&0_i32.to_be_bytes());
    let part = fixture
        .partitions
        .get("sched", krabka_ids::PartitionIndex(0))
        .expect("the partition is registered");
    let stored = part
        .log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .read_raw(Offset(1), Offset(2), bytes(4096))
        .expect("read the appended batch back");
    check!(stored.bytes == Bytes::from(want_bytes));
}
