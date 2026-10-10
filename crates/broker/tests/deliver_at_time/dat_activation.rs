//! What happens to a record between its produce and its delivery time: a read
//! taken before that time must not serve it, and a consumer already parked in a
//! long poll must be woken by the delivery advance rather than by the poll
//! expiring.
//!
//! Both cases run once on a scheduled topic and once on an immediate one, and
//! the immediate half is what shows the behaviour is the configuration.

use std::time::Instant;

use assert2::check;

use crate::{
    dat_fixtures::{ACTIVATION_DELAY_MS, IMMEDIATE, Mode, SCHEDULED, Visible, batch_at, now_ms},
    dat_wire::{fetch_values, produce, ready_topic, visible, wait_until_visible},
    support,
};

async fn produce_due_soon(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) -> i64 {
    let deliver_at_ms = now_ms() + ACTIVATION_DELAY_MS;
    produce(
        client,
        topic,
        topic_id,
        batch_at(deliver_at_ms, &["due-soon"]),
    )
    .await;
    deliver_at_ms
}

/// `max.wait.ms` of the long poll in
/// [`a_parked_long_poll_wakes_when_the_record_comes_due`]. A consumer that the
/// delivery advance does not wake waits all of it out.
const LONG_POLL_MS: i32 = 20_000;

/// What the long poll has to beat to prove that it woke rather than expired.
/// The record it waits for comes due after [`ACTIVATION_DELAY_MS`] plus the
/// declared clock bound, so the two outcomes are seconds apart.
const LONG_POLL_WOKE_MS: u128 = 10_000;

#[tokio::test]
async fn a_record_stamped_in_the_future_waits_for_its_delivery_time() {
    struct Case {
        mode: Mode,
        /// What a read taken before the record's delivery time serves.
        before_delivery: Visible,
        /// Whether the record first reached a consumer at or after its delivery
        /// time. Only a scheduled topic holds it that long.
        held_until_delivery: bool,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            before_delivery: Visible::of(0, &[]),
            held_until_delivery: true,
        },
        Case {
            mode: IMMEDIATE,
            before_delivery: Visible::of(1, &["due-soon"]),
            held_until_delivery: false,
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-wait-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let deliver_at_ms = produce_due_soon(&p.client, &topic, topic_id).await;

        let seen = visible(&p.client, &topic, topic_id).await;
        let read_at_ms = now_ms();
        check!(
            read_at_ms < deliver_at_ms,
            "{topic}: the read finished {} ms after the delivery time, so it proves nothing",
            read_at_ms - deliver_at_ms
        );
        check!(
            seen == case.before_delivery,
            "{topic}: before the delivery time"
        );

        let delivered = Visible::of(1, &["due-soon"]);
        let served_at_ms = wait_until_visible(&p.client, &topic, topic_id, &delivered).await;
        check!(
            (served_at_ms >= deliver_at_ms) == case.held_until_delivery,
            "{topic}: first served at {served_at_ms}, delivery time {deliver_at_ms}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn a_parked_long_poll_wakes_when_the_record_comes_due() {
    struct Case {
        mode: Mode,
        /// Whether the poll returned at or after the record's delivery time.
        returns_after_delivery: bool,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            returns_after_delivery: true,
        },
        Case {
            mode: IMMEDIATE,
            returns_after_delivery: false,
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-longpoll-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let deliver_at_ms = produce_due_soon(&p.client, &topic, topic_id).await;

        // The record is already in the log, so nothing appends and no watermark
        // this consumer reads moves while it waits. On a scheduled topic the
        // only thing that can end the wait early is the delivery advance.
        let started = Instant::now();
        let served = fetch_values(&p.client, &topic, topic_id, LONG_POLL_MS).await;
        let elapsed = started.elapsed();
        let returned_at_ms = now_ms();

        check!(
            served == vec!["due-soon".to_owned()],
            "{topic}: the long poll served {served:?}"
        );
        check!(
            elapsed.as_millis() < LONG_POLL_WOKE_MS,
            "{topic}: the long poll took {elapsed:?}, so it expired rather than woke"
        );
        check!(
            (returned_at_ms >= deliver_at_ms) == case.returns_after_delivery,
            "{topic}: returned at {returned_at_ms}, delivery time {deliver_at_ms}"
        );
    }

    p.broker.shutdown().await;
}
