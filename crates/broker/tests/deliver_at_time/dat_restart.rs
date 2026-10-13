//! The delivery watermark after a broker restart.
//!
//! Nothing about the schedule is written anywhere but the records themselves,
//! so a reopened log has to derive the same answer from the batch timestamps
//! and the clock. This case is the only one in the suite that outlives a single
//! broker lifetime, which is why it boots on an explicit directory instead of
//! the shared in-process fixture.

use {
    crate::{
        dat_fixtures::{
            ALREADY_DUE_MS, IMMEDIATE, Mode, PENDING_HORIZON_MS, SCHEDULED, Visible, batch_at,
            now_ms,
        },
        dat_wire::{produce, ready_topic, visible, wait_for_delivery_policy},
        support,
    },
    assert2::check,
    krabka_broker::NodeId,
};

#[tokio::test]
async fn the_delivery_watermark_survives_a_broker_restart() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(1, &["due"]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(2, &["due", "pending"]),
        },
    ];
    let topic_of = |mode: Mode| format!("deliver-at-time-restart-{}", mode.value);

    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        for case in &cases {
            let topic = topic_of(case.mode);
            let topic_id = ready_topic(&broker, &client, &topic, case.mode).await;

            let now = now_ms();
            produce(
                &client,
                &topic,
                topic_id,
                batch_at(now - ALREADY_DUE_MS, &["due"]),
            )
            .await;
            produce(
                &client,
                &topic,
                topic_id,
                batch_at(now + PENDING_HORIZON_MS, &["pending"]),
            )
            .await;

            check!(
                visible(&client, &topic, topic_id).await == case.expected,
                "{topic}: before the restart"
            );
        }
        broker.shutdown().await;
    }

    // Nothing about the schedule was written anywhere but the records
    // themselves, so the reopened log has to derive the same answer from the
    // batch timestamps and the clock.
    let (broker, client) = support::start_with_dir(dir.path()).await;
    for case in &cases {
        let topic = topic_of(case.mode);
        broker
            .wait_until_local_partition_leader(&topic, 0, NodeId(1))
            .await;
        wait_for_delivery_policy(&broker, &topic, case.mode).await;
        let topic_id = support::topic_id_for(&client, &topic).await;

        check!(
            visible(&client, &topic, topic_id).await == case.expected,
            "{topic}: after the restart"
        );
    }

    broker.shutdown().await;
}
