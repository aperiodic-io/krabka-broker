//! The difference between the two validation modes: `id` decides from the
//! Confluent header alone, and `full` decodes the body against the schema the
//! header names.
//!
//! One case covers both, because the claim is a comparison: the same value has
//! to be admitted under `id` and rejected under `full` for either half to mean
//! anything.

use assert2::check;

use crate::harness::{INVALID_RECORD, KNOWN_ID, VALIDATED, create_topic, framed, order_avro_body};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_mode_checks_the_body_and_id_mode_does_not() {
    let (_registry, broker, client, _dir, id_mode) =
        crate::harness::mock_topic_fixture("validated", VALIDATED).await;
    let full_mode = create_topic(
        &broker,
        &client,
        crate::harness::SchemaTopicSetup {
            name: "validated-full",
            configs: &[
                ("schema.validation.value", "true"),
                ("schema.validation.mode", "full"),
            ],
            ..Default::default()
        },
    )
    .await;

    // Framed with a bound id, but the body is not an Avro datum of the schema
    // that id names.
    let garbage = framed(KNOWN_ID, b"\xff\xff\xff\xff\xff\xff");

    let under_id =
        crate::harness::produce_value(&client, "validated", id_mode, Some(garbage.clone())).await;
    let under_full =
        crate::harness::produce_value(&client, "validated-full", full_mode, Some(garbage)).await;

    // `id` mode decides from the header alone, so it admits this.
    check!(under_id.error_code == 0, "{under_id:?}");
    // `full` mode decodes the body, so it does not.
    check!(under_full.error_code == INVALID_RECORD, "{under_full:?}");

    check!(broker.local_log_end_offset("validated", 0) == Some(1));
    check!(broker.local_log_end_offset("validated-full", 0) == Some(0));

    // And a body that IS an instance of the schema passes `full`.
    let good = crate::harness::produce_value(
        &client,
        "validated-full",
        full_mode,
        Some(framed(KNOWN_ID, &order_avro_body())),
    )
    .await;
    check!(good.error_code == 0, "{good:?}");
    check!(broker.local_log_end_offset("validated-full", 0) == Some(1));

    broker.shutdown().await;
}
