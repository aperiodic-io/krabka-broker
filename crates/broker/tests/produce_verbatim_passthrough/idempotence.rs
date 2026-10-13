//! Idempotent-producer dedup over the verbatim path, including the wrap of the
//! producer sequence at `i32::MAX`.
//!
//! Both tests matter to the passthrough because dedup reads only the batch
//! header fields the verbatim append leaves untouched: the producer id, the
//! epoch, `base_sequence`, and `last_offset_delta`.

use {
    crate::{
        harness::{boot, create_topic, idempotent_lz4_batch, produce_one, topic_id_for},
        support::client::connect_client,
    },
    assert2::{assert, check},
};

/// Idempotent-producer dedup runs on the HEADER fields that the verbatim path
/// exposes: pid, epoch, `base_sequence`, and `last_offset_delta`. Two appends
/// with increasing sequences both succeed. A retry of the latest sequence is a
/// duplicate and returns the SAME base offset. An out-of-order sequence is
/// rejected. Structural validation may transiently decompress the lz4 body,
/// but the append retains the producer's original compressed bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_dedup_over_verbatim_path() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "idem").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "idem").await;

    // seq 0..=2 (3 records) → base offset 0.
    let base0 = produce_one(&client, "idem", topic_id, idempotent_lz4_batch(9_001, 0, 3))
        .await
        .expect("first append ok");
    assert!(base0 == 0);

    // seq 3..=4 (2 records) → base offset 3.
    let base1 = produce_one(&client, "idem", topic_id, idempotent_lz4_batch(9_001, 3, 2))
        .await
        .expect("second append ok");
    assert!(base1 == 3);
    broker.wait_until_local_log_end_offset("idem", 0, 5).await;

    // Retry the MOST RECENT batch (seq 3..=4) → DUPLICATE: the dedup tracker
    // tracks the last committed batch and echoes its base offset (3), no error.
    // This is driven purely by the header pid/epoch/base_sequence — the lz4
    // body is never re-encoded.
    let base_dup = produce_one(&client, "idem", topic_id, idempotent_lz4_batch(9_001, 3, 2))
        .await
        .expect("duplicate must be NONE");
    assert!(base_dup == 3, "duplicate returns the committed base offset");

    // An out-of-order sequence (skip ahead past last+1) →
    // OUT_OF_ORDER_SEQUENCE_NUMBER (45). The last committed sequence is 4, so
    // base_sequence 99 leaves a gap.
    let err = produce_one(
        &client,
        "idem",
        topic_id,
        idempotent_lz4_batch(9_001, 99, 1),
    )
    .await
    .expect_err("out-of-order must error");
    assert!(err == 45, "out-of-order sequence must be 45; got {err}");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_sequence_rollover_over_verbatim_path() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "idem-rollover").await;

    let client = connect_client(bootstrap, None).await;
    let topic_id = topic_id_for(&client, "idem-rollover").await;

    // This batch spans MAX-1, MAX, 0. Kafka sequences wrap modulo 2^31.
    let first = produce_one(
        &client,
        "idem-rollover",
        topic_id,
        idempotent_lz4_batch(9_002, i32::MAX - 1, 3),
    )
    .await
    .expect("batch crossing producer-sequence rollover");
    check!(first == 0);

    let next = produce_one(
        &client,
        "idem-rollover",
        topic_id,
        idempotent_lz4_batch(9_002, 1, 1),
    )
    .await
    .expect("wrapped successor");
    check!(next == 3);

    let duplicate = produce_one(
        &client,
        "idem-rollover",
        topic_id,
        idempotent_lz4_batch(9_002, 1, 1),
    )
    .await
    .expect("exact retry");
    check!(duplicate == 3);

    let wrong_span = produce_one(
        &client,
        "idem-rollover",
        topic_id,
        idempotent_lz4_batch(9_002, 1, 2),
    )
    .await
    .expect_err("same base sequence with a different span is not a duplicate");
    check!(wrong_span == 45);

    let gap = produce_one(
        &client,
        "idem-rollover",
        topic_id,
        idempotent_lz4_batch(9_002, 3, 1),
    )
    .await
    .expect_err("sequence gap after rollover");
    check!(gap == 45);

    broker.shutdown().await;
}
