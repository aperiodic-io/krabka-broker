//! What a validated topic admits: a record framed with a schema id bound to
//! the topic's subject, and a tombstone.
//!
//! The cache-counter case sits here too, because the counters only move on a
//! produce that the validator accepted: the first one pays a registry round
//! trip and the second is served from the cache.

use assert2::check;

use crate::harness::{KNOWN_ID, VALIDATED, framed};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_framed_with_a_bound_schema_id_is_accepted() {
    let (_registry, broker, client, _dir, id) =
        crate::harness::mock_topic_fixture("validated", VALIDATED).await;

    crate::harness::check_value_append(
        &broker,
        &client,
        "validated",
        id,
        Some(framed(KNOWN_ID, b"anything")),
        (0, Some(1)),
    )
    .await;

    broker.shutdown().await;
}

/// The cache counters must move on a real produce.
///
/// Both were declared, registered and documented, and nothing incremented
/// them: a live broker scraped zero for the life of the process. The unit test
/// that called `record_schema_cache_hit` directly proved the counter counts,
/// not that anything counts with it, so the assertion belongs here — behind an
/// actual produce through the validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cache_counters_move_on_a_validated_produce() {
    let (_registry, broker, client, _dir, id) =
        crate::harness::mock_topic_fixture("validated", VALIDATED).await;

    // The first produce loads the schema; the next is served within its TTL.
    for (misses, hits) in [(0, 0), (1, 0), (1, 1)] {
        check!(broker.metrics().schema_validation_cache_misses.get() == misses);
        check!(broker.metrics().schema_validation_cache_hits.get() == hits);
        if hits == 0 {
            crate::harness::check_value_append(
                &broker,
                &client,
                "validated",
                id,
                Some(framed(KNOWN_ID, b"anything")),
                (0, None),
            )
            .await;
        }
    }

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tombstone_is_accepted_on_a_validated_topic() {
    let (_registry, broker, client, _dir, id) =
        crate::harness::mock_topic_fixture("validated", VALIDATED).await;

    // A null value is a tombstone. Rejecting it would make schema validation
    // and compaction mutually exclusive.
    crate::harness::check_value_append(&broker, &client, "validated", id, None, (0, Some(1))).await;

    broker.shutdown().await;
}
