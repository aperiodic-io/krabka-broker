//! The gate itself: a batch at the cap appends, a batch one byte over it is
//! refused with `MESSAGE_TOO_LARGE` (10) and appends nothing.

use assert2::check;

use crate::{
    support,
    wire::{
        COMPRESSION_TYPE, KAFKA_DEFAULT, MAX_MESSAGE_BYTES, SEGMENT_BYTES, accepted, create_topic,
        gzip_batch, produce_batch, produce_batch_of_wire_len, record_list_too_large, too_large,
        wire_len,
    },
};

/// A per-topic cap of 2048 bytes, the value the boundary was settled at
/// against `apache/kafka:4.1.0`.
const CAP: usize = 2048;

async fn check_exact_limit(
    process: &support::InProcess,
    topic: krabka_protocol::primitives::uuid::Uuid,
    limit: usize,
    refusal: krabka_protocol::owned::produce_response::PartitionProduceResponse,
) {
    check!(
        produce_batch_of_wire_len(&process.client, "orders", topic, limit).await == accepted(0, 0)
    );
    check!(produce_batch_of_wire_len(&process.client, "orders", topic, limit + 1).await == refusal);
    check!(process.broker.local_log_end_offset("orders", 0) == Some(1));
    check!(
        produce_batch_of_wire_len(&process.client, "orders", topic, limit).await == accepted(1, 0)
    );
    check!(process.broker.local_log_end_offset("orders", 0) == Some(2));
}

/// The cap is exact, and a refusal costs the log nothing.
///
/// This is the feature in one case. The batch at the cap goes first, so the
/// refusal that follows is the size and not a produce path that never worked;
/// and the batch at the cap after it lands at offset 1, which is the assertion
/// that matters most. A broker that answered `MESSAGE_TOO_LARGE` *and*
/// appended would pass on the error code alone and have written the very
/// 100-MiB batch the cap exists to keep out of the partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_at_the_cap_appends_and_one_byte_over_it_is_refused() {
    let (p, topic) = crate::wire::orders_fixture(&[(MAX_MESSAGE_BYTES, &CAP.to_string())]).await;

    check_exact_limit(&p, topic, CAP, too_large()).await;

    p.broker.shutdown().await;
}

/// A record set larger than `segment.bytes` is refused with
/// `RECORD_LIST_TOO_LARGE` (18) even when `max.message.bytes` would take it.
///
/// Kafka's floor for `segment.bytes` is 1 MiB and the default
/// `max.message.bytes` is a little over it, so the two meet in the default
/// configuration: `UnifiedLog.append` throws `RecordBatchTooLargeException`
/// for a set larger than the segment (`apache/kafka:4.3.1`, and trunk keeps it
/// for client appends). A batch is never split across segments, so without the
/// refusal it would land whole in a segment of its own, bigger than the size
/// the operator configured.
///
/// The batch of exactly `segment.bytes` goes first and goes again after the
/// refusal: the limit is strict, and the refusal costs the log nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_larger_than_segment_bytes_is_refused_even_when_the_message_cap_allows_it() {
    /// Kafka's floor for `segment.bytes`.
    const SEGMENT: usize = 1_048_576;

    let p = support::start().await;
    let topic = create_topic(
        &p.broker,
        &p.client,
        "orders",
        &[
            (MAX_MESSAGE_BYTES, &(SEGMENT * 2).to_string()),
            (SEGMENT_BYTES, &SEGMENT.to_string()),
        ],
    )
    .await;

    check_exact_limit(&p, topic, SEGMENT, record_list_too_large()).await;

    p.broker.shutdown().await;
}

/// A topic that sets no `max.message.bytes` inherits the broker's
/// `message.max.bytes`.
///
/// Kafka reports exactly this as the `DEFAULT_CONFIG` synonym of an unset
/// `max.message.bytes`, and the number is the same 1048588 on both keys. The
/// case exists because the per-topic override is the easy half: a broker that
/// enforced the cap only where an operator had spelled one out would leave
/// every default topic accepting the 100-MiB batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_with_no_override_inherits_the_brokers_message_max_bytes() {
    let p = support::start().await;
    let topic = create_topic(&p.broker, &p.client, "orders", &[]).await;

    check!(
        produce_batch_of_wire_len(&p.client, "orders", topic, KAFKA_DEFAULT).await
            == accepted(0, 0)
    );
    check!(
        produce_batch_of_wire_len(&p.client, "orders", topic, KAFKA_DEFAULT + 1).await
            == too_large()
    );
    check!(p.broker.local_log_end_offset("orders", 0) == Some(1));

    p.broker.shutdown().await;
}

/// A per-topic override overrides the broker default in both directions.
///
/// The tightened topic refuses a batch the broker default would have taken,
/// and the loosened topic takes one the broker default would have refused.
/// Together they say the produce path reads the topic's value rather than a
/// constant that happens to agree with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_override_binds_below_and_above_the_broker_default() {
    let p = support::start().await;
    let tight = create_topic(
        &p.broker,
        &p.client,
        "tight",
        &[(MAX_MESSAGE_BYTES, &CAP.to_string())],
    )
    .await;
    let loose = create_topic(
        &p.broker,
        &p.client,
        "loose",
        &[(MAX_MESSAGE_BYTES, &(KAFKA_DEFAULT * 2).to_string())],
    )
    .await;

    check!(produce_batch_of_wire_len(&p.client, "tight", tight, CAP + 1).await == too_large());
    check!(p.broker.local_log_end_offset("tight", 0) == Some(0));
    check!(
        produce_batch_of_wire_len(&p.client, "loose", loose, KAFKA_DEFAULT + 1).await
            == accepted(0, 0)
    );

    p.broker.shutdown().await;
}

/// A topic that forces `compression.type=uncompressed` measures the batch it
/// stores, not the batch that arrived.
///
/// This is the hole a wire-length-only gate leaves open. The producer sends a
/// gzip batch of a few hundred bytes, comfortably under a 2048-byte cap, and
/// the broker has to expand it before it can store it because the topic
/// forbids the producer's codec. What lands in the log is a six-figure batch,
/// which is precisely the batch the cap exists to keep out: every consumer of
/// that partition has to fetch it whole.
///
/// Kafka refuses it. `message.max.bytes` is documented as "the largest record
/// batch size allowed by Kafka (after compression if compression is enabled)",
/// and `UnifiedLog.append` in `apache/kafka:4.3.1` re-runs its per-batch size
/// check over the re-encoded batches whenever `LogValidator` reports
/// `messageSizeMaybeChanged`, throwing the same `RecordTooLargeException` its
/// pre-append check throws.
///
/// The compressed wire length is asserted to sit under the cap, so the case
/// cannot pass because the pre-append gate caught the batch on its way in. The
/// small gzip batch afterwards is what separates "the broker measures the
/// stored form" from "the broker refuses every recompressed batch".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_the_topic_expands_is_measured_after_the_broker_re_encodes_it() {
    /// Repeated bytes, so gzip returns a batch hundreds of times smaller than
    /// the one the broker will store.
    const EXPANDS_PAST_THE_CAP: usize = 100_000;
    /// Small enough to sit under the cap in both forms.
    const FITS_EITHER_WAY: usize = 100;

    let p = support::start().await;
    let topic = create_topic(
        &p.broker,
        &p.client,
        "orders",
        &[
            (MAX_MESSAGE_BYTES, &CAP.to_string()),
            (COMPRESSION_TYPE, "uncompressed"),
        ],
    )
    .await;

    let oversized = gzip_batch(EXPANDS_PAST_THE_CAP);
    check!(wire_len(&oversized) < CAP);
    check!(oversized.encoded_len() > CAP);

    check!(produce_batch(&p.client, "orders", topic, oversized).await == too_large());
    check!(p.broker.local_log_end_offset("orders", 0) == Some(0));
    check!(
        produce_batch(&p.client, "orders", topic, gzip_batch(FITS_EITHER_WAY)).await
            == accepted(0, 0)
    );
    check!(p.broker.local_log_end_offset("orders", 0) == Some(1));

    p.broker.shutdown().await;
}
