//! The record bytes a `ShareFetch` returns for the offsets it acquired. An
//! acquired offset that begins a later on-disk batch must still carry its
//! payload, and a fetch whose acquired window is fragmented must carry every
//! acquired offset and nothing from the gap between them.

use std::time::Duration;

use assert2::assert;
use krabka_client_core::Client;

use crate::{
    harness::{bootstrap_share_state, produce_n},
    share_rpc::{acquired_count, share_fetch},
};

/// Regression: a `ShareFetch` whose acquired offset begins a *later* record
/// batch must still return that offset's record bytes, not an empty payload.
/// The broker already consumed or archived a leading multi-record batch.
///
/// `ShareFetch.partition_max_bytes` is a v0-only field. At the supported
/// versions (v1+) it is absent and decodes to 0. The read path must not use
/// that 0 as the log-read byte budget. A 0 budget reads only one batch header,
/// which cannot skip the leading batch to reach the acquired offset. The broker
/// then returns the acquired record with no bytes, and the record stays locked.
/// The read must fall back to the request-level `max_bytes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_past_leading_batch_returns_bytes() {
    let crate::harness::ConsumptionFixture {
        _permit,
        broker: _broker,
        client,
        _dir,
        tid,
        member,
    } = crate::harness::consumption_fixture(crate::support::share::ShareRecordSeedSetup::default())
        .await;
    let session = crate::share_rpc::acquire_three_and_acknowledge(
        &client,
        crate::support::share::ShareAck::prefix_for(&member, tid, krabka_ids::Offset(2)).reject(),
    )
    .await;

    // A separate single-record batch at offset 3 (this starts a new batch; the
    // acquired range 3..3 begins past the leading 0..2 batch).
    produce_n(
        &client,
        crate::support::share::ShareRecordsSetup {
            topic_id: tid,
            ..Default::default()
        },
    )
    .await;

    // Acquire offset 3 — the payload must carry the record bytes.
    let row3 = share_fetch(
        &client,
        session.fetch_at(crate::support::share::ShareSessionEpoch(2)),
    )
    .await;
    let row3 = crate::support::share::refetch_while_empty(
        &client,
        row3,
        crate::support::share::RefetchSetup {
            session: crate::support::share::ShareSessionSetup {
                member: &member,
                topic_id: tid,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    assert!(
        acquired_count(&row3) == 1,
        "offset 3 must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 3,
        "acquired offset must be 3, got {:?}",
        row3.acquired_records
    );
    let batches = row3
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("acquired offset 3 must carry decodable v2 record bytes");
    let values: Vec<String> = crate::support::share::record_values(batches);
    assert!(
        values == vec!["v0"],
        "offset 3's record bytes must be returned, got {values:?}"
    );
}

/// Produce a single record carrying `value` into `(topic, partition)` as its OWN
/// batch, so each offset is a distinct on-disk batch. This matters for the
/// fragmented-window read test: the share-fetch read path reads verbatim bytes
/// at *batch* granularity, so each offset must be its own batch to surface
/// byte-exact disjoint offsets. This helper retries while the partition is still
/// materializing.
async fn produce_one(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, value: &str) {
    crate::support::share::produce_values(
        client,
        vec![bytes::Bytes::from(value.to_string())],
        crate::support::share::ShareProduceSetup {
            topic,
            topic_id: tid,
            partition: krabka_ids::PartitionIndex(partition),
        },
    )
    .await;
}

/// F5 (fragmented window): a single share fetch that returns DISJOINT acquired
/// ranges must carry record bytes for exactly the acquired offsets. The gap
/// offset's value must not appear.
///
/// Scenario: produce 3 records as THREE separate single-record batches, so
/// offsets 0, 1, 2 are each their own on-disk batch. The share-fetch read is
/// batch-granular, so byte-exact disjoint reads need separate batches. Acquire
/// 0..2, then Accept the MIDDLE offset (1) only and Release the outer offsets 0
/// and 2. The SPSO stays at 0 because offset 0 is not accepted. The broker
/// acknowledges offset 1, and offsets 0, 2 return to Available, and that leaves
/// a gap at offset 1.
///
/// The re-fetch acquires the DISJOINT set {0, 2}. The read concatenates the
/// per-range bytes, so the payload decodes to exactly offsets {0, 2} (values
/// v0, v2), never the gap offset 1's value v1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_window_records_match_acquired_offsets() {
    let (_permit, broker, client, _dir, tid) = crate::support::share::permitted_topic_fixture(
        crate::support::share::ShareTopicSetup::default(),
        |_| {},
    )
    .await;
    bootstrap_share_state(&broker, &client, "g1").await;
    // Three separate single-record batches: offset 0=v0, 1=v1, 2=v2.
    produce_one(&client, "t", tid, 0, "v0").await;
    produce_one(&client, "t", tid, 0, "v1").await;
    produce_one(&client, "t", tid, 0, "v2").await;
    let (member, _member_epoch) =
        crate::support::share::join_consume_member(&broker, &client, tid).await;
    let session = crate::share_rpc::acquire_three_and_acknowledge(
        &client,
        crate::support::share::ShareAck::single_for(&member, tid, krabka_ids::Offset(1)),
    )
    .await;
    crate::support::share::acknowledge_success(
        &client,
        crate::support::share::ShareAck::prefix(
            session.with_epoch(crate::support::share::ShareSessionEpoch(2)),
            krabka_ids::Offset(0),
        )
        .release(),
    )
    .await;
    crate::support::share::acknowledge_success(
        &client,
        crate::support::share::ShareAck::prefix(
            session.with_epoch(crate::support::share::ShareSessionEpoch(3)),
            krabka_ids::Offset(2),
        )
        .starting_at(krabka_ids::Offset(2))
        .release(),
    )
    .await;

    // Re-fetch: the acquired set is the DISJOINT {0, 2} (offset 1 is gone). The
    // returned records payload must decode to exactly offsets {0, 2}.
    let mut row2 = share_fetch(
        &client,
        session.fetch_at(crate::support::share::ShareSessionEpoch(4)),
    )
    .await;
    for epoch in 5..20 {
        if acquired_count(&row2) >= 2 {
            break;
        }
        // intentional: bounded RPC poll — re-acquiring the released disjoint set
        // {0, 2} happens only via this ShareFetch; no image/metric reflects it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row2 = share_fetch(
            &client,
            session.fetch_at(crate::support::share::ShareSessionEpoch(epoch)),
        )
        .await;
    }
    // The authoritative acquired offset set.
    let acquired_offsets: std::collections::BTreeSet<i64> = row2
        .acquired_records
        .iter()
        .flat_map(|r| r.first_offset..=r.last_offset)
        .collect();
    assert!(
        acquired_offsets == maplit::btreeset! {0, 2},
        "must re-acquire the disjoint set {{0, 2}}, got {acquired_offsets:?}"
    );

    // Decode the records payload and collect the absolute offsets it carries.
    let batches = row2
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("disjoint acquired ranges must carry decodable v2 record bytes");
    let record_offsets: std::collections::BTreeSet<i64> = batches
        .iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records
                .iter()
                .map(move |r| base + i64::from(r.offset_delta))
        })
        .collect();
    assert!(
        record_offsets == acquired_offsets,
        "records payload offsets {record_offsets:?} must equal acquired offsets \
         {acquired_offsets:?} (gap offset 1 must be excluded)"
    );
    // Belt-and-suspenders: the gap offset's value (v1) must never appear.
    let values: Vec<String> = crate::support::share::record_values(batches);
    assert!(
        !values.contains(&"v1".to_string()),
        "the gap offset's value v1 must be excluded, got {values:?}"
    );
}
