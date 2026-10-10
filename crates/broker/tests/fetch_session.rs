//! KIP-227 incremental fetch session round-trip tests against an in-process
//! broker.
//!
//! These tests drive the wire protocol directly through the shared `Client`,
//! so they exercise the exact `session_id` / `session_epoch` paths
//! end-to-end.

use assert2::{assert, check};

use crate::support::fetch::{fetch_request_for, fetch_topic_row};
mod support;

use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};
use support::topic_id_for;

const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
const INVALID_FETCH_SESSION_EPOCH: i16 = 71;

use crate::support::records::empty_record_batch as one_record_batch;

async fn incremental_after_produce(
    process: &support::InProcess,
    session: i32,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    process
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::FetchLimits::wait_for_data(
                crate::support::fetch::RequestWaitMillis(200),
            ),
            crate::support::fetch::FetchSessionSetup::at_epoch(
                crate::support::fetch::FetchSessionId(session),
                crate::support::fetch::FetchSessionEpoch(2),
            ),
        ))
        .await
        .expect("Fetch incremental after produce")
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    crate::support::client::create_topic(&p.client, name, num_partitions).await;
}

async fn produce(p: &support::InProcess, topic: &str, partition: i32, records: i32) {
    let topic_id = topic_id_for(&p.client, topic).await;
    let req = crate::support::produce::batch_request(
        one_record_batch(records),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            partition: krabka_ids::PartitionIndex(partition),
            ..Default::default()
        },
    );
    let resp = p.client.send(req).await.expect("Produce");
    assert!(
        resp.responses[0].partition_responses[0].error_code == 0,
        "Produce error"
    );
}

fn fetch_partition(partition: i32, offset: i64) -> FetchPartition {
    FetchPartition {
        partition,
        fetch_offset: offset,
        partition_max_bytes: 1_048_576,
        ..Default::default()
    }
}

fn fetch_topic(name: &str, topic_id: WireUuid, partitions: Vec<FetchPartition>) -> FetchTopic {
    fetch_topic_row(name, topic_id, partitions)
}

/// A new session opens, the immediate incremental is empty, and one produced
/// batch appears on the next incremental as the only partition.
#[tokio::test]
async fn new_session_then_incremental_filters_unchanged_partitions() {
    let (p, _tid, r1) = Box::pin(topic_session(3, 100)).await;
    check!(r1.error_code == 0, "no top-level error");
    check!(r1.session_id > 0, "broker allocated a session id");
    assert!(r1.responses.len() == 1, "new session emits full response");
    check!(r1.responses[0].partitions.len() == 3, "all 3 partitions");
    let sid = r1.session_id;

    // (2) Immediate incremental: nothing changed → empty response.
    let r2 = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::FetchLimits::immediate(),
            crate::support::fetch::FetchSessionSetup::incremental(
                crate::support::fetch::FetchSessionId(sid),
            ),
        ))
        .await
        .expect("Fetch incremental empty");
    check!(r2.error_code == 0);
    check!(r2.session_id == sid, "session id echoed");
    check!(
        r2.responses.is_empty(),
        "no partition changed → no topics in response, got {:?}",
        r2.responses
    );

    // (3) Produce one batch to t-0 → next incremental returns only t-0.
    produce(&p, "t", 0, 5).await;
    let r3 = incremental_after_produce(&p, sid).await;
    check!(r3.error_code == 0);
    check!(r3.session_id == sid);
    assert!(r3.responses.len() == 1);
    assert!(r3.responses[0].partitions.len() == 1);
    check!(r3.responses[0].partitions[0].partition_index == 0);
    let batches = r3.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records present");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 5);

    p.broker.shutdown().await;
}

/// The broker drops forgotten partitions from the cached subscription, and
/// they never reappear on later fetches, even after a produce.
#[tokio::test]
async fn forgotten_topics_drop_partitions_from_subscription() {
    let (p, tid, r1) = Box::pin(topic_session(3, 100)).await;
    let sid = r1.session_id;
    assert!(sid > 0);

    // Forget t-1.
    let r2 = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::FetchLimits::immediate(),
            crate::support::fetch::FetchSessionSetup {
                id: crate::support::fetch::FetchSessionId(sid),
                epoch: crate::support::fetch::FetchSessionEpoch(1),
                forgotten: vec![ForgottenTopic {
                    topic: "t".into(),
                    topic_id: tid,
                    partitions: vec![1],
                    ..Default::default()
                }],
            },
        ))
        .await
        .expect("forget t-1");
    assert!(r2.error_code == 0);
    assert!(r2.session_id == sid);

    // Produce to t-1 — should NOT reappear in the next incremental.
    produce(&p, "t", 1, 4).await;
    // Also produce to t-2 — that one SHOULD appear.
    produce(&p, "t", 2, 2).await;

    let r3 = incremental_after_produce(&p, sid).await;
    assert!(r3.error_code == 0);
    let mut seen_partitions: Vec<i32> = r3
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
        .collect();
    seen_partitions.sort_unstable();
    assert!(
        seen_partitions == vec![2],
        "t-1 forgotten, only t-2 should appear (t-0 had no new data)"
    );

    p.broker.shutdown().await;
}

/// A wrong `session_id` gives `FETCH_SESSION_ID_NOT_FOUND` at the top level
/// with no per-partition rows.
#[tokio::test]
async fn unknown_session_id_returns_not_found() {
    let p = support::start().await;
    let r = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::FetchLimits::immediate(),
            crate::support::fetch::FetchSessionSetup::incremental(
                crate::support::fetch::FetchSessionId(999_999),
            ),
        ))
        .await
        .expect("Fetch unknown sid");
    check!(r.error_code == FETCH_SESSION_ID_NOT_FOUND);
    check!(r.session_id == 0);
    check!(r.responses.is_empty());
    p.broker.shutdown().await;
}

/// A stale epoch on a valid session gives `INVALID_FETCH_SESSION_EPOCH`.
#[tokio::test]
async fn stale_session_epoch_returns_invalid_epoch() {
    let (p, _tid, r1) = Box::pin(topic_session(1, 0)).await;
    let sid = r1.session_id;
    assert!(sid > 0);

    // Broker expects epoch=1; send 99.
    let r2 = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::default_fetch_limits(),
            crate::support::fetch::FetchSessionSetup::at_epoch(
                crate::support::fetch::FetchSessionId(sid),
                crate::support::fetch::FetchSessionEpoch(99),
            ),
        ))
        .await
        .expect("stale epoch");
    check!(r2.error_code == INVALID_FETCH_SESSION_EPOCH);
    check!(r2.session_id == 0);
    check!(r2.responses.is_empty());
    p.broker.shutdown().await;
}

/// A close request with epoch=-1 returns a full response and drops the cache
/// entry. A later request with the same id is `NOT_FOUND`.
#[tokio::test]
async fn close_session_drops_cache_entry() {
    let (p, tid, r1) = Box::pin(topic_session(1, 0)).await;
    let sid = r1.session_id;
    assert!(sid > 0);

    // Close: session_id=sid, session_epoch=-1. Broker serves the request
    // sessionless-style (session_id=0 in response) and removes the entry.
    let r2 = p
        .client
        .send(crate::support::fetch::session_fetch_request(
            fetch_request_for(
                vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
                crate::support::fetch::default_fetch_limits(),
            ),
            crate::support::fetch::FetchSessionSetup::at_epoch(
                crate::support::fetch::FetchSessionId(sid),
                crate::support::fetch::FetchSessionEpoch(-1),
            ),
        ))
        .await
        .expect("close");
    assert!(r2.error_code == 0);
    assert!(r2.session_id == 0, "close → response session_id=0");

    // Re-using sid afterwards is NOT_FOUND.
    let r3 = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::default_fetch_limits(),
            crate::support::fetch::FetchSessionSetup::incremental(
                crate::support::fetch::FetchSessionId(sid),
            ),
        ))
        .await
        .expect("after close");
    assert!(r3.error_code == FETCH_SESSION_ID_NOT_FOUND);
    p.broker.shutdown().await;
}

/// `session_id=0` with a stray epoch, that is, not 0 and not -1, is an
/// incremental fetch. Kafka's `FetchManager.newContext` looks the id up, and
/// id 0 is never allocated, so it answers `FETCH_SESSION_ID_NOT_FOUND`.
#[tokio::test]
async fn sessionless_zero_id_with_stray_epoch_is_session_id_not_found() {
    let p = support::start().await;
    let r = p
        .client
        .send(crate::support::fetch::empty_session_fetch(
            crate::support::fetch::default_fetch_limits(),
            crate::support::fetch::FetchSessionSetup {
                epoch: crate::support::fetch::FetchSessionEpoch(7),
                ..Default::default()
            },
        ))
        .await
        .expect("stray");
    assert!(r.error_code == FETCH_SESSION_ID_NOT_FOUND);
    assert!(r.session_id == 0);
    p.broker.shutdown().await;
}

/// A sessionless request, `session_id=0` with session_epoch=-1, returns a
/// full response with `session_id=0`. This is the legacy path.
#[tokio::test]
async fn sessionless_full_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p.client, "t").await;
    produce(&p, "t", 0, 2).await;

    let r = p
        .client
        .send(crate::support::fetch::session_fetch_request(
            fetch_request_for(
                vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
                crate::support::fetch::FetchLimits::wait_for_data(
                    crate::support::fetch::RequestWaitMillis(100),
                ),
            ),
            crate::support::fetch::FetchSessionSetup {
                epoch: crate::support::fetch::FetchSessionEpoch(-1),
                ..Default::default()
            },
        ))
        .await
        .expect("sessionless");
    check!(r.error_code == 0);
    check!(r.session_id == 0, "sessionless → no allocation");
    assert!(r.responses.len() == 1);
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert!(total == 2);
    p.broker.shutdown().await;
}

async fn open_session(
    client: &krabka_client_core::Client,
    tid: WireUuid,
    partitions: i32,
    max_wait_ms: i32,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    client
        .send(crate::support::fetch::session_fetch_request(
            fetch_request_for(
                vec![fetch_topic(
                    "t",
                    tid,
                    (0..partitions).map(|p| fetch_partition(p, 0)).collect(),
                )],
                crate::support::fetch::FetchLimits {
                    wait: crate::support::fetch::RequestWaitMillis(max_wait_ms),
                    minimum: crate::support::fetch::FetchByteLimit(0),
                    ..Default::default()
                },
            ),
            crate::support::fetch::FetchSessionSetup {
                epoch: crate::support::fetch::FetchSessionEpoch(0),
                ..Default::default()
            },
        ))
        .await
        .expect("new session")
}

/// Creates t and opens a full fetch session with the caller's wait and partition count.
async fn topic_session(
    partitions: i32,
    max_wait_ms: i32,
) -> (
    support::InProcess,
    WireUuid,
    krabka_protocol::owned::fetch_response::FetchResponse,
) {
    let p = support::start().await;
    create_topic(&p, "t", partitions).await;
    let tid = topic_id_for(&p.client, "t").await;
    let response = open_session(&p.client, tid, partitions, max_wait_ms).await;
    (p, tid, response)
}
