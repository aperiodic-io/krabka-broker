//! Kafka's `KafkaRaftClient.handleFetchRequest` on the leader: the request
//! checks, the replica id that moves no replica state, and the long poll of
//! `fetchPurgatory`. Each row compares the whole decoded response.

use std::time::Duration as StdDuration;

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        fetch_request::{self as fetch_req, FetchRequest},
        fetch_response::FetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::time::Instant;

use super::*;
use crate::kraft::{
    controller::test_support::{EngineSetup, build_engine_only, one_offset_batch},
    transport::wire::FETCH_VERSION,
};

const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const INVALID_REQUEST: i16 = 42;
const FENCED_LEADER_EPOCH: i16 = 74;
const UNKNOWN_LEADER_EPOCH: i16 = 75;
const INCONSISTENT_CLUSTER_ID: i16 = 104;

/// Node 1 elected leader of voters 0 and 1, and the epoch it leads.
fn leader_of_0_and_1() -> (Engine, tempfile::TempDir, i32) {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(0), NodeId(1)],
        ..Default::default()
    });
    engine.on_event(Event::ElectionTimeout);
    for epoch in [0, 1] {
        engine.on_event(Event::ReceiveVoteResponse {
            from: NodeId(0),
            epoch,
            vote_granted: true,
        });
    }
    assert2::assert!(engine.core.role().is_leader());
    let epoch = i32::try_from(engine.core.quorum_state().leader_epoch).unwrap();
    (engine, dir, epoch)
}

/// A Fetch from replica 0 of the metadata partition at `fetch_offset`, in
/// `epoch`, that does not wait.
fn fetch(epoch: i32, fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        max_wait_ms: 0,
        replica_state: fetch_req::ReplicaState {
            replica_id: 0,
            ..Default::default()
        },
        topics: vec![fetch_req::FetchTopic {
            topic_id: wire::METADATA_TOPIC_ID,
            partitions: vec![fetch_req::FetchPartition {
                current_leader_epoch: epoch,
                fetch_offset,
                last_fetched_epoch: epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn partition(request: &mut FetchRequest) -> &mut fetch_req::FetchPartition {
    &mut request.topics[0].partitions[0]
}

/// Delivers `request` and returns the answer, or `None` while it waits.
fn deliver(
    engine: &mut Engine,
    request: &FetchRequest,
) -> (Option<FetchResponse>, oneshot::Receiver<bytes::Bytes>) {
    let mut body = bytes::BytesMut::new();
    request.encode(&mut body, FETCH_VERSION).expect("encode");
    let (reply, mut answer) = oneshot::channel();
    engine.on_inbound(Inbound::Fetch {
        req: body.freeze(),
        version: FETCH_VERSION,
        reply,
    });
    let decoded = answer.try_recv().ok().map(|bytes| decode(&bytes));
    (decoded, answer)
}

fn decode(bytes: &[u8]) -> FetchResponse {
    FetchResponse::decode(&mut &bytes[..], FETCH_VERSION).expect("decode")
}

/// The leader's answer with `error_code`, as `buildFetchResponse` shapes it.
fn answer(engine: &Engine, error_code: i16, hwm: i64, records: bytes::Bytes) -> FetchResponse {
    decode(
        &wire::FetchAnswer {
            error_code,
            leader: engine.quorum_leader(),
            diverging: None,
            snapshot_id: None,
            hwm,
            log_start_offset: engine.log.log_start_offset().0,
            records,
        }
        .encode(FETCH_VERSION),
    )
}

fn top_level(error_code: i16) -> FetchResponse {
    FetchResponse {
        error_code,
        ..Default::default()
    }
}

/// The request checks of `handleFetchRequest` and `validateLeaderOnlyRequest`,
/// in Kafka's order.
#[tokio::test]
async fn a_fetch_is_checked_as_handle_fetch_request_checks_it() {
    type Edit = fn(&mut FetchRequest, i32);
    let (mut engine, _dir, epoch) = leader_of_0_and_1();
    let refused = |engine: &Engine, code| answer(engine, code, -1, bytes::Bytes::new());
    let cases: Vec<(&str, Edit, FetchResponse)> = vec![
        (
            "a cluster id of another cluster",
            |request, _| request.cluster_id = Some("AAAAAAAAAAAAAAAAAAAABw".into()),
            top_level(INCONSISTENT_CLUSTER_ID),
        ),
        (
            "another topic id",
            |request, _| request.topics[0].topic_id = WireUuid([7; 16]),
            top_level(INVALID_REQUEST),
        ),
        (
            "partition 1",
            |request, _| partition(request).partition = 1,
            top_level(INVALID_REQUEST),
        ),
        (
            "two partitions",
            |request, _| {
                let copy = request.topics[0].partitions[0].clone();
                request.topics[0].partitions.push(copy);
            },
            top_level(INVALID_REQUEST),
        ),
        (
            "two topics",
            |request, _| {
                let copy = request.topics[0].clone();
                request.topics.push(copy);
            },
            top_level(INVALID_REQUEST),
        ),
        (
            "a negative max wait",
            |request, _| request.max_wait_ms = -1,
            refused(&engine, INVALID_REQUEST),
        ),
        (
            "a negative fetch offset",
            |request, _| partition(request).fetch_offset = -1,
            refused(&engine, INVALID_REQUEST),
        ),
        (
            "a negative last fetched epoch",
            |request, _| partition(request).last_fetched_epoch = -1,
            refused(&engine, INVALID_REQUEST),
        ),
        (
            "a last fetched epoch above the current leader epoch",
            |request, epoch| partition(request).last_fetched_epoch = epoch + 1,
            refused(&engine, INVALID_REQUEST),
        ),
        (
            "an older epoch",
            |request, epoch| {
                partition(request).current_leader_epoch = epoch - 1;
                partition(request).last_fetched_epoch = 0;
            },
            refused(&engine, FENCED_LEADER_EPOCH),
        ),
        (
            "a newer epoch",
            |request, epoch| partition(request).current_leader_epoch = epoch + 1,
            refused(&engine, UNKNOWN_LEADER_EPOCH),
        ),
    ];
    for (name, edit, expected) in cases {
        let mut request = fetch(epoch, 0);
        partition(&mut request).last_fetched_epoch = 0;
        edit(&mut request, epoch);
        let (response, _answer) = deliver(&mut engine, &request);
        check!(response == Some(expected), "{name}");
    }
}

/// `LeaderState.updateReplicaState` ignores a fetch from a negative replica
/// id: it is served, but its offset does not count toward the high
/// watermark, and it is not taken for replica 0.
#[tokio::test]
async fn a_negative_replica_id_moves_no_replica_state() {
    for (name, replica_id, hwm_moves) in [("replica -1", -1, false), ("replica 0", 0, true)] {
        let (mut engine, _dir, epoch) = leader_of_0_and_1();
        let log_end = engine.log.log_end_offset();
        let before = engine.log.hwm();
        let mut request = fetch(epoch, log_end.0);
        request.replica_state.replica_id = replica_id;

        let (response, _answer) = deliver(&mut engine, &request);

        let hwm = if hwm_moves { log_end } else { before };
        check!(engine.log.hwm() == hwm, "{name}");
        check!(
            response == Some(answer(&engine, 0, hwm.0, bytes::Bytes::new())),
            "{name}"
        );
    }
}

/// What happens to a parked fetch before it is answered.
#[derive(Debug, Clone, Copy)]
enum WhileParked {
    /// Nothing: the wait runs out.
    Nothing,
    /// The leader appends a batch past the fetch offset.
    Append,
    /// A higher epoch begins under another leader.
    LoseLeadership,
}

/// Kafka's long poll: a fetch with nothing new waits in `fetchPurgatory`,
/// and is answered when the log grows past its offset, when leadership ends,
/// or when `MaxWaitMs` runs out, with the answer it would have had on
/// arrival. A fetcher whose high watermark is below the leader's is answered
/// at once.
#[tokio::test]
async fn a_fetch_with_nothing_new_waits_until_there_is_something_to_send() {
    for (name, fetcher_hwm_behind, while_parked) in [
        ("the wait runs out", false, WhileParked::Nothing),
        ("the log grows", false, WhileParked::Append),
        ("leadership ends", false, WhileParked::LoseLeadership),
        (
            "the fetcher's high watermark is behind",
            true,
            WhileParked::Nothing,
        ),
    ] {
        let (mut engine, _dir, epoch) = leader_of_0_and_1();
        let log_end = engine.log.log_end_offset();
        // Replica 0 catches up, which commits the log.
        let (caught_up, _answer) = deliver(&mut engine, &fetch(epoch, log_end.0));
        check!(caught_up.is_some(), "{name}");
        check!(engine.log.hwm() == log_end, "{name}");

        let mut request = fetch(epoch, log_end.0);
        request.max_wait_ms = 500;
        partition(&mut request).high_watermark = if fetcher_hwm_behind {
            log_end.0 - 1
        } else {
            log_end.0
        };
        let (immediate, mut parked) = deliver(&mut engine, &request);
        if fetcher_hwm_behind {
            check!(
                immediate == Some(answer(&engine, 0, log_end.0, bytes::Bytes::new())),
                "{name}"
            );
            continue;
        }
        check!(immediate == None, "{name}: the fetch waits");
        // Nothing moved, so the fetch is still waiting just before its
        // deadline.
        engine.complete_parked_fetches(Instant::now());
        check!(parked.try_recv().is_err(), "{name}: still waiting");

        let expected = match while_parked {
            WhileParked::Nothing => {
                engine.complete_parked_fetches(Instant::now() + StdDuration::from_secs(1));
                answer(&engine, 0, log_end.0, bytes::Bytes::new())
            }
            WhileParked::Append => {
                let mut batch = one_offset_batch(log_end.0, epoch, b"new");
                engine.log.append(&mut batch, 0).expect("append");
                engine.complete_parked_fetches(Instant::now());
                answer(&engine, 0, log_end.0, engine.serve_fetch_records(log_end))
            }
            WhileParked::LoseLeadership => {
                engine.on_event(Event::ReceiveBeginQuorumEpoch {
                    leader_id: NodeId(0),
                    leader_epoch: engine.core.quorum_state().leader_epoch + 1,
                });
                engine.complete_parked_fetches(Instant::now());
                answer(&engine, NOT_LEADER_OR_FOLLOWER, -1, bytes::Bytes::new())
            }
        };
        let body = parked.try_recv().expect("the parked fetch is answered");
        check!(decode(&body) == expected, "{name}");
    }
}
