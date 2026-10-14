//! Kafka's `KafkaRaftClient.handleFetchRequest` on the leader: the request
//! checks, the replica id that moves no replica state, and the long poll of
//! `fetchPurgatory`. Each row compares the whole decoded response.

use {
    super::*,
    crate::kraft::{
        controller::test_support::{build_engine_only, one_offset_batch},
        transport::wire::FETCH_VERSION,
    },
    assert2::check,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            fetch_request::{self as fetch_req, FetchRequest},
            fetch_response::FetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::time::Duration as StdDuration,
    tokio::time::Instant,
};

const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const INVALID_REQUEST: i16 = 42;
const FENCED_LEADER_EPOCH: i16 = 74;
const UNKNOWN_LEADER_EPOCH: i16 = 75;
const INCONSISTENT_CLUSTER_ID: i16 = 104;

/// Node 1 elected leader of voters 0 and 1, with its data directory and the
/// epoch it leads.
struct LeaderOf0And1 {
    engine: Engine,
    dir: tempfile::TempDir,
    epoch: i32,
}

fn leader_of_0_and_1() -> LeaderOf0And1 {
    let (mut engine, dir) = build_engine_only().ids(&[NodeId(0), NodeId(1)]).call();
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
    LeaderOf0And1 { engine, dir, epoch }
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

/// What delivering a request left: the answer if it came at once, and the
/// channel the answer arrives on otherwise.
struct Delivery {
    answer: Option<FetchResponse>,
    pending: oneshot::Receiver<bytes::Bytes>,
}

/// Delivers `request` and returns the answer, or `None` while it waits.
fn deliver(engine: &mut Engine, request: &FetchRequest) -> Delivery {
    let mut body = bytes::BytesMut::new();
    request.encode(&mut body, FETCH_VERSION).expect("encode");
    let (reply, mut answer) = oneshot::channel();
    engine.on_inbound(Inbound::Fetch {
        req: body.freeze(),
        version: FETCH_VERSION,
        reply,
    });
    let decoded = answer.try_recv().ok().map(|bytes| decode(&bytes));
    Delivery {
        answer: decoded,
        pending: answer,
    }
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
    struct Case {
        name: &'static str,
        edit: Edit,
        expected: FetchResponse,
    }

    type Edit = fn(&mut FetchRequest, i32);
    let LeaderOf0And1 {
        mut engine,
        dir: _dir,
        epoch,
    } = leader_of_0_and_1();
    let refused = |engine: &Engine, code| answer(engine, code, -1, bytes::Bytes::new());
    let cases = vec![
        Case {
            name: "a cluster id of another cluster",
            edit: |request, _| request.cluster_id = Some("AAAAAAAAAAAAAAAAAAAABw".into()),
            expected: top_level(INCONSISTENT_CLUSTER_ID),
        },
        Case {
            name: "another topic id",
            edit: |request, _| request.topics[0].topic_id = WireUuid([7; 16]),
            expected: top_level(INVALID_REQUEST),
        },
        Case {
            name: "partition 1",
            edit: |request, _| partition(request).partition = 1,
            expected: top_level(INVALID_REQUEST),
        },
        Case {
            name: "two partitions",
            edit: |request, _| {
                let copy = request.topics[0].partitions[0].clone();
                request.topics[0].partitions.push(copy);
            },
            expected: top_level(INVALID_REQUEST),
        },
        Case {
            name: "two topics",
            edit: |request, _| {
                let copy = request.topics[0].clone();
                request.topics.push(copy);
            },
            expected: top_level(INVALID_REQUEST),
        },
        Case {
            name: "a negative max wait",
            edit: |request, _| request.max_wait_ms = -1,
            expected: refused(&engine, INVALID_REQUEST),
        },
        Case {
            name: "a negative fetch offset",
            edit: |request, _| partition(request).fetch_offset = -1,
            expected: refused(&engine, INVALID_REQUEST),
        },
        Case {
            name: "a negative last fetched epoch",
            edit: |request, _| partition(request).last_fetched_epoch = -1,
            expected: refused(&engine, INVALID_REQUEST),
        },
        Case {
            name: "a last fetched epoch above the current leader epoch",
            edit: |request, epoch| partition(request).last_fetched_epoch = epoch + 1,
            expected: refused(&engine, INVALID_REQUEST),
        },
        Case {
            name: "an older epoch",
            edit: |request, epoch| {
                partition(request).current_leader_epoch = epoch - 1;
                partition(request).last_fetched_epoch = 0;
            },
            expected: refused(&engine, FENCED_LEADER_EPOCH),
        },
        Case {
            name: "a newer epoch",
            edit: |request, epoch| partition(request).current_leader_epoch = epoch + 1,
            expected: refused(&engine, UNKNOWN_LEADER_EPOCH),
        },
    ];
    for Case {
        name,
        edit,
        expected,
    } in cases
    {
        let mut request = fetch(epoch, 0);
        partition(&mut request).last_fetched_epoch = 0;
        edit(&mut request, epoch);
        let Delivery {
            answer: response,
            pending: _answer,
        } = deliver(&mut engine, &request);
        check!(response == Some(expected), "{name}");
    }
}

/// `LeaderState.updateReplicaState` ignores a fetch from a negative replica
/// id: it is served, but its offset does not count toward the high
/// watermark, and it is not taken for replica 0.
#[yare::parameterized(
    negative_replica_id = { -1, false },
    zero_replica_id = { 0, true },
)]
#[test_macro(tokio::test)]
async fn a_negative_replica_id_moves_no_replica_state(replica_id: i32, hwm_moves: bool) {
    let LeaderOf0And1 {
        mut engine,
        dir: _dir,
        epoch,
    } = leader_of_0_and_1();
    let log_end = engine.log.log_end_offset();
    let before = engine.log.hwm();
    let mut request = fetch(epoch, log_end.0);
    request.replica_state.replica_id = replica_id;

    let Delivery {
        answer: response,
        pending: _answer,
    } = deliver(&mut engine, &request);

    let hwm = if hwm_moves { log_end } else { before };
    check!(engine.log.hwm() == hwm);
    check!(response == Some(answer(&engine, 0, hwm.0, bytes::Bytes::new())));
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
#[yare::parameterized(
    wait_runs_out = { false,
    WhileParked::Nothing },
    log_grows = { false,
    WhileParked::Append },
    leadership_ends = { false,
    WhileParked::LoseLeadership },
    fetcher_high_watermark_is_behind = { true,
    WhileParked::Nothing }
)]
#[test_macro(tokio::test)]
async fn a_fetch_with_nothing_new_waits_until_there_is_something_to_send(
    fetcher_hwm_behind: bool,
    while_parked: WhileParked,
) {
    let LeaderOf0And1 {
        mut engine,
        dir: _dir,
        epoch,
    } = leader_of_0_and_1();
    let log_end = engine.log.log_end_offset();
    // Replica 0 catches up, which commits the log.
    let Delivery {
        answer: caught_up,
        pending: _answer,
    } = deliver(&mut engine, &fetch(epoch, log_end.0));
    check!(caught_up.is_some());
    check!(engine.log.hwm() == log_end);

    let mut request = fetch(epoch, log_end.0);
    request.max_wait_ms = 500;
    partition(&mut request).high_watermark = if fetcher_hwm_behind {
        log_end.0 - 1
    } else {
        log_end.0
    };
    let Delivery {
        answer: immediate,
        pending: mut parked,
    } = deliver(&mut engine, &request);
    if fetcher_hwm_behind {
        check!(immediate == Some(answer(&engine, 0, log_end.0, bytes::Bytes::new())));
        return;
    }
    check!(immediate == None, "the fetch waits");
    // Nothing moved, so the fetch is still waiting just before its deadline.
    engine.complete_parked_fetches(Instant::now());
    check!(parked.try_recv().is_err(), "still waiting");

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
    check!(decode(&body) == expected);
}
