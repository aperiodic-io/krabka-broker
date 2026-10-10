//! Bounded crash/failover model for the in-process three-replica WAL harness.
//!
//! Bounds: three voters, two one-record batches, three leader epochs, eight
//! ordered operations, and fewer than 20,000 generated states. Every append, quorum
//! acknowledgement, and crash recovery reconstructs real `Log` instances and
//! drives `Log::append`, `WalShardEngine::replicate_and_sync`, or
//! `WalShardEngine::new(OpenMode::Recover)`. Recovery performs the real tail
//! truncation and repair. Stateright enumerates replica failures and
//! revivals, and clean leader changes, around those operations.
//!
//! Scope: this is the in-process replica harness, the `#[cfg(test)]`
//! `WalShardEngine::new` and the local-replica branch of `replicate_and_sync`.
//! The production diskless path, where remote voters fetch the leader's tail
//! and report durable offsets (`record_follower_ack`, `serve_fetch`), is not
//! driven here; its unit tests in `engine/distributed.rs`, `engine.rs`, and
//! `follower.rs` cover it.
//!
//! DRIVEN: record-batch append, exact-range replica sync with byte
//! verification of each replica's whole retained log, quorum acknowledgement
//! and watermark advancement, and byte-agreement recovery and truncation.
//! Each acknowledgement is checked against its rule: it succeeds exactly when
//! at least two live voters (the leader among them) hold no record that
//! disagrees with the leader's log up to the target. Each recovery must
//! succeed; a recovery error fails the run instead of pruning the edge.
//!
//! MODELED: `KRaft` supplies a monotonically increasing leader epoch and
//! elects a new leader only when the current one is down. The model does
//! not derive leader completeness; it assumes it. `Elect(v)` is enabled only
//! when `v` is live and its log starts with the whole acknowledged prefix
//! (`has_committed_prefix`), which is what KIP-595's vote rule (a voter grants
//! only to a candidate whose log is at least as up to date) and quorum
//! intersection guarantee in production. Appends are the leader's own; a
//! deposed leader's append is fenced outside this engine (the registry
//! checks the leader epoch before it reaches a shard), so the model does not
//! enumerate one. Process crashes preserve each modelled disk exactly.
//! Filesystem calls are assumed atomic at the successful operation boundaries
//! exposed by `krabka-log`.

use std::sync::{Arc, Mutex};

use krabka_ids::Offset;
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::convert::ByteSizeExt as _;
use stateright::{Checker, Model, Property};

use super::{OpenMode, WalReplica, WalShardEngine, split_batches};
use crate::model_check::run_bfs;

fn record_committed_watermark(state: &mut WalState, hwm: usize) {
    assert2::assert!(hwm >= state.committed.len());
    assert2::assert!(state.logs[state.leader][..state.committed.len()] == state.committed[..]);
    state.hwm = hwm;
    state.committed = state.logs[state.leader][..hwm].to_vec();
    state.last_ack_failed = false;
}

const VOTERS: usize = 3;
const MAX_RECORDS: usize = 2;
const MAX_EPOCH: u8 = 2;
const MAX_STEPS: u8 = 8;
const MAX_DEPTH: usize = 10;
const MAX_STATES: usize = 20_000;
// The exact unique-state count of the exhaustive BFS. A changed count is a
// changed reachable set -- a dropped action, a `next_state` arm that stops
// firing, a state field that stops being hashed -- and fails the run rather
// than silently shrinking the search. The generated count is not pinned: it
// depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 2_996;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WalState {
    steps: u8,
    logs: [Vec<u8>; VOTERS],
    leader: usize,
    leader_epoch: u8,
    live: u8,
    hwm: usize,
    committed: Vec<u8>,
    last_ack_failed: bool,
    recovered: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Append,
    Acknowledge,
    Fail(usize),
    Revive(usize),
    Elect(usize),
    CrashRecover,
}

#[derive(Clone, Debug)]
struct WalModel;

impl Model for WalModel {
    type Action = Action;
    type State = WalState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WalState {
            steps: 0,
            logs: std::array::from_fn(|_| Vec::new()),
            leader: 0,
            leader_epoch: 0,
            live: 0b111,
            hwm: 0,
            committed: Vec::new(),
            last_ack_failed: false,
            recovered: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.steps == MAX_STEPS {
            return;
        }
        if is_live(state, state.leader) {
            if state.logs[state.leader].len() < MAX_RECORDS {
                actions.push(Action::Append);
            }
            if state.logs[state.leader].len() > state.hwm {
                actions.push(Action::Acknowledge);
            }
        }
        for voter in 0..VOTERS {
            if is_live(state, voter) {
                actions.push(Action::Fail(voter));
                if !is_live(state, state.leader)
                    && voter != state.leader
                    && state.leader_epoch < MAX_EPOCH
                    && has_committed_prefix(state, voter)
                {
                    actions.push(Action::Elect(voter));
                }
            } else {
                actions.push(Action::Revive(voter));
            }
        }
        if state.live == 0b111 && state.logs.iter().any(|log| log != &state.logs[0]) {
            actions.push(Action::CrashRecover);
        }
    }

    krabka_macros::model_transition!(last, action, state; {
            state.steps += 1;
            match action {
                Action::Append => {
                    state.logs = drive_append(&state);
                    state.last_ack_failed = false;
                }
                Action::Acknowledge => {
                    let expected_success = ack_has_a_majority(&state);
                    let (logs, result) = drive_ack(&state);
                    assert2::assert!(
                        result.is_ok() == expected_success,
                        "an acknowledgement succeeds exactly when a majority can hold the leader's log"
                    );
                    state.logs = logs;
                    match result {
                        Ok(hwm) => {
                            record_committed_watermark(&mut state, hwm);
                        }
                        Err(()) => {
                            state.last_ack_failed = true;
                        }
                    }
                }
                Action::Fail(voter) => {
                    state.live &= !(1 << voter);
                }
                Action::Revive(voter) => {
                    state.live |= 1 << voter;
                }
                Action::Elect(voter) => {
                    state.leader = voter;
                    state.leader_epoch += 1;
                    state.last_ack_failed = false;
                }
                Action::CrashRecover => {
                    let (logs, hwm) = drive_recovery(&state);
                    state.logs = logs;
                    record_committed_watermark(&mut state, hwm);
                    state.recovered = true;
                }
            }
            Some(state)

    });

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hwm_is_inside_leader_log", |_, state: &WalState| {
                state.hwm <= state.logs[state.leader].len()
            }),
            Property::always("committed_frontier_matches_hwm", |_, state: &WalState| {
                state.committed.len() == state.hwm
            }),
            Property::always("committed_prefix_has_a_quorum", |_, state: &WalState| {
                state
                    .logs
                    .iter()
                    .filter(|log| log.len() >= state.hwm && log[..state.hwm] == state.committed[..])
                    .count()
                    >= 2
            }),
            Property::sometimes("quorum_acknowledges", |_, state: &WalState| state.hwm > 0),
            Property::sometimes("minority_ack_fails", |_, state: &WalState| {
                state.last_ack_failed && state.live.count_ones() < 2
            }),
            Property::sometimes(
                "divergent_ack_fails_with_a_live_majority",
                |_, state: &WalState| state.last_ack_failed && state.live.count_ones() >= 2,
            ),
            Property::sometimes("recovery_runs", |_, state: &WalState| state.recovered),
            Property::sometimes("leader_changes", |_, state: &WalState| state.leader != 0),
            Property::sometimes("replicas_diverge", |_, state: &WalState| {
                state.logs.iter().any(|log| log != &state.logs[0])
            }),
        ]
    }
}

fn is_live(state: &WalState, voter: usize) -> bool {
    state.live & (1 << voter) != 0
}

fn has_committed_prefix(state: &WalState, voter: usize) -> bool {
    state.logs[voter].len() >= state.hwm && state.logs[voter][..state.hwm] == state.committed[..]
}

/// The rule an acknowledgement follows: a live voter can hold the leader's
/// records up to the target exactly when its log agrees with the leader's
/// wherever both have a record, and the acknowledgement succeeds when at
/// least two voters can.
fn ack_has_a_majority(state: &WalState) -> bool {
    let leader_log = &state.logs[state.leader];
    (0..VOTERS)
        .filter(|voter| is_live(state, *voter))
        .filter(|voter| {
            let log = &state.logs[*voter];
            let shared = log.len().min(leader_log.len());
            log[..shared] == leader_log[..shared]
        })
        .count()
        >= 2
}

fn drive_append(state: &WalState) -> [Vec<u8>; VOTERS] {
    let (_directory, logs) = materialize(state);
    logs[state.leader]
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut RecordBatch {
            partition_leader_epoch: i32::from(state.leader_epoch),
            records: vec![Record::default()],
            ..RecordBatch::default()
        })
        .expect("bounded WAL append");
    observe(&logs)
}

fn drive_ack(state: &WalState) -> ([Vec<u8>; VOTERS], Result<usize, ()>) {
    let (_directory, logs) = materialize(state);
    let replicas = ordered_replicas(state.leader, &logs);
    let engine = WalShardEngine::for_model(replicas, Offset(model_offset(state.hwm)));
    for voter in 0..VOTERS {
        engine.set_replica_alive(node(voter), is_live(state, voter));
    }
    let target = Offset(model_offset(state.logs[state.leader].len()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bounded WAL runtime");
    let result = runtime
        .block_on(engine.replicate_and_sync(&logs[state.leader], target))
        .map(|hwm| model_index(hwm.0))
        .map_err(|_| ());
    (observe(&logs), result)
}

fn drive_recovery(state: &WalState) -> ([Vec<u8>; VOTERS], usize) {
    let (_directory, logs) = materialize(state);
    let replicas = ordered_replicas(state.leader, &logs);
    let engine = WalShardEngine::new(replicas, OpenMode::Recover)
        .expect("WAL recovery opens every reachable replica set");
    let hwm = model_index(engine.durable_watermark().0);
    (observe(&logs), hwm)
}

fn materialize(state: &WalState) -> (tempfile::TempDir, [Arc<Mutex<Log>>; VOTERS]) {
    let directory = tempfile::tempdir().expect("bounded WAL directory");
    let logs = std::array::from_fn(|voter| {
        let mut log = Log::open(
            directory.path().join(format!("voter-{voter}")),
            LogConfig::default(),
        )
        .expect("open bounded WAL");
        for epoch in &state.logs[voter] {
            log.append(&mut RecordBatch {
                partition_leader_epoch: i32::from(*epoch),
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .expect("materialize bounded WAL");
        }
        log.sync().expect("sync bounded WAL");
        Arc::new(Mutex::new(log))
    });
    (directory, logs)
}

fn ordered_replicas(leader: usize, logs: &[Arc<Mutex<Log>>; VOTERS]) -> Vec<WalReplica> {
    std::iter::once(leader)
        .chain((0..VOTERS).filter(|voter| *voter != leader))
        .map(|voter| WalReplica::for_test(node(voter), Arc::clone(&logs[voter])))
        .collect()
}

fn observe(logs: &[Arc<Mutex<Log>>; VOTERS]) -> [Vec<u8>; VOTERS] {
    std::array::from_fn(|voter| {
        let log = logs[voter]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let end = log.log_end_offset();
        let raw = log
            .read_raw(Offset(0), end, krabka_units::ByteSize::from_bytes(u64::MAX))
            .expect("read bounded WAL");
        split_batches(&raw.bytes)
            .expect("decode bounded WAL")
            .into_iter()
            .map(|batch| {
                u8::try_from(batch.verbatim.leader_epoch.0).expect("bounded epoch fits in u8")
            })
            .collect()
    })
}

fn node(voter: usize) -> NodeId {
    NodeId(u64::try_from(voter).expect("bounded voter fits in u64"))
}

fn model_offset(offset: usize) -> i64 {
    i64::try_from(offset).expect("bounded offset fits in i64")
}

fn model_index(offset: i64) -> usize {
    usize::try_from(offset).expect("nonnegative bounded offset fits in usize")
}

#[test]
fn quorum_wal_append_ack_and_recovery_model() {
    let checker = run_bfs(WalModel, "quorum_wal_model", MAX_DEPTH, MAX_STATES);
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
