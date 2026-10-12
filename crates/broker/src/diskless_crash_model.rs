//! Diskless WAL crash-restart model for partial durability windows.
//!
//! This model is small on purpose. It composes the [crash-window recovery] rules
//! with the [distributed WAL quorum] and stateless appenders. The
//! [diskless WAL design] defines both slices.
//!
//! `KRaft` can reserve offsets before the bytes fsync. An object PUT can come
//! before the index commit. An fsync can tear the active tail. Any WAL member
//! can start a reservation. A trim must stop at the committed index frontier.
//!
//! DRIVEN: three production kernels decide the steps that carry the
//! properties. `krabka_verified::consensus::majority_watermark` computes the
//! quorum-durable frontier (`wal_acked`) from the live WAL members, as
//! `WalShardEngine::record_durable_offset` does over voter-reported durable
//! offsets. `krabka_verified::offset_allocator::wal_reservation_frontier`
//! folds the pending reservation chain and
//! `krabka_verified::offset_allocator::reserve_offsets` places the next one,
//! as the controller's `V1PartitionOffsetAdvance` submit path does, so
//! `reservations_gap_free_and_unique` checks what those kernels produce under
//! every interleaving of two appenders and the crash windows.
//! `krabka_verified::diskless::diskless_trim_decision` places every trim, as
//! the flusher does, so `trim_at_committed_index_frontier` checks its output
//! against the index and quorum frontiers.
//!
//! MODELED: the WAL members, the fsync, the object PUT, and the index commit
//! are counters, not logs or objects. Every reservation the model makes stays
//! pending in the controller (the image frontier is 0), so the reservation
//! frontier is the fold over all of them. The flusher's trim runs with no
//! safety lag against `wal_acked` as the high watermark. At most one WAL
//! member is lost. The idempotent-producer dedup rebuild is not modelled:
//! the model has no producer sequences to rebuild, so any property about it
//! would hold by construction; `diskless::recovery`'s unit tests rebuild the
//! dedup state from a real recovered log instead.
//!
//! [crash-window recovery]: ../docs/diskless-wal-design.md#crash-window-recovery
//! [distributed WAL quorum]: ../docs/diskless-wal-design.md#distributed-wal-quorum-and-stateless-appenders
//! [diskless WAL design]: ../docs/diskless-wal-design.md

use stateright::{Checker, Model, Property};

use crate::model_check::run_bfs;

const MAX_OFFSET: i64 = 2;
const APPENDERS: usize = 2;
const MAX_DEPTH: usize = 24;
const TARGET_STATE_COUNT: usize = 100_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 6_341;

const WITNESS_KRAFT_FSYNC_GAP: u8 = 1 << 0;
const WITNESS_PUT_BEFORE_INDEX: u8 = 1 << 1;
const WITNESS_MID_FSYNC: u8 = 1 << 2;
const WITNESS_TRIM_AT_INDEX: u8 = 1 << 3;
const WITNESS_MINORITY_WAL_LOSS: u8 = 1 << 4;
const WITNESS_STATELESS_APPEND: u8 = 1 << 5;
const WITNESS_SEQUENCER_HANDOFF: u8 = 1 << 6;

const WAL_NODES: usize = 3;
const WAL_MAJORITY: usize = 2;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CrashState {
    kraft_next: i64,
    log_end: i64,
    wal_nodes: [i64; WAL_NODES],
    wal_lost: [bool; WAL_NODES],
    wal_acked: i64,
    /// Last quorum-durable frontier observed before a sequencer handoff.
    /// `wal_acked` may never fall below it.
    handoff_wal_acked: i64,
    /// Client-visible frontier. Unlike `wal_acked`, this may temporarily
    /// regress while a new sequencer re-derives its view from durable media.
    advertised_hwm: i64,
    sequencer_epoch: u8,
    object_frontier: i64,
    index_frontier: i64,
    trimmed: i64,
    /// The log end the last fsync acknowledged to the producer.
    producer_committed: i64,
    reservations: Vec<(i64, i64)>,
    appenders_seen: u8,
    witnesses: u8,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Act {
    ReserveVia(usize),
    FsyncAppend,
    CrashBeforeFsync,
    CrashMidFsync,
    PutObject,
    CommitIndex,
    Trim,
    LoseWalNode(usize),
    SequencerHandoff,
}

#[derive(Clone, Debug)]
struct CrashModel;

impl Model for CrashModel {
    type Action = Act;
    type State = CrashState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CrashState {
            kraft_next: 0,
            log_end: 0,
            wal_nodes: [0; WAL_NODES],
            wal_lost: [false; WAL_NODES],
            wal_acked: 0,
            handoff_wal_acked: 0,
            advertised_hwm: 0,
            sequencer_epoch: 0,
            object_frontier: 0,
            index_frontier: 0,
            trimmed: 0,
            producer_committed: 0,
            reservations: Vec::new(),
            appenders_seen: 0,
            witnesses: 0,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        if s.kraft_next < MAX_OFFSET {
            for node in 0..APPENDERS {
                if !s.wal_lost[node] {
                    acts.push(Act::ReserveVia(node));
                }
            }
        }
        if s.log_end < s.kraft_next {
            acts.push(Act::FsyncAppend);
            acts.push(Act::CrashBeforeFsync);
            acts.push(Act::CrashMidFsync);
        }
        if s.object_frontier < s.wal_acked {
            acts.push(Act::PutObject);
        }
        if s.index_frontier < s.object_frontier {
            acts.push(Act::CommitIndex);
        }
        if s.trimmed < s.index_frontier {
            acts.push(Act::Trim);
        }
        if s.wal_lost.iter().filter(|lost| **lost).count() == 0 && s.wal_acked > 0 {
            for node in 0..WAL_NODES {
                acts.push(Act::LoseWalNode(node));
            }
        }
        if s.wal_acked > 0 && s.sequencer_epoch < 2 {
            acts.push(Act::SequencerHandoff);
        }
    }

    krabka_macros::model_transition!(last, action, s; {
            match action {
                Act::ReserveVia(node) => {
                    let (base, next) = reserve_via_controller(&s);
                    s.kraft_next = next;
                    s.reservations.push((base, next));
                    s.appenders_seen |= 1 << node;
                    if s.appenders_seen.count_ones() >= 2 {
                        s.witnesses |= WITNESS_STATELESS_APPEND;
                    }
                }
                Act::FsyncAppend => {
                    s.log_end += 1;
                    fsync_quorum(&mut s);
                    s.producer_committed = s.log_end;
                }
                Act::CrashBeforeFsync => {
                    if s.kraft_next > s.log_end {
                        s.witnesses |= WITNESS_KRAFT_FSYNC_GAP;
                    }
                }
                Act::CrashMidFsync => {
                    s.witnesses |= WITNESS_MID_FSYNC;
                    s.kraft_next = s.kraft_next.max(s.log_end);
                }
                Act::PutObject => {
                    s.object_frontier = s.wal_acked;
                    if s.object_frontier > s.index_frontier {
                        s.witnesses |= WITNESS_PUT_BEFORE_INDEX;
                    }
                }
                Act::CommitIndex => {
                    s.index_frontier = s.object_frontier;
                }
                Act::Trim => {
                    let decision = krabka_verified::diskless::diskless_trim_decision(
                        s.index_frontier,
                        s.wal_acked,
                        0,
                        s.trimmed,
                    );
                    if decision.should_trim {
                        s.trimmed = decision.target;
                    }
                    if s.trimmed > 0 && s.trimmed == s.index_frontier {
                        s.witnesses |= WITNESS_TRIM_AT_INDEX;
                    }
                }
                Act::LoseWalNode(node) => {
                    s.wal_lost[node] = true;
                    s.witnesses |= WITNESS_MINORITY_WAL_LOSS;
                }
                Act::SequencerHandoff => {
                    s.sequencer_epoch += 1;
                    s.handoff_wal_acked = s.handoff_wal_acked.max(s.wal_acked);
                    // KIP-207 permits the newly advertised frontier to move back
                    // while authority changes. Durability does not move back: the
                    // new sequencer can re-derive `wal_acked` from the surviving
                    // quorum, independently of this conservative visible value.
                    s.advertised_hwm = s.index_frontier.min(s.wal_acked);
                    s.witnesses |= WITNESS_SEQUENCER_HANDOFF;
                }
            }
            Some(s)

    });

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("wal_acked_durable", |_, s: &CrashState| {
                s.wal_acked <= surviving_wal_frontier(s)
            }),
            Property::always("committed_durable", |_, s: &CrashState| {
                s.producer_committed <= surviving_wal_frontier(s)
            }),
            Property::always(
                "sequencer_handoff_never_regresses_wal_acked",
                |_, s: &CrashState| s.wal_acked >= s.handoff_wal_acked,
            ),
            Property::always("trim_at_committed_index_frontier", |_, s: &CrashState| {
                s.trimmed <= s.index_frontier && s.trimmed <= s.wal_acked
            }),
            Property::always("reservations_gap_free_and_unique", |_, s: &CrashState| {
                let mut next = 0;
                for &(base, end) in &s.reservations {
                    if base != next || end <= base {
                        return false;
                    }
                    next = end;
                }
                next == s.kraft_next
            }),
            Property::sometimes("crash_in_kraft_fsync_gap", |_, s: &CrashState| {
                s.witnesses & WITNESS_KRAFT_FSYNC_GAP != 0
            }),
            Property::sometimes("crash_between_put_and_index", |_, s: &CrashState| {
                s.witnesses & WITNESS_PUT_BEFORE_INDEX != 0
            }),
            Property::sometimes("crash_mid_fsync", |_, s: &CrashState| {
                s.witnesses & WITNESS_MID_FSYNC != 0
            }),
            Property::sometimes("trim_at_index_frontier", |_, s: &CrashState| {
                s.witnesses & WITNESS_TRIM_AT_INDEX != 0
            }),
            Property::sometimes(
                "acked_unflushed_survives_minority_wal_loss",
                |_, s: &CrashState| {
                    s.witnesses & WITNESS_MINORITY_WAL_LOSS != 0 && s.wal_acked > s.trimmed
                },
            ),
            Property::sometimes("two_appenders_race_gap_free", |_, s: &CrashState| {
                s.witnesses & WITNESS_STATELESS_APPEND != 0
            }),
            Property::sometimes(
                "sequencer_handoff_regresses_only_advertised_hwm",
                |_, s: &CrashState| {
                    s.witnesses & WITNESS_SEQUENCER_HANDOFF != 0 && s.advertised_hwm < s.wal_acked
                },
            ),
        ]
    }
}

fn fsync_quorum(s: &mut CrashState) {
    let mut synced = 0;
    for node in 0..WAL_NODES {
        if !s.wal_lost[node] && synced < WAL_MAJORITY {
            s.wal_nodes[node] = s.log_end;
            synced += 1;
        }
    }
    s.wal_acked = quorum_frontier(s);
    s.advertised_hwm = s.wal_acked;
}

fn quorum_frontier(s: &CrashState) -> i64 {
    let live: Vec<i64> = live_wal_offsets(s).collect();
    if live.len() < WAL_MAJORITY {
        return s.trimmed;
    }
    let mut live = live;
    live.sort_unstable();
    let leader_end = live.pop().unwrap_or(0);
    let followers = live;
    krabka_verified::consensus::majority_watermark(leader_end, &followers, WAL_MAJORITY, 0)
}

/// The controller's reservation for one appender's one-record batch: the
/// pending chain folded from the image frontier with
/// `wal_reservation_frontier`, then `reserve_offsets` from its end. Either
/// kernel refusing is a failure of the run, not a pruned step.
fn reserve_via_controller(s: &CrashState) -> (i64, i64) {
    let pending_frontier = s
        .reservations
        .iter()
        .try_fold(0, |frontier, &(base, end)| {
            krabka_verified::offset_allocator::wal_reservation_frontier(frontier, base, end - base)
        })
        .expect("the controller's pending reservation chain stays exact");
    krabka_verified::offset_allocator::reserve_offsets(pending_frontier, 1)
        .expect("a bounded reservation is representable")
}

fn surviving_wal_frontier(s: &CrashState) -> i64 {
    live_wal_offsets(s)
        .max()
        .unwrap_or(s.index_frontier)
        .max(s.index_frontier)
}

fn live_wal_offsets(s: &CrashState) -> impl Iterator<Item = i64> + '_ {
    s.wal_nodes
        .iter()
        .copied()
        .zip(s.wal_lost.iter().copied())
        .filter_map(|(offset, lost)| (!lost).then_some(offset))
}

fn run() {
    let checker = run_bfs(
        CrashModel,
        "diskless_crash_model",
        MAX_DEPTH,
        TARGET_STATE_COUNT,
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn diskless_crash_model() {
    run();
}
