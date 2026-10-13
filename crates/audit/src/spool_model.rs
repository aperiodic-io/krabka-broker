//! Bounded Stateright model of audit-spool append, replay, crash recovery, and
//! loss accounting.
//!
//! DRIVEN: every spool append, a record's or a loss marker's, calls the
//! production `spool_append_decision` kernel used by `Spool::append`. `Reopen`
//! calls the production `replay_recovery` classifier used by `Spool::open`.
//! `Lose` calls the production `add_loss_state` helper used by
//! `PendingLosses::add`. `CommitLossMarker` and the loss reconciliation in
//! `Reopen` call the production `settle_loss_batch` kernel that
//! `PendingLosses::commit` and `PendingLosses::reconcile` apply.
//!
//! MODELED: filesystem writes are split into volatile and durable records;
//! `Sync` publishes all completed appends; `Crash` drops volatile and torn
//! writes; replay poison, sink delivery, cursor persistence, and poison removal
//! are separate actions. A poison at the current cursor makes `Reopen` stop for
//! explicit recovery, before loss reconciliation, as `Spool::open` returns
//! early. A poison behind the cursor is cleared.
//!
//! The loss path is `PendingLosses` as the writer drives it. A lost event adds
//! to the in-memory count, from the writer or concurrently from
//! `AuditHandle::emit`. `PersistLosses` writes the sidecar. The writer's
//! `persist_with` is one step per call it makes: take the snapshot, persist the
//! sidecar, append the marker (a spool record, subject to the spool's capacity),
//! sync it, commit in memory, and persist the commit. Only a concurrent loss or
//! a crash can fall between two of those steps, because the writer is serial.
//!
//! `superseded_settlement_loses_and_rereports_losses` keeps the settlement
//! `PendingLosses` applied before `settle_loss_batch` (the commit left a
//! remainder in the marker's generation, and reconcile zeroed the count) and
//! records the counterexamples the checker finds for it.
//!
//! Bounds: three record IDs, two loss events, a two-record spool, sync cadence
//! two, at most two crashes, and depth 64. Properties require every admitted
//! record to be delivered or durably pending, at-most-once automatic delivery,
//! and exact loss accounting: every loss is reported by exactly one durable
//! marker, still pending, or was held only in memory at a crash. Reachability
//! witnesses cover torn append recovery, definite replay retry, uncertain
//! replay poison, committed-poison cleanup, a marker refused by a full spool,
//! loss-marker reconciliation, and a concurrent loss carried past a marker.

use {
    crate::spool::{ReplayRecovery, add_loss_state, replay_recovery},
    krabka_verified::{
        audit::{AuditLosses as Losses, settle_loss_batch},
        spool_append_decision,
    },
    stateright::{Checker, Model, Property},
};

const MAX_DEPTH: usize = 64;
const MAX_STATES: usize = 4_000_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// It grew from 11,184 when the loss path became the production sequence of
// steps, the marker a spool record, and a third record id was added so that a
// marker competes with records for capacity.
const PINNED_UNIQUE_STATES: usize = 71_351;
const MAX_RECORDS: u8 = 3;
const MAX_LOSSES: u8 = 2;
const MAX_BYTES: u64 = 2;
const SYNC_EVERY: u64 = 2;
const SAW_TORN_APPEND: u8 = 1;
const SAW_RETRY: u8 = 1 << 1;
const SAW_UNCERTAIN_POISON: u8 = 1 << 2;
const SAW_COMMITTED_POISON: u8 = 1 << 3;
const SAW_LOSS_RECONCILE: u8 = 1 << 4;
const SAW_MARKER_REFUSED: u8 = 1 << 5;
const SAW_REMAINDER_CARRIED: u8 = 1 << 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Runtime {
    Open,
    Closed,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ReplayPhase {
    Idle,
    Poisoned { record: u8, offset: u8 },
    Delivered { record: u8, offset: u8 },
    CursorCommitted { offset: u8 },
}

/// Where the writer's `persist_with` call is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum MarkerFlow {
    Idle,
    /// `snapshot` returned `batch`.
    Snapshotted {
        batch: Losses,
    },
    /// `persist` wrote the sidecar.
    Persisted {
        batch: Losses,
    },
    /// `append_loss_marker` appended the marker as a spool record.
    Appended {
        batch: Losses,
    },
    /// `append_loss_marker` synced the spool.
    Synced {
        batch: Losses,
    },
    /// `commit` settled the in-memory count and has yet to persist it.
    Committed {
        batch: Losses,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SpoolState {
    runtime: Runtime,
    next_record: u8,
    durable_history: u8,
    volatile: u8,
    durable: u8,
    deliveries: [u8; MAX_RECORDS as usize],
    cursor: u8,
    unsynced: u64,
    replay: ReplayPhase,
    crashes: u8,
    /// Ghost: every loss event so far.
    loss_events: u8,
    /// `PendingLosses`'s in-memory state.
    memory: Losses,
    /// The `audit.losses` sidecar.
    sidecar: Losses,
    /// Ghost: losses added to memory since the last sidecar write.
    unpersisted: u8,
    /// Ghost: losses that only memory held when the process crashed.
    forgotten_at_crash: u8,
    flow: MarkerFlow,
    /// The loss marker each spool record carries, if it is one.
    markers: [Option<Losses>; MAX_RECORDS as usize],
    /// Ghost: losses taken off the pending count because a durable marker
    /// reports them, credited when that settlement is durable (the commit's
    /// sidecar write, or open-time reconciliation).
    accounted: u64,
    witnesses: u8,
}

impl SpoolState {
    /// Ghost: the losses that durable markers report, whether or not they
    /// have been replayed yet.
    fn reported(&self) -> u64 { self.durable_markers().map(|marker| marker.count).sum() }

    fn durable_markers(&self) -> impl Iterator<Item = Losses> + '_ {
        self.markers
            .iter()
            .enumerate()
            .filter(|&(record, _)| self.durable_history & (1 << record) != 0)
            .filter_map(|(_, marker)| *marker)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Action {
    Append,
    Sync,
    TearAppend,
    BeginReplay,
    Deliver,
    DefiniteFailure,
    CommitCursor,
    ClearPoison,
    Crash,
    Reopen,
    Lose,
    PersistLosses,
    SnapshotLosses,
    PersistSnapshot,
    AppendLossMarker,
    SyncLossMarker,
    CommitLossMarker,
    PersistCommit,
}

/// The two settlement functions a model run applies.
#[derive(Clone, Copy)]
struct SpoolModel {
    commit: fn(Losses, Losses) -> Losses,
    reconcile: fn(Losses, Losses) -> Losses,
}

impl SpoolModel {
    const SETTLE: Self = Self {
        commit: settle_loss_batch,
        reconcile: settle_loss_batch,
    };
}

fn bit(record: u8) -> u8 { 1 << record }

fn pending_mask(state: &SpoolState) -> u8 { state.volatile | state.durable }

/// Append one frame through the production admission kernel. It returns the
/// record id, or `None` when the spool is full.
fn spool_append(state: &mut SpoolState) -> Option<u8> {
    let decision = spool_append_decision(
        u64::from(pending_mask(state).count_ones()),
        1,
        MAX_BYTES,
        state.unsynced,
        SYNC_EVERY,
    );
    if !decision.accepted {
        return None;
    }
    let id = state.next_record;
    let record = bit(id);
    state.next_record += 1;
    if decision.sync {
        state.durable |= state.volatile | record;
        state.durable_history |= state.volatile | record;
        state.volatile = 0;
    } else {
        state.volatile |= record;
    }
    state.unsynced = decision.next_unsynced;
    Some(id)
}

fn sync(state: &mut SpoolState) {
    state.durable |= state.volatile;
    state.durable_history |= state.volatile;
    state.volatile = 0;
    state.unsynced = 0;
}

fn persist_sidecar(state: &mut SpoolState) {
    state.sidecar = state.memory;
    state.unpersisted = 0;
}

impl Model for SpoolModel {
    type Action = Action;
    type State = SpoolState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SpoolState {
            runtime: Runtime::Open,
            next_record: 0,
            durable_history: 0,
            volatile: 0,
            durable: 0,
            deliveries: [0; MAX_RECORDS as usize],
            cursor: 0,
            unsynced: 0,
            replay: ReplayPhase::Idle,
            crashes: 0,
            loss_events: 0,
            memory: Losses::default(),
            sidecar: Losses::default(),
            unpersisted: 0,
            forgotten_at_crash: 0,
            flow: MarkerFlow::Idle,
            markers: [None; MAX_RECORDS as usize],
            accounted: 0,
            witnesses: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.runtime == Runtime::Stopped {
            return;
        }
        if state.runtime == Runtime::Closed {
            actions.push(Action::Reopen);
            return;
        }
        // A loss from `AuditHandle::emit` and a crash can land anywhere.
        if state.loss_events < MAX_LOSSES {
            actions.push(Action::Lose);
        }
        if state.crashes < 2 {
            actions.push(Action::Crash);
        }
        // The writer is serial: while `persist_with` runs, its next step is
        // the only writer action.
        let next_step = match state.flow {
            MarkerFlow::Idle => None,
            MarkerFlow::Snapshotted { .. } => Some(Action::PersistSnapshot),
            MarkerFlow::Persisted { .. } => Some(Action::AppendLossMarker),
            MarkerFlow::Appended { .. } => Some(Action::SyncLossMarker),
            MarkerFlow::Synced { .. } => Some(Action::CommitLossMarker),
            MarkerFlow::Committed { .. } => Some(Action::PersistCommit),
        };
        if let Some(step) = next_step {
            actions.push(step);
            return;
        }
        if state.next_record < MAX_RECORDS {
            actions.push(Action::Append);
            actions.push(Action::TearAppend);
        }
        if state.volatile != 0 {
            actions.push(Action::Sync);
        }
        match state.replay {
            ReplayPhase::Idle if state.durable != 0 => actions.push(Action::BeginReplay),
            ReplayPhase::Poisoned { .. } => {
                actions.push(Action::Deliver);
                actions.push(Action::DefiniteFailure);
            }
            ReplayPhase::Delivered { .. } => actions.push(Action::CommitCursor),
            ReplayPhase::CursorCommitted { .. } => actions.push(Action::ClearPoison),
            ReplayPhase::Idle => {}
        }
        if state.unpersisted > 0 {
            actions.push(Action::PersistLosses);
        }
        if state.memory.count > 0 && state.next_record < MAX_RECORDS {
            actions.push(Action::SnapshotLosses);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Append => {
                spool_append(&mut state)?;
            }
            Action::Sync => sync(&mut state),
            Action::TearAppend => {
                state.next_record += 1;
                state.witnesses |= SAW_TORN_APPEND;
            }
            Action::BeginReplay => {
                let record = u8::try_from(state.durable.trailing_zeros()).unwrap_or(u8::MAX);
                state.replay = ReplayPhase::Poisoned {
                    record,
                    offset: state.cursor,
                };
            }
            Action::Deliver => {
                let ReplayPhase::Poisoned { record, offset } = state.replay else {
                    return None;
                };
                state.deliveries[usize::from(record)] += 1;
                state.replay = ReplayPhase::Delivered { record, offset };
            }
            Action::DefiniteFailure => {
                if !matches!(state.replay, ReplayPhase::Poisoned { .. }) {
                    return None;
                }
                state.replay = ReplayPhase::Idle;
                state.witnesses |= SAW_RETRY;
            }
            Action::CommitCursor => {
                let ReplayPhase::Delivered { record, offset } = state.replay else {
                    return None;
                };
                state.durable &= !bit(record);
                state.cursor += 1;
                state.replay = ReplayPhase::CursorCommitted { offset };
            }
            Action::ClearPoison => state.replay = ReplayPhase::Idle,
            Action::Crash => {
                state.volatile = 0;
                state.unsynced = 0;
                state.crashes += 1;
                state.runtime = Runtime::Closed;
                // Memory is gone: the flow with it, and a marker frame that
                // never reached disk.
                state.forgotten_at_crash += state.unpersisted;
                state.unpersisted = 0;
                state.memory = state.sidecar;
                state.flow = MarkerFlow::Idle;
                for (record, marker) in state.markers.iter_mut().enumerate() {
                    if state.durable_history & (1 << record) == 0 {
                        *marker = None;
                    }
                }
            }
            Action::Reopen => {
                state.runtime = Runtime::Open;
                return self.reopen(state);
            }
            Action::Lose => {
                (state.memory.generation, state.memory.count) =
                    add_loss_state(state.memory.generation, state.memory.count, 1);
                state.loss_events += 1;
                state.unpersisted += 1;
            }
            Action::PersistLosses => persist_sidecar(&mut state),
            Action::SnapshotLosses => {
                state.flow = MarkerFlow::Snapshotted {
                    batch: state.memory,
                };
            }
            Action::PersistSnapshot => {
                let MarkerFlow::Snapshotted { batch } = state.flow else {
                    return None;
                };
                persist_sidecar(&mut state);
                state.flow = MarkerFlow::Persisted { batch };
            }
            Action::AppendLossMarker => {
                let MarkerFlow::Persisted { batch } = state.flow else {
                    return None;
                };
                if let Some(record) = spool_append(&mut state) {
                    state.markers[usize::from(record)] = Some(batch);
                    state.flow = MarkerFlow::Appended { batch };
                } else {
                    // `append_loss_marker` fails with "spool is full"; the
                    // losses stay pending for the next attempt.
                    state.flow = MarkerFlow::Idle;
                    state.witnesses |= SAW_MARKER_REFUSED;
                }
            }
            Action::SyncLossMarker => {
                let MarkerFlow::Appended { batch } = state.flow else {
                    return None;
                };
                sync(&mut state);
                state.flow = MarkerFlow::Synced { batch };
            }
            Action::CommitLossMarker => {
                let MarkerFlow::Synced { batch } = state.flow else {
                    return None;
                };
                state.memory = (self.commit)(state.memory, batch);
                state.flow = MarkerFlow::Committed { batch };
            }
            Action::PersistCommit => {
                let MarkerFlow::Committed { batch } = state.flow else {
                    return None;
                };
                persist_sidecar(&mut state);
                state.accounted = state.accounted.saturating_add(batch.count);
                if state.sidecar.count > 0 {
                    state.witnesses |= SAW_REMAINDER_CARRIED;
                }
                state.flow = MarkerFlow::Idle;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("delivered_or_durably_pending", |_, state: &SpoolState| {
                let delivered = state
                    .deliveries
                    .iter()
                    .enumerate()
                    .fold(0_u8, |mask, (record, count)| {
                        mask | u8::from(*count > 0) << record
                    });
                state.durable_history & !(delivered | state.durable) == 0
            }),
            Property::always(
                "automatic_delivery_at_most_once",
                |_, state: &SpoolState| state.deliveries.iter().all(|count| *count <= 1),
            ),
            // No marker in the spool names a generation the sidecar has not
            // reached, and no two name the same one: reconciliation finds a
            // generation's marker by its generation alone.
            Property::always("marker_generations_unique", |_, state: &SpoolState| {
                let generations: Vec<u64> = state.durable_markers().map(|m| m.generation).collect();
                generations.iter().all(|g| *g <= state.sidecar.generation)
                    && generations
                        .iter()
                        .enumerate()
                        .all(|(i, g)| !generations[..i].contains(g))
            }),
            // Every loss is settled against a durable marker, still pending in
            // the sidecar, added since the last sidecar write, or was held
            // only in memory at a crash, which is where the queued events a
            // crash drops are too. Exactly one of these.
            Property::always("losses_accounted", |_, state: &SpoolState| {
                u64::from(state.loss_events)
                    == state.accounted
                        + state.sidecar.count
                        + u64::from(state.unpersisted)
                        + u64::from(state.forgotten_at_crash)
            }),
            // A settlement is credited only against losses a durable marker
            // reports, and, once the writer is idle on an open spool, every
            // durable marker's losses are settled: none is reported twice or
            // left pending beside its marker.
            Property::always("markers_settled_once", |_, state: &SpoolState| {
                state.accounted <= state.reported()
                    && (state.runtime != Runtime::Open
                        || state.flow != MarkerFlow::Idle
                        || state.accounted == state.reported())
            }),
            Property::sometimes("torn_append_recovered", |_, state: &SpoolState| {
                state.witnesses & SAW_TORN_APPEND != 0 && state.crashes > 0
            }),
            Property::sometimes("definite_failure_can_retry", |_, state: &SpoolState| {
                state.witnesses & SAW_RETRY != 0
            }),
            Property::sometimes("uncertain_delivery_stops", |_, state: &SpoolState| {
                state.witnesses & SAW_UNCERTAIN_POISON != 0 && state.runtime == Runtime::Stopped
            }),
            Property::sometimes("committed_poison_clears", |_, state: &SpoolState| {
                state.witnesses & SAW_COMMITTED_POISON != 0
                    && matches!(state.replay, ReplayPhase::Idle)
            }),
            Property::sometimes("full_spool_refuses_marker", |_, state: &SpoolState| {
                state.witnesses & SAW_MARKER_REFUSED != 0
            }),
            Property::sometimes("durable_loss_marker_reconciles", |_, state: &SpoolState| {
                state.witnesses & SAW_LOSS_RECONCILE != 0
            }),
            Property::sometimes(
                "concurrent_loss_carried_forward",
                |_, state: &SpoolState| state.witnesses & SAW_REMAINDER_CARRIED != 0,
            ),
        ]
    }
}

impl SpoolModel {
    fn reopen(self, mut state: SpoolState) -> Option<SpoolState> {
        match state.replay {
            ReplayPhase::Idle => {}
            ReplayPhase::Poisoned { offset, .. } | ReplayPhase::Delivered { offset, .. } => {
                let decision =
                    replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
                if decision == ReplayRecovery::RequireExplicitRecovery {
                    // `Spool::open` returns the poison error before it
                    // reconciles losses.
                    state.runtime = Runtime::Stopped;
                    state.witnesses |= SAW_UNCERTAIN_POISON;
                    return Some(state);
                }
            }
            ReplayPhase::CursorCommitted { offset } => {
                let decision =
                    replay_recovery(true, Some(u64::from(offset)), u64::from(state.cursor));
                if decision != ReplayRecovery::ClearPoison {
                    return None;
                }
                state.replay = ReplayPhase::Idle;
                state.witnesses |= SAW_COMMITTED_POISON;
            }
        }
        // `PendingLosses::reconcile`: a durable marker of the sidecar's
        // generation settles it.
        let sidecar = state.sidecar;
        let marker = state
            .durable_markers()
            .find(|m| m.generation == sidecar.generation);
        if sidecar.count > 0
            && let Some(marker) = marker
        {
            state.memory = (self.reconcile)(state.sidecar, marker);
            persist_sidecar(&mut state);
            state.accounted = state.accounted.saturating_add(marker.count);
            state.witnesses |= SAW_LOSS_RECONCILE;
        }
        Some(state)
    }
}

fn check(model: SpoolModel) -> impl Checker<SpoolModel> {
    model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join()
}

krabka_macros::bounded_bfs!(bounded_bfs);

#[test]
fn audit_spool_crash_and_replay_interleavings() {
    let checker = bounded_bfs(SpoolModel::SETTLE, "audit-spool", MAX_DEPTH, MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

/// `PendingLosses::commit` before `settle_loss_batch`: it settled the
/// marker's count but left any remainder in the marker's generation.
fn superseded_commit(state: Losses, batch: Losses) -> Losses {
    if state.generation != batch.generation {
        return state;
    }
    Losses {
        generation: state.generation,
        count: state.count.saturating_sub(batch.count),
    }
}

/// `PendingLosses::reconcile` before `settle_loss_batch`: a marker of the
/// sidecar's generation zeroed the whole count.
fn superseded_reconcile(state: Losses, _marker: Losses) -> Losses {
    Losses {
        generation: state.generation,
        count: 0,
    }
}

/// RED witness: the checker rejects the settlement `PendingLosses` applied
/// before `settle_loss_batch`. A loss that `AuditHandle::emit` adds while a
/// marker is in flight stays in the marker's generation. Reconciliation after
/// a crash then zeroes it along with the marker's own losses, or a second
/// marker names the same generation.
#[test]
fn superseded_settlement_loses_and_rereports_losses() {
    let checker = check(SpoolModel {
        commit: superseded_commit,
        reconcile: superseded_reconcile,
    });
    for property in [
        "losses_accounted",
        "marker_generations_unique",
        "markers_settled_once",
    ] {
        assert2::assert!(checker.discovery(property).is_some(), "{property}");
    }
}

#[test]
fn loss_accounting_saturates_without_wrapping() {
    assert2::check!(add_loss_state(u64::MAX, 0, 1) == (u64::MAX, 1));
    assert2::check!(add_loss_state(7, u64::MAX, 1) == (7, u64::MAX));
}

#[test]
fn malformed_or_stale_replay_poison_requires_recovery() {
    assert2::check!(replay_recovery(false, Some(0), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, None, 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(1), 1) == ReplayRecovery::RequireExplicitRecovery);
    assert2::check!(replay_recovery(true, Some(0), 1) == ReplayRecovery::ClearPoison);
}
