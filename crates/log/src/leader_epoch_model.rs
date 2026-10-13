//! Exhaustive stateright model of KIP-101/279/320 log reconciliation between a
//! leader and its followers, driven through the production lookup
//! (`epoch_and_offset_for_entries`, Kafka's `LeaderEpochFileCache.endOffsetFor`)
//! and the production checkpoint truncation (`truncate_to`).
//!
//! # What is modeled
//!
//! A small cluster of replicas, each with a log -- one leader epoch per
//! offset, so a record is identified by `(offset, epoch)` -- and a leader
//! epoch checkpoint. Three actions interleave freely:
//!
//! * `Elect(r)` bumps the leader epoch and makes any replica `r` the leader,
//!   clean or unclean. As Kafka's `Partition.makeLeader` does, the new leader
//!   assigns the new epoch at its log end (`maybeAssignEpochStartOffset`), so
//!   an epoch in which a leader writes nothing still leaves a gap in every
//!   other replica's history.
//! * `Write` appends one record at the current epoch on the leader.
//! * `Fetch(f)` runs one follower fetch round: the follower sends its log end
//!   and its latest checkpoint epoch as `lastFetchedEpoch`; the leader answers
//!   as `Partition.readRecords` does (`OFFSET_OUT_OF_RANGE`, a
//!   `diverging_epoch`, or one record); and the follower acts on a
//!   `diverging_epoch` as `AbstractFetcherThread.getOffsetTruncationState`
//!   does, asking its *own* checkpoint `endOffsetFor` and truncating, over
//!   several rounds when it does not know the leader's epoch (KIP-279).
//!
//! # What is checked
//!
//! * `reconciled_follower_is_leader_prefix` (the headline) -- once the leader
//!   accepts a follower's position and serves it records, the follower's log
//!   is a prefix of the leader's: the divergent suffix is gone.
//! * `no_agreed_record_truncated` -- no truncation ever drops a record the
//!   follower shares with the leader (the common prefix survives).
//! * `divergence_makes_progress` -- every `diverging_epoch` changes the
//!   follower, so the protocol cannot livelock on it.
//! * `leader_places_every_follower_epoch` -- the leader never answers
//!   `OFFSET_OUT_OF_RANGE`: with the epoch assigned at election, its latest
//!   epoch bounds every follower's.
//! * `checkpoints_strictly_increasing` -- the lookup's precondition.
//!
//! `sometimes` witnesses show the interesting paths are reached: a divergent
//! suffix really truncated, a gap epoch resolved to the leader's floor epoch,
//! a KIP-279 step-back round, and every replica converged afterwards.
//!
//! # Bounds
//!
//! Two replicas with leader epochs through 5 and at most three records per
//! leader log, and three replicas with epochs through 3 and two records; each
//! search is exhaustive over every interleaving within those bounds.
//!
//! A third configuration -- two replicas, epochs through 4, three records --
//! drops the assign-at-election step and shows that the model is sharp: the
//! leader then cannot place a follower's newer epoch and answers
//! `OFFSET_OUT_OF_RANGE`, while the safety properties still hold.
//!
//! [`fuzz`](super::fuzz) drives the same step function over random schedules
//! at a scale this search cannot reach.

use {
    super::{EpochEntry, epoch_and_offset_for_entries, truncate_to},
    crate::model_check::run_bfs,
    krabka_ids::{LeaderEpoch, Offset},
    stateright::{Checker, Model, Property},
};

const MAX_STATES: usize = 2_000_000;
const MAX_DEPTH: usize = 64;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_TWO: usize = 35_274;
const PINNED_UNIQUE_STATES_THREE: usize = 20_212;
const PINNED_UNIQUE_STATES_NO_ASSIGN: usize = 10_865;

/// One replica: its log, one leader epoch per offset, and its checkpoint.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Replica {
    pub(super) log: Vec<LeaderEpoch>,
    pub(super) epochs: Vec<EpochEntry>,
}

impl Replica {
    pub(super) fn log_end(&self) -> Offset {
        Offset(i64::try_from(self.log.len()).expect("model log length fits in i64"))
    }

    /// Kafka's `LeaderEpochFileCache.assign`: a no-op for the latest epoch at
    /// or after its recorded start; otherwise drop every trailing entry that
    /// the new one does not strictly follow (`maybeTruncateNonMonotonicEntries`)
    /// and record it.
    pub(super) fn assign(&mut self, epoch: LeaderEpoch, start_offset: Offset) {
        if self
            .epochs
            .last()
            .is_some_and(|latest| latest.epoch == epoch && latest.start_offset <= start_offset)
        {
            return;
        }
        while self
            .epochs
            .last()
            .is_some_and(|last| last.epoch >= epoch || last.start_offset >= start_offset)
        {
            self.epochs.pop();
        }
        self.epochs.push(EpochEntry {
            epoch,
            start_offset,
        });
    }

    /// `UnifiedLog.append`: one record, with its batch epoch assigned.
    pub(super) fn append(&mut self, epoch: LeaderEpoch) {
        let offset = self.log_end();
        self.log.push(epoch);
        self.assign(epoch, offset);
    }

    /// `UnifiedLog.truncateTo`: the log, and the checkpoint through the
    /// production `truncate_to` (`LeaderEpochFileCache.truncateFromEnd`).
    pub(super) fn truncate(&mut self, offset: Offset) {
        self.log
            .truncate(usize::try_from(offset.0).expect("truncation offset is non-negative"));
        truncate_to(&mut self.epochs, offset);
    }

    /// `UnifiedLog.endOffsetForEpoch` over the production lookup.
    fn end_offset_for(&self, epoch: LeaderEpoch) -> (LeaderEpoch, Offset) {
        epoch_and_offset_for_entries(&self.epochs, epoch, self.log_end())
    }
}

/// The leader's answer to one follower fetch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum FetchAnswer {
    OutOfRange,
    Diverging {
        epoch: LeaderEpoch,
        end_offset: Offset,
    },
    Records,
}

/// Kafka's `Partition.readRecords` divergence gate for a fetch at
/// `fetch_offset` that carries `last_fetched_epoch`. The model's log start is
/// always 0, so the below-log-start check never fires.
pub(super) fn leader_answer(
    leader: &Replica,
    fetch_offset: Offset,
    last_fetched_epoch: Option<LeaderEpoch>,
) -> FetchAnswer {
    if let Some(last_fetched_epoch) = last_fetched_epoch {
        let (epoch, end_offset) = leader.end_offset_for(last_fetched_epoch);
        if end_offset == Offset(-1) || epoch == LeaderEpoch::UNKNOWN {
            return FetchAnswer::OutOfRange;
        }
        if epoch < last_fetched_epoch || end_offset < fetch_offset {
            return FetchAnswer::Diverging { epoch, end_offset };
        }
    }
    if fetch_offset > leader.log_end() {
        return FetchAnswer::OutOfRange;
    }
    FetchAnswer::Records
}

/// A follower's truncation for one `diverging_epoch`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Truncation {
    pub(super) offset: Offset,
    /// `false` for a KIP-279 step-back: the follower does not know the
    /// leader's epoch and asks again from a lower one.
    pub(super) complete: bool,
}

/// Kafka's `AbstractFetcherThread.getOffsetTruncationState` for a
/// `diverging_epoch`. The leader never sends an undefined one (it answers
/// `OFFSET_OUT_OF_RANGE` instead), so those two branches are not reached.
pub(super) fn follower_truncation(
    follower: &Replica,
    leader_epoch: LeaderEpoch,
    leader_end_offset: Offset,
) -> Truncation {
    let log_end = follower.log_end();
    let (follower_epoch, follower_end) = follower.end_offset_for(leader_epoch);
    if follower_end == Offset(-1) {
        return Truncation {
            offset: leader_end_offset.min(log_end),
            complete: true,
        };
    }
    if follower_epoch == leader_epoch {
        Truncation {
            offset: follower_end.min(leader_end_offset).min(log_end),
            complete: true,
        }
    } else {
        Truncation {
            offset: follower_end.min(log_end),
            complete: false,
        }
    }
}

/// The number of leading records two logs share.
fn common_prefix(a: &[LeaderEpoch], b: &[LeaderEpoch]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn is_strictly_increasing(entries: &[EpochEntry]) -> bool {
    entries
        .windows(2)
        .all(|w| w[0].epoch < w[1].epoch && w[0].start_offset < w[1].start_offset)
}

/// Safety ghosts: each is set once, when its property is broken, and never
/// cleared.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Violations {
    /// A truncation dropped a record the follower shared with the leader.
    pub(super) over_truncated: bool,
    /// A `diverging_epoch` left the follower unchanged.
    pub(super) stalled: bool,
    /// The leader answered `OFFSET_OUT_OF_RANGE`.
    pub(super) out_of_range: bool,
}

/// Non-vacuity witnesses: each is set once, when its path is taken, and
/// never cleared.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub(super) struct Witnesses {
    pub(super) divergent_truncation: bool,
    pub(super) gap: bool,
    pub(super) step_back: bool,
}

/// The cluster: every replica, the current leader and epoch, and the ghost
/// facts the properties read.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Cluster {
    pub(super) epoch: LeaderEpoch,
    pub(super) leader: usize,
    pub(super) replicas: Vec<Replica>,
    /// The leader accepted this follower's position since the last election
    /// or truncation.
    pub(super) reconciled: Vec<bool>,
    pub(super) violations: Violations,
    pub(super) witnesses: Witnesses,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Action {
    Elect(usize),
    Write,
    Fetch(usize),
}

impl Cluster {
    pub(super) fn new(replicas: usize, assign_on_election: bool) -> Self {
        let mut cluster = Self {
            epoch: LeaderEpoch(0),
            leader: 0,
            replicas: vec![Replica::default(); replicas],
            reconciled: vec![false; replicas],
            violations: Violations::default(),
            witnesses: Witnesses::default(),
        };
        if assign_on_election {
            cluster.replicas[0].assign(LeaderEpoch(0), Offset(0));
        }
        cluster
    }

    /// Apply `action`. Returns `false` when it changed nothing.
    pub(super) fn step(&mut self, action: Action, assign_on_election: bool) -> bool {
        match action {
            Action::Elect(r) => {
                self.epoch = LeaderEpoch(self.epoch.0 + 1);
                self.leader = r;
                self.reconciled.fill(false);
                if assign_on_election {
                    let log_end = self.replicas[r].log_end();
                    self.replicas[r].assign(self.epoch, log_end);
                }
                true
            }
            Action::Write => {
                let epoch = self.epoch;
                self.replicas[self.leader].append(epoch);
                true
            }
            Action::Fetch(f) => self.fetch(f),
        }
    }

    fn fetch(&mut self, f: usize) -> bool {
        let before = self.clone();
        let leader = self.replicas[self.leader].clone();
        let follower = &self.replicas[f];
        let fetch_offset = follower.log_end();
        let last_fetched_epoch = follower.epochs.last().map(|e| e.epoch);
        match leader_answer(&leader, fetch_offset, last_fetched_epoch) {
            FetchAnswer::Records => {
                self.reconciled[f] = true;
                let at = usize::try_from(fetch_offset.0).expect("fetch offset is non-negative");
                if let Some(&epoch) = leader.log.get(at) {
                    self.replicas[f].append(epoch);
                }
            }
            FetchAnswer::Diverging { epoch, end_offset } => {
                let requested = last_fetched_epoch.expect("only an epoch-carrying fetch diverges");
                let floor_and_higher = leader.epochs.iter().any(|e| e.epoch < requested)
                    && leader.epochs.iter().any(|e| e.epoch > requested);
                if floor_and_higher && leader.epochs.iter().all(|e| e.epoch != requested) {
                    self.witnesses.gap = true;
                }
                let truncation = follower_truncation(follower, epoch, end_offset);
                let agreed = common_prefix(&follower.log, &leader.log);
                let keep = usize::try_from(truncation.offset.0).expect("truncation is >= 0");
                if keep < agreed {
                    self.violations.over_truncated = true;
                }
                if keep < follower.log.len() {
                    self.witnesses.divergent_truncation = true;
                }
                if !truncation.complete {
                    self.witnesses.step_back = true;
                }
                let unchanged = self.replicas[f].clone();
                self.replicas[f].truncate(truncation.offset);
                if self.replicas[f] == unchanged {
                    self.violations.stalled = true;
                }
                self.reconciled[f] = false;
            }
            FetchAnswer::OutOfRange => {
                // `AbstractFetcherThread.fetchOffsetAndTruncate`: truncate to
                // the leader's log end when the follower is past it.
                self.violations.out_of_range = true;
                if leader.log_end() < fetch_offset {
                    self.replicas[f].truncate(leader.log_end());
                }
                self.reconciled[f] = false;
            }
        }
        *self != before
    }

    pub(super) fn follower_prefix_holds(&self) -> bool {
        let leader = &self.replicas[self.leader].log;
        self.replicas.iter().enumerate().all(|(r, replica)| {
            r == self.leader || !self.reconciled[r] || leader.starts_with(&replica.log)
        })
    }

    pub(super) fn checkpoints_hold(&self) -> bool {
        self.replicas
            .iter()
            .all(|replica| is_strictly_increasing(&replica.epochs))
    }

    fn converged(&self) -> bool {
        let leader = &self.replicas[self.leader].log;
        !leader.is_empty()
            && self
                .reconciled
                .iter()
                .enumerate()
                .all(|(r, &ok)| r == self.leader || ok)
            && self.replicas.iter().all(|replica| &replica.log == leader)
    }
}

struct ReconcileModel {
    replicas: usize,
    max_epoch: i32,
    max_log: usize,
    assign_on_election: bool,
}

impl Model for ReconcileModel {
    type Action = Action;
    type State = Cluster;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Cluster::new(self.replicas, self.assign_on_election)]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.epoch.0 < self.max_epoch {
            actions.extend((0..self.replicas).map(Action::Elect));
        }
        if s.replicas[s.leader].log.len() < self.max_log {
            actions.push(Action::Write);
        }
        actions.extend(
            (0..self.replicas)
                .filter(|&r| r != s.leader)
                .map(Action::Fetch),
        );
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = last.clone();
        next.step(action, self.assign_on_election).then_some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("reconciled_follower_is_leader_prefix", |_, s: &Cluster| {
                s.follower_prefix_holds()
            }),
            Property::always("no_agreed_record_truncated", |_, s: &Cluster| {
                !s.violations.over_truncated
            }),
            Property::always("divergence_makes_progress", |_, s: &Cluster| {
                !s.violations.stalled
            }),
            Property::always("leader_places_every_follower_epoch", |_, s: &Cluster| {
                !s.violations.out_of_range
            }),
            Property::always("checkpoints_strictly_increasing", |_, s: &Cluster| {
                s.checkpoints_hold()
            }),
            Property::sometimes("divergent_suffix_truncated", |_, s: &Cluster| {
                s.witnesses.divergent_truncation
            }),
            Property::sometimes("gap_epoch_resolved_to_floor", |_, s: &Cluster| {
                s.witnesses.gap
            }),
            Property::sometimes("step_back_truncation", |_, s: &Cluster| {
                s.witnesses.step_back
            }),
            Property::sometimes("converged_after_divergence", |_, s: &Cluster| {
                s.witnesses.divergent_truncation && s.converged()
            }),
        ]
    }
}

fn run(
    model: ReconcileModel,
    label: &str,
    pinned_unique_states: usize,
) -> impl Checker<ReconcileModel> {
    let checker = run_bfs(model, label, MAX_DEPTH, MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker
}

#[test]
fn reconciliation_two_replicas() {
    run(
        ReconcileModel {
            replicas: 2,
            max_epoch: 5,
            max_log: 3,
            assign_on_election: true,
        },
        "reconciliation_two_replicas",
        PINNED_UNIQUE_STATES_TWO,
    )
    .assert_properties();
}

#[test]
fn reconciliation_three_replicas() {
    run(
        ReconcileModel {
            replicas: 3,
            max_epoch: 3,
            max_log: 2,
            assign_on_election: true,
        },
        "reconciliation_three_replicas",
        PINNED_UNIQUE_STATES_THREE,
    )
    .assert_properties();
}

/// Without Kafka's assign-at-election a new leader that has not written yet
/// cannot place a follower's newer epoch, and answers `OFFSET_OUT_OF_RANGE`.
/// The safety properties still hold: the lookup never licenses a wrong
/// truncation, it only stops answering.
#[test]
fn without_assign_on_election_the_leader_cannot_place_newer_epochs() {
    let checker = run(
        ReconcileModel {
            replicas: 2,
            max_epoch: 4,
            max_log: 3,
            assign_on_election: false,
        },
        "without_assign_on_election",
        PINNED_UNIQUE_STATES_NO_ASSIGN,
    );
    checker.assert_any_discovery("leader_places_every_follower_epoch");
    for property in [
        "reconciled_follower_is_leader_prefix",
        "no_agreed_record_truncated",
        "checkpoints_strictly_increasing",
    ] {
        checker.assert_no_discovery(property);
    }
}
