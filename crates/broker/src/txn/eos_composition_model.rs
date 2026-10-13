//! COMPOSITIONAL model of the exactly-once READ visibility algebra.
//!
//! This is the second end-to-end model, after the data-path composition. It
//! runs over a single partition with >= 2 interleaving transactional producers
//! and an advancing HWM. It verifies that what a `read_committed` consumer may
//! see is EXACTLY the committed records below `min(lso, hw)`:
//!
//! - `only_committed_visible`: every visible batch belongs to a committed
//!   transaction, so no open and no aborted record is visible.
//! - `window_is_min_lso_hw`: the window the real `compute_visibility_window`
//!   returns equals `min(first unstable offset, hw)`, with the first unstable
//!   offset recomputed from each producer's control markers, not from `lso()`.
//! - `committed_prefix_complete`: every committed Data batch below that
//!   independent window is visible.
//! - `nothing_visible_above_hw`: no visible offset is at or above the HWM.
//! - `no_visible_aborted`: no visible offset lies in an aborted range as
//!   Kafka's aborted-transaction index derives it from the Abort markers,
//!   independently of the abort filter in `visible()`.
//! - `hw_within_log` and `no_transition_violation`: the HWM stays within the
//!   log, the HWM and the LSO never regress, and every `End` Proceeds.
//!
//! Concurrent producers' batches interleave at the offset level. So a
//! committed txn can sit partly above the LSO, behind an older open txn, or
//! above the HWM. The guarantee is prefix-correctness, not whole-txn snapshot
//! atomicity.
//!
//! Scope: what is DRIVEN and what is MODELED.
//!
//! - DRIVEN (real code): the EndTxn decision cores `decide_phase1_transition`,
//!   `prepare_completion_identities_with_fresh` and
//!   `decide_end_txn_completion` on their Proceed path. `decision_model.rs`
//!   exercises the fencing and retry arms; a ghost flag fails
//!   `no_transition_violation` if they ever fire here. Also DRIVEN: the real
//!   `read_committed` clamp of `compute_visibility_window`
//!   (`effective_lso = lso.min(hw)`). That clamp bites non-trivially when an
//!   open txn's records sit above the HWM. The witness is `hwm_clamp_active`.
//! - MODELED (faithful abstraction, NOT driving real code): the LSO rule
//!   `lso()` and the abort filter in `visible()`. The LSO rule is Kafka's
//!   first-unstable-offset; `Log::lso()`'s incremental maintenance is stored
//!   state, not a pure fn. The filter hides a Data batch if and only if its
//!   txn aborted, which matches the consumer's range filtering only under the
//!   one-in-flight-txn-per-producer invariant this model enforces. The
//!   properties check both against oracles derived by producer and marker
//!   rather than by generation tag.
//! - NOT covered, and left to the per-slice log / txn-index / fetch models:
//!   the `Log::lso()` maintenance internals, `TxnIndex::aborted_in_range`
//!   overlap arithmetic, and the consumer `aborted_pids` state machine.
//!
//! See the design spec.

use {
    super::{
        decision::{CompletionDecision, decide_end_txn_completion, decide_phase1_transition},
        state::{TxnEntry, TxnState},
        version::TxnVersion,
    },
    crate::{
        handlers::fetch::{FetchWatermarks, compute_visibility_window},
        model_check::{assert_pinned_count, run_bfs},
    },
    krabka_log::{Offset, ProducerId},
    stateright::{Checker, Model, Property},
};

const TARGET_STATE_COUNT: usize = 20_000_000;
const MAX_UNIQUE_STATES: usize = 2_000_000;
const MAX_DEPTH: usize = 50;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 1_228;
const PINNED_UNIQUE_STATES_WIDE: usize = 58_524;

const PID0: i64 = 1000; // base producer id; per-producer pid = PID0 + producer index

fn model_offset(value: usize) -> i64 {
    i64::try_from(value).expect("bounded model offset fits in i64")
}

fn model_index(value: i64) -> usize {
    usize::try_from(value).expect("model offsets are non-negative and bounded")
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Data,
    Commit,
    Abort,
}

/// One appended batch (offset = index in the log).
///
/// A transaction is a producer's run of `Data` batches in one `generation`. A
/// Commit or Abort marker ends the transaction.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Batch {
    producer: u8,
    generation: u8,
    kind: Kind,
}

/// Per-producer coordinator projection (drives the real `TxnEntry`, hashably).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Prod {
    state: i8, // TxnState::to_kafka_status()
    epoch: i16,
    generation: u8,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct EosState {
    log: Vec<Batch>,
    prod: Vec<Prod>, // index = producer
    /// High watermark = offsets replicated/durable so far (`hw <= log_end`).
    /// `Ack` advances it. An OPEN transaction's not-yet-replicated records can
    /// push the LSO ABOVE the HWM. The real `compute_visibility_window` clamp
    /// `effective_lso = lso.min(hw)` then bites and returns `hw`.
    hw: Offset,
    violations: Violations,
}

/// Ghost transition-violation flags. The transition that commits a violation
/// sets its flag, and `no_transition_violation` requires every flag to stay
/// `false`, so the checker reports the trace instead of panicking mid-search.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct Violations {
    /// The HWM moved backwards.
    hw_regressed: bool,
    /// The LSO moved backwards.
    lso_regressed: bool,
    /// An `End` did not Proceed. The no-window `End` path never has its epoch
    /// bumped underneath it, so only a changed decision core reaches this;
    /// `decision_model` exercises the fencing and retry arms.
    end_not_proceed: bool,
}

struct EosModel {
    producers: u8,
    max_gen: u8,
    max_data_per_txn: usize,
    max_log: usize,
}

fn tstate(id: i8) -> TxnState { TxnState::from_kafka_status(id).expect("valid TxnState id") }

/// Rebuild a real `TxnEntry` for producer `p` so the real decision cores behave
/// exactly as in a live run. Partitions and timestamps do not change the
/// decision.
fn rebuild(p: usize, pr: Prod) -> TxnEntry {
    // Per-producer pid = PID0 + index; wrap into `ProducerId` at the seam.
    let mut e = TxnEntry::new_empty(
        "tid".to_string(),
        ProducerId(PID0 + model_offset(p)),
        pr.epoch,
        60_000,
        1,
    );
    e.state = tstate(pr.state);
    e
}

// ----- derived txn structure (faithful LSO + aborted-list mechanics) -----

/// Outcome of txn (producer, generation) from the log: a marker resolves it.
fn txn_outcome(log: &[Batch], producer: u8, generation: u8) -> Option<Kind> {
    log.iter()
        .find(|b| {
            b.producer == producer
                && b.generation == generation
                && matches!(b.kind, Kind::Commit | Kind::Abort)
        })
        .map(|b| b.kind)
}

/// A transaction of the model: a producer in one of its generations.
#[derive(Clone, Copy, PartialEq, Eq)]
struct TxnKey {
    producer: u8,
    generation: u8,
}

/// LSO = base offset of the oldest still-OPEN txn (Data present, no marker yet),
/// else the log end. This is Kafka's first-unstable-offset rule. This function
/// derives the LSO from the log alone. The txn universe is whatever
/// (producer, generation) pairs appear, so the property closures stay
/// non-capturing.
fn lso(log: &[Batch]) -> Offset {
    let mut min_open: Option<i64> = None;
    let mut seen: Vec<TxnKey> = Vec::new();
    for (off, b) in log.iter().enumerate() {
        let key = TxnKey {
            producer: b.producer,
            generation: b.generation,
        };
        if b.kind == Kind::Data && !seen.contains(&key) {
            seen.push(key);
            // First occurrence of this (producer, generation) — its base offset.
            if txn_outcome(log, b.producer, b.generation).is_none() {
                min_open = Some(min_open.map_or(model_offset(off), |m| m.min(model_offset(off))));
            }
        }
    }
    Offset(min_open.unwrap_or(model_offset(log.len())))
}

/// The exclusive offset a `read_committed` consumer may see.
///
/// This function drives the REAL `compute_visibility_window` on its
/// read-committed branch, where `effective_lso = lso.min(hw)`. When an open
/// txn's records sit above the HWM, `lso > hw` and the clamp returns `hw`. The
/// consumer never reads above the watermark.
fn effective_lso(log: &[Batch], hw: Offset) -> Offset {
    let log_end = Offset(model_offset(log.len()));
    let l = lso(log);
    let vw = compute_visibility_window(
        false, // consumer, not follower
        true,  // read_committed
        FetchWatermarks {
            log_start: Offset(0),
            // The HW may sit below the log end: replication lag.
            hw,
            lso: l,
            log_end,
            // This model's topic delivers immediately.
            deliverable: hw,
        },
        Offset(0), // fetch_offset
    );
    vw.effective_lso // = lso.min(hw)
}

/// The `read_committed` visible set: `Data` batch offsets below `effective_lso`
/// whose txn did NOT abort.
fn visible(log: &[Batch], hw: Offset) -> Vec<i64> {
    let eff = effective_lso(log, hw);
    (0..model_offset(log.len()))
        .filter(|&off| off < eff)
        .filter(|&off| {
            let b = log[model_index(off)];
            b.kind == Kind::Data && txn_outcome(log, b.producer, b.generation) != Some(Kind::Abort)
        })
        .collect()
}

// ----- independent oracles (by producer and marker, never by generation) -----

/// Kafka's first unstable offset, derived without the generation tags `lso()`
/// reads: the smallest offset of a Data batch whose producer has written no
/// control marker after it, else the log end.
fn first_unstable_offset(log: &[Batch]) -> Offset {
    let open = log.iter().enumerate().position(|(off, b)| {
        b.kind == Kind::Data
            && !log[off + 1..]
                .iter()
                .any(|later| later.producer == b.producer && later.kind != Kind::Data)
    });
    Offset(model_offset(open.unwrap_or(log.len())))
}

/// The Data offsets a `read_committed` consumer drops as aborted, derived the
/// way Kafka's aborted-transaction index and consumer do: each Abort marker of
/// producer `p` at offset `m` aborts every Data batch of `p` after `p`'s
/// previous control marker and before `m`.
fn aborted_by_markers(log: &[Batch]) -> Vec<i64> {
    let mut aborted = Vec::new();
    for (m, marker) in log.iter().enumerate() {
        if marker.kind != Kind::Abort {
            continue;
        }
        for (off, b) in log[..m].iter().enumerate().rev() {
            if b.producer != marker.producer {
                continue;
            }
            if b.kind != Kind::Data {
                break;
            }
            aborted.push(model_offset(off));
        }
    }
    aborted
}

// ----- model -----

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    Begin(u8),     // producer p: -> Ongoing (new generation)
    Append(u8),    // producer p: append a Data batch to its open txn
    End(u8, bool), // producer p: commit? -> drive decision cores + append marker
    Ack,           // a follower replicates one more offset: hw += 1
}

impl Model for EosModel {
    type Action = Act;
    type State = EosState;

    krabka_macros::model_transition! { last, a, s; {
        match a {
            Act::Begin(p) => {
                let pr = &mut s.prod[usize::from(p)];
                if pr.state == TxnState::Ongoing.to_kafka_status()
                    || !tstate(pr.state).can_transition_to(TxnState::Ongoing)
                {
                    return None;
                }
                pr.generation += 1;
                pr.state = TxnState::Ongoing.to_kafka_status();
            }
            Act::Append(p) => {
                let g = s.prod[usize::from(p)].generation;
                s.log.push(Batch {
                    producer: p,
                    generation: g,
                    kind: Kind::Data,
                });
            }
            Act::End(p, commit) => {
                let pr = s.prod[usize::from(p)];
                let mut entry = rebuild(usize::from(p), pr);
                let Ok((prepare, complete)) = decide_phase1_transition(&mut entry, commit) else {
                    return None; // illegal transition
                };
                crate::txn::handlers::end_txn::prepare_completion_identities_with_fresh(
                    &mut entry,
                    TxnVersion::Verified,
                    None,
                )
                .expect("model epochs never reach the rotation boundary");
                let completion =
                    crate::txn::handlers::end_txn::completion_producer_identity(&entry);
                match decide_end_txn_completion(
                    &entry,
                    ProducerId(PID0 + i64::from(p)),
                    entry.producer_epoch,
                    completion.0,
                    completion.1,
                    prepare,
                    complete,
                ) {
                    CompletionDecision::Proceed {
                        next_state,
                        response_epoch,
                        ..
                    } => {
                        s.log.push(Batch {
                            producer: p,
                            generation: pr.generation,
                            kind: if commit { Kind::Commit } else { Kind::Abort },
                        });
                        let np = &mut s.prod[usize::from(p)];
                        np.state = next_state.to_kafka_status();
                        np.epoch = response_epoch; // TV_2 bumps the epoch on completion
                    }
                    CompletionDecision::AlreadyComplete { .. } | CompletionDecision::Reject(_) => {
                        s.violations.end_not_proceed = true;
                    }
                }
            }
            Act::Ack => {
                s.hw += 1; // a follower replicated one more offset
            }
        }
        // The HWM and the LSO never regress: offsets only grow, and the
        // oldest open transaction's base only advances.
        if s.hw < last.hw {
            s.violations.hw_regressed = true;
        }
        if lso(&s.log) < lso(&last.log) {
            s.violations.lso_regressed = true;
        }
        Some(s)
    }}

    fn init_states(&self) -> Vec<Self::State> {
        vec![EosState {
            log: vec![],
            prod: (0..self.producers)
                .map(|_| Prod {
                    state: TxnState::Empty.to_kafka_status(),
                    epoch: 0,
                    generation: 0,
                })
                .collect(),
            hw: Offset(0),
            violations: Violations::default(),
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        // A follower replicating: advance the HWM toward the log end.
        if s.hw < model_offset(s.log.len()) {
            acts.push(Act::Ack);
        }
        if s.log.len() >= self.max_log {
            return;
        }
        for p in 0..self.producers {
            let pr = s.prod[usize::from(p)];
            // `AddPartitionsToTxn` opens a NEW transaction only from a state
            // that is not already `Ongoing`; on an `Ongoing` one it extends
            // the open transaction, which `Append` covers.
            if pr.generation < self.max_gen
                && pr.state != TxnState::Ongoing.to_kafka_status()
                && tstate(pr.state).can_transition_to(TxnState::Ongoing)
            {
                acts.push(Act::Begin(p));
            }
            if pr.state == TxnState::Ongoing.to_kafka_status() {
                let n = s
                    .log
                    .iter()
                    .filter(|b| {
                        b.producer == p && b.generation == pr.generation && b.kind == Kind::Data
                    })
                    .count();
                if n < self.max_data_per_txn {
                    acts.push(Act::Append(p));
                }
                if n >= 1 {
                    acts.push(Act::End(p, true));
                    acts.push(Act::End(p, false));
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: every visible batch belongs to a COMMITTED txn — no
            // open/uncommitted and no aborted record is ever visible. Catches a
            // real `compute_visibility_window` returning effective_lso ABOVE
            // min(lso, hw) (which would expose open-txn or above-HWM data).
            Property::always("only_committed_visible", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| {
                    let b = s.log[model_index(off)];
                    txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // The read_committed window is exactly min(first unstable offset,
            // HWM), with the first unstable offset recomputed here from the
            // log's markers, not from `lso()`. Catches a window above OR
            // below min(lso, hw), and an `lso()` that drifts from Kafka's rule.
            Property::always("window_is_min_lso_hw", |_, s: &EosState| {
                effective_lso(&s.log, s.hw) == first_unstable_offset(&s.log).min(s.hw)
            }),
            // Every committed Data batch below min(first unstable offset, HWM)
            // is visible: no committed, durable, stable record is hidden.
            Property::always("committed_prefix_complete", |_, s: &EosState| {
                let v = visible(&s.log, s.hw);
                let window = first_unstable_offset(&s.log).min(s.hw);
                s.log.iter().enumerate().all(|(off, b)| {
                    !(b.kind == Kind::Data
                        && model_offset(off) < window
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit))
                        || v.contains(&model_offset(off))
                })
            }),
            // Nothing at or above the HWM is visible.
            Property::always("nothing_visible_above_hw", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| off < s.hw)
            }),
            // No visible offset lies in an aborted range as Kafka's consumer
            // derives it from the Abort markers alone (see
            // `aborted_by_markers`), independently of `visible()`'s filter.
            Property::always("no_visible_aborted", |_, s: &EosState| {
                let aborted = aborted_by_markers(&s.log);
                visible(&s.log, s.hw)
                    .into_iter()
                    .all(|off| !aborted.contains(&off))
            }),
            // The HWM stays within the log.
            Property::always("hw_within_log", |_, s: &EosState| {
                s.hw <= model_offset(s.log.len())
            }),
            // Every transition keeps the LSO and the HWM from regressing, and
            // every End drives the decision cores to Proceed.
            Property::always("no_transition_violation", |_, s: &EosState| {
                s.violations == Violations::default()
            }),
            // ----- non-vacuity witnesses -----
            Property::sometimes("committed_visible", |_, s: &EosState| {
                !visible(&s.log, s.hw).is_empty()
            }),
            Property::sometimes("aborted_filtered", |_, s: &EosState| {
                let eff = effective_lso(&s.log, s.hw);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (model_offset(off)) < eff
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Abort)
                })
            }),
            // The key EOS subtlety: a COMMITTED batch sits ABOVE the LSO, held
            // back by an older still-open transaction (out-of-order commit).
            Property::sometimes("interleaved_held_back", |_, s: &EosState| {
                let l = lso(&s.log);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (model_offset(off)) >= l
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // The visibility CORE actively clamps: the HWM holds the effective LSO
            // BELOW the LSO (an open txn's records sit above the not-yet-replicated
            // HWM). Proves `compute_visibility_window`'s `lso.min(hw)` is exercised
            // non-trivially, not as an identity pass-through.
            Property::sometimes("hwm_clamp_active", |_, s: &EosState| {
                effective_lso(&s.log, s.hw) < lso(&s.log)
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool { s.log.len() <= self.max_log }
}

krabka_macros::model_runner! {


    fn run(model: EosModel, label: &str, pinned_unique_states: usize)
    [bfs = run_bfs, pin = assert_pinned_count, depth = MAX_DEPTH, states = TARGET_STATE_COUNT, order = "properties_first", checker = checker]
    {
        assert2::assert!(
            checker.unique_state_count() < MAX_UNIQUE_STATES,
            "[{label}] unique bound exceeded ({})",
            checker.unique_state_count()
        );
    }
}

#[test]
fn txn_basic() {
    run(
        EosModel {
            producers: 2,
            max_gen: 1,
            max_data_per_txn: 2,
            max_log: 5,
        },
        "txn_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide() {
    // Deeper interleaving: a second transaction generation per producer + a
    // longer log, so a producer's committed txn can be held back by another
    // producer's later open txn across more offset orderings.
    run(
        EosModel {
            producers: 2,
            max_gen: 2,
            max_data_per_txn: 2,
            max_log: 7,
        },
        "txn_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
