//! Exhaustive stateright enumeration of the fetch read-path visibility decision
//! (`super::compute_visibility_window`).
//!
//! The state is the advancing partition watermarks
//! `{log_start, hw, lso, deliverable, log_end}`, with the Kafka invariant
//! `0 <= log_start <= lso <= hw <= log_end` and KFC-1's
//! `log_start <= deliverable <= hw`. `Advance*` actions raise them
//! monotonically: appends raise LEO, ISR catch-up raises HW, txn commits raise
//! LSO, retention raises `log_start`, and a batch coming due raises the
//! delivery watermark. `Fetch` probes drive the real decision.
//!
//! For each `Fetch` the model asserts the clamp contract. A consumer fetch
//! never exposes an offset beyond the high-watermark, so there is no dirty
//! read, and it never exposes one at or beyond the delivery watermark, so it
//! never delivers a record before it is due. The broker clamps a
//! `read_committed` consumer at `lso.min(hw)`, and it serves a follower up to
//! the log-end whatever the delivery watermark says, because replication is not
//! gated. The model also asserts the single-source-of-truth response-field
//! contract, the de-dup'd hazard from `do_read`: every fetcher, follower or
//! consumer, is told the partition's high watermark and `lso.min(hw)`, as
//! Kafka's `Partition.readRecords` reports them, so a KIP-392 follower never
//! adopts an offset the leader has not committed. For each `Advance*` it
//! asserts KIP-227 monotonicity: the reported HW/LSO never regress as the log
//! progresses, and the delivery watermark does not enter those two formulas at
//! all. See the design spec
//! `docs/superpowers/specs/2026-06-14-krabka-fetch-hwm-visibility-model-design.md`.

use {
    super::{FetchWatermarks, Offset, compute_visibility_window},
    assert2::assert,
    stateright::{Checker, Model, Property},
};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 182;
const PINNED_UNIQUE_STATES_WIDE: usize = 1_254;

struct VisModel {
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct VisState {
    log_start: i64,
    hw: i64,
    lso: i64,
    /// KFC-1's delivery watermark: the first offset that is not due yet.
    deliverable: i64,
    log_end: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum VisAction {
    AdvanceLogEnd,
    AdvanceHw,
    AdvanceLso,
    AdvanceLogStart,
    /// A scheduled batch reached its activation time.
    AdvanceDeliverable,
    /// `(is_follower, read_committed, fetch_offset)`. `read_committed` implies
    /// `!is_follower`.
    Fetch(bool, bool, i64),
}

/// Kafka's reported high watermark: the partition's own, whoever fetches.
/// Neither the fetch shape, the log end nor the delivery watermark is an input:
/// the broker reports the true high watermark on a scheduled topic too, and a
/// follower learns the committed bound rather than the leader's log end.
fn response_hw(s: &VisState) -> i64 { s.hw }

/// Kafka's reported last stable offset, `UnifiedLog.lastStableOffset`: the
/// first unstable offset capped at the high watermark, whoever fetches.
fn response_lso(s: &VisState) -> i64 { s.lso.min(s.hw) }

impl Model for VisModel {
    type Action = VisAction;
    type State = VisState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![VisState {
            log_start: 0,
            hw: 0,
            lso: 0,
            deliverable: 0,
            log_end: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Advance watermarks, preserving 0 <= log_start <= lso <= hw <= log_end
        // <= max_offset, and log_start <= deliverable <= hw.
        if s.log_end < self.max_offset {
            actions.push(VisAction::AdvanceLogEnd);
        }
        if s.hw < s.log_end {
            actions.push(VisAction::AdvanceHw);
        }
        if s.lso < s.hw {
            actions.push(VisAction::AdvanceLso);
        }
        if s.log_start < s.lso {
            actions.push(VisAction::AdvanceLogStart);
        }
        // The delivery watermark trails the HW independently of the LSO: a
        // batch comes due because time passed, not because a txn committed.
        if s.deliverable < s.hw {
            actions.push(VisAction::AdvanceDeliverable);
        }
        // Probe every fetch shape over a bounded fetch-offset window (incl. one
        // past log_end so the empty/out-of-range edges are exercised).
        for fo in 0..=(self.max_offset + 1) {
            actions.push(VisAction::Fetch(false, false, fo)); // consumer, read_uncommitted
            actions.push(VisAction::Fetch(false, true, fo)); // consumer, read_committed
            actions.push(VisAction::Fetch(true, false, fo)); // follower
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            VisAction::AdvanceLogEnd => {
                let mut s = last.clone();
                s.log_end += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceHw => {
                let mut s = last.clone();
                s.hw += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLso => {
                let mut s = last.clone();
                s.lso += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLogStart => {
                // log_start advancing never lowers response_hw/lso.
                let mut s = last.clone();
                s.log_start += 1;
                // `plan_read` clamps the delivery watermark into the range the
                // log still holds, so retention carries it along rather than
                // leaving it below the first offset that exists.
                s.deliverable = s.deliverable.max(s.log_start);
                Some(s)
            }
            VisAction::AdvanceDeliverable => {
                // A batch coming due widens what a consumer may read and moves
                // no reported watermark, so KIP-227 holds here too.
                let mut s = last.clone();
                s.deliverable += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::Fetch(is_follower, read_committed, fetch_offset) => {
                let w = compute_visibility_window(
                    is_follower,
                    read_committed,
                    FetchWatermarks {
                        log_start: Offset(last.log_start),
                        hw: Offset(last.hw),
                        lso: Offset(last.lso),
                        log_end: Offset(last.log_end),
                        deliverable: Offset(last.deliverable),
                    },
                    Offset(fetch_offset),
                );
                assert_fetch_contract(last, is_follower, read_committed, fetch_offset, &w);
                None // probes never change state
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("watermarks_ordered", |_, s: &VisState| {
                0 <= s.log_start
                    && s.log_start <= s.lso
                    && s.lso <= s.hw
                    && s.hw <= s.log_end
                    && s.log_start <= s.deliverable
                    && s.deliverable <= s.hw
            }),
            // A read_committed clamp strictly below HW is reachable (lso < hw).
            Property::sometimes("can_clamp_lso", |_, s: &VisState| s.lso < s.hw),
            // A delivery clamp strictly below HW is reachable, and it is the
            // one that binds: a scheduled record is committed but not due.
            Property::sometimes("can_clamp_deliverable", |_, s: &VisState| {
                s.deliverable < s.hw && s.deliverable < s.lso
            }),
            // The delivery watermark can also be the loosest of the three, so
            // the LSO clamp is still reachable under it.
            Property::sometimes("deliverable_above_lso", |_, s: &VisState| {
                s.lso < s.deliverable
            }),
            // A follower can be served strictly beyond HW (hw < log_end).
            Property::sometimes("follower_beyond_hw", |_, s: &VisState| s.hw < s.log_end),
            // OFFSET_OUT_OF_RANGE is reachable (log_start > 0 ⟹ a sub-log_start
            // fetch_offset exists).
            Property::sometimes("can_out_of_range", |_, s: &VisState| s.log_start > 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool { s.log_end <= self.max_offset }
}

/// KIP-227: a watermark advance must never lower the reported HW/LSO. The
/// delivery watermark is not an input to either formula, so an advance of it
/// must leave both exactly where they were.
fn assert_monotonic(old: &VisState, new: &VisState) {
    assert!(
        response_hw(new) >= response_hw(old),
        "response_hw regressed on advance"
    );
    assert!(
        response_lso(new) >= response_lso(old),
        "response_lso regressed on advance"
    );
}

fn assert_fetch_contract(
    s: &VisState,
    is_follower: bool,
    read_committed: bool,
    fetch_offset: i64,
    w: &super::VisibilityWindow,
) {
    // Unwrap the `Offset` window fields into this model's `i64` world.
    let limit_offset = w.limit_offset.0;
    let win_response_hw = w.response_hw.0;
    let win_response_lso = w.response_lso.0;
    let effective_lso = w.effective_lso.0;
    // Valid targets.
    assert!(limit_offset >= 0 && win_response_hw >= 0 && win_response_lso >= 0);
    // out_of_range / empty correctness.
    assert!(w.out_of_range == (fetch_offset < s.log_start || fetch_offset > s.log_end));
    let upper = if is_follower {
        s.log_end
    } else {
        s.deliverable
    };
    if !w.out_of_range {
        assert!(w.empty == (fetch_offset >= upper));
    }
    // Response single-source-of-truth contract (OOR and success paths share
    // it). Neither field moves with the fetch shape or the delivery watermark,
    // and neither ever names an offset beyond the high watermark.
    assert!(win_response_hw == response_hw(s));
    assert!(win_response_lso == response_lso(s));
    assert!(win_response_lso <= win_response_hw && win_response_hw <= s.hw);
    if is_follower {
        // Follower bound: serve up to the log-end (>= hw), ungated by the
        // delivery watermark, so a scheduled record replicates and counts
        // toward the ISR before any consumer can see it. The bytes run past
        // the HW; the reported HW does not.
        assert!(limit_offset == s.log_end && limit_offset >= s.hw);
    } else {
        // No dirty read: never expose beyond the high-watermark.
        assert!(limit_offset <= s.hw, "consumer fetch exposed beyond HW");
        // KFC-1: never expose a record before it is due.
        assert!(
            limit_offset <= s.deliverable,
            "consumer fetch exposed beyond the delivery watermark"
        );
        if read_committed {
            assert!(effective_lso == s.lso.min(s.hw));
            assert!(limit_offset <= s.lso.min(s.hw));
        }
    }
}

fn run(model: VisModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] state cap hit"
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn visibility_basic() {
    run(
        VisModel { max_offset: 4 },
        "visibility_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn visibility_wide() {
    run(
        VisModel { max_offset: 7 },
        "visibility_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
