//! The checker bounds and the one entry point every model test calls, together
//! with the two exhaustive runs.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use {
    super::state::{Cleaner, CompactModel},
    crate::model_check::run_bfs,
    stateright::Checker,
};

// The `Compact` action converges many append/tick paths onto shared logs, so the
// BFS's *generated* count (`state_count()`) runs ~2-2.5x the *unique* count. We
// therefore bound exhaustiveness on the two metrics that actually matter:
//
//   * `TARGET_STATE_COUNT` — the stateright truncation target. Set high so the
//     BFS runs to *completion* on the configs below; `state_count() < TARGET`
//     after `.join()` then certifies the run was exhaustive (it stopped because
//     the frontier emptied, not because it hit the target). The 3 GB host memory
//     watchdog (see `[[feedback_bound_model_checkers]]`) is the other runaway
//     guard — never run this unguarded.
//   * `MAX_UNIQUE_STATES` — the memory-proportional bound (resident memory ∝
//     distinct states). At the bounds below the unique space is ~144k (basic) /
//     ~1.10M (wide), generated ~0.7M / ~5.5M, and resident memory stays well
//     under 1 GB.
const TARGET_STATE_COUNT: usize = 12_000_000;
const MAX_UNIQUE_STATES: usize = 1_500_000;
const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// The state carries `last_pass`, the log the most recent `Compact` consumed, so
// that the pass rules are `always` properties with counterexample paths. Every
// `(log, clock)` a `Compact` leaves behind is therefore reached once per
// distinct input, which roughly doubled the counts (66,831 and 459,869 before).
const PINNED_UNIQUE_STATES_BASIC: usize = 144_128;
const PINNED_UNIQUE_STATES_WIDE: usize = 1_102_770;

fn run(model: CompactModel, label: &str, pinned_unique_states: usize) {
    // Exhaustiveness: `run_bfs` fails unless the BFS stopped because the
    // frontier emptied, not because it hit the truncation target.
    let checker = run_bfs(model, label, MAX_DEPTH, TARGET_STATE_COUNT);
    // Memory-proportional bound (resident memory ∝ distinct states).
    assert2::assert!(checker.unique_state_count() < MAX_UNIQUE_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn compaction_basic() {
    run(
        CompactModel {
            max_len: 4,
            max_clock: 4,
            cleaner: Cleaner::PRODUCTION,
        },
        "compaction_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn compaction_wide() {
    run(
        CompactModel {
            max_len: 5,
            max_clock: 4,
            cleaner: Cleaner::PRODUCTION,
        },
        "compaction_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
