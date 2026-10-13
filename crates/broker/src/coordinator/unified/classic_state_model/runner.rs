//! The checker bounds and the one entry point that both model tests call.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use {
    super::config::ClassicModel,
    crate::{
        coordinator::unified::actor::reconciliation_model_support::pinned_model_runner,
        model_check::run_bfs,
    },
    stateright::Checker,
};

// Exhaustiveness is bounded on UNIQUE states (memory-proportional); the BFS's
// generated count runs several times the unique count here (high branching:
// every idle member has join/leave/heartbeat actions). `TARGET_STATE_COUNT` is
// the truncation ceiling set high so the BFS runs to completion (the 3 GB host
// watchdog is the other runaway guard — `[[feedback_bound_model_checkers]]`);
// `state_count() < TARGET` then certifies the run was exhaustive.
const TARGET_STATE_COUNT: usize = 8_000_000;
const MAX_UNIQUE_STATES: usize = 600_000; // wide ~483k unique; margin for determinism
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_BASIC: usize = 3_853;
pub(super) const PINNED_UNIQUE_STATES_WIDE: usize = 482_874;

pinned_model_runner! {
    @checked
    pub(super) fn run(ClassicModel);
    run_bfs, crate::model_check::assert_pinned_count;
    MAX_DEPTH, TARGET_STATE_COUNT; properties_last;
    |checker, label| {
        assert2::assert!(
            checker.unique_state_count() < MAX_UNIQUE_STATES,
            "[{label}] unique-state bound exceeded ({})",
            checker.unique_state_count()
        );
    }
}
