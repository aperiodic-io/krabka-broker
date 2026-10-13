//! The checker bounds and the one entry point that both model tests call.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use {
    super::config::ReconModel,
    crate::model_check::{assert_pinned_count, run_bfs},
};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_BASIC: usize = 552;
pub(super) const PINNED_UNIQUE_STATES_WIDE: usize = 3_813;

krabka_macros::model_runner! {

    pub(super) fn run(model: ReconModel, label: &str, pinned_unique_states: usize)
    [bfs = run_bfs, pin = assert_pinned_count, depth = MAX_DEPTH, states = MAX_STATES, order = "properties_last", checker = checker]
    {}
}
