//! The checker bounds and the one entry point that every model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use {
    super::config::ShareModel,
    crate::model_check::{assert_pinned_count, run_bfs},
};

/// Hard backstop on generated states. It bounds host memory even if
/// `within_boundary` is looser than intended. Set it well above each config's
/// true bounded count, so a real exhaustive run never truncates.
const MAX_STATES: usize = 200_000;
/// Depth backstop. It must exceed each config's reachable-graph diameter.
/// Otherwise the search is depth-truncated and incomplete, and the `run`
/// harness fails.
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
pub(super) const PINNED_UNIQUE_STATES_INFLIGHT_FULL: usize = 7_969;
pub(super) const PINNED_UNIQUE_STATES_RECORD_LOCK_LIMIT: usize = 1_521;
pub(super) const PINNED_UNIQUE_STATES_FAILOVER: usize = 623;
pub(super) const PINNED_UNIQUE_STATES_DEFERRAL: usize = 1_118;
pub(super) const PINNED_UNIQUE_STATES_DEFERRAL_WIDE: usize = 5_012;
pub(super) const PINNED_UNIQUE_STATES_LOG_START_ADVANCE: usize = 6_477;

krabka_macros::model_runner! {

    /// Run one bounded config to completion. Assert that the run was exhaustive,
    /// that is, that no cap truncated it, and that all properties hold.
    pub(super) fn run(model: ShareModel, label: &str, pinned_unique_states: usize)
    [bfs = run_bfs, pin = assert_pinned_count, depth = MAX_DEPTH, states = MAX_STATES, order = "properties_last", checker = checker]
    {

    }
}
