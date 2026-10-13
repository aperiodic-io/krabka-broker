//! The checker bounds and the one entry point that the model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use {super::model::ClientServerFailoverModel, crate::model_check::run_bfs, stateright::Checker};

const MAX_DEPTH: usize = 36;
const MAX_STATES: usize = 120_000;

// The exact unique-state count of the exhaustive BFS over this model.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES: usize = 38_131;

pub fn run_model() {
    let checker = run_bfs(
        ClientServerFailoverModel,
        "client_server_failover",
        MAX_DEPTH,
        MAX_STATES,
    );
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == PINNED_UNIQUE_STATES,
        "unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}
