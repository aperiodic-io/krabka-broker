//! The checker bounds and the one entry point that every model test calls.
//!
//! The bounds are here, next to the assertions that prove a run was
//! exhaustive, because a truncated search proves nothing and the two must move
//! together.

use super::config::StretchModel;
use crate::model_check::check_model;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// The two `failover_one` configs moved (2,226 -> 1,596 and 3,710 -> 2,660)
// when `failover_one` began removing only the fenced broker from the ISR and
// electing in assignment order, as Kafka's `handleBrokerFenced` does: it no
// longer drops a dead leader that is still awaiting failover, so the ISRs
// that path produced are gone. The `legacy_elect` config does not run it and
// kept its count.
pub(super) const PINNED_UNIQUE_STATES_THREE_SITES: usize = 1_596;
pub(super) const PINNED_UNIQUE_STATES_RED_LEGACY_ELECT: usize = 5_724;
pub(super) const PINNED_UNIQUE_STATES_RED_MIN_INSYNC_ONE: usize = 2_660;

pub fn run(model: StretchModel, label: &str, pinned_unique_states: usize) {
    check_model(
        model,
        label,
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        pinned_unique_states,
    );
}
