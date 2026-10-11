//! The checker bounds and the one entry point every model test calls.
//!
//! State explosion is the central risk of this composition, so the bounds sit
//! next to the assertions that prove a run was exhaustive: a search that hit
//! the depth cap, the generated-state cap or the unique-state bound proves
//! nothing, and the two must be tuned together.

use stateright::Checker;

use super::model::DpModel;
use crate::model_check::{assert_pinned_count, run_bfs};

const TARGET_STATE_COUNT: usize = 60_000_000;
const MAX_UNIQUE_STATES: usize = 8_000_000;
const MAX_DEPTH: usize = 70;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
// CLEAN moved from 521,626, UNCLEAN from 1,255,681 and ELR from 2,795,492
// when the leader's high watermark took Kafka's rule and the model's ISR
// expansion took the real `isr_candidate_selected` kernel. The watermark now
// never falls within a leadership and a new leader keeps the one it inherited
// (it used to be recomputed from scratch and could drop back), which splits
// states the old model merged, so CLEAN and UNCLEAN grew. It also stands still
// while the ISR is under `min.insync.replicas`, and a follower rejoins only
// once its log reaches the start of the leader's epoch, which removes the
// under-min-ISR commits and the early re-admissions ELR used to enumerate, so
// ELR shrank.
pub(super) const PINNED_UNIQUE_STATES_CLEAN: usize = 762_237;
pub(super) const PINNED_UNIQUE_STATES_UNCLEAN: usize = 1_471_603;
pub(super) const PINNED_UNIQUE_STATES_ELR: usize = 898_023;
pub(super) const PINNED_UNIQUE_STATES_DISKLESS: usize = 450;

pub(super) fn run(model: DpModel, label: &str, pinned_unique_states: usize) {
    let checker = run_bfs(model, label, MAX_DEPTH, TARGET_STATE_COUNT);
    assert2::assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique bound exceeded ({})",
        checker.unique_state_count()
    );
    assert_pinned_count(checker.unique_state_count(), pinned_unique_states, label);
    checker.assert_properties();
}
