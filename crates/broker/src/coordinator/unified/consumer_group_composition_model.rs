//! COMPOSITIONAL model of consumer-group offset-commit FENCING through
//! rebalances. This is the third compositional model, after data-path #539 and
//! txn/EOS #541. It reuses the proven driving machinery of
//! `reconciler_model.rs` (#521): it rebuilds a real `GroupState` and drives the
//! REAL `step_heartbeat` / `reconcile_member` over join / leave / heartbeat /
//! faithful-client moves. It then composes that machinery with the REAL
//! `OffsetCommit` epoch-fence.
//!
//! The model verifies the genuine consumer-group offset-integrity guarantee.
//! Through any rebalance interleaving, (a) no two members ever own the same
//! partition, and (b) a member may commit ONLY with its CURRENT member epoch.
//! Point (a) is exclusivity: the real reconciliation's withholding, re-verified
//! in the composed context. Under point (b) the fence stops a zombie from
//! before a rebalance, whose epoch the reconciliation has since bumped. So a
//! stale or forward commit can never corrupt the committed offset.
//!
//! Scope, DRIVEN vs MODELED. The txn/EOS review found that the first draft
//! over-claimed, so this doc states the scope up front:
//!   - DRIVEN (real code): the KIP-848 reconciliation engine through
//!     `step_heartbeat`, AND the real `OffsetCommit` fence
//!     `GroupState::validate_offset_commit`, which is extracted from the
//!     actor's `ValidateCommit`. The model drives the real fence and
//!     cross-checks it against an INDEPENDENT oracle, the expected epoch
//!     comparison and the error codes. A divergence is a real fence or
//!     regression bug.
//!   - NOT verified here (deliberately): NO-DUPLICATE / NO-GAP delivery. Plain
//!     consumer groups are AT-LEAST-ONCE, and a zombie reprocesses until the
//!     fence stops it. Exactly-once is the txn/EOS path (#541). The committed
//!     offset is a small bounded counter, present only to witness accepted
//!     commits. The `__consumer_offsets` log persistence (#539) and the classic
//!     protocol (`classic_state_model` #534) are out of scope.
//!
//! Memory safety: run under the host memory watchdog while you tune the bounds.
//!
//! # Module layout
//!
//! This file is the module root. It holds the topic and offset bounds that the
//! whole model shares, and the two tests that run the checker. Each child holds
//! one concern: `config` the bounded model shape and the metadata and
//! coordinator config the driven code needs, `state` the enumerated state and
//! actions, `projection` the two-way mapping onto the real `GroupState`, `commit` the
//! `OffsetCommit` fence oracle and the transition that drives the real fence,
//! `properties` the stateright [`Model`](stateright::Model) implementation, and
//! `runner` the checker bounds. `reconciliation_model_support` provides the
//! shared wire requests and ownership oracles.

// Each child is declared with an explicit `#[path]`, because this root is
// itself reached through a `#[path]` and so owns its declaring directory.
#[path = "consumer_group_composition_model/commit.rs"]
mod commit;
#[path = "consumer_group_composition_model/config.rs"]
mod config;
#[path = "consumer_group_composition_model/projection.rs"]
mod projection;
#[path = "consumer_group_composition_model/properties.rs"]
mod properties;
#[path = "consumer_group_composition_model/runner.rs"]
mod runner;
#[path = "consumer_group_composition_model/state.rs"]
mod state;

use {
    self::{
        config::CgcModel,
        runner::{PINNED_UNIQUE_STATES_BASIC, PINNED_UNIQUE_STATES_WIDE, run},
    },
    super::reconciliation_model_support::TOPIC,
    krabka_log::Offset,
};
const MAX_OFFSET: Offset = Offset(2); // bound the committed offset so the state space stays finite

#[test]
fn cg_basic() { run(CgcModel::basic(), "cg_basic", PINNED_UNIQUE_STATES_BASIC); }

#[test]
fn cg_wide() { run(CgcModel::wide(), "cg_wide", PINNED_UNIQUE_STATES_WIDE); }
