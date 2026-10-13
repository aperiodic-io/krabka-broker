#[cfg(creusot)]
use super::logic;
use {
    super::{FreezeReplacementDecision, FreezeReplacementFacts, FreezeStoredState},
    creusot_std::prelude::*,
};

/// The replacement rule. A thaw of a scope with no committed freeze is
/// `Missing`, and a record no newer than the committed one at its exact key is
/// `Stale`; both are permanent refusals the committed image decides alone. An
/// admissible record behind an uncommitted metadata tail is `InFlight`, the
/// one refusal that clears once the tail commits, and otherwise it is
/// `Append`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn freeze_replacement_model(facts: FreezeReplacementFacts) -> FreezeReplacementDecision {
    pearlite! {
        let admissible = match facts.stored {
            FreezeStoredState::Missing => facts.incoming_frozen,
            FreezeStoredState::Present { set_at_ms } => facts.incoming_set_at_ms@ > set_at_ms@,
        };
        if !admissible {
            match facts.stored {
                FreezeStoredState::Missing => FreezeReplacementDecision::Missing,
                FreezeStoredState::Present { .. } => FreezeReplacementDecision::Stale,
            }
        } else if facts.uncommitted_tail {
            FreezeReplacementDecision::InFlight
        } else {
            FreezeReplacementDecision::Append
        }
    }
}

/// Admit only a new freeze or a strictly newer exact-key replacement, with no
/// uncommitted metadata tail; see `freeze_replacement_model`. The
/// controller retries only `InFlight`, so every variant is pinned.
#[ensures(result == freeze_replacement_model(facts))]
#[must_use]
pub fn freeze_replacement_decision(facts: FreezeReplacementFacts) -> FreezeReplacementDecision {
    match facts.stored {
        FreezeStoredState::Missing if !facts.incoming_frozen => FreezeReplacementDecision::Missing,
        FreezeStoredState::Present { set_at_ms } if facts.incoming_set_at_ms <= set_at_ms => {
            FreezeReplacementDecision::Stale
        }
        FreezeStoredState::Missing | FreezeStoredState::Present { .. } => {
            if facts.uncommitted_tail {
                FreezeReplacementDecision::InFlight
            } else {
                FreezeReplacementDecision::Append
            }
        }
    }
}
