#[cfg(creusot)]
use super::logic;
use {
    super::{
        FreezeIdentityState, FreezeMutationDecision, FreezeMutationKind, FreezeScopeDecision,
        FreezeScopeRank, FreezeSignatureDecision, FreezeSignatureFacts,
    },
    creusot_std::prelude::*,
};

/// Mathematical skew-window predicate, without machine-integer overflow.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn freeze_timestamp_in_window_model(set_at_ms: i64, now_ms: i64, max_skew_ms: i64) -> bool {
    pearlite! {
        max_skew_ms@ >= 0
            && set_at_ms@ >= now_ms@ - max_skew_ms@
            && set_at_ms@ <= now_ms@ + max_skew_ms@
    }
}

/// Check the symmetric skew window with saturating machine bounds.
#[ensures(result == freeze_timestamp_in_window_model(set_at_ms, now_ms, max_skew_ms))]
#[must_use]
pub const fn freeze_timestamp_in_window(set_at_ms: i64, now_ms: i64, max_skew_ms: i64) -> bool {
    if max_skew_ms < 0 {
        return false;
    }
    let earliest = now_ms.saturating_sub(max_skew_ms);
    let latest = now_ms.saturating_add(max_skew_ms);
    set_at_ms >= earliest && set_at_ms <= latest
}

open_logic! {
fn freeze_signature_preconditions(facts: FreezeSignatureFacts) -> bool {
    pearlite! { facts.identity == FreezeIdentityState::Bound
    && freeze_timestamp_in_window_model(
        facts.set_at_ms,
        facts.now_ms,
        facts.max_skew_ms,
    )
    && (!facts.replaces || facts.set_at_ms@ > facts.replaced_set_at_ms@) }
}
}

/// Apply the six signature rules in their security-sensitive order.
#[ensures(match result {
    FreezeSignatureDecision::UnknownKey => facts.identity == FreezeIdentityState::UnknownKey,
    FreezeSignatureDecision::AuthorIsNotKeyPrincipal => {
        facts.identity == FreezeIdentityState::WrongKeyPrincipal
    }
    FreezeSignatureDecision::AuthorIsNotConnectionPrincipal => {
        facts.identity == FreezeIdentityState::WrongConnectionPrincipal
    }
    FreezeSignatureDecision::TimestampOutsideSkewWindow => {
        facts.identity == FreezeIdentityState::Bound
            && !freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
    }
    FreezeSignatureDecision::TimestampNotNewer => {
        facts.identity == FreezeIdentityState::Bound
            && freeze_timestamp_in_window_model(
                facts.set_at_ms,
                facts.now_ms,
                facts.max_skew_ms,
            )
            && facts.replaces
            && facts.set_at_ms@ <= facts.replaced_set_at_ms@
    }
    FreezeSignatureDecision::SignatureInvalid => {
        freeze_signature_preconditions(facts)
            && !facts.signature_valid
    }
    FreezeSignatureDecision::Admit => {
        freeze_signature_preconditions(facts)
            && facts.signature_valid
    }
})]
#[must_use]
pub fn freeze_signature_decision(facts: FreezeSignatureFacts) -> FreezeSignatureDecision {
    match facts.identity {
        FreezeIdentityState::UnknownKey => FreezeSignatureDecision::UnknownKey,
        FreezeIdentityState::WrongKeyPrincipal => FreezeSignatureDecision::AuthorIsNotKeyPrincipal,
        FreezeIdentityState::WrongConnectionPrincipal => {
            FreezeSignatureDecision::AuthorIsNotConnectionPrincipal
        }
        FreezeIdentityState::Bound => {
            if !freeze_timestamp_in_window(facts.set_at_ms, facts.now_ms, facts.max_skew_ms) {
                FreezeSignatureDecision::TimestampOutsideSkewWindow
            } else if facts.replaces && facts.set_at_ms <= facts.replaced_set_at_ms {
                FreezeSignatureDecision::TimestampNotNewer
            } else if !facts.signature_valid {
                FreezeSignatureDecision::SignatureInvalid
            } else {
                FreezeSignatureDecision::Admit
            }
        }
    }
}

/// Make every caller use literal-over-prefix and longest-prefix precedence.
#[ensures((result == FreezeScopeDecision::Replace) == match (current, candidate) {
    (_, FreezeScopeRank::NoMatch) | (FreezeScopeRank::Literal, _) => false,
    (FreezeScopeRank::NoMatch, _) => true,
    (FreezeScopeRank::Prefix { .. }, FreezeScopeRank::Literal) => true,
    (
        FreezeScopeRank::Prefix { length: current_length },
        FreezeScopeRank::Prefix { length: candidate_length },
    ) => candidate_length@ > current_length@,
})]
#[must_use]
pub fn freeze_scope_decision(
    current: FreezeScopeRank,
    candidate: FreezeScopeRank,
) -> FreezeScopeDecision {
    match (current, candidate) {
        (_, FreezeScopeRank::NoMatch) | (FreezeScopeRank::Literal, _) => FreezeScopeDecision::Keep,
        (FreezeScopeRank::NoMatch, _)
        | (FreezeScopeRank::Prefix { .. }, FreezeScopeRank::Literal) => {
            FreezeScopeDecision::Replace
        }
        (
            FreezeScopeRank::Prefix {
                length: current_length,
            },
            FreezeScopeRank::Prefix {
                length: candidate_length,
            },
        ) => {
            if candidate_length > current_length {
                FreezeScopeDecision::Replace
            } else {
                FreezeScopeDecision::Keep
            }
        }
    }
}

// One exhaustive mutation policy supplies both the executable and logical classifiers.
macro_rules! freeze_refusal {
    ($kind:ident; {$($head:tt)*}) => {
        $($head)* {
            match $kind {
            FreezeMutationKind::Produce
            | FreezeMutationKind::TransactionEnlistment
            | FreezeMutationKind::DeleteRecords
            | FreezeMutationKind::DeleteTopic
            | FreezeMutationKind::ReassignmentAlter
            | FreezeMutationKind::Compaction
            | FreezeMutationKind::Retention => true,
            FreezeMutationKind::TransactionCompletion
            | FreezeMutationKind::ReassignmentCompletion
            | FreezeMutationKind::OffsetCommit
            | FreezeMutationKind::Replication
            | FreezeMutationKind::BarrierMarker
            | FreezeMutationKind::TieringCopy => false,
            }
        }
    };
}

freeze_refusal! { kind; {
/// Classify the complete topic-mutation inventory under a live freeze.
#[ensures(result == freeze_refusal_model(kind))]
#[must_use]
pub const fn freeze_refuses(kind: FreezeMutationKind) -> bool
} }

freeze_refusal! { kind; {
/// Logical mirror of [`freeze_refuses`] for the mutation contract.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn freeze_refusal_model(kind: FreezeMutationKind) -> bool
} }

/// Rank authorization ahead of freeze detail, then apply the one refusal
/// classification shared by every mutation adapter.
#[ensures((result == FreezeMutationDecision::AuthorizationDenied) == !authorized)]
#[ensures((result == FreezeMutationDecision::Frozen) == (
    authorized && frozen && freeze_refusal_model(kind)
))]
#[ensures((result == FreezeMutationDecision::Admit) == (
    authorized && (!frozen || !freeze_refusal_model(kind))
))]
#[must_use]
pub const fn freeze_mutation_decision(
    authorized: bool,
    frozen: bool,
    kind: FreezeMutationKind,
) -> FreezeMutationDecision {
    if !authorized {
        FreezeMutationDecision::AuthorizationDenied
    } else if frozen && freeze_refuses(kind) {
        FreezeMutationDecision::Frozen
    } else {
        FreezeMutationDecision::Admit
    }
}
