//! Topic-freeze signature, scope, and replacement decisions.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};

/// How far a signed record passed through the principal-binding rules.
#[krabka_macros::model_type]
pub enum FreezeIdentityState {
    UnknownKey,
    WrongKeyPrincipal,
    WrongConnectionPrincipal,
    Bound,
}

/// Facts used by the ordered six-rule signature admission.
#[krabka_macros::model_type]
pub struct FreezeSignatureFacts {
    pub identity: FreezeIdentityState,
    pub set_at_ms: i64,
    pub now_ms: i64,
    pub max_skew_ms: i64,
    pub replaces: bool,
    pub replaced_set_at_ms: i64,
    pub signature_valid: bool,
}

/// The first failed rule, or admission after all six pass.
#[krabka_macros::model_type]
pub enum FreezeSignatureDecision {
    UnknownKey,
    AuthorIsNotKeyPrincipal,
    AuthorIsNotConnectionPrincipal,
    TimestampOutsideSkewWindow,
    TimestampNotNewer,
    SignatureInvalid,
    Admit,
}

/// The precedence of one scope that matches a topic.
#[krabka_macros::model_type]
pub enum FreezeScopeRank {
    NoMatch,
    Prefix { length: u64 },
    Literal,
}

/// Whether a matching scope replaces the current resolver candidate.
#[krabka_macros::model_type]
pub enum FreezeScopeDecision {
    Keep,
    Replace,
}

/// Every operation whose interaction with a topic freeze is deliberate.
///
/// Keeping the allowed operations in the same closed enum as the refused
/// ones makes additions visible to the proof and to its exhaustive tests.
#[krabka_macros::model_type]
pub enum FreezeMutationKind {
    Produce,
    TransactionEnlistment,
    DeleteRecords,
    DeleteTopic,
    ReassignmentAlter,
    ReassignmentCompletion,
    Compaction,
    Retention,
    TransactionCompletion,
    OffsetCommit,
    Replication,
    BarrierMarker,
    TieringCopy,
}

/// The single externally observable result of authorization plus freeze
/// admission.
#[krabka_macros::model_type]
pub enum FreezeMutationDecision {
    AuthorizationDenied,
    Frozen,
    Admit,
}

/// The committed entry at the exact incoming scope key.
#[krabka_macros::model_type]
pub enum FreezeStoredState {
    Missing,
    Present { set_at_ms: i64 },
}

/// Facts checked before a freeze mutation enters the metadata log.
#[krabka_macros::model_type]
pub struct FreezeReplacementFacts {
    pub stored: FreezeStoredState,
    pub incoming_frozen: bool,
    pub incoming_set_at_ms: i64,
    pub uncommitted_tail: bool,
}

/// Why a freeze mutation may or may not enter the metadata log.
#[krabka_macros::model_type]
pub enum FreezeReplacementDecision {
    Missing,
    Stale,
    InFlight,
    Append,
}

mod mutation_decision;
#[cfg(creusot)]
pub use mutation_decision::freeze_timestamp_in_window_model;
pub use mutation_decision::{
    freeze_mutation_decision, freeze_refuses, freeze_scope_decision, freeze_signature_decision,
    freeze_timestamp_in_window,
};

mod replacement_decision;
pub use replacement_decision::freeze_replacement_decision;

#[cfg(test)]
mod tests;
