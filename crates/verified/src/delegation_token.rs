//! KIP-48 delegation-token deadline decisions.

use creusot_std::prelude::*;

/// Delegation-token API whose admission policy is being evaluated.
#[krabka_macros::model_type]
pub enum TokenApi {
    Create,
    Renew,
    Expire,
    Describe,
}

/// Whether a connection may invoke a delegation-token API.
#[krabka_macros::model_type]
pub enum TokenApiAdmission {
    Reject,
    Allow,
}

/// Credential source selected for the first SCRAM round.
#[krabka_macros::model_type]
pub enum ScramCredentialSource {
    Regular,
    DelegationToken,
    ExpiredDelegationToken,
    Unknown,
}

/// Absolute deadlines stored on a freshly created delegation token.
#[krabka_macros::model_type]
pub struct TokenDeadlines {
    pub max_timestamp_ms: i64,
    pub initial_expiry_ms: i64,
}

/// Whether the host configuration admits a create and, if so, its deadlines.
#[krabka_macros::model_type]
pub enum TokenCreateDecision {
    Invalid,
    Create(TokenDeadlines),
}

/// Whether a token can be renewed and, if so, its next expiry.
#[krabka_macros::model_type]
pub enum TokenRenewDecision {
    Invalid,
    Expired,
    Renew(i64),
}

/// Mutation selected by `ExpireDelegationToken`.
#[krabka_macros::model_type]
pub enum TokenExpireDecision {
    Expired,
    Delete,
    Update(i64),
}

/// Delegation-token mutation whose committed-state precondition is checked.
#[krabka_macros::model_type]
pub enum TokenMutationKind {
    Renew,
    Expire,
    Delete,
}

/// Relationship between the committed token and a guarded mutation.
#[krabka_macros::model_type]
pub enum TokenMutationState {
    Missing,
    Expected,
    Applied,
    Stale,
}

/// Controller action for a generation-bound delegation-token mutation.
#[krabka_macros::model_type]
pub enum TokenMutationDecision {
    Append,
    Retry,
    Reject,
}

/// Scalar facts projected by the controller before it mutates token state.
#[krabka_macros::model_type]
pub struct TokenMutationFacts {
    pub kind: TokenMutationKind,
    pub state: TokenMutationState,
    pub now_ms: i64,
    pub expected_expiry_ms: i64,
    pub incoming_expiry_ms: i64,
    pub max_timestamp_ms: i64,
    pub uncommitted_tail: bool,
}

mod token_mutation_decision;
pub use token_mutation_decision::{
    scram_credential_source, token_api_admission, token_describe_visible, token_mutation_decision,
};

mod token_is_active;
pub use token_is_active::{
    create_token_deadlines, expire_token_deadline, renew_token_expiry, token_is_active,
};

#[cfg(test)]
mod tests;
