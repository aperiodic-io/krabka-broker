//! Per-group state for KIP-848 next-gen consumer groups. Exactly one
//! `actor::GroupActor` task owns this state. It is never shared.
//!
//! This file is the module root. The [`MemberState`] record lives in
//! `member`, the [`GroupState`] container and its membership transitions in
//! `group`, the regular-expression subscriptions and what they resolved to in
//! `regex`, and the target-assignment reconciliation in `reconcile`.
//! [`TargetAssignment`] stays here: it is a leaf record with no behaviour of
//! its own.

use {krabka_protocol::primitives::uuid::Uuid, std::collections::HashMap};

mod group;
mod member;
mod reconcile;
mod regex;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use self::reconcile::same_assignment;
pub use self::{
    group::GroupState,
    member::{ClassicMemberFacade, MemberState},
    regex::ResolvedRegularExpression,
};

#[derive(Debug, Clone, Default)]
pub struct TargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}
