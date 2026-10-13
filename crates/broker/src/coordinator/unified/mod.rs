//! Unified group-coordinator subsystem for KIP-848.
//!
//! The subsystem gives shared infrastructure and persistence to both the
//! classic and the next-gen group protocols.
//!
//! [`GroupCoordinator`] is the single owner of the next-gen consumer-group
//! machinery. It spawns per-group actors, tracks each group's locked type,
//! and replays persisted state during bootstrap.
use crate::codes;

pub mod actor;
pub mod assignor;
pub(crate) mod classic_ops;
pub(crate) mod classic_state;
pub mod config;
pub(crate) mod consumer_state;
pub(crate) mod group;
pub(crate) mod migration;
pub mod offsets_log;
pub(crate) mod persistence;
pub mod persistence_next_gen;
pub mod reconciler;
pub mod regex_resolver;
pub mod share;
pub mod streams;
pub(crate) mod topic_hash;

mod admin_ops;
mod group_coordinator;
mod image_metadata;
mod member_helpers;
mod member_records;
mod offset_batch;
mod registry;
mod replay_next_gen;
pub(crate) mod replay_policy;
mod replay_share;
mod replay_streams;
mod seed_cache;
mod seeds;
mod streams_conversion;
mod type_lock;

#[cfg(test)]
mod coordinator_replay_model;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod value_version_tests;

/// The group epoch and target assignment epoch of a new consumer, share or
/// streams group: Kafka 4.3.1's `ModernGroup` and `StreamsGroup` constructors
/// set the group epoch to 1, and `TargetAssignmentMetadata.INITIAL` holds
/// assignment epoch 1. The first epoch bump of a new group therefore writes
/// group epoch 2.
pub const INITIAL_GROUP_EPOCH: i32 = 1;

/// Map a coordinator actor's commit-validation reply to its protocol error.
pub(crate) fn commit_validation_reply<E>(reply: &Result<Result<(), i16>, E>) -> Option<i16> {
    match reply {
        Ok(Ok(())) => None,
        Ok(Err(code)) => Some(*code),
        Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
    }
}

pub use self::{
    group_coordinator::{GroupCoordinator, GroupType},
    image_metadata::ImageMetadataProvider,
    seeds::{GroupSeed, ShareGroupSeed, StreamsGroupSeed},
};
pub(crate) use self::{
    member_helpers::{
        ClientIdentity, can_compute_next_target_assignment, expired_member_ids,
        first_join_member_id, wall_clock_ms,
    },
    offset_batch::OffsetRecordBatchBuilder,
};
