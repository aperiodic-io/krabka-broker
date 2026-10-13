//! The read-only view of the cluster that every partition in one `ElectLeaders`
//! request shares.
//!
//! The request resolves the metadata image, the liveness state and the witness
//! set once and lends them to each partition, so electing a hundred partitions
//! costs one image lookup each rather than a hundred rebuilds of the same
//! state.

use {
    crate::{broker::Broker, handlers::RequestContext, leader_election::ElectionType},
    krabka_metadata::{MetadataImage, NodeId},
    std::collections::HashSet,
};

/// Everything one partition's election reads, and nothing it writes.
///
/// The whole request resolves these once. A partition then costs one image
/// lookup and, on an unclean election, one walk of the proposal registry.
pub(super) struct ElectionEnv<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    pub(super) alive: &'a HashSet<u64>,
    pub(super) witnesses: &'a HashSet<NodeId>,
    pub(super) election: ElectionType,
}
