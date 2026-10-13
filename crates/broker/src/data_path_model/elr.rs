//! The KIP-966 eligible-leader-replica state the search carries, and the seam
//! onto the real maintenance rule that computes it.
//!
//! # Why the model has to maintain it rather than stipulate it
//!
//! [`failover_one`](crate::leader_election::failover_one) elects a surviving
//! ELR member once the live ISR has emptied, ahead of a longer log and of both
//! the offset-aware recovery and the KIP-841 election, and reports that
//! election as clean; [`select_leader`](crate::unclean_recovery::select_leader)
//! makes the same ranking inside a recovery. That report is not an
//! observation, it is a claim about how
//! the set was built: an ELR member left the ISR while the partition still met
//! `min.insync.replicas`, and the leader's high watermark does not move while
//! the partition is below it, so every committed record was already on it. A
//! model that let the search pick the ELR out
//! of thin air would refute the claim on its first step, and would refute it
//! for a partition no controller could ever produce. So the model runs the
//! production rule instead: every transition that changes the leader or the
//! ISR calls the real
//! [`next_partition_elr`](crate::elr::maintain::next_partition_elr), the same
//! function [`ElrPublisher`](crate::elr::ElrPublisher) drives before it writes the
//! topic config, and the set the search carries is whatever that returns.
//!
//! # What the state carries, and what it leaves out
//!
//! [`DpState::elr`](super::state::DpState::elr) is the published
//! eligible-leader set as a broker bitmask. The published *last-known* ELR is
//! deliberately absent. It holds the last leader of a partition that has none,
//! and this model's failover applies nothing to such a partition (`Unavailable`
//! changes no state here), so the search never reaches the state in which an
//! election reads it. While a partition has a leader,
//! `next_partition_elr` derives the eligible set from `old_isr ∪ elr` alone,
//! so passing the last-known half back as empty yields the same eligible set
//! on every call, at a fraction of the reachable states. It is a
//! `DescribeTopicPartitions` field, and [`crate::elr::maintain`]'s own tests
//! are where it is checked.

use {
    super::{
        bounds::{NB, NB_U8, has, model_broker, node},
        state::DpState,
    },
    crate::test_support::elr_model_prelude::*,
};

/// The one topic the modelled cluster holds.
pub(super) const TOPIC: &str = "t";
/// The one partition of it.
pub(super) const PARTITION: i32 = 0;

/// The metadata image the real rules resolve `min.insync.replicas` against.
///
/// The model does not hard-code the threshold it then reasons about: it reads
/// it back out with [`min_insync_replicas`], the same
/// [`effective_min_insync_replicas`] the controller calls, and hands the
/// same number to the leader's high-watermark core, so the threshold the ELR
/// rule clears the set at and the one the watermark stops at cannot drift
/// apart.
pub(super) fn image(min_isr: usize) -> MetadataImage {
    elr_model_image(
        TOPIC,
        i16::try_from(NB).expect("the modelled cluster is tiny"),
        min_isr,
    )
}

/// The `min.insync.replicas` that `image` resolves for the modelled topic.
pub(super) fn min_insync_replicas(image: &MetadataImage) -> usize {
    effective_min_insync_replicas(image, TOPIC, NB)
}

/// The model state projected onto the partition record the controller rules
/// read: every broker is a replica, and `leader`/`isr` are the two halves a
/// change moves.
pub(super) fn partition_record(leader: u8, isr: u8, leader_epoch: u8) -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.to_string(),
        partition: PARTITION,
        leader: krabka_metadata::NodeId(node(leader)),
        replicas: (0..NB_U8)
            .map(|b| krabka_metadata::NodeId(node(b)))
            .collect(),
        isr: (0..NB_U8)
            .filter(|&b| has(isr, b))
            .map(|b| krabka_metadata::NodeId(node(b)))
            .collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(i32::from(leader_epoch)),
        ..Default::default()
    }
}

/// Recompute `s.elr` for the change from `previous` to the partition the state
/// now holds, by driving the real maintenance rule.
///
/// Call it from every transition that moves the leader or the ISR, which is
/// what [`ElrPublisher::extend`](crate::elr::ElrPublisher::extend) does for
/// every controller batch: a change that skipped it would leave the model
/// electing out of a set no controller would still be publishing.
pub(super) fn maintain(image: &MetadataImage, s: &mut DpState, previous: &PartitionRecord) {
    let next = partition_record(s.leader, s.isr, s.leader_epoch);
    let published = PartitionElr {
        eligible_leader_replicas: ids(s.elr),
        last_known_elr: Vec::new(),
    };
    let computed = next_partition_elr(
        image,
        Some(previous),
        &next,
        &published,
        // Nothing in this model restarts unclean: `Revive` is a broker coming
        // back with the log it had, which is the clean-shutdown case.
        &BTreeSet::new(),
    );
    s.elr = mask(&computed.eligible_leader_replicas);
}

/// The wire ids of a broker bitmask.
pub(super) fn ids(mask: u8) -> Vec<i32> {
    (0..NB_U8)
        .filter(|&b| has(mask, b))
        .map(i32::from)
        .collect()
}

/// The broker bitmask of a set of wire ids. Every id the rule can return came
/// out of a replica set this model built, so each one is a modelled broker.
fn mask(ids: &[i32]) -> u8 {
    ids.iter()
        .map(|&id| {
            1u8 << model_broker(u64::try_from(id).expect("a modelled broker id is non-negative"))
        })
        .fold(0u8, |m, bit| m | bit)
}
