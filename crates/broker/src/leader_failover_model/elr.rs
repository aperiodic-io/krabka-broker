//! The KIP-966 eligible-leader-replica seam of the failover-scan model: the
//! metadata image the real maintenance rule reads, and the call that
//! recomputes the published set after every change the model makes.
//!
//! The model does not choose the set. Every transition that moves the leader
//! or the ISR calls the real
//! [`next_partition_elr`](crate::elr::maintain::next_partition_elr), which is
//! what [`ElrPublisher`](crate::elr::ElrPublisher) runs over every controller
//! batch, so `failover_one` only ever reads a set a controller could have
//! published. Whether the members so published really hold every committed
//! record is a claim about logs, which this model does not carry;
//! `data_path_model`'s `data_elr` configuration checks it.

use {
    super::failover_state::{FailoverState, pr_of},
    crate::test_support::elr_model_prelude::*,
};

/// The one topic the modelled partition belongs to.
pub(super) const TOPIC: &str = "t";
/// The modelled partition's replication factor.
const REPLICAS: usize = 3;

/// The metadata image that sets the topic's `min.insync.replicas` to
/// `min_isr`, at both the topic and the cluster default, so the rule and the
/// model read the same number back.
pub(super) fn image(min_isr: usize) -> MetadataImage {
    elr_model_image(
        TOPIC,
        i16::try_from(REPLICAS).expect("the modelled partition is tiny"),
        min_isr,
    )
}

/// The `min.insync.replicas` that `image` resolves for the modelled topic.
pub(super) fn min_insync_replicas(image: &MetadataImage) -> usize {
    effective_min_insync_replicas(image, TOPIC, REPLICAS)
}

/// Recompute `s.elr` for the change from `previous` to the partition `s` now
/// holds, by driving the real maintenance rule. Nothing in this model restarts
/// uncleanly, so no replica is withheld as an unclean-shutdown one.
///
/// The published last-known ELR is not carried: it holds the last leader of a
/// partition that has none, this model applies nothing to such a partition,
/// and the rule derives the eligible set from the old ISR and the old eligible
/// set alone while there is a leader, so feeding it back empty yields the same
/// eligible set.
pub(super) fn maintain(image: &MetadataImage, s: &mut FailoverState, previous: &PartitionRecord) {
    let computed = next_partition_elr(
        image,
        Some(previous),
        &pr_of(s),
        &PartitionElr {
            eligible_leader_replicas: s.elr.clone(),
            last_known_elr: Vec::new(),
        },
        &BTreeSet::new(),
    );
    s.elr = computed.eligible_leader_replicas;
}
