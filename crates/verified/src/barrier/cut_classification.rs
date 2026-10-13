#[cfg(creusot)]
use super::logic;
use {
    super::{
        BarrierCutClassification, BarrierMarkerFenceDecision, BarrierMarkerFenceFacts,
        BarrierPlacementDecision, BarrierTargetCountDecision,
    },
    creusot_std::prelude::*,
};

/// Add a topic's exact positive partition count without wrapping.
#[ensures(match result {
    BarrierTargetCountDecision::Malformed => partition_count@ <= 0,
    BarrierTargetCountDecision::Overflow => partition_count@ > 0
        && total@ > i64::MAX@ - partition_count@,
    BarrierTargetCountDecision::Expand { next } => partition_count@ > 0
        && total@ <= i64::MAX@ - partition_count@
        && next@ == total@ + partition_count@,
})]
#[must_use]
pub fn barrier_target_count_decision(
    total: i64,
    partition_count: i32,
) -> BarrierTargetCountDecision {
    if partition_count <= 0 {
        return BarrierTargetCountDecision::Malformed;
    }
    let count = i64::from(partition_count);
    match total.checked_add(count) {
        Some(next) => BarrierTargetCountDecision::Expand { next },
        None => BarrierTargetCountDecision::Overflow,
    }
}

/// The marker fence, in the order its refusals rank: facts that name no
/// leadership generation at all (no image entry, or a negative epoch) are
/// `Malformed`; a leader other than the expected one in the image or in the
/// installed partition is `NotLeader`; a matching leader at another epoch is
/// `FencedEpoch`; and only an exact match of all three is `Append`.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn barrier_marker_fence_model(facts: BarrierMarkerFenceFacts) -> BarrierMarkerFenceDecision {
    pearlite! {
        if !facts.image_present
            || facts.expected_epoch@ < 0
            || facts.image_epoch@ < 0
            || facts.current_epoch@ < 0
        {
            BarrierMarkerFenceDecision::Malformed
        } else if facts.image_leader != facts.expected_leader
            || facts.current_leader != facts.expected_leader
        {
            BarrierMarkerFenceDecision::NotLeader
        } else if facts.image_epoch != facts.expected_epoch
            || facts.current_epoch != facts.expected_epoch
        {
            BarrierMarkerFenceDecision::FencedEpoch
        } else {
            BarrierMarkerFenceDecision::Append
        }
    }
}

/// Require the metadata image and installed partition to name exactly the
/// expected leader generation; see `barrier_marker_fence_model` for which
/// refusal each mismatch gets. The host maps the refusals to different wire
/// codes, so every variant is pinned.
#[ensures(result == barrier_marker_fence_model(facts))]
#[must_use]
pub fn barrier_marker_fence_decision(facts: BarrierMarkerFenceFacts) -> BarrierMarkerFenceDecision {
    if !facts.image_present
        || facts.expected_epoch < 0
        || facts.image_epoch < 0
        || facts.current_epoch < 0
    {
        return BarrierMarkerFenceDecision::Malformed;
    }
    if facts.image_leader != facts.expected_leader || facts.current_leader != facts.expected_leader
    {
        return BarrierMarkerFenceDecision::NotLeader;
    }
    if facts.image_epoch != facts.expected_epoch || facts.current_epoch != facts.expected_epoch {
        return BarrierMarkerFenceDecision::FencedEpoch;
    }
    BarrierMarkerFenceDecision::Append
}

#[ensures((result == BarrierPlacementDecision::Accept) == (requested && offset@ >= 0))]
#[must_use]
pub fn barrier_placement_decision(requested: bool, offset: i64) -> BarrierPlacementDecision {
    if requested && offset >= 0 {
        BarrierPlacementDecision::Accept
    } else {
        BarrierPlacementDecision::Reject
    }
}

#[ensures((result == BarrierCutClassification::Complete) == !has_missing)]
#[must_use]
pub fn barrier_cut_classification(has_missing: bool) -> BarrierCutClassification {
    if has_missing {
        BarrierCutClassification::Partial
    } else {
        BarrierCutClassification::Complete
    }
}
