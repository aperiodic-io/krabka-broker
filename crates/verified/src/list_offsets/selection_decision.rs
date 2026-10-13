#[cfg(creusot)]
use super::list_offsets_selection_model;
use {
    super::{ListOffsetsKind, ListOffsetsSelectionDecision, ListOffsetsSelectionFacts},
    creusot_std::prelude::*,
};

/// Apply the common final clamp after any local, diskless, or remote lookup.
///
/// The contract pins every variant to the rule `list_offsets_selection_model`
/// states.
#[must_use]
#[ensures(result == list_offsets_selection_model(facts))]
pub const fn list_offsets_selection_decision(
    facts: ListOffsetsSelectionFacts,
) -> ListOffsetsSelectionDecision {
    let mode = match facts.kind {
        ListOffsetsKind::Unsupported => {
            return ListOffsetsSelectionDecision::RejectMalformed;
        }
        ListOffsetsKind::Earliest | ListOffsetsKind::EarliestLocal => 0,
        ListOffsetsKind::Latest => 1,
        _ => 2,
    };
    if facts.candidate_offset < -1 || facts.candidate_epoch < -1 || facts.last_fetchable < 0 {
        return ListOffsetsSelectionDecision::RejectMalformed;
    }
    if facts.candidate_offset == -1 {
        return ListOffsetsSelectionDecision::Unknown;
    }
    let offset = if mode == 1 && facts.last_fetchable < facts.candidate_offset {
        facts.last_fetchable
    } else {
        facts.candidate_offset
    };
    if mode == 2 && offset >= facts.last_fetchable {
        return ListOffsetsSelectionDecision::Unknown;
    }
    ListOffsetsSelectionDecision::Resolved {
        offset,
        timestamp: facts.candidate_timestamp,
        leader_epoch: facts.candidate_epoch,
    }
}
