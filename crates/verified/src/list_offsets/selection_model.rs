#[cfg(creusot)]
use super::{ListOffsetsSelectionDecision, ListOffsetsSelectionFacts, logic};
use {
    super::{
        ListOffsetsBoundDecision, ListOffsetsBoundFacts, ListOffsetsEarliestFacts,
        ListOffsetsEpochDecision, ListOffsetsKind,
    },
    creusot_std::prelude::*,
};

/// Classify every timestamp sentinel, including its first supported version.
#[must_use]
#[ensures(match result {
    ListOffsetsKind::Earliest => timestamp@ == -2,
    ListOffsetsKind::Latest => timestamp@ == -1,
    ListOffsetsKind::MaxTimestamp => timestamp@ == -3 && version@ >= 7,
    ListOffsetsKind::EarliestLocal => timestamp@ == -4 && version@ >= 8,
    ListOffsetsKind::LatestTiered => timestamp@ == -5 && version@ >= 9,
    ListOffsetsKind::EarliestPendingUpload => timestamp@ == -6 && version@ >= 11,
    ListOffsetsKind::Timestamp => timestamp@ >= 0,
    ListOffsetsKind::Unsupported => timestamp@ < -6
        || (timestamp@ == -6 && version@ < 11)
        || (timestamp@ == -5 && version@ < 9)
        || (timestamp@ == -4 && version@ < 8)
        || (timestamp@ == -3 && version@ < 7),
})]
pub const fn list_offsets_kind(timestamp: i64, version: i16) -> ListOffsetsKind {
    match timestamp {
        -2 => ListOffsetsKind::Earliest,
        -1 => ListOffsetsKind::Latest,
        -3 if version >= 7 => ListOffsetsKind::MaxTimestamp,
        -4 if version >= 8 => ListOffsetsKind::EarliestLocal,
        -5 if version >= 9 => ListOffsetsKind::LatestTiered,
        -6 if version >= 11 => ListOffsetsKind::EarliestPendingUpload,
        value if value >= 0 => ListOffsetsKind::Timestamp,
        _ => ListOffsetsKind::Unsupported,
    }
}

/// Fence every asserted epoch except the exact `-1` no-epoch sentinel.
#[must_use]
#[ensures((result == ListOffsetsEpochDecision::RejectMalformed) == (current_epoch@ < 0))]
#[ensures((result == ListOffsetsEpochDecision::Proceed) == (current_epoch@ >= 0
    && (requested_epoch@ == -1 || requested_epoch@ == current_epoch@)))]
#[ensures((result == ListOffsetsEpochDecision::Fenced) == (current_epoch@ >= 0
    && requested_epoch@ != -1 && requested_epoch@ < current_epoch@))]
#[ensures((result == ListOffsetsEpochDecision::Unknown) == (current_epoch@ >= 0
    && requested_epoch@ > current_epoch@))]
pub const fn list_offsets_epoch_decision(
    requested_epoch: i32,
    current_epoch: i32,
) -> ListOffsetsEpochDecision {
    if current_epoch < 0 {
        ListOffsetsEpochDecision::RejectMalformed
    } else if requested_epoch == -1 || requested_epoch == current_epoch {
        ListOffsetsEpochDecision::Proceed
    } else if requested_epoch < current_epoch {
        ListOffsetsEpochDecision::Fenced
    } else {
        ListOffsetsEpochDecision::Unknown
    }
}

/// Select LEO for replicas, HWM for ordinary consumers, and `min(LSO, HWM)`
/// for read-committed consumers.
#[must_use]
#[ensures(match result {
    ListOffsetsBoundDecision::RejectMalformed => {
        (facts.replica_id@ != -1 && facts.log_end@ < 0)
            || (facts.replica_id@ == -1 && facts.high_watermark@ < 0)
            || (facts.replica_id@ == -1 && facts.isolation_level@ == 1
                && facts.last_stable@ < 0)
    }
    ListOffsetsBoundDecision::Bound { offset } => {
        offset@ >= 0
            && (facts.replica_id@ != -1 ==> offset@ == facts.log_end@)
            && (facts.replica_id@ == -1 && facts.isolation_level@ != 1
                ==> offset@ == facts.high_watermark@)
            && (facts.replica_id@ == -1 && facts.isolation_level@ == 1
                ==> offset@ == if facts.last_stable@ < facts.high_watermark@ {
                    facts.last_stable@
                } else {
                    facts.high_watermark@
                })
    }
})]
pub const fn list_offsets_bound_decision(facts: ListOffsetsBoundFacts) -> ListOffsetsBoundDecision {
    if (facts.replica_id != -1 && facts.log_end < 0)
        || (facts.replica_id == -1 && facts.high_watermark < 0)
        || (facts.replica_id == -1 && facts.isolation_level == 1 && facts.last_stable < 0)
    {
        return ListOffsetsBoundDecision::RejectMalformed;
    }
    let offset = if facts.replica_id != -1 {
        facts.log_end
    } else if facts.isolation_level == 1 {
        if facts.last_stable < facts.high_watermark {
            facts.last_stable
        } else {
            facts.high_watermark
        }
    } else {
        facts.high_watermark
    };
    ListOffsetsBoundDecision::Bound { offset }
}

/// Select the equal logical minimum across every available tier.
#[must_use]
#[ensures(match result {
    None => facts.local@ < 0
        || (facts.has_remote && facts.remote@ < 0)
        || (facts.has_diskless && facts.diskless@ < 0),
    Some(offset) => facts.local@ >= 0
        && (!facts.has_remote || facts.remote@ >= 0)
        && (!facts.has_diskless || facts.diskless@ >= 0)
        && offset@ <= facts.local@
        && (!facts.has_remote || offset@ <= facts.remote@)
        && (!facts.has_diskless || offset@ <= facts.diskless@)
        && (offset@ == facts.local@
            || (facts.has_remote && offset@ == facts.remote@)
            || (facts.has_diskless && offset@ == facts.diskless@)),
})]
pub const fn list_offsets_earliest(facts: ListOffsetsEarliestFacts) -> Option<i64> {
    if facts.local < 0
        || (facts.has_remote && facts.remote < 0)
        || (facts.has_diskless && facts.diskless < 0)
    {
        return None;
    }
    let local_or_remote = if facts.has_remote && facts.remote < facts.local {
        facts.remote
    } else {
        facts.local
    };
    let offset = if facts.has_diskless && facts.diskless < local_or_remote {
        facts.diskless
    } else {
        local_or_remote
    };
    Some(offset)
}

open_logic! {
/// The Kafka rule for one partition row's final `ListOffsets` value.
///
/// A malformed input fails closed, and a lookup that found nothing (`-1`) is
/// unknown. `EARLIEST` and `EARLIEST_LOCAL` answer a log start, which Kafka
/// returns whatever the isolation bound, so they resolve to the candidate
/// unclamped. `LATEST` answers the isolation bound itself, so it resolves to
/// the lower of the candidate and `last_fetchable`. Every other kind is
/// record-derived and, as in `Partition.fetchOffsetForTimestamp`, resolves
/// only when its offset is strictly below `last_fetchable`; otherwise it is
/// unknown. A resolved row carries the candidate's timestamp and leader epoch.
pub fn list_offsets_selection_model(
    facts: ListOffsetsSelectionFacts,
) -> ListOffsetsSelectionDecision {
    pearlite! {
        if facts.kind == ListOffsetsKind::Unsupported
            || facts.candidate_offset@ < -1
            || facts.candidate_epoch@ < -1
            || facts.last_fetchable@ < 0
        {
            ListOffsetsSelectionDecision::RejectMalformed
        } else if facts.candidate_offset@ == -1 {
            ListOffsetsSelectionDecision::Unknown
        } else if facts.kind == ListOffsetsKind::Earliest
            || facts.kind == ListOffsetsKind::EarliestLocal
        {
            ListOffsetsSelectionDecision::Resolved {
                offset: facts.candidate_offset,
                timestamp: facts.candidate_timestamp,
                leader_epoch: facts.candidate_epoch,
            }
        } else if facts.kind == ListOffsetsKind::Latest {
            ListOffsetsSelectionDecision::Resolved {
                offset: if facts.last_fetchable@ < facts.candidate_offset@ {
                    facts.last_fetchable
                } else {
                    facts.candidate_offset
                },
                timestamp: facts.candidate_timestamp,
                leader_epoch: facts.candidate_epoch,
            }
        } else if facts.candidate_offset@ < facts.last_fetchable@ {
            ListOffsetsSelectionDecision::Resolved {
                offset: facts.candidate_offset,
                timestamp: facts.candidate_timestamp,
                leader_epoch: facts.candidate_epoch,
            }
        } else {
            ListOffsetsSelectionDecision::Unknown
        }
    }
}
}
