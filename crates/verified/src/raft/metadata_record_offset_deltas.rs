use super::{FetchContent, FetchFence, FetchResponseFacts, FetchResponseMutation};
#[cfg(creusot)]
use creusot_std::prelude::*;

open_logic! {
/// The response is a successful answer from the leader this node already
/// follows, in this node's epoch: only such a response may change the log or
/// the HWM (`KafkaRaftClient.handleFetchResponse` applies content only for
/// `Errors.NONE` from the followed leader).
pub fn leader_fence_holds(fence: FetchFence, response: FetchResponseFacts) -> bool {
    pearlite! {
        fence.role_leader == Some(response.from)
            && fence.current_leader == Some(response.from)
            && response.leader == Some(response.from)
            && response.epoch == fence.current_epoch
            && response.error_none
    }
}
}

open_logic! {
/// Kafka's `maybeHandleCommonResponse` discovery case: a response in this
/// node's epoch names a leader while the node knows none. It applies whatever
/// the error is, since a follower that knows the leader answers
/// `NOT_LEADER_OR_FOLLOWER` naming it. A newer epoch is the host's
/// `BeginQuorumEpoch` path and never reaches this kernel.
pub fn discovery_applies(fence: FetchFence, response: FetchResponseFacts) -> bool {
    pearlite! {
        fence.discovering
            && fence.role_leader == None
            && fence.current_leader == None
            && response.epoch == fence.current_epoch
            && response.leader != None
    }
}
}

/// Fence a Fetch response against the live role, leader, and epoch, then
/// select exactly one mutation path.
///
/// A discovery response carries no mutation: the node attaches to the
/// advertised leader and fetches again under the leader fence, which admits
/// content only from that leader itself. So no response is ever applied on
/// the strength of who the responder is assumed to be.
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Discover)
    == discovery_applies(fence, response)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Reject)
    == (!discovery_applies(fence, response) && !leader_fence_holds(fence, response))))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Snapshot)
    == (leader_fence_holds(fence, response) && content.has_snapshot)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Truncate)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && content.has_divergence)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::Append)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && !content.has_divergence
        && content.has_records)))]
#[cfg_attr(creusot, ensures((result == FetchResponseMutation::HighWatermark)
    == (leader_fence_holds(fence, response)
        && !content.has_snapshot
        && !content.has_divergence
        && !content.has_records)))]
#[must_use]
pub fn fetch_response_mutation(
    fence: FetchFence,
    response: FetchResponseFacts,
    content: FetchContent,
) -> FetchResponseMutation {
    if fence.discovering
        && fence.role_leader.is_none()
        && fence.current_leader.is_none()
        && response.epoch == fence.current_epoch
        && response.leader.is_some()
    {
        FetchResponseMutation::Discover
    } else if fence.role_leader != Some(response.from)
        || fence.current_leader != Some(response.from)
        || response.leader != Some(response.from)
        || response.epoch != fence.current_epoch
        || !response.error_none
    {
        FetchResponseMutation::Reject
    } else if content.has_snapshot {
        FetchResponseMutation::Snapshot
    } else if content.has_divergence {
        FetchResponseMutation::Truncate
    } else if content.has_records {
        FetchResponseMutation::Append
    } else {
        FetchResponseMutation::HighWatermark
    }
}

/// Advance a high watermark monotonically without passing the log end.
#[must_use]
#[cfg_attr(creusot, ensures(result@ >= previous@))]
#[cfg_attr(creusot, ensures(previous@ <= log_end@ ==> result@ <= log_end@))]
#[cfg_attr(creusot, ensures(result@ ==
    if requested@ <= log_end@ {
        if previous@ >= requested@ { previous@ } else { requested@ }
    } else if previous@ >= log_end@ { previous@ } else { log_end@ }))]
pub const fn advance_high_watermark(previous: i64, requested: i64, log_end: i64) -> i64 {
    let clamped = if requested <= log_end {
        requested
    } else {
        log_end
    };
    if previous >= clamped {
        previous
    } else {
        clamped
    }
}

/// Whether `value` lies in `[start, end)`.
#[must_use]
#[cfg_attr(creusot, ensures(result == (start@ <= value@ && value@ < end@)))]
pub const fn in_half_open_window(value: i64, start: i64, end: i64) -> bool {
    start <= value && value < end
}

/// Whether a monotonic frontier has reached a target offset.
#[must_use]
#[cfg_attr(creusot, ensures(result == (frontier@ >= target@)))]
pub const fn frontier_reaches(frontier: i64, target: i64) -> bool { frontier >= target }

/// Return the length of the strictly ordered control-record prefix below a
/// half-open frontier.
#[cfg_attr(creusot, requires(crate::sequence::strictly_increasing(offsets@)))]
#[cfg_attr(creusot, ensures(result@ <= offsets@.len()))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    0 <= i && i < result@ ==> offsets@[i]@ < frontier@))]
#[cfg_attr(creusot, ensures(forall<i: Int>
    result@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
#[must_use]
pub fn control_history_frontier(offsets: &[i64], frontier: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = offsets.len();
    #[cfg_attr(creusot, invariant(lo@ <= hi@ && hi@ <= offsets@.len()))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        0 <= i && i < lo@ ==> offsets@[i]@ < frontier@))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        hi@ <= i && i < offsets@.len() ==> frontier@ <= offsets@[i]@))]
    #[cfg_attr(creusot, variant(hi - lo))]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if offsets[mid] < frontier {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The contiguous offset deltas `0, 1, ..., record_count - 1` of one metadata
/// batch, in record order; the last element is the batch's `lastOffsetDelta`.
///
/// An empty batch, and a batch whose final delta Kafka's signed 32-bit
/// `lastOffsetDelta` cannot represent, fail closed. The deltas are counted in
/// `i32` beside the vector's length rather than narrowed from a `usize`
/// index, so the one executable body is the one Creusot proves.
#[must_use]
#[cfg_attr(creusot, ensures(match result {
    Some(deltas) => record_count@ > 0
        && record_count@ <= i32::MAX@ + 1
        && deltas@.len() == record_count@
        && forall<i: Int> 0 <= i && i < deltas@.len() ==> deltas@[i]@ == i,
    None => record_count@ == 0 || record_count@ > i32::MAX@ + 1,
}))]
pub fn metadata_record_offset_deltas(record_count: usize) -> Option<Vec<i32>> {
    // `i32::MAX + 1` records have final delta `i32::MAX`.
    let max_records: usize = 0x8000_0000;
    if record_count == 0 || record_count > max_records {
        return None;
    }
    let mut deltas = Vec::with_capacity(record_count);
    deltas.push(0);
    let mut last: i32 = 0;
    #[cfg_attr(creusot, invariant(1 <= deltas@.len() && deltas@.len() <= record_count@))]
    #[cfg_attr(creusot, invariant(last@ == deltas@.len() - 1))]
    #[cfg_attr(creusot, invariant(forall<i: Int>
        0 <= i && i < deltas@.len() ==> deltas@[i]@ == i))]
    #[cfg_attr(creusot, variant(record_count@ - deltas@.len()))]
    while deltas.len() < record_count {
        // `last + 1 == deltas.len() < record_count <= i32::MAX + 1`.
        last += 1;
        deltas.push(last);
    }
    Some(deltas)
}
