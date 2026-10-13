use {super::LocalRetentionSegment, creusot_std::prelude::*};

open_logic! {
/// Mathematical bytes in an oldest prefix; totals can exceed `u64::MAX`.
#[variant(count)]
pub fn local_prefix_bytes(segments: Seq<LocalRetentionSegment>, count: Int) -> Int {
    pearlite! {
        if count <= 0 { 0 } else {
            local_prefix_bytes(segments, count - 1) + segments[count - 1].size@
        }
    }
}
}

/// Extending a prefix cannot lower its byte cost, including zero-byte rows.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= segments.len())]
#[ensures(forall<first: Int, last: Int> 0 <= first && first <= last && last <= limit
    ==> local_prefix_bytes(segments, first) <= local_prefix_bytes(segments, last))]
#[variant(limit)]
pub fn local_prefix_bytes_monotone(segments: Seq<LocalRetentionSegment>, limit: Int) {
    if limit > 0 {
        local_prefix_bytes_monotone(segments, limit - 1);
    }
}
