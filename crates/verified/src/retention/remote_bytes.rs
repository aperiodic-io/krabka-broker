use {super::RemoteRetentionSegment, creusot_std::prelude::*};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Prefix bytes charged to remote retention: log-start breaches are free,
/// even when also time-expired. Totals are mathematical integers.
#[variant(count)]
pub fn remote_prefix_charge(segments: Seq<RemoteRetentionSegment>, count: Int) -> Int {
    pearlite! {
        if count <= 0 { 0 } else {
            remote_prefix_charge(segments, count - 1)
                + if segments[count - 1].log_start_breached { 0 } else { segments[count - 1].size@ }
        }
    }
}
