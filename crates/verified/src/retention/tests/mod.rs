use assert2::check;

use super::*;

/// A local segment that is neither blocked nor expired.
const fn fresh(size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: false,
        expired: false,
        size,
    }
}

/// A local segment past `retention.ms` or below the log start.
const fn expired(size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: false,
        expired: true,
        size,
    }
}

/// A local segment no pass may delete.
const fn blocked(expired: bool, size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: true,
        expired,
        size,
    }
}

/// A finished remote segment with the given axes.
const fn remote(log_start_breached: bool, time_expired: bool, size: u64) -> RemoteRetentionSegment {
    RemoteRetentionSegment {
        blocked: false,
        log_start_breached,
        time_expired,
        size,
    }
}

mod barrier_cut_expiry_is_exact_and_fails_closed_at_extremes;

mod delete_target_rejects_offset_exhaustion;

mod remote_charge;

mod remote_floor;

#[test]
fn a_blocked_remote_segment_stops_every_retention_axis() {
    for (floor, time) in [(false, false), (true, false), (false, true), (true, true)] {
        let segments = [
            remote(false, true, 10),
            RemoteRetentionSegment {
                blocked: true,
                ..remote(floor, time, 10)
            },
            remote(true, true, 10),
        ];
        for debt in [0, 10, 100] {
            check!(remote_retention_prefix(true, &segments, debt) == 1);
        }
    }
}
