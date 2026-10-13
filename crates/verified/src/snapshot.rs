//! Snapshot-transfer chunk admission.

use creusot_std::prelude::*;

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// Pure decision for one snapshot response chunk.
    pub enum SnapshotChunkDecision {
        Restart,
        Continue { next_position: i64 },
        Complete,
    }

    /// Admission result for a fully assembled snapshot at the controller boundary.
    pub enum SnapshotInstallDecision {
        Reject,
        Stale,
        Install,
    }
}

/// Classify a decoded, fully assembled snapshot against live controller state.
#[ensures(match result {
    SnapshotInstallDecision::Reject => downgrade_pending
        || snapshot_end@ < 0
        || snapshot_epoch@ < 0
        || log_end@ < 0,
    SnapshotInstallDecision::Stale => !downgrade_pending
        && snapshot_end@ >= 0
        && snapshot_epoch@ >= 0
        && log_end@ >= 0
        && snapshot_end@ <= log_end@,
    SnapshotInstallDecision::Install => !downgrade_pending
        && snapshot_end@ >= 0
        && snapshot_epoch@ >= 0
        && log_end@ >= 0
        && snapshot_end@ > log_end@,
})]
#[must_use]
pub fn snapshot_install_decision(
    downgrade_pending: bool,
    snapshot_end: i64,
    snapshot_epoch: i32,
    log_end: i64,
) -> SnapshotInstallDecision {
    if downgrade_pending || snapshot_end < 0 || snapshot_epoch < 0 || log_end < 0 {
        SnapshotInstallDecision::Reject
    } else if snapshot_end <= log_end {
        SnapshotInstallDecision::Stale
    } else {
        SnapshotInstallDecision::Install
    }
}

/// A checkpoint may prune only a nonnegative boundary already committed.
#[ensures(result == (snapshot_end@ >= 0 && snapshot_end@ <= committed_end@))]
#[must_use]
pub fn snapshot_prune_admission(snapshot_end: i64, committed_end: i64) -> bool {
    snapshot_end >= 0 && snapshot_end <= committed_end
}

/// Admit exactly the next bounded chunk for one fixed snapshot identity and size.
#[ensures(match result {
    SnapshotChunkDecision::Restart => !identity_matches
        || received@ < 0
        || chunk_len@ < 0
        || max_size@ < 0
        || declared_size@ < 0
        || declared_size@ > max_size@
        || position@ != received@
        || match fixed_size {
            Some(size) => size@ != declared_size@,
            None => false,
        }
        || received@ + chunk_len@ > declared_size@,
    SnapshotChunkDecision::Continue { next_position } => snapshot_chunk_shape_valid(identity_matches, fixed_size, received@, declared_size@, position@, chunk_len@, max_size@)
        && next_position@ == received@ + chunk_len@
        && next_position@ < declared_size@,
    SnapshotChunkDecision::Complete => snapshot_chunk_shape_valid(identity_matches, fixed_size, received@, declared_size@, position@, chunk_len@, max_size@)
        && received@ + chunk_len@ == declared_size@,
})]
#[must_use]
pub fn snapshot_chunk_admission(
    identity_matches: bool,
    fixed_size: Option<i64>,
    received: i64,
    declared_size: i64,
    position: i64,
    chunk_len: i64,
    max_size: i64,
) -> SnapshotChunkDecision {
    if !identity_matches
        || received < 0
        || chunk_len < 0
        || max_size < 0
        || declared_size < 0
        || declared_size > max_size
        || position != received
    {
        return SnapshotChunkDecision::Restart;
    }
    if let Some(size) = fixed_size
        && size != declared_size
    {
        return SnapshotChunkDecision::Restart;
    }
    let Some(next_position) = received.checked_add(chunk_len) else {
        return SnapshotChunkDecision::Restart;
    };
    match next_position.cmp(&declared_size) {
        std::cmp::Ordering::Greater => SnapshotChunkDecision::Restart,
        std::cmp::Ordering::Equal => SnapshotChunkDecision::Complete,
        std::cmp::Ordering::Less => SnapshotChunkDecision::Continue { next_position },
    }
}

open_logic! {
/// A chunk continues one fixed identity and declared size at the exact received position.
fn snapshot_chunk_shape_valid(
    identity_matches: bool,
    fixed_size: Option<i64>,
    received: Int,
    declared_size: Int,
    position: Int,
    chunk_len: Int,
    max_size: Int,
) -> bool {
    pearlite! { identity_matches && received >= 0 && chunk_len >= 0 && max_size >= 0
    && declared_size >= 0 && declared_size <= max_size && position == received
    && match fixed_size { Some(size) => size@ == declared_size, None => true, } }
}
}

#[cfg(test)]
mod tests {
    use {
        super::{
            SnapshotChunkDecision, SnapshotInstallDecision, snapshot_chunk_admission,
            snapshot_install_decision, snapshot_prune_admission,
        },
        assert2::check,
    };

    #[test]
    fn install_admission_rejects_malformed_and_separates_stale_from_future() {
        use SnapshotInstallDecision::{Install, Reject, Stale};

        for (pending, end, epoch, log_end, expected) in [
            (false, 0, 0, 0, Stale),
            (false, 1, 0, 0, Install),
            (false, 11, 3, 10, Install),
            (false, 10, 3, 10, Stale),
            (false, 9, 3, 10, Stale),
            (true, 11, 3, 10, Reject),
            (false, -1, 3, 10, Reject),
            (false, 11, -1, 10, Reject),
            (false, 11, 3, -1, Reject),
        ] {
            check!(snapshot_install_decision(pending, end, epoch, log_end) == expected);
        }
    }

    #[test]
    fn prune_admission_never_crosses_the_committed_frontier() {
        check!(snapshot_prune_admission(10, 10));
        check!(snapshot_prune_admission(9, 10));
        check!(!snapshot_prune_admission(11, 10));
        check!(!snapshot_prune_admission(-1, 10));
    }

    #[test]
    fn chunk_admission_covers_retries_order_bounds_and_completion() {
        use SnapshotChunkDecision::{Complete, Continue, Restart};

        check!(
            snapshot_chunk_admission(true, None, 0, 6, 0, 3, 10) == Continue { next_position: 3 }
        );
        check!(snapshot_chunk_admission(true, Some(6), 3, 6, 3, 3, 10) == Complete);
        check!(snapshot_chunk_admission(true, None, 0, 0, 0, 0, 10) == Complete);
        check!(snapshot_chunk_admission(true, None, 0, 0, 0, 0, 0) == Complete);
        check!(snapshot_chunk_admission(true, None, 0, 10, 0, 10, 10) == Complete);

        for decision in [
            snapshot_chunk_admission(false, Some(6), 3, 6, 3, 3, 10),
            snapshot_chunk_admission(true, Some(7), 3, 6, 3, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 0, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 2, 3, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 4, 2, 10),
            snapshot_chunk_admission(true, Some(6), 3, 6, 3, 4, 10),
            snapshot_chunk_admission(true, None, -1, 6, -1, 3, 10),
            snapshot_chunk_admission(true, None, 0, 6, 0, -1, 10),
            snapshot_chunk_admission(true, None, 0, 6, 0, 3, -1),
            snapshot_chunk_admission(true, None, 0, -1, 0, 0, 10),
            snapshot_chunk_admission(true, None, 0, 11, 0, 1, 10),
            snapshot_chunk_admission(true, None, i64::MAX, i64::MAX, i64::MAX, 1, i64::MAX),
        ] {
            check!(decision == Restart);
        }
    }
}
