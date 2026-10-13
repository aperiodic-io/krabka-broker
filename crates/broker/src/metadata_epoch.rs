//! Broker adapters for the proved metadata-epoch successor.

use {krabka_metadata::LeaderEpoch, krabka_verified::exact_epoch_successor};

/// Advance a Kafka `int32` epoch exactly, rejecting exhaustion.
pub(crate) fn next_i32(current: i32) -> Option<i32> {
    exact_epoch_successor(i64::from(current), i64::from(i32::MAX))
        .and_then(|next| i32::try_from(next).ok())
}

/// Advance a partition leader epoch exactly, rejecting exhaustion.
pub(crate) fn next_leader(current: LeaderEpoch) -> Option<LeaderEpoch> {
    next_i32(current.0).map(LeaderEpoch)
}

/// Advance the partition epoch and, for a leader change, the leader epoch.
pub(crate) fn next_partition_change(
    partition_epoch: i32,
    leader_epoch: LeaderEpoch,
    leader_changes: bool,
) -> Option<(i32, LeaderEpoch)> {
    let partition_epoch = next_i32(partition_epoch)?;
    let leader_epoch = if leader_changes {
        next_leader(leader_epoch)?
    } else {
        leader_epoch
    };
    Some((partition_epoch, leader_epoch))
}

/// Advance a barrier epoch exactly, rejecting exhaustion.
pub(crate) fn next_i64(current: i64) -> Option<i64> { exact_epoch_successor(current, i64::MAX) }

#[cfg(test)]
mod tests {
    use {
        super::{LeaderEpoch, next_i32, next_i64, next_leader, next_partition_change},
        assert2::assert,
    };

    #[test]
    fn int32_adapter_preserves_the_exact_boundary() {
        assert!(next_i32(41) == Some(42));
        assert!(next_i32(i32::MAX - 1) == Some(i32::MAX));
        assert!(next_i32(i32::MAX).is_none());
    }

    #[test]
    fn leader_adapter_rejects_exhaustion() {
        assert!(next_leader(LeaderEpoch(41)) == Some(LeaderEpoch(42)));
        assert!(next_leader(LeaderEpoch(i32::MAX)).is_none());
    }

    #[test]
    fn partition_change_rejects_either_exhausted_field() {
        assert!(next_partition_change(8, LeaderEpoch(3), true) == Some((9, LeaderEpoch(4))));
        assert!(next_partition_change(i32::MAX, LeaderEpoch(3), false).is_none());
        assert!(next_partition_change(8, LeaderEpoch(i32::MAX), true).is_none());
    }

    #[test]
    fn int64_adapter_preserves_the_exact_boundary() {
        assert!(next_i64(i64::MAX - 1) == Some(i64::MAX));
        assert!(next_i64(i64::MAX).is_none());
    }
}
