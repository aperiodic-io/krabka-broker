//! Producer-identity selection for a transactional `InitProducerId`.
//!
//! Both entry points answer the same question from a different starting
//! point: which `(producer_id, producer_epoch)` pair the coordinator hands
//! back when a transactional id is re-initialised, and which pair it stages on
//! the entry for a KIP-939 recovery. Kafka rotates to a fresh producer id once
//! the epoch is exhausted (`i16::MAX - 1` and above), at every transaction
//! version, so the coordinator always keeps one epoch to fence the producer
//! with.

use crate::{
    error::BrokerError,
    producer_id_manager::ProducerIdManager,
    txn::{
        handlers::end_txn::{
            client_producer_identity, next_producer_identity, next_recovery_producer_identity,
        },
        state::TxnEntry,
        version::TxnVersion,
    },
};

pub(super) async fn next_init_producer_identity(
    entry: &TxnEntry,
    producer_ids: &ProducerIdManager,
) -> Result<(krabka_log::ProducerId, i16), BrokerError> {
    let (pid, epoch) = client_producer_identity(entry);
    // `TransactionCoordinator.handleInitProducerId` rotates whenever
    // `isEpochExhausted` holds, whatever the transaction version, so the
    // rotation rule here is the verified one at every level.
    next_producer_identity(TxnVersion::Verified, pid, epoch, producer_ids).await
}

pub(super) async fn stage_recovery_identity(
    entry: &mut TxnEntry,
    producer_ids: &ProducerIdManager,
) -> Result<(krabka_log::ProducerId, i16), BrokerError> {
    let already_staged = entry.has_staged_producer_identity();
    let (client_pid, client_epoch) = client_producer_identity(entry);
    let (next_pid, next_epoch) = if already_staged {
        next_recovery_producer_identity(client_pid, client_epoch, producer_ids).await?
    } else {
        next_producer_identity(TxnVersion::Verified, client_pid, client_epoch, producer_ids).await?
    };
    entry.next_producer_id = next_pid;
    entry.next_producer_epoch = next_epoch;
    Ok((next_pid, next_epoch))
}

#[cfg(test)]
mod tests {
    use {super::*, crate::txn::state::TxnState, assert2::assert, krabka_log::ProducerId};

    /// Kafka `prepareIncrementProducerEpoch` bumps the epoch, and
    /// `prepareProducerIdRotation` rotates to a fresh producer id once the
    /// epoch is exhausted. The rule does not depend on the transaction
    /// version.
    #[tokio::test]
    async fn the_epoch_bumps_until_it_is_exhausted_and_then_the_producer_id_rotates() {
        let ids = ProducerIdManager::new();
        // (entry epoch, the epoch handed back, whether the producer id rotates)
        let cases = [
            (3_i16, 4_i16, false),
            (i16::MAX - 2, i16::MAX - 1, false),
            (i16::MAX - 1, 0, true),
            (i16::MAX, 0, true),
        ];
        for (entry_epoch, expected_epoch, rotates) in cases {
            let mut entry =
                TxnEntry::new_empty("tid-init".into(), ProducerId(11), entry_epoch, i32::MAX, 0);
            entry.state = TxnState::CompleteCommit;
            let (pid, epoch) = next_init_producer_identity(&entry, &ids).await.unwrap();
            assert!(epoch == expected_epoch, "epoch at {entry_epoch}");
            assert!(
                (pid != ProducerId(11)) == rotates,
                "rotation at {entry_epoch}"
            );
        }
    }

    #[tokio::test]
    async fn recovery_identity_advances_through_max_then_rotates() {
        let ids = ProducerIdManager::new();
        let mut entry = TxnEntry::new_empty("tid-recover".into(), ProducerId(7), 3, i32::MAX, 0);
        entry.state = TxnState::Ongoing;

        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 4));
        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 5));
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);

        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = i16::MAX - 1;
        assert!(
            stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), i16::MAX)
        );
        let (rotated_pid, rotated_epoch) = stage_recovery_identity(&mut entry, &ids).await.unwrap();
        assert!(rotated_pid != 7);
        assert!(rotated_epoch == 0);
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);

        entry.next_producer_id = ProducerId(-1);
        entry.next_producer_epoch = -1;
        entry.producer_epoch = i16::MAX - 1;
        let (initial_rotated_pid, initial_rotated_epoch) =
            stage_recovery_identity(&mut entry, &ids).await.unwrap();
        assert!(initial_rotated_pid != 7);
        assert!(initial_rotated_epoch == 0);
    }
}
