//! The producer identity a transaction continues with once it completes.
//! KIP-890 bumps the epoch on completion and rotates to a fresh producer id at
//! the epoch boundary the transaction marker reserves, so `EndTxn` and
//! `InitProducerId` share these rules from one place.

use {
    crate::{
        error::BrokerError,
        producer_id_manager::ProducerIdManager,
        txn::{state::TxnEntry, version::TxnVersion},
    },
    krabka_log::ProducerId,
};

/// KIP-890: the `(producer_id, producer_epoch)` a producer continues with after
/// a transaction completes.
///
/// - Below `TV_2`: unchanged — the epoch only moves on `InitProducerId` reuse.
/// - `TV >= 2`, normal: same `producer_id`, `epoch + 1` — bumping on completion
///   fences a zombie holding the old epoch without a fresh `InitProducerId`.
/// - `TV >= 2`, marker-epoch boundary (`epoch >= i16::MAX - 1`): `i16::MAX` is
///   reserved for the transaction marker, so a *new* `producer_id` is allocated
///   at epoch 0 before the client can receive the reserved epoch. The caller
///   records the old id as `prev_producer_id`; `EndTxn` v5 returns the new pair.
pub(crate) async fn next_producer_identity(
    txnv: TxnVersion,
    pid: ProducerId,
    epoch: i16,
    ids: &ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if txnv.verified() && epoch >= i16::MAX - 1 {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(
        next_identity_with_fresh(txnv.verified(), false, pid, epoch, fresh)
            .expect("fresh producer ID supplied at the rotation boundary"),
    )
}

fn next_identity_with_fresh(
    verified: bool,
    recovery: bool,
    pid: ProducerId,
    epoch: i16,
    fresh: Option<ProducerId>,
) -> Option<(ProducerId, i16)> {
    krabka_verified::transaction::next_producer_identity(
        verified,
        recovery,
        pid.0,
        epoch,
        fresh.map(|producer_id| producer_id.0),
    )
    .map(|(producer_id, epoch)| (ProducerId(producer_id), epoch))
}

/// KIP-939 recovery identities have already moved past the original producer
/// identity that must retain `i16::MAX` for its transaction marker. A staged
/// recovery identity can therefore advance through `i16::MAX`; only a later
/// recovery or completion rotates it to a fresh producer ID.
pub(crate) async fn next_recovery_producer_identity(
    pid: ProducerId,
    epoch: i16,
    ids: &ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if epoch == i16::MAX {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(next_identity_with_fresh(true, true, pid, epoch, fresh)
        .expect("fresh producer ID supplied at the recovery rotation boundary"))
}

pub(crate) fn client_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    if entry.has_staged_producer_identity() {
        (entry.next_producer_id, entry.next_producer_epoch)
    } else {
        (entry.producer_id, entry.producer_epoch)
    }
}

pub(crate) fn completion_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    client_producer_identity(entry)
}

/// Whether completing `entry` at `txnv` rotates to a fresh producer ID, which
/// the caller has to allocate before it prepares the identities.
fn completion_needs_fresh_producer_id(entry: &TxnEntry, txnv: TxnVersion) -> bool {
    let (_, client_epoch) = client_producer_identity(entry);
    let at_rotation_boundary = if entry.has_staged_producer_identity() {
        client_epoch == i16::MAX
    } else {
        client_epoch >= i16::MAX - 1
    };
    txnv.verified() && at_rotation_boundary
}

pub(crate) async fn prepare_completion_identities(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    ids: &ProducerIdManager,
) -> Result<(), BrokerError> {
    let fresh = if completion_needs_fresh_producer_id(entry, txnv) {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    prepare_completion_identities_with_fresh(entry, txnv, fresh)
        .expect("fresh producer ID supplied at the rotation boundary");
    Ok(())
}

/// Prepares the abort that the coordinator itself runs on an `Ongoing`
/// transaction: the timeout reaper, and the fence `InitProducerId` runs for a
/// producer that re-initialises. See
/// [`prepare_server_abort_identities_with_fresh`].
pub(crate) async fn prepare_server_abort_identities(
    entry: &mut TxnEntry,
    server_version: TxnVersion,
    ids: &ProducerIdManager,
) -> Result<(), BrokerError> {
    let fresh = if completion_needs_fresh_producer_id(entry, server_version) {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    prepare_server_abort_identities_with_fresh(entry, server_version, fresh)
        .expect("fresh producer ID supplied at the rotation boundary");
    Ok(())
}

/// The server abort of [`prepare_server_abort_identities`], with the fresh
/// producer ID the caller allocated when the epoch is exhausted. `None` is
/// returned when the rotation needs one and `fresh` is `None`.
///
/// Kafka's `prepareFenceProducerEpoch` followed by
/// `endTransaction(isFromClient = false)` at the cluster's transaction version
/// `server_version`: `TransactionMetadata.prepareFenceProducerEpoch` and
/// `prepareAbortOrCommit`, as `handleInitProducerId` and
/// `abortTimedOutTransactions` drive them.
///
/// - `TV_2`: the completion bump in [`prepare_completion_identities_with_fresh`]
///   is the only epoch bump. The abort and its markers sit at `epoch + 1`, and
///   `last_producer_epoch` names the epoch the producer still holds, so its
///   retry of `InitProducerId` is recognised. The record carries `TV_2`.
/// - Below `TV_2`: nothing bumps at completion, so the fence raises the epoch
///   itself, unless an earlier fence already did (`has_failed_epoch_fence`) or
///   the epoch is `i16::MAX`, and no last epoch is kept. The timed-out producer
///   is then fenced at its partitions. `endTransactionWithTV1` always stamps
///   `TV_0` on the record, also on a `TV_1` cluster.
pub(crate) fn prepare_server_abort_identities_with_fresh(
    entry: &mut TxnEntry,
    server_version: TxnVersion,
    fresh: Option<ProducerId>,
) -> Option<()> {
    entry.client_transaction_version = if server_version.verified() {
        TxnVersion::Verified
    } else {
        TxnVersion::Classic
    }
    .level();
    if !server_version.verified() {
        entry.last_producer_epoch = -1;
        if !entry.has_failed_epoch_fence && entry.producer_epoch < i16::MAX {
            entry.producer_epoch += 1;
        }
    }
    prepare_completion_identities_with_fresh(entry, server_version, fresh)
}

pub(crate) fn prepare_completion_identities_with_fresh(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    fresh: Option<ProducerId>,
) -> Option<()> {
    if !txnv.verified() {
        return Some(());
    }

    let had_recovery_identity = entry.has_staged_producer_identity();
    let (client_pid, client_epoch) = client_producer_identity(entry);
    let (completion_pid, completion_epoch) =
        next_identity_with_fresh(true, had_recovery_identity, client_pid, client_epoch, fresh)?;

    // The transaction marker fences the identity that wrote the transaction.
    // i16::MAX is reserved for this final marker epoch. Kafka's
    // `prepareAbortOrCommit` records the epoch it leaves as the last epoch,
    // which is how `InitProducerId` recognises a retry that names it.
    entry.last_producer_epoch = entry.producer_epoch;
    entry.producer_epoch = entry.producer_epoch.saturating_add(1);

    if had_recovery_identity || completion_pid != entry.producer_id {
        entry.next_producer_id = completion_pid;
        entry.next_producer_epoch = completion_epoch;
    } else {
        entry.next_producer_id = ProducerId(-1);
        entry.next_producer_epoch = -1;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::txn::{
            coordinator::completion::apply_completion, handlers::end_txn::test_support::entry,
            state::TxnState,
        },
        assert2::assert,
    };

    #[tokio::test]
    async fn epoch_bumps_only_at_tv2() {
        use crate::txn::version::TxnVersion;
        let ids = ProducerIdManager::new();
        let cases = [
            // Below TV_2 (Classic, Flexible): pid + epoch unchanged.
            (TxnVersion::Classic, (ProducerId(7), 3)),
            (TxnVersion::Flexible, (ProducerId(7), 3)),
            // TV_2 non-overflow: same pid, epoch + 1.
            (TxnVersion::Verified, (ProducerId(7), 4)),
        ];
        for (version, want) in cases {
            assert!(
                next_producer_identity(version, ProducerId(7), 3, &ids)
                    .await
                    .unwrap()
                    == want,
                "txn version {version:?}"
            );
        }
    }

    #[tokio::test]
    async fn epoch_overflow_at_tv2_allocates_new_pid_at_epoch_zero() {
        use crate::txn::version::TxnVersion;
        let ids = ProducerIdManager::new();
        // MAX is reserved for the marker epoch, so the client rotates at
        // MAX-1 and receives a fresh producer_id at epoch 0.
        let (new_pid, new_epoch) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX - 1, &ids)
                .await
                .unwrap();
        assert!(new_pid != 7);
        assert!(new_epoch == 0);
        // The allocator hands out a distinct pid on the next overflow too.
        let (next_pid, _) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap();
        assert!(next_pid != new_pid);
        // Below TV_2 at i16::MAX: no roll, epoch stays (no bump path taken).
        assert!(
            next_producer_identity(TxnVersion::Classic, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap()
                == (ProducerId(7), i16::MAX)
        );
    }

    #[tokio::test]
    async fn normal_completion_rotates_before_the_reserved_marker_epoch() {
        let ids = ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        let (completion_pid, completion_epoch) = completion_producer_identity(&entry);
        assert!(completion_pid != 7);
        assert!(completion_epoch == 0);
    }

    #[tokio::test]
    async fn legacy_completion_does_not_allocate_at_the_tv2_boundary() {
        let ids = ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Classic, &ids)
            .await
            .unwrap();

        assert!(completion_producer_identity(&entry) == (ProducerId(7), i16::MAX - 1));
        assert!(
            ids.allocate().await.unwrap() == (ProducerId(0), 0),
            "legacy completion must not consume a fresh producer ID"
        );
    }

    /// Kafka's `prepareFenceProducerEpoch` followed by the server's
    /// `endTransaction`: `TV_2` bumps the epoch once, at completion, and keeps
    /// the epoch the producer holds; below it the fence bumps once and keeps no
    /// last epoch. `endTransactionWithTV1` stamps `TV_0` on the record at every
    /// level below `TV_2`, `TV_1` included.
    #[test]
    fn a_server_abort_bumps_the_epoch_once_and_stamps_the_version_kafka_stamps() {
        let ongoing = entry(7, 3, TxnState::Ongoing);
        // (cluster level, epoch after the abort, last epoch, stamped version)
        let cases = [
            (TxnVersion::Classic, 4, -1, 0),
            (TxnVersion::Flexible, 4, -1, 0),
            (TxnVersion::Verified, 4, 3, 2),
        ];
        for (version, epoch, last_epoch, stamp) in cases {
            let mut aborted = ongoing.clone();
            assert!(
                prepare_server_abort_identities_with_fresh(&mut aborted, version, None).is_some()
            );
            let expected = TxnEntry {
                producer_epoch: epoch,
                last_producer_epoch: last_epoch,
                client_transaction_version: stamp,
                ..ongoing.clone()
            };
            assert!(aborted == expected, "{version:?}");
            assert!(
                completion_producer_identity(&aborted) == (ProducerId(7), epoch),
                "{version:?}: the identity the abort completes with"
            );
        }
    }

    #[test]
    fn a_server_abort_at_the_epoch_boundary_rotates_the_producer_id_at_tv2_only() {
        let ongoing = entry(7, i16::MAX - 1, TxnState::Ongoing);

        // `TV_2` needs a fresh producer ID for the rotation, and hands out
        // epoch 0 of it after the reserved marker epoch.
        assert!(
            prepare_server_abort_identities_with_fresh(
                &mut ongoing.clone(),
                TxnVersion::Verified,
                None
            )
            .is_none()
        );
        let mut rotated = ongoing.clone();
        assert!(
            prepare_server_abort_identities_with_fresh(
                &mut rotated,
                TxnVersion::Verified,
                Some(ProducerId(11))
            )
            .is_some()
        );
        let expected = TxnEntry {
            producer_epoch: i16::MAX,
            last_producer_epoch: i16::MAX - 1,
            client_transaction_version: 2,
            next_producer_id: ProducerId(11),
            next_producer_epoch: 0,
            ..ongoing.clone()
        };
        assert!(rotated == expected);
        assert!(completion_producer_identity(&rotated) == (ProducerId(11), 0));

        // Below `TV_2` the fence takes the epoch to the reserved one, and the
        // producer id stays.
        let mut fenced = ongoing.clone();
        assert!(
            prepare_server_abort_identities_with_fresh(&mut fenced, TxnVersion::Classic, None)
                .is_some()
        );
        assert!(fenced.producer_epoch == i16::MAX);
        assert!(completion_producer_identity(&fenced) == (ProducerId(7), i16::MAX));
    }

    /// Kafka's `prepareFenceProducerEpoch` does not raise the epoch again after
    /// a fence whose write failed, and `prepareComplete` clears that memory once
    /// the abort completes. Without the clearing, every later fence of the
    /// transactional id would leave the producer unfenced.
    #[test]
    fn a_failed_fence_holds_back_one_bump_and_completion_forgets_it() {
        let mut ongoing = entry(7, 3, TxnState::Ongoing);
        ongoing.has_failed_epoch_fence = true;

        assert!(
            prepare_server_abort_identities_with_fresh(&mut ongoing, TxnVersion::Classic, None)
                .is_some()
        );
        assert!(ongoing.producer_epoch == 3, "the epoch was already raised");

        let identity = completion_producer_identity(&ongoing);
        apply_completion(&mut ongoing, TxnState::CompleteAbort, identity, 1);
        assert!(!ongoing.has_failed_epoch_fence);

        ongoing.state = TxnState::Ongoing;
        assert!(
            prepare_server_abort_identities_with_fresh(&mut ongoing, TxnVersion::Classic, None)
                .is_some()
        );
        assert!(ongoing.producer_epoch == 4, "the next fence raises it");
    }

    #[tokio::test]
    async fn prepared_recovery_uses_marker_identity_and_fences_the_recovery_client() {
        let ids = ProducerIdManager::new();
        let mut entry = entry(7, 3, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = 4;

        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();

        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 4, "marker identity must advance");
        assert!(completion_producer_identity(&entry) == (ProducerId(7), 5));
    }

    #[tokio::test]
    async fn prepared_recovery_can_use_max_epoch_before_rotating() {
        let ids = ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(11);
        entry.next_producer_epoch = i16::MAX - 1;

        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        assert!(completion_producer_identity(&entry) == (ProducerId(11), i16::MAX));

        entry.next_producer_epoch = i16::MAX;
        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();
        let (rotated_pid, rotated_epoch) = completion_producer_identity(&entry);
        assert!(rotated_pid != 11);
        assert!(rotated_epoch == 0);
    }
}
