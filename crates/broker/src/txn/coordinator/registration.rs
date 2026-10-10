//! Partition enrollment into a locally-coordinated transaction.
//!
//! The module holds the `AddPartitionsToTxn` transition that validates the
//! producer identity, moves the entry to `Ongoing`, and records the partitions
//! the transaction writes.

use krabka_log::ProducerId;
use krabka_verified::transaction::{
    TransactionRegistrationDecision, TransactionRegistrationFacts,
    TransactionRegistrationIdentityFacts, TransactionRegistrationIdentityMatchFacts,
    TransactionRegistrationOwnershipFacts, TransactionRegistrationStateFacts,
    transaction_partition_registration,
};

use super::TxnCoordinator;
use crate::txn::{state::TxnState, util::PRODUCER_FENCED_MIN_VERSION, version::TxnVersion};

impl TxnCoordinator {
    /// Add partitions to the locally-coordinated transaction after validating
    /// its producer identity. This is shared by client `AddPartitionsToTxn` and
    /// the KIP-890 server-side `TxnOffsetCommit` path.
    ///
    /// `version` is the `AddPartitionsToTxn` request version. It selects
    /// between `PRODUCER_FENCED` and the legacy `INVALID_PRODUCER_EPOCH` on a
    /// producer-epoch mismatch, and it decides the transaction version that
    /// the transition records (Kafka's `transactionVersionForAddPartitionsToTxn`).
    /// `format_txnv` is the cluster's level, which only picks the log format.
    pub(crate) async fn register_partitions(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<crate::txn::state::TopicPartition>,
        format_txnv: TxnVersion,
        version: i16,
    ) -> i16 {
        if tid.is_empty() {
            return crate::codes::INVALID_REQUEST;
        }
        if let Some(code @ crate::codes::COORDINATOR_LOAD_IN_PROGRESS) =
            self.coordinator_error(tid).await
        {
            return code;
        }
        let is_coordinator = self.is_coordinator_for(tid).await;
        if !is_coordinator {
            return registration_code(
                transaction_partition_registration(TransactionRegistrationFacts {
                    ownership: TransactionRegistrationOwnershipFacts {
                        is_coordinator: false,
                        producer_id_valid: true,
                        entry_exists: false,
                    },
                    identity: TransactionRegistrationIdentityFacts {
                        pending_transition: false,
                        matching: TransactionRegistrationIdentityMatchFacts {
                            transactional_id_matches: false,
                            producer_id_matches: false,
                            producer_epoch_matches: false,
                        },
                    },
                    state: TransactionRegistrationStateFacts {
                        state_allows_registration: false,
                        state_is_ongoing: false,
                        exact_partitions_registered: false,
                    },
                }),
                version,
            );
        }
        // Hold the state-partition write lock across the whole read, decide,
        // mutate and persist sequence. Two concurrent registrations for the
        // same transactional id must not each clone the same pre-mutation
        // snapshot and then race `put` under this same lock: whichever
        // published last would silently discard the other's newly registered
        // partitions even though both callers observe success. Serializing
        // the read with the write, the way `add_offsets_partition` and the
        // reaper's compare-and-swap transitions already do, closes that
        // window.
        let _state_partition_write = self.lock_state_partition_for(tid).await;
        let Some(entry_mutex) = self.get(tid) else {
            // A leadership change can evict the entry after the check above.
            return self.missing_entry_error(tid).await;
        };
        let mut entry = entry_mutex.lock().await;
        let decision = transaction_partition_registration(TransactionRegistrationFacts {
            ownership: TransactionRegistrationOwnershipFacts {
                is_coordinator: true,
                producer_id_valid: producer_id.get() >= 0,
                entry_exists: true,
            },
            identity: TransactionRegistrationIdentityFacts {
                pending_transition: entry.has_staged_producer_identity(),
                matching: TransactionRegistrationIdentityMatchFacts {
                    transactional_id_matches: entry.transactional_id == tid,
                    producer_id_matches: entry.producer_id == producer_id,
                    producer_epoch_matches: entry.producer_epoch == producer_epoch,
                },
            },
            state: TransactionRegistrationStateFacts {
                state_allows_registration: entry.state.can_transition_to(TxnState::Ongoing),
                state_is_ongoing: entry.state == TxnState::Ongoing,
                exact_partitions_registered: partitions
                    .iter()
                    .all(|partition| entry.partitions.contains(partition)),
            },
        });
        match decision {
            // Kafka's optimization: every requested partition is already in
            // an Ongoing transaction's set, so the answer is NONE with no
            // append. A stale exact match from a completed or not-yet-started
            // transaction does not qualify: `state_is_ongoing` in the facts
            // above rules it out, so those cases fall to `PersistRegistration`
            // below and still write.
            TransactionRegistrationDecision::PersistRetry => return crate::codes::NONE,
            TransactionRegistrationDecision::PersistRegistration => {}
            other => return registration_code(other, version),
        }
        // Stage the mutation on a clone: the live entry is replaced only on a
        // successful append. Mutating the locked guard directly would leave
        // the *live* entry looking registered (Ongoing, with the requested
        // partitions) even when the append below fails, which would let a
        // subsequent identical retry take the no-write `PersistRetry` path
        // above and wrongly report success for a registration that was never
        // made durable.
        let mut snapshot = entry.clone();
        let prior_state = snapshot.state;
        if matches!(
            prior_state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            snapshot.partitions.clear();
        }
        snapshot.state = TxnState::Ongoing;
        if prior_state != TxnState::Ongoing {
            snapshot.start_ms = crate::txn::util::now_millis();
        }
        snapshot.partitions.extend(partitions);
        snapshot.last_update_ms = crate::txn::util::now_millis();
        // Kafka `prepareAddPartitions` stamps the request's transaction
        // version, whatever level the cluster has finalized.
        snapshot.client_transaction_version =
            TxnVersion::for_add_partitions_to_txn(version).level();

        // Still holding both the entry lock and the state-partition write
        // lock acquired above: a second concurrent caller for this tid blocks
        // on the entry lock until this whole read-modify-persist sequence
        // finishes, so it always rereads the just-published state rather than
        // cloning the same stale snapshot this call started from.
        match self
            .put_under_state_partition_lock(snapshot, format_txnv)
            .await
        {
            Ok(persisted) => {
                *entry = persisted;
                crate::codes::NONE
            }
            Err(error) => {
                tracing::error!(tid, %error, "failed to persist registered transaction partitions");
                self.append_error_code(tid, &error).await
            }
        }
    }
}

/// Maps a registration decision to its wire code, at the given
/// `AddPartitionsToTxn` request version. Kafka's
/// `TransactionCoordinator.handleAddPartitionsToTransaction`:
///
/// - a pending transition, and the `PrepareCommit`/`PrepareAbort` state
///   check, both answer the retriable `CONCURRENT_TRANSACTIONS` (51);
/// - a producer-id mismatch answers `INVALID_PRODUCER_ID_MAPPING` (49);
/// - a producer-epoch mismatch answers `PRODUCER_FENCED` (90), downgraded to
///   the legacy `INVALID_PRODUCER_EPOCH` (47) below request version 2
///   (`KafkaApis.scala`).
fn registration_code(decision: TransactionRegistrationDecision, version: i16) -> i16 {
    match decision {
        TransactionRegistrationDecision::RejectNotCoordinator => crate::codes::NOT_COORDINATOR,
        TransactionRegistrationDecision::RejectUnknownProducer
        | TransactionRegistrationDecision::RejectProducerId => {
            crate::codes::INVALID_PRODUCER_ID_MAPPING
        }
        TransactionRegistrationDecision::RejectPendingTransition
        | TransactionRegistrationDecision::RejectState => crate::codes::CONCURRENT_TRANSACTIONS,
        TransactionRegistrationDecision::RejectProducerEpoch => {
            if version < PRODUCER_FENCED_MIN_VERSION {
                crate::codes::INVALID_PRODUCER_EPOCH
            } else {
                crate::codes::PRODUCER_FENCED
            }
        }
        TransactionRegistrationDecision::PersistRetry
        | TransactionRegistrationDecision::PersistRegistration => crate::codes::NONE,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::ProducerId;
    use tokio::sync::Mutex;

    use super::{TxnCoordinator, TxnState, TxnVersion};
    use crate::txn::{bootstrap, coordinator::test_support::test_coordinator, state::TxnEntry};

    fn partition(topic: &str, index: i32) -> crate::txn::state::TopicPartition {
        crate::txn::state::TopicPartition {
            topic: topic.to_string(),
            partition: PartitionIndex(index),
        }
    }

    async fn install_entry(coordinator: &TxnCoordinator, entry: TxnEntry) {
        let coordinator_partition = coordinator.partition_for(&entry.transactional_id);
        coordinator
            .lead_state_partition_for_test(coordinator_partition)
            .await;
        coordinator
            .state
            .insert(entry.transactional_id.clone(), Arc::new(Mutex::new(entry)));
    }

    fn open_transaction_partition(
        coordinator: &TxnCoordinator,
        directory: &std::path::Path,
        transactional_id: &str,
    ) -> Arc<crate::partition::Partition> {
        let index = coordinator.partition_for(transactional_id);
        let opened = crate::test_support::open_partition(
            directory,
            crate::test_support::StandalonePartitionSetup {
                topic: bootstrap::TOPIC,
                partition: krabka_ids::PartitionIndex(index.get()),
                ..Default::default()
            },
        );
        coordinator
            .partitions
            .insert(bootstrap::TOPIC.into(), index, Arc::clone(&opened));
        opened
    }

    /// Registers the transaction fixture's partitions while keeping every
    /// producer identity, requested partition set, and API version explicit.
    async fn register_fixture(
        coordinator: &TxnCoordinator,
        producer: (ProducerId, i16),
        partitions: Vec<crate::txn::state::TopicPartition>,
        version: i16,
    ) -> i16 {
        coordinator
            .register_partitions(
                "tid-a",
                producer.0,
                producer.1,
                partitions,
                TxnVersion::Classic,
                version,
            )
            .await
    }

    #[tokio::test]
    async fn registration_adapter_fences_coordinator_mapping_and_generation() {
        let coordinator = test_coordinator();
        let requested = vec![partition("orders", 4)];

        check!(
            register_fixture(&coordinator, (ProducerId(7), 3), requested.clone(), 3).await
                == crate::codes::NOT_COORDINATOR
        );
        coordinator
            .lead_state_partition_for_test(coordinator.partition_for("tid-a"))
            .await;
        check!(
            register_fixture(&coordinator, (ProducerId(7), 3), requested.clone(), 3).await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        let malformed = TxnEntry::new_empty("other-tid".into(), ProducerId(7), 3, 60_000, 0);
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(malformed)));
        check!(
            register_fixture(&coordinator, (ProducerId(7), 3), requested.clone(), 3).await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        // A pending transition (a staged producer identity) is
        // CONCURRENT_TRANSACTIONS, checked ahead of the producer id and
        // epoch, per Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction`.
        let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        entry.next_producer_epoch = 0;
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(entry)));
        check!(
            register_fixture(
                &coordinator,
                (ProducerId(7), i16::MAX),
                requested.clone(),
                3
            )
            .await
                == crate::codes::CONCURRENT_TRANSACTIONS
        );

        // A producer-epoch mismatch is PRODUCER_FENCED at request version 2
        // and above, downgraded to the legacy INVALID_PRODUCER_EPOCH below.
        let dead = || {
            let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
            entry.state = TxnState::Dead;
            entry
        };
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            register_fixture(
                &coordinator,
                (ProducerId(7), i16::MIN),
                requested.clone(),
                1
            )
            .await
                == crate::codes::INVALID_PRODUCER_EPOCH
        );
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            register_fixture(
                &coordinator,
                (ProducerId(7), i16::MIN),
                requested.clone(),
                3
            )
            .await
                == crate::codes::PRODUCER_FENCED
        );

        check!(
            register_fixture(
                &coordinator,
                (ProducerId(-1), i16::MAX),
                requested.clone(),
                3
            )
            .await
                == crate::codes::INVALID_PRODUCER_ID_MAPPING
        );

        // Matching identity but a state that cannot move to Ongoing (Dead)
        // is CONCURRENT_TRANSACTIONS, matching Kafka's PrepareCommit /
        // PrepareAbort retriable answer rather than a fatal one.
        coordinator
            .state
            .insert("tid-a".into(), Arc::new(Mutex::new(dead())));
        check!(
            register_fixture(&coordinator, (ProducerId(7), i16::MAX), requested, 3).await
                == crate::codes::CONCURRENT_TRANSACTIONS
        );
    }

    /// Kafka's `TransactionCoordinator.handleAddPartitionsToTransaction` never
    /// checks the transactional id for null/empty -- it is the very first
    /// check, and it answers `INVALID_REQUEST` whatever else is true.
    #[tokio::test]
    async fn empty_transactional_id_is_invalid_request() {
        let coordinator = test_coordinator();
        check!(
            coordinator
                .register_partitions(
                    "",
                    ProducerId(7),
                    0,
                    vec![partition("orders", 0)],
                    TxnVersion::Classic,
                    3,
                )
                .await
                == crate::codes::INVALID_REQUEST
        );
    }

    #[tokio::test]
    async fn exact_registration_persists_once_then_answers_none_without_a_write() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = test_coordinator();
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");
        let requested = partition("orders", i32::MAX);

        let first = register_fixture(
            &coordinator,
            (ProducerId(7), i16::MAX),
            vec![requested.clone()],
            3,
        )
        .await;
        check!(first == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);

        // Kafka's optimization: every requested partition is already in this
        // Ongoing transaction's set, so the retry answers NONE without an
        // append (#848). The log end offset must not move.
        let retry = register_fixture(
            &coordinator,
            (ProducerId(7), i16::MAX),
            vec![requested.clone()],
            3,
        )
        .await;
        check!(retry == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);

        // A new partition in the same request still writes.
        let grown = register_fixture(
            &coordinator,
            (ProducerId(7), i16::MAX),
            vec![requested.clone(), partition("orders", 0)],
            3,
        )
        .await;
        check!(grown == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 2);

        let stored = coordinator.get("tid-a").expect("registered entry");
        let stored = stored.lock().await;
        assert!(stored.partitions.contains(&requested));
        assert!(stored.partitions.contains(&partition("orders", 0)));
        drop(stored);

        let stale = register_fixture(
            &coordinator,
            (ProducerId(7), i16::MAX - 1),
            vec![partition("payments", 0)],
            3,
        )
        .await;
        check!(stale == crate::codes::PRODUCER_FENCED);
        check!(transaction_partition.log_end_offset().0 == 2);
    }

    /// The no-write retry optimization requires the *current* state to be
    /// exactly Ongoing (Kafka: `txnMetadata.state == ONGOING`). A stale exact
    /// match left over from a completed transaction's partition set must
    /// still transition and persist, not vacuously answer NONE (#848).
    #[tokio::test]
    async fn stale_partition_match_on_a_completed_entry_still_persists() {
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = test_coordinator();
        let mut entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), 0, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        let requested = partition("orders", 0);
        entry.partitions.insert(requested.clone());
        install_entry(&coordinator, entry).await;
        let transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");

        let code =
            register_fixture(&coordinator, (ProducerId(7), 0), vec![requested.clone()], 3).await;
        check!(code == crate::codes::NONE);
        check!(transaction_partition.log_end_offset().0 == 1);
        let stored = coordinator.get("tid-a").expect("registered entry");
        let stored = stored.lock().await;
        check!(stored.state == TxnState::Ongoing);
        assert!(stored.partitions.contains(&requested));
    }

    #[tokio::test]
    async fn failed_registration_retry_never_reports_success_or_leaves_dirty_state() {
        let coordinator = test_coordinator();
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), 0, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let requested = partition("orders", 9);

        // The state partition has no local replica, which Kafka's append
        // refuses with `NOT_LEADER_OR_FOLLOWER` and `appendTransactionToLog`
        // answers `NOT_COORDINATOR`.
        for _ in 0..2 {
            check!(
                register_fixture(&coordinator, (ProducerId(7), 0), vec![requested.clone()], 3)
                    .await
                    == crate::codes::NOT_COORDINATOR
            );
        }
        // No partition, on the state-partition being unopened for this test,
        // ever becomes durable, so the live entry must stay exactly as it
        // was: no partition added, and no Ongoing transition either. Leaving
        // it mutated in place would let a second failing attempt take the
        // no-write PersistRetry path and wrongly answer NONE.
        let stored = coordinator.get("tid-a").expect("entry remains present");
        let stored = stored.lock().await;
        assert!(!stored.partitions.contains(&requested));
        check!(stored.state == TxnState::Empty);
    }

    /// Regression test for a P1 review finding on PR #1135: two concurrent
    /// `register_partitions` calls for the same transactional id each cloned
    /// the same pre-mutation snapshot and raced to publish under the
    /// state-partition write lock. Whichever call published last won
    /// outright and silently discarded the other call's newly registered
    /// partitions, even though both callers observed `NONE`. Fanning out many
    /// concurrent, disjoint registrations and requiring every one of their
    /// partitions to survive makes that loss observable without the fix and
    /// proves it is gone with it: with the fix, the whole read-modify-persist
    /// sequence for a given tid runs under one continuous lock scope, so a
    /// concurrent caller always rereads the just-published state instead of a
    /// stale clone.
    #[tokio::test]
    async fn concurrent_registrations_do_not_lose_partitions() {
        const CONCURRENT_CALLS: i32 = 32;
        let directory = tempfile::tempdir().expect("tempdir");
        let coordinator = Arc::new(test_coordinator());
        let entry = TxnEntry::new_empty("tid-a".into(), ProducerId(7), i16::MAX, 60_000, 0);
        install_entry(&coordinator, entry).await;
        let _transaction_partition =
            open_transaction_partition(&coordinator, directory.path(), "tid-a");

        let mut expected = std::collections::HashSet::new();
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..CONCURRENT_CALLS {
            let requested = partition("orders", index);
            expected.insert(requested.clone());
            let coordinator = Arc::clone(&coordinator);
            tasks.spawn(async move {
                register_fixture(&coordinator, (ProducerId(7), i16::MAX), vec![requested], 3).await
            });
        }
        while let Some(result) = tasks.join_next().await {
            check!(result.expect("registration task did not panic") == crate::codes::NONE);
        }

        let stored = coordinator.get("tid-a").expect("entry remains present");
        assert!(stored.lock().await.partitions == expected);
    }
}
