//! Test-only [`BrokerHandle`] helpers for KIP-932 share state: the persisted
//! summary for one share partition and the awaiters that block until its
//! start offset, delivery-complete count, or acquired-batch count reaches an
//! expected value.

use crate::broker::BrokerHandle;

impl BrokerHandle {
    /// Test-only: read the share-state summary
    /// `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`
    /// for `(group, topic_id, partition)` straight from this broker's
    /// internal `ShareCoordinator`.
    /// Returns `None` when the key has no initialized state. KIP-932 lifecycle
    /// tests use this to assert that the group coordinator initialized the
    /// per-partition share state. The persister RPCs stay off the wire.
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn share_state_summary_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<(i32, i32, i64, i32)> {
        // Unwrap the summary's `start_offset` -> `i64` at this test-helper
        // boundary: integration tests compare it against raw offset literals.
        self.broker
            .share_coordinator
            .read_summary(group, topic_id, partition)
            .await
            .ok()
            .flatten()
            .map(|(state_epoch, leader_epoch, start_offset, count)| {
                (state_epoch, leader_epoch, start_offset.0, count)
            })
    }

    /// Test-only: await until the persisted share-state summary exists for
    /// `(group, topic_id, partition)` (share-state initialized / recovered).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_for_share_state_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) {
        let present = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some()
        })
        .await;
        assert2::assert!(
            present,
            "share-state summary for {group}:{topic_id}:{partition} not present within 30s"
        );
    }

    /// Test-only: await until the share-partition SPSO (start_offset) >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_spso(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i64,
    ) {
        let reached = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some_and(|(_, _, spso, _)| spso >= min)
        })
        .await;
        assert2::assert!(
            reached,
            "share SPSO for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the share-partition delivery-complete count >= `min`.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_delivery_complete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        min: i32,
    ) {
        let reached = super::await_until(|| async {
            self.share_state_summary_for_test(group, topic_id, partition)
                .await
                .is_some_and(|(_, _, _, dcc)| dcc >= min)
        })
        .await;
        assert2::assert!(
            reached,
            "share dcc for {group}:{topic_id}:{partition} did not reach {min} within 30s"
        );
    }

    /// Test-only: await until the live share-partition has exactly `n` Acquired
    /// in-flight batches. The count rises after a ShareFetch acquires, and it
    /// drops back after lock-timeout redelivery returns records to Available.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_share_acquired_count(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        n: i32,
    ) {
        let reached = super::await_until(|| async {
            match self
                .broker
                .share_partition_leaders
                .peek_for_test(group, topic_id, partition)
            {
                Some(cell) => cell.lock().await.count_acquired_batches() == n,
                None => false,
            }
        })
        .await;
        assert2::assert!(
            reached,
            "share acquired-batch count for {group}:{topic_id}:{partition} did not reach {n} within 30s"
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    #[tokio::test]
    async fn single_broker_handle_share_and_raft_helpers_observe_real_state() {
        let (handle, dir) = crate::test_support::start_broker_with(|_| {}).await;
        let broker = handle.broker_arc_for_test();

        let share_group = "handle-share-summary-mutant-group";
        let share_topic_id = uuid::Uuid::from_u128(0xBEE5);
        let share_partition = 3;
        assert2::assert!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                .is_none()
        );
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
            .await;
        let share_image = crate::share_coordinator::coordinator::test_support::image_with_topic(
            share_topic_id,
            share_partition + 1,
        );
        broker
            .share_coordinator
            .initialize(
                &share_image,
                share_group,
                share_topic_id,
                share_partition,
                11,
                krabka_log::Offset(90),
            )
            .await
            .expect("initialize share state");
        broker
            .share_coordinator
            .read(
                &share_image,
                share_group,
                share_topic_id,
                share_partition,
                2,
            )
            .await
            .expect("raise the stored leader epoch");
        broker
            .share_coordinator
            .write(
                &share_image,
                share_group,
                share_topic_id,
                share_partition,
                crate::share_coordinator::coordinator::test_support::share_write(
                    (11, 2),
                    (95, 7),
                    vec![crate::share_coordinator::persistence::StateBatch {
                        first_offset: krabka_log::Offset(95),
                        last_offset: krabka_log::Offset(99),
                        delivery_state: 0,
                        delivery_count: 1,
                    }],
                ),
            )
            .await
            .expect("write share state summary");
        check!(
            handle
                .share_state_summary_for_test(share_group, share_topic_id, share_partition)
                .await
                == Some((11, 2, 95, 7))
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_spso(share_group, share_topic_id, share_partition, 95),
            )
            .await
            .is_ok()
        );
        check!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_delivery_complete(
                    share_group,
                    share_topic_id,
                    share_partition,
                    7,
                ),
            )
            .await
            .is_ok()
        );

        let acquired_group = "handle-share-acquired-mutant-group";
        let acquired_topic_id = uuid::Uuid::from_u128(0xACCD);
        // The share coordinator refuses a read of a key with no state, so the
        // cell is cached directly. This part checks only the acquired count.
        let acquired_cell = broker.share_partition_leaders.insert_for_test(
            acquired_group,
            acquired_topic_id,
            0,
            crate::share_partition::state::AcquisitionState::new(krabka_log::Offset(0)),
        );
        assert2::assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(75),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_err()
        );
        {
            let mut state = acquired_cell.lock().await;
            state.materialize(krabka_log::Offset(3), 10);
            let acquired = state.acquire(
                "member-1",
                3,
                krabka_log::Offset(i64::MAX),
                std::time::Instant::now(),
                std::time::Duration::from_secs(30),
                i16::MAX,
            );
            assert2::assert!(!acquired.is_empty());
        }
        assert2::assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle.wait_until_share_acquired_count(acquired_group, acquired_topic_id, 0, 1),
            )
            .await
            .is_ok()
        );

        let closed_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed learner port");
        let closed_addr = closed_listener.local_addr().expect("closed learner addr");
        drop(closed_listener);
        let add_learner = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            handle.add_learner(krabka_raft::NodeId(handle.node_id() + 10), closed_addr),
        )
        .await
        .expect("add_learner returned before timeout");
        assert2::assert!(add_learner.is_ok());

        let own_directory = handle
            .voter_directory_id_for_test(krabka_raft::NodeId(handle.node_id()))
            .expect("own voter directory id");
        check!(own_directory != uuid::Uuid::nil());
        check!(
            handle.voter_directory_id_for_test(krabka_raft::NodeId(handle.node_id() + 10_000))
                == None
        );

        // Marking the same log dir offline twice: first succeeds, second is a
        // no-op.
        check!(handle.test_mark_log_dir_offline(dir.path()));
        check!(!handle.test_mark_log_dir_offline(dir.path()));
        handle.shutdown().await;
    }
}
