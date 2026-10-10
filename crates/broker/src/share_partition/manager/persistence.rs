//! The write-back of a dirty acquisition machine to the share coordinator,
//! and the mapping of a persister failure to the error a client gets.
//!
//! A failed write keeps `dirty` set, so the sweeper and the next request
//! retry it. The caller learns the mapped error code, so an acknowledgement
//! that did not become durable can roll back and answer that code, as Kafka's
//! `SharePartition.rollbackOrProcessStateUpdates` does. A fenced partition
//! drops its cell, so the next request reads the state again.

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::warn;

use super::SharePartitionLeaderManager;
use crate::{codes, error::BrokerError, share_partition::state::AcquisitionState};

/// Maps a failed `ReadShareGroupState` or `WriteShareGroupState` call to the
/// error code that the share-partition leader gives the client.
///
/// A partition error code from the coordinator follows Kafka's
/// `SharePartition.fetchPersisterError`. The persister reports a coordinator
/// that it cannot find as `COORDINATOR_NOT_AVAILABLE` too. Any other failure
/// (a transport error, a response with no row for the partition) is
/// `UNKNOWN_SERVER_ERROR`, which is what Kafka's `Errors.forException` gives
/// for the `IllegalStateException` that `SharePartition` raises then.
#[must_use]
pub(crate) fn persister_error_code(error: &BrokerError) -> i16 {
    let BrokerError::SharePartitionState { code, .. } = error else {
        return codes::UNKNOWN_SERVER_ERROR;
    };
    match *code {
        codes::NOT_COORDINATOR
        | codes::COORDINATOR_NOT_AVAILABLE
        | codes::COORDINATOR_LOAD_IN_PROGRESS => codes::COORDINATOR_NOT_AVAILABLE,
        codes::GROUP_ID_NOT_FOUND => codes::GROUP_ID_NOT_FOUND,
        codes::UNKNOWN_TOPIC_OR_PARTITION => codes::UNKNOWN_TOPIC_OR_PARTITION,
        codes::FENCED_LEADER_EPOCH | codes::FENCED_STATE_EPOCH => codes::NOT_LEADER_OR_FOLLOWER,
        _ => codes::UNKNOWN_SERVER_ERROR,
    }
}

/// True when a mapped persister error fences the share partition on this
/// broker. Kafka's `SharePartitionManager.fencedSharePartitionHandler` removes
/// the partition from its cache for these errors.
#[must_use]
pub(crate) fn fences_the_partition(code: i16) -> bool {
    matches!(
        code,
        codes::NOT_LEADER_OR_FOLLOWER
            | codes::GROUP_ID_NOT_FOUND
            | codes::UNKNOWN_TOPIC_OR_PARTITION
    )
}

impl SharePartitionLeaderManager {
    /// Persists `st` if it is dirty, then clears the dirty flag.
    ///
    /// `cell` is the cached cell that holds `st`, or `None` for a state that
    /// is not cached yet.
    ///
    /// # Errors
    ///
    /// Returns the mapped error code ([`persister_error_code`]) when the write
    /// fails. `dirty` then stays set, so a later write retries. When the code
    /// fences the partition, the method also drops `cell` from the cache, so
    /// the next request loads the state again. It drops only that cell: a
    /// replacement that another request already loaded stays cached.
    pub(crate) async fn persist_if_dirty(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        cell: Option<&Arc<Mutex<AcquisitionState>>>,
        st: &mut AcquisitionState,
    ) -> Result<(), i16> {
        if !st.dirty {
            return Ok(());
        }
        let (start, dcc, batches) = st.to_persist_batches();
        let result = self
            .persister
            .write_state(
                group,
                topic_id,
                partition,
                (st.state_epoch, st.leader_epoch),
                (start, dcc),
                batches,
            )
            .await;
        match result {
            Ok(()) => {
                st.dirty = false;
                // The runs that went to `Archiving` are durable now, so their
                // dead-letter writes may start (KIP-1191).
                if let Some(cell) = cell {
                    let key = (group.to_owned(), topic_id, partition);
                    self.dispatch_dead_letters(&key, cell, st.take_pending_dlq());
                }
                Ok(())
            }
            Err(e) => {
                let code = persister_error_code(&e);
                warn!(
                    group,
                    %topic_id, partition, error = %e, code,
                    "share-partition state persist failed"
                );
                if fences_the_partition(code)
                    && let Some(cell) = cell
                {
                    self.invalidate_cell(group, topic_id, partition, cell);
                }
                Err(code)
            }
        }
    }

    /// Runs `apply` on `st` and makes its change durable, or undoes it.
    ///
    /// This is the acknowledgement path. `apply` returns the acknowledge
    /// error code. When `apply` changed the state and the write fails, the
    /// method restores the state that `st` held before `apply`, and returns
    /// the mapped write error in place of the acknowledge error. The client
    /// therefore never gets `NONE` for an acknowledgement that is not durable.
    pub(crate) async fn apply_durably(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        cell: &Arc<Mutex<AcquisitionState>>,
        st: &mut AcquisitionState,
        apply: impl FnOnce(&mut AcquisitionState) -> i16,
    ) -> i16 {
        let before = st.clone();
        let code = apply(st);
        if *st == before {
            return code;
        }
        match self
            .persist_if_dirty(group, topic_id, partition, Some(cell), st)
            .await
        {
            Ok(()) => code,
            Err(write_code) => {
                *st = before;
                write_code
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use assert2::assert;
    use krabka_log::Offset;

    use super::*;
    use crate::share_partition::{
        manager::test_support::{LOCK, manager},
        state::AckType,
    };

    /// Kafka's `SharePartition.fetchPersisterError`, and whether
    /// `SharePartitionManager.fencedSharePartitionHandler` drops the partition.
    #[test]
    fn persister_errors_map_as_kafka_maps_them() {
        let partition_error = |code| BrokerError::SharePartitionState {
            code,
            message: String::new(),
        };
        let cases = [
            (
                partition_error(codes::NOT_COORDINATOR),
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (
                partition_error(codes::COORDINATOR_NOT_AVAILABLE),
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (
                partition_error(codes::COORDINATOR_LOAD_IN_PROGRESS),
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (
                partition_error(codes::GROUP_ID_NOT_FOUND),
                codes::GROUP_ID_NOT_FOUND,
            ),
            (
                partition_error(codes::UNKNOWN_TOPIC_OR_PARTITION),
                codes::UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                partition_error(codes::FENCED_LEADER_EPOCH),
                codes::NOT_LEADER_OR_FOLLOWER,
            ),
            (
                partition_error(codes::FENCED_STATE_EPOCH),
                codes::NOT_LEADER_OR_FOLLOWER,
            ),
            (
                partition_error(codes::INVALID_REQUEST),
                codes::UNKNOWN_SERVER_ERROR,
            ),
            (
                BrokerError::Share("no route".into()),
                codes::UNKNOWN_SERVER_ERROR,
            ),
        ];
        let actual: Vec<_> = cases
            .iter()
            .map(|(error, _)| {
                let code = persister_error_code(error);
                (code, fences_the_partition(code))
            })
            .collect();
        let expected: Vec<_> = cases
            .iter()
            .map(|(_, code)| {
                let fenced = matches!(
                    *code,
                    codes::NOT_LEADER_OR_FOLLOWER
                        | codes::GROUP_ID_NOT_FOUND
                        | codes::UNKNOWN_TOPIC_OR_PARTITION
                );
                (*code, fenced)
            })
            .collect();
        assert!(actual == expected);
    }

    #[tokio::test]
    async fn persist_if_dirty_is_noop_when_clean() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([22; 16]);
        let mut st = AcquisitionState::new(Offset(0));

        let result = mgr.persist_if_dirty("g1", tid, 0, None, &mut st).await;

        assert!((result, st.dirty) == (Ok(()), false));
    }

    #[tokio::test]
    async fn persist_if_dirty_keeps_dirty_on_write_failure() {
        // Over a broker-less image the persister can't bootstrap the
        // share-state topic, so `write_state` answers COORDINATOR_NOT_AVAILABLE. A failed durable write
        // leaves `dirty` set so the sweeper or the next request retries.
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([25; 16]);
        let mut st =
            crate::share_partition::state::test_support::acquired_state(krabka_log::Offset(4));

        let result = mgr.persist_if_dirty("g1", tid, 0, None, &mut st).await;

        assert!((result, st.dirty) == (Err(codes::COORDINATOR_NOT_AVAILABLE), true));
    }

    /// An acknowledgement whose write fails is rolled back, and the write
    /// error replaces `NONE`. An acknowledgement that changes nothing needs no
    /// write and keeps its own error.
    #[tokio::test]
    async fn a_failed_write_rolls_the_acknowledgement_back() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([26; 16]);
        let mut st =
            crate::share_partition::state::test_support::acquired_state(krabka_log::Offset(4));
        let before = st.clone();
        // A cell that the cache does not hold: the write fails without fencing.
        let cell = Arc::new(Mutex::new(before.clone()));

        let accepted = mgr
            .apply_durably("g1", tid, 0, &cell, &mut st, |st| {
                st.acknowledge("m1", Offset(0), Offset(3), AckType::Accept, 5)
                    .err()
                    .unwrap_or(codes::NONE)
            })
            .await;
        let rolled_back = st == before;
        let refused = mgr
            .apply_durably("g1", tid, 0, &cell, &mut st, |st| {
                st.acknowledge("m2", Offset(0), Offset(3), AckType::Accept, 5)
                    .err()
                    .unwrap_or(codes::NONE)
            })
            .await;

        assert!(
            (accepted, rolled_back, refused)
                == (
                    codes::COORDINATOR_NOT_AVAILABLE,
                    true,
                    codes::INVALID_RECORD_STATE
                )
        );
    }

    /// A failed write takes back the records that the acknowledgement made
    /// terminal, as Kafka's `rollbackOrProcessStateUpdates` subtracts them from
    /// `deliveryCompleteCount`, and leaves the terminal records that were
    /// counted before it alone.
    #[tokio::test]
    async fn a_failed_write_rolls_the_delivery_complete_count_back() {
        // (acknowledge type, max attempts, count before the failed write)
        let cases = [
            (AckType::Accept, 5, 1),
            (AckType::Reject, 5, 1),
            (AckType::Release, 1, 1),
        ];
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([27; 16]);
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (ack, max_attempts, counted) in cases {
            // [0] held alone, [1,4] held together, and [4] already accepted, so
            // the SPSO stays at 0 and a count of 1 is in place before the write.
            let mut st = AcquisitionState::new(Offset(0));
            st.materialize(Offset(5), 100);
            let _ = st.acquire("m1", 1, Offset(i64::MAX), Instant::now(), LOCK, 5);
            let _ = st.acquire("m1", 10, Offset(i64::MAX), Instant::now(), LOCK, 5);
            st.acknowledge("m1", Offset(4), Offset(4), AckType::Accept, 5)
                .unwrap();
            let before = st.delivery_complete_count();
            // A cell that the cache does not hold: the write fails without fencing.
            let cell = Arc::new(Mutex::new(st.clone()));

            let code = mgr
                .apply_durably("g1", tid, 0, &cell, &mut st, |st| {
                    st.acknowledge("m1", Offset(1), Offset(3), ack, max_attempts)
                        .err()
                        .unwrap_or(codes::NONE)
                })
                .await;

            actual.push((ack, before, code, st.delivery_complete_count()));
            expected.push((ack, counted, codes::COORDINATOR_NOT_AVAILABLE, counted));
        }
        assert!(actual == expected);
    }
}
