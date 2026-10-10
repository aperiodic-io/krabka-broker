//! The share-session lifecycle: the fetch and acknowledge epoch bumps, and the
//! release path that a member's disconnect takes.
//!
//! Session bookkeeping is a thin pass-through to [`ShareSessionCache`], but the
//! release path is not: a closing session gives its acquired records back to
//! the group, which is the one place session state reaches into the
//! acquisition machines. The two sit together because a disconnect runs both.

use std::collections::HashSet;

use super::SharePartitionLeaderManager;
use crate::share_partition::{
    group_settings::GroupShareSettings,
    session::{FetchPartitions, ResponseRow, ShareFetchSessionUpdate, SharePartitionKey},
};

impl SharePartitionLeaderManager {
    /// Kafka's `SharePartitionManager.newContext`: see
    /// [`ShareSessionCache::update_fetch`](crate::share_partition::session::ShareSessionCache::update_fetch).
    pub(crate) fn update_fetch_session(
        &self,
        (group, member): (&str, &str),
        connection_id: &str,
        epoch: i32,
        partitions: FetchPartitions<'_>,
        has_acknowledgements: bool,
    ) -> Result<ShareFetchSessionUpdate, i16> {
        self.sessions.update_fetch(
            (group, member),
            connection_id,
            epoch,
            partitions,
            has_acknowledgements,
        )
    }

    /// Which rows an incremental `ShareFetch` response carries: see
    /// [`ShareSessionCache::prune_response`](crate::share_partition::session::ShareSessionCache::prune_response).
    pub(crate) fn prune_fetch_response(
        &self,
        group: &str,
        member: &str,
        rows: &[ResponseRow],
    ) -> Vec<bool> {
        self.sessions.prune_response(group, member, rows)
    }

    pub(crate) fn update_acknowledge_session(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<HashSet<SharePartitionKey>, i16> {
        self.sessions.update_acknowledge(group, member, epoch)
    }

    /// Releases all acquisitions owned by `member` in the supplied session
    /// partitions. Only already-loaded cells can contain live acquisitions;
    /// durable state stores acquired records as available.
    pub(crate) async fn release_session_partitions(
        &self,
        group: &str,
        member: &str,
        partitions: &HashSet<SharePartitionKey>,
    ) {
        let settings =
            GroupShareSettings::resolve(&self.controller.current_image(), group, &self.config);
        for &(topic_id, partition) in partitions {
            let cell = self
                .leaders
                .get(&(group.to_string(), topic_id, partition))
                .map(|entry| entry.value().clone());
            let Some(cell) = cell else {
                continue;
            };
            let mut state = cell.lock().await;
            state.set_dlq_enabled(settings.dlq_enabled);
            state.release_member(member, settings.delivery_count_limit);
            // Best-effort: a failed write keeps the state dirty for a retry.
            let _ = self
                .persist_if_dirty(group, topic_id, partition, Some(&cell), &mut state)
                .await;
        }
    }

    /// Closes the share session tied to a disconnected client and releases
    /// its outstanding acquisitions.
    pub(crate) async fn release_connection(&self, connection_id: &str) {
        let Some(session) = self.sessions.disconnect(connection_id) else {
            return;
        };
        self.release_session_partitions(&session.group, &session.member, &session.partitions)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::assert;
    use krabka_log::Offset;

    use crate::share_partition::{
        manager::test_support::{LOCK, manager, manager_with_unlimited_fallback},
        session::FetchPartitions,
    };

    #[test]
    fn nondefault_unlimited_fallback_bounds_sessions() {
        let manager = manager_with_unlimited_fallback(
            crate::share_partition::manager::test_support::SessionCapacity(2),
        );
        let partitions = HashSet::new();

        assert!(
            manager
                .update_fetch_session(
                    ("g", "m1"),
                    "connection-1",
                    0,
                    FetchPartitions {
                        requested: &[],
                        forgotten: &partitions,
                    },
                    false,
                )
                .is_ok()
        );
        assert!(
            manager
                .update_fetch_session(
                    ("g", "m2"),
                    "connection-2",
                    0,
                    FetchPartitions {
                        requested: &[],
                        forgotten: &partitions,
                    },
                    false,
                )
                .is_ok()
        );
        assert!(
            manager.update_fetch_session(
                ("g", "m3"),
                "connection-3",
                0,
                FetchPartitions {
                    requested: &[],
                    forgotten: &partitions,
                },
                false,
            ) == Err(crate::codes::SHARE_SESSION_LIMIT_REACHED)
        );
    }

    #[tokio::test]
    async fn disconnect_releases_the_sessions_acquired_records() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([27; 16]);
        let cell = mgr.insert_for_test(
            "g1",
            tid,
            0,
            crate::share_partition::state::AcquisitionState::new(Offset(0)),
        );
        {
            let mut state = cell.lock().await;
            state.materialize(Offset(1), 100);
            let acquired = state.acquire(
                "m1",
                1,
                krabka_log::Offset(i64::MAX),
                std::time::Instant::now(),
                LOCK,
                5,
            );
            assert!(acquired.len() == 1);
        }
        mgr.update_fetch_session(
            ("g1", "m1"),
            "connection-1",
            0,
            FetchPartitions {
                requested: &[(tid, 0)],
                forgotten: &HashSet::new(),
            },
            false,
        )
        .expect("open session");

        mgr.release_connection("connection-1").await;

        let mut state = cell.lock().await;
        let redelivered = state.acquire(
            "m2",
            1,
            krabka_log::Offset(i64::MAX),
            std::time::Instant::now(),
            LOCK,
            5,
        );
        assert!(redelivered.len() == 1);
        assert!(redelivered[0].delivery_count == 2);
    }
}
