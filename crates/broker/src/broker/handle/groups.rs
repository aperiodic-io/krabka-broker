//! Test-only [`BrokerHandle`] helpers that inspect the live group coordinator:
//! consumer, classic, and streams group views, the group-type marker, and the
//! awaiters that block until an actor settles at an expected membership.

use crate::broker::{BrokerHandle, TEST_AWAITER_TIMEOUT};

impl BrokerHandle {
    async fn inspect_group_actor<T>(
        &self,
        group_id: &str,
        message: impl FnOnce(
            tokio::sync::oneshot::Sender<T>,
        ) -> crate::coordinator::unified::actor::GroupActorMessage,
    ) -> Option<T> {
        let handle = self
            .broker
            .group_coordinator
            .groups
            .get(group_id)?
            .value()
            .clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle.tx.send(message(tx)).await.ok()?;
        rx.await.ok()
    }

    // ── consumer/streams/share group awaiters ─────────────────────────────────

    /// Test-only: describe a consumer/share/streams group via its actor.
    /// `None` if the group has no live actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn group_describe_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::actor::DescribeView> {
        self.inspect_group_actor(group_id, |reply| {
            crate::coordinator::unified::actor::GroupActorMessage::Describe { reply }
        })
        .await
    }

    /// Test-only: await until the group has exactly `n` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_group_member_count(&self, group_id: &str, n: usize) {
        let settled = super::await_until(|| async {
            self.group_describe_for_test(group_id)
                .await
                .map_or(0, |v| v.members.len())
                == n
        })
        .await;
        assert2::assert!(
            settled,
            "group {group_id} did not settle at {n} members within 30s"
        );
    }

    /// Test-only: await until the group is empty/drained (no members).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_group_empty(&self, group_id: &str) {
        self.wait_until_group_member_count(group_id, 0).await;
    }

    /// Test-only: describe a **classic**-protocol group via `ClassicInspect`.
    /// Returns `None` when no actor exists or the actor is consumer-kind (not classic).
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn classic_group_inspect_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::actor::ClassicView> {
        self.inspect_group_actor(group_id, |reply| {
            crate::coordinator::unified::actor::GroupActorMessage::ClassicInspect { reply }
        })
        .await
    }

    /// Test-only: await until the classic group has exactly `n` live members
    /// (i.e., they have been registered in the actor via `ClassicJoin`).
    ///
    /// Use this rather than `wait_until_group_member_count` for classic-protocol
    /// groups, because the next-gen `Describe` message is a no-op on a
    /// classic-kind actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_classic_group_member_count(&self, group_id: &str, n: usize) {
        let settled = super::await_until(|| async {
            self.classic_group_inspect_for_test(group_id)
                .await
                .map_or(0, |v| v.members.len())
                == n
        })
        .await;
        assert2::assert!(
            settled,
            "classic group {group_id} did not settle at {n} members within 30s"
        );
    }

    // ── streams-group awaiters ────────────────────────────────────────────────

    /// Test-only: describe a streams group via its actor.
    /// `None` if the group has no live streams actor.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn streams_group_describe_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::streams::actor::StreamsDescribeView> {
        let handle = self
            .broker
            .group_coordinator
            .streams_groups
            .get(group_id)?
            .value()
            .clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(
                crate::coordinator::unified::streams::actor::StreamsGroupActorMessage::Describe {
                    reply: tx,
                },
            )
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Test-only: await until the streams group has exactly `n` members.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_streams_group_member_count(&self, group_id: &str, n: usize) {
        let settled = super::await_until(|| async {
            self.streams_group_describe_for_test(group_id)
                .await
                .map_or(0, |v| v.members.len())
                == n
        })
        .await;
        assert2::assert!(
            settled,
            "streams group {group_id} did not settle at {n} members within 30s"
        );
    }

    /// Test-only: await until the streams group is empty/drained (no members).
    ///
    /// The downgrade integration tests call this after a `streams_leave()`
    /// heartbeat instead of a fixed-duration `tokio::time::sleep`. The test must
    /// make sure the leave has propagated through the streams actor before it
    /// sends the classic `JoinGroup` that triggers the streams→classic conversion.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_streams_group_empty(&self, group_id: &str) {
        self.wait_until_streams_group_member_count(group_id, 0)
            .await;
    }

    /// Test-only: insert a classic group into this broker's
    /// `GroupCoordinator`. Returns immediately if the group already exists
    /// (idempotent). Used by admin-handler integration tests to seed the group
    /// registry without running a full `JoinGroup` / `SyncGroup` exchange.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn group_create_for_test(&self, group_id: &str) {
        let _ = self
            .broker
            .group_coordinator
            .get_or_create_classic(group_id);
    }

    /// Test-only: return the locked `GroupType` for `group_id`, if any.
    /// Integration tests use this to assert a group has been flagged as
    /// Classic (after `JoinGroup`) or converted to Streams (after a
    /// `StreamsGroupHeartbeat` on a drained classic group).
    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub fn group_type_for_test(
        &self,
        group_id: &str,
    ) -> Option<crate::coordinator::unified::GroupType> {
        self.broker.group_coordinator.group_type(group_id)
    }

    /// Test-only: await until the coordinator's group-type lock for `group_id`
    /// reaches `expected`. Use this instead of an immediate assertion after a
    /// protocol request. Such requests enqueue actor work and then persist the
    /// classic/streams type marker asynchronously.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-helpers"))]
    pub async fn wait_until_group_type(
        &self,
        group_id: &str,
        expected: crate::coordinator::unified::GroupType,
    ) {
        let settled =
            super::await_until(|| async { self.group_type_for_test(group_id) == Some(expected) })
                .await;
        assert2::assert!(
            settled,
            "group {group_id} did not settle at type {expected:?} within {TEST_AWAITER_TIMEOUT:?}; \
             last={:?}",
            self.group_type_for_test(group_id)
        );
    }
}

#[cfg(test)]
mod tests;
