//! The actor registry: the lookups that hand out a group's actor handle, the
//! spawn-on-miss and respawn-on-death paths behind them, and the bootstrap
//! finalize pass that turns replayed seeds into live actors.
//!
//! One registry exists per group protocol, and all of them share the same
//! dead-actor detection and hydrate-from-cache rules, so they are defined
//! together.

use {
    super::{
        actor::{self, GroupActorHandle, GroupActorMessage, GroupKindTag},
        group::CoordinatorGroup,
        group_coordinator::{GroupCoordinator, GroupType},
        share::actor::{ShareGroupActorHandle, ShareGroupActorMessage},
        streams::actor::{StreamsGroupActorHandle, StreamsGroupActorMessage},
    },
    std::sync::Arc,
};

/// The `GROUP_ID_NOT_FOUND` message of a `ConsumerGroupHeartbeat` that names a
/// group that does not exist: Kafka's `getOrMaybeCreateConsumerGroup` says
/// `Consumer group <id> not found.`, and `consumerGroupLeave` reaches the
/// generic `Group <id> not found.` through `consumerGroup`.
pub(crate) fn consumer_group_not_found(group_id: &str, member_epoch: i32) -> String {
    if member_epoch < 0 {
        format!("Group {group_id} not found.")
    } else {
        format!("Consumer group {group_id} not found.")
    }
}

impl GroupCoordinator {
    /// Get the one actor for `group_id`, and spawn it with `initial_kind` when
    /// it is absent.
    ///
    /// The kind argument only decides the spawn kind for a brand-new group.
    /// Both families route to one actor; the actor rejects the family it does
    /// not currently serve and can change kind in place, so a group is not
    /// pinned to its spawn kind. Keeps the dead-actor
    /// (closed tx) respawn and the consumer re-hydrate-from-seed paths.
    #[must_use]
    pub fn get_or_create_group(
        self: &Arc<Self>,
        group_id: &str,
        initial_kind: GroupKindTag,
    ) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            // Dead-actor detection: if the mpsc sender is closed, the actor
            // has exited (typically after a log-write failure). Drop the
            // entry and fall through to spawn a fresh actor.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.groups.remove(group_id);
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            initial_kind,
            self.config.clone(),
            self.metadata.clone(),
            self.offsets_log.clone(),
            self.clone(),
        ));
        let inserted = self
            .groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        // Re-hydrate a respawned consumer actor from its last-known-good state.
        if initial_kind == GroupKindTag::Consumer
            && let Some(seed) = self.cached_seed(group_id)
        {
            let _ = inserted.tx.try_send(GroupActorMessage::Seed(seed));
        }
        inserted
    }

    /// Get or create a classic-protocol actor.
    ///
    /// This method spawns a classic actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_classic(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Classic)
    }

    /// Get or create a next-gen consumer-protocol actor.
    ///
    /// This method spawns a consumer actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_consumer(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Consumer)
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
    }

    #[must_use]
    pub fn get_or_create_share(self: &Arc<Self>, group_id: &str) -> Arc<ShareGroupActorHandle> {
        if let Some(h) = self.share_groups.get(group_id) {
            // Dead-actor detection: a closed mpsc sender means the actor exited
            // (typically after a log-write failure). Drop the entry and respawn.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.share_groups.remove(group_id);
        }
        let h = Arc::new(ShareGroupActorHandle::spawn(
            group_id.into(),
            self.share_config.clone(),
            self.metadata.clone(),
            self.clone(),
        ));
        let inserted = self
            .share_groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_share_seed(group_id) {
            let _ = inserted.tx.try_send(ShareGroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find_share(&self, group_id: &str) -> Option<Arc<ShareGroupActorHandle>> {
        self.share_groups.get(group_id).map(|e| e.value().clone())
    }

    /// Snapshot the ids of every live share group, per KIP-932.
    ///
    /// The call is synchronous and cheap. It reads the registry keys and makes
    /// no actor round-trip. `ListGroups`, `api_key` 16, can therefore include
    /// share groups together with classic ones without the per-group
    /// `Describe` mpsc hop.
    #[must_use]
    pub fn share_group_ids(&self) -> Vec<String> {
        self.share_groups.iter().map(|e| e.key().clone()).collect()
    }

    // ── KIP-1071 streams-group registry ──────────────────────────────────

    #[must_use]
    pub fn get_or_create_streams(self: &Arc<Self>, group_id: &str) -> Arc<StreamsGroupActorHandle> {
        if let Some(h) = self.streams_groups.get(group_id) {
            // Dead-actor detection: a closed mpsc sender means the actor exited
            // (typically after a log-write failure). Drop the entry and respawn.
            if !h.value().tx.is_closed() {
                return h.value().clone();
            }
            drop(h);
            self.streams_groups.remove(group_id);
        }
        let h = Arc::new(StreamsGroupActorHandle::spawn(
            group_id.into(),
            self.streams_config.clone(),
            self.clone(),
        ));
        let inserted = self
            .streams_groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone();
        if let Some(seed) = self.cached_streams_seed(group_id) {
            let _ = inserted.tx.try_send(StreamsGroupActorMessage::Seed(seed));
        }
        inserted
    }

    #[must_use]
    pub fn find_streams(&self, group_id: &str) -> Option<Arc<StreamsGroupActorHandle>> {
        self.streams_groups.get(group_id).map(|e| e.value().clone())
    }

    /// Snapshot the ids of every live streams group, per KIP-1071.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="streams"` entries without a per-group `Describe` hop.
    #[must_use]
    pub fn streams_group_ids(&self) -> Vec<String> {
        self.streams_groups
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    /// Ids of every live next-gen KIP-848 consumer group actor.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="consumer"` entries without an actor round-trip.
    ///
    /// Note: this method returns all group ids from the shared `groups` map,
    /// classic groups included. The `emitted` dedup set in the `ListGroups`
    /// handler prevents a double wire emission. A classic group therefore goes
    /// out once as `group_type="classic"` and not again here.
    pub fn consumer_group_ids(&self) -> Vec<String> {
        self.groups.iter().map(|e| e.key().clone()).collect()
    }

    /// `true` when `group_id` names a share or streams group, which is never a
    /// consumer group.
    #[must_use]
    pub fn is_share_or_streams_group(&self, group_id: &str) -> bool {
        matches!(
            self.group_type(group_id),
            Some(GroupType::Share | GroupType::Streams)
        ) || self.find_share(group_id).is_some()
            || self.find_streams(group_id).is_some()
    }

    /// The `error_message` of the `GROUP_ID_NOT_FOUND` answer to a
    /// `ConsumerGroupHeartbeat` for `group_id`, or `None` when the heartbeat
    /// may go to the consumer-group actor.
    ///
    /// It follows Kafka's `GroupMetadataManager`: `getOrMaybeCreateConsumerGroup`
    /// creates a missing group only when `member_epoch == 0` and refuses a
    /// share or streams group, and `consumerGroupLeave` looks the group up
    /// through `consumerGroup`, which refuses a missing group with the generic
    /// message. A classic group is the actor's to judge, because a join may
    /// convert an empty one.
    #[must_use]
    pub fn consumer_group_lookup_error(&self, group_id: &str, member_epoch: i32) -> Option<String> {
        if self.is_share_or_streams_group(group_id) {
            return Some(format!("Group {group_id} is not a consumer group."));
        }
        (member_epoch != 0 && self.find(group_id).is_none())
            .then(|| consumer_group_not_found(group_id, member_epoch))
    }

    /// Spawn a classic actor seeded with a fully-replayed `Group` at
    /// bootstrap.
    pub fn seed_classic(self: &Arc<Self>, group_id: &str, group: Box<CoordinatorGroup>) {
        let handle = self.get_or_create_classic(group_id);
        let _ = handle.tx.try_send(GroupActorMessage::ClassicSeed(group));
    }

    pub fn finalize_bootstrap(self: &Arc<Self>) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create_consumer(&gid);
                let _ = handle.tx.try_send(actor::GroupActorMessage::Seed(seed));
            }
        }
        let share_ids: Vec<String> = self.share_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in share_ids {
            if let Some((_, seed)) = self.share_seeds.remove(&gid) {
                let handle = self.get_or_create_share(&gid);
                let _ = handle.tx.try_send(ShareGroupActorMessage::Seed(seed));
            }
        }
        let streams_ids: Vec<String> = self.streams_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in streams_ids {
            if let Some((_, seed)) = self.streams_seeds.remove(&gid) {
                let handle = self.get_or_create_streams(&gid);
                let _ = handle.tx.try_send(StreamsGroupActorMessage::Seed(seed));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            actor::MetadataProvider,
            config::NextGenConfig,
            share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
            test_support::{ImageMetadatalessProvider, make_coord},
        },
        assert2::{assert, check},
    };

    #[tokio::test]
    async fn actor_mailboxes_use_component_configuration() {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;

        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        let coord = Arc::new(GroupCoordinator::new(
            NextGenConfig {
                actor_mailbox_capacity: 3,
                ..NextGenConfig::default()
            },
            ShareGroupConfig {
                actor_mailbox_capacity: 5,
                ..ShareGroupConfig::default()
            },
            metadata,
            Arc::new(InMemoryOffsetsLog::default()),
            StreamsGroupConfig {
                actor_mailbox_capacity: 7,
                ..StreamsGroupConfig::default()
            },
        ));

        assert!(coord.get_or_create_classic("classic").tx.max_capacity() == 3);
        assert!(coord.get_or_create_share("share").tx.max_capacity() == 5);
        assert!(coord.get_or_create_streams("streams").tx.max_capacity() == 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_group_returns_the_one_actor_regardless_of_kind() {
        // KIP-848 live migration: BOTH RPC families route to the one actor.
        // The kind argument only decides the spawn kind for a brand-new group;
        // a later request of the other kind returns the SAME actor (the kind
        // lock now lives in the actor's message arms, not in this registry).
        let coord = make_coord();
        let a = coord.get_or_create_group("g", GroupKindTag::Classic);
        let b = coord.get_or_create_group("g", GroupKindTag::Consumer);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_share_is_idempotent() {
        let coord = make_coord();
        let a = coord.get_or_create_share("sg");
        let b = coord.get_or_create_share("sg");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(coord.find_share("sg").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn share_and_streams_registries_report_live_ids() {
        let coord = make_coord();
        let share_a = coord.get_or_create_share("share-a");
        let share_b = coord.get_or_create_share("share-b");
        check!(Arc::ptr_eq(&share_a, &coord.get_or_create_share("share-a")));
        check!(coord.find_share("share-b").is_some());
        check!(!Arc::ptr_eq(&share_a, &share_b));

        let streams_a = coord.get_or_create_streams("streams-a");
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.get_or_create_streams("streams-a")
        ));
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.find_streams("streams-a").unwrap()
        ));

        let mut share_ids = coord.share_group_ids();
        share_ids.sort();
        assert!(share_ids == vec!["share-a".to_string(), "share-b".to_string()]);

        let mut streams_ids = coord.streams_group_ids();
        streams_ids.sort();
        assert!(streams_ids == vec!["streams-a".to_string()]);

        coord.shutdown_all().await;
    }
}
