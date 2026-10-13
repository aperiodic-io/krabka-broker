//! The group-directory operations behind the admin RPCs — `ListGroups`,
//! `DescribeGroups`, and `DeleteGroups` — and the coordinated shutdown that
//! drains every actor.
//!
//! Each one walks the registries and talks to the actors over their mailboxes
//! rather than reading group state directly, which is what separates them from
//! the registry lookups next door.

use {
    super::{
        actor::GroupActorMessage,
        group_coordinator::{GroupCoordinator, GroupType},
        share::actor::ShareGroupActorMessage,
        streams::{self, actor::StreamsGroupActorMessage},
    },
    crate::{
        coordinator::{DeleteGroupError, GroupSnapshot},
        task_util::{ask, cloned_registry_values, shutdown_actor},
        time_util::now_ms,
    },
    std::sync::Arc,
};

impl GroupCoordinator {
    /// Snapshot every **live-classic** group for the wire `ListGroups` pass
    /// that emits `group_type="classic"`.
    ///
    /// The method walks ALL handles and selects on the group's LIVE kind, not
    /// on the spawn-time `handle.kind` hint. A KIP-848 live migration can make
    /// the two differ. The `ClassicInspect` arm replies for a classic-kind
    /// group only, so a consumer group or an upgraded group drops its reply
    /// sender and this method skips it.
    ///
    /// This keeps `list_groups` the only producer of the `classic` rows. The
    /// `ListGroups` handler emits the consumer-kind groups separately through
    /// [`consumer_group_ids`](Self::consumer_group_ids) with the tag
    /// `group_type="consumer"`, so it does NOT count them twice or mislabel
    /// them. A *downgraded* group whose handle still reads `Consumer` still
    /// appears here, because its live kind is `Classic`.
    pub async fn list_groups(&self) -> Vec<GroupSnapshot> {
        let handles = cloned_registry_values(&self.groups);
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            // `ClassicInspect` replies only for a classic-kind group; a
            // consumer-kind group never sends, so the ask errors and we skip.
            if let Ok(view) = ask(&h.tx, |reply| GroupActorMessage::ClassicInspect { reply }).await
            {
                out.push(view.snapshot());
            }
        }
        out
    }

    /// Snapshot a single group, classic OR consumer or migrated, and return
    /// `None` when the group is unknown.
    ///
    /// The method inspects the LIVE group through [`InspectAny`] and does not
    /// gate on the spawn-time `handle.kind`. An upgraded consumer group
    /// therefore still reports.
    ///
    /// [`InspectAny`]: GroupActorMessage::InspectAny
    pub async fn describe_group(&self, group_id: &str) -> Option<GroupSnapshot> {
        let handle = self.find(group_id)?;
        ask(&handle.tx, |reply| GroupActorMessage::InspectAny { reply })
            .await
            .ok()
    }

    /// Delete a **classic**, **consumer**, **streams** or **share** group.
    ///
    /// For a classic or KIP-848 consumer group, the actor atomically verifies
    /// that the group is empty and appends the durable tombstones of its
    /// offsets and its group records before the method removes it from the
    /// registry and drops its seeds, so a later request cannot re-hydrate the
    /// deleted group. The method returns `NonEmpty` when the group still has
    /// live members, as Kafka's `validateDeleteGroup` does for both kinds, and
    /// `NotFound` when the group is unknown.
    /// # Errors
    /// Returns an error when the group is not deletable or the tombstone cannot
    /// be appended.
    pub async fn delete_group(self: &Arc<Self>, group_id: &str) -> Result<(), DeleteGroupError> {
        // KIP-1071: a Streams-locked group is deleted through the streams path —
        // never fall through to the classic path, which would remove the offset-home
        // `groups` entry out from under a live streams group.
        match self.group_type(group_id) {
            Some(GroupType::Streams) => return self.delete_streams_group(group_id).await,
            Some(GroupType::Share) => return self.delete_share_group(group_id).await,
            _ => {}
        }
        let handle = self.find(group_id).ok_or(DeleteGroupError::NotFound)?;
        // The actor serializes this check with Join/Leave so a concurrent join
        // cannot slip between the empty check and the tombstone append.
        ask(&handle.tx, |reply| GroupActorMessage::ClassicDelete {
            reply,
        })
        .await
        .map_err(|_| DeleteGroupError::NotFound)??;
        self.groups.remove(group_id);
        self.group_types.remove(group_id);
        self.seeds.remove(group_id);
        self.remove_cached_seed(group_id);
        self.forget_group_metrics(group_id);
        Ok(())
    }

    /// Delete a **streams** group, per KIP-1071.
    ///
    /// The method returns `NonEmpty` when the streams actor still has live
    /// members. It returns `NotFound` when no streams actor exists for the id.
    /// In every other case it tombstones the group's records k15–21, drops the
    /// streams actor, and removes the offset-home `groups` entry. It returns
    /// `Internal` when the tombstone append fails.
    async fn delete_streams_group(&self, group_id: &str) -> Result<(), DeleteGroupError> {
        // A Streams-locked id with no live streams actor reports NotFound — the
        // safe failure mode (never silently drop an offset home). In practice a
        // live streams group always has an actor (respawned by finalize_bootstrap
        // on replay), so this only guards a genuinely-absent group.
        let handle = self
            .find_streams(group_id)
            .ok_or(DeleteGroupError::NotFound)?;
        let view = ask(&handle.tx, |reply| StreamsGroupActorMessage::Describe {
            reply,
        })
        .await
        .map_err(|_| DeleteGroupError::NotFound)?;
        if !view.members.is_empty() {
            return Err(DeleteGroupError::NonEmpty);
        }
        // Drained group: per-member records (k16/k20/k21) were already tombstoned
        // on member leave/expiry, so only the group-level keys remain.
        let batch = streams::migration::streams_records_tombstone_batch(group_id, &[], now_ms())
            .map_err(|_| DeleteGroupError::Internal)?;
        self.offsets_log
            .append(group_id, batch)
            .await
            .map_err(|_| DeleteGroupError::Internal)?;
        self.streams_groups.remove(group_id);
        self.groups.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.forget_group_metrics(group_id);
        Ok(())
    }

    /// Delete a **share** group, per KIP-932.
    ///
    /// The share actor answers `NonEmpty` when the group has members. For an
    /// empty group it deletes the share state of every initialized partition,
    /// appends the group tombstones, and drops the group seeds. It returns
    /// `NotFound` when the coordinator knows no share group with the id.
    ///
    /// An actor that stopped after a log-write failure is respawned from its
    /// seed first, as `get_or_create_share` does for any other request. On
    /// success the method drops the registry entry and the type lock only while
    /// the entry is still the handle that deleted the group: a heartbeat that
    /// created a new group with the same id right after the delete keeps its
    /// actor.
    async fn delete_share_group(self: &Arc<Self>, group_id: &str) -> Result<(), DeleteGroupError> {
        if self.find_share(group_id).is_none() && self.cached_share_seed(group_id).is_none() {
            return Err(DeleteGroupError::NotFound);
        }
        let handle = self.get_or_create_share(group_id);
        ask(&handle.tx, |reply| ShareGroupActorMessage::Delete { reply })
            .await
            .map_err(|_| DeleteGroupError::NotFound)??;
        if self
            .share_groups
            .remove_if(group_id, |_, registered| Arc::ptr_eq(registered, &handle))
            .is_some()
        {
            self.group_types
                .remove_if(group_id, |_, group_type| *group_type == GroupType::Share);
            self.forget_group_metrics(group_id);
            self.forget_share_group_dlq_metrics(group_id);
        }
        Ok(())
    }

    pub async fn shutdown_all(&self) {
        let handles = cloned_registry_values(&self.groups);
        for h in handles {
            shutdown_actor(
                &h.tx,
                GroupActorMessage::Shutdown,
                self.config.shutdown_ack_timeout,
            )
            .await;
        }
        let share_handles = cloned_registry_values(&self.share_groups);
        for h in share_handles {
            shutdown_actor(
                &h.tx,
                ShareGroupActorMessage::Shutdown,
                self.config.shutdown_ack_timeout,
            )
            .await;
        }
        let streams_handles = cloned_registry_values(&self.streams_groups);
        for h in streams_handles {
            shutdown_actor(
                &h.tx,
                StreamsGroupActorMessage::Shutdown,
                self.config.shutdown_ack_timeout,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use {crate::coordinator::unified::test_support::make_coord, assert2::assert};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_all_closes_all_group_actors() {
        let coord = make_coord();
        let group = coord.get_or_create_classic("classic");
        let share = coord.get_or_create_share("share");
        let streams = coord.get_or_create_streams("streams");

        coord.shutdown_all().await;

        // The reply can precede receiver closure; wait for each channel's
        // notification rather than counting scheduler yields.
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(group.tx.closed(), share.tx.closed(), streams.tx.closed());
        })
        .await
        .expect("all group actor channels closed");
        assert!(group.tx.is_closed());
        assert!(share.tx.is_closed());
        assert!(streams.tx.is_closed());
    }
}
