//! KIP-932 share-group membership state machine.
//!
//! It mirrors the consumer next-gen `consumer_state::GroupState` without any
//! offset or revocation machinery. Share-group assignment is non-exclusive,
//! so members carry no assignment-ack state beyond a member epoch.

use {
    super::super::INITIAL_GROUP_EPOCH,
    crate::{
        codes::{FENCED_MEMBER_EPOCH, UNKNOWN_MEMBER_ID},
        coordinator::unified::{
            actor::MetadataProvider, assignor::TopicIdPartition,
            member_helpers::new_target_assignment,
        },
    },
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{HashMap, HashSet},
        time::Instant,
    },
};

/// The name and partition count of a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedTopic {
    pub name: String,
    pub partition_count: i32,
}

/// The id and partition count of a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentifiedTopic {
    pub topic_id: uuid::Uuid,
    pub partition_count: i32,
}

/// The partitions of one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitionList {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

/// A topic whose share state a group delete removes: its name, and the
/// partitions to delete.
struct DeleteCandidate {
    name: String,
    partitions: HashSet<i32>,
}

/// A share partition of `topic_id`.
#[cfg(test)]
pub(crate) fn share_partition(topic_id: Uuid, partition: i32) -> TopicIdPartition {
    TopicIdPartition {
        topic_id,
        partition,
    }
}

/// One member of a share group.
#[derive(Debug, Clone)]
pub struct ShareMemberState {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    pub member_epoch: i32,
    /// The member epoch before the last bump, or `-1` before the first one.
    /// A heartbeat that carries it is accepted, because the response with the
    /// bumped epoch may have been lost (Kafka's
    /// `ShareGroupMember.previousMemberEpoch`).
    pub previous_member_epoch: i32,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
}

impl ShareMemberState {
    /// Construct a freshly joining member at epoch 0 with no assignment.
    pub fn joining(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
        subs: HashSet<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            rack_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: subs,
            member_epoch: 0,
            previous_member_epoch: -1,
            assigned_partitions: HashMap::new(),
            last_seen: Instant::now(),
        }
    }
}

/// The target assignment computed by the most recent reconcile.
#[derive(Debug, Clone)]
pub struct ShareTargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}

/// Full membership state of a single share group.
#[derive(Debug, Clone, krabka_macros::GroupLifecycle)]
#[group_lifecycle(protocol = "share")]
pub struct ShareGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, ShareMemberState>,
    pub target: ShareTargetAssignment,
    /// KIP-932: `(topic_id, partition)` share-states this
    /// group has already Initialized in the share-state persister. Seeded from
    /// the replayed `ShareGroupStatePartitionMetadata` (key v15) so a
    /// post-restart heartbeat does not re-Initialize. The lifecycle hook adds
    /// to this on each successful `SharePersister::initialize`.
    pub initialized: HashSet<TopicIdPartition>,
    /// KIP-932: `(topic_id, partition)` share-states whose `Initialize` the
    /// group has recorded but the persister has not yet confirmed, each with
    /// the time in milliseconds it was recorded. Kafka's
    /// `ShareGroupStatePartitionMetadata.InitializingTopics`: the group writes
    /// a partition here before it calls the persister, so a group delete
    /// never misses state that an initialize may have written. An entry older
    /// than the retry interval is initialized again.
    pub initializing: HashMap<TopicIdPartition, i64>,
    /// KIP-932: the topics whose share state the group is deleting, each with
    /// the topic name the group writes for it. Kafka's
    /// `ShareGroupStatePartitionMetadata.DeletingTopics`: `DeleteShareGroupOffsets`
    /// and `DeleteGroups` move a topic here before they call the persister,
    /// and a later retry of either deletes it again.
    pub deleting: HashMap<Uuid, String>,
    /// Kafka's `ModernGroup.metadataHash`: the `topic_hash` group hash of
    /// the subscribed topics as the group last recorded it in
    /// `ShareGroupMetadataValue`. A heartbeat that computes another value
    /// bumps the group epoch.
    pub metadata_hash: i64,
    /// The topic name behind each topic id in [`Self::initialized`] and
    /// [`Self::initializing`].
    ///
    /// KIP-932's `ShareGroupStatePartitionMetadata` carries `TopicName` beside
    /// `TopicId` on every entry, and the tooling that reads that record has no
    /// topic-id index of its own, so the name has to live as long as the entry
    /// does. It is learned from the metadata image when the lifecycle hook
    /// initializes a partition, and re-seeded from the replayed record after a
    /// restart, exactly as Kafka's `GroupMetadataManager` keeps
    /// `InitMapValue.name()`. An id with no name here is written as
    /// [`UNKNOWN_TOPIC_NAME`], the same fallback Kafka writes for an id its
    /// metadata image no longer holds.
    ///
    /// [`UNKNOWN_TOPIC_NAME`]: super::persistence::UNKNOWN_TOPIC_NAME
    pub topic_names: HashMap<Uuid, String>,
    /// Kafka's `ShareGroup.assignmentTimestamp`: the wall-clock time in
    /// milliseconds at which the last target assignment calculation
    /// finished, or 0 when there is no previous assignment or its time is
    /// unknown. It is the `AssignmentTimestamp` of the group's target
    /// assignment metadata record.
    pub assignment_timestamp_ms: i64,
}

impl ShareGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: INITIAL_GROUP_EPOCH,
            members: HashMap::new(),
            target: ShareTargetAssignment {
                epoch: INITIAL_GROUP_EPOCH,
                per_member: HashMap::new(),
            },
            initialized: HashSet::new(),
            initializing: HashMap::new(),
            deleting: HashMap::new(),
            metadata_hash: 0,
            topic_names: HashMap::new(),
            assignment_timestamp_ms: 0,
        }
    }

    /// Records that the persister initialized `tp`, as Kafka's
    /// `initializeShareGroupState` moves it from initializing to initialized.
    pub fn mark_initialized(&mut self, tp: TopicIdPartition) {
        self.initializing.remove(&tp);
        self.initialized.insert(tp);
    }

    /// Kafka's `GroupMetadataManager.addInitializingTopicsRecords`: every
    /// partition of `partitions` becomes initializing at `now_ms`, each of
    /// their topics leaves the deleting set, and `names` gives the name of
    /// each topic it knows.
    ///
    /// Kafka keeps one timestamp per topic (`InitMapValue.timestamp`), and
    /// `combineInitMaps` replaces it with the new one, so a partition of the
    /// same topic that was already initializing takes `now_ms` too. A
    /// partition that is also initialized stays initialized.
    pub fn add_initializing(
        &mut self,
        partitions: &[TopicIdPartition],
        names: &HashMap<Uuid, String>,
        now_ms: i64,
    ) {
        if partitions.is_empty() {
            return;
        }
        let topics: HashSet<Uuid> = partitions.iter().map(|tp| tp.topic_id).collect();
        for (tp, at) in &mut self.initializing {
            if topics.contains(&tp.topic_id) {
                *at = now_ms;
            }
        }
        for tp in partitions {
            self.initializing.insert(*tp, now_ms);
        }
        for topic_id in &topics {
            self.deleting.remove(topic_id);
            if let Some(name) = names.get(topic_id) {
                self.topic_names.insert(*topic_id, name.clone());
            }
        }
    }

    /// Kafka's `GroupMetadataManager.uninitializeShareGroupState`: the
    /// partitions of `partitions` leave the initializing set, and the
    /// initialized and deleting sets stay as they are.
    pub fn uninitialize(&mut self, partitions: &[TopicIdPartition]) {
        for tp in partitions {
            self.initializing.remove(tp);
        }
    }

    /// Kafka's `GroupMetadataManager.maybeCleanupShareGroupState` for one
    /// group: the topics of `deleted` leave the initializing, initialized and
    /// deleting sets. It returns whether any set changed, which is when Kafka
    /// writes a new `ShareGroupStatePartitionMetadata` record. The share
    /// coordinator removes the share state of a deleted topic on its own, so
    /// nothing here calls the persister.
    pub fn cleanup_deleted_topics(&mut self, deleted: &HashSet<Uuid>) -> bool {
        let before = (
            self.initializing.len(),
            self.initialized.len(),
            self.deleting.len(),
        );
        self.initializing
            .retain(|tp, _| !deleted.contains(&tp.topic_id));
        self.initialized
            .retain(|tp| !deleted.contains(&tp.topic_id));
        self.deleting
            .retain(|topic_id, _| !deleted.contains(topic_id));
        let changed = before
            != (
                self.initializing.len(),
                self.initialized.len(),
                self.deleting.len(),
            );
        if changed {
            self.forget_unused_topic_names();
        }
        changed
    }

    /// The topic ids the group's `ShareGroupStatePartitionMetadata` names in
    /// any of its three sets.
    #[must_use]
    pub fn state_topic_ids(&self) -> HashSet<Uuid> {
        self.initialized
            .iter()
            .chain(self.initializing.keys())
            .map(|tp| tp.topic_id)
            .chain(self.deleting.keys().copied())
            .collect()
    }

    /// Whether the group holds a `ShareGroupStatePartitionMetadata` record
    /// that names any topic, the approximation of Kafka's
    /// `shareGroupStatePartitionMetadata.containsKey(groupId)` that this
    /// state can answer: a replayed record with three empty sets reads as no
    /// record.
    #[must_use]
    pub fn has_state_partition_metadata(&self) -> bool {
        !(self.initialized.is_empty() && self.initializing.is_empty() && self.deleting.is_empty())
    }

    /// Kafka's `GroupMetadataManager.sharePartitionsEligibleForOffsetDeletion`
    /// for one requested topic that the metadata image holds as `topic_name`
    /// with `partition_count` partitions.
    ///
    /// A topic with initialized partitions moves from the initialized set to
    /// the deleting set, and the persister deletes those partitions. A topic
    /// that is already deleting is deleted again, every partition of it. Any
    /// other topic has no offset information to delete, and the method
    /// returns `None`. The initializing set stays as it is.
    pub fn mark_topic_deleting(
        &mut self,
        topic_id: Uuid,
        topic_name: &str,
        partition_count: i32,
    ) -> Option<Vec<i32>> {
        let mut initialized: Vec<i32> = self
            .initialized
            .iter()
            .filter(|tp| tp.topic_id == topic_id)
            .map(|tp| tp.partition)
            .collect();
        if !initialized.is_empty() {
            initialized.sort_unstable();
            self.initialized.retain(|tp| tp.topic_id != topic_id);
            self.deleting.insert(topic_id, topic_name.to_owned());
            self.forget_unused_topic_names();
            return Some(initialized);
        }
        if let Some(name) = self.deleting.get_mut(&topic_id) {
            topic_name.clone_into(name);
            return Some((0..partition_count).collect());
        }
        None
    }

    /// Kafka's `GroupMetadataManager.completeDeleteShareGroupOffsets`: the
    /// topics whose share state the persister deleted leave the deleting set.
    pub fn complete_deleting(&mut self, topics: &[Uuid]) {
        for topic_id in topics {
            self.deleting.remove(topic_id);
        }
    }

    /// Kafka's `GroupMetadataManager.shareGroupBuildPartitionDeleteRequest`
    /// for `DeleteGroups`: every initialized and initializing partition, and
    /// every partition of a topic that is still deleting and that the
    /// metadata image holds, moves to the deleting set, which afterwards
    /// names exactly those topics. `image` answers a topic's name and
    /// partition count.
    ///
    /// It returns the partitions whose share state the persister deletes, per
    /// topic, sorted.
    pub fn move_all_to_deleting(
        &mut self,
        image: impl Fn(&Uuid) -> Option<NamedTopic>,
    ) -> Vec<TopicPartitionList> {
        let mut candidates: HashMap<Uuid, DeleteCandidate> = HashMap::new();
        for tp in self.initialized.iter().chain(self.initializing.keys()) {
            let name = self
                .topic_names
                .get(&tp.topic_id)
                .cloned()
                .unwrap_or_else(|| super::persistence::UNKNOWN_TOPIC_NAME.to_owned());
            candidates
                .entry(tp.topic_id)
                .or_insert_with(|| DeleteCandidate {
                    name,
                    partitions: HashSet::new(),
                })
                .partitions
                .insert(tp.partition);
        }
        // A deleting topic that the image no longer holds is dropped: the
        // share coordinator removed its state when the topic went.
        for topic_id in self.deleting.keys() {
            if let Some(NamedTopic {
                name,
                partition_count,
            }) = image(topic_id)
            {
                let entry = candidates
                    .entry(*topic_id)
                    .or_insert_with(|| DeleteCandidate {
                        name: name.clone(),
                        partitions: HashSet::new(),
                    });
                entry.name = name;
                entry.partitions.extend(0..partition_count);
            }
        }
        self.initialized.clear();
        self.initializing.clear();
        self.deleting = candidates
            .iter()
            .map(|(topic_id, candidate)| (*topic_id, candidate.name.clone()))
            .collect();
        self.forget_unused_topic_names();
        let mut out: Vec<TopicPartitionList> = candidates
            .into_iter()
            .map(|(topic_id, candidate)| {
                let mut partitions: Vec<i32> = candidate.partitions.into_iter().collect();
                partitions.sort_unstable();
                TopicPartitionList {
                    topic_id,
                    partitions,
                }
            })
            .collect();
        out.sort_unstable_by_key(|list| list.topic_id.0);
        out
    }

    /// Drop the name of every topic that no longer has an initialized or
    /// initializing partition, so the map stays exactly the set of topics the
    /// next `ShareGroupStatePartitionMetadata` record will name.
    pub fn forget_unused_topic_names(&mut self) {
        let Self {
            initialized,
            initializing,
            topic_names,
            ..
        } = self;
        // The deleting set carries its own names.
        let live: HashSet<Uuid> = initialized
            .iter()
            .chain(initializing.keys())
            .map(|tp| tp.topic_id)
            .collect();
        topic_names.retain(|topic_id, _| live.contains(topic_id));
    }

    pub fn add_or_update_member(&mut self, m: ShareMemberState) {
        self.members.insert(m.member_id.clone(), m);
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<ShareMemberState> {
        self.members.remove(member_id)
    }

    /// Validates the `member_epoch` of a heartbeat from `member_id`, as Kafka's
    /// `throwIfShareGroupMemberEpochIsInvalid` does, and returns the member
    /// epoch that the group holds.
    ///
    /// Epoch 0 from a known member is a rejoin. The member epoch and the
    /// previous member epoch are accepted. Any other epoch gets
    /// `FENCED_MEMBER_EPOCH`. An unknown member gets `UNKNOWN_MEMBER_ID`.
    /// This API never answers `STALE_MEMBER_EPOCH`: the share consumer treats
    /// it as fatal.
    ///
    /// # Errors
    ///
    /// Returns the wire error code of a refused heartbeat.
    pub fn validate_member_epoch(&self, member_id: &str, requested_epoch: i32) -> Result<i32, i16> {
        let Some(member) = self.members.get(member_id) else {
            return Err(UNKNOWN_MEMBER_ID);
        };
        if requested_epoch == 0
            || requested_epoch == member.member_epoch
            || requested_epoch == member.previous_member_epoch
        {
            Ok(member.member_epoch)
        } else {
            Err(FENCED_MEMBER_EPOCH)
        }
    }

    /// Install a freshly computed target assignment stamped with the current
    /// group epoch, and record that the calculation finished at `now_ms`, the
    /// wall-clock time that Kafka's `TargetAssignmentBuilder` writes as the
    /// record's `AssignmentTimestamp`.
    ///
    /// Every member gets a target, an empty one when the assignor gave it
    /// nothing, as Kafka's `TargetAssignmentBuilder.newMemberAssignment` does.
    /// It returns the members whose target differs from the one they held, a
    /// member that held none included, sorted: the members for which Kafka's
    /// builder writes a target assignment record.
    pub fn install_target(
        &mut self,
        per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
        now_ms: i64,
    ) -> Vec<String> {
        let (target, changed) =
            new_target_assignment(self.members.keys(), per_member, &self.target.per_member);
        self.target = ShareTargetAssignment {
            epoch: self.group_epoch,
            per_member: target,
        };
        self.assignment_timestamp_ms = now_ms;
        changed
    }

    /// Kafka's `GroupMetadataManager.maybeReconcile` with
    /// `ShareGroupAssignmentBuilder` for `member_id`: a member behind the
    /// target epoch moves to it, stable, with its target filtered to the
    /// topics it subscribes to; a member at the target epoch whose
    /// subscription changed in this heartbeat gets its filtered target; any
    /// other member stays as it is. `metadata` names the topics of the target.
    pub fn reconcile_member(
        &mut self,
        member_id: &str,
        has_subscription_changed: bool,
        metadata: &dyn MetadataProvider,
    ) {
        let target_epoch = self.target.epoch;
        let target = self
            .target
            .per_member
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        let Some(m) = self.members.get_mut(member_id) else {
            return;
        };
        if m.member_epoch == target_epoch && !has_subscription_changed {
            return;
        }
        let filtered: HashMap<Uuid, Vec<i32>> = target
            .into_iter()
            .filter(|(topic_id, _)| {
                metadata
                    .topic_name(topic_id)
                    .is_some_and(|name| m.subscribed_topic_names.contains(&name))
            })
            .collect();
        m.assigned_partitions = filtered;
        if m.member_epoch != target_epoch {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = target_epoch;
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::test_support::assert_group_epoch_exhaustion,
        assert2::assert,
        std::time::{Duration, Instant},
    };

    #[test]
    fn add_member_bumps_nothing() {
        let mut g = ShareGroupState::new("g1");
        // Kafka's `ModernGroup` starts at group epoch 1 and
        // `TargetAssignmentMetadata.INITIAL` at assignment epoch 1.
        assert!((g.group_epoch, g.target.epoch) == (1, 1));
        g.add_or_update_member(ShareMemberState::joining(
            "m1",
            "c1",
            "h1",
            ["t1".to_string()].into_iter().collect(),
        ));
        assert!(g.members.len() == 1);
        assert!((g.group_epoch, g.target.epoch) == (1, 1));
    }

    #[test]
    fn bump_epoch_rejects_exhaustion() {
        assert_group_epoch_exhaustion(
            ShareGroupState::new("g"),
            |group| group.group_epoch = i32::MAX,
            ShareGroupState::bump_epoch,
            |group| group.group_epoch,
        );
    }

    #[test]
    fn evict_expired_removes_silent_members() {
        let mut g = ShareGroupState::new("g1");
        let mut m = ShareMemberState::joining("m1", "c1", "h1", HashSet::default());
        // `last_seen` is "now"; evaluating eviction at a point slightly in the
        // future with a tiny session timeout makes the member overdue without
        // ever subtracting from an `Instant` (which underflows on low-uptime CI).
        m.last_seen = Instant::now();
        g.add_or_update_member(m);

        // A member seen within the session timeout is retained.
        let recent = Instant::now() + Duration::from_millis(50);
        let kept = g.evict_expired(recent, Duration::from_secs(45));
        assert!(kept.is_empty());
        assert!(g.members.len() == 1);

        // The same member is overdue once the timeout shrinks below its silence.
        let later = Instant::now() + Duration::from_millis(50);
        let evicted = g.evict_expired(later, Duration::from_millis(1));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.is_empty());
    }

    #[test]
    fn forgetting_names_keeps_exactly_the_initialized_topics() {
        let kept = Uuid([1; 16]);
        let dropped = Uuid([2; 16]);
        let mut g = ShareGroupState::new("g1");
        g.initialized.insert(share_partition(kept, 0));
        g.topic_names.insert(kept, "orders".to_owned());
        g.topic_names.insert(dropped, "carts".to_owned());

        g.forget_unused_topic_names();

        assert!(g.topic_names == HashMap::from([(kept, "orders".to_owned())]));
    }

    /// Kafka's `addInitializingTopicsRecords`: the new partitions are
    /// initializing at the new time, an initializing partition of the same
    /// topic takes that time too (Kafka keeps one timestamp per topic), and
    /// the topic leaves the deleting set. Another topic keeps its time.
    #[test]
    fn adding_initializing_partitions_refreshes_their_topic() {
        let orders = Uuid([1; 16]);
        let carts = Uuid([2; 16]);
        let mut g = ShareGroupState::new("g1");
        g.initializing.insert(share_partition(orders, 0), 10);
        g.initializing.insert(share_partition(carts, 0), 10);
        g.initialized.insert(share_partition(orders, 1));
        g.deleting.insert(orders, "orders".to_owned());
        g.deleting.insert(carts, "carts".to_owned());

        g.add_initializing(
            &[share_partition(orders, 1), share_partition(orders, 2)],
            &HashMap::from([(orders, "orders".to_owned())]),
            99,
        );

        assert!(
            g.initializing
                == HashMap::from([
                    (share_partition(orders, 0), 99),
                    (share_partition(orders, 1), 99),
                    (share_partition(orders, 2), 99),
                    (share_partition(carts, 0), 10),
                ])
        );
        assert!(g.initialized == HashSet::from([share_partition(orders, 1)]));
        assert!(g.deleting == HashMap::from([(carts, "carts".to_owned())]));
        assert!(g.topic_names == HashMap::from([(orders, "orders".to_owned())]));
    }

    /// Kafka's `sharePartitionsEligibleForOffsetDeletion`, per topic: the
    /// initialized partitions move to deleting, a deleting topic is deleted
    /// again across all its partitions, and any other topic has nothing to
    /// delete. The initializing set is never touched.
    #[test]
    fn offset_deletion_moves_initialized_topics_to_deleting() {
        struct Row {
            name: &'static str,
            initialized: Vec<i32>,
            is_deleting: bool,
            expected: Option<Vec<i32>>,
            after_initialized: HashSet<TopicIdPartition>,
            after_deleting: HashMap<Uuid, String>,
        }
        let orders = Uuid([1; 16]);
        let deleting = || HashMap::from([(orders, "orders".to_owned())]);
        let rows: [Row; 3] = [
            Row {
                name: "initialized partitions",
                initialized: vec![2, 0],
                is_deleting: false,
                expected: Some(vec![0, 2]),
                after_initialized: HashSet::new(),
                after_deleting: deleting(),
            },
            Row {
                name: "a retry of a deleting topic",
                initialized: vec![],
                is_deleting: true,
                expected: Some(vec![0, 1, 2]),
                after_initialized: HashSet::new(),
                after_deleting: deleting(),
            },
            Row {
                name: "nothing to delete",
                initialized: vec![],
                is_deleting: false,
                expected: None,
                after_initialized: HashSet::new(),
                after_deleting: HashMap::new(),
            },
        ];
        for Row {
            name,
            initialized,
            is_deleting,
            expected,
            after_initialized,
            after_deleting,
        } in rows
        {
            let mut g = ShareGroupState::new("g1");
            g.initialized.extend(
                initialized
                    .into_iter()
                    .map(|partition| share_partition(orders, partition)),
            );
            g.initializing.insert(share_partition(orders, 5), 1);
            if is_deleting {
                g.deleting.insert(orders, "stale".to_owned());
            }

            assert!(
                g.mark_topic_deleting(orders, "orders", 3) == expected,
                "{name}"
            );
            assert!(g.initialized == after_initialized, "{name}");
            assert!(g.deleting == after_deleting, "{name}");
            assert!(
                g.initializing == HashMap::from([(share_partition(orders, 5), 1)]),
                "{name}"
            );
        }
    }

    /// Kafka's `shareGroupBuildPartitionDeleteRequest`: initialized and
    /// initializing partitions, and every partition of a deleting topic the
    /// image still holds, are deleted, and exactly those topics are left
    /// deleting. A deleting topic the image no longer holds is dropped.
    #[test]
    fn a_group_delete_moves_every_topic_to_deleting() {
        let orders = Uuid([1; 16]);
        let carts = Uuid([2; 16]);
        let gone = Uuid([3; 16]);
        let mut g = ShareGroupState::new("g1");
        g.initialized.insert(share_partition(orders, 1));
        g.initializing.insert(share_partition(orders, 0), 5);
        g.topic_names.insert(orders, "orders".to_owned());
        g.deleting.insert(carts, "carts".to_owned());
        g.deleting.insert(gone, "gone".to_owned());

        let deletes = g.move_all_to_deleting(|topic_id| {
            (*topic_id == carts).then(|| NamedTopic {
                name: "carts".to_owned(),
                partition_count: 2,
            })
        });

        assert!(
            deletes
                == vec![
                    TopicPartitionList {
                        topic_id: orders,
                        partitions: vec![0, 1],
                    },
                    TopicPartitionList {
                        topic_id: carts,
                        partitions: vec![0, 1],
                    },
                ]
        );
        assert!(g.initialized.is_empty());
        assert!(g.initializing.is_empty());
        assert!(
            g.deleting
                == HashMap::from([(orders, "orders".to_owned()), (carts, "carts".to_owned())])
        );
        assert!(g.topic_names.is_empty());
    }
}
