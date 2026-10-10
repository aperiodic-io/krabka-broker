//! The [`GroupState`] container for a next-gen consumer group and the
//! membership transitions that do not compute an assignment.
//!
//! These are the epoch bump, the add, remove, and session-timeout eviction of
//! members, the static-instance binding, and the `OffsetCommit` epoch fence.
//! Installing a target assignment and reconciling a member against it live in
//! the `reconcile` sibling.

use std::{cmp::Ordering, collections::HashMap, time::Instant};

use krabka_protocol::primitives::uuid::Uuid;

use super::{TargetAssignment, member::MemberState, regex::ResolvedRegularExpression};
use crate::{
    codes,
    coordinator::unified::{
        INITIAL_GROUP_EPOCH, actor::CommitFence, persistence_next_gen::MemberAssignmentState,
    },
};

/// The first `OffsetCommit` version that a member of the consumer protocol
/// (KIP-848) may use. Kafka's `ConsumerGroup.validateOffsetCommit` answers
/// `UNSUPPORTED_VERSION` below it.
const FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// Kafka's `JoinGroupRequest.UNKNOWN_GENERATION_ID`.
const UNKNOWN_GENERATION_ID: i32 = -1;

#[derive(Debug)]
pub struct GroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, MemberState>,
    pub instance_to_member: HashMap<String, String>,
    pub target: TargetAssignment,
    /// The armed rebalance timeouts, by member id: the instant each one fires.
    /// Kafka's `scheduleConsumerGroupRebalanceTimeout` keeps the same deadline
    /// in a timer.
    rebalance_deadlines: HashMap<String, Instant>,
    /// Kafka's `ConsumerGroup.resolvedRegularExpressions`: the topics that
    /// each regex the members subscribe to resolved to, persisted as
    /// `ConsumerGroupRegularExpression` records. See `regex`.
    pub(super) resolved_regexes: HashMap<String, ResolvedRegularExpression>,
    /// Kafka's `ModernGroup.metadataHash`: the hash of the subscribed topics'
    /// metadata that the current target assignment was computed from. See
    /// `reconciler::metadata_hash`.
    metadata_hash: i64,
    /// Kafka's `ModernGroup.metadataRefreshDeadline` set to
    /// `DeadlineAndEpoch.EMPTY`: a subscribed topic changed, so the next
    /// heartbeat computes the metadata hash again.
    metadata_refresh_requested: bool,
    /// Kafka's `ConsumerGroup.assignmentTimestamp`: the wall-clock time in
    /// milliseconds at which the last target assignment calculation finished,
    /// or 0 when there is no previous assignment or its time is unknown. It
    /// is the `AssignmentTimestamp` of the group's target assignment metadata
    /// record.
    assignment_timestamp_ms: i64,
    /// Kafka's `ConsumerGroup.hasSubscriptionMetadataRecord`: the log holds a
    /// deprecated `ConsumerGroupPartitionMetadata` value (key v4), so the next
    /// metadata update writes its tombstone.
    has_subscription_metadata_record: bool,
    /// Whether the log holds the group: a batch of its records was appended,
    /// or the group was loaded from the log. Kafka's
    /// `getOrMaybeCreateConsumerGroup` creates a group in its timeline and
    /// keeps it only once records commit, so a group that no record holds
    /// does not exist for a heartbeat that does not join.
    presence: LogPresence,
}

/// Whether the log holds a consumer group. See [`GroupState::mark_persisted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogPresence {
    /// No batch of the group's records was appended or loaded.
    Absent,
    /// A batch was appended, or the group was loaded from the log.
    Present,
}

impl GroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: INITIAL_GROUP_EPOCH,
            members: HashMap::new(),
            instance_to_member: HashMap::new(),
            target: TargetAssignment {
                epoch: INITIAL_GROUP_EPOCH,
                per_member: HashMap::new(),
            },
            rebalance_deadlines: HashMap::new(),
            resolved_regexes: HashMap::new(),
            metadata_hash: 0,
            // Kafka's `DeadlineAndEpoch.EMPTY`: a new group's metadata starts
            // expired, so its first heartbeat computes the metadata hash.
            metadata_refresh_requested: true,
            assignment_timestamp_ms: 0,
            has_subscription_metadata_record: false,
            presence: LogPresence::Absent,
        }
    }

    /// Whether the log holds the group. See [`Self::mark_persisted`].
    #[must_use]
    pub(crate) fn is_persisted(&self) -> bool {
        self.presence == LogPresence::Present
    }

    /// Records that the log holds the group: a batch of its records was
    /// appended, or it was loaded from the log.
    pub(crate) fn mark_persisted(&mut self) {
        self.presence = LogPresence::Present;
    }

    crate::coordinator::unified::member_helpers::assignment_delay_method!();

    /// Records that a target assignment calculation finished at `now_ms`, the
    /// wall-clock time that Kafka's `TargetAssignmentBuilder` writes as the
    /// record's `AssignmentTimestamp`.
    pub(crate) fn record_assignment(&mut self, now_ms: i64) {
        self.assignment_timestamp_ms = now_ms;
    }

    /// Kafka's `ConsumerGroup.assignmentTimestamp`.
    #[must_use]
    pub(crate) fn assignment_timestamp_ms(&self) -> i64 {
        self.assignment_timestamp_ms
    }

    crate::coordinator::unified::member_helpers::bump_group_epoch!(self;);

    /// `true` while the group epoch is ahead of the target assignment epoch:
    /// Kafka's `maybeUpdateTargetAssignment` then computes a new target at
    /// the next heartbeat that the assignment interval allows.
    #[must_use]
    pub fn target_is_stale(&self) -> bool {
        self.group_epoch > self.target.epoch
    }

    /// Kafka's `ConsumerGroup.validateOffsetCommit`, with the per-partition
    /// validator of `createAssignmentEpochValidator` (KIP-1251) run over
    /// `partitions`, as `OffsetMetadataManager.commitOffset` and
    /// `commitTransactionalOffset` run it over each partition they commit.
    /// `Ok(())` accepts the commit. Any other result is the Kafka error code
    /// for the whole commit.
    ///
    /// The rule, in order:
    ///
    /// 1. A negative epoch commits on a group with no members: that is the
    ///    admin client or a consumer that does not use group management.
    /// 2. A `TxnOffsetCommit` with no member id, no instance id and epoch -1
    ///    commits: its producer gave no group metadata.
    /// 3. Any other member id must be a member (`UNKNOWN_MEMBER_ID`).
    /// 4. An `OffsetCommit` from a member of the consumer protocol must be v9
    ///    or later (`UNSUPPORTED_VERSION`).
    /// 5. The member's epoch commits every partition.
    /// 6. A newer epoch is refused.
    /// 7. An older epoch commits a partition only if the member holds it,
    ///    assigned or pending revocation, and the epoch is at least the
    ///    partition's assignment epoch.
    ///
    /// A refusal in 6 or 7 is `STALE_MEMBER_EPOCH` for a member of the
    /// consumer protocol and `ILLEGAL_GENERATION` for a member of the classic
    /// protocol. The `TxnOffsetCommit` handler maps `STALE_MEMBER_EPOCH` by
    /// version, as `validateTransactionalOffsetCommit` does.
    ///
    /// The method is pure, so that the consumer-group composition model can
    /// drive the real rule.
    pub(crate) fn validate_offset_commit(
        &self,
        member_id: &str,
        group_instance_id: Option<&str>,
        member_epoch: i32,
        fence: CommitFence,
        partitions: &[(Uuid, i32)],
    ) -> Result<(), i16> {
        if member_epoch < 0 && self.members.is_empty() {
            return Ok(());
        }
        if fence == CommitFence::Transactional
            && member_epoch == UNKNOWN_GENERATION_ID
            && member_id.is_empty()
            && group_instance_id.is_none()
        {
            return Ok(());
        }
        let member = self
            .members
            .get(member_id)
            .ok_or(codes::UNKNOWN_MEMBER_ID)?;
        let classic = member.is_classic();
        if let CommitFence::Offset { api_version } = fence
            && !classic
            && api_version < FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION
        {
            return Err(codes::UNSUPPORTED_VERSION);
        }
        let refused = if classic {
            codes::ILLEGAL_GENERATION
        } else {
            codes::STALE_MEMBER_EPOCH
        };
        let accepted = match member_epoch.cmp(&member.member_epoch) {
            Ordering::Equal => true,
            Ordering::Greater => false,
            Ordering::Less => partitions.iter().all(|(topic_id, partition)| {
                member
                    .assignment_epoch(topic_id, *partition)
                    .is_some_and(|assigned_at| member_epoch >= assigned_at)
            }),
        };
        if accepted { Ok(()) } else { Err(refused) }
    }

    pub fn add_or_update_member(&mut self, m: MemberState) {
        if let Some(iid) = m.instance_id.clone() {
            self.instance_to_member.insert(iid, m.member_id.clone());
        }
        self.members.insert(m.member_id.clone(), m);
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberState> {
        self.rebalance_deadlines.remove(member_id);
        let m = self.members.remove(member_id)?;
        if let Some(ref iid) = m.instance_id
            && self.instance_to_member.get(iid).map(String::as_str) == Some(member_id)
        {
            self.instance_to_member.remove(iid);
        }
        Some(m)
    }

    /// `true` when a member subscribes to one of `topics`, by name or through
    /// a regex that resolved to it. This is Kafka's
    /// `GroupMetadataManager.groupsSubscribedToTopic`, asked about this group.
    ///
    /// A new topic that a regex matches is not resolved yet. The next
    /// heartbeat resolves it, as Kafka's regex refresh does.
    #[must_use]
    pub fn subscribes_to_any(&self, topics: &[String]) -> bool {
        self.members.values().any(|member| {
            topics
                .iter()
                .any(|topic| self.member_subscribes_to(member, topic))
        })
    }

    /// Kafka's `ModernGroup.requestMetadataRefresh`: the next heartbeat
    /// computes the metadata hash of the subscribed topics again.
    pub fn request_metadata_refresh(&mut self) {
        self.metadata_refresh_requested = true;
    }

    /// Kafka's `ModernGroup.hasMetadataExpired`: `true` from a
    /// [`Self::request_metadata_refresh`] until the group records a metadata
    /// hash again.
    ///
    /// Kafka 4.3.1 has no periodic refresh. Its
    /// `GroupMetadataManager.METADATA_REFRESH_INTERVAL_MS` is
    /// `Integer.MAX_VALUE`, so only a request makes the metadata expire.
    #[must_use]
    pub fn metadata_refresh_requested(&self) -> bool {
        self.metadata_refresh_requested
    }

    /// Kafka's `ConsumerGroup.hasSubscriptionMetadataRecord`.
    #[must_use]
    pub fn has_subscription_metadata_record(&self) -> bool {
        self.has_subscription_metadata_record
    }

    /// Kafka's `ConsumerGroup.setHasSubscriptionMetadataRecord`, which the
    /// replay of a key-v4 value sets and the replay of its tombstone clears.
    pub fn set_has_subscription_metadata_record(&mut self, present: bool) {
        self.has_subscription_metadata_record = present;
    }

    /// The metadata hash that the group recorded last.
    #[must_use]
    pub fn metadata_hash(&self) -> i64 {
        self.metadata_hash
    }

    /// Records `hash` as the metadata of the current target and ends a
    /// requested refresh. Kafka's `updateSubscriptionMetadata` also sets the
    /// hash and the next refresh deadline together.
    pub fn record_metadata_hash(&mut self, hash: i64) {
        self.metadata_hash = hash;
        self.metadata_refresh_requested = false;
    }

    /// Records `hash` as the group's metadata hash and leaves a requested
    /// refresh as it is, as the replay of the epoch record that Kafka's
    /// `consumerGroupFenceMembers` writes does.
    pub fn set_metadata_hash(&mut self, hash: i64) {
        self.metadata_hash = hash;
    }

    crate::coordinator::unified::member_helpers::expired_members_method!();

    crate::coordinator::unified::member_helpers::evict_expired!();

    /// Arms or cancels the rebalance timeout of `member_id` after a
    /// reconciliation, as Kafka's `GroupMetadataManager.maybeReconcile` does.
    ///
    /// A member that enters the state with partitions to revoke gets a timeout
    /// of its `rebalance_timeout_ms`. The timeout stays armed while the member
    /// stays in that state, so a member that keeps its partitions cannot push
    /// the deadline back with more heartbeats. Kafka gets the same effect
    /// because the member epoch does not move while the member still owns
    /// revoked partitions. Any other state cancels the timeout.
    pub fn track_rebalance_timeout(&mut self, member_id: &str, now: Instant) {
        match self.members.get(member_id) {
            Some(member)
                if member.assignment_state == MemberAssignmentState::UnrevokedPartitions =>
            {
                let timeout = member.rebalance_timeout;
                self.rebalance_deadlines
                    .entry(member_id.to_string())
                    .or_insert(now + timeout);
            }
            _ => {
                self.rebalance_deadlines.remove(member_id);
            }
        }
    }

    /// Cancels the rebalance timeout of every member that no longer has
    /// partitions to revoke.
    ///
    /// A reconciliation can end the obligation of a member that did not send
    /// the heartbeat, for example when another member leaves and the target
    /// gives the pending partition back. Without this pass, a later revocation
    /// of that member would reuse the old, maybe already expired, deadline.
    pub fn prune_rebalance_timeouts(&mut self) {
        let members = &self.members;
        self.rebalance_deadlines.retain(|member_id, _| {
            members.get(member_id).is_some_and(|member| {
                member.assignment_state == MemberAssignmentState::UnrevokedPartitions
            })
        });
    }

    /// The earliest armed rebalance deadline, so the actor can wake at it.
    #[must_use]
    pub fn next_rebalance_deadline(&self) -> Option<Instant> {
        self.rebalance_deadlines.values().min().copied()
    }

    crate::coordinator::unified::member_helpers::fence_rebalance_timeouts_method! {
    /// Removes every member whose rebalance timeout fired at `now` while the
    /// member still had partitions to revoke. Returns the removed member ids,
    /// sorted.
    ///
    /// This is the fence of Kafka's `scheduleConsumerGroupRebalanceTimeout`
    /// (`consumerGroupFenceMember`). The removal frees the partitions the
    /// member did not revoke, so their new owners can take them.
    ///
    /// A hosted classic member never sends `ConsumerGroupHeartbeat`, so no
    /// heartbeat arms its timeout. This pass arms it the first time it sees
    /// the member with partitions to revoke. Kafka bounds the same member with
    /// its join and sync timers, which also use the rebalance timeout. The
    /// pass also cancels the deadlines that no longer apply, so a past
    /// deadline never stays armed.
    }

    /// The members whose rebalance timeout fired at `now` while they still
    /// had partitions to revoke, sorted, without removing them. See
    /// [`Self::fence_rebalance_timeouts`].
    pub fn rebalance_timeouts_due(&mut self, now: Instant) -> Vec<String> {
        self.prune_rebalance_timeouts();
        let unarmed_classic: Vec<String> = self
            .members
            .values()
            .filter(|member| {
                member.is_classic()
                    && member.assignment_state == MemberAssignmentState::UnrevokedPartitions
                    && !self.rebalance_deadlines.contains_key(&member.member_id)
            })
            .map(|member| member.member_id.clone())
            .collect();
        for member_id in &unarmed_classic {
            self.track_rebalance_timeout(member_id, now);
        }
        let mut fenced: Vec<String> = self
            .rebalance_deadlines
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(member_id, _)| member_id.clone())
            .collect();
        fenced.sort_unstable();
        fenced
    }

    /// Moves the static member `previous` to the id `member_id` at epoch 0, as
    /// Kafka's `getOrMaybeSubscribeStaticConsumerGroupMember` copies a
    /// released static member for the member that rejoins with its instance
    /// id. The member keeps its subscription, classic metadata, target and
    /// assignment, as Kafka's `ConsumerGroupMember.Builder(member, memberId)`
    /// copies them, and the group does not rebalance for the change.
    pub fn replace_static_member(&mut self, previous: &str, member_id: &str) {
        let Some(mut member) = self.members.remove(previous) else {
            return;
        };
        self.rebalance_deadlines.remove(previous);
        // Kafka writes the copy under `member_id` over any member that already
        // has that id. Remove that member through `remove_member`, so its
        // instance id does not keep pointing at the copy.
        if member_id != previous {
            self.remove_member(member_id);
        }
        member.member_id = member_id.to_string();
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        if let Some(instance_id) = &member.instance_id {
            self.instance_to_member
                .insert(instance_id.clone(), member_id.to_string());
        }
        if let Some(target) = self.target.per_member.remove(previous) {
            self.target.per_member.insert(member_id.to_string(), target);
        }
        self.members.insert(member_id.to_string(), member);
    }

    /// Sets a static member that leaves for a while to epoch -2, as Kafka's
    /// `consumerGroupStaticMemberGroupLeave` does. It keeps its assignment
    /// and drops the partitions it had still to revoke.
    ///
    /// Every partition it keeps is set to assignment epoch 0, as Kafka's
    /// `resetAssignedPartitionsEpochsToZero` does. The member that rejoins
    /// with the instance id holds them from epoch 0 under its new id, and a
    /// commit under the old member id is refused.
    pub fn release_static_member(&mut self, member_id: &str) {
        self.rebalance_deadlines.remove(member_id);
        if let Some(member) = self.members.get_mut(member_id) {
            member.member_epoch = -2;
            member.partitions_pending_revocation.clear();
            member.reset_assignment_epochs(0);
        }
    }

    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = self.group_epoch;
        }
    }

    pub fn current_member_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.instance_to_member.get(instance_id).map(String::as_str)
    }

    /// The group state that `ListGroups` reports, from Kafka's
    /// `ConsumerGroup.maybeUpdateGroupState`.
    ///
    /// A group with no members is `Empty`. A group whose epoch is ahead of its
    /// target assignment is `Assigning`. A group with a member that is not
    /// reconciled to the target, which means a member that is not `Stable` or
    /// not at the target epoch, is `Reconciling`. Every other group is
    /// `Stable`.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        if self.members.is_empty() {
            "Empty"
        } else if self.target_is_stale() {
            "Assigning"
        } else if self.members.values().any(|m| {
            m.assignment_state != MemberAssignmentState::Stable
                || m.member_epoch != self.target.epoch
        }) {
            "Reconciling"
        } else {
            "Stable"
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::{
        Topics, member, subscribed_member,
    };

    #[test]
    fn state_name_follows_kafka_consumer_group_state() {
        let at = |epoch, assignment_state| {
            let mut m = member("m1");
            m.member_epoch = epoch;
            m.assignment_state = assignment_state;
            m
        };
        // (group epoch, target epoch, members, expected state)
        let rows = [
            (3, 3, vec![], "Empty"),
            (4, 3, vec![], "Empty"),
            (
                4,
                3,
                vec![at(3, MemberAssignmentState::Stable)],
                "Assigning",
            ),
            (
                3,
                3,
                vec![at(2, MemberAssignmentState::Stable)],
                "Reconciling",
            ),
            (
                3,
                3,
                vec![at(3, MemberAssignmentState::UnreleasedPartitions)],
                "Reconciling",
            ),
            (3, 3, vec![at(3, MemberAssignmentState::Stable)], "Stable"),
        ];
        for (group_epoch, target_epoch, members, expected) in rows {
            let mut g = GroupState::new("g");
            g.group_epoch = group_epoch;
            g.target.epoch = target_epoch;
            for m in members {
                g.members.insert(m.member_id.clone(), m);
            }
            assert!(g.state_name() == expected);
        }
    }

    const T: Uuid = Uuid([1; 16]);

    /// One row of issue #800's table: the committing member's protocol, the
    /// request, the partitions it commits, and Kafka's answer. The group
    /// holds `native` and `classic`, both at member epoch 5, each assigned
    /// partition 0 at epoch 3, partition 1 at epoch 5, and partition 2 pending
    /// revocation from epoch 2.
    struct CommitRow {
        name: &'static str,
        member_id: &'static str,
        instance_id: Option<&'static str>,
        epoch: i32,
        fence: CommitFence,
        partitions: &'static [i32],
        result: Result<(), i16>,
    }

    fn commit_group(empty: bool) -> GroupState {
        let mut g = GroupState::new("g");
        if empty {
            return g;
        }
        for (id, classic) in [("native", false), ("classic", true)] {
            let mut m = member(id);
            m.member_epoch = 5;
            m.assigned_partitions = [(T, vec![0, 1])].into();
            m.partitions_pending_revocation = [(T, vec![2])].into();
            m.assignment_epochs = [(T, [(0, 3), (1, 5), (2, 2)].into())].into();
            m.classic = classic.then(|| super::super::ClassicMemberFacade {
                supported_protocols: vec![],
                session_timeout: Duration::from_secs(45),
            });
            g.members.insert(id.into(), m);
        }
        g
    }

    // The rows of `offset_commit_follows_kafka_consumer_group_rule`.
    fn commit_rows() -> Vec<CommitRow> {
        const V9: CommitFence = CommitFence::Offset { api_version: 9 };
        const TXN: CommitFence = CommitFence::Transactional;
        let row = |name, member_id, epoch, fence, partitions, result| CommitRow {
            name,
            member_id,
            instance_id: None,
            epoch,
            fence,
            partitions,
            result,
        };
        vec![
            row("native, member epoch", "native", 5, V9, &[0, 1, 3], Ok(())),
            row(
                "native, newer epoch",
                "native",
                6,
                V9,
                &[0],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "native, older epoch, assigned before it",
                "native",
                4,
                V9,
                &[0],
                Ok(()),
            ),
            row(
                "native, older epoch, pending since before it",
                "native",
                4,
                V9,
                &[2],
                Ok(()),
            ),
            row(
                "native, older epoch, assigned at it",
                "native",
                3,
                V9,
                &[0],
                Ok(()),
            ),
            row(
                "native, older epoch, assigned after it",
                "native",
                4,
                V9,
                &[1],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "native, older epoch, not assigned",
                "native",
                4,
                V9,
                &[3],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "native, older epoch, one partition refused",
                "native",
                4,
                V9,
                &[0, 1],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "native, older epoch, no partition",
                "native",
                4,
                V9,
                &[],
                Ok(()),
            ),
            row(
                "native, v8",
                "native",
                5,
                CommitFence::Offset { api_version: 8 },
                &[0],
                Err(codes::UNSUPPORTED_VERSION),
            ),
            row(
                "classic, v8",
                "classic",
                5,
                CommitFence::Offset { api_version: 8 },
                &[0],
                Ok(()),
            ),
            row(
                "classic, newer generation",
                "classic",
                6,
                V9,
                &[0],
                Err(codes::ILLEGAL_GENERATION),
            ),
            row(
                "classic, older generation, assigned before it",
                "classic",
                4,
                V9,
                &[0],
                Ok(()),
            ),
            row(
                "classic, older generation, assigned after it",
                "classic",
                4,
                V9,
                &[1],
                Err(codes::ILLEGAL_GENERATION),
            ),
            row(
                "classic, older generation, not assigned",
                "classic",
                4,
                V9,
                &[3],
                Err(codes::ILLEGAL_GENERATION),
            ),
            row(
                "admin, group with members",
                "",
                -1,
                V9,
                &[0],
                Err(codes::UNKNOWN_MEMBER_ID),
            ),
            row(
                "unknown member",
                "ghost",
                5,
                V9,
                &[0],
                Err(codes::UNKNOWN_MEMBER_ID),
            ),
            row("txn, no group metadata", "", -1, TXN, &[0], Ok(())),
            row(
                "txn, native, v8 is not checked",
                "native",
                5,
                TXN,
                &[0],
                Ok(()),
            ),
            row(
                "txn, native, older epoch, assigned before it",
                "native",
                4,
                TXN,
                &[0],
                Ok(()),
            ),
            row(
                "txn, native, older epoch, assigned after it",
                "native",
                4,
                TXN,
                &[1],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "txn, native, newer epoch",
                "native",
                6,
                TXN,
                &[0],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            row(
                "txn, classic, older generation, not assigned",
                "classic",
                4,
                TXN,
                &[3],
                Err(codes::ILLEGAL_GENERATION),
            ),
            CommitRow {
                instance_id: Some("i1"),
                ..row(
                    "txn, instance id and no member",
                    "",
                    -1,
                    TXN,
                    &[0],
                    Err(codes::UNKNOWN_MEMBER_ID),
                )
            },
        ]
    }

    /// Kafka 4.3.1's `ConsumerGroup.validateOffsetCommit` and
    /// `createAssignmentEpochValidator`, row by row.
    #[test]
    fn offset_commit_follows_kafka_consumer_group_rule() {
        let rows = commit_rows();
        let g = commit_group(false);
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for r in &rows {
            let partitions: Vec<(Uuid, i32)> = r.partitions.iter().map(|&p| (T, p)).collect();
            let got =
                g.validate_offset_commit(r.member_id, r.instance_id, r.epoch, r.fence, &partitions);
            actual.push((r.name, got));
            expected.push((r.name, r.result));
        }
        assert!(actual == expected);
    }

    /// A group with no members takes a commit with a negative epoch from
    /// anyone, at any version, and refuses any other.
    #[test]
    fn empty_group_takes_only_a_negative_epoch() {
        let g = commit_group(true);
        let rows = [
            ("", -1, CommitFence::Offset { api_version: 9 }, Ok(())),
            ("", -1, CommitFence::Offset { api_version: 2 }, Ok(())),
            ("m", -1, CommitFence::Transactional, Ok(())),
            (
                "ghost",
                1,
                CommitFence::Offset { api_version: 9 },
                Err(codes::UNKNOWN_MEMBER_ID),
            ),
        ];
        let actual: Vec<_> = rows
            .iter()
            .map(|&(member_id, epoch, fence, _)| {
                (
                    member_id,
                    epoch,
                    fence,
                    g.validate_offset_commit(member_id, None, epoch, fence, &[(T, 0)]),
                )
            })
            .collect();
        assert!(actual == rows);
    }

    /// Reconciliation stamps a partition with the epoch at which the member
    /// is granted it, keeps the epoch of a partition it keeps or moves to
    /// pending revocation, and drops the epoch of a partition it gives up.
    /// A static member that leaves for a while keeps its partitions at epoch 0.
    #[test]
    fn assignment_epochs_follow_kafka_current_assignment_builder() {
        let mut g = GroupState::new("g");
        let topics = Topics(vec![("t", T)]);
        g.add_or_update_member(subscribed_member("m1", &["t"]));
        let epochs = |g: &GroupState| {
            crate::coordinator::test_support::sorted_epoch_pairs(
                g.members["m1"].assignment_epochs.get(&T),
            )
        };
        let mut steps = Vec::new();

        // Epoch 1: granted partitions 0 and 1.
        g.group_epoch = 1;
        g.install_target([("m1".to_string(), [(T, vec![0, 1])].into())].into());
        g.reconcile_member("m1", Some(&HashMap::new()), true, &topics);
        steps.push(epochs(&g));

        // Epoch 2: partition 1 goes. The member still owns it, so it is
        // pending revocation with the epoch it was assigned at.
        g.group_epoch = 2;
        g.install_target([("m1".to_string(), [(T, vec![0])].into())].into());
        g.reconcile_member("m1", Some(&[(T, vec![0, 1])].into()), false, &topics);
        steps.push(epochs(&g));

        // The member revokes it and moves to epoch 2.
        g.reconcile_member("m1", Some(&[(T, vec![0])].into()), false, &topics);
        steps.push(epochs(&g));

        // Epoch 3: partition 2 comes, at epoch 3; partition 0 keeps epoch 1.
        g.group_epoch = 3;
        g.install_target([("m1".to_string(), [(T, vec![0, 2])].into())].into());
        g.reconcile_member("m1", Some(&[(T, vec![0])].into()), false, &topics);
        steps.push(epochs(&g));

        // A static leave keeps the assignment at epoch 0.
        g.release_static_member("m1");
        steps.push(epochs(&g));

        assert!(
            steps
                == vec![
                    vec![(0, 1), (1, 1)],
                    vec![(0, 1), (1, 1)],
                    vec![(0, 1)],
                    vec![(0, 1), (2, 3)],
                    vec![(0, 0), (2, 0)],
                ]
        );
    }

    #[test]
    fn the_assignment_interval_holds_the_next_assignment_back() {
        let assigned_at = 1_000_000;
        let second = Duration::from_secs(1);
        // (last assignment recorded, interval, milliseconds since it, delayed)
        let rows = [
            (false, second, 0, false),
            (true, Duration::ZERO, 0, false),
            (true, second, 999, true),
            (true, second, 1_000, false),
            (true, second, 60_000, false),
        ];
        for (recorded, interval, since, delayed) in rows {
            let mut g = GroupState::new("g");
            if recorded {
                g.record_assignment(assigned_at);
            }
            assert!(
                g.assignment_delayed(interval, assigned_at + since) == delayed,
                "{recorded} {interval:?} {since:?}"
            );
        }
    }

    #[test]
    fn evict_expired_drops_old_members() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.last_seen = Instant::now().checked_sub(Duration::from_mins(2)).unwrap();
        g.add_or_update_member(m);
        g.add_or_update_member(member("m2"));
        let evicted = g.evict_expired(Instant::now(), Duration::from_mins(1));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn instance_binding_tracked() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.instance_id = Some("inst1".into());
        g.add_or_update_member(m);
        assert!(g.current_member_for_instance("inst1") == Some("m1"));
    }

    /// The rebalance timeout is armed once when a member enters the state with
    /// partitions to revoke, is not pushed back by later reconciliations in
    /// that state, is cancelled by any other state, and is never armed for a
    /// hosted classic member.
    #[test]
    fn rebalance_timeout_arms_once_and_fences_at_the_deadline() {
        struct Row {
            name: &'static str,
            classic: bool,
            /// The member state at the second reconciliation, 30 s later.
            second_state: MemberAssignmentState,
            /// Seconds after the first reconciliation that the check runs.
            check_after_secs: u64,
            fenced: Vec<String>,
        }
        let rows = [
            Row {
                name: "still unrevoked at the deadline",
                classic: false,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 60,
                fenced: vec!["m1".into()],
            },
            Row {
                name: "still unrevoked before the deadline",
                classic: false,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 59,
                fenced: vec![],
            },
            Row {
                name: "revoked before the deadline",
                classic: false,
                second_state: MemberAssignmentState::Stable,
                check_after_secs: 60,
                fenced: vec![],
            },
            Row {
                name: "hosted classic member, same rule",
                classic: true,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 60,
                fenced: vec!["m1".into()],
            },
        ];
        for row in rows {
            let start = Instant::now();
            let mut g = GroupState::new("g");
            let mut m = member("m1");
            m.assignment_state = MemberAssignmentState::UnrevokedPartitions;
            if row.classic {
                m.classic = Some(super::super::ClassicMemberFacade {
                    supported_protocols: vec![],
                    session_timeout: Duration::from_secs(45),
                });
            }
            g.add_or_update_member(m);
            g.track_rebalance_timeout("m1", start);
            g.members.get_mut("m1").unwrap().assignment_state = row.second_state;
            g.track_rebalance_timeout("m1", start + Duration::from_secs(30));
            g.members.get_mut("m1").unwrap().assignment_state =
                MemberAssignmentState::UnrevokedPartitions;

            let fenced =
                g.fence_rebalance_timeouts(start + Duration::from_secs(row.check_after_secs));

            assert!(fenced == row.fenced, "{}", row.name);
            assert!(
                g.members.contains_key("m1") == row.fenced.is_empty(),
                "{}",
                row.name
            );
        }
    }

    /// A reconciliation that ends a member's revocation cancels its deadline,
    /// so a later revocation gets a fresh one. A hosted classic member, which
    /// no heartbeat arms, is armed by the fence pass itself.
    #[test]
    fn stale_deadlines_are_pruned_and_classic_members_are_armed_by_the_sweep() {
        let start = Instant::now();
        let mut g = GroupState::new("g");
        let mut native = member("native");
        native.assignment_state = MemberAssignmentState::UnrevokedPartitions;
        g.add_or_update_member(native);
        g.track_rebalance_timeout("native", start);
        // Another member's change gives the partition back, then takes it
        // away again 50 s later.
        g.members.get_mut("native").unwrap().assignment_state = MemberAssignmentState::Stable;
        g.prune_rebalance_timeouts();
        g.members.get_mut("native").unwrap().assignment_state =
            MemberAssignmentState::UnrevokedPartitions;
        g.track_rebalance_timeout("native", start + Duration::from_secs(50));
        assert!(g.next_rebalance_deadline() == Some(start + Duration::from_secs(110)));
        assert!(
            g.fence_rebalance_timeouts(start + Duration::from_secs(60))
                .is_empty()
        );

        let mut classic = member("classic");
        classic.assignment_state = MemberAssignmentState::UnrevokedPartitions;
        classic.classic = Some(super::super::ClassicMemberFacade {
            supported_protocols: vec![],
            session_timeout: Duration::from_secs(45),
        });
        g.add_or_update_member(classic);
        assert!(g.fence_rebalance_timeouts(start).is_empty());
        assert!(g.next_rebalance_deadline() == Some(start + Duration::from_mins(1)));
        assert!(
            g.fence_rebalance_timeouts(start + Duration::from_mins(1))
                == vec!["classic".to_string()]
        );
    }

    /// A static replacement whose member id another member already holds
    /// takes that id over: the other member and its instance id go, and the
    /// instance index stays coherent.
    #[test]
    fn static_replacement_over_an_occupied_member_id_keeps_the_index_coherent() {
        let mut g = GroupState::new("g");
        let mut released = member("s1");
        released.instance_id = Some("i1".into());
        released.member_epoch = -2;
        g.add_or_update_member(released);
        let mut occupant = member("m1");
        occupant.instance_id = Some("i2".into());
        g.add_or_update_member(occupant);

        g.replace_static_member("s1", "m1");

        let mut members: Vec<(&str, Option<&str>, i32)> = g
            .members
            .values()
            .map(|m| {
                (
                    m.member_id.as_str(),
                    m.instance_id.as_deref(),
                    m.member_epoch,
                )
            })
            .collect();
        members.sort_unstable();
        assert!(members == vec![("m1", Some("i1"), 0)]);
        assert!(g.current_member_for_instance("i1") == Some("m1"));
        assert!(g.current_member_for_instance("i2") == None);
    }

    #[test]
    fn bump_epoch_increments_and_stales_the_target() {
        let mut g = GroupState::new("g");
        // Kafka's `ModernGroup` starts at group epoch 1 and
        // `TargetAssignmentMetadata.INITIAL` at assignment epoch 1.
        assert!((g.group_epoch, g.target.epoch, g.target_is_stale()) == (1, 1, false));
        assert!(g.bump_epoch());
        assert!((g.group_epoch, g.target_is_stale()) == (2, true));
    }

    #[test]
    fn bump_epoch_rejects_exhaustion() {
        let mut group = GroupState::new("g");
        group.group_epoch = i32::MAX;

        assert!(!group.bump_epoch());
        assert!(group.group_epoch == i32::MAX);
    }

    #[test]
    fn advance_member_epoch_records_previous() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.group_epoch = 5;
        g.advance_member_epoch("m1");
        let m = &g.members["m1"];
        assert!(m.member_epoch == 5);
        assert!(m.previous_member_epoch == 0);
    }
}
