//! KIP-1071 streams-group in-memory state machine.
//!
//! This module mirrors the overall shape of the KIP-932 share-group state
//! machine (`super::super::share::state`): the `dirty` flag pattern,
//! `evict_expired`, `bump_epoch` and `install_target`. Streams members hold
//! *tasks* `(subtopology, partition)` across three disjoint roles, **active**,
//! **standby**, and **warmup**, instead of topic partitions.
//!
//! A new target changes no member. Each member reconciles toward it in its
//! own heartbeat, with Kafka's `CurrentAssignmentBuilder` rules
//! (`current_assignment`): a member revokes the tasks of every role before it
//! moves to the target epoch, and it gets a task only when no other member
//! still owns it and no other member of its process still runs it in another
//! role.
//!
//! A role's assignment is a `BTreeMap<String, Vec<i32>>`, from
//! `subtopology_id` to a sorted, deduped partition list. Everything here uses
//! that representation, so the state machine stays independent of any wire,
//! codec, or persistence newtype.
//!
//! This module is fully self-contained. It depends only on `std` and the
//! `uuid` crate, and it needs `uuid` only for the
//! [`StreamsMemberState::joining`] helper that synthesizes a random
//! `process_id`. It deliberately does NOT import the sibling `persistence`
//! module. The `i8` conversions on [`StreamsMemberAssignmentState`] live here,
//! so the actor can persist the state without coupling the two files.
//!
//! # Module layout
//!
//! This file is the module root. It holds the group-level state, meaning
//! [`StreamsGroupState`] and the epoch, membership, eviction, and target
//! transitions on it, plus the target assignment and the stored-topology
//! handle. Each child holds one concern: `member` the per-member state and its
//! reconciliation-state enum, `current_assignment` the reconciliation of one
//! member toward its target, `phase` the group lifecycle phase and its Kafka
//! group-state string, and `task_map` the normalization of a role's task map.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use self::current_assignment::{TaskOwners, next_member_state};

mod current_assignment;
mod member;
mod phase;
mod task_map;

#[cfg(test)]
mod test_support;

pub use self::{
    current_assignment::RoleTasks,
    member::{StreamsMemberAssignmentState, StreamsMemberState},
    phase::StreamsGroupStatePhase,
};

/// The target assignment from the most recent reconcile, stamped with the
/// assignment epoch it was computed against. Each role maps a member id to
/// that member's per-subtopology partition lists.
#[derive(Debug, Clone, Default)]
pub struct StreamsTargetAssignment {
    pub epoch: i32,
    pub active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    pub standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
    pub warmup: HashMap<String, BTreeMap<String, Vec<i32>>>,
}

/// Kafka's `TasksTuple`: a member's active, standby and warmup tasks, each by
/// subtopology.
pub type TasksTuple = (TaskMap, TaskMap, TaskMap);

/// Tasks of one role, keyed by subtopology with sorted partition lists.
pub type TaskMap = BTreeMap<String, Vec<i32>>;

/// A minimal handle for the resolved topology that lives in `topology.rs`.
///
/// The state machine tracks only the topology's *presence* and *epoch*. The
/// topology module derives the full subtopology and task sets.
#[derive(Debug, Clone, Default)]
pub struct StoredTopologyHandle {
    pub epoch: i32,
}

/// Full in-memory state of one streams group. Exactly one
/// `actor::GroupActor` task owns it, and it is never shared.
#[derive(Debug, Clone)]
pub struct StreamsGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub members: HashMap<String, StreamsMemberState>,
    /// The topology epoch. 0 means no topology is initialized yet.
    pub topology_epoch: i32,
    /// Presence and epoch of the stored topology. The full topology lives in
    /// `topology.rs`. This field only records that one exists.
    pub topology: Option<StoredTopologyHandle>,
    pub target: StreamsTargetAssignment,
    /// The state machine sets this on every change to membership,
    /// subscription, or topology epoch, so the actor knows a reconcile is
    /// pending. It clears the flag once the reconcile installs a target.
    pub dirty: bool,
    pub phase: StreamsGroupStatePhase,
    /// The `(status_code, status_detail)` that the most recent topology
    /// configuration gave, for example `MISSING_SOURCE_TOPICS`: Kafka's
    /// `topicConfigurationException`. Every heartbeat response carries it.
    pub status: Option<(i8, String)>,
    /// The member that first asked the application to shut down (KIP-1071
    /// `ShutdownApplication`). Kafka keeps it in memory only and clears it
    /// when the group becomes empty.
    pub shutdown_request_member_id: Option<String>,
    /// The armed rebalance timeouts, by member id: the instant each one fires
    /// and the member epoch that armed it. Kafka's
    /// `scheduleStreamsGroupRebalanceTimeout` keeps the same deadline in a
    /// timer.
    pub rebalance_deadlines: HashMap<String, (Instant, i32)>,
    /// Kafka's `StreamsGroup.endpointInformationEpoch`: bumped when the user
    /// endpoint of a member changes, or the tasks of a member with an
    /// endpoint change. A member whose last seen epoch differs gets the
    /// endpoint information of the group. Kafka keeps it in memory only.
    pub endpoint_information_epoch: i32,
}

/// The group epoch and target assignment epoch of a new streams group, as
/// Kafka's `StreamsGroup` constructor sets them. The first join bumps the group
/// epoch past the assignment epoch. A member that joins while the initial
/// rebalance delay holds the assignment back so reconciles to epoch 1, and does
/// not stay at the join epoch 0.
pub const INITIAL_EPOCH: i32 = crate::coordinator::unified::INITIAL_GROUP_EPOCH;

impl StreamsGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: INITIAL_EPOCH,
            assignment_epoch: INITIAL_EPOCH,
            members: HashMap::new(),
            topology_epoch: 0,
            topology: None,
            target: StreamsTargetAssignment {
                epoch: INITIAL_EPOCH,
                ..StreamsTargetAssignment::default()
            },
            dirty: false,
            phase: StreamsGroupStatePhase::Empty,
            status: None,
            shutdown_request_member_id: None,
            rebalance_deadlines: HashMap::new(),
            endpoint_information_epoch: 0,
        }
    }

    crate::coordinator::unified::member_helpers::bump_group_epoch! {
        /// Increments the group epoch. This mirrors the share and consumer state
        /// machines: a fresh epoch makes the assignment stale, so the method marks
        /// the group dirty.
        self; self.dirty = true;
    }

    /// Inserts or replaces a member.
    ///
    /// The method marks the group dirty when the membership is new or the
    /// member's topology epoch changed. Those are the two signals that can
    /// force a reconcile. Re-adding an identical member leaves `dirty`
    /// unchanged.
    pub fn add_or_update_member(&mut self, m: StreamsMemberState) {
        let changed = match self.members.get(&m.member_id) {
            None => true,
            Some(prev) => prev.topology_epoch != m.topology_epoch,
        };
        self.members.insert(m.member_id.clone(), m);
        if changed {
            self.dirty = true;
        }
    }

    /// Kafka's `streamsGroupFenceMember`: removes `member_id` and bumps the
    /// group epoch, which the group records with its metadata hash, validated
    /// topology epoch and assignment configuration as they are. The target
    /// waits for the next heartbeat; the group is not marked dirty. Returns
    /// `false`, with the group unchanged, when the member is unknown or the
    /// epoch is exhausted.
    pub fn fence_member(&mut self, member_id: &str) -> bool {
        if !self.members.contains_key(member_id) {
            return false;
        }
        let Some(group_epoch) = crate::metadata_epoch::next_i32(self.group_epoch) else {
            return false;
        };
        self.members.remove(member_id);
        self.rebalance_deadlines.remove(member_id);
        self.clear_shutdown_request_when_empty();
        self.group_epoch = group_epoch;
        self.refresh_phase();
        true
    }

    /// Removes a member and returns it if it was present. The method marks
    /// the group dirty only on a real removal.
    pub fn remove_member(&mut self, member_id: &str) -> Option<StreamsMemberState> {
        let m = self.members.remove(member_id);
        self.rebalance_deadlines.remove(member_id);
        if m.is_some() {
            self.dirty = true;
            self.clear_shutdown_request_when_empty();
        }
        m
    }

    crate::coordinator::unified::member_helpers::expired_members_method!();

    crate::coordinator::unified::member_helpers::evict_expired! {
        /// Removes members whose `last_seen` is older than `session_timeout` and
        /// returns the evicted member ids. The method marks the group dirty if it
        /// removed any member.
    }

    /// Records `member_id` as the member that asked the application to shut
    /// down, unless another member asked first (Kafka's
    /// `setShutdownRequestMemberId`).
    pub fn request_shutdown(&mut self, member_id: &str) {
        if self.shutdown_request_member_id.is_none() {
            self.shutdown_request_member_id = Some(member_id.to_string());
        }
    }

    /// Kafka's `clearShutdownRequestMemberId`, which runs when the group
    /// becomes empty. A restarted application therefore does not get the
    /// shutdown request of its previous run.
    fn clear_shutdown_request_when_empty(&mut self) {
        if self.members.is_empty() {
            self.shutdown_request_member_id = None;
        }
    }

    /// Installs a newly computed target assignment, stamped at the current
    /// group epoch, which becomes the new `assignment_epoch`.
    ///
    /// The members keep their current assignment. Each one reconciles toward
    /// the new target in its own heartbeat through [`Self::reconcile_member`],
    /// as Kafka's `maybeReconcile` does.
    ///
    /// Every member gets a target, an empty one when the assignor gave it
    /// nothing, as Kafka's `TargetAssignmentBuilder.newMemberAssignment` does.
    /// It returns the members whose target differs from the one they held, a
    /// member that held none included, sorted: the members for which Kafka's
    /// builder writes a target assignment record.
    pub fn install_target(&mut self, mut target: StreamsTargetAssignment) -> Vec<String> {
        fn take(
            role: &mut HashMap<String, BTreeMap<String, Vec<i32>>>,
            member_id: &str,
        ) -> BTreeMap<String, Vec<i32>> {
            let mut tasks = role.remove(member_id).unwrap_or_default();
            tasks.retain(|_, partitions| !partitions.is_empty());
            for partitions in tasks.values_mut() {
                partitions.sort_unstable();
                partitions.dedup();
            }
            tasks
        }
        let mut changed = Vec::new();
        let mut installed = StreamsTargetAssignment::default();
        let mut member_ids: Vec<&String> = self.members.keys().collect();
        member_ids.sort_unstable();
        for member_id in member_ids {
            let tuple = (
                take(&mut target.active, member_id),
                take(&mut target.standby, member_id),
                take(&mut target.warmup, member_id),
            );
            if self.target_tuple(member_id).as_ref() != Some(&tuple) {
                changed.push(member_id.clone());
            }
            let (active, standby, warmup) = tuple;
            installed.active.insert(member_id.clone(), active);
            if !standby.is_empty() {
                installed.standby.insert(member_id.clone(), standby);
            }
            if !warmup.is_empty() {
                installed.warmup.insert(member_id.clone(), warmup);
            }
        }
        self.assignment_epoch = self.group_epoch;
        self.target = installed;
        self.target.epoch = self.assignment_epoch;
        changed
    }

    /// The target of `member_id` as Kafka's `TasksTuple`, or `None` when the
    /// group holds no target for it.
    #[must_use]
    pub fn target_tuple(&self, member_id: &str) -> Option<TasksTuple> {
        let active = self.target.active.get(member_id);
        let standby = self.target.standby.get(member_id);
        let warmup = self.target.warmup.get(member_id);
        if active.is_none() && standby.is_none() && warmup.is_none() {
            return None;
        }
        Some((
            active.cloned().unwrap_or_default(),
            standby.cloned().unwrap_or_default(),
            warmup.cloned().unwrap_or_default(),
        ))
    }

    /// Validates the `member_epoch` of a heartbeat from `member_id`, as Kafka's
    /// `throwIfStreamsGroupMemberEpochIsInvalid` does, and returns the member
    /// epoch that the group holds.
    ///
    /// Epoch 0 from a known member is a rejoin, and the member epoch is
    /// accepted. The previous member epoch is accepted only when the owned
    /// active, standby and warmup tasks of the request are all in the current
    /// assignment of the member; an absent owned-task list does not count as
    /// contained. Any other epoch gets `FENCED_MEMBER_EPOCH`, and an unknown
    /// member gets `UNKNOWN_MEMBER_ID`. This API never answers
    /// `STALE_MEMBER_EPOCH`: the Streams client treats it as fatal.
    ///
    /// # Errors
    ///
    /// Returns the wire error code of a refused heartbeat.
    pub fn validate_heartbeat_epoch(
        &self,
        member_id: &str,
        requested_epoch: i32,
        owned: OwnedTasks<'_>,
    ) -> Result<i32, i16> {
        let Some(member) = self.members.get(member_id) else {
            return Err(crate::codes::UNKNOWN_MEMBER_ID);
        };
        let lost_bump = requested_epoch == member.previous_member_epoch
            && tasks_contained(owned.active, &member.active)
            && tasks_contained(owned.standby, &member.standby)
            && tasks_contained(owned.warmup, &member.warmup);
        if requested_epoch == 0 || requested_epoch == member.member_epoch || lost_bump {
            Ok(member.member_epoch)
        } else {
            Err(crate::codes::FENCED_MEMBER_EPOCH)
        }
    }

    /// Kafka's `maybeReconcile`: moves the current assignment of `member_id`
    /// toward its target at the assignment epoch. Returns `true` when the
    /// member changed.
    ///
    /// `owned` holds the tasks that the heartbeat reports, and it is `Some`
    /// only when the heartbeat reports all three roles. A member that must
    /// revoke tasks keeps its epoch until a heartbeat no longer reports them.
    pub fn reconcile_member(&mut self, member_id: &str, owned: Option<&RoleTasks>) -> bool {
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        if member.assignment_state == StreamsMemberAssignmentState::Stable
            && member.member_epoch == self.target.epoch
        {
            return false;
        }
        let target = RoleTasks {
            active: self
                .target
                .active
                .get(member_id)
                .cloned()
                .unwrap_or_default(),
            standby: self
                .target
                .standby
                .get(member_id)
                .cloned()
                .unwrap_or_default(),
            warmup: self
                .target
                .warmup
                .get(member_id)
                .cloned()
                .unwrap_or_default(),
        };
        let owners = TaskOwners::of(self.members.values());
        let Some(next) = next_member_state(member, self.target.epoch, &target, &owners, owned)
        else {
            return false;
        };
        let changed = next.member_epoch != member.member_epoch
            || next.previous_member_epoch != member.previous_member_epoch
            || next.assignment_state != member.assignment_state
            || next.active != member.active
            || next.standby != member.standby
            || next.warmup != member.warmup
            || next.active_pending_revocation != member.active_pending_revocation
            || next.standby_pending_revocation != member.standby_pending_revocation
            || next.warmup_pending_revocation != member.warmup_pending_revocation;
        self.members.insert(member_id.to_string(), next);
        self.refresh_phase();
        changed
    }

    /// Arms or cancels the rebalance timeout of `member_id` after its
    /// assignment changed, as Kafka's `maybeReconcile` does: a member in
    /// `UnrevokedTasks` must revoke within its `rebalance_timeout_ms`, and any
    /// other state cancels the timeout.
    pub fn track_rebalance_timeout(&mut self, member_id: &str, now: Instant) {
        match self.members.get(member_id) {
            Some(member)
                if member.assignment_state == StreamsMemberAssignmentState::UnrevokedTasks =>
            {
                let timeout =
                    Duration::from_millis(u64::try_from(member.rebalance_timeout_ms).unwrap_or(0));
                self.rebalance_deadlines
                    .insert(member_id.to_string(), (now + timeout, member.member_epoch));
            }
            _ => {
                self.rebalance_deadlines.remove(member_id);
            }
        }
    }

    /// The earliest armed rebalance deadline, so that the actor can wake at
    /// it.
    #[must_use]
    pub fn next_rebalance_deadline(&self) -> Option<Instant> {
        self.rebalance_deadlines
            .values()
            .map(|(deadline, _)| *deadline)
            .min()
    }

    crate::coordinator::unified::member_helpers::fence_rebalance_timeouts_method! {
    /// Removes every member whose rebalance timeout fired at `now` while it
    /// was still at the epoch that armed it, and returns the removed ids,
    /// sorted. This is the fence of Kafka's
    /// `scheduleStreamsGroupRebalanceTimeout`.
    }

    /// The members whose rebalance timeout fired at `now`, sorted, without
    /// removing them; the fired deadlines are dropped. See
    /// [`Self::fence_rebalance_timeouts`].
    pub fn rebalance_timeouts_due(&mut self, now: Instant) -> Vec<String> {
        let mut fenced: Vec<String> = self
            .rebalance_deadlines
            .iter()
            .filter(|(member_id, (deadline, epoch))| {
                now >= *deadline
                    && self
                        .members
                        .get(*member_id)
                        .is_some_and(|member| member.member_epoch == *epoch)
            })
            .map(|(member_id, _)| member_id.clone())
            .collect();
        self.rebalance_deadlines
            .retain(|_, (deadline, _)| now < *deadline);
        fenced.sort_unstable();
        fenced
    }

    /// Arms the rebalance timeout of every member in `UnrevokedTasks`, as
    /// Kafka's `onLoaded` does for a loaded group.
    pub fn arm_loaded_rebalance_timeouts(&mut self, now: Instant) {
        let unrevoked: Vec<String> = self
            .members
            .values()
            .filter(|member| {
                member.assignment_state == StreamsMemberAssignmentState::UnrevokedTasks
            })
            .map(|member| member.member_id.clone())
            .collect();
        for member_id in unrevoked {
            self.track_rebalance_timeout(&member_id, now);
        }
    }

    /// Kafka's `StreamsGroup.maybeUpdateGroupState` for a group with a ready
    /// topology: `Empty` with no members, `Assigning` while the target
    /// assignment is behind the group epoch, `Reconciling` while a member is
    /// not reconciled to the assignment epoch, and `Stable` otherwise. A
    /// `NotReady` group stays `NotReady` until a target is computed.
    pub fn refresh_phase(&mut self) {
        if self.members.is_empty() {
            self.phase = StreamsGroupStatePhase::Empty;
            return;
        }
        if self.phase == StreamsGroupStatePhase::NotReady {
            return;
        }
        if self.group_epoch > self.target.epoch {
            self.phase = StreamsGroupStatePhase::Assigning;
            return;
        }
        let reconciled = self.members.values().all(|member| {
            member.assignment_state == StreamsMemberAssignmentState::Stable
                && member.member_epoch == self.target.epoch
        });
        self.phase = if reconciled {
            StreamsGroupStatePhase::Stable
        } else {
            StreamsGroupStatePhase::Reconciling
        };
    }
}

/// The tasks that a `StreamsGroupHeartbeat` reports as owned: `None` when the
/// request leaves the list out.
#[derive(Debug, Clone, Copy, Default)]
pub struct OwnedTasks<'a> {
    pub active: Option<&'a BTreeMap<String, Vec<i32>>>,
    pub standby: Option<&'a BTreeMap<String, Vec<i32>>>,
    pub warmup: Option<&'a BTreeMap<String, Vec<i32>>>,
}

/// Kafka's `areOwnedTasksContainedInAssignedTasks`: every owned partition of
/// every owned subtopology is assigned. An absent list is not contained.
fn tasks_contained(
    owned: Option<&BTreeMap<String, Vec<i32>>>,
    assigned: &BTreeMap<String, Vec<i32>>,
) -> bool {
    owned.is_some_and(|owned| {
        owned.iter().all(|(subtopology, partitions)| {
            assigned.get(subtopology).is_some_and(|assigned| {
                partitions
                    .iter()
                    .all(|partition| assigned.contains(partition))
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{test_support::task_map, *};

    fn clean_member_group() -> StreamsGroupState {
        let mut group = StreamsGroupState::new("g");
        group.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        group.dirty = false;
        group
    }

    #[test]
    fn add_member_marks_dirty_first_time() {
        let mut g = StreamsGroupState::new("g");
        assert!(!g.dirty);
        g.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        assert!(g.members.len() == 1);
        assert!(g.dirty);
    }

    #[test]
    fn re_add_identical_member_keeps_clean() {
        let mut g = clean_member_group();
        // Re-add a member with the same id and same topology epoch.
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.topology_epoch = 0;
        g.add_or_update_member(m);
        assert!(!g.dirty);
    }

    #[test]
    fn topology_epoch_change_marks_dirty() {
        let mut g = clean_member_group();
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.topology_epoch = 3;
        g.add_or_update_member(m);
        assert!(g.dirty);
    }

    #[test]
    fn remove_member_marks_dirty() {
        let mut g = clean_member_group();
        let removed = g.remove_member("m1");
        assert!(removed.is_some());
        assert!(g.dirty);
        // Removing a now-absent member does not re-dirty.
        g.dirty = false;
        assert!(g.remove_member("m1").is_none());
        assert!(!g.dirty);
    }

    /// Kafka 4.3's `StreamsGroup` constructor starts a group at group epoch 1
    /// and `TargetAssignmentMetadata.INITIAL`, target assignment epoch 1.
    #[test]
    fn a_new_group_starts_at_kafkas_initial_epochs() {
        let g = StreamsGroupState::new("g");
        check!(
            (g.group_epoch, g.assignment_epoch, g.target.epoch, g.phase)
                == (1, 1, 1, StreamsGroupStatePhase::Empty)
        );
    }

    #[test]
    fn bump_epoch_increments_and_dirties() {
        let mut g = StreamsGroupState::new("g");
        g.dirty = false;
        assert!(g.bump_epoch());
        assert!(g.group_epoch == 2);
        assert!(g.dirty);
    }

    #[test]
    fn bump_epoch_rejects_exhaustion() {
        let mut group = StreamsGroupState::new("g");
        group.group_epoch = i32::MAX;

        assert!(!group.bump_epoch());
        assert!(group.group_epoch == i32::MAX);
    }

    #[test]
    fn evict_expired_removes_and_returns_ids() {
        let mut g = StreamsGroupState::new("g");
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        // Anchor `last_seen` at "now"; evaluate eviction slightly in the future
        // so we never subtract from an `Instant` (underflows on low-uptime CI).
        m.last_seen = Instant::now();
        g.add_or_update_member(m);
        g.add_or_update_member(StreamsMemberState::joining("m2", "c1", "h1"));
        g.dirty = false;

        // Within the timeout: nothing evicted, stays clean.
        let recent = Instant::now() + Duration::from_millis(50);
        let kept = g.evict_expired(recent, Duration::from_secs(45));
        check!(kept.is_empty());
        check!(g.members.len() == 2);
        check!(!g.dirty);

        // Timeout shrinks below the silence: both overdue, dirty flips.
        let later = Instant::now() + Duration::from_millis(50);
        let mut evicted = g.evict_expired(later, Duration::from_millis(1));
        evicted.sort();
        check!(evicted == vec!["m1".to_string(), "m2".to_string()]);
        check!(g.members.is_empty());
        check!(g.dirty);
    }

    /// A member of the table: its process id, state, epoch, and assigned and
    /// pending tasks of subtopology `s`, active then standby.
    struct Member {
        id: &'static str,
        process: &'static str,
        state: StreamsMemberAssignmentState,
        epoch: i32,
        active: &'static [i32],
        standby: &'static [i32],
        active_pending: &'static [i32],
        standby_pending: &'static [i32],
    }

    const fn stable(id: &'static str, process: &'static str, epoch: i32) -> Member {
        Member {
            id,
            process,
            state: StreamsMemberAssignmentState::Stable,
            epoch,
            active: &[],
            standby: &[],
            active_pending: &[],
            standby_pending: &[],
        }
    }

    fn tasks_of(partitions: &[i32]) -> BTreeMap<String, Vec<i32>> {
        if partitions.is_empty() {
            BTreeMap::new()
        } else {
            task_map(&[("s", partitions)])
        }
    }

    type Expected = (
        bool,
        StreamsMemberAssignmentState,
        i32,
        &'static [i32],
        &'static [i32],
        &'static [i32],
        &'static [i32],
    );

    /// One row: the name, `m1`, another member, the target active and standby
    /// tasks of `m1`, the owned active and standby tasks of its heartbeat or
    /// `None`, and the expected (changed, state, epoch, active, standby, active
    /// pending, standby pending) of `m1`.
    type BuilderRow = (
        &'static str,
        Member,
        Option<Member>,
        &'static [i32],
        &'static [i32],
        Option<(&'static [i32], &'static [i32])>,
        Expected,
    );

    fn unrevoked_member() -> Member {
        Member {
            state: StreamsMemberAssignmentState::UnrevokedTasks,
            active: &[0],
            active_pending: &[1],
            ..stable("m1", "p1", 1)
        }
    }

    fn builder_rows() -> Vec<BuilderRow> {
        use StreamsMemberAssignmentState::{Stable, UnreleasedTasks, UnrevokedTasks};

        vec![
            (
                "a stable member takes the new tasks at the target epoch",
                Member {
                    active: &[0],
                    ..stable("m1", "p1", 1)
                },
                None,
                &[0, 1],
                &[],
                Some((&[0], &[])),
                (true, Stable, 2, &[0, 1], &[], &[], &[]),
            ),
            (
                "a member keeps its epoch while it owns a revoked task",
                Member {
                    active: &[0, 1],
                    ..stable("m1", "p1", 1)
                },
                None,
                &[0],
                &[],
                Some((&[0, 1], &[])),
                (true, UnrevokedTasks, 1, &[0], &[], &[1], &[]),
            ),
            (
                "a member that reports no tasks has nothing to revoke",
                Member {
                    active: &[0, 1],
                    ..stable("m1", "p1", 1)
                },
                None,
                &[0],
                &[],
                Some((&[], &[])),
                (true, Stable, 2, &[0], &[], &[], &[]),
            ),
            (
                "an unrevoked member moves on once it stops reporting the task",
                unrevoked_member(),
                None,
                &[0],
                &[],
                Some((&[0], &[])),
                (true, Stable, 2, &[0], &[], &[], &[]),
            ),
            (
                "an unrevoked member that still reports the task waits",
                unrevoked_member(),
                None,
                &[0],
                &[],
                Some((&[0, 1], &[])),
                (false, UnrevokedTasks, 1, &[0], &[], &[1], &[]),
            ),
            (
                "an unrevoked member without owned tasks waits",
                unrevoked_member(),
                None,
                &[0],
                &[],
                None,
                (false, UnrevokedTasks, 1, &[0], &[], &[1], &[]),
            ),
            (
                "an active task that another member still owns is held back",
                stable("m1", "p1", 1),
                Some(Member {
                    state: UnrevokedTasks,
                    active_pending: &[1],
                    ..stable("m2", "p2", 1)
                }),
                &[0, 1],
                &[],
                Some((&[], &[])),
                (true, UnreleasedTasks, 2, &[0], &[], &[], &[]),
            ),
            (
                "a standby task that the same process runs as active is held back",
                stable("m1", "p1", 1),
                Some(Member {
                    active: &[0],
                    ..stable("m2", "p1", 1)
                }),
                &[],
                &[0],
                Some((&[], &[])),
                (true, UnreleasedTasks, 2, &[], &[], &[], &[]),
            ),
            (
                "a standby task that another process runs as active is given",
                stable("m1", "p1", 1),
                Some(Member {
                    active: &[0],
                    ..stable("m2", "p2", 1)
                }),
                &[],
                &[0],
                Some((&[], &[])),
                (true, Stable, 2, &[], &[0], &[], &[]),
            ),
            (
                "a revoked standby task waits for its release",
                Member {
                    standby: &[0],
                    ..stable("m1", "p1", 1)
                },
                None,
                &[],
                &[],
                Some((&[], &[0])),
                (true, UnrevokedTasks, 1, &[], &[], &[], &[0]),
            ),
        ]
    }

    /// Kafka's `CurrentAssignmentBuilder`. Each row puts `m1` and maybe a
    /// second member in a group with a target at epoch 2, reconciles `m1`
    /// with the owned tasks of its heartbeat, and compares the whole
    /// reconciliation state of `m1` afterwards.
    #[test]
    fn reconcile_member_follows_kafka_current_assignment_builder() {
        let rows = builder_rows();
        for (name, m1, other, target_active, target_standby, owned, expected) in rows {
            let mut group = StreamsGroupState::new("g");
            group.group_epoch = 2;
            for member in std::iter::once(m1).chain(other) {
                let mut state = StreamsMemberState::joining(member.id, "c", "h");
                state.process_id = member.process.to_string();
                state.assignment_state = member.state;
                state.member_epoch = member.epoch;
                state.active = tasks_of(member.active);
                state.standby = tasks_of(member.standby);
                state.active_pending_revocation = tasks_of(member.active_pending);
                state.standby_pending_revocation = tasks_of(member.standby_pending);
                group.members.insert(member.id.to_string(), state);
            }
            let mut target = StreamsTargetAssignment::default();
            target.active.insert("m1".into(), tasks_of(target_active));
            target.standby.insert("m1".into(), tasks_of(target_standby));
            group.install_target(target);
            let owned = owned.map(|(active, standby)| RoleTasks {
                active: tasks_of(active),
                standby: tasks_of(standby),
                warmup: BTreeMap::new(),
            });

            let changed = group.reconcile_member("m1", owned.as_ref());

            let m1 = &group.members["m1"];
            let (
                e_changed,
                e_state,
                e_epoch,
                e_active,
                e_standby,
                e_active_pending,
                e_standby_pending,
            ) = expected;
            check!(
                (
                    changed,
                    m1.assignment_state,
                    m1.member_epoch,
                    m1.active.clone(),
                    m1.standby.clone(),
                    m1.active_pending_revocation.clone(),
                    m1.standby_pending_revocation.clone(),
                ) == (
                    e_changed,
                    e_state,
                    e_epoch,
                    tasks_of(e_active),
                    tasks_of(e_standby),
                    tasks_of(e_active_pending),
                    tasks_of(e_standby_pending),
                ),
                "{name}"
            );
        }
    }

    /// Kafka's `scheduleStreamsGroupRebalanceTimeout`: a member that enters
    /// `UnrevokedTasks` must revoke within its rebalance timeout, or it is
    /// removed. A member that left that state is not.
    #[test]
    fn a_member_past_its_rebalance_timeout_is_fenced() {
        let rows = [
            (
                "still unrevoked at the deadline",
                false,
                vec!["m1".to_string()],
            ),
            ("revoked before the deadline", true, vec![]),
        ];
        for (name, revokes, fenced) in rows {
            let mut group = StreamsGroupState::new("g");
            let mut m1 = StreamsMemberState::joining("m1", "c", "h");
            m1.member_epoch = 1;
            m1.rebalance_timeout_ms = 100;
            m1.active = tasks_of(&[0, 1]);
            group.members.insert("m1".into(), m1);
            group.group_epoch = 2;
            let mut target = StreamsTargetAssignment::default();
            target.active.insert("m1".into(), tasks_of(&[0]));
            group.install_target(target);
            let now = Instant::now();
            let owned = |active: &[i32]| RoleTasks {
                active: tasks_of(active),
                ..RoleTasks::default()
            };

            check!(
                group.reconcile_member("m1", Some(&owned(&[0, 1]))),
                "{name}"
            );
            group.track_rebalance_timeout("m1", now);
            if revokes && group.reconcile_member("m1", Some(&owned(&[0]))) {
                group.track_rebalance_timeout("m1", now);
            }

            check!(
                group
                    .fence_rebalance_timeouts(now + Duration::from_millis(99))
                    .is_empty(),
                "{name}"
            );
            check!(
                group.fence_rebalance_timeouts(now + Duration::from_millis(100)) == fenced,
                "{name}"
            );
        }
    }
}
