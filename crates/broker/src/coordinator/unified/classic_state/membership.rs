//! Membership transitions on a [`ClassicGroup`]: adding, removing, and
//! session-timeout expiry of members.
//!
//! These carry the KIP-345 static-membership rules, where a rejoin inside the
//! session timeout replaces a static slot in place, and the per-round join
//! bookkeeping the `JoinGroup` handler reads.

use std::time::{Duration, Instant};

use bytes::Bytes;

use super::{
    group::{ClassicGroup, GroupState},
    member::{AddMemberOutcome, Member},
};

/// Kafka's `CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS`: how long a member that
/// joined in the current round may wait in `JoinGroup` before it expires. An
/// existing member that waits in `JoinGroup` does not expire.
pub const NEW_MEMBER_JOIN_TIMEOUT: Duration = Duration::from_mins(5);

impl ClassicGroup {
    /// Kafka's `ClassicGroup.addPendingMember`: remember a member id that a
    /// `JoinGroup` v4+ answered with `MEMBER_ID_REQUIRED`, until `expires_at`.
    pub fn add_pending_member(&mut self, member_id: String, expires_at: Instant) {
        self.pending_members.insert(member_id, expires_at);
    }

    /// Kafka's `ClassicGroup.add`, without the rebalance. It pins the
    /// member's instance id, takes the member's protocol type when the group
    /// has no members, makes the member the leader when the group has none,
    /// and clears its pending id. The member does not count as joined until
    /// [`Self::mark_awaiting_join`].
    pub fn insert_joining_member(&mut self, member: Member, protocol_type: &str) {
        if let Some(instance_id) = &member.group_instance_id {
            self.static_members
                .insert(instance_id.clone(), member.id.clone());
        }
        if self.members.is_empty() {
            self.protocol_type = Some(protocol_type.to_string());
        }
        if self.leader_id.is_none() {
            self.leader_id = Some(member.id.clone());
        }
        self.pending_members.remove(&member.id);
        self.members.insert(member.id.clone(), member);
    }

    /// Kafka's `ClassicGroup.updateMember`, without the join future: the
    /// member's protocols and timeouts become the ones of its `JoinGroup`.
    pub fn update_joining_member(
        &mut self,
        member_id: &str,
        protocols: Vec<(String, Bytes)>,
        rebalance_timeout: Duration,
        session_timeout: Duration,
    ) {
        if let Some(member) = self.members.get_mut(member_id) {
            member.protocol_metadata = protocols
                .first()
                .map(|(_, b)| b.clone())
                .unwrap_or_default();
            member.protocols = protocols;
            member.rebalance_timeout = rebalance_timeout;
            member.session_timeout = session_timeout;
        }
    }

    /// Records that `member_id` waits in `JoinGroup` for this round, Kafka's
    /// `isAwaitingJoin`.
    pub fn mark_awaiting_join(&mut self, member_id: &str) {
        self.joined_this_round.insert(member_id.to_string());
    }

    /// Kafka's `ClassicGroup.replaceStaticMember`. The member of `instance_id`
    /// moves from `old_member_id` to `new_member_id` with everything else
    /// kept, the assignment included. The leader follows it, and the old id
    /// is gone, so a request that still carries it is fenced.
    pub fn replace_static_member(
        &mut self,
        instance_id: &str,
        old_member_id: &str,
        new_member_id: &str,
    ) {
        let Some(mut member) = self.members.remove(old_member_id) else {
            return;
        };
        self.joined_this_round.remove(old_member_id);
        // The replacement owes the `SyncGroup` the old id did.
        if self.pending_sync_members.remove(old_member_id) {
            self.pending_sync_members.insert(new_member_id.to_string());
        }
        member.id = new_member_id.to_string();
        member.is_new = false;
        self.members.insert(new_member_id.to_string(), member);
        if self.leader_id.as_deref() == Some(old_member_id) {
            self.leader_id = Some(new_member_id.to_string());
        }
        self.static_members
            .insert(instance_id.to_string(), new_member_id.to_string());
    }

    /// Add or refresh a member.
    ///
    /// **Dynamic** (no `group_instance_id`): the group inserts the member and
    /// moves to `PreparingRebalance` if it was `Empty` or `Stable` before. It
    /// returns [`AddMemberOutcome::NewMember`].
    ///
    /// **Static** (KIP-345, `group_instance_id` set): three cases.
    /// 1. The instance id is new. This behaves like a dynamic add and returns
    ///    [`AddMemberOutcome::NewMember`].
    /// 2. The instance id maps to an existing live `member_id` that matches
    ///    the incoming `member.member_id`. It also matches when the incoming
    ///    id differs but the leader-side bootstrap-rejoin path supplied a
    ///    fresh id for the same instance. The group replaces the slot in
    ///    place and keeps the prior `assignment` and the group's `state`, so a
    ///    `Stable` rejoin triggers no rebalance. It returns
    ///    [`AddMemberOutcome::StaticRejoin`] with the prior member id.
    /// 3. The instance id maps to a different live `member_id` and the caller
    ///    did not ask for a takeover. The caller must then reject the request
    ///    with `FENCED_INSTANCE_ID`, and this method returns
    ///    [`AddMemberOutcome::Fenced`]. The handler decides whether a
    ///    non-empty mismatched `req.member_id` is a real fence or a valid
    ///    replacement. `add_member` itself always does the takeover unless the
    ///    caller checks first with
    ///    [`Self::current_member_id_for_instance`].
    pub fn add_member(&mut self, member: Member) -> AddMemberOutcome {
        if let Some(instance_id) = member.group_instance_id.clone() {
            if let Some(prior_member_id) = self.static_members.get(&instance_id).cloned() {
                // Static rejoin: replace slot in-place, preserve assignment.
                let prior = self.members.remove(&prior_member_id);
                let mut next = member;
                if let Some(p) = prior {
                    // Inherit the previously installed assignment so a
                    // Stable-state rejoin can short-circuit the rebalance.
                    if next.assignment.is_none() {
                        next.assignment = p.assignment;
                    }
                }
                let new_member_id = next.id.clone();
                self.static_members
                    .insert(instance_id, new_member_id.clone());
                self.members.insert(new_member_id.clone(), next);
                // If this static rejoin lands during PreparingRebalance,
                // count the (potentially renamed) member as joined so the
                // early-completion check still fires.
                if matches!(self.state, GroupState::PreparingRebalance) {
                    self.joined_this_round.remove(&prior_member_id);
                    self.joined_this_round.insert(new_member_id);
                }
                // Crucially: do NOT touch self.state. Static rejoin from
                // Stable stays Stable; from PreparingRebalance stays
                // PreparingRebalance.
                return AddMemberOutcome::StaticRejoin { prior_member_id };
            }
            // Brand-new instance id: pin it and fall through to a
            // dynamic-style add.
            self.static_members.insert(instance_id, member.id.clone());
        }
        let was_empty = matches!(self.state, GroupState::Empty);
        // Also restart a rebalance when a new member joins while stuck in
        // CompletingRebalance. Real Kafka (KIP-62 AwaitingSync) transitions
        // back to PreparingRebalance on any new join so a dead leader's
        // CompletingRebalance deadlock doesn't strand the group forever.
        let was_first_or_stable = matches!(
            self.state,
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance
        );
        let member_id = member.id.clone();
        self.members.insert(member_id.clone(), member);
        if was_first_or_stable {
            self.state = GroupState::PreparingRebalance;
            self.joined_this_round.clear();
            // A round that opens from `Empty` batches the startup herd over
            // the configured initial delay (see `rebalance_from_empty`); one that
            // opens from `Stable` is a live-membership change and
            // eager-completes once every still-live member rejoins.
            self.rebalance_from_empty = was_empty;
        }
        if matches!(self.state, GroupState::PreparingRebalance) {
            self.joined_this_round.insert(member_id);
        }
        AddMemberOutcome::NewMember
    }

    /// Removes a member. The group moves to `Empty` if no member remains.
    /// KIP-345: this also clears the static-membership index entry.
    pub fn remove_member(&mut self, member_id: &str) {
        if let Some(m) = self.members.remove(member_id)
            && let Some(ref iid) = m.group_instance_id
        {
            // Only clear the index entry if it still points at *this*
            // member_id — a takeover may have already repointed it.
            if self.static_members.get(iid).map(String::as_str) == Some(member_id) {
                self.static_members.remove(iid);
            }
        }
        self.joined_this_round.remove(member_id);
        self.pending_members.remove(member_id);
        self.pending_sync_members.remove(member_id);
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol_name = None;
            self.rebalance_deadline = None;
            self.clear_sync_expiration();
        }
    }

    /// Kafka's `expireClassicGroupMemberHeartbeat` on a pending member: a
    /// `MEMBER_ID_REQUIRED` id that never joined goes away after the session
    /// timeout of the request that got it. It returns the expired ids so the
    /// caller can complete a join phase they were the last to block, as
    /// Kafka's `removePendingMemberAndUpdateClassicGroup` does.
    pub fn expire_pending_members(&mut self, now: Instant) -> Vec<String> {
        let mut expired: Vec<String> = self
            .pending_members
            .iter()
            .filter(|(_, expires_at)| now > **expires_at)
            .map(|(member_id, _)| member_id.clone())
            .collect();
        expired.sort_unstable();
        for member_id in &expired {
            self.pending_members.remove(member_id);
        }
        expired
    }

    /// Drops every member whose `last_heartbeat` is older than its
    /// `session_timeout`. It returns the dropped member IDs. The group moves
    /// to `Empty` when it became empty. When members remain, a `Stable` or
    /// `CompletingRebalance` group prepares a rebalance and waits for the
    /// survivors for the group rebalance timeout, the way Kafka's
    /// `removeMemberAndUpdateClassicGroup` reaches `prepareRebalance`. The
    /// initial rebalance delay applies only to a round that opens from
    /// `Empty`, and this one does not.
    ///
    /// KIP-345: a static member, one with `group_instance_id.is_some()`,
    /// expires like a dynamic one, and its `static_members` entry goes with
    /// it. Kafka's `GroupMetadataManager.expireClassicGroupMemberHeartbeat`
    /// has no static exception. A static client that restarts inside its
    /// session timeout still takes its slot back without a rebalance; a
    /// static client that does not come back frees its partitions.
    pub fn expire_dead_members(
        &mut self,
        now: Instant,
        initial_rebalance_delay: Duration,
    ) -> Vec<String> {
        // Expire members in all active states (PreparingRebalance,
        // CompletingRebalance, Stable). Empty and Dead have no members to
        // expire. Importantly, expiring in CompletingRebalance lets the broker
        // evict a dead group leader that will never send SyncGroup — otherwise
        // the group is permanently stuck: expire_dead_members only ran in
        // Stable (post-SyncGroup), so a dead leader stranded the group forever.
        //
        // The original Stable-only guard existed to prevent a race where a
        // member with a very small session_timeout could self-evict before its
        // JoinGroup returned (last_heartbeat = add_member time, session_timeout
        // equal to the initial rebalance delay). With the default session timeout
        // 45s and a 3s rebalance delay this race is impossible in practice, and
        // real Kafka expires members regardless of group state.
        if self.state == GroupState::Empty {
            return Vec::new();
        }
        let dropped: Vec<String> = self
            .members
            .iter()
            .filter(|(id, m)| !self.has_satisfied_heartbeat(id, m, now))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dropped {
            // `remove_member` also clears the member's `static_members` entry
            // and its `joined_this_round` mark, so a member expired
            // mid-`PreparingRebalance` does not linger as a ghost that
            // `all_members_joined_this_round` counts.
            self.remove_member(id);
        }
        if !dropped.is_empty() {
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.leader_id = None;
                self.protocol_name = None;
                self.rebalance_deadline = None;
                self.joined_this_round.clear();
                self.rebalance_from_empty = false;
                self.clear_sync_expiration();
            } else if self.can_rebalance() {
                // Kafka's `maybePrepareRebalanceOrCompleteJoin`: the survivors
                // of a `Stable` or `CompletingRebalance` group get the whole
                // group rebalance timeout to rejoin, and the round completes
                // early once they all have.
                self.prepare_rebalance(initial_rebalance_delay, now);
            } else {
                // A member timed out during a round that is already open.
                // This is a live-membership change, not a start-from-empty
                // herd: eager-complete once the survivors rejoin rather than
                // holding the initial-delay window.
                self.rebalance_from_empty = false;
                if self.rebalance_deadline.is_none() {
                    self.rebalance_deadline = Some(now + self.rebalance_timeout());
                }
            }
        }
        dropped
    }

    /// Kafka's `ClassicGroupMember.hasSatisfiedHeartbeat` combined with its
    /// heartbeat deadline. A member that joined in this round has the
    /// new-member join timeout. An existing member that waits in `JoinGroup`
    /// cannot heartbeat and does not expire while it waits. Any other member
    /// has its session timeout.
    fn has_satisfied_heartbeat(&self, member_id: &str, member: &Member, now: Instant) -> bool {
        let since = now.saturating_duration_since(member.last_heartbeat);
        if member.is_new {
            since <= NEW_MEMBER_JOIN_TIMEOUT
        } else if self.state == GroupState::PreparingRebalance
            && self.joined_this_round.contains(member_id)
        {
            true
        } else {
            since <= member.session_timeout
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::classic_state::test_support::{sample_member, static_member};

    #[test]
    fn empty_to_preparing_on_first_join() {
        let mut g = ClassicGroup::new("g");
        assert!(g.state == GroupState::Empty);
        g.add_member(sample_member("m1"));
        assert!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn remove_last_member_empties_group() {
        let mut g = ClassicGroup::new("g");
        g.add_member(sample_member("m1"));
        g.remove_member("m1");
        assert!(g.state == GroupState::Empty);
        assert!(g.leader_id.is_none());
    }

    #[test]
    fn static_rejoin_preserves_stable_state_and_assignment() {
        let mut g = ClassicGroup::new("g");
        let outcome = g.add_member(static_member("m1", "inst-a"));
        assert!(outcome == AddMemberOutcome::NewMember);
        super::super::test_support::stabilize_m1(&mut g);

        // Rejoin with the same instance id but a fresh `member_id` (the
        // client restarted; KIP-394 bootstrap gave it a new id).
        let outcome = g.add_member(static_member("m2", "inst-a"));
        check!(
            outcome
                == AddMemberOutcome::StaticRejoin {
                    prior_member_id: "m1".into()
                }
        );
        // State preserved: no rebalance kicked off.
        check!(g.state == GroupState::Stable);
        check!(g.members.len() == 1);
        // New member inherited the prior assignment.
        check!(g.members.contains_key("m2"));
        check!(g.members["m2"].assignment.as_deref() == Some(b"assignment-bytes" as &[u8]));
        // Index repointed.
        check!(g.current_member_id_for_instance("inst-a") == Some("m2"));
    }

    /// Kafka expires a static member on session timeout like a dynamic one
    /// (`GroupMetadataManager.expireClassicGroupMemberHeartbeat`), removes its
    /// instance id from the static index (`ClassicGroup.remove`), and
    /// rebalances the rest of the group.
    #[test]
    fn session_timeout_expires_static_and_dynamic_members() {
        struct Row {
            name: &'static str,
            instance_id: Option<&'static str>,
            with_survivor: bool,
            timed_out: bool,
            dropped: Vec<String>,
            members: Vec<&'static str>,
            state: GroupState,
            index: Option<&'static str>,
        }
        let rows = [
            Row {
                name: "static member alone, timed out",
                instance_id: Some("inst-a"),
                with_survivor: false,
                timed_out: true,
                dropped: vec!["m1".into()],
                members: vec![],
                state: GroupState::Empty,
                index: None,
            },
            Row {
                name: "static member with a survivor, timed out",
                instance_id: Some("inst-a"),
                with_survivor: true,
                timed_out: true,
                dropped: vec!["m1".into()],
                members: vec!["survivor"],
                state: GroupState::PreparingRebalance,
                index: None,
            },
            Row {
                name: "static member inside its session",
                instance_id: Some("inst-a"),
                with_survivor: true,
                timed_out: false,
                dropped: vec![],
                members: vec!["m1", "survivor"],
                state: GroupState::Stable,
                index: Some("m1"),
            },
            Row {
                name: "dynamic member with a survivor, timed out",
                instance_id: None,
                with_survivor: true,
                timed_out: true,
                dropped: vec!["m1".into()],
                members: vec!["survivor"],
                state: GroupState::PreparingRebalance,
                index: None,
            },
        ];
        for row in rows {
            let mut g = ClassicGroup::new("g");
            let mut m = sample_member("m1").with_instance_id(row.instance_id.map(str::to_string));
            m.session_timeout = Duration::from_millis(1);
            if row.timed_out {
                m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
            } else {
                m.session_timeout = Duration::from_mins(1);
            }
            g.add_member(m);
            if row.with_survivor {
                g.add_member(sample_member("survivor"));
            }
            g.complete_rebalance("range");
            g.state = GroupState::Stable;

            let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));

            let mut members: Vec<&str> = g.members.keys().map(String::as_str).collect();
            members.sort_unstable();
            check!(dropped == row.dropped, "{}", row.name);
            check!(members == row.members, "{}", row.name);
            check!(g.state == row.state, "{}", row.name);
            check!(
                g.current_member_id_for_instance("inst-a") == row.index,
                "{}",
                row.name
            );
        }
    }

    #[test]
    fn dynamic_member_timeout_still_drops_in_mixed_group() {
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("static-1", "inst-a"));
        let mut dyn_m = sample_member("dyn-1");
        dyn_m.session_timeout = Duration::from_millis(1);
        dyn_m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(dyn_m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));
        check!(dropped == vec!["dyn-1".to_string()]);
        check!(g.state == GroupState::PreparingRebalance);
        check!(g.members.contains_key("static-1"));
    }

    /// Kafka's `removeMemberAndUpdateClassicGroup` reaches `prepareRebalance`,
    /// which waits for the survivors for `group.rebalanceTimeoutMs()`, the
    /// largest member rebalance timeout. `group.initial.rebalance.delay.ms`
    /// applies only to a round that opens from `Empty`.
    #[test]
    fn session_expiry_gives_survivors_the_group_rebalance_timeout() {
        for (name, from_stable) in [("completing", false), ("stable", true)] {
            let mut g = ClassicGroup::new("g");
            let mut stale = sample_member("stale");
            stale.session_timeout = Duration::from_millis(1);
            stale.rebalance_timeout = Duration::from_mins(10);
            stale.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
            g.add_member(stale);
            let mut survivor = sample_member("survivor");
            survivor.rebalance_timeout = Duration::from_secs(90);
            g.add_member(survivor);
            g.complete_rebalance("range");
            if from_stable {
                g.install_assignments(HashMap::new());
            }
            check!(g.rebalance_deadline.is_none(), "{name}");

            let delay = Duration::from_millis(19);
            let now = Instant::now();
            let dropped = g.expire_dead_members(now, delay);

            check!(dropped == vec!["stale".to_string()], "{name}");
            check!(g.state == GroupState::PreparingRebalance, "{name}");
            check!(!g.rebalance_from_empty, "{name}");
            check!(g.initial_join.is_none(), "{name}");
            check!(
                g.rebalance_deadline == Some(now + Duration::from_secs(90)),
                "{name}"
            );
        }
    }

    #[test]
    fn remove_static_member_clears_index() {
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("m1", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m1"));
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == None);
        assert!(g.state == GroupState::Empty);
    }

    #[test]
    fn static_takeover_does_not_clear_index_for_prior_id() {
        // After a takeover, the index points at the new id. Removing the
        // *prior* id (e.g. via a stale LeaveGroup from the old session)
        // must NOT wipe the index entry.
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("m1", "inst-a"));
        g.add_member(static_member("m2", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        // m1 is no longer in members (replaced), so this is a no-op.
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn expire_dead_members_drops_stale() {
        let mut g = ClassicGroup::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;
        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));
        assert!(dropped == vec!["m1".to_string()]);
        assert!(g.state == GroupState::Empty);
    }

    #[test]
    fn expire_last_dead_member_clears_rebalance_bookkeeping() {
        let mut g = ClassicGroup::new("g");
        // A new member that waits in `JoinGroup` past the new-member join
        // timeout.
        let mut m = sample_member("m1");
        m.is_new = true;
        m.last_heartbeat = Instant::now()
            .checked_sub(NEW_MEMBER_JOIN_TIMEOUT + Duration::from_secs(1))
            .unwrap();
        g.add_member(m);
        g.rebalance_deadline = Some(Instant::now() + Duration::from_secs(3));
        g.rebalance_from_empty = true;
        assert!(g.joined_this_round.contains("m1"));

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));

        check!(dropped == vec!["m1".to_string()]);
        check!(g.state == GroupState::Empty);
        check!(g.leader_id.is_none());
        check!(g.protocol_name.is_none());
        check!(g.rebalance_deadline.is_none());
        check!(g.joined_this_round.is_empty());
        check!(!g.rebalance_from_empty);
    }

    /// #797: Kafka's `hasSatisfiedHeartbeat`. A member that waits in
    /// `JoinGroup` cannot heartbeat: an existing one does not expire while it
    /// waits, and a new one has the new-member join timeout.
    #[test]
    fn members_waiting_in_join_group_expire_as_kafka_does() {
        let session = Duration::from_secs(10);
        for (name, is_new, awaiting, elapsed, dropped) in [
            (
                "existing, waiting, past session",
                false,
                true,
                Duration::from_secs(60),
                false,
            ),
            (
                "existing, not waiting, past session",
                false,
                false,
                Duration::from_secs(60),
                true,
            ),
            (
                "existing, not waiting, in session",
                false,
                false,
                Duration::from_secs(5),
                false,
            ),
            (
                "new, waiting, past session",
                true,
                true,
                Duration::from_secs(60),
                false,
            ),
            (
                "new, waiting, past new-member timeout",
                true,
                true,
                NEW_MEMBER_JOIN_TIMEOUT + Duration::from_secs(1),
                true,
            ),
        ] {
            let mut g = ClassicGroup::new("g");
            g.add_member(sample_member("keeper"));
            let mut m = sample_member("m1");
            m.is_new = is_new;
            m.session_timeout = session;
            let now = Instant::now();
            m.last_heartbeat = now.checked_sub(elapsed).unwrap();
            g.add_member(m);
            if !awaiting {
                g.joined_this_round.remove("m1");
            }
            let want: Vec<String> = if dropped { vec!["m1".into()] } else { vec![] };
            check!(
                g.expire_dead_members(now, Duration::from_secs(3)) == want,
                "{name}"
            );
        }
    }

    /// A `MEMBER_ID_REQUIRED` id that never joins goes away after its
    /// session timeout, and the expiry is reported so that the caller can
    /// complete a join phase the id was blocking.
    #[test]
    fn pending_member_expires() {
        let mut g = ClassicGroup::new("g");
        let now = Instant::now();
        g.add_pending_member("late".into(), now);
        g.add_pending_member("waiting".into(), now + Duration::from_secs(10));
        let expired = g.expire_pending_members(now + Duration::from_secs(1));
        check!(expired == vec!["late".to_string()]);
        check!(g.pending_members.keys().collect::<Vec<_>>() == vec!["waiting"]);
    }
}
