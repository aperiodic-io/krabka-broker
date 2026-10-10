//! Target-assignment reconciliation for a next-gen consumer group: installing
//! a freshly computed target and turning it into one member's current
//! assignment.
//!
//! This is the KIP-848 `CurrentAssignmentBuilder`, the safety-critical half of
//! the group state. It decides which partitions a member keeps, which it must
//! revoke, and which it may claim, so that the broker never advertises one
//! partition to two members at once.

use std::collections::{BTreeSet, HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::{group::GroupState, member::MemberState};
use crate::coordinator::unified::{
    actor::MetadataProvider, member_helpers::new_target_assignment,
    persistence_next_gen::MemberAssignmentState,
};

/// The partitions each topic holds, by topic id.
type Partitions = HashMap<Uuid, Vec<i32>>;

/// The member that `CurrentAssignmentBuilder` builds from the current one.
struct Rebuilt {
    state: MemberAssignmentState,
    epoch: i32,
    assigned: Partitions,
    pending: Partitions,
    /// The partitions of `assigned` that the member did not hold before, so
    /// that they are assigned at `epoch`.
    granted: Partitions,
}

/// The topics a member subscribes to, as Kafka's `TopicIds` over
/// `CurrentAssignmentBuilder.subscribedTopicIds`: the names of the member and
/// the topics its regex resolved to. A topic is asked about by id, and it is
/// in the set when it exists and its name is one of them.
struct Subscription<'a> {
    group: &'a GroupState,
    member: &'a MemberState,
    metadata: &'a dyn MetadataProvider,
}

impl Subscription<'_> {
    fn contains(&self, topic_id: &Uuid) -> bool {
        self.metadata
            .topic_name(topic_id)
            .is_some_and(|name| self.group.member_subscribes_to(self.member, &name))
    }
}

impl GroupState {
    /// Installs a freshly computed target at the current group epoch.
    ///
    /// Only the target changes. Kafka reconciles a member inside that
    /// member's own heartbeat, so a target change never rewrites another
    /// member's assignment: the member is told about its new, smaller
    /// assignment by its own next heartbeat, and it stays behind the target
    /// epoch until it acknowledges the revocation. See [`Self::reconcile_member`].
    ///
    /// Every member gets a target, an empty one when the assignor gave it
    /// nothing, as Kafka's `TargetAssignmentBuilder.newMemberAssignment` does.
    /// It returns the members whose target differs from the one they held, a
    /// member that held none included, sorted: the members for which Kafka's
    /// builder writes a target assignment record.
    pub fn install_target(&mut self, per_member: HashMap<String, Partitions>) -> Vec<String> {
        let (target, changed) =
            new_target_assignment(self.members.keys(), per_member, &self.target.per_member);
        self.target.epoch = self.group_epoch;
        self.target.per_member = target;
        changed
    }

    /// Kafka's `GroupMetadataManager.maybeReconcile` and
    /// `CurrentAssignmentBuilder.build`: reconciles the current assignment of
    /// `member_id` towards the target, inside that member's heartbeat.
    ///
    /// `owned` is the `TopicPartitions` the heartbeat reports, and `None` when
    /// the heartbeat sent none. The Java client sends it only when its
    /// assignment changed, so `None` means "unchanged" and the member still
    /// owns everything it was assigned or asked to revoke
    /// (`ownsRevokedPartitions(null)` is true). `has_subscription_changed` is
    /// true when this heartbeat changed the member's subscription.
    /// `metadata` names the topics of the target and of the assignment, which
    /// the member's subscription is checked against.
    ///
    /// A member keeps every target partition it holds and moves the others to
    /// `partitions_pending_revocation`. It stays in its epoch, in the
    /// `UnrevokedPartitions` state, until it reports an owned set that omits
    /// them. Only then does it move to the target epoch and claim the target
    /// partitions that are free. A partition another member holds, assigned or
    /// pending revocation, stays withheld until that member lets it go, so the
    /// broker never advertises a partition to two members. That is the main
    /// KIP-848 safety property. See `reconciler_model.rs`.
    ///
    /// The member's subscription filters all of it. A partition of a topic
    /// the member no longer subscribes to is not in its target, whatever the
    /// installed target says, and a heartbeat that changed the subscription
    /// removes those partitions at once, without waiting for the next target
    /// (`updateCurrentAssignment`). The target of a group that waits for its
    /// assignment interval is stale in exactly that way.
    ///
    /// It returns `true` when the member's epoch, previous epoch, state,
    /// assignment or pending set changed. A member that is rebuilt records the
    /// epoch it leaves as its previous epoch, even when it stays in it, as
    /// Kafka's `ConsumerGroupMember.Builder.updateMemberEpoch` does.
    pub fn reconcile_member(
        &mut self,
        member_id: &str,
        owned: Option<&Partitions>,
        has_subscription_changed: bool,
        metadata: &dyn MetadataProvider,
    ) -> bool {
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        let target_epoch = self.target.epoch;
        // `isReconciledTo`: a stable member at the target epoch has nothing to do.
        if !has_subscription_changed
            && member.assignment_state == MemberAssignmentState::Stable
            && member.member_epoch == target_epoch
        {
            return false;
        }
        let subscription = Subscription {
            group: self,
            member,
            metadata,
        };
        let rebuilt = match member.assignment_state {
            MemberAssignmentState::Stable if member.member_epoch != target_epoch => {
                Some(self.compute_next_assignment(&subscription, target_epoch, owned))
            }
            // At the target epoch, and not reconciled only because the
            // subscription changed.
            MemberAssignmentState::Stable => Self::update_current_assignment(&subscription, owned),
            // A member that still owns the partitions it must revoke cannot move
            // forward, however the target changed meanwhile.
            MemberAssignmentState::UnrevokedPartitions
                if owns_any(owned, &member.partitions_pending_revocation) =>
            {
                if has_subscription_changed {
                    Self::update_current_assignment(&subscription, owned)
                } else {
                    None
                }
            }
            MemberAssignmentState::UnrevokedPartitions
            | MemberAssignmentState::UnreleasedPartitions => {
                Some(self.compute_next_assignment(&subscription, target_epoch, owned))
            }
        };
        let Some(rebuilt) = rebuilt else {
            return false;
        };

        let member = self
            .members
            .get_mut(member_id)
            .expect("member exists in reconcile_member");
        let left = member.member_epoch;
        let changed = member.previous_member_epoch != left
            || member.member_epoch != rebuilt.epoch
            || member.assignment_state != rebuilt.state
            || member.assigned_partitions != rebuilt.assigned
            || member.partitions_pending_revocation != rebuilt.pending;
        member.previous_member_epoch = left;
        member.member_epoch = rebuilt.epoch;
        member.assignment_state = rebuilt.state;
        member.assigned_partitions = rebuilt.assigned;
        member.partitions_pending_revocation = rebuilt.pending;
        // A partition granted now is assigned at this epoch, as Kafka stamps it
        // with the target assignment epoch, even one the member held before and
        // gave up. A partition it keeps, or moves to pending revocation, keeps
        // its epoch.
        for (topic_id, parts) in &rebuilt.granted {
            if let Some(epochs) = member.assignment_epochs.get_mut(topic_id) {
                for partition in parts {
                    epochs.remove(partition);
                }
            }
        }
        member.stamp_assignment_epochs(rebuilt.epoch);
        changed
    }

    /// Kafka's `CurrentAssignmentBuilder.updateCurrentAssignment`: removes the
    /// partitions of topics the member no longer subscribes to, and leaves the
    /// rest as it is. A partition that the member still owns moves to pending
    /// revocation, and the member waits for the client to release it. `None`
    /// when no topic was removed.
    fn update_current_assignment(
        subscription: &Subscription<'_>,
        owned: Option<&Partitions>,
    ) -> Option<Rebuilt> {
        let member = subscription.member;
        let mut assigned = member.assigned_partitions.clone();
        let mut pending = member.partitions_pending_revocation.clone();
        let mut removed = false;
        for (topic_id, parts) in &member.assigned_partitions {
            if subscription.contains(topic_id) {
                continue;
            }
            removed = true;
            assigned.remove(topic_id);
            let merged: BTreeSet<i32> = pending
                .get(topic_id)
                .into_iter()
                .flatten()
                .chain(parts)
                .copied()
                .collect();
            pending.insert(*topic_id, merged.into_iter().collect());
        }
        if !removed {
            return None;
        }
        Some(if !pending.is_empty() && owns_any(owned, &pending) {
            Rebuilt {
                state: MemberAssignmentState::UnrevokedPartitions,
                epoch: member.member_epoch,
                assigned,
                pending,
                granted: Partitions::new(),
            }
        } else {
            // The partitions were removed, and the client had released them
            // already: the member shrinks and keeps its state.
            Rebuilt {
                state: member.assignment_state,
                epoch: member.member_epoch,
                assigned,
                pending: member.partitions_pending_revocation.clone(),
                granted: Partitions::new(),
            }
        })
    }

    /// Kafka's `CurrentAssignmentBuilder.computeNextAssignment`.
    fn compute_next_assignment(
        &self,
        subscription: &Subscription<'_>,
        target_epoch: i32,
        owned: Option<&Partitions>,
    ) -> Rebuilt {
        let member = subscription.member;
        let no_target = Partitions::new();
        let target = self
            .target
            .per_member
            .get(&member.member_id)
            .unwrap_or(&no_target);

        let mut kept = Partitions::new();
        let mut to_revoke = Partitions::new();
        // The target partitions the member does not hold yet.
        let mut wanted = Partitions::new();
        let topic_ids: HashSet<Uuid> = target
            .keys()
            .chain(member.assigned_partitions.keys())
            .copied()
            .collect();
        for topic_id in topic_ids {
            // A topic the member no longer subscribes to has an empty target.
            let target_here: HashSet<i32> = if subscription.contains(&topic_id) {
                target
                    .get(&topic_id)
                    .map(|parts| parts.iter().copied().collect())
                    .unwrap_or_default()
            } else {
                HashSet::new()
            };
            let held: &[i32] = member
                .assigned_partitions
                .get(&topic_id)
                .map_or(&[], Vec::as_slice);
            for &partition in held {
                let side = if target_here.contains(&partition) {
                    &mut kept
                } else {
                    &mut to_revoke
                };
                side.entry(topic_id).or_default().push(partition);
            }
            let held: HashSet<i32> = held.iter().copied().collect();
            let new: Vec<i32> = target_here.difference(&held).copied().collect();
            if !new.is_empty() {
                wanted.insert(topic_id, new);
            }
        }

        // `currentPartitionEpoch`: what every other member still holds,
        // assigned or pending revocation. The member's own holdings need no
        // entry: what it keeps is in `kept`, and what it gives up is not in its
        // target. Only the partitions the member wants are looked up, so a
        // member with nothing to claim reads no other member.
        let held_by_others = self.held_by_others(&member.member_id, &wanted);
        let mut has_unreleased = false;
        let mut granted = Partitions::new();
        for (topic_id, partitions) in wanted {
            // A partition the member itself is pending to revoke is free
            // for it: only one member holds a partition at a time.
            let pending_here: HashSet<i32> = member
                .partitions_pending_revocation
                .get(&topic_id)
                .map(|parts| parts.iter().copied().collect())
                .unwrap_or_default();
            for partition in partitions {
                if held_by_others.contains(&(topic_id, partition))
                    && !pending_here.contains(&partition)
                {
                    has_unreleased = true;
                } else {
                    granted.entry(topic_id).or_default().push(partition);
                }
            }
        }

        let mut rebuilt = if !to_revoke.is_empty() && owns_any(owned, &to_revoke) {
            // The member keeps its epoch and waits for the client to
            // acknowledge the revocation.
            Rebuilt {
                state: MemberAssignmentState::UnrevokedPartitions,
                epoch: member.member_epoch,
                assigned: kept,
                pending: to_revoke,
                granted: Partitions::new(),
            }
        } else {
            // Whatever the member had to revoke it has revoked. It moves to
            // the target epoch with what it keeps and what is free.
            let state = if has_unreleased {
                MemberAssignmentState::UnreleasedPartitions
            } else {
                MemberAssignmentState::Stable
            };
            for (topic_id, parts) in &granted {
                kept.entry(*topic_id).or_default().extend(parts);
            }
            Rebuilt {
                state,
                epoch: target_epoch,
                assigned: kept,
                pending: Partitions::new(),
                granted,
            }
        };
        for parts in rebuilt
            .assigned
            .values_mut()
            .chain(rebuilt.pending.values_mut())
        {
            parts.sort_unstable();
        }
        rebuilt
    }

    /// Kafka's `ConsumerGroup.waitingOnUnreleasedPartition`: `true` when the
    /// member is in `UnreleasedPartitions` and a partition of its target that
    /// it does not hold yet is still held by another member.
    #[must_use]
    pub fn waiting_on_unreleased_partition(&self, member_id: &str) -> bool {
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        if member.assignment_state != MemberAssignmentState::UnreleasedPartitions {
            return false;
        }
        let Some(target) = self.target.per_member.get(member_id) else {
            return false;
        };
        let wanted: Partitions = target
            .iter()
            .map(|(topic_id, partitions)| {
                let assigned = member.assigned_partitions.get(topic_id);
                let missing: Vec<i32> = partitions
                    .iter()
                    .copied()
                    .filter(|partition| assigned.is_none_or(|held| !held.contains(partition)))
                    .collect();
                (*topic_id, missing)
            })
            .filter(|(_, missing)| !missing.is_empty())
            .collect();
        !self.held_by_others(member_id, &wanted).is_empty()
    }

    /// The partitions of `wanted` that a member other than `member_id` holds,
    /// assigned or pending revocation.
    fn held_by_others(&self, member_id: &str, wanted: &Partitions) -> HashSet<(Uuid, i32)> {
        let mut held = HashSet::new();
        if wanted.is_empty() {
            return held;
        }
        let wanted: HashMap<Uuid, HashSet<i32>> = wanted
            .iter()
            .map(|(topic_id, parts)| (*topic_id, parts.iter().copied().collect()))
            .collect();
        for other in self
            .members
            .values()
            .filter(|other| other.member_id != member_id)
        {
            for map in [
                &other.assigned_partitions,
                &other.partitions_pending_revocation,
            ] {
                for (topic_id, parts) in map {
                    let Some(wanted) = wanted.get(topic_id) else {
                        continue;
                    };
                    held.extend(
                        parts
                            .iter()
                            .filter(|partition| wanted.contains(partition))
                            .map(|&partition| (*topic_id, partition)),
                    );
                }
            }
        }
        held
    }
}

/// Kafka's `ownsRevokedPartitions`: `true` when the heartbeat reports any of
/// the `pending` partitions, or reports nothing at all.
/// Kafka's `Assignment.equals`: the same partitions of the same topics,
/// whatever their order.
pub(crate) fn same_assignment(a: &Partitions, b: &Partitions) -> bool {
    let set = |assignment: &Partitions| -> HashSet<(Uuid, i32)> {
        assignment
            .iter()
            .flat_map(|(topic_id, partitions)| partitions.iter().map(|p| (*topic_id, *p)))
            .collect()
    };
    set(a) == set(b)
}

fn owns_any(owned: Option<&Partitions>, pending: &Partitions) -> bool {
    owned.is_none_or(|owned| {
        owned.iter().any(|(topic_id, parts)| {
            pending.get(topic_id).is_some_and(|pending| {
                let pending: HashSet<&i32> = pending.iter().collect();
                parts.iter().any(|partition| pending.contains(partition))
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::coordinator::unified::{
        consumer_state::{
            ResolvedRegularExpression,
            test_support::{Topics, member, subscribed_member},
        },
        test_support::{MemberEpoch, MemberEpochs},
    };

    #[derive(Clone, Copy, Default, PartialEq, Eq)]
    enum SubscriptionChange {
        #[default]
        Unchanged,
        Changed,
    }

    const T: Uuid = Uuid([1; 16]);
    const TARGET_EPOCH: i32 = 6;

    fn topics() -> Topics {
        Topics(vec![("t", T)])
    }

    fn parts(partitions: &[PartitionIndex]) -> Partitions {
        if partitions.is_empty() {
            Partitions::new()
        } else {
            [(T, partitions.iter().map(|index| index.0).collect())].into()
        }
    }

    /// What the coordinator stores about a member.
    #[derive(Debug, PartialEq)]
    struct Held {
        epoch: MemberEpoch,
        state: MemberAssignmentState,
        assigned: Vec<PartitionIndex>,
        pending: Vec<PartitionIndex>,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct HeldSetup<'a> {
        #[default(MemberEpoch(5))]
        epoch: MemberEpoch,
        #[default(MemberAssignmentState::Stable)]
        state: MemberAssignmentState,
        #[default(&[PartitionIndex(0), PartitionIndex(1)])]
        assigned: &'a [PartitionIndex],
        pending: &'a [PartitionIndex],
    }

    fn held(setup: HeldSetup<'_>) -> Held {
        let HeldSetup {
            epoch,
            state,
            assigned,
            pending,
        } = setup;
        Held {
            epoch,
            state,
            assigned: assigned.to_vec(),
            pending: pending.to_vec(),
        }
    }

    fn held_by(g: &GroupState, member_id: &str) -> Held {
        let member = &g.members[member_id];
        let list = |partitions: &Partitions| {
            partitions
                .get(&T)
                .map(|ids| ids.iter().copied().map(PartitionIndex).collect())
                .unwrap_or_default()
        };
        Held {
            epoch: MemberEpoch(member.member_epoch),
            state: member.assignment_state,
            assigned: list(&member.assigned_partitions),
            pending: list(&member.partitions_pending_revocation),
        }
    }

    /// A group at epoch 6 with member `a`, holding `before`, and member `b`,
    /// holding `other`, assigned and pending revocation, and the target `target`
    /// for `a`.
    fn group(
        before: &Held,
        other: (&[PartitionIndex], &[PartitionIndex]),
        target: &[PartitionIndex],
    ) -> GroupState {
        let mut g = GroupState::new("g");
        g.group_epoch = TARGET_EPOCH;
        let mut a = subscribed_member("a", &["t"]);
        a.member_epoch = before.epoch.0;
        // The member already recorded the epoch it is in as its previous epoch.
        a.previous_member_epoch = before.epoch.0;
        a.assignment_state = before.state;
        a.assigned_partitions = parts(&before.assigned);
        a.partitions_pending_revocation = parts(&before.pending);
        a.stamp_assignment_epochs(before.epoch.0);
        g.members.insert("a".into(), a);
        let mut b = member("b");
        b.member_epoch = TARGET_EPOCH;
        b.assigned_partitions = parts(other.0);
        b.partitions_pending_revocation = parts(other.1);
        g.members.insert("b".into(), b);
        g.install_target([("a".to_string(), parts(target))].into());
        g
    }

    /// Kafka's `CurrentAssignmentBuilder`: one row per rule. `owned` is what
    /// the heartbeat reports, and `None` is a heartbeat without
    /// `TopicPartitions`.
    #[test]
    fn reconcile_member_follows_the_current_assignment_builder() {
        use MemberAssignmentState::UnrevokedPartitions as U;

        #[derive(Clone, Copy, Default, PartialEq, Eq)]
        enum StateChange {
            #[default]
            Unchanged,
            Changed,
        }

        struct Row {
            name: &'static str,
            before: Held,
            other: (&'static [PartitionIndex], &'static [PartitionIndex]),
            target: &'static [PartitionIndex],
            owned: Option<&'static [PartitionIndex]>,
            subscription: SubscriptionChange,
            after: Held,
            state_change: StateChange,
        }

        impl Default for Row {
            fn default() -> Self {
                Self {
                    name: "assignment row",
                    before: held(HeldSetup::default()),
                    other: (&[], &[]),
                    target: &[PartitionIndex(0), PartitionIndex(1)],
                    owned: None,
                    subscription: SubscriptionChange::Unchanged,
                    after: held(HeldSetup::default()),
                    state_change: StateChange::Unchanged,
                }
            }
        }

        let rows = [
            // A member that must revoke stays where it is until it reports an
            // owned set without the pending partitions.
            Row {
                name: "an absent owned set keeps a member that must revoke",
                before: revoking_input(),
                after: revoking_expected(),
                ..Default::default()
            },
            Row {
                name: "an owned set with the pending partition keeps the member",
                before: revoking_input(),
                owned: Some(&[PartitionIndex(0), PartitionIndex(1), PartitionIndex(2)]),
                after: revoking_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "an owned set without the pending partition moves the member on",
                before: revoking_input(),
                owned: Some(&[PartitionIndex(0), PartitionIndex(1)]),
                after: settled_expected(),
                ..Default::default()
            },
            // A stable member behind the target epoch learns what to revoke.
            Row {
                state_change: StateChange::Changed,
                name: "a shrunk target is a revocation, in the member's epoch",
                before: expanded_input(),
                after: revoking_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a member that already dropped the partition moves on",
                before: expanded_input(),
                owned: Some(&[PartitionIndex(0), PartitionIndex(1)]),
                after: settled_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a free target partition is granted at the target epoch",
                before: single_partition_input(),
                after: settled_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a partition another member holds is withheld",
                before: single_partition_input(),
                other: (&[PartitionIndex(1)], &[]),
                after: withheld_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a partition another member must still revoke is withheld",
                before: single_partition_input(),
                other: (&[], &[PartitionIndex(1)]),
                after: withheld_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "an unreleased partition is granted once it is free",
                before: unreleased_input(),
                after: settled_expected(),
                ..Default::default()
            },
            Row {
                name: "an unreleased partition stays withheld while it is held",
                before: unreleased_input(),
                other: (&[PartitionIndex(1)], &[]),
                after: withheld_expected(),
                ..Default::default()
            },
            Row {
                name: "a stable member at the target epoch has nothing to do",
                before: current_input(),
                after: settled_expected(),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a revocation waits, and so does a free partition",
                before: held(HeldSetup::default()),
                target: &[PartitionIndex(1), PartitionIndex(2)],
                after: held(HeldSetup {
                    state: U,
                    assigned: &[PartitionIndex(1)],
                    pending: &[PartitionIndex(0)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Row {
                state_change: StateChange::Changed,
                name: "a partition the member gave up is free for it again",
                before: held(HeldSetup {
                    state: U,
                    assigned: &[PartitionIndex(0)],
                    pending: &[PartitionIndex(1)],
                    ..Default::default()
                }),
                owned: Some(&[PartitionIndex(0)]),
                after: settled_expected(),
                ..Default::default()
            },
            // A subscription change at the target epoch only filters the
            // assignment by the subscription, and the member still subscribes
            // to the topic (`updateCurrentAssignment`).
            Row {
                subscription: SubscriptionChange::Changed,
                name: "a subscription change alone leaves a member at the target epoch",
                before: held(HeldSetup {
                    epoch: MemberEpoch(6),
                    assigned: &[PartitionIndex(0), PartitionIndex(1), PartitionIndex(2)],
                    ..Default::default()
                }),
                after: held(HeldSetup {
                    epoch: MemberEpoch(6),
                    assigned: &[PartitionIndex(0), PartitionIndex(1), PartitionIndex(2)],
                    ..Default::default()
                }),
                ..Default::default()
            },
        ];
        for r in rows {
            let mut g = group(&r.before, r.other, r.target);
            let owned = r.owned.map(parts);

            let changed = g.reconcile_member(
                "a",
                owned.as_ref(),
                r.subscription == SubscriptionChange::Changed,
                &topics(),
            );

            check!(held_by(&g, "a") == r.after, "{}", r.name);
            check!(
                changed == (r.state_change == StateChange::Changed),
                "{}",
                r.name
            );
        }
    }

    /// A target change leaves every member as it was. The member learns about
    /// it in its own heartbeat, and it keeps its epoch until it revokes.
    #[test]
    fn install_target_does_not_reconcile_any_member() {
        let before = held(HeldSetup {
            assigned: &[PartitionIndex(0), PartitionIndex(1), PartitionIndex(2)],
            ..Default::default()
        });
        let mut g = group(&before, (&[], &[]), &[PartitionIndex(0), PartitionIndex(1)]);

        check!(held_by(&g, "a") == before);
        check!(g.target.epoch == TARGET_EPOCH);

        g.reconcile_member("a", None, false, &topics());
        check!(
            held_by(&g, "a")
                == held(HeldSetup {
                    state: MemberAssignmentState::UnrevokedPartitions,
                    pending: &[PartitionIndex(2)],
                    ..Default::default()
                })
        );
    }

    /// A member moved to a new epoch remembers the one it left, and a partition
    /// granted at that epoch is assigned at it, even one the member gave up.
    #[test]
    fn a_granted_partition_is_assigned_at_the_new_epoch() {
        let before = held(HeldSetup {
            state: MemberAssignmentState::UnrevokedPartitions,
            assigned: &[PartitionIndex(0)],
            pending: &[PartitionIndex(1)],
            ..Default::default()
        });
        let mut g = group(&before, (&[], &[]), &[PartitionIndex(0), PartitionIndex(1)]);

        g.reconcile_member("a", Some(&parts(&[PartitionIndex(0)])), false, &topics());

        let member = &g.members["a"];
        check!(member.member_epoch == TARGET_EPOCH);
        check!(member.previous_member_epoch == 5);
        check!(member.assignment_epoch(&T, 0) == Some(5));
        check!(member.assignment_epoch(&T, 1) == Some(TARGET_EPOCH));
    }

    const U: Uuid = Uuid([2; 16]);
    const TWO_PARTITIONS_PER_TOPIC: &[(Uuid, &[PartitionIndex])] = &[
        (T, &[PartitionIndex(0), PartitionIndex(1)]),
        (U, &[PartitionIndex(0), PartitionIndex(1)]),
    ];
    const ONE_PARTITION_PER_TOPIC: &[(Uuid, &[PartitionIndex])] =
        &[(T, &[PartitionIndex(0)]), (U, &[PartitionIndex(0)])];
    /// A topic that the metadata does not hold, as one that was deleted.
    const W: Uuid = Uuid([3; 16]);

    /// What the coordinator stores about a member, with the epoch it left.
    #[derive(Debug, PartialEq)]
    struct Shape {
        epoch: i32,
        previous_epoch: i32,
        state: MemberAssignmentState,
        assigned: Partitions,
        pending: Partitions,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ShapeSetup<'a> {
        epochs: MemberEpochs,
        #[default(MemberAssignmentState::Stable)]
        state: MemberAssignmentState,
        assigned: &'a [(Uuid, &'a [PartitionIndex])],
        pending: &'a [(Uuid, &'a [PartitionIndex])],
    }

    impl<'a> ShapeSetup<'a> {
        fn revoking_current_epoch(pending: &'a [(Uuid, &'a [PartitionIndex])]) -> Self {
            Self {
                epochs: MemberEpochs {
                    current: MemberEpoch(5),
                    ..Default::default()
                },
                state: MemberAssignmentState::UnrevokedPartitions,
                assigned: &[(T, &[PartitionIndex(0)])],
                pending,
            }
        }
    }

    fn shape(setup: ShapeSetup<'_>) -> Shape {
        let ShapeSetup {
            epochs,
            state,
            assigned,
            pending,
        } = setup;
        Shape {
            epoch: epochs.current.0,
            previous_epoch: epochs.previous.0,
            state,
            assigned: topic_parts(assigned),
            pending: topic_parts(pending),
        }
    }

    fn topic_parts(entries: &[(Uuid, &[PartitionIndex])]) -> Partitions {
        entries
            .iter()
            .map(|(topic_id, parts)| (*topic_id, parts.iter().map(|index| index.0).collect()))
            .collect()
    }

    fn shape_of(g: &GroupState, member_id: &str) -> Shape {
        let member = &g.members[member_id];
        Shape {
            epoch: member.member_epoch,
            previous_epoch: member.previous_member_epoch,
            state: member.assignment_state,
            assigned: member.assigned_partitions.clone(),
            pending: member.partitions_pending_revocation.clone(),
        }
    }

    /// A row of [`subscription_rows`]: a group at epoch 6, with the target it
    /// names installed for member `a`, and `other` held by member `b`.
    struct SubscriptionRow {
        name: &'static str,
        names: &'static [&'static str],
        /// The regex of the member, and what the group resolved it to.
        regex: Option<(&'static str, Option<&'static [&'static str]>)>,
        before: Shape,
        other: Partitions,
        target: Partitions,
        owned: Option<Partitions>,
        subscription: SubscriptionChange,
        after: Shape,
    }

    impl Default for SubscriptionRow {
        fn default() -> Self {
            Self {
                name: "subscription row",
                names: &["t"],
                regex: None,
                before: shape(ShapeSetup::default()),
                other: Partitions::new(),
                target: Partitions::new(),
                owned: None,
                subscription: SubscriptionChange::Unchanged,
                after: shape(ShapeSetup::default()),
            }
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct WaitingSubscriptionSetup {
        #[default("unreleased member waiting")]
        name: &'static str,
        history: MemberEpochs,
    }

    fn waiting_subscription_row(setup: WaitingSubscriptionSetup) -> SubscriptionRow {
        SubscriptionRow {
            name: setup.name,
            before: shape(ShapeSetup {
                epochs: setup.history,
                state: MemberAssignmentState::UnreleasedPartitions,
                assigned: &[(T, &[PartitionIndex(0)])],
                ..Default::default()
            }),
            other: topic_parts(&[(T, &[PartitionIndex(1)])]),
            target: topic_parts(&[(T, &[PartitionIndex(0), PartitionIndex(1)])]),
            after: withheld_6_6_expected(),
            ..Default::default()
        }
    }

    fn subscription_rows() -> Vec<SubscriptionRow> {
        use MemberAssignmentState::UnrevokedPartitions;

        let owning_everything = topic_parts(&[
            (T, &[PartitionIndex(0), PartitionIndex(1)]),
            (U, &[PartitionIndex(0)]),
        ]);
        vec![
            SubscriptionRow {
                name: "an unrevoked member that owns its pending set drops a topic it left",
                before: revoking_5_4_input(),
                target: topic_parts(ONE_PARTITION_PER_TOPIC),
                owned: Some(owning_everything.clone()),
                subscription: SubscriptionChange::Changed,
                after: shape(ShapeSetup::revoking_current_epoch(&[
                    (T, &[PartitionIndex(1)]),
                    (U, &[PartitionIndex(0)]),
                ])),
                ..Default::default()
            },
            SubscriptionRow {
                name: "an unrevoked member that owns its pending set keeps it without a change",
                before: revoking_5_4_input(),
                target: topic_parts(ONE_PARTITION_PER_TOPIC),
                owned: Some(owning_everything.clone()),
                after: shape(ShapeSetup {
                    epochs: MemberEpochs {
                        current: MemberEpoch(5),
                        previous: MemberEpoch(4),
                    },
                    state: UnrevokedPartitions,
                    assigned: ONE_PARTITION_PER_TOPIC,
                    pending: &[(T, &[PartitionIndex(1)])],
                }),
                ..Default::default()
            },
            SubscriptionRow {
                name: "a stable member at the target epoch revokes a topic it left and still owns",
                before: stable_6_5_input(),
                target: topic_parts(TWO_PARTITIONS_PER_TOPIC),
                subscription: SubscriptionChange::Changed,
                after: shape(ShapeSetup {
                    epochs: MemberEpochs {
                        previous: MemberEpoch(6),
                        ..Default::default()
                    },
                    state: UnrevokedPartitions,
                    assigned: &[(T, &[PartitionIndex(0), PartitionIndex(1)])],
                    pending: &[(U, &[PartitionIndex(0), PartitionIndex(1)])],
                }),
                ..Default::default()
            },
            SubscriptionRow {
                name: "a stable member at the target epoch drops a topic it left and released",
                before: stable_6_5_input(),
                target: topic_parts(TWO_PARTITIONS_PER_TOPIC),
                owned: Some(topic_parts(&[(T, &[PartitionIndex(0), PartitionIndex(1)])])),
                subscription: SubscriptionChange::Changed,
                after: shape(ShapeSetup {
                    epochs: MemberEpochs {
                        previous: MemberEpoch(6),
                        ..Default::default()
                    },
                    assigned: &[(T, &[PartitionIndex(0), PartitionIndex(1)])],
                    ..Default::default()
                }),
                ..Default::default()
            },
            SubscriptionRow {
                name: "a member with no subscription revokes everything",
                names: &[],
                before: shape(ShapeSetup {
                    assigned: ONE_PARTITION_PER_TOPIC,
                    ..Default::default()
                }),
                target: topic_parts(ONE_PARTITION_PER_TOPIC),
                subscription: SubscriptionChange::Changed,
                after: shape(ShapeSetup {
                    epochs: MemberEpochs {
                        previous: MemberEpoch(6),
                        ..Default::default()
                    },
                    state: UnrevokedPartitions,
                    pending: ONE_PARTITION_PER_TOPIC,
                    ..Default::default()
                }),
                ..Default::default()
            },
            SubscriptionRow {
                name: "the target of a topic the member left grants nothing",
                before: stable_5_4_input(),
                target: topic_parts(&[
                    (T, &[PartitionIndex(0)]),
                    (U, &[PartitionIndex(0), PartitionIndex(1)]),
                ]),
                after: stable_6_5_expected(),
                ..Default::default()
            },
            SubscriptionRow {
                name: "a partition of a topic the member left is revoked though the target holds it",
                before: shape(ShapeSetup {
                    epochs: MemberEpochs {
                        current: MemberEpoch(5),
                        previous: MemberEpoch(4),
                    },
                    assigned: ONE_PARTITION_PER_TOPIC,
                    ..Default::default()
                }),
                target: topic_parts(ONE_PARTITION_PER_TOPIC),
                after: shape(ShapeSetup::revoking_current_epoch(&[(
                    U,
                    &[PartitionIndex(0)],
                )])),
                ..Default::default()
            },
            SubscriptionRow {
                name: "a topic that does not exist is not subscribed",
                names: &["t", "w"],
                before: stable_5_4_input(),
                target: topic_parts(&[(T, &[PartitionIndex(0)]), (W, &[PartitionIndex(0)])]),
                after: stable_6_5_expected(),
                ..Default::default()
            },
            SubscriptionRow {
                regex: Some(("u.*", Some(&["u"]))),
                name: "the topics of a resolved regex are subscribed",
                names: &[],
                before: stable_5_4_input_two_topics(),
                target: topic_parts(&[(U, &[PartitionIndex(0)])]),
                after: shape(ShapeSetup {
                    assigned: &[(U, &[PartitionIndex(0)])],
                    ..Default::default()
                }),
                ..Default::default()
            },
            SubscriptionRow {
                regex: Some(("u.*", None)),
                name: "a regex the group has not resolved subscribes to nothing",
                names: &[],
                before: stable_5_4_input_two_topics(),
                target: topic_parts(&[(U, &[PartitionIndex(0)])]),
                after: shape(ShapeSetup::default()),
                ..Default::default()
            },
            // A member that keeps its epoch still records it as its previous epoch.
            waiting_subscription_row(WaitingSubscriptionSetup {
                name: "an unreleased member that keeps waiting records the epoch it stays in",
                ..Default::default()
            }),
            waiting_subscription_row(WaitingSubscriptionSetup {
                name: "an unreleased member that already recorded it changes nothing",
                history: MemberEpochs {
                    previous: MemberEpoch(6),
                    ..Default::default()
                },
            }),
        ]
    }

    /// Kafka's `CurrentAssignmentBuilder` reads the member's subscription:
    /// `subscribedTopicIds` is the member's names and the topics of its
    /// resolved regex, and a topic that does not exist is not in it.
    /// `updateCurrentAssignment` removes the partitions of a topic the member
    /// left, and `computeNextAssignment` treats the target of such a topic as
    /// empty. The member is rebuilt when the shape after differs from the
    /// shape before, and it records the epoch it leaves whenever it is
    /// rebuilt.
    #[test]
    fn the_subscription_filters_the_reconciliation() {
        let metadata = Topics(vec![("t", T), ("u", U)]);
        for r in subscription_rows() {
            let mut g = GroupState::new("g");
            g.group_epoch = TARGET_EPOCH;
            let mut a = subscribed_member("a", r.names);
            a.member_epoch = r.before.epoch;
            a.previous_member_epoch = r.before.previous_epoch;
            a.assignment_state = r.before.state;
            a.assigned_partitions = r.before.assigned.clone();
            a.partitions_pending_revocation = r.before.pending.clone();
            a.stamp_assignment_epochs(r.before.epoch);
            if let Some((regex, resolved)) = r.regex {
                a.subscribed_topic_regex = Some(regex.to_owned());
                if let Some(topics) = resolved {
                    g.set_resolved_regex(
                        regex.to_owned(),
                        ResolvedRegularExpression {
                            topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                            version: 1,
                            timestamp_ms: 0,
                        },
                    );
                }
            }
            g.members.insert("a".into(), a);
            let mut b = member("b");
            b.member_epoch = TARGET_EPOCH;
            b.assigned_partitions = r.other;
            g.members.insert("b".into(), b);
            g.install_target([("a".to_string(), r.target)].into());

            let changed = g.reconcile_member(
                "a",
                r.owned.as_ref(),
                r.subscription == SubscriptionChange::Changed,
                &metadata,
            );

            check!(changed == (r.before != r.after), "{}", r.name);
            check!(shape_of(&g, "a") == r.after, "{}", r.name);
        }
    }

    /// The lookup of the partitions that other members hold reads only the
    /// partitions the member wants, assigned or pending revocation, and never
    /// the member's own.
    #[test]
    fn held_by_others_reports_only_the_wanted_partitions_of_other_members() {
        let mut g = GroupState::new("g");
        for (id, assigned, pending) in [
            (
                "a",
                topic_parts(&[(T, &[PartitionIndex(0), PartitionIndex(1)])]),
                topic_parts(&[(U, &[PartitionIndex(5)])]),
            ),
            (
                "b",
                topic_parts(&[(T, &[PartitionIndex(2), PartitionIndex(3)])]),
                topic_parts(&[(T, &[PartitionIndex(4)])]),
            ),
            (
                "c",
                topic_parts(&[(U, &[PartitionIndex(7)])]),
                Partitions::new(),
            ),
        ] {
            let mut m = member(id);
            m.assigned_partitions = assigned;
            m.partitions_pending_revocation = pending;
            g.members.insert(id.into(), m);
        }

        let held = g.held_by_others(
            "a",
            &topic_parts(&[
                (
                    T,
                    &[
                        PartitionIndex(0),
                        PartitionIndex(2),
                        PartitionIndex(4),
                        PartitionIndex(9),
                    ],
                ),
                (U, &[PartitionIndex(5), PartitionIndex(7)]),
            ]),
        );

        check!(held == HashSet::from([(T, 2), (T, 4), (U, 7)]));
        check!(g.held_by_others("a", &Partitions::new()).is_empty());
    }
    fn revoking_input() -> Held {
        held(HeldSetup {
            state: MemberAssignmentState::UnrevokedPartitions,
            pending: &[PartitionIndex(2)],
            ..Default::default()
        })
    }
    fn expanded_input() -> Held {
        held(HeldSetup {
            assigned: &[PartitionIndex(0), PartitionIndex(1), PartitionIndex(2)],
            ..Default::default()
        })
    }
    fn single_partition_input() -> Held {
        held(HeldSetup {
            assigned: &[PartitionIndex(0)],
            ..Default::default()
        })
    }
    fn unreleased_input() -> Held {
        held(HeldSetup {
            epoch: MemberEpoch(6),
            state: MemberAssignmentState::UnreleasedPartitions,
            assigned: &[PartitionIndex(0)],
            ..Default::default()
        })
    }
    fn current_input() -> Held {
        held(HeldSetup {
            epoch: MemberEpoch(6),
            ..Default::default()
        })
    }
    fn revoking_expected() -> Held {
        held(HeldSetup {
            state: MemberAssignmentState::UnrevokedPartitions,
            pending: &[PartitionIndex(2)],
            ..Default::default()
        })
    }
    fn settled_expected() -> Held {
        held(HeldSetup {
            epoch: MemberEpoch(6),
            ..Default::default()
        })
    }
    fn withheld_expected() -> Held {
        held(HeldSetup {
            epoch: MemberEpoch(6),
            state: MemberAssignmentState::UnreleasedPartitions,
            assigned: &[PartitionIndex(0)],
            ..Default::default()
        })
    }
    fn revoking_5_4_input() -> Shape {
        shape(ShapeSetup {
            epochs: MemberEpochs {
                current: MemberEpoch(5),
                previous: MemberEpoch(4),
            },
            state: MemberAssignmentState::UnrevokedPartitions,
            assigned: ONE_PARTITION_PER_TOPIC,
            pending: &[(T, &[PartitionIndex(1)])],
        })
    }
    fn stable_6_5_input() -> Shape {
        shape(ShapeSetup {
            assigned: &[
                (T, &[PartitionIndex(0), PartitionIndex(1)]),
                (U, &[PartitionIndex(0), PartitionIndex(1)]),
            ],
            ..Default::default()
        })
    }
    fn stable_5_4_input() -> Shape {
        shape(ShapeSetup {
            epochs: MemberEpochs {
                current: MemberEpoch(5),
                previous: MemberEpoch(4),
            },
            assigned: &[(T, &[PartitionIndex(0)])],
            ..Default::default()
        })
    }
    fn stable_6_5_expected() -> Shape {
        shape(ShapeSetup {
            assigned: &[(T, &[PartitionIndex(0)])],
            ..Default::default()
        })
    }
    fn stable_5_4_input_two_topics() -> Shape {
        shape(ShapeSetup {
            epochs: MemberEpochs {
                current: MemberEpoch(5),
                previous: MemberEpoch(4),
            },
            ..Default::default()
        })
    }
    fn withheld_6_6_expected() -> Shape {
        shape(ShapeSetup {
            epochs: MemberEpochs {
                previous: MemberEpoch(6),
                ..Default::default()
            },
            state: MemberAssignmentState::UnreleasedPartitions,
            assigned: &[(T, &[PartitionIndex(0)])],
            ..Default::default()
        })
    }
}
