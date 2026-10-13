//! Unified `Group` container for KIP-848 migration.
//!
//! One in-memory model for a consumer group, whichever protocol its members
//! speak.
//!
//! A `Group` is a discriminated container over the two existing, tested state
//! machines: the classic 5-state machine ([`ClassicState`]) and the next-gen
//! epoch machine ([`ConsumerState`]). The unified coordinator and the
//! persistence path can therefore hold either one behind a single type.
//!
//! The actor can change the live kind during KIP-848 upgrade and downgrade.
//! The container keeps both protocol-specific state machines behind one surface
//! so the coordinator and persistence code always use the live state.

// The state machines are reused verbatim, relocated under `unified/`. These
// aliases give the unified surface its types without renaming the moved code
// (the classic file keeps its internal `Group`/`GroupState` names).
pub(crate) use crate::coordinator::unified::{
    classic_state::{ClassicGroup as ClassicState, OffsetEntry},
    consumer_state::GroupState as ConsumerState,
};
use {
    crate::codes::{ILLEGAL_GENERATION, STALE_MEMBER_EPOCH, UNKNOWN_MEMBER_ID},
    std::collections::{HashMap, HashSet},
};

/// Which protocol the members of a [`Group`] speak. The variant carries that
/// protocol's full state machine.
#[derive(Debug)]
pub enum GroupKind {
    /// Classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` group.
    Classic(ClassicState),
    /// KIP-848 `ConsumerGroupHeartbeat` group.
    Consumer(ConsumerState),
}

/// A consumer group in the unified coordinator.
#[derive(Debug)]
pub struct CoordinatorGroup {
    pub group_id: String,
    pub kind: GroupKind,
    /// Committed offsets, from `__consumer_offsets` k0 and k1.
    ///
    /// They do not depend on the protocol. A group's offsets key by
    /// `(topic, partition)` whichever protocol its members speak, so they live
    /// on the container instead of inside either state machine. A later type
    /// change, during share-group consumption and acknowledgement, can therefore carry the committed offsets
    /// through a conversion untouched.
    pub committed_offsets: HashMap<(String, i32), OffsetEntry>,
    /// Wall-clock millisecond at which the group last lost its final member,
    /// and `None` while it still has one.
    ///
    /// This is Kafka's `currentStateTimestamp` narrowed to the one question
    /// the offset-retention sweep asks: how long has this group been empty?
    /// The actor restamps it once per mailbox turn through
    /// [`observe_membership`](Self::observe_membership), so no membership
    /// transition has to remember to maintain it. A group that has never had a
    /// member — the "simple consumer" that only commits offsets — is empty
    /// from the moment its actor first runs, which is why the retention sweep
    /// reads this stamp only for a classic group that carries a protocol type:
    /// that is the one kind whose k2 snapshot persists the moment it emptied,
    /// so the stamp means the same thing after a restart as before one.
    pub empty_since_ms: Option<i64>,
    /// KIP-447: the `(topic, partition)` keys a transaction has written an
    /// offset commit for and that no commit or abort marker has resolved yet,
    /// grouped by the producer whose transaction wrote them.
    ///
    /// The offsets themselves are not here: they live in the log below the
    /// partition's LSO until the marker lands, and `WriteTxnMarkers`
    /// materializes them into `committed_offsets` only for a commit. These
    /// keys are what makes an `OffsetFetch` with `require_stable = true`
    /// answer `UNSTABLE_OFFSET_COMMIT` instead of the older stable offset.
    ///
    /// Grouping by producer is what lets one producer's marker resolve its own
    /// keys while another producer's in-flight transaction keeps its own.
    pending_txn_offsets: HashMap<i64, ProducerTxnOffsets>,
}

/// One producer's KIP-447 state on this group: the keys of its open
/// transaction, and the offsets-log position of the newest marker that has
/// already resolved a transaction of the same producer.
///
/// The watermark is what puts two out-of-band updates back into the log order
/// that decides them. `TxnOffsetCommit` records its mark only once its records
/// are durable, so the producer's marker can be appended -- and resolved on
/// this actor -- in the window between that append and that mark. The log
/// settles which came first: records written below a marker belong to the
/// transaction the marker ends, so a mark whose records sit at or below
/// `resolved_through` is already resolved and is dropped. Without the
/// watermark such a mark would never be cleared and the partition would answer
/// `UNSTABLE_OFFSET_COMMIT` for ever.
///
/// A resolved producer keeps its (empty) entry, because the watermark is what
/// rejects the late mark. That is one `i64` per producer that has ever
/// committed transactional offsets to this group.
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
struct ProducerTxnOffsets {
    keys: HashSet<(String, i32)>,
    /// Keys a `TxnOffsetCommit` of this producer is about to append, before
    /// its records are durable and its mark arrives.
    ///
    /// `DeleteGroups` tombstones them with the pending keys. Without them a
    /// delete that runs between the durable append and the mark leaves the
    /// transactional record untombstoned, and its commit marker brings the
    /// deleted group back. The mark or a failed append removes them.
    reserved: HashSet<(String, i32)>,
    /// Offsets-log position of the newest marker resolved for this producer,
    /// or `-1` when no marker has been. Log offsets start at zero, so `-1`
    /// accepts every mark.
    #[default(-1)]
    resolved_through: i64,
}

/// A group's offset state as `OffsetFetch` needs to see it: the stable
/// committed offsets, plus the keys an unresolved transaction has written.
///
/// The two travel together because a `require_stable` fetch has to decide per
/// partition between them. Reading them in two actor turns would let a
/// transaction marker land in between and produce a row that is neither the
/// pre-transaction offset nor the post-transaction one.
#[derive(Debug, Default, Clone)]
pub struct GroupOffsets {
    pub committed: HashMap<(String, i32), OffsetEntry>,
    pub pending_txn: HashSet<(String, i32)>,
}

/// The member fields of an `OffsetFetch` request group (v9+): no member id
/// and epoch -1 before v9, and for the admin client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchMember {
    pub member_id: Option<String>,
    pub member_epoch: i32,
}

impl CoordinatorGroup {
    /// A fresh, empty classic group.
    pub fn new_classic(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Classic(ClassicState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
            pending_txn_offsets: HashMap::new(),
            empty_since_ms: None,
        }
    }

    /// A fresh, empty next-gen group, on the consumer protocol.
    pub fn new_consumer(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Consumer(ConsumerState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
            pending_txn_offsets: HashMap::new(),
            empty_since_ms: None,
        }
    }

    /// A group rebuilt from state that already exists: a replayed or
    /// hand-built state machine together with the committed offsets that go
    /// with it. No transaction is open on a group built this way; a replay
    /// that recovered one re-registers it with `add_pending_txn_offsets`.
    pub fn seeded(
        group_id: impl Into<String>,
        kind: GroupKind,
        committed_offsets: HashMap<(String, i32), OffsetEntry>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            kind,
            committed_offsets,
            pending_txn_offsets: HashMap::new(),
            empty_since_ms: None,
        }
    }

    /// `true` while the group has at least one member, whichever protocol
    /// they speak.
    #[must_use]
    pub fn has_members(&self) -> bool {
        match &self.kind {
            GroupKind::Classic(state) => !state.members.is_empty(),
            GroupKind::Consumer(state) => !state.members.is_empty(),
        }
    }

    /// Record whether the group has members as of `now_ms`.
    ///
    /// The actor calls this once per mailbox turn. A group that has members
    /// clears [`empty_since_ms`](Self::empty_since_ms); a group that has none
    /// stamps it, and a group that was already empty keeps the earlier stamp,
    /// so the sweep measures from the moment the last member left rather than
    /// from the most recent turn.
    pub fn observe_membership(&mut self, now_ms: i64) {
        if self.has_members() {
            self.empty_since_ms = None;
        } else if self.empty_since_ms.is_none() {
            self.empty_since_ms = Some(now_ms);
        }
    }

    /// Returns `true` if this group speaks the classic protocol.
    pub fn is_classic(&self) -> bool { matches!(self.kind, GroupKind::Classic(_)) }

    /// Returns `true` if this group speaks the next-gen protocol.
    pub fn is_consumer(&self) -> bool { matches!(self.kind, GroupKind::Consumer(_)) }

    pub fn as_classic(&self) -> Option<&ClassicState> {
        match &self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_classic_mut(&mut self) -> Option<&mut ClassicState> {
        match &mut self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_consumer(&self) -> Option<&ConsumerState> {
        match &self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }

    pub fn as_consumer_mut(&mut self) -> Option<&mut ConsumerState> {
        match &mut self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }

    /// Gives mutable access to the discriminated `kind`, so that a
    /// live-migration trigger can replace `Classic(..)` with `Consumer(..)` in
    /// place. This is the KIP-848 upgrade.
    pub fn kind_mut(&mut self) -> &mut GroupKind { &mut self.kind }

    /// Marks `keys` as written by `producer_id`'s open transaction at
    /// offsets-log position `written_at`.
    ///
    /// The caller must have made the offset-commit records durable first, so
    /// that the transaction's marker can always find the same keys again and
    /// clear them. `written_at` is the offset those records landed at, and a
    /// marker already resolved at or above it has resolved them: the mark is
    /// stale and is dropped rather than left behind for ever.
    pub fn add_pending_txn_offsets(
        &mut self,
        producer_id: i64,
        written_at: i64,
        keys: impl IntoIterator<Item = (String, i32)>,
    ) {
        let entry = self.pending_txn_offsets.entry(producer_id).or_default();
        let keys: Vec<(String, i32)> = keys.into_iter().collect();
        for key in &keys {
            entry.reserved.remove(key);
        }
        if written_at <= entry.resolved_through {
            return;
        }
        entry.keys.extend(keys);
    }

    /// Reserves `keys` for a `TxnOffsetCommit` of `producer_id` that is about
    /// to append them. See [`ProducerTxnOffsets::reserved`].
    pub fn reserve_txn_offsets(
        &mut self,
        producer_id: i64,
        keys: impl IntoIterator<Item = (String, i32)>,
    ) {
        self.pending_txn_offsets
            .entry(producer_id)
            .or_default()
            .reserved
            .extend(keys);
    }

    /// Releases the reservation of a `TxnOffsetCommit` whose append failed.
    pub fn release_txn_offsets(&mut self, producer_id: i64, keys: &[(String, i32)]) {
        if let Some(entry) = self.pending_txn_offsets.get_mut(&producer_id) {
            for key in keys {
                entry.reserved.remove(key);
            }
        }
    }

    /// Every key an unresolved transaction has written or is about to write:
    /// the pending marks and the reservations. `DeleteGroups` tombstones all
    /// of them.
    pub fn unresolved_txn_keys(&self) -> HashSet<(String, i32)> {
        self.pending_txn_offsets
            .values()
            .flat_map(|producer| producer.keys.iter().chain(&producer.reserved).cloned())
            .collect()
    }

    /// Drops every pending mark `producer_id`'s transaction holds, whether its
    /// marker committed or aborted, and records the marker's offsets-log
    /// position so that a mark for records below it cannot come back.
    /// Publishing the committed offsets is a separate step, because an abort
    /// publishes nothing.
    pub fn resolve_pending_txn_offsets(&mut self, producer_id: i64, resolved_through: i64) {
        let entry = self.pending_txn_offsets.entry(producer_id).or_default();
        entry.keys.clear();
        entry.resolved_through = entry.resolved_through.max(resolved_through);
    }

    /// Removes `keys` from the committed offsets and from every open
    /// transaction, after their `OffsetCommit` tombstones are durable.
    ///
    /// Kafka's `OffsetMetadataManager.replay` does the same for a tombstone:
    /// it removes the offset and the key's pending transactional offsets.
    pub fn drop_offsets(&mut self, keys: &[(String, i32)]) {
        for key in keys {
            self.committed_offsets.remove(key);
            for producer in self.pending_txn_offsets.values_mut() {
                producer.keys.remove(key);
            }
        }
    }

    /// Kafka's `validateOffsetFetch` for the member fields of an
    /// `OffsetFetch` request (v9+).
    ///
    /// A classic group accepts every fetch (`ClassicGroup.validateOffsetFetch`
    /// refuses only a dead group, and a dead group has no actor). A consumer
    /// group (`ConsumerGroup.validateOffsetFetch`) accepts a fetch with no
    /// member id and a negative epoch, which is the admin client. Otherwise
    /// the member must exist (`UNKNOWN_MEMBER_ID`) and the epoch must be its
    /// member epoch: `ILLEGAL_GENERATION` for a member of the classic
    /// protocol, `STALE_MEMBER_EPOCH` for the others.
    pub fn validate_offset_fetch(
        &self,
        member_id: Option<&str>,
        member_epoch: i32,
    ) -> Result<(), i16> {
        let Some(state) = self.as_consumer() else {
            return Ok(());
        };
        if member_id.is_none() && member_epoch < 0 {
            return Ok(());
        }
        let member = member_id
            .and_then(|id| state.members.get(id))
            .ok_or(UNKNOWN_MEMBER_ID)?;
        if member.member_epoch == member_epoch {
            Ok(())
        } else if member.is_classic() {
            Err(ILLEGAL_GENERATION)
        } else {
            Err(STALE_MEMBER_EPOCH)
        }
    }

    /// The group's offset state for an `OffsetFetch` that names `member`, or
    /// the error code of [`validate_offset_fetch`](Self::validate_offset_fetch).
    pub fn offsets_for_member(&self, member: &OffsetFetchMember) -> Result<GroupOffsets, i16> {
        self.validate_offset_fetch(member.member_id.as_deref(), member.member_epoch)
            .map(|()| self.offsets())
    }

    /// The group's offset state for `OffsetFetch`, with every open
    /// transaction's pending keys flattened into one set.
    pub fn offsets(&self) -> GroupOffsets {
        GroupOffsets {
            committed: self.committed_offsets.clone(),
            pending_txn: self
                .pending_txn_offsets
                .values()
                .flat_map(|producer| producer.keys.iter().cloned())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn classic_container_exposes_classic_state_only() {
        let mut g = CoordinatorGroup::new_classic("g");
        check!(g.is_classic());
        check!(!g.is_consumer());
        check!(g.as_classic().is_some());
        check!(g.as_consumer().is_none());
        check!(g.as_classic_mut().is_some());
        check!(g.group_id == "g");
    }

    #[test]
    fn empty_since_stamps_once_and_clears_while_members_are_present() {
        use {crate::coordinator::unified::classic_state::Member, std::time::Duration};

        let mut group = CoordinatorGroup::new_classic("g");
        check!(!group.has_members());

        // First observation of an empty group stamps it; later ones keep the
        // first stamp, so the sweep measures from when it emptied.
        group.observe_membership(100);
        check!(group.empty_since_ms == Some(100));
        group.observe_membership(500);
        check!(group.empty_since_ms == Some(100));

        group.as_classic_mut().unwrap().add_member(Member::new(
            "m1",
            "client",
            "host",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), bytes::Bytes::new())],
        ));
        group.observe_membership(600);
        check!(group.has_members());
        check!(group.empty_since_ms == None);

        group.as_classic_mut().unwrap().members.clear();
        group.observe_membership(900);
        check!(group.empty_since_ms == Some(900));
    }

    #[test]
    fn consumer_container_exposes_consumer_state_only() {
        let mut g = CoordinatorGroup::new_consumer("g");
        check!(g.is_consumer());
        check!(!g.is_classic());
        check!(g.as_consumer().is_some());
        check!(g.as_classic().is_none());
        check!(g.as_consumer_mut().is_some());
        check!(g.group_id == "g");
    }

    /// Kafka's `ClassicGroup.validateOffsetFetch` and
    /// `ConsumerGroup.validateOffsetFetch`.
    #[test]
    fn validate_offset_fetch_follows_the_group_kind_and_member_protocol() {
        struct Row<'a> {
            name: &'a str,
            group: &'a CoordinatorGroup,
            member_id: Option<&'a str>,
            epoch: i32,
            expected: Result<(), i16>,
        }

        use {
            crate::{
                codes,
                coordinator::unified::consumer_state::{ClassicMemberFacade, test_support::member},
            },
            std::time::Duration,
        };

        let mut consumer = CoordinatorGroup::new_consumer("g");
        let state = consumer.as_consumer_mut().unwrap();
        for (member_id, classic) in [("native", false), ("classic", true)] {
            let mut next = member(member_id);
            next.member_epoch = 7;
            if classic {
                next.classic = Some(ClassicMemberFacade {
                    supported_protocols: vec![],
                    session_timeout: Duration::from_secs(45),
                });
            }
            state.add_or_update_member(next);
        }
        let classic = CoordinatorGroup::new_classic("g");

        let rows: [Row<'_>; 8] = [
            Row {
                name: "classic group, any member",
                group: &classic,
                member_id: Some("ghost"),
                epoch: 3,
                expected: Ok(()),
            },
            Row {
                name: "admin fetch",
                group: &consumer,
                member_id: None,
                epoch: -1,
                expected: Ok(()),
            },
            Row {
                name: "native member, its epoch",
                group: &consumer,
                member_id: Some("native"),
                epoch: 7,
                expected: Ok(()),
            },
            Row {
                name: "native member, other epoch",
                group: &consumer,
                member_id: Some("native"),
                epoch: 6,
                expected: Err(codes::STALE_MEMBER_EPOCH),
            },
            Row {
                name: "classic member, its generation",
                group: &consumer,
                member_id: Some("classic"),
                epoch: 7,
                expected: Ok(()),
            },
            Row {
                name: "classic member, other generation",
                group: &consumer,
                member_id: Some("classic"),
                epoch: 8,
                expected: Err(codes::ILLEGAL_GENERATION),
            },
            Row {
                name: "unknown member",
                group: &consumer,
                member_id: Some("ghost"),
                epoch: 7,
                expected: Err(codes::UNKNOWN_MEMBER_ID),
            },
            Row {
                name: "no member id with an epoch",
                group: &consumer,
                member_id: None,
                epoch: 0,
                expected: Err(codes::UNKNOWN_MEMBER_ID),
            },
        ];
        for Row {
            name,
            group,
            member_id,
            epoch,
            expected,
        } in rows
        {
            check!(
                group.validate_offset_fetch(member_id, epoch) == expected,
                "{name}"
            );
        }
    }

    #[test]
    fn pending_txn_offsets_flatten_across_producers_and_clear_per_producer() {
        let mut g = CoordinatorGroup::new_classic("g");
        check!(g.offsets().pending_txn.is_empty());

        g.add_pending_txn_offsets(7, 10, [
            ("orders".to_string(), 0),
            ("orders".to_string(), 1),
        ]);
        g.add_pending_txn_offsets(9, 11, [("payments".to_string(), 3)]);
        check!(
            g.offsets().pending_txn
                == HashSet::from([
                    ("orders".to_string(), 0),
                    ("orders".to_string(), 1),
                    ("payments".to_string(), 3),
                ])
        );

        g.resolve_pending_txn_offsets(7, 12);
        check!(g.offsets().pending_txn == HashSet::from([("payments".to_string(), 3)]));

        g.resolve_pending_txn_offsets(9, 13);
        check!(g.offsets().pending_txn.is_empty());
    }

    /// `TxnOffsetCommit` marks its keys only after the append is durable, so
    /// the producer's marker can resolve on this group in between. The log
    /// order decides: a mark for records below an applied marker belongs to
    /// the transaction that marker ended, and taking it would leave the
    /// partition answering `UNSTABLE_OFFSET_COMMIT` with nothing left to
    /// clear it. Records above the marker are a new transaction and still
    /// count.
    #[test]
    fn a_mark_for_records_below_an_applied_marker_is_dropped() {
        let mut g = CoordinatorGroup::new_classic("g");

        // The abort marker for the producer's records at offset 40 lands at
        // offset 55 while its `TxnOffsetCommit` is still in flight.
        g.resolve_pending_txn_offsets(7, 55);
        g.add_pending_txn_offsets(7, 40, [("orders".to_string(), 0)]);
        check!(g.offsets().pending_txn.is_empty());

        // The producer's next transaction writes above the marker and is
        // pending as usual.
        g.add_pending_txn_offsets(7, 60, [("orders".to_string(), 1)]);
        check!(g.offsets().pending_txn == HashSet::from([("orders".to_string(), 1)]));

        // Another producer's marker cannot resolve it.
        g.resolve_pending_txn_offsets(9, 70);
        check!(g.offsets().pending_txn == HashSet::from([("orders".to_string(), 1)]));
    }
}
