//! The per-`group_id` type lock that keeps the classic, next-gen, share, and
//! streams namespaces from colliding on one id.
//!
//! Every mark is first-mark-wins, except the three forced transitions that an
//! in-place upgrade or downgrade needs. They belong together because the
//! forced variants are only correct next to the marks they override.

use super::group_coordinator::{GroupCoordinator, GroupType};

impl GroupCoordinator {
    /// The locked protocol type for `group_id`, if the coordinator recorded
    /// one.
    ///
    /// Share groups from KIP-932 record their lock here with
    /// [`mark_share`](Self::mark_share). Classic and next-gen actors also
    /// enforce their lock through the actor [`GroupKindTag`].
    ///
    /// [`GroupKindTag`]: super::actor::GroupKindTag
    #[must_use]
    pub fn group_type(&self, group_id: &str) -> Option<GroupType> {
        self.group_types.get(group_id).map(|e| *e.value())
    }

    pub fn mark_classic(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Classic);
    }

    /// After an in-place KIP-848 downgrade, drop the consumer seed and record
    /// the group as classic.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as next-gen
    /// again. [`Self::mark_classic`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Classic`. A downgrade must override
    /// any earlier `NextGen` lock that the group carried while it was a
    /// consumer group.
    pub fn mark_classic_after_downgrade(&self, group_id: &str) {
        self.mark_after_consumer_transition(group_id, GroupType::Classic);
    }

    /// After an in-place classic→streams upgrade from KIP-1071, drop the
    /// classic seed and record the group as streams.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as classic
    /// again. [`Self::mark_streams`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Streams`. It overrides any earlier
    /// `Classic` lock that the group carried while it was a classic group.
    pub fn mark_streams_after_upgrade(&self, group_id: &str) {
        self.mark_after_consumer_transition(group_id, GroupType::Streams);
    }

    fn mark_after_consumer_transition(&self, group_id: &str, kind: GroupType) {
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.group_types.insert(group_id.into(), kind);
    }

    /// After an in-place streams→classic downgrade from KIP-1071, drop the
    /// streams seed and record the group as classic.
    ///
    /// The dropped seed keeps a respawn from hydrating the group as streams
    /// again. [`Self::mark_classic`] keeps the first mark through `or_insert`,
    /// but this method FORCES the type to `Classic`. It overrides any earlier
    /// `Streams` lock. It is the mirror of
    /// [`Self::mark_streams_after_upgrade`]. It drops the **streams** seeds,
    /// which are `streams_seeds` and `streams_seeds_cache`. It does not drop
    /// the consumer `seeds` that [`Self::mark_classic_after_downgrade`] drops.
    pub fn mark_classic_after_streams_downgrade(&self, group_id: &str) {
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.group_types.insert(group_id.into(), GroupType::Classic);
    }

    pub fn mark_next_gen(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::NextGen);
    }

    pub fn mark_share(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Share);
    }

    pub fn mark_streams(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Streams);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::coordinator::unified::test_support::make_coord, assert2::assert};

    #[test]
    fn mark_share_locks_group_type() {
        let coord = make_coord();
        coord.mark_share("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
        // First mark wins: a later mark_classic must not override.
        coord.mark_classic("sg");
        assert!(coord.group_type("sg") == Some(GroupType::Share));
    }

    #[test]
    fn mark_streams_after_upgrade_forces_streams_over_classic() {
        let c = make_coord();
        c.mark_classic("g");
        assert!(c.group_type("g") == Some(GroupType::Classic));
        // or_insert mark_streams must NOT override an existing Classic lock:
        c.mark_streams("g");
        assert!(c.group_type("g") == Some(GroupType::Classic));
        // The forced upgrade variant MUST override it:
        c.mark_streams_after_upgrade("g");
        assert!(c.group_type("g") == Some(GroupType::Streams));
    }

    #[test]
    fn mark_classic_after_streams_downgrade_forces_classic_over_streams() {
        let c = make_coord();
        c.mark_streams("g");
        assert2::assert!((c.group_type("g")) == (Some(GroupType::Streams)));
        // mark_classic is first-mark-wins, so it must NOT override an existing lock:
        c.mark_classic("g");
        assert2::assert!((c.group_type("g")) == (Some(GroupType::Streams)));
        // The forced downgrade variant MUST override it:
        c.mark_classic_after_streams_downgrade("g");
        assert2::assert!((c.group_type("g")) == (Some(GroupType::Classic)));
    }
}
