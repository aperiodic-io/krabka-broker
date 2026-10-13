//! The definition of a barrier group and the live entry the coordinator keeps.
//!
//! A caller supplies a [`GroupSpec`], the state topic carries a `GroupValue`,
//! and the coordinator holds a [`GroupEntry`] that folds the group record, the
//! cuts the group retains, and the injection that published nothing yet. The
//! epoch arithmetic reads all three of those sources, so it lives here with
//! them.

use {
    super::NO_EPOCH_YET,
    crate::{
        barrier::persistence::{CutValue, GroupValue, InjectionStartValue},
        metadata_epoch::next_i64,
    },
    krabka_units::Time,
    std::collections::BTreeMap,
};

/// The definition a caller supplies for a barrier group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`Time`] is backed by a
/// float.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupSpec {
    /// The topics the group cuts across.
    pub(crate) topics: Vec<String>,
    /// How often the coordinator injects without a trigger request. `None`
    /// turns periodic injection off.
    pub(crate) interval: Option<Time>,
    /// How many cuts the group keeps.
    pub(crate) retained_cuts: i32,
}

/// The injection-start record of an epoch that carries no cut record yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingInjection {
    pub(crate) epoch: i64,
    pub(crate) start: InjectionStartValue,
}

/// The live state of one barrier group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`GroupValue`] carries a
/// [`Time`].
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub(crate) struct GroupEntry {
    /// The last definition that a group record carried.
    #[default(GroupValue {
        topics: Vec::new(),
        interval: None,
        retained_cuts: 0,
        last_epoch: NO_EPOCH_YET,
    })]
    pub(crate) definition: GroupValue,
    /// The cuts the group retains, keyed by epoch and ordered by it.
    pub(crate) cuts: BTreeMap<i64, CutValue>,
    /// The injection that started and published no cut.
    pub(crate) pending: Option<PendingInjection>,
    /// When the scheduler should inject next, in milliseconds since the Unix
    /// epoch. It is `None` for a group that injects only on demand.
    pub(crate) next_due_ms: Option<i64>,
}

impl GroupEntry {
    /// The entry of a group the caller just defined.
    ///
    /// The coordinator folds an entry out of the state topic instead, so only
    /// the tests here build one straight from a spec.
    #[cfg(test)]
    pub(crate) fn from_spec(spec: GroupSpec, last_epoch: i64) -> Self {
        Self {
            definition: GroupValue {
                topics: spec.topics,
                interval: spec.interval,
                retained_cuts: spec.retained_cuts,
                last_epoch,
            },
            cuts: BTreeMap::new(),
            pending: None,
            next_due_ms: None,
        }
    }

    /// The highest epoch this group has allocated.
    ///
    /// The group record can carry an older value than the log holds, because a
    /// coordinator writes the injection-start record before it rewrites the
    /// group record. The maximum over all three sources is what makes an epoch
    /// impossible to reuse.
    pub(crate) fn last_epoch(&self) -> i64 {
        let from_pending = self.pending.as_ref().map_or(NO_EPOCH_YET, |p| p.epoch);
        let from_cuts = self.cuts.keys().copied().max().unwrap_or(NO_EPOCH_YET);
        self.definition.last_epoch.max(from_pending).max(from_cuts)
    }

    /// Whether a group record ever defined this entry.
    ///
    /// A cut record of a retained epoch can sit before the newest group record
    /// in the log, so the fold creates an entry for it. A group needs at least
    /// one topic, so an entry with no topic saw no group record.
    pub(crate) fn is_defined(&self) -> bool { !self.definition.topics.is_empty() }
}

/// The next epoch of a group.
pub(crate) fn next_epoch(last_epoch: i64) -> Option<i64> { next_i64(last_epoch) }

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::{
            persistence::CutStatus,
            state::test_support::{cut_value, start_value},
        },
        assert2::assert,
    };

    #[test]
    fn the_next_epoch_follows_the_last_one() {
        assert!(next_epoch(NO_EPOCH_YET) == Some(1));
        assert!(next_epoch(41) == Some(42));
        assert!(next_epoch(i64::MAX).is_none());
    }

    #[test]
    fn the_last_epoch_is_the_highest_of_every_source() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: None,
                retained_cuts: 4,
            },
            3,
        );
        assert!(entry.last_epoch() == 3);

        entry.cuts.insert(5, cut_value(CutStatus::Complete));
        assert!(entry.last_epoch() == 5);

        entry.pending = Some(PendingInjection {
            epoch: 6,
            start: start_value(1),
        });
        assert!(entry.last_epoch() == 6);
    }
}
