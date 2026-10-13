//! The last-known-good seed caches, one per group protocol.
//!
//! Every successful actor write also writes here, so a respawned actor can be
//! hydrated from the cache when the previous instance died after a log-write
//! failure. The three protocols keep separate caches but the same read, write,
//! and drop shape, so they are defined together.

use super::{
    group_coordinator::GroupCoordinator,
    seeds::{GroupSeed, ShareGroupSeed, StreamsGroupSeed},
};

impl GroupCoordinator {
    /// Mutate the cached seed for `group_id` after a successful durable write.
    ///
    /// Applying only the records in the write avoids cloning the whole group
    /// after a one-member heartbeat while keeping cache and replay semantics
    /// identical.
    pub(crate) fn update_cached_seed(&self, group_id: &str, update: impl FnOnce(&mut GroupSeed)) {
        let mut seed = self.seeds_cache.entry(group_id.into()).or_default();
        update(seed.value_mut());
    }

    /// Remove a cached next-gen seed after its group-metadata tombstone is
    /// durable.
    pub(crate) fn remove_cached_seed(&self, group_id: &str) { self.seeds_cache.remove(group_id); }

    /// Fetch the most recently cached seed for `group_id`, if any.
    #[must_use]
    pub fn cached_seed(&self, group_id: &str) -> Option<GroupSeed> {
        self.seeds_cache.get(group_id).map(|e| e.value().clone())
    }

    /// Replace the cached share-group seed for `group_id`.
    ///
    /// The share actor calls this after every successful
    /// `OffsetsLog::append`.
    pub fn update_share_cache(&self, group_id: &str, seed: ShareGroupSeed) {
        self.share_seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached share-group seed for `group_id`, if any.
    #[must_use]
    pub fn cached_share_seed(&self, group_id: &str) -> Option<ShareGroupSeed> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().clone())
    }

    /// Replace the cached streams-group seed for `group_id`.
    ///
    /// The streams actor calls this after every successful
    /// `OffsetsLog::append`.
    pub fn update_streams_cache(&self, group_id: &str, seed: StreamsGroupSeed) {
        self.streams_seeds_cache.insert(group_id.into(), seed);
    }

    /// Fetch the most recently cached streams-group seed for `group_id`, if any.
    #[must_use]
    pub fn cached_streams_seed(&self, group_id: &str) -> Option<StreamsGroupSeed> {
        self.streams_seeds_cache
            .get(group_id)
            .map(|e| e.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{GroupType, test_support::make_coord},
        assert2::{assert, check},
    };

    #[test]
    fn cache_updates_and_forced_type_transitions_are_observable() {
        let coord = make_coord();
        check!(coord.cached_seed("g") == None);
        check!(coord.cached_share_seed("sg") == None);
        check!(coord.cached_streams_seed("st") == None);

        coord.update_cached_seed("g", |seed| {
            seed.group_epoch = 7;
            seed.target_epoch = 8;
        });
        let cached = coord.cached_seed("g").unwrap();
        assert!(cached.group_epoch == 7);
        assert!(cached.target_epoch == 8);

        coord.seeds.insert("g".into(), GroupSeed {
            group_epoch: 99,
            ..GroupSeed::default()
        });
        coord.mark_next_gen("g");
        assert!(coord.group_type("g") == Some(GroupType::NextGen));
        coord.mark_classic_after_downgrade("g");
        check!(coord.group_type("g") == Some(GroupType::Classic));
        check!(coord.seeds.get("g").is_none());
        check!(coord.cached_seed("g") == None);

        coord.update_share_cache("sg", ShareGroupSeed {
            group_epoch: 17,
            target_epoch: 18,
            ..ShareGroupSeed::default()
        });
        let share_cached = coord.cached_share_seed("sg").unwrap();
        assert!(share_cached.group_epoch == 17);
        assert!(share_cached.target_epoch == 18);

        coord.update_streams_cache("st", StreamsGroupSeed {
            group_epoch: 27,
            assignment_epoch: 28,
            ..StreamsGroupSeed::default()
        });
        let streams_cached = coord.cached_streams_seed("st").unwrap();
        assert!(streams_cached.group_epoch == 27);
        assert!(streams_cached.assignment_epoch == 28);
    }
}
