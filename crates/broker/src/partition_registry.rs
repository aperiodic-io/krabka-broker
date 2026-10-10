//! Concurrent registry of locally-hosted partitions.
//!
//! The registry uses a nested `DashMap<Arc<str>, DashMap<i32, Arc<Partition>>>`,
//! so a lookup runs with a borrowed `&str` topic name and allocates nothing.
//!
//! A flat `Arc<DashMap<(String, i32), Arc<Partition>>>` would force every
//! lookup to allocate an owned `String` for the tuple key, as in
//! `partitions.get(&(topic.to_string(), idx))`. On the produce and fetch hot
//! path one request can resolve hundreds of partitions, which would make that
//! an O(partitions) burst of allocations.
//!
//! The outer key is an `Arc<str>` rather than a `String` for the same reason
//! read from the other side: the registry already owns one copy of every
//! hosted topic name, so a data-path caller that needs an *owned* name —
//! a metric label above all — can take a refcount bump off
//! [`PartitionRegistry::shared_topic_name`] instead of allocating and copying
//! its own.

use std::sync::Arc;

use dashmap::DashMap;
use krabka_ids::PartitionIndex;

use crate::partition::Partition;

/// Concurrent registry of locally-hosted partitions, keyed by
/// (topic, partition).
///
/// A nested `DashMap<Arc<str>, DashMap<PartitionIndex, Arc<Partition>>>` backs
/// it, so a lookup runs with a borrowed `&str` topic name and allocates no
/// `String`. That matters on the produce and fetch hot path, where one request
/// can resolve hundreds of partitions.
#[derive(Debug, Default)]
pub(crate) struct PartitionRegistry {
    inner: DashMap<Arc<str>, DashMap<PartitionIndex, Arc<Partition>>>,
    preferred_log_dirs: DashMap<(uuid::Uuid, PartitionIndex), std::path::PathBuf>,
    /// KIP-1066: the log directories this broker holds cordoned, which new
    /// partition directories avoid. It lives beside the preferred directories
    /// because both decide where a partition this broker opens goes.
    cordoned_log_dirs: crate::cordoned_log_dirs::CordonedLogDirs,
    stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
}

impl PartitionRegistry {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Build a registry whose partitions share one internal timestamp source.
    #[must_use]
    pub(crate) fn with_stamp_source(
        stamp_source: Option<Arc<dyn krabka_log::StampSource>>,
    ) -> Self {
        Self {
            inner: DashMap::new(),
            preferred_log_dirs: DashMap::new(),
            cordoned_log_dirs: crate::cordoned_log_dirs::CordonedLogDirs::default(),
            stamp_source,
        }
    }

    /// Hold `cordoned` as the set of cordoned log directories.
    #[must_use]
    pub(crate) fn with_cordoned_log_dirs(
        mut self,
        cordoned: crate::cordoned_log_dirs::CordonedLogDirs,
    ) -> Self {
        self.cordoned_log_dirs = cordoned;
        self
    }

    /// The log directories this broker holds cordoned.
    #[must_use]
    pub(crate) fn cordoned_log_dirs(&self) -> &crate::cordoned_log_dirs::CordonedLogDirs {
        &self.cordoned_log_dirs
    }

    /// Return the source to install on a newly opened partition log.
    #[must_use]
    pub(crate) fn stamp_source(&self) -> Option<Arc<dyn krabka_log::StampSource>> {
        self.stamp_source.as_ref().map(Arc::clone)
    }

    /// Allocation-free lookup. It returns a cheap `Arc` clone of the
    /// partition, if the registry holds it.
    #[must_use]
    pub(crate) fn get(&self, topic: &str, partition: PartitionIndex) -> Option<Arc<Partition>> {
        self.inner
            .get(topic)
            .and_then(|m| m.get(&partition).map(|p| Arc::clone(&p)))
    }

    /// The registry's own copy of `topic`, as a cheap `Arc` clone.
    ///
    /// The outer map is keyed by `Arc<str>`, so a caller that needs an owned
    /// topic name pays one refcount bump rather than an allocation and a
    /// copy of the bytes. The metric label sets are the caller that matters:
    /// [`crate::metrics::PartitionLabel`] and
    /// [`crate::metrics::TopicLabel`] hold an `Arc<str>`, and the produce,
    /// fetch and replication paths build one of them per partition per
    /// request.
    ///
    /// A topic this broker hosts no partition of has no shared copy to give,
    /// and the name is allocated instead. That is the cold path by
    /// construction: the data path reaches a metric only for a topic it just
    /// resolved a local partition of, and everything else here is a
    /// once-per-request miss.
    #[must_use]
    pub(crate) fn shared_topic_name(&self, topic: &str) -> Arc<str> {
        self.inner
            .get(topic)
            .map_or_else(|| Arc::from(topic), |e| Arc::clone(e.key()))
    }

    /// Returns `true` if the given (topic, partition) is hosted locally.
    #[must_use]
    pub(crate) fn contains(&self, topic: &str, partition: PartitionIndex) -> bool {
        self.inner
            .get(topic)
            .is_some_and(|m| m.contains_key(&partition))
    }

    /// Inserts a partition and replaces any current one. It returns the
    /// previous value, if there was one.
    pub(crate) fn insert(
        &self,
        topic: Arc<str>,
        partition: PartitionIndex,
        part: Arc<Partition>,
    ) -> Option<Arc<Partition>> {
        self.inner.entry(topic).or_default().insert(partition, part)
    }

    /// Removes a partition and returns it, if the registry held it. It also
    /// drops the topic's inner map when that map becomes empty.
    pub(crate) fn remove(&self, topic: &str, partition: PartitionIndex) -> Option<Arc<Partition>> {
        let removed = self
            .inner
            .get(topic)
            .and_then(|m| m.remove(&partition).map(|(_, v)| v));
        // Prune empty inner map (best-effort; safe because a later `insert`
        // re-creates it via `entry(..).or_default()`).
        self.inner.remove_if(topic, |_, m| m.is_empty());
        removed
    }

    /// Materializes a partition atomically, and only when the registry does
    /// not hold it. It runs `build` UNDER the per-key lock, so two concurrent
    /// materializations of the same (topic, partition) can never both build.
    /// That keeps the TOCTOU-free KIP-113 guarantee that the old
    /// `DashMap::entry` gave. This method calls `build` only when the slot is
    /// empty.
    pub(crate) fn materialize_if_vacant<E>(
        &self,
        topic: &str,
        partition: PartitionIndex,
        build: impl FnOnce() -> Result<Arc<Partition>, E>,
    ) -> Result<(), E> {
        use dashmap::mapref::entry::Entry;
        let inner = self.inner.entry(Arc::from(topic)).or_default();
        match inner.entry(partition) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(slot) => {
                let part = build()?;
                slot.insert(part);
                Ok(())
            }
        }
    }

    pub(crate) fn set_preferred_log_dir(
        &self,
        topic_id: uuid::Uuid,
        partition: PartitionIndex,
        log_dir: std::path::PathBuf,
    ) {
        self.preferred_log_dirs
            .insert((topic_id, partition), log_dir);
    }

    #[must_use]
    pub(crate) fn preferred_log_dir(
        &self,
        topic_id: uuid::Uuid,
        partition: PartitionIndex,
    ) -> Option<std::path::PathBuf> {
        self.preferred_log_dirs
            .get(&(topic_id, partition))
            .map(|entry| entry.value().clone())
    }

    pub(crate) fn clear_preferred_log_dir(&self, topic_id: uuid::Uuid, partition: PartitionIndex) {
        self.preferred_log_dirs.remove(&(topic_id, partition));
    }

    /// The partition indices currently hosted for `topic`. It is empty when
    /// the topic hosts nothing locally. `DeleteTopics` uses it to list the
    /// local partitions to remove, without a scan of every registry key.
    #[must_use]
    pub(crate) fn partitions_of(&self, topic: &str) -> Vec<PartitionIndex> {
        self.inner
            .get(topic)
            .map(|m| m.iter().map(|e| *e.key()).collect())
            .unwrap_or_default()
    }

    /// Snapshot of all partition handles, as cheap `Arc` clones. Maintenance
    /// sweeps use it.
    #[must_use]
    pub(crate) fn arcs(&self) -> Vec<Arc<Partition>> {
        self.inner
            .iter()
            .flat_map(|m| m.value().iter().map(|p| Arc::clone(&p)).collect::<Vec<_>>())
            .collect()
    }

    /// Total number of hosted partitions across all topics.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.inner.iter().map(|m| m.value().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig};
    use tempfile::tempdir;

    use super::PartitionRegistry;
    use crate::partition::Partition;

    /// Builds a `Partition` rooted at `<log_dir>/<topic>-<partition>` through
    /// the real `spawn_partition` path. It mirrors the test fixture in
    /// `future_log`.
    fn fixture_partition(log_dir: &Path, topic: &str, partition: PartitionIndex) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition.get());
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        crate::test_support::spawn_standalone_partition(
            log_dir,
            log,
            crate::test_support::StandalonePartitionSetup {
                topic,
                partition,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn insert_get_contains_remove() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        check!(reg.arcs().is_empty());
        check!(reg.arcs().len() == 0);
        check!(reg.get("t", PartitionIndex(0)).is_none());
        check!(!reg.contains("t", PartitionIndex(0)));

        let p = fixture_partition(dir.path(), "t", PartitionIndex(0));
        check!(
            reg.insert("t".into(), PartitionIndex(0), Arc::clone(&p))
                .is_none()
        );
        check!(reg.contains("t", PartitionIndex(0)));
        check!(!reg.arcs().is_empty());
        check!(reg.arcs().len() == 1);

        let got = reg.get("t", PartitionIndex(0)).expect("present");
        assert!(Arc::ptr_eq(&got, &p));

        // Replace returns previous.
        let p2 = fixture_partition(dir.path(), "t", PartitionIndex(0));
        let prev = reg
            .insert("t".into(), PartitionIndex(0), Arc::clone(&p2))
            .expect("prev");
        assert!(Arc::ptr_eq(&prev, &p));
        assert!(reg.arcs().len() == 1);

        let removed = reg.remove("t", PartitionIndex(0)).expect("removed");
        check!(Arc::ptr_eq(&removed, &p2));
        check!(reg.remove("t", PartitionIndex(0)).is_none());
        check!(!reg.contains("t", PartitionIndex(0)));
        check!(reg.arcs().is_empty());
    }

    /// The point of keying the outer map by `Arc<str>`: a caller that needs
    /// an owned topic name gets the registry's own allocation back, not a
    /// fresh copy of the bytes. `Arc::ptr_eq` is the only thing that can tell
    /// those two apart — string equality holds either way.
    #[tokio::test]
    async fn shared_topic_name_hands_back_the_registry_s_own_allocation() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();

        // A topic no partition of which is hosted here has no shared copy to
        // give, so the name is allocated. It still compares equal.
        let absent = reg.shared_topic_name("t");
        assert!(&*absent == "t");

        let key: Arc<str> = Arc::from("t");
        reg.insert(
            Arc::clone(&key),
            PartitionIndex(0),
            fixture_partition(dir.path(), "t", PartitionIndex(0)),
        );

        let shared = reg.shared_topic_name("t");
        check!(Arc::ptr_eq(&shared, &key));
        // The miss above really was one: it handed back a different
        // allocation from the one the registry now holds.
        check!(!Arc::ptr_eq(&absent, &key));

        // A second partition of the same topic does not fork the key.
        reg.insert(
            Arc::from("t"),
            PartitionIndex(1),
            fixture_partition(dir.path(), "t", PartitionIndex(1)),
        );
        check!(Arc::ptr_eq(&reg.shared_topic_name("t"), &key));
    }

    #[tokio::test]
    async fn partitions_of_and_len_track_topics_and_removals() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        assert!(reg.partitions_of("missing").is_empty());
        assert!(reg.len() == 0);

        reg.insert(
            "a".into(),
            PartitionIndex(2),
            fixture_partition(dir.path(), "a", PartitionIndex(2)),
        );
        reg.insert(
            "a".into(),
            PartitionIndex(4),
            fixture_partition(dir.path(), "a", PartitionIndex(4)),
        );
        reg.insert(
            "b".into(),
            PartitionIndex(7),
            fixture_partition(dir.path(), "b", PartitionIndex(7)),
        );

        let mut a_parts = reg.partitions_of("a");
        a_parts.sort_unstable();
        check!(a_parts == vec![PartitionIndex(2), PartitionIndex(4)]);
        check!(reg.partitions_of("b") == vec![PartitionIndex(7)]);
        check!(reg.len() == 3);

        let removed = reg.remove("a", PartitionIndex(2)).expect("removed");
        drop(removed);

        check!(reg.partitions_of("a") == vec![PartitionIndex(4)]);
        check!(reg.partitions_of("missing") == Vec::<PartitionIndex>::new());
        check!(reg.len() == 2);
    }

    #[tokio::test]
    async fn materialize_if_vacant_builds_once() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        let p = fixture_partition(dir.path(), "t", PartitionIndex(1));
        reg.materialize_if_vacant::<String>("t", PartitionIndex(1), || Ok(Arc::clone(&p)))
            .expect("build ok");
        assert!(reg.contains("t", PartitionIndex(1)));

        // Already occupied: build closure must not run.
        reg.materialize_if_vacant::<String>("t", PartitionIndex(1), || {
            panic!("build must not be called when slot is occupied");
        })
        .expect("occupied ok");

        let got = reg.get("t", PartitionIndex(1)).expect("present");
        assert!(Arc::ptr_eq(&got, &p));
    }

    #[tokio::test]
    async fn materialize_if_vacant_propagates_error() {
        let reg = PartitionRegistry::new();
        let err =
            reg.materialize_if_vacant::<String>("t", PartitionIndex(2), || Err("boom".to_string()));
        assert!(err == Err("boom".to_string()));
        assert!(!reg.contains("t", PartitionIndex(2)));
    }

    #[test]
    fn preferred_log_dirs_are_scoped_to_the_topic_incarnation() {
        let reg = PartitionRegistry::new();
        let old_topic_id = uuid::Uuid::from_u128(1);
        let new_topic_id = uuid::Uuid::from_u128(2);
        reg.set_preferred_log_dir(old_topic_id, PartitionIndex(0), "/old".into());

        assert!(
            reg.preferred_log_dir(new_topic_id, PartitionIndex(0))
                .is_none()
        );
        assert!(reg.preferred_log_dir(old_topic_id, PartitionIndex(0)) == Some("/old".into()));
    }

    #[tokio::test]
    async fn arcs_snapshots_all_partitions() {
        let dir = tempdir().unwrap();
        let reg = PartitionRegistry::new();
        reg.insert(
            "a".into(),
            PartitionIndex(0),
            fixture_partition(dir.path(), "a", PartitionIndex(0)),
        );
        reg.insert(
            "a".into(),
            PartitionIndex(1),
            fixture_partition(dir.path(), "a", PartitionIndex(1)),
        );
        reg.insert(
            "b".into(),
            PartitionIndex(0),
            fixture_partition(dir.path(), "b", PartitionIndex(0)),
        );
        let arcs = reg.arcs();
        assert!(arcs.len() == 3);
    }
}
