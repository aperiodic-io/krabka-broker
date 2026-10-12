//! Share-partition leader manager (KIP-932).
//!
//! The manager owns one [`AcquisitionState`] machine per
//! `(group, topic_id, partition)` that this broker leads. It loads each machine
//! lazily from the durable `SharePersister` and persists it again each time it
//! goes dirty. The `ShareFetch` and `ShareAcknowledge` handlers drive the
//! per-cell state under its `tokio::sync::Mutex`. A background sweeper expires
//! the acquisition locks.
//!
//! Locking discipline: the `DashMap` guard is NEVER held across an `.await`.
//! Callers clone the cell `Arc` out of the map first, then lock and await.
//!
//! This file holds the type, its constructor, and its `Debug` view. The methods
//! sit in one module per concern: `sessions` runs the share-session lifecycle
//! and the release path a disconnect takes, `metadata_lookup` answers the
//! topic, leader, and epoch questions against the metadata image, `cells` owns
//! the lazily loaded per-partition machines, `persistence` writes a dirty
//! machine back to the persister, `sweeper` runs the background
//! acquisition-lock timeout, `dead_letter` runs the second phase of the
//! share-group dead-letter queue (KIP-1191), and `toggle` clears the manager
//! when the finalized `share.version` drops to 0.

use std::sync::{Arc, OnceLock, Weak};

use dashmap::DashMap;
use krabka_metadata::NodeId;
use tokio::sync::Mutex;

use crate::{
    coordinator::unified::share::config::ShareGroupConfig,
    diskless::read::DisklessReadHandle,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
    remote_reader::RemoteReader,
    share_coordinator::persister_client::SharePersister,
    share_partition::{dlq::DlqSink, session::ShareSessionCache, state::AcquisitionState},
};

mod cells;
mod dead_letter;
mod metadata_lookup;
pub(crate) mod persistence;
mod sessions;
mod sweeper;
mod toggle;

#[cfg(test)]
pub(crate) mod test_support;

/// Live acquisition-state machines keyed by `(group, topic_id, partition)`.
type LeaderKey = (String, uuid::Uuid, i32);

/// Per-broker owner of the share-partition acquisition state machines.
///
/// The manager owns one machine for each `(group, topic, partition)` triple
/// that this broker leads.
pub(crate) struct SharePartitionLeaderManager {
    node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    persister: Arc<SharePersister>,
    config: Arc<ShareGroupConfig>,
    sessions: ShareSessionCache,
    leaders: DashMap<LeaderKey, Arc<Mutex<AcquisitionState>>>,
    /// Where the records of a group's dead-letter queue are written
    /// (KIP-1191).
    dlq: Arc<dyn DlqSink>,
    /// The writes to the dead-letter queue that may run at once, across every
    /// partition this broker leads: the writes that wait for a permit hold
    /// their records in `Archiving`, and hold the SPSO with them.
    dead_letter_writes: tokio::sync::Semaphore,
    /// This manager, for the tasks that finish a dead-letter write after the
    /// request that began it has answered.
    me: Weak<Self>,
    remote_reader: OnceLock<Arc<RemoteReader>>,
    diskless_read: OnceLock<Arc<DisklessReadHandle>>,
}

impl std::fmt::Debug for SharePartitionLeaderManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharePartitionLeaderManager")
            .field("node_id", &self.node_id)
            .field("live_partitions", &self.leaders.len())
            .finish_non_exhaustive()
    }
}

impl SharePartitionLeaderManager {
    pub(crate) fn new(
        node_id: NodeId,
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        persister: Arc<SharePersister>,
        config: Arc<ShareGroupConfig>,
        session_max: usize,
        dlq: Arc<dyn DlqSink>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            node_id,
            partitions,
            controller,
            persister,
            config,
            sessions: ShareSessionCache::new(session_max),
            leaders: DashMap::new(),
            dlq,
            dead_letter_writes: tokio::sync::Semaphore::new(
                dead_letter::MAX_CONCURRENT_DEAD_LETTER_WRITES,
            ),
            me: me.clone(),
            remote_reader: OnceLock::new(),
            diskless_read: OnceLock::new(),
        })
    }

    /// Install the cold sources once the broker runtime has constructed them.
    pub(crate) fn set_read_sources(
        &self,
        remote: Option<Arc<RemoteReader>>,
        diskless: Option<Arc<DisklessReadHandle>>,
    ) {
        if let Some(reader) = remote {
            let _ = self.remote_reader.set(reader);
        }
        if let Some(reader) = diskless {
            let _ = self.diskless_read.set(reader);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::manager;

    #[test]
    fn format_debug_reports_node_id_and_live_partitions() {
        let mgr = manager();
        let debug = format!("{mgr:?}");
        assert2::check!(debug.contains("SharePartitionLeaderManager"));
        assert2::check!(debug.contains("live_partitions: 0"));
    }
}
