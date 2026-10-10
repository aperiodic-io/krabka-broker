//! Quorum-backed diskless WAL implementation.

pub(crate) mod engine;
pub(crate) mod follower;
pub(crate) mod log_view;
#[cfg(test)]
mod membership;
pub(crate) mod placement;
pub(crate) mod registry;
mod shard_dirs;
pub(crate) mod wire;

#[cfg(test)]
mod distributed_tests;
#[cfg(test)]
mod durability_tests;
#[cfg(test)]
mod fetch_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod test_support;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use krabka_ids::{Offset, PartitionIndex};
#[cfg(test)]
use krabka_kraft_core::NodeId;
use krabka_log::Log;
#[cfg(test)]
use krabka_log::LogConfig;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use uuid::Uuid;

use self::engine::WalShardEngine;
pub(crate) use self::shard_dirs::{
    ShardRemoval, prune_orphaned_shard_dirs, remove_shard, shard_dir,
};
#[cfg(test)]
use self::{
    engine::OpenMode,
    membership::{load_or_prepare_quorum_membership, persist_quorum_membership},
};
use super::WalStore;
use crate::error::BrokerError;

/// A [`WalStore`] backed by a quorum of durable WAL replica logs.
#[derive(Debug, Clone)]
pub(crate) struct QuorumWalStore {
    source: Arc<Mutex<Log>>,
    engine: Arc<WalShardEngine>,
    hot_tail: Option<HotTailTarget>,
}

#[derive(Debug, Clone)]
struct HotTailTarget {
    topic_id: Uuid,
    partition: PartitionIndex,
    cache: Arc<crate::diskless::hot_tail::HotTailCache>,
}

impl QuorumWalStore {
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(source: Arc<Mutex<Log>>, engine: Arc<WalShardEngine>) -> Self {
        Self {
            source,
            engine,
            hot_tail: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_partition(
        topic: &str,
        topic_id: Option<Uuid>,
        partition: PartitionIndex,
        log_dir: &std::path::Path,
        source: Arc<Mutex<Log>>,
        hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
        replica_count: usize,
    ) -> Result<Self, BrokerError> {
        let config = source
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config_snapshot();
        let mut replicas = Vec::with_capacity(replica_count);
        replicas.push(engine::WalReplica::for_test(NodeId(0), source.clone()));
        let root = shard_dir(log_dir, topic, topic_id, partition);
        for id in 1..replica_count {
            let id = NodeId(u64::try_from(id).map_err(|_| {
                BrokerError::Replication("diskless WAL replica count exceeds u64".into())
            })?);
            let replica_dir = root.join(format!("replica-{}", id.0));
            let log = Log::open(&replica_dir, replica_config(&config))?;
            replicas.push(engine::WalReplica::for_test(id, Arc::new(Mutex::new(log))));
        }
        let voter_ids: Vec<_> = replicas.iter().map(engine::WalReplica::id).collect();
        let is_new = load_or_prepare_quorum_membership(&root, &voter_ids)?;
        let mode = if is_new {
            OpenMode::BootstrapFrom(NodeId(0))
        } else {
            OpenMode::Recover
        };
        let engine = Arc::new(WalShardEngine::new(replicas, mode)?);
        if is_new {
            persist_quorum_membership(&root, &voter_ids)?;
        }
        let hot_tail = topic_id
            .zip(hot_tail)
            .map(|(topic_id, cache)| HotTailTarget {
                topic_id,
                partition,
                cache,
            });
        Ok(Self {
            source,
            engine,
            hot_tail,
        })
    }

    pub(crate) fn for_distributed_partition(
        topic_id: Uuid,
        partition: PartitionIndex,
        source: Arc<Mutex<Log>>,
        hot_tail: Option<Arc<crate::diskless::hot_tail::HotTailCache>>,
        voter_count: usize,
    ) -> Result<Self, BrokerError> {
        let engine = Arc::new(WalShardEngine::new_distributed(
            source.clone(),
            voter_count,
        )?);
        let hot_tail = hot_tail.map(|cache| HotTailTarget {
            topic_id,
            partition,
            cache,
        });
        Ok(Self {
            source,
            engine,
            hot_tail,
        })
    }

    pub(crate) fn engine(&self) -> Arc<WalShardEngine> {
        self.engine.clone()
    }
}

#[cfg(test)]
fn replica_config(config: &LogConfig) -> LogConfig {
    let mut config = config.clone();
    config.validate_on_open = true;
    config
}

#[async_trait]
impl WalStore for QuorumWalStore {
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        let start = self.engine.durable_watermark();
        let durable = self.engine.replicate_and_sync(&self.source, leo).await?;
        if let Some(target) = &self.hot_tail
            && durable > start
        {
            let raw = self
                .source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                // The hot tail must mirror every batch that just went durable,
                // so the read is uncapped.
                .read_raw(start, durable, ByteSize::from_bytes(u64::MAX))?;
            target
                .cache
                .insert_run(target.topic_id, target.partition, &raw.bytes);
        }
        Ok(durable)
    }

    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        let result = self.engine.trim_to_offset(&self.source, new_start).await;
        if result.is_ok() {
            self.invalidate_hot_tail();
        }
        result
    }

    fn invalidate_hot_tail(&self) {
        if let Some(target) = &self.hot_tail {
            target
                .cache
                .remove_partition(target.topic_id, target.partition);
        }
    }
}
