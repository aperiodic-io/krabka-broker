//! Fixtures shared by the WAL quorum unit tests: a synthetic record batch, and
//! an append that reaches the source log through the real produce path.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use krabka_ids::{Offset, PartitionIndex};
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use uuid::Uuid;

use super::{QuorumWalStore, engine::WalShardEngine};
use crate::error::BrokerError;

pub(super) fn open_log(path: &Path) -> Arc<Mutex<Log>> {
    Arc::new(Mutex::new(Log::open(path, LogConfig::default()).unwrap()))
}

pub(super) fn source_log(root: &Path) -> Arc<Mutex<Log>> {
    open_log(&root.join("source"))
}

pub(super) fn local_replicas(count: usize) -> (Vec<tempfile::TempDir>, Arc<WalShardEngine>) {
    let dirs = (0..count)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let logs = dirs
        .iter()
        .enumerate()
        .map(|(index, dir)| {
            (
                NodeId(u64::try_from(index + 1).unwrap()),
                open_log(dir.path()),
            )
        })
        .collect();
    (dirs, Arc::new(WalShardEngine::for_logs(logs)))
}

pub(super) fn partition_store(
    root: &Path,
    source: Arc<Mutex<Log>>,
    count: usize,
) -> QuorumWalStore {
    QuorumWalStore::for_partition("topic", None, PartitionIndex(0), root, source, None, count)
        .unwrap()
}

pub(super) fn distributed_store(
    source: Arc<Mutex<Log>>,
    topic_id: Uuid,
    count: usize,
) -> QuorumWalStore {
    QuorumWalStore::for_distributed_partition(topic_id, PartitionIndex(0), source, None, count)
        .unwrap()
}

pub(super) fn distributed_engine(
    source: &Arc<Mutex<Log>>,
    count: usize,
    voters: &[NodeId],
) -> WalShardEngine {
    let engine = WalShardEngine::new_distributed(Arc::clone(source), count).unwrap();
    engine.configure_distributed(NodeId(1), voters);
    engine
}

pub(super) fn batch(records: i32) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: records - 1,
        ..RecordBatch::default()
    };
    for offset_delta in 0..records {
        batch.records.push(Record {
            offset_delta,
            ..Record::default()
        });
    }
    batch
}

pub(super) async fn append_source(
    store: &QuorumWalStore,
    records: i32,
) -> (
    Vec<Result<crate::partition::AppendedBatch, BrokerError>>,
    Offset,
) {
    let (results, leo, _) = crate::partition_writer::run_produce_append_batch(
        store.source.clone(),
        None,
        (
            vec![crate::partition::ProduceData::Owned(batch(records))],
            Vec::new(),
        ),
    )
    .await
    .unwrap();
    (results, leo)
}

/// A three-replica partition with a fresh source log under the caller's root.
pub(super) fn fresh_partition_store(root: &Path) -> QuorumWalStore {
    partition_store(root, source_log(root), 3)
}

/// Append two independently acknowledged source batches for frontier tests.
pub(super) async fn append_two(store: &QuorumWalStore) -> (Offset, Offset) {
    let (_, first) = append_source(store, 1).await;
    let (_, second) = append_source(store, 1).await;
    (first, second)
}

/// A distributed three-voter store with a fresh log and cluster identity.
pub(super) fn fresh_distributed_store(root: &Path) -> QuorumWalStore {
    distributed_store(source_log(root), Uuid::new_v4(), 3)
}

/// Source directory and open handle for tests that close and reopen a quorum.
pub(super) fn reopenable_source(root: &Path) -> (std::path::PathBuf, Arc<Mutex<Log>>) {
    let dir = root.join("source");
    let log = open_log(&dir);
    (dir, log)
}
