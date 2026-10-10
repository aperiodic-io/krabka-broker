//! Tests for opening a local-replica WAL quorum: how many replicas the
//! configured count creates, how an existing source log bootstraps them, and
//! which prefix a reopen recovers or discards.

use assert2::assert;
use krabka_ids::Offset;
use krabka_kraft_core::NodeId;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::test_support::{append_source, batch, open_log, partition_store, source_log};
use crate::wal::WalStore;

#[test]
fn partition_quorum_uses_configured_local_replica_count() {
    let dir = tempfile::tempdir().unwrap();
    let source = source_log(dir.path());

    partition_store(dir.path(), source, 2);

    let root = dir.path().join("__diskless_wal_quorum/topic-0");
    assert!(root.join("replica-1").is_dir());
    assert!(!root.join("replica-2").exists());
}

#[test]
fn partition_quorum_bootstraps_existing_source_into_every_replica() {
    let dir = tempfile::tempdir().unwrap();
    let source = source_log(dir.path());
    source.lock().unwrap().append(&mut batch(3)).unwrap();
    source.lock().unwrap().sync().unwrap();

    let store = partition_store(dir.path(), source, 3);

    assert!(store.engine.durable_watermark() == Offset(3));
    assert!(store.engine.replica_end_offsets() == vec![Offset(3), Offset(3), Offset(3)]);
    assert!(
        dir.path()
            .join("__diskless_wal_quorum/topic-0/quorum-state.json")
            .is_file()
    );
}

async fn committed_source_prefix(
    root: &std::path::Path,
    records: i32,
) -> (
    std::path::PathBuf,
    std::sync::Arc<std::sync::Mutex<krabka_log::Log>>,
    super::QuorumWalStore,
) {
    let (source_dir, source) = super::test_support::reopenable_source(root);
    let store = partition_store(root, source.clone(), 3);
    let (_, leo) = append_source(&store, records).await;
    assert!(store.sync_durable(leo).await.unwrap() == Offset(i64::from(records)));
    (source_dir, source, store)
}

#[tokio::test]
async fn partition_quorum_recovers_watermark_and_repairs_one_lost_replica() {
    let dir = tempfile::tempdir().unwrap();
    let (source_dir, source, store) = committed_source_prefix(dir.path(), 1).await;

    drop(store);
    drop(source);

    let lost_replica = dir.path().join("__diskless_wal_quorum/topic-0/replica-2");
    std::fs::remove_dir_all(&lost_replica).unwrap();
    let source = open_log(&source_dir);
    let reopened = partition_store(dir.path(), source, 3);

    assert!(reopened.engine.durable_watermark() == Offset(1));
    assert!(reopened.engine.replica_end_offsets() == vec![Offset(1), Offset(1), Offset(1)]);
    let fetch = reopened
        .engine
        .serve_fetch(Offset(0), -1, ByteSize::from_bytes(u64::MAX))
        .unwrap();
    assert!(fetch.high_watermark == Offset(1));
    assert!(!fetch.records.is_empty());
}

#[tokio::test]
async fn partition_quorum_discards_uncommitted_suffix_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (source_dir, source, store) = committed_source_prefix(dir.path(), 2).await;

    store.engine.set_replica_alive(NodeId(1), false);
    store.engine.set_replica_alive(NodeId(2), false);
    let (_results, leo) = append_source(&store, 1).await;
    assert!(store.sync_durable(leo).await.is_err());
    assert!(store.engine.replica_end_offsets() == vec![Offset(3), Offset(2), Offset(2)]);
    drop(store);
    drop(source);

    let source = open_log(&source_dir);
    let reopened = partition_store(dir.path(), source, 3);

    assert!(reopened.engine.durable_watermark() == Offset(2));
    assert!(reopened.engine.replica_end_offsets() == vec![Offset(2), Offset(2), Offset(2)]);
}
