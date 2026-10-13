//! The logical diskless floor shared by share acquisition and initialization.

use {
    crate::{diskless::read::DisklessReadHandle, error::BrokerError, partition::Partition},
    krabka_log::Offset,
};

/// Local trimming leaves WAL records readable. Only the index's deletion
/// floor and oldest surviving range can raise a diskless share position.
pub(crate) async fn diskless_logical_start(
    reader: &DisklessReadHandle,
    partition: &Partition,
    topic_id: uuid::Uuid,
) -> Result<Offset, BrokerError> {
    if reader.index_log().is_some_and(|log| !log.is_valid()) {
        return Err(BrokerError::Txn(
            "diskless WAL index projection is unavailable or invalid".into(),
        ));
    }
    let local_start = partition.log_start_offset().0;
    let index = reader.index.lock().await;
    let floor = index.delete_floor(topic_id, partition.index.0);
    Ok(Offset(
        index
            .earliest_covered(topic_id, partition.index.0)
            .map_or(local_start, |remote| remote.min(local_start))
            .max(floor),
    ))
}

#[cfg(test)]
pub(crate) mod test_support {
    use {
        crate::{
            diskless::{
                read::DisklessReadHandle,
                wal_index::{WalFlushRecord, WalIndexCache, WalIndexEntry},
            },
            partition::{Partition, test_support::test_partition},
        },
        bytes::{Bytes, BytesMut},
        krabka_log::Offset,
        krabka_protocol::records::{Record, RecordBatch},
        object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path},
        std::sync::Arc,
    };

    /// Real batch bytes survive local trimming in an object and a replayed WAL index.
    pub(crate) async fn diskless_partition(
        timestamp: i64,
    ) -> (
        Arc<Partition>,
        Arc<DisklessReadHandle>,
        uuid::Uuid,
        tempfile::TempDir,
    ) {
        let (mut partition, dir) = test_partition(Arc::new(tokio::sync::Notify::new()));
        partition.diskless = true;
        let topic_id = uuid::Uuid::from_u128(0x0057_414c);
        let mut bytes = BytesMut::new();
        let mut entries = Vec::new();
        {
            let mut log = partition.log.lock().unwrap();
            for (offset, value) in [
                (0, b"cold-0".as_slice()),
                (1, b"cold-1".as_slice()),
                (2, b"warm".as_slice()),
            ] {
                let mut batch = RecordBatch {
                    base_timestamp: timestamp,
                    max_timestamp: timestamp,
                    records: vec![Record {
                        value: Some(Bytes::copy_from_slice(value)),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                log.append(&mut batch).unwrap();
                if offset < 2 {
                    let start = bytes.len();
                    batch.encode(&mut bytes).unwrap();
                    entries.push(WalIndexEntry {
                        topic_id,
                        partition: 0,
                        first_offset: offset,
                        last_offset: offset,
                        byte_start: u64::try_from(start).unwrap(),
                        byte_len: u32::try_from(bytes.len() - start).unwrap(),
                        max_timestamp_ms: timestamp,
                    });
                }
            }
            log.trim_to_offset(Offset(2)).unwrap();
        }
        partition.replica_state.lock().await.hw = Offset(3);
        let store = Arc::new(InMemory::new());
        store
            .put(&Path::from("share-wal"), PutPayload::from(bytes.freeze()))
            .await
            .unwrap();
        let mut index = WalIndexCache::default();
        index.apply(&WalFlushRecord {
            object_key: "share-wal".into(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries,
        });
        let reader = Arc::new(DisklessReadHandle::new(
            Arc::new(tokio::sync::Mutex::new(index)),
            store,
        ));
        (Arc::new(partition), reader, topic_id, dir)
    }
}
