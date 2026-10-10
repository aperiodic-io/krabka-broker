//! Segment fixtures shared by the filesystem and object-store backend tests.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use uuid::Uuid;

use crate::{
    metadata::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    },
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionIndex {
    #[default]
    Present,
    Omitted,
}

pub fn sample_metadata(id: Uuid) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(TopicIdPartition::new(Uuid::from_u128(1), "orders", 0), id),
        0,
        99,
        123,
        1,
        456,
        crate::metadata::RemoteLogSegmentDetails::new(
            8,
            RemoteLogSegmentState::CopySegmentStarted,
            maplit::btreemap! {LeaderEpoch(0) => 0},
        ),
    )
    .unwrap()
}

pub fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

pub fn sample_data(src: &Path, transaction_index: TransactionIndex) -> LogSegmentData {
    LogSegmentData {
        log_segment: write_file(src, "00.log", b"0123456789"),
        offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
        time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
        transaction_index: (transaction_index == TransactionIndex::Present)
            .then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
        producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    }
}

/// The independent literal contents every sample index is expected to preserve.
pub fn check_sample_indexes(
    store: &impl RemoteStorageManager,
    metadata: &RemoteLogSegmentMetadata,
) {
    for (index_type, expected) in [
        (IndexType::Offset, b"OFFSET-IDX".as_ref()),
        (IndexType::Timestamp, b"TIME-IDX".as_ref()),
        (IndexType::ProducerSnapshot, b"SNAP".as_ref()),
        (IndexType::LeaderEpoch, b"EPOCH-BYTES".as_ref()),
        (IndexType::Transaction, b"TXN-IDX".as_ref()),
    ] {
        assert2::check!(
            store.fetch_index(metadata, index_type).unwrap() == expected,
            "{index_type:?}"
        );
    }
}
