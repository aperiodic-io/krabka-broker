//! The [`RemoteStorageManager`] SPI: copy / fetch / delete of segment data
//! and indexes to and from the remote tier.

use {
    crate::{
        error::RemoteStorageError,
        metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    },
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::Bytes,
    std::path::{Path, PathBuf},
    uuid::Uuid,
};

/// Trace each storage operation with the same remote-segment identity.
macro_rules! remote_operation {
    (copy($receiver:ident, $metadata:ident, $data:ident) $body:block) => {
        $crate::storage_manager::remote_operation! {
            [] $metadata => [start_offset = $metadata.start_offset(), end_offset = $metadata.end_offset(),];
            fn copy_log_segment_data(
                &$receiver,
                $metadata: &$crate::metadata::RemoteLogSegmentMetadata,
                $data: &$crate::storage_manager::LogSegmentData,
            ) -> Result<Option<$crate::metadata::CustomMetadata>, $crate::error::RemoteStorageError> $body
        }
    };
    (fetch($receiver:ident, $metadata:ident, $start:ident, $end:ident) $body:block) => {
        $crate::storage_manager::remote_operation! {
            [level = "debug",] $metadata => [start_position = $start, end_position = ?$end,];
            fn fetch_log_segment(
                &$receiver,
                $metadata: &$crate::metadata::RemoteLogSegmentMetadata,
                $start: u32,
                $end: Option<u32>,
            ) -> Result<Vec<u8>, $crate::error::RemoteStorageError> $body
        }
    };
    (index($receiver:ident, $metadata:ident, $kind:ident) $body:block) => {
        $crate::storage_manager::remote_operation! {
            [level = "debug",] $metadata => [index_type = ?$kind,];
            fn fetch_index(
                &$receiver,
                $metadata: &$crate::metadata::RemoteLogSegmentMetadata,
                $kind: $crate::storage_manager::IndexType,
            ) -> Result<Vec<u8>, $crate::error::RemoteStorageError> $body
        }
    };
    (delete($receiver:ident, $metadata:ident) $body:block) => {
        $crate::storage_manager::remote_operation! {
            [] $metadata => [];
            fn delete_log_segment_data(
                &$receiver,
                $metadata: &$crate::metadata::RemoteLogSegmentMetadata,
            ) -> Result<(), $crate::error::RemoteStorageError> $body
        }
    };
    ([$($options:tt)*] $metadata:ident => [$($fields:tt)*]; $implementation:item) => {
        #[tracing::instrument(
            $($options)*
            skip_all,
            fields(
                topic_id = %$metadata.remote_log_segment_id().topic_id_partition.topic_id,
                partition = $metadata.remote_log_segment_id().topic_id_partition.partition,
                segment = %$metadata.remote_log_segment_id().id,
                $($fields)*
            ),
            err
        )]
        $implementation
    };
}

pub(crate) use remote_operation;

/// The kinds of index a segment carries alongside its `.log` data.
///
/// Mirrors Kafka's `RemoteStorageManager.IndexType`. A
/// [`RemoteStorageManager`] copies all of these on
/// [`RemoteStorageManager::copy_log_segment_data`] and serves any of them
/// back on [`RemoteStorageManager::fetch_index`].
///
/// `suffix` is the Kafka `LocalTieredStorage` filename suffix of each index
/// type. Its remote leader-epoch artifact uses `.leader_epoch_checkpoint`,
/// distinct from a partition log's local `leader-epoch-checkpoint` file.
/// `from_suffix` is its inverse. A reader that discovers an archive from the
/// object store alone identifies each artifact by the suffix of its key, and
/// every key that is not an index ends in [`LOG_FILE_SUFFIX`], so `None` means
/// "not an index". `ALL` lists every index kind, so a caller that has to act
/// on all of them -- such as dropping one segment's whole cache footprint --
/// cannot silently miss one that is added later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, krabka_macros::EnumStr)]
#[enum_str(as_str = suffix, parse = from_suffix, all)]
pub enum IndexType {
    /// Sparse offset → byte-position index (`.index`).
    #[enum_str(name = ".index")]
    Offset,
    /// Sparse timestamp → relative-offset index (`.timeindex`).
    #[enum_str(name = ".timeindex")]
    Timestamp,
    /// Producer id snapshot (`.snapshot`).
    #[enum_str(name = ".snapshot")]
    ProducerSnapshot,
    /// Leader-epoch checkpoint (`.leader_epoch_checkpoint` in Kafka's
    /// `LocalTieredStorage`).
    #[enum_str(name = ".leader_epoch_checkpoint")]
    LeaderEpoch,
    /// Aborted-transaction index (`.txnindex`). It is optional. A segment
    /// with no aborted transactions has none.
    #[enum_str(name = ".txnindex")]
    Transaction,
}

/// Filename suffix of a segment's data, as distinct from one of its indexes.
pub const LOG_FILE_SUFFIX: &str = ".log";

/// Every artifact a segment copy can write, including optional indexes.
pub fn segment_suffixes() -> impl Iterator<Item = &'static str> {
    std::iter::once(LOG_FILE_SUFFIX).chain(IndexType::ALL.iter().map(|index| index.suffix()))
}

/// Character count of a Kafka Base64-rendered UUID.
///
/// The 16 raw bytes encode to 22 URL-safe unpadded Base64 characters. The
/// width is fixed, which is what lets [`parse_partition_dir_name`] find the
/// topic id in a name whose topic component may itself contain `-`.
const KAFKA_UUID_LEN: usize = 22;

/// Character count of the zero-padded base offset that starts a segment
/// filename. `i64::MAX` is 19 digits, so the field never overflows its width.
const BASE_OFFSET_LEN: usize = 20;

/// Kafka renders UUIDs in URL-safe, unpadded Base64 for remote-tier paths.
#[must_use]
pub fn kafka_uuid(uuid: Uuid) -> String { URL_SAFE_NO_PAD.encode(uuid.as_bytes()) }

/// Reads back a UUID that [`kafka_uuid`] rendered.
///
/// Returns `None` unless the input is URL-safe unpadded Base64 that decodes to
/// exactly the 16 bytes of a UUID. The engine rejects a non-canonical encoding,
/// so one UUID has one accepted spelling and the round trip is exact.
#[must_use]
pub fn decode_kafka_uuid(encoded: &str) -> Option<Uuid> {
    let bytes: [u8; 16] = URL_SAFE_NO_PAD.decode(encoded).ok()?.try_into().ok()?;
    Some(Uuid::from_bytes(bytes))
}

/// Directory name used by Kafka's `LocalTieredStorage` for a partition.
#[must_use]
pub fn partition_dir_name(metadata: &RemoteLogSegmentMetadata) -> String {
    let tp = &metadata.remote_log_segment_id().topic_id_partition;
    format!("{}-{}-{}", tp.topic, tp.partition, kafka_uuid(tp.topic_id))
}

/// The partition that a remote-tier directory name identifies.
///
/// A reader that discovers an archive from object storage alone has nothing but
/// the keys, so the directory component of a key is where the partition comes
/// from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionDirName {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Stable topic UUID, as assigned at topic creation.
    pub topic_id: Uuid,
}

/// Reads back the partition that [`partition_dir_name`] rendered.
///
/// The name is parsed from the right, because a topic name may contain `-`
/// while neither the partition index nor the fixed-width topic id can. Returns
/// `None` when a component is absent or malformed, which includes a topic that
/// is empty or that holds the `/` key separator.
#[must_use]
pub fn parse_partition_dir_name(name: &str) -> Option<PartitionDirName> {
    let (head, topic_id) = name.split_at_checked(name.len().checked_sub(KAFKA_UUID_LEN)?)?;
    let topic_id = decode_kafka_uuid(topic_id)?;
    let (topic, partition) = head.strip_suffix('-')?.rsplit_once('-')?;
    if topic.is_empty() || topic.contains('/') || !partition.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(PartitionDirName {
        topic: topic.to_owned(),
        partition: partition.parse().ok()?,
        topic_id,
    })
}

/// Filename used by Kafka's `LocalTieredStorage` for one segment artifact.
#[must_use]
pub fn segment_file_name(metadata: &RemoteLogSegmentMetadata, suffix: &str) -> String {
    format!(
        "{:020}-{}{}",
        metadata.start_offset(),
        kafka_uuid(metadata.remote_log_segment_id().id),
        suffix
    )
}

/// The segment artifact that a remote-tier filename identifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentFileName<'a> {
    /// First offset the segment holds.
    pub base_offset: i64,
    /// Random per-segment UUID.
    pub segment_id: Uuid,
    /// Which artifact of the segment this is: [`LOG_FILE_SUFFIX`] for the data,
    /// or an [`IndexType::suffix`] for one of the indexes.
    pub suffix: &'a str,
}

/// Reads back the segment artifact that [`segment_file_name`] rendered.
///
/// Both leading components have a fixed width, so the name parses from the
/// left even though the Base64 alphabet contains the `-` separator. Returns
/// `None` when a component is absent or malformed.
#[must_use]
pub fn parse_segment_file_name(name: &str) -> Option<SegmentFileName<'_>> {
    let (base_offset, rest) = name.split_at_checked(BASE_OFFSET_LEN)?;
    if !base_offset.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (segment_id, suffix) = rest.strip_prefix('-')?.split_at_checked(KAFKA_UUID_LEN)?;
    Some(SegmentFileName {
        base_offset: base_offset.parse().ok()?,
        segment_id: decode_kafka_uuid(segment_id)?,
        suffix,
    })
}

/// The local files, and the in-memory leader-epoch bytes, that make up one
/// log segment for copy to the remote tier.
///
/// Mirrors Kafka's `LogSegmentData`. `transaction_index` is optional; a
/// segment with no aborted transactions has no `.txnindex` file.
/// `producer_snapshot_index` is optional too so third-party callers can copy
/// legacy segments that predate snapshots. krabka log exports provide one.
/// The broker passes the leader-epoch index as bytes and not as a path,
/// because it holds the relevant slice in memory at copy time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSegmentData {
    /// Path to the `.log` data file.
    pub log_segment: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index: PathBuf,
    /// Path to the `.txnindex` file, when present.
    pub transaction_index: Option<PathBuf>,
    /// Path to the producer-id `.snapshot` file, when present. Older segment
    /// sources can omit it; krabka log exports always provide one.
    pub producer_snapshot_index: Option<PathBuf>,
    /// Serialized leader-epoch index bytes for this segment's offset range.
    pub leader_epoch_index: Bytes,
}

/// The source of one remote segment artifact.
pub enum ArtifactBody<'a> {
    File(&'a Path),
    Memory(&'a Bytes),
}

impl LogSegmentData {
    /// Walk the copy order shared by filesystem and object-store backends.
    pub(crate) fn artifacts(&self) -> impl Iterator<Item = (&'static str, ArtifactBody<'_>)> {
        [
            Some((
                LOG_FILE_SUFFIX,
                ArtifactBody::File(self.log_segment.as_path()),
            )),
            Some((
                IndexType::Offset.suffix(),
                ArtifactBody::File(self.offset_index.as_path()),
            )),
            Some((
                IndexType::Timestamp.suffix(),
                ArtifactBody::File(self.time_index.as_path()),
            )),
            self.producer_snapshot_index.as_deref().map(|path| {
                (
                    IndexType::ProducerSnapshot.suffix(),
                    ArtifactBody::File(path),
                )
            }),
            Some((
                IndexType::LeaderEpoch.suffix(),
                ArtifactBody::Memory(&self.leader_epoch_index),
            )),
            self.transaction_index
                .as_deref()
                .map(|path| (IndexType::Transaction.suffix(), ArtifactBody::File(path))),
        ]
        .into_iter()
        .flatten()
    }
}

/// SPI for the remote object store that holds offloaded segment data.
///
/// Implementations are **synchronous and blocking**. They mirror Kafka's
/// `RemoteStorageManager`, which the broker drives from a dedicated thread
/// pool, and the broker wraps these calls in `spawn_blocking`.
/// Implementations must be `Send + Sync` so the broker can share one instance
/// across tasks.
pub trait RemoteStorageManager: Send + Sync {
    /// Copies a segment's data and all of its indexes to the remote tier.
    ///
    /// Returns optional [`CustomMetadata`], for example an object-store key
    /// or a version id. The broker records it on the segment and passes it
    /// back on every later fetch or delete for that segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] if any underlying store operation
    /// fails.
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError>;

    /// Fetches a byte range of a segment's `.log` data.
    ///
    /// `start_position` is the inclusive starting byte offset within the
    /// segment. `end_position`, when `Some`, is the inclusive last byte
    /// offset; when `None`, the read runs to the end of the segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment is
    /// not present, or [`RemoteStorageError::Io`] on a store failure.
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Fetches one of a segment's indexes in full.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment or the
    /// requested index is not present, or [`RemoteStorageError::Io`] on a
    /// store failure.
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Deletes a segment's data and all of its indexes from the remote tier.
    ///
    /// Implementations must be idempotent: a delete of an absent segment
    /// succeeds — or return an error, when the backend is an immutable
    /// archive.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Io`] on a store failure.
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;
}

#[cfg(test)]
mod tests;
