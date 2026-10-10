//! The archive inventory: what the archive holds, before anything is read.
//!
//! This module owns the scan. It streams the archive listing under the
//! operator's prefix, decodes every key with the KIP-405 archive key codec
//! (`parse_partition_dir_name` and `parse_segment_file_name`), and groups
//! the result into one [`SegmentInventory`] per archived segment, with the
//! object key and size of each artifact that is present. The listing is
//! consumed as it arrives, so the scan holds one entry per archived segment
//! rather than one per object: an archive of tens of millions of keys is
//! exactly the archive a restore runs against. It keeps the keys it cannot
//! attribute rather than discarding them, because an unrecognized key is the
//! first sign that `--archive-prefix` points at the wrong tree, and it keeps a
//! bounded sample with a count of the rest, because a wrong prefix makes every
//! key in the bucket unrecognized. When
//! `--rlmm-snapshot` is given, this module reconciles the scan against it and
//! reports a disagreement instead of choosing a winner; without the snapshot a
//! segment the old cluster had marked for deletion looks exactly like a live
//! one. This module decides nothing about completeness or integrity. It
//! reports presence, and the verify stage judges it.
//!
//! This file holds the scan itself and the shapes it produces. The
//! snapshot comparison, which is the only part with a second source of truth to
//! weigh, lives beside it in [`self::reconcile`].

use std::collections::HashMap;

use futures_util::stream::TryStreamExt as _;
use krabka_ids::Offset;
use krabka_remote_storage::{
    IndexType, LOG_FILE_SUFFIX, TopicIdPartition, parse_partition_dir_name, parse_segment_file_name,
};
use object_store::{ObjectMeta, path::Path};
use uuid::Uuid;

mod reconcile;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::reconcile::reconcile_with_snapshot;
pub(crate) use self::reconcile::{load_snapshot, reconcile_authenticated_with_snapshot};
use crate::{args::RestoreArgs, backend::ArchiveStore, error::RestoreError};

/// One artifact of one archived segment, as the scan found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveObject {
    /// The full object key, prefix included.
    pub key: Path,
    /// The object size the store reports. It stays the store's own `u64`,
    /// because it is compared against byte positions inside the object.
    pub size: u64,
}

/// One archived segment and the artifacts the archive holds for it.
///
/// Kafka copies the log, the offset index, the time index, the producer
/// snapshot, and the leader-epoch checkpoint together. The transaction index
/// is absent for a segment with no aborted transaction, so its absence is not
/// by itself a torn copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentInventory {
    /// The per-copy segment id the archive names the artifacts with.
    pub segment_id: Uuid,
    /// First offset the segment holds, as the key encodes it.
    pub base_offset: Offset,
    /// The segment data.
    pub log: Option<ArchiveObject>,
    /// The sparse offset index.
    pub offset_index: Option<ArchiveObject>,
    /// The sparse time index.
    pub time_index: Option<ArchiveObject>,
    /// The producer id snapshot.
    pub producer_snapshot: Option<ArchiveObject>,
    /// The leader-epoch checkpoint.
    pub leader_epoch: Option<ArchiveObject>,
    /// The aborted-transaction index. It is optional.
    pub transaction_index: Option<ArchiveObject>,
}

impl SegmentInventory {
    /// Present artifacts in Kafka's log, offset, time, producer, epoch and transaction order.
    pub(crate) fn artifacts(&self) -> impl Iterator<Item = &ArchiveObject> {
        [
            self.log.as_ref(),
            self.offset_index.as_ref(),
            self.time_index.as_ref(),
            self.producer_snapshot.as_ref(),
            self.leader_epoch.as_ref(),
            self.transaction_index.as_ref(),
        ]
        .into_iter()
        .flatten()
    }
}

/// One partition of the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInventory {
    /// The partition the archive directory names.
    pub partition: TopicIdPartition,
    /// Every segment found for the partition, in base-offset order.
    pub segments: Vec<SegmentInventory>,
}

impl PartitionInventory {
    /// Order partitions by topic name and index, preserving scan order for equal keys.
    pub(crate) fn sort_by_topic_and_partition(partitions: &mut [Self]) {
        partitions.sort_by(|a, b| {
            (a.partition.topic.as_str(), a.partition.partition)
                .cmp(&(b.partition.topic.as_str(), b.partition.partition))
        });
    }
}

/// How many unattributable keys the scan keeps. Past this many the scan
/// counts and drops, because the sample is diagnostic: an operator who
/// pointed `--archive-prefix` at the wrong tree learns it from the first
/// handful of keys and from the total, not from the millionth key.
pub const UNRECOGNIZED_SAMPLE_LIMIT: usize = 64;

/// The keys the scan could not attribute to a segment.
///
/// Bounded on purpose: the count is the whole truth, and the sample is the
/// part worth printing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnrecognizedKeys {
    /// The first [`UNRECOGNIZED_SAMPLE_LIMIT`] such keys, in listing order.
    pub sample: Vec<Path>,
    /// Keys past the sample, counted but not kept.
    pub omitted: u64,
}

impl UnrecognizedKeys {
    /// Whether the scan attributed every key it saw.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sample.is_empty() && self.omitted == 0
    }

    /// Every unattributable key the scan saw, sample and omitted together.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.sample.len() as u64 + self.omitted
    }

    /// Record one key, keeping it only while the sample has room.
    fn record(&mut self, key: Path) {
        if self.sample.len() < UNRECOGNIZED_SAMPLE_LIMIT {
            self.sample.push(key);
        } else {
            self.omitted += 1;
        }
    }
}

/// Everything the scan found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveInventory {
    /// The selected partitions, ordered by topic then partition index.
    pub partitions: Vec<PartitionInventory>,
    /// Keys the codec could not attribute to a segment.
    pub unrecognized: UnrecognizedKeys,
}

impl ArchiveInventory {
    /// Whether the scan found `topic`-`partition`.
    ///
    /// Compared by topic name and partition index, the pair a bound is
    /// written against, not by the archive's internal topic id.
    #[must_use]
    pub fn holds(&self, topic: &str, partition: i32) -> bool {
        holds_partition(&self.partitions, topic, partition)
    }
}

/// Whether `partitions` names `topic`-`partition`, by topic name and
/// partition index rather than by the archive's internal topic id.
///
/// Shared by [`ArchiveInventory::holds`] and by [`inventory`]'s own bound
/// check, which runs against the partitions built so far before they are
/// wrapped in an [`ArchiveInventory`].
fn holds_partition(partitions: &[PartitionInventory], topic: &str, partition: i32) -> bool {
    partitions
        .iter()
        .any(|entry| entry.partition.topic == topic && entry.partition.partition == partition)
}

/// One artifact of a segment, as [`inventory`] is still accumulating it.
///
/// Distinguishes the segment's `.log` data from its indexes so the artifact a
/// key names can be assigned to the right [`SegmentInventory`] field without
/// an intermediate `unrecognized` entry for a key whose suffix is simply not
/// yet known at the point the accumulator slot is chosen.
#[derive(Debug, Clone, Copy)]
enum Artifact {
    /// The segment's `.log` data.
    Log,
    /// One of the segment's indexes.
    Index(IndexType),
}

/// Segments accumulated so far for one partition, keyed by segment id so an
/// artifact found later for a segment already seen joins the same entry.
#[derive(Debug, Default)]
struct PartitionAccum {
    /// The topic name, as the partition directory names it.
    topic: String,
    /// Segments seen so far, keyed by segment id.
    segments: HashMap<Uuid, SegmentInventory>,
}

/// A [`SegmentInventory`] with every artifact absent, the accumulator's
/// starting point for a segment id seen for the first time.
fn empty_segment(segment_id: Uuid, base_offset: Offset) -> SegmentInventory {
    SegmentInventory {
        segment_id,
        base_offset,
        log: None,
        offset_index: None,
        time_index: None,
        producer_snapshot: None,
        leader_epoch: None,
        transaction_index: None,
    }
}

/// Fold one listed object into the accumulator, or record its key as
/// unattributable.
///
/// This is the whole per-object step of [`inventory`]. It is a function rather
/// than a loop body so the scan's state is exactly its arguments: everything
/// that survives one object is either partition-shaped or a bounded sample.
fn absorb(
    object: ObjectMeta,
    root: &Path,
    args: &RestoreArgs,
    accum: &mut HashMap<(Uuid, i32), PartitionAccum>,
    unrecognized: &mut UnrecognizedKeys,
) {
    // Collected to owned `String`s immediately, so the parsed pieces
    // below never borrow `object.location` and it stays free to move
    // into `unrecognized` or clone into an `ArchiveObject` right after.
    let Some(relative) = object.location.prefix_match(root) else {
        unrecognized.record(object.location);
        return;
    };
    let relative: Vec<String> = relative.map(|part| part.as_ref().to_owned()).collect();
    let [partition_dir, segment_file] = relative.as_slice() else {
        unrecognized.record(object.location);
        return;
    };

    let Some(dir) = parse_partition_dir_name(partition_dir) else {
        unrecognized.record(object.location);
        return;
    };
    let Some(file) = parse_segment_file_name(segment_file) else {
        unrecognized.record(object.location);
        return;
    };

    if !args.selects_topic(&dir.topic) {
        unrecognized.record(object.location);
        return;
    }

    let artifact = if file.suffix == LOG_FILE_SUFFIX {
        Artifact::Log
    } else if let Some(index_type) = IndexType::from_suffix(file.suffix) {
        Artifact::Index(index_type)
    } else {
        unrecognized.record(object.location);
        return;
    };

    let archive_object = ArchiveObject {
        key: object.location.clone(),
        size: object.size,
    };

    let partition_accum = accum
        .entry((dir.topic_id, dir.partition))
        .or_insert_with(|| PartitionAccum {
            topic: dir.topic.clone(),
            segments: HashMap::new(),
        });
    let segment_accum = partition_accum
        .segments
        .entry(file.segment_id)
        .or_insert_with(|| empty_segment(file.segment_id, Offset(file.base_offset)));

    match artifact {
        Artifact::Log => segment_accum.log = Some(archive_object),
        Artifact::Index(IndexType::Offset) => segment_accum.offset_index = Some(archive_object),
        Artifact::Index(IndexType::Timestamp) => {
            segment_accum.time_index = Some(archive_object);
        }
        Artifact::Index(IndexType::ProducerSnapshot) => {
            segment_accum.producer_snapshot = Some(archive_object);
        }
        Artifact::Index(IndexType::LeaderEpoch) => {
            segment_accum.leader_epoch = Some(archive_object);
        }
        Artifact::Index(IndexType::Transaction) => {
            segment_accum.transaction_index = Some(archive_object);
        }
    }
}

/// Scan `store` and build the inventory of the partitions `args` selects.
///
/// The listing is consumed as it arrives, so what the scan holds is the
/// archive's segments and a bounded sample of its unattributable keys, never
/// its objects.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the listing fails,
/// [`RestoreError::UnknownPartition`] when a `--to-offset` or
/// `--exclude-offset` bound names a topic partition the scan did not find,
/// [`RestoreError::EmptyArchive`] when no selected topic has a segment, and
/// [`RestoreError::MetadataDisagreement`] when `--rlmm-snapshot` contradicts
/// the scan.
pub async fn inventory(
    store: &ArchiveStore,
    args: &RestoreArgs,
) -> Result<ArchiveInventory, RestoreError> {
    let root = store.root().unwrap_or(Path::ROOT);

    let mut unrecognized = UnrecognizedKeys::default();
    let mut accum: HashMap<(Uuid, i32), PartitionAccum> = HashMap::new();

    let mut listing = store.ops().list_stream(store.root());
    while let Some(object) = listing.try_next().await? {
        absorb(object, &root, args, &mut accum, &mut unrecognized);
    }
    if unrecognized.omitted > 0 {
        tracing::warn!(
            total = unrecognized.total(),
            kept = unrecognized.sample.len(),
            "the archive holds keys this restore cannot attribute to a segment"
        );
    }

    let mut partitions: Vec<PartitionInventory> = accum
        .into_iter()
        .map(|((topic_id, partition), partition_accum)| {
            let mut segments: Vec<SegmentInventory> =
                partition_accum.segments.into_values().collect();
            segments.sort_by_key(|segment| segment.base_offset);
            PartitionInventory {
                partition: TopicIdPartition::new(topic_id, partition_accum.topic, partition),
                segments,
            }
        })
        .collect();
    PartitionInventory::sort_by_topic_and_partition(&mut partitions);

    // Checked against the raw scan before RLMM reconciliation. When a diskless
    // capture is present, `diskless::add_partitions` performs this check after
    // merging its partitions with this inventory.
    if args.archive.diskless_wal_capture.is_none() {
        for partition in args
            .to_offset
            .iter()
            .map(|bound| &bound.partition)
            .chain(args.exclude_offset.iter().map(|range| &range.partition))
        {
            if !holds_partition(&partitions, &partition.topic, partition.partition) {
                return Err(RestoreError::UnknownPartition {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                });
            }
        }
    }

    if !args.archive.worm_key_id.is_empty() && args.archive.rlmm_snapshot.is_some() {
        tracing::info!(
            "deferring RLMM reconciliation until the snapshot's signed digest is authenticated"
        );
    } else if let Some(snapshot_path) = &args.archive.rlmm_snapshot {
        reconcile_with_snapshot(&mut partitions, args, snapshot_path)?;
    }

    if partitions.is_empty() && args.archive.diskless_wal_capture.is_none() {
        return Err(RestoreError::EmptyArchive {
            prefix: store.prefix().unwrap_or("").to_owned(),
        });
    }

    Ok(ArchiveInventory {
        partitions,
        unrecognized,
    })
}
