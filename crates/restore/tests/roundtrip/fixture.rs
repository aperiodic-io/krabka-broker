//! The KIP-405 archive the round-trip tests restore from, and the batches a
//! correct restore must reproduce for every partition it holds.
//!
//! Each fixture partition is a real `krabka_log::Log` whose sealed segments
//! are copied through the real `LocalTieredStorage`, so building the archive
//! is the half of every test that has nothing to do with what the restore
//! then makes of it.

use assert2::assert;
use krabka_audit::signing::FileEd25519Signer;
use krabka_log::Log;
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::{
    ChainHead, ChainStamp, EpochId, LocalTieredStorage, MANIFEST_SUFFIX, ManifestSeq, ObjectEntry,
    RemoteLogSegmentMetadata, Sha256Digest, TopicIdPartition, WormArchiver, WormChainRecord,
    parse_segment_file_name, partition_dir_name, segment_file_name,
};
use tempfile::TempDir;
use uuid::Uuid;

use crate::batches::{append_groups, tiny_segment_config};

/// One archived segment: the id it was archived under, and the exact batch
/// (with its real, log-assigned `base_offset`) a correct restore must
/// reproduce verbatim.
pub(crate) struct SegmentFixture {
    pub(crate) segment_id: Uuid,
    pub(crate) batch: RecordBatch,
    /// The remote metadata this segment was archived under. It is what a
    /// broker's RLMM would hold for the segment, so a test that needs a
    /// consistent `--rlmm-snapshot` builds one out of these.
    #[allow(dead_code)] // only `dr_roundtrip` builds a snapshot out of these
    pub(crate) metadata: RemoteLogSegmentMetadata,
}

/// One archived partition: its identity, and every segment archived for it,
/// in base-offset order.
pub(crate) struct PartitionFixture {
    pub(crate) topic: &'static str,
    pub(crate) topic_id: Uuid,
    pub(crate) partition: i32,
    pub(crate) segments: Vec<SegmentFixture>,
}

impl PartitionFixture {
    /// The batches a correct restore must reproduce, in offset order.
    pub(crate) fn expected_batches(&self) -> Vec<RecordBatch> {
        self.segments.iter().map(|s| s.batch.clone()).collect()
    }

    /// The first offset the archive holds for this partition, and so the
    /// offset a restored broker must report for `EARLIEST_TIMESTAMP` and the
    /// offset a `Fetch` from the beginning must start at.
    ///
    /// It is zero for a partition whose whole history was archived, and the
    /// base offset of the oldest surviving segment for one whose earliest
    /// segments the old cluster had already aged out of the bucket.
    pub(crate) fn base_offset(&self) -> i64 {
        self.segments
            .first()
            .expect("every fixture partition archives at least one segment")
            .batch
            .base_offset
    }

    /// One past the last offset the archive holds, which is where a restored
    /// partition's log ends and where the next produced batch must land.
    pub(crate) fn end_offset(&self) -> i64 {
        let last = &self
            .segments
            .last()
            .expect("every fixture partition archives at least one segment")
            .batch;
        last.base_offset + i64::from(last.last_offset_delta) + 1
    }
}

/// What one partition's fixture is built from: its identity, and its record
/// values grouped into batches so that every group but the last seals into
/// its own archived segment (see [`build_partition`]). The last group keeps
/// the log's active segment non-empty but is never archived: a real
/// tiered-storage copy never tiers the still-open active segment either.
#[derive(Clone, Copy)]
struct PartitionSpec<'a> {
    topic: &'static str,
    topic_id: Uuid,
    partition: i32,
    groups: &'a [&'a [&'a str]],
    /// How many of the leading sealed segments the archive does NOT hold.
    ///
    /// Zero archives every sealed segment, so the partition's archived
    /// history starts at offset 0. A higher number leaves that many oldest
    /// segments out of the bucket, which is what a partition looks like once
    /// the old cluster's remote retention has aged its earliest segments out:
    /// the first surviving segment has a non-zero base offset, and a restore
    /// of it must produce a log whose start offset is that base rather than
    /// zero.
    unarchived_prefix: usize,
}

/// Build a real, multi-batch `krabka_log::Log` for one partition, roll it so
/// every group but the last seals into its own segment, and archive each
/// sealed segment through the real `LocalTieredStorage::copy_log_segment_data`
/// -- the same call the broker's remote-log-manager makes.
fn build_partition(storage: &LocalTieredStorage, spec: PartitionSpec<'_>) -> PartitionFixture {
    let local = tempfile::tempdir().expect("local log tempdir");
    let mut log = Log::open(local.path(), tiny_segment_config()).expect("open local log");

    let (appended, exports) = append_groups(&mut log, spec.groups);
    assert!(
        exports.len() == spec.groups.len() - 1,
        "{}-{}: every append after the first should roll exactly one segment",
        spec.topic,
        spec.partition,
    );
    assert!(
        spec.unarchived_prefix < exports.len(),
        "{}-{}: the archive must keep at least one segment",
        spec.topic,
        spec.partition,
    );

    let segments = exports
        .iter()
        .zip(appended.iter())
        .skip(spec.unarchived_prefix)
        .map(|(export, batch)| {
            let segment_id = Uuid::new_v4();
            let partition_id = TopicIdPartition::new(spec.topic_id, spec.topic, spec.partition);
            let metadata =
                crate::archive_fixture::archive_segment(storage, partition_id, segment_id, export);

            SegmentFixture {
                segment_id,
                batch: batch.clone(),
                metadata,
            }
        })
        .collect();

    PartitionFixture {
        topic: spec.topic,
        topic_id: spec.topic_id,
        partition: spec.partition,
        segments,
    }
}

/// The whole archive this file tests against: two topics ("orders" with two
/// partitions, "payments" with two), one partition ("orders-0") with two
/// archived segments, and one ("payments-1") whose oldest archived segment
/// begins above offset 0.
pub(crate) struct Fixture {
    pub(crate) archive_root: TempDir,
    pub(crate) orders_id: Uuid,
    pub(crate) payments_id: Uuid,
    orders_0: PartitionFixture,
    orders_1: PartitionFixture,
    payments_0: PartitionFixture,
    payments_1: PartitionFixture,
}

impl Fixture {
    /// Every partition, in the (topic, partition) order discovery sorts to.
    pub(crate) fn partitions(&self) -> [&PartitionFixture; 4] {
        [
            &self.orders_0,
            &self.orders_1,
            &self.payments_0,
            &self.payments_1,
        ]
    }

    /// Distinct topics the archive holds.
    pub(crate) fn topic_count() -> usize {
        2
    }

    /// The archived topic id for `topic`, which a restore must carry into the
    /// target unchanged and which a `Fetch` or `Produce` addresses the topic
    /// by.
    pub(crate) fn topic_id(&self, topic: &str) -> Uuid {
        match topic {
            "orders" => self.orders_id,
            "payments" => self.payments_id,
            other => panic!("this fixture holds no topic {other:?}"),
        }
    }

    /// One partition by name, for a test that drives a single one.
    pub(crate) fn partition(&self, topic: &str, partition: i32) -> &PartitionFixture {
        self.partitions()
            .into_iter()
            .find(|entry| entry.topic == topic && entry.partition == partition)
            .unwrap_or_else(|| panic!("this fixture holds no {topic}-{partition}"))
    }

    /// Seal every archived segment into a signed WORM chain. Signers rotate by
    /// segment position, so two signers exercise trusted-key rotation.
    pub(crate) fn sign_worm(
        &self,
        signers: &[std::sync::Arc<FileEd25519Signer>],
    ) -> Vec<(String, String)> {
        assert!(!signers.is_empty());
        let mut heads = Vec::new();
        for partition in self.partitions() {
            let mut stamp = ChainStamp {
                epoch_id: EpochId(partition.topic_id),
                seq: ManifestSeq(0),
                prev_head: ChainHead::GENESIS,
            };
            for (index, segment) in partition.segments.iter().enumerate() {
                let metadata = segment
                    .metadata
                    .clone()
                    .with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata());
                let partition_dir = partition_dir_name(&metadata);
                let objects = std::fs::read_dir(self.archive_root.path().join(&partition_dir))
                    .expect("read archive partition")
                    .filter_map(Result::ok)
                    .filter_map(|entry| {
                        let name = entry.file_name().into_string().ok()?;
                        let parsed = parse_segment_file_name(&name)?;
                        (parsed.base_offset == metadata.start_offset()
                            && parsed.segment_id == metadata.remote_log_segment_id().id
                            && parsed.suffix != MANIFEST_SUFFIX)
                            .then(|| {
                                let bytes = std::fs::read(entry.path()).expect("read artifact");
                                ObjectEntry {
                                    suffix: parsed.suffix.to_owned(),
                                    key: format!("{partition_dir}/{name}"),
                                    size_bytes: u64::try_from(bytes.len()).unwrap(),
                                    sha256: Sha256Digest::of(&bytes),
                                    e_tag: None,
                                    version_id: None,
                                    create_precondition: true,
                                }
                            })
                    })
                    .collect();
                let sealed = WormArchiver::new(Some(signers[index % signers.len()].clone()))
                    .seal(&metadata, objects)
                    .expect("seal WORM manifest");
                std::fs::write(
                    self.archive_root
                        .path()
                        .join(&partition_dir)
                        .join(segment_file_name(&metadata, MANIFEST_SUFFIX)),
                    &sealed.bytes,
                )
                .expect("write WORM manifest");
                stamp = sealed.receipt.next_stamp().expect("next chain stamp");
            }
            heads.push((
                partition_dir_name(&partition.segments[0].metadata),
                stamp.prev_head.to_string(),
            ));
        }
        heads
    }
}

pub(crate) fn build_fixture() -> Fixture {
    let archive_root = tempfile::tempdir().expect("archive root");
    let storage = LocalTieredStorage::new(archive_root.path());
    let orders_id = Uuid::new_v4();
    let payments_id = Uuid::new_v4();

    let orders_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 0,
            groups: &[&["o0-0", "o0-1"], &["o0-2"], &["o0-3", "o0-4"]],
            unarchived_prefix: 0,
        },
    );
    let orders_1 = build_partition(
        &storage,
        PartitionSpec {
            topic: "orders",
            topic_id: orders_id,
            partition: 1,
            groups: &[&["o1-0", "o1-1"], &["o1-2"]],
            unarchived_prefix: 0,
        },
    );
    let payments_0 = build_partition(
        &storage,
        PartitionSpec {
            topic: "payments",
            topic_id: payments_id,
            partition: 0,
            groups: &[&["p0-0", "p0-1"], &["p0-2"]],
            unarchived_prefix: 0,
        },
    );
    // The oldest two records never reach the bucket, so this partition's
    // archived history starts at offset 2. A restore of it is the only case
    // in the fixture where "the log starts at offset 0" and "the log starts
    // where the archive does" are different answers.
    let payments_1 = build_partition(
        &storage,
        PartitionSpec {
            topic: "payments",
            topic_id: payments_id,
            partition: 1,
            groups: &[&["p1-0", "p1-1"], &["p1-2", "p1-3"], &["p1-4"], &["p1-5"]],
            unarchived_prefix: 1,
        },
    );

    Fixture {
        archive_root,
        orders_id,
        payments_id,
        orders_0,
        orders_1,
        payments_0,
        payments_1,
    }
}
