//! The archive scan against a bucket far larger than the machine that runs
//! the restore.
//!
//! A tiered cluster archives six objects per segment per partition, so a year
//! of a busy cluster is tens of millions of keys, and `krabka restore` is run
//! precisely when that cluster is gone and the operator has whatever laptop is
//! to hand. The scan therefore consumes the listing as it arrives and keeps
//! only per-segment state plus a bounded sample of the keys it cannot
//! attribute. This suite drives it against a synthetic store that yields a
//! million objects without ever holding them, which no on-disk fixture could
//! do, and reads the process's own peak resident set to show the scan did not
//! grow with the listing.

use {
    assert2::check,
    futures_util::stream::StreamExt as _,
    krabka_ids::Offset,
    krabka_remote_storage::{TopicIdPartition, kafka_uuid},
    krabka_restore::{
        ArchiveInventory, ArchiveObject, ArchiveStore, Cli, PartitionInventory, RestoreArgs,
        SegmentInventory, UNRECOGNIZED_SAMPLE_LIMIT, UnrecognizedKeys, inventory,
    },
    object_store::{ObjectMeta, path::Path},
    uuid::Uuid,
};

/// Partitions the synthetic archive holds, all of topic `orders`.
const PARTITIONS: i32 = 4;

/// Segments per partition. Six artifacts each, so the recognized part of the
/// listing is `PARTITIONS * SEGMENTS * 6` objects and the state the scan keeps
/// is that many `ArchiveObject`s -- the state a restore genuinely needs.
const SEGMENTS: i64 = 100;

/// Keys under a directory the archive key codec cannot decode. This is the
/// part that used to be kept in full: a `--archive-prefix` typo makes every
/// key in the bucket land here.
const JUNK: u64 = 1_000_000;

/// The suffixes one complete segment copy is archived with, in the order the
/// key codec sorts them.
const SUFFIXES: [&str; 6] = [
    ".index",
    ".leader_epoch_checkpoint",
    ".log",
    ".snapshot",
    ".timeindex",
    ".txnindex",
];

/// Every artifact carries this size, so an expected [`ArchiveObject`] can name
/// it without the store holding any bytes.
const OBJECT_SIZE: u64 = 4;

/// The topic id every synthetic partition directory encodes.
fn topic_id() -> Uuid { Uuid::from_u128(0x5eed) }

/// The segment id of segment `index` of `partition`, distinct per segment so
/// the scan has to key its accumulator by more than the partition.
fn segment_id(partition: i32, index: i64) -> Uuid {
    let partition = u128::from(partition.unsigned_abs());
    let index = u128::try_from(index).expect("segment indexes are non-negative");
    Uuid::from_u128((partition << 64) | (index + 1))
}

/// The base offset of segment `index`, spaced so the offsets are not the
/// segment indexes.
fn base_offset(index: i64) -> i64 { index * 1_000 }

/// The archive key of one artifact, at the exact KIP-405 layout.
fn artifact_key(partition: i32, index: i64, suffix: &str) -> Path {
    Path::from(format!(
        "orders-{partition}-{}/{:020}-{}{suffix}",
        kafka_uuid(topic_id()),
        base_offset(index),
        kafka_uuid(segment_id(partition, index)),
    ))
}

/// The key of junk object `index`, zero-padded so the synthetic listing is in
/// lexicographic order the way a real store's listing is.
fn junk_key(index: u64) -> Path { Path::from(format!("junk/{index:09}")) }

/// Every key the synthetic store lists, in order: the junk keys first (`j`
/// sorts before `o`), then one complete copy of every segment.
fn keys() -> impl Iterator<Item = Path> {
    let junk = (0..JUNK).map(junk_key);
    let segments = (0..PARTITIONS).flat_map(|partition| {
        (0..SEGMENTS).flat_map(move |index| {
            SUFFIXES
                .iter()
                .map(move |suffix| artifact_key(partition, index, suffix))
        })
    });
    junk.chain(segments)
}

/// [`UNRECOGNIZED_SAMPLE_LIMIT`] as the count the sample and the omitted
/// tally are expressed in.
fn sample_limit() -> u64 {
    u64::try_from(UNRECOGNIZED_SAMPLE_LIMIT).expect("the sample limit is small")
}

/// The inventory a correct scan of [`keys`] produces.
fn expected_inventory() -> ArchiveInventory {
    let partitions = (0..PARTITIONS)
        .map(|partition| PartitionInventory {
            partition: TopicIdPartition::new(topic_id(), "orders", partition),
            segments: (0..SEGMENTS)
                .map(|index| {
                    let object = |suffix: &str| {
                        Some(ArchiveObject {
                            key: artifact_key(partition, index, suffix),
                            size: OBJECT_SIZE,
                        })
                    };
                    SegmentInventory {
                        segment_id: segment_id(partition, index),
                        base_offset: Offset(base_offset(index)),
                        log: object(".log"),
                        offset_index: object(".index"),
                        time_index: object(".timeindex"),
                        producer_snapshot: object(".snapshot"),
                        leader_epoch: object(".leader_epoch_checkpoint"),
                        transaction_index: object(".txnindex"),
                    }
                })
                .collect(),
        })
        .collect();
    ArchiveInventory {
        partitions,
        unrecognized: UnrecognizedKeys {
            sample: (0..sample_limit()).map(junk_key).collect(),
            omitted: JUNK - sample_limit(),
        },
    }
}

/// A store whose listing is generated as it is polled.
///
/// Nothing here is stored: the objects exist only for as long as the scan
/// holds each one, which is what makes the listing a million objects long
/// without the test itself being the thing that allocates.
#[derive(Debug)]
struct GeneratedListing;

impl std::fmt::Display for GeneratedListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeneratedListing")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for GeneratedListing {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: object_store::PutPayload,
        _opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        unimplemented!("the scan under test only lists")
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        unimplemented!("the scan under test only lists")
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        unimplemented!("the scan under test only lists")
    }

    fn delete_stream(
        &self,
        _locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        unimplemented!("the scan under test only lists")
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        futures_util::stream::iter(keys())
            .filter_map(move |location| {
                let matched = prefix
                    .as_ref()
                    .is_none_or(|prefix| location.prefix_match(prefix).is_some());
                async move {
                    matched.then(|| {
                        Ok(ObjectMeta {
                            location,
                            // The epoch, reached through `SystemTime` so the
                            // test names no chrono type: nothing in the scan
                            // reads a modification time.
                            last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
                            size: OBJECT_SIZE,
                            e_tag: None,
                            version: None,
                        })
                    })
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        unimplemented!("the scan under test only lists")
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        unimplemented!("the scan under test only lists")
    }
}

/// The flags the scan reads. `--archive-local` names a path that is never
/// opened: the store under test is passed to [`ArchiveStore::with_store`]
/// directly, and the parser requires one archive source to be named.
fn scan_args() -> RestoreArgs {
    use clap::Parser as _;

    Cli::try_parse_from([
        "krabka-restore",
        "--archive-local",
        "/nonexistent",
        "--log-dir",
        "/nonexistent/target",
    ])
    .expect("valid command line")
    .args
}

/// The process's peak resident set in KiB, on a kernel that reports one.
///
/// `VmHWM` is a high-water mark, so it answers "how big did this process ever
/// get", which is exactly the question a scan that used to collect the whole
/// listing gets wrong. It is Linux-only; elsewhere the correctness half of the
/// test still runs.
fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
}

/// The scan may grow by this much across a million-object listing. Holding
/// that listing costs well over a hundred megabytes; streaming it measures at
/// about one and a half, so this bound sits an order of magnitude above what
/// the scan needs and an order of magnitude below what collecting would take.
const PEAK_GROWTH_LIMIT_KIB: u64 = 16 * 1024;

#[tokio::test]
async fn a_million_object_listing_is_scanned_without_being_held() {
    let store = ArchiveStore::with_store(std::sync::Arc::new(GeneratedListing), None);
    let args = scan_args();

    let before = peak_rss_kib();
    let scanned = inventory(&store, &args).await.expect("scan the archive");
    let after = peak_rss_kib();

    check!(scanned == expected_inventory());

    if let (Some(before), Some(after)) = (before, after) {
        let growth = after.saturating_sub(before);
        check!(
            growth < PEAK_GROWTH_LIMIT_KIB,
            "peak resident set grew by {growth} KiB scanning {JUNK} unattributable keys",
        );
    }
}
