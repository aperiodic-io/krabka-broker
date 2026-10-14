//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use {
    krabka_compression::CompressionType,
    krabka_protocol::records::TimestampType,
    krabka_units::prelude::{
        ByteSize, Ratio, Time, TimeExt as _, bytes, days, fraction, gibibytes, hours, kibibytes,
        mebibytes, millis,
    },
};

/// Kafka's `segment.bytes` default: roll the active segment at 1 GiB.
const DEFAULT_SEGMENT_SIZE: ByteSize = gibibytes(1);

/// Kafka's `segment.ms` default: roll the active segment once its first
/// record is a week old.
const DEFAULT_SEGMENT_ROLL_INTERVAL: Time = days(7);

/// Kafka's `retention.ms` default: delete sealed segments a week after their
/// newest record.
const DEFAULT_RETENTION: Time = days(7);

/// Kafka's `index.interval.bytes` default: one sparse `.index`/`.timeindex`
/// entry per 4 KiB of `.log`.
pub(crate) const DEFAULT_INDEX_INTERVAL: ByteSize = kibibytes(4);

/// Kafka's `segment.index.bytes` default: each sparse index of a segment
/// holds up to 10 MiB of entries before the segment rolls.
const DEFAULT_SEGMENT_INDEX_SIZE: ByteSize = mebibytes(10);

/// Kafka's `max.message.bytes` default: 1 MiB of records plus the 12-byte
/// `Records.LOG_OVERHEAD` that prefixes every batch on the wire. Kafka's
/// broker-wide `message.max.bytes` carries the same number, and a topic that
/// sets neither inherits it.
pub const DEFAULT_MAX_MESSAGE_SIZE: ByteSize = bytes(1_048_588);

/// Kafka's `delete.retention.ms` default: a tombstone or transaction marker
/// stays readable for a day after it first becomes compaction-eligible.
const DEFAULT_DELETE_RETENTION: Time = hours(24);

/// Default clock-confidence bound for scheduled delivery: the broker treats
/// its own clock as accurate to within a quarter of a second.
const DEFAULT_DELIVERY_CLOCK_UNCERTAINTY: Time = millis(250);

/// Default upper bound on a single read's initial allocation.
pub const DEFAULT_READ_BUFFER_CAP: ByteSize = mebibytes(4);

/// Default cap on how far past its own window a verbatim read asks the
/// kernel to read ahead.
pub const DEFAULT_READ_AHEAD_MAX: ByteSize = mebibytes(4);

/// Default for [`LogConfig::tail_cache_size`].
///
/// A consumer that falls more than this far behind the producer reads from
/// the disk, because `O_DIRECT` writes leave nothing in the page cache, and
/// the disk reads slow it down further. At 1 MiB, one default
/// `max.partition.fetch.bytes`, a consumer of a single partition taking about
/// 600 MB/s fell seconds behind (krabka-broker#1370). 32 MiB, about 50 ms of
/// writes at that rate, kept its end-to-end p99 under 100 ms.
pub const DEFAULT_TAIL_CACHE_SIZE: ByteSize = mebibytes(32);

/// Default byte window for timestamp scans between sparse index entries.
pub const DEFAULT_TIMESTAMP_SCAN_WINDOW: ByteSize = kibibytes(64);

/// Kafka's `min.cleanable.dirty.ratio` default: half the log has to be
/// uncleaned before a compaction pass is worth its I/O.
const DEFAULT_MIN_CLEANABLE_DIRTY_RATIO: Ratio = fraction(0.5);

/// Per-topic policy for what to do with old log segments.
///
/// Kafka's `cleanup.policy` is a list, and `LogConfig` derives two independent
/// booleans from it: `compact` when the list contains `compact` and `delete`
/// when it contains `delete`. The four sets an operator can write are the
/// four variants here.
///
/// `Delete` is the default. It deletes segments by age or by size in
/// `crate::retention`. `Compact` does newest-wins dedup by key.
/// `crate::compact` implements it, and [`crate::Log::compact`] invokes it.
/// `CompactAndDelete` is Kafka's `compact,delete`: the log cleaner runs over
/// it *and* retention deletes its old segments. Kafka Streams writes exactly
/// that value on every windowed-store changelog topic, so a broker that
/// refuses it cannot host a Streams application with a windowed store.
/// `NoCleanup` is the empty list, which Kafka accepts and documents as
/// infinite retention: neither the cleaner nor time and size retention runs.
///
/// Ask [`Self::contains_compact`] and [`Self::contains_delete`] rather than
/// comparing variants: `Compact` and `CompactAndDelete` both run the cleaner.
///
/// `as_str` is the `cleanup.policy` value Kafka reports for the policy, which
/// is what `DescribeConfigs` echoes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, krabka_macros::EnumStr)]
#[enum_str(case = "lowercase")]
pub enum CleanupPolicy {
    #[default]
    Delete,
    Compact,
    #[enum_str(name = "compact,delete")]
    CompactAndDelete,
    #[enum_str(name = "")]
    NoCleanup,
}

impl CleanupPolicy {
    /// `true` when the policy list contains `compact`, which is what makes a
    /// partition the log cleaner's work.
    #[must_use]
    pub const fn contains_compact(self) -> bool {
        matches!(self, Self::Compact | Self::CompactAndDelete)
    }

    /// `true` when the policy list contains `delete`, which is what makes a
    /// partition's old segments eligible for time-, size- and
    /// start-offset-based retention.
    #[must_use]
    pub const fn contains_delete(self) -> bool {
        matches!(self, Self::Delete | Self::CompactAndDelete)
    }
}

/// How a new segment's `.log` file gets its disk blocks: Kafka's
/// `preallocate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SegmentAllocation {
    /// `preallocate=false`, Kafka's default: the file allocates blocks as
    /// appends grow it.
    #[default]
    OnWrite,
    /// `preallocate=true`: reserve `segment_size` of disk blocks for a
    /// segment before the first append into it, so appends do not allocate
    /// as they grow it, and write its batches through `O_DIRECT`. A truncate
    /// gives the reservation up, and the segment takes it again.
    ///
    /// The `O_DIRECT` writes bypass the page cache. An append returns once
    /// its write is queued, and shared write threads put the queued writes on
    /// the disk, several at a time, so a produce request is acknowledged
    /// before its bytes reach the disk, as it is through the page cache. The
    /// newest [`LogConfig::tail_cache_size`] of the segment, and every byte
    /// that is not on the disk yet, stays in memory to serve reads. A write
    /// that fails fails every later append and sync of the segment. A direct
    /// write covers whole blocks, so the active segment's file runs up to a
    /// block past its last batch until it is sealed. Where
    /// the kernel does not report the alignment `O_DIRECT` needs (before Linux
    /// 6.1, or on tmpfs), the segment writes through the page cache.
    ///
    /// Kafka sets the file's length to `segment.bytes` and trims it back when
    /// the segment closes. krabka reserves the blocks without changing the
    /// length (`fallocate(FALLOC_FL_KEEP_SIZE)`), so a segment's file is
    /// never longer than the batches in it, and gives back what is left of
    /// the reservation when the segment is sealed. The bytes on disk are the
    /// same either way. Off Linux, or on a filesystem that cannot reserve
    /// blocks, a segment allocates as it is written.
    Preallocate,
}

/// Per-topic policy for when a durable record becomes visible to consumers.
///
/// `Immediate` is the default and is every ordinary topic. `Scheduled` gates
/// visibility on each batch's activation time, so a producer can write a
/// record now and have it delivered later. `crate::delivery` implements it,
/// and [`Log::advance_delivery_watermark`](crate::Log::advance_delivery_watermark)
/// computes the offset that separates the visible prefix from the scheduled
/// tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryPolicy {
    /// A batch is visible as soon as it is durable.
    #[default]
    Immediate,
    /// A batch is visible once its activation time has passed. The activation
    /// time is the batch's `max_timestamp`, the v2 header field, so the
    /// schedule travels with the records and needs no sidecar.
    Scheduled,
}

/// Whether a scheduled partition's delivery times must not run backwards.
///
/// KFC-1's `delivery.schedule.monotonic`. It is an enum rather than a `bool`
/// because it is meaningless outside [`DeliveryPolicy::Scheduled`], and the
/// two names say at the call site which rule is in force.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleOrder {
    /// Accept any delivery time, so a later batch may come due before an
    /// earlier one. The default, and the only behaviour Kafka has.
    Unordered,
    /// Refuse, with `INVALID_TIMESTAMP`, a batch whose delivery time precedes
    /// the largest delivery time the partition already holds.
    Monotonic,
}

/// KIP-950's two tiered-storage disablement controls.
///
/// They are separate from [`LogConfig::remote_storage_enable`] because they
/// describe how the tier is *left*, not whether it is on: `copy_disable`
/// freezes the remote copy while reads keep being served from it, and
/// `delete_on_disable` is the operator's standing consent to erase the remote
/// copies when tiering is turned off. Kafka refuses the
/// `remote.storage.enable` `true -> false` flip without the second one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RemoteTierFlags {
    /// `remote.log.copy.disable`: the remote log manager copies nothing new
    /// for this partition, and retention over what is already there keeps
    /// running.
    pub copy_disable: bool,
    /// `remote.log.delete.on.disable`: turning `remote.storage.enable` off
    /// erases the partition's remote segments and raises its log start offset
    /// to the local log start.
    pub delete_on_disable: bool,
}

impl RemoteTierFlags {
    /// Both off, which is Kafka's default: a tiered topic copies, and
    /// disabling tiering is refused until an operator opts into the delete.
    pub const DEFAULT: Self = Self {
        copy_disable: false,
        delete_on_disable: false,
    };
}

/// Tunables for [`Log`](crate::Log) behavior.
///
/// Defaults match Apache Kafka 4.2 for `segment.bytes`, `segment.ms`,
/// `retention.ms`, `index.interval.bytes`, and the other tunables. Start from
/// the [`Default`](Self::default) impl. Most production deployments override
/// only [`Self::retention`] and [`Self::retention_size`].
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub struct LogConfig {
    /// Cap the initial allocation used by decoded and raw segment reads.
    #[default(DEFAULT_READ_BUFFER_CAP)]
    pub read_buffer_cap: ByteSize,

    /// How far past a verbatim read's own window the read asks the kernel to
    /// read ahead, as a `POSIX_FADV_WILLNEED` hint, so a consumer that is
    /// behind finds its next fetch in the page cache. The hint covers the
    /// window and then up to one more window, capped at this. `0` hints only
    /// the window itself. The hint is a no-op off Linux.
    #[default(DEFAULT_READ_AHEAD_MAX)]
    pub read_ahead_max: ByteSize,

    /// Read timestamp searches in windows of this size.
    #[default(DEFAULT_TIMESTAMP_SCAN_WINDOW)]
    pub timestamp_scan_window: ByteSize,

    /// Roll the active segment once it grows past this. Kafka's
    /// `segment.bytes`; default 1 GiB.
    #[default(DEFAULT_SEGMENT_SIZE)]
    pub segment_size: ByteSize,

    /// Roll the active segment when its first record is older than this.
    /// Kafka's `segment.ms`; default 7 days.
    #[default(DEFAULT_SEGMENT_ROLL_INTERVAL)]
    pub segment_roll_interval: Time,

    /// Maximum random subtraction from the segment roll interval, sampled per segment.
    pub segment_jitter: Time,

    /// Delay before retired segment files are unlinked. Readers stop seeing them immediately.
    #[default(krabka_units::secs(60))]
    pub file_delete_delay: Time,

    /// Offset span between forced syncs. `None` disables the threshold.
    pub flush_messages: Option<u64>,

    /// Time between forced syncs of dirty data. `None` disables the timer.
    pub flush_interval: Option<Time>,

    /// Gzip level used when encoding owned batches; -1 selects the codec default.
    #[default(-1)]
    pub compression_gzip_level: i32,

    /// Lz4 level used when encoding owned batches.
    #[default(9)]
    pub compression_lz4_level: i32,

    /// Zstd level used when encoding owned batches.
    #[default(3)]
    pub compression_zstd_level: i32,

    /// Delete sealed segments older than this. `None` = unlimited. Kafka's
    /// `retention.ms`; default 7 days.
    #[default(Some(DEFAULT_RETENTION))]
    pub retention: Option<Time>,

    /// Delete oldest sealed segments until the total `.log` size fits.
    /// `None` = unlimited. Kafka's `retention.bytes`.
    pub retention_size: Option<ByteSize>,

    /// Largest single record batch this partition accepts, measured over the
    /// batch's whole wire encoding including its 61-byte v2 header. Kafka's
    /// `max.message.bytes`; default 1048588. A `Produce` carrying a larger
    /// batch is refused with `MESSAGE_TOO_LARGE` (10) rather than truncated,
    /// because a batch that lands in the log is a batch every consumer has to
    /// fetch whole.
    #[default(DEFAULT_MAX_MESSAGE_SIZE)]
    pub max_message_size: ByteSize,

    /// Kafka trunk's `max.decompressed.message.bytes`: the largest record body a
    /// compressed batch may hold when the log decompresses it, which it does to
    /// compact a partition and to answer a by-timestamp `ListOffsets`. A record
    /// above it fails the lookup or the pass with
    /// [`LogError::RecordTooLarge`](crate::LogError::RecordTooLarge),
    /// as `DefaultRecord.readFrom` throws `InvalidRecordException`. An
    /// uncompressed batch is never held to it. `None` is no limit, which is
    /// Kafka's default (`Records.SOFT_MAX_ARRAY_LENGTH`) and the only setting
    /// Kafka 4.3.1 has: the broker sets it only under
    /// `unstable.api.versions.enable`.
    pub max_decompressed_record: Option<ByteSize>,

    /// Write one `.index`/`.timeindex` entry per this much `.log`. Kafka's
    /// `index.interval.bytes`; default 4 KiB.
    #[default(DEFAULT_INDEX_INTERVAL)]
    pub index_interval: ByteSize,

    /// Roll the active segment once its `.index` or its `.timeindex` holds
    /// this many bytes of entries. Kafka's `segment.index.bytes`; default
    /// 10 MiB. Kafka rounds the size down to whole entries, so the offset
    /// index holds `size / 8` entries and the time index `size / 12`.
    #[default(DEFAULT_SEGMENT_INDEX_SIZE)]
    pub segment_index_size: ByteSize,

    /// fsync after every `append`. Default off. The broker manages fsync
    /// separately.
    pub flush_on_append: bool,

    /// Kafka's `preallocate`. Defaults to [`SegmentAllocation::OnWrite`],
    /// Kafka's `false`. See [`SegmentAllocation`].
    pub segment_allocation: SegmentAllocation,

    /// How much of the newest data an active segment that writes through
    /// `O_DIRECT` keeps in memory to serve reads with. Those writes bypass
    /// the page cache and drop what it held of the range, so without this a
    /// consumer reading right behind the producer reads from disk. Every
    /// partition under [`SegmentAllocation::Preallocate`] holds up to this
    /// much, plus one batch; `0` keeps only the newest batch. Default 32 MiB:
    /// see [`DEFAULT_TAIL_CACHE_SIZE`].
    #[default(DEFAULT_TAIL_CACHE_SIZE)]
    pub tail_cache_size: ByteSize,

    /// On open, CRC every batch in the active segment and rebuild its sparse indexes.
    #[default(true)]
    pub validate_on_open: bool,

    /// Cleanup policy. Defaults to `Delete`. See [`CleanupPolicy`].
    #[default(CleanupPolicy::Delete)]
    pub cleanup_policy: CleanupPolicy,

    /// Kafka's `min.compaction.lag.ms`: a record stays uncompacted for at
    /// least this long after it is written. The broker's cleaner reads it and
    /// leaves a partition whose newest dirty record is younger than this
    /// alone. Default 0, which is Kafka's.
    #[default(Time::ZERO)]
    pub min_compaction_lag: Time,

    /// Kafka's `max.compaction.lag.ms`: however clean a partition looks, once
    /// its oldest dirty record is older than this the cleaner runs anyway.
    /// `None` is no bound, which is Kafka's default (`Long.MAX_VALUE`).
    pub max_compaction_lag: Option<Time>,

    /// Kafka's `min.cleanable.dirty.ratio`: the share of a compacted
    /// partition's log that must be uncleaned before the cleaner spends a pass
    /// on it. Default 0.5, which is Kafka's.
    #[default(DEFAULT_MIN_CLEANABLE_DIRTY_RATIO)]
    pub min_cleanable_dirty_ratio: Ratio,

    /// Kafka's `message.timestamp.type`: whose clock the stored records carry.
    /// `CreateTime` is the producer's own timestamp and is the default;
    /// `LogAppendTime` is the broker's clock at append time.
    #[default(TimestampType::CreateTime)]
    pub message_timestamp_type: TimestampType,

    /// Broker-side recompression target. `None` is Kafka's
    /// `compression.type=producer`, which is pass-through: the broker stores
    /// the batch exactly as the producer sent it. `Some(c)` re-encodes every
    /// batch the broker accepts on this partition to `c` before the write.
    /// This matches Kafka's per-topic `compression.type` config. `gzip`,
    /// `snappy`, `lz4`, `zstd`, and `uncompressed` map to `Some(_)`.
    /// `producer`, the default, maps to `None`.
    pub compression_type: Option<CompressionType>,

    /// When `true`, the broker's `RemoteLogManager` may copy this
    /// partition's sealed segments (KIP-405) to the remote tier. This maps to
    /// Kafka's per-topic `remote.storage.enable`. Default `false`, which is
    /// also Kafka's default, because tiered storage is opt-in per topic.
    pub remote_storage_enable: bool,

    /// KIP-950's two disablement controls, `remote.log.copy.disable` and
    /// `remote.log.delete.on.disable`. See [`RemoteTierFlags`].
    // Both KIP-950 controls default off, as in Kafka: a tiered topic
    // copies, and turning tiering off needs the operator's consent.
    #[default(RemoteTierFlags::DEFAULT)]
    pub remote_tier: RemoteTierFlags,

    /// Local-disk time-retention window for tiered partitions (KIP-405).
    /// `None` inherits [`Self::retention`]. Default `None`.
    pub local_retention: Option<Time>,

    /// Local-disk size budget for tiered partitions (KIP-405).
    /// `None` inherits [`Self::retention_size`]. Default `None`.
    pub local_retention_size: Option<ByteSize>,

    /// KIP-534. After a tombstone or transaction marker first becomes
    /// compaction-eligible, the log retains it for at least this long before
    /// deletion. This is the delete-horizon grace window. Default 24h.
    #[default(DEFAULT_DELETE_RETENTION)]
    pub delete_retention: Time,

    /// When a durable record becomes visible. Defaults to
    /// [`DeliveryPolicy::Immediate`]. See [`DeliveryPolicy`].
    // Scheduled delivery is opt-in per topic; an ordinary topic pays
    // nothing for it.
    #[default(DeliveryPolicy::Immediate)]
    pub delivery_policy: DeliveryPolicy,

    /// KFC-1 `delivery.schedule.monotonic`, read only under
    /// [`DeliveryPolicy::Scheduled`]. See [`ScheduleOrder`].
    ///
    /// The check belongs to the log because only the log serializes appends.
    /// A partition's schedule is a property of what it already holds, so a
    /// test that reads it and an append that extends it have to be the same
    /// critical section: two producers that each pass a check taken before the
    /// append can still land out of order, and so can two jobs the writer
    /// batches into one group. Kafka runs its own record-shape rejections in
    /// the same place, under `UnifiedLog.append`'s lock.
    // Kafka has no such setting, and a scheduled topic accepts a
    // schedule that runs backwards unless an operator asks otherwise.
    #[default(ScheduleOrder::Unordered)]
    pub schedule_order: ScheduleOrder,

    /// Declared bound on how far this broker's clock can be from true time.
    /// Default 250 ms. It has an effect only under
    /// [`DeliveryPolicy::Scheduled`].
    ///
    /// A batch is visible once
    /// `max_timestamp + delivery_clock_uncertainty <= now`. If the clock
    /// reads `c` while true time is somewhere in `[c - e, c + e]`, then
    /// `c >= activation + e` proves true time has reached the activation
    /// instant. Provided the declared bound holds, this test does not deliver
    /// early. The clock test's additional delay is at most twice the bound;
    /// polling, unavailable storage and an earlier pending batch can delay
    /// delivery further.
    ///
    /// Changes are prospective for the published delivery prefix: increasing
    /// this bound or moving the clock backward does not retract offsets the
    /// in-memory watermark already released. The new bound governs subsequent
    /// advances and pending batches. A restart rebuilds that watermark from
    /// retained records using the current bound; the watermark is not durable.
    #[default(DEFAULT_DELIVERY_CLOCK_UNCERTAINTY)]
    pub delivery_clock_uncertainty: Time,
}

impl LogConfig {
    /// The configured level for a codec that supports compression levels.
    #[must_use]
    pub fn compression_level(&self, codec: CompressionType) -> Option<i32> {
        match codec {
            CompressionType::Gzip => Some(self.compression_gzip_level),
            CompressionType::Lz4 => Some(self.compression_lz4_level),
            CompressionType::Zstd => Some(self.compression_zstd_level),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        krabka_units::prelude::{ByteSizeExt as _, TimeExt, bytes, secs},
    };

    #[test]
    fn defaults_match_kafka_4x() {
        assert2::assert!(
            LogConfig::default()
                == LogConfig {
                    read_buffer_cap: mebibytes(4),
                    read_ahead_max: mebibytes(4),
                    timestamp_scan_window: kibibytes(64),
                    segment_size: bytes(1 << 30),
                    segment_roll_interval: days(7),
                    segment_jitter: Time::ZERO,
                    file_delete_delay: secs(60),
                    flush_messages: None,
                    flush_interval: None,
                    compression_gzip_level: -1,
                    compression_lz4_level: 9,
                    compression_zstd_level: 3,
                    retention: Some(days(7)),
                    retention_size: None,
                    max_message_size: bytes(1_048_588),
                    max_decompressed_record: None,
                    index_interval: bytes(4096),
                    segment_index_size: bytes(10 * 1024 * 1024),
                    flush_on_append: false,
                    segment_allocation: SegmentAllocation::OnWrite,
                    tail_cache_size: mebibytes(32),
                    validate_on_open: true,
                    cleanup_policy: CleanupPolicy::Delete,
                    min_compaction_lag: Time::ZERO,
                    max_compaction_lag: None,
                    min_cleanable_dirty_ratio: fraction(0.5),
                    message_timestamp_type: TimestampType::CreateTime,
                    compression_type: None,
                    remote_storage_enable: false,
                    remote_tier: RemoteTierFlags::DEFAULT,
                    local_retention: None,
                    local_retention_size: None,
                    delete_retention: secs(24 * 60 * 60),
                    delivery_policy: DeliveryPolicy::Immediate,
                    schedule_order: ScheduleOrder::Unordered,
                    delivery_clock_uncertainty: krabka_units::prelude::millis(250),
                }
        );
    }

    #[test]
    fn defaults_cross_the_raw_seams_as_kafkas_documented_numbers() {
        // The quantities exist to be handed to `.index` sizing, retention
        // arithmetic, and Kafka config reporting as plain integers; a
        // scale slip in a constructor would show up here.
        let c = LogConfig::default();
        assert2::check!(c.segment_size.bytes_u64() == 1_073_741_824);
        assert2::check!(c.index_interval.bytes_u64() == 4_096);
        assert2::check!(c.segment_index_size.bytes_u64() == 10_485_760);
        assert2::check!(c.max_message_size.bytes_u64() == 1_048_588);
        assert2::check!(c.segment_roll_interval.millis_i64() == 604_800_000);
        assert2::check!(c.retention.map(TimeExt::millis_i64) == Some(604_800_000));
        assert2::check!(c.delete_retention.millis_i64() == 86_400_000);
    }

    #[test]
    fn default_cleanup_policy_is_delete() {
        let c = LogConfig::default();
        assert2::assert!(c.cleanup_policy == CleanupPolicy::Delete);
    }

    #[yare::parameterized(
        delete = { CleanupPolicy::Delete, false, true, "delete" },
        compact = { CleanupPolicy::Compact, true, false, "compact" },
        compact_and_delete = { CleanupPolicy::CompactAndDelete, true, true, "compact,delete" },
        no_cleanup = { CleanupPolicy::NoCleanup, false, false, "" },
    )]
    fn a_policy_reports_the_halves_of_kafkas_cleanup_policy_list_it_contains(
        policy: CleanupPolicy,
        compact: bool,
        delete: bool,
        rendered: &'static str,
    ) {
        // `LogConfig` in Kafka derives `compact` and `delete` from the list by
        // membership, so `compact,delete` is both, and each name alone is one.
        assert2::check!(policy.contains_compact() == compact, "{policy:?}");
        assert2::check!(policy.contains_delete() == delete, "{policy:?}");
        assert2::check!(policy.as_str() == rendered, "{policy:?}");
    }

    #[test]
    fn default_compression_is_producer_passthrough() {
        let c = LogConfig::default();
        assert2::assert!(c.compression_type == None);
    }

    #[test]
    fn delivery_is_immediate_with_a_quarter_second_clock_bound() {
        let c = LogConfig::default();
        assert2::check!(c.delivery_policy == DeliveryPolicy::Immediate);
        assert2::check!(c.delivery_clock_uncertainty.millis_i64() == 250);
    }

    #[test]
    fn default_local_retention_is_none() {
        let c = LogConfig::default();
        assert2::assert!(c.local_retention == None);
        assert2::assert!(c.local_retention_size == None);
    }
}
