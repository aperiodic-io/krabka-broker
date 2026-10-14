//! What one soak run does, and the bounds it is judged against.
//!
//! Every memory bound here is derived from the two entry limits the brokers
//! run with, `dedup_max_entries_per_partition` and `dedup_max_entries`, and
//! from the layout of the index that `krabka-dedup` documents: 64 segment
//! tables, each a dense entry array plus a hash table, which shrink when a
//! quarter full and stop shrinking at a floor of 256 slots.

use {krabka_dedup::DedupWindowFilter, std::time::Duration};

/// The environment variable that sets how long the cycles run, in seconds.
pub const SOAK_SECS_ENV: &str = "DEDUP_SOAK_SECS";

/// How long the cycles run when [`SOAK_SECS_ENV`] is unset: with the cluster
/// boot and the teardown, about a minute and a half.
const DEFAULT_SOAK_SECS: u64 = 75;

/// The brokers of the cluster. Every partition has a replica on each, so each
/// broker indexes every partition: as leader or as follower.
pub const BROKERS: u64 = 3;

/// The broker the churn restarts. No partition prefers it as leader, so a
/// restart never moves a leader and the producers keep their routes; what the
/// restart forces is a rebuild of every follower index it held.
pub const RESTARTED_NODE: u64 = 3;

/// The topics that exist at any one time. The churn replaces, toggles and
/// reconfigures them one at a time.
pub const SLOTS: usize = 4;

/// Partitions of each topic.
pub const PARTITIONS: i32 = 3;

/// The replicas of each partition, preferred leader first: brokers 1 and 2
/// lead, [`RESTARTED_NODE`] only follows.
pub const PLACEMENT: [&[i32]; 3] = [&[1, 2, 3], &[2, 1, 3], &[1, 3, 2]];

/// The dedup windows the topics move between: a few seconds, so an index
/// fills, ages out and refills many times in one run.
pub const WINDOWS_MS: [i64; 3] = [1_500, 2_000, 3_000];

/// The longest of [`WINDOWS_MS`].
pub const MAX_WINDOW_MS: i64 = 3_000;

/// The partitions each broker hosts a replica of.
pub const HOSTED_PARTITIONS: u64 = 12;

/// The broker's dedup ticker period: each tick visits
/// [`DedupWindowFilter::TICK_VISITS`] entries of every idle index.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

/// A segment table at rest never holds more slots than this: it stops
/// shrinking at this capacity.
const SHRINK_FLOOR_SLOTS: u64 = 256;

/// An upper bound on the heap of one slot of a segment table: a 32-byte entry
/// (stored hash, 128-bit digest, timestamp) in the dense array, plus the hash
/// table's share, at most `2 * 8 / 7` buckets of 9 bytes after power-of-two
/// rounding. That is under 53 bytes; 64 leaves room for rounding.
const SLOT_BYTES: u64 = 64;

/// A segment shrinks only when a quarter full, so a live segment above the
/// floor has at most four slots per entry.
const SLOTS_PER_LIVE_ENTRY: u64 = 4;

/// The fixed part of one index: the segment headers and the cursor's
/// capacity histogram, a few KiB, rounded up.
const FIXED_INDEX_BYTES: u64 = 16 * 1024;

/// What the allocator may hold on top of the heap the index estimates:
/// size-class rounding and freed blocks it has not returned.
const ALLOCATOR_SLACK: u64 = 2;

/// Resident memory the traffic itself may take above the warmed-up baseline,
/// whatever the index holds: request buffers in flight (six producers, at
/// most one ~100 KiB batch each, decoded and re-encoded on the leader and
/// fetched by two followers), the segment files rolled and mapped during a
/// cycle, and the allocator arenas that the blocking pool's threads fill.
const TRAFFIC_HEADROOM_KIB: u64 = 384 * 1024;

/// How long every soak topic keeps a segment: `retention.ms`.
pub const RETENTION_MS: i64 = 20_000;

/// How often a soak topic rolls its segment while it takes produces:
/// `segment.ms`.
pub const SEGMENT_MS: i64 = 5_000;

/// The files an open segment holds open: `.log`, `.index` and `.timeindex`.
const FILES_PER_SEGMENT: u64 = 3;

/// The segment-file descriptors of the brokers' internal topics -- the
/// metadata log and the audit topic -- a few segments each.
const INTERNAL_SEGMENT_FDS: u64 = 64;

/// The descriptors other than segment files that the process may open above
/// its warmed-up baseline: producer and replica connections that reconnect,
/// and the files a restart reopens.
pub const OTHER_FD_HEADROOM: u64 = 256;

/// The shape of one soak run.
#[derive(Debug, Clone, bon::Builder)]
pub struct Settings {
    /// How long the cycles run. The run ends with the first cycle that
    /// finishes after this, and not before [`Self::min_cycles`] cycles.
    #[builder(default = Duration::from_secs(DEFAULT_SOAK_SECS))]
    pub soak: Duration,
    /// The fewest cycles a run takes, however slow the machine, so that the
    /// per-cycle trends always have something to judge.
    #[builder(default = 6)]
    pub min_cycles: usize,
    /// The cycles left out of every per-cycle trend: the allocator arenas,
    /// the segment files and the metric families are still growing to their
    /// working size.
    #[builder(default = 2)]
    pub warmup_cycles: usize,
    /// How often the recorder samples the process and the gauges.
    #[builder(default = Duration::from_secs(2))]
    pub sample_every: Duration,
    /// How long the steady load of each cycle lasts.
    #[builder(default = Duration::from_secs(3))]
    pub steady_load: Duration,
    /// Concurrent producers of the steady load.
    #[builder(default = 6)]
    pub producers: usize,
    /// The requests each producer sends per second in the steady load.
    #[builder(default = 10)]
    pub requests_per_second: u32,
    /// The records of one steady batch.
    #[builder(default = 100)]
    pub records_per_batch: u64,
    /// `dedup_max_entries_per_partition` of every broker.
    #[builder(default = 4_096)]
    pub max_entries_per_partition: u64,
    /// `dedup_max_entries` of every broker: below
    /// `HOSTED_PARTITIONS * max_entries_per_partition`, so the broker budget
    /// binds before every partition is full.
    #[builder(default = 32_768)]
    pub max_entries: u64,
    /// The churn restarts [`RESTARTED_NODE`] every this many cycles, or
    /// never when unset.
    pub restart_every: Option<usize>,
}

impl Settings {
    /// The default run, lengthened by [`SOAK_SECS_ENV`] when it is set, and
    /// restarting a broker every `restart_every` cycles when that is set.
    pub fn from_env(restart_every: Option<usize>) -> Self {
        let soak = soak_from(std::env::var(SOAK_SECS_ENV).ok().as_deref());
        Self::builder()
            .maybe_soak(soak)
            .maybe_restart_every(restart_every)
            .build()
    }

    /// Whether the churn restarts a broker after `cycle`.
    pub fn restarts_after(&self, cycle: usize) -> bool {
        self.restart_every
            .is_some_and(|every| cycle % every == every / 2)
    }

    /// The most entries one broker's indexes may hold. Every filter checks the
    /// shared budget before it inserts, and the budget documents that a
    /// concurrent read can be briefly out of date, so each partition writer
    /// that inserts at the same moment can take one entry past the limit.
    pub fn broker_entry_ceiling(&self) -> u64 { self.max_entries + HOSTED_PARTITIONS }

    /// The heap an index of `partitions` partitions keeps when it holds no
    /// entry: every segment at its shrink floor, plus the fixed tables.
    pub fn residual_heap_ceiling(partitions: u64) -> u64 {
        let segments = u64::try_from(DedupWindowFilter::SEGMENTS).expect("segment count");
        partitions * (segments * SHRINK_FLOOR_SLOTS * SLOT_BYTES + FIXED_INDEX_BYTES)
    }

    /// The most heap one broker's indexes may report: the residual of every
    /// hosted partition plus four slots for every entry the budget allows.
    pub fn heap_ceiling(&self) -> u64 {
        Self::residual_heap_ceiling(HOSTED_PARTITIONS)
            + self.broker_entry_ceiling() * SLOTS_PER_LIVE_ENTRY * SLOT_BYTES
    }

    /// The highest resident size the process may reach, given its resident
    /// size once warmed up: the heap every broker's indexes may hold, twice
    /// over for the allocator, plus [`TRAFFIC_HEADROOM_KIB`].
    pub fn peak_rss_ceiling_kib(&self, warm_rss_kib: u64) -> u64 {
        warm_rss_kib + BROKERS * self.heap_ceiling() * ALLOCATOR_SLACK / 1024 + TRAFFIC_HEADROOM_KIB
    }

    /// The most segment-file descriptors the process may hold: every hosted
    /// replica on every broker with all the segments retention keeps -- one
    /// per `segment.ms` within `retention.ms` -- plus its active segment and
    /// one whose deletion is still delayed.
    pub fn segment_fd_ceiling() -> u64 {
        let retained = u64::try_from(RETENTION_MS / SEGMENT_MS).expect("a segment count") + 2;
        BROKERS * HOSTED_PARTITIONS * FILES_PER_SEGMENT * retained + INTERNAL_SEGMENT_FDS
    }

    /// The most descriptors of deleted files the process may hold at once:
    /// one segment of every hosted replica on every broker, which a read
    /// still had open when retention unlinked it.
    pub fn deleted_fd_ceiling() -> u64 { BROKERS * HOSTED_PARTITIONS * FILES_PER_SEGMENT }

    /// How long an idle drain may take before it counts as stuck: the longest
    /// window, then one tick for every [`DedupWindowFilter::TICK_VISITS`]
    /// entries of the largest index, then a wide margin for a loaded machine.
    pub fn drain_timeout(&self) -> Duration {
        let visits = u64::try_from(DedupWindowFilter::TICK_VISITS).expect("tick visits");
        let ticks =
            u32::try_from(self.max_entries_per_partition.div_ceil(visits)).expect("a tick count");
        Duration::from_millis(MAX_WINDOW_MS.unsigned_abs())
            + TICK_INTERVAL * (ticks + 1)
            + Duration::from_secs(20)
    }

    /// The shortest idle period between two loads: the longest window and a
    /// tick, so that every entry has expired and a tick has seen it expire.
    pub fn min_idle() -> Duration {
        Duration::from_millis(MAX_WINDOW_MS.unsigned_abs()) + TICK_INTERVAL
    }
}

/// The soak duration a [`SOAK_SECS_ENV`] value asks for, `None` when unset.
fn soak_from(value: Option<&str>) -> Option<Duration> {
    let value = value?.trim();
    let secs = value
        .parse()
        .unwrap_or_else(|_| panic!("{SOAK_SECS_ENV}={value} is not a whole number of seconds"));
    Some(Duration::from_secs(secs))
}

mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[yare::parameterized(
        unset = { None, None },
        seconds = { Some("1200"), Some(Duration::from_mins(20)) },
        padded = { Some(" 90\n"), Some(Duration::from_secs(90)) },
    )]
    fn the_soak_length_comes_from_the_environment(value: Option<&str>, expected: Option<Duration>) {
        check!(soak_from(value) == expected);
    }

    #[test]
    #[should_panic(expected = "is not a whole number of seconds")]
    fn a_malformed_soak_length_is_refused() { soak_from(Some("an hour")); }

    #[yare::parameterized(
        never = { None, &[] },
        every_fourth = { Some(4), &[2, 6, 10] },
        every_cycle = { Some(1), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] },
    )]
    fn restarts_fall_mid_period(restart_every: Option<usize>, cycles: &[usize]) {
        let settings = Settings::builder()
            .maybe_restart_every(restart_every)
            .build();
        let restarted: Vec<usize> = (0..12).filter(|c| settings.restarts_after(*c)).collect();
        check!(restarted == cycles);
    }

    #[test]
    fn the_bounds_follow_the_entry_limits() {
        let small = Settings::builder()
            .max_entries_per_partition(1_024)
            .max_entries(8_192)
            .build();
        let large = Settings::builder()
            .max_entries_per_partition(4_096)
            .max_entries(32_768)
            .build();
        // Four slots of 64 bytes for each extra entry the budget allows.
        assert!(large.heap_ceiling() - small.heap_ceiling() == (32_768 - 8_192) * 4 * 64);
        // 64 segments at a 256-slot floor, plus the fixed tables.
        assert!(Settings::residual_heap_ceiling(1) == 64 * 256 * 64 + 16 * 1024);
        let slots = u64::try_from(SLOTS).unwrap();
        let partitions = u64::try_from(PARTITIONS).unwrap();
        assert!(HOSTED_PARTITIONS == slots * partitions);
        assert!(PLACEMENT.len() == usize::try_from(PARTITIONS).unwrap());
        assert!(WINDOWS_MS.iter().max() == Some(&MAX_WINDOW_MS));
        // Six segments of three files for each of 36 replicas, and the
        // internal topics.
        assert!(Settings::segment_fd_ceiling() == 36 * 3 * 6 + 64);
        assert!(Settings::deleted_fd_ceiling() == 36 * 3);
        assert!(large.drain_timeout() > small.drain_timeout());
        assert!(
            large.peak_rss_ceiling_kib(0) - small.peak_rss_ceiling_kib(0)
                == 3 * (large.heap_ceiling() - small.heap_ceiling()) * 2 / 1024
        );
    }
}
