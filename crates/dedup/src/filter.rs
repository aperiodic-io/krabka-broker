use {
    crate::{
        budget::DedupBudget,
        digest::{DigestBuildHasher, IdentityDigest},
    },
    indexmap::IndexMap,
    std::{fmt, sync::Arc},
};

#[cfg(test)]
mod bounds_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

// Entries the cursor visits for each new identity. Eight per new identity
// sweeps the whole index once every `len / 8` new identities, spread evenly.
// One whole-index pass at a time would stall the thread that runs it, and
// sweeps happen at the same log position on every replica, so the leader and
// its followers would stall together and leave nobody to complete an acks=all
// quorum.
const SWEEP_BUDGET: usize = 8;

// What one sweep step may drop. Expired entries can match nothing, so dropping
// them costs no coverage at any time. Live entries below the capacity cutoff
// can still match, so only a step that makes room for a new identity drops
// them: a tick makes room for nobody.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SweepMode {
    Expired,
    ExpiredAndCapacity,
}

// Extra entries the cursor may visit when a new identity arrives with the
// index exactly at its limit: enough to find something to free while the soft
// limit keeps the tail trimmed, and small enough to pay on every record. The
// cursor advances either way, so repeated attempts walk the index and refresh
// the capacity cutoff instead of doing the same work again.
const CAPACITY_EMERGENCY_BUDGET: usize = 64;

// The margin below a limit, as a divisor of that limit, where the cursor
// starts to drop entries below the capacity cutoff. A tenth, so trimming starts
// at 90% and the index converges on its limit instead of dropping a quarter of
// itself in one step at the limit.
const CAPACITY_SOFT_MARGIN_DIVISOR: usize = 10;

// The capacity cutoff marks `visited / CAPACITY_EVICT_DIVISOR` of the entries
// that a completed pass visited as droppable: the oldest quarter.
const CAPACITY_EVICT_DIVISOR: usize = 4;

// Buckets in the histogram that finds the capacity cutoff. A fixed array adds
// no memory per entry. The entry limit exists to bound the index, and an LRU
// list would cost 8 to 16 bytes on every entry.
const CAPACITY_EVICT_BUCKETS: usize = 512;
// `CAPACITY_EVICT_BUCKETS - 1` as a timestamp divisor.
const LAST_BUCKET: i64 = 511;

// The index is `1 << SEGMENT_BITS` hash tables. When a table grows or shrinks,
// it moves its own entries and no others, so more tables make the largest
// single move smaller. 64 makes it 1/64 of the index, and the set of tables
// that hold entries fits in one `u64`, which lets the cursor skip empty tables
// in constant time.
const SEGMENT_BITS: u32 = 6;

// A segment holds at most this multiple of its even share of the entry limit.
// With well-spread digests a segment is near its share: at 10^5 entries per
// segment the spread is a few hundred. Identities chosen to share a segment are
// the case this limit is for. Without it, they would put the whole index in one
// table, and that table would move every entry when it doubled.
const SEGMENT_LIMIT_FACTOR: usize = 2;

// The lowest per-segment entry limit. It keeps a small entry limit from making
// segments that fill on an ordinary spread of identities. A move of this many
// entries takes well under a millisecond.
const SEGMENT_LIMIT_FLOOR: usize = 4096;

// A segment shrinks when its entries fall to this fraction of its capacity. It
// shrinks to `SHRINK_TARGET_FACTOR` times its length, which is half of the
// capacity, and it grows only when it is full. The gap between a quarter and
// full keeps a segment that changes size near one boundary from moving its
// entries on each change.
const SHRINK_DIVISOR: usize = 4;
const SHRINK_TARGET_FACTOR: usize = 2;

// A segment at or below this capacity does not shrink. Moving so few entries
// costs little, but a burst of small allocations gives no memory back that
// matters: 64 segments at this capacity hold about 0.8 MiB.
const SHRINK_FLOOR: usize = 256;

// The bytes of one entry in a segment's dense array: the stored hash, the
// digest and the timestamp. `indexmap` keeps the hash beside each entry.
const ENTRY_BYTES: usize = size_of::<usize>() + size_of::<IdentityDigest>() + size_of::<i64>();

// The bytes of one bucket in a segment's hash table: the index into the dense
// array and one control byte.
const INDEX_BUCKET_BYTES: usize = size_of::<usize>() + 1;

// The control bytes that hashbrown adds after the buckets: one SIMD group.
const CONTROL_TAIL_BYTES: usize = 16;

type Table = IndexMap<IdentityDigest, i64, DigestBuildHasher>;

// One hash table of the index, and the capacity it was last sized to.
#[derive(Default)]
struct Segment {
    table: Table,
    // `IndexMap::capacity` counts the slots an insert can still use, and a
    // removal can leave a tombstone that is not one of them. That number then
    // falls with no change to the allocation. This one changes only when the
    // segment grows or shrinks, so it describes the allocation.
    capacity: usize,
}

impl Segment {
    // Inserts an identity the segment does not hold. Returns the number of
    // entries the insert moved or rehashed.
    fn insert_new(&mut self, identity: IdentityDigest, timestamp: i64) -> usize {
        let length = self.table.len();
        // With a usable slot, the insert does not grow or rehash anything. So
        // every grow and rehash happens here, and is counted.
        let moved = if length < self.table.capacity() {
            0
        } else {
            // A full segment doubles, which is also what hashbrown does by
            // default. Doing it here makes the step a rule of this filter.
            // When tombstones, not entries, use up the slots, the segment
            // asks for one slot: hashbrown then rehashes it in place, or
            // doubles it past half load.
            let additional = if length < self.capacity {
                1
            } else {
                length.max(1)
            };
            self.table.reserve(additional);
            self.capacity = self.table.capacity();
            length
        };
        self.table.insert(identity, timestamp);
        moved
    }

    // Shrinks the segment to twice its length when it is a quarter full and
    // above `floor`. Returns the number of entries that moved. Shrinking keeps
    // the order of the entries, so a cursor in the segment stays valid.
    fn shrink_if_sparse(&mut self, floor: usize) -> usize {
        let length = self.table.len();
        if self.capacity <= floor || length > self.capacity / SHRINK_DIVISOR {
            return 0;
        }
        self.table.shrink_to(length * SHRINK_TARGET_FACTOR);
        self.capacity = self.table.capacity();
        length
    }
}

/// One record as the filter sees it: its digested identity, if it has one, and its timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DedupRecord {
    /// The record's identity, or `None` when dedup does not apply to it (see [`crate::IdentityLookup::NoIdentity`]).
    pub identity: Option<IdentityDigest>,
    /// The record's `CreateTime` in milliseconds since the epoch.
    pub timestamp_ms: i64,
}

/// One index change that [`DedupWindowFilter::classify`] made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UndoEntry {
    /// The identity that changed.
    pub identity: IdentityDigest,
    /// The timestamp the request wrote for the identity.
    pub applied_timestamp: i64,
    /// The timestamp the identity held before, or `None` when the request inserted it.
    pub previous_timestamp: Option<i64>,
}

/// The index changes of one produce request, in the order the request made them.
///
/// [`DedupWindowFilter::revert`] uses it to undo exactly that request when its replication fails. It lives only for the duration of one replication.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Undo {
    entries: Vec<UndoEntry>,
}

impl Undo {
    /// The changes, in the order the request made them.
    #[must_use]
    pub fn entries(&self) -> &[UndoEntry] { &self.entries }

    /// Whether the request changed nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
}

/// The result of [`DedupWindowFilter::classify`] for one produce request.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Classification {
    /// For each record in request order, whether it is a duplicate to drop.
    pub dropped: Vec<bool>,
    /// The number of records kept.
    pub kept: usize,
    /// The index changes, for [`DedupWindowFilter::revert`].
    pub undo: Undo,
}

impl Classification {
    /// Whether every record is kept. The caller can then send the batch on unchanged. True for an empty request.
    #[must_use]
    pub fn all_kept(&self) -> bool { self.kept == self.dropped.len() }

    /// Whether every record is dropped. The caller then has nothing to replicate. True for an empty request.
    #[must_use]
    pub fn none_kept(&self) -> bool { self.kept == 0 }
}

/// What a filter has done over its lifetime.
///
/// The filter can stop deduplicating without failing a request in three ways: through producer clock skew, through a full index, and through records that arrive older than the window. Each is counted, with the drop count that makes them readable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DedupCounters {
    /// Records dropped as duplicates.
    pub dropped_records: u64,
    /// Records whose timestamp was ahead of the broker's time and was clamped. Non-zero means producer clock skew reaches the index.
    pub skew_clamped_records: u64,
    /// Records admitted without their identity being indexed, because the index was full. Non-zero means dedup does not cover some new identities.
    pub unindexed_records: u64,
    /// Entries still inside the window that were dropped to make room. Non-zero means the effective window is narrower than the configured one: about the entry limit divided by the rate of new identities.
    pub capacity_evicted_entries: u64,
    /// Bounded extra sweep steps taken because a new identity arrived with the index at its limit. Non-zero means the soft limit does not keep up with the rate of new identities.
    pub capacity_sweeps: u64,
    /// Records admitted without their identity being indexed, because their timestamp was already more than the window behind the broker's time when they arrived. A backfill with old `CreateTime`s does this. Such a record can be neither a duplicate nor matched later.
    pub expired_on_arrival_records: u64,
    /// Requests where some, but not all, records were dropped. Only these make the caller rebuild and recompress a batch, so this rate drives the cost of dedup on the produce path.
    pub rebuilt_batches: u64,
}

/// The first-wins dedup index of one partition.
///
/// A record is dropped when the index holds a live entry for its identity, and the two timestamps are at most `window_ms` apart. The leader classifies each produce batch with [`classify`](Self::classify) under the partition's log lock, immediately before it appends the batch, and undoes the classification with [`revert`](Self::revert) when that append fails. Every replica indexes the batches it appends with [`populate`](Self::populate), and rebuilds a stale index from the log the same way.
///
/// Call it for plain and idempotent produce batches. A dropped record of an idempotent batch leaves a gap in the offsets, and the batch keeps its offset span, so the producer's sequence numbers stay intact. Transactional batches bypass dedup, because a transactional record can still abort.
///
/// Expiry uses the broker's clock, as Kafka's `retention.ms` does. The filter keeps a watermark: the highest `now_ms` that a call gave it. It never moves back. An entry with timestamp `ts` is live while `watermark - ts <= window_ms`. A lookup treats an entry that is not live as absent, so the time at which a sweep removes it never changes a decision. A record that is already not live when it arrives, for example a backfill with old `CreateTime`s, is admitted and not indexed, on every path. A running index and an index rebuilt from the log at the same time therefore agree on which records are live, because the rebuild also reads only the records of the last window before the broker's time.
///
/// An entry leaves the index only when the cursor visits it. A partition with no traffic makes no calls, so without ticks it would hold its expired entries, and their share of the [`DedupBudget`], forever. The caller calls [`tick`](Self::tick) at a regular interval, which moves the watermark and gives the cursor those visits.
///
/// Memory is bounded in three ways. Each entry is a fixed-size [`IdentityDigest`]. Entries that are not live cannot cause a drop, and a cursor drops them: it visits a few entries for each new identity, and [`TICK_VISITS`](Self::TICK_VISITS) for each tick. A hard entry limit, and an optional broker-wide [`DedupBudget`], stop a large window or many identities from using all memory. From a soft limit a tenth below either limit, the cursor also drops, for each new identity, the entries that a completed pass found in the oldest quarter. The effective window then narrows to about the limit divided by the rate of new identities, and dedup does not stop for new identities.
///
/// The broker budget makes the effective capacity of a partition depend on the other partitions of its broker, and replicas of one partition seldom share a broker with the same set. Under broker pressure, replicas can therefore trim different amounts. This is the same best-effort divergence that capacity trimming already has.
///
/// No operation does work proportional to the index. This is a requirement: sweeps happen at the same log position on every replica, so a whole-index pass stalls the leader and then the followers together.
///
/// For the same reason, the index is [`SEGMENTS`](Self::SEGMENTS) hash tables and not one. The top bits of an identity's digest choose its segment. A segment that is full doubles its capacity, and a segment that falls to a quarter of its capacity, above a floor of 256 entries, shrinks to twice its length. Either step moves the entries of that one segment, and each segment holds at most `max(2 * ceil(max_entries / SEGMENTS), 4096)` entries, so no call moves more than that many. One table for the partition would move every entry at once: tens to hundreds of milliseconds at millions of entries. [`largest_relocation`](Self::largest_relocation) reports the largest move so far. Shrinking keeps the heap within a small factor of the live entries, plus the floor of each segment, after a burst expires. [`heap_bytes`](Self::heap_bytes) estimates it.
///
/// The per-segment limit binds only when identities crowd one segment. Well-spread digests stay far below it, but xxh3 does not resist chosen input, so a producer can choose identities that share a segment. A new identity for a full segment is then admitted unindexed and counted, as at the entry limit.
///
/// Record timestamps are client-supplied `CreateTime`s, so each one is clamped to the broker's time before it reaches the index. Without the clamp, an entry dated ahead of the broker would stay live until the broker's time passed it by a whole window, and would drop correctly timed records for that long. The clamp does not bound reordering within the window, so the window edge stays approximate.
pub struct DedupWindowFilter {
    window_ms: i64,
    max_entries: usize,
    budget: Option<Arc<DedupBudget>>,
    // This filter's contribution to the budget's entry count.
    budget_accounted: usize,
    // `SEGMENTS` tables. Each is a dense array of entries in insertion order
    // with a hash index over it. A removal moves the last entry of its segment
    // into the freed slot, which the cursor relies on.
    segments: Box<[Segment]>,
    // The sum of the segment lengths.
    len: usize,
    // Bit `i` is set when segment `i` holds an entry.
    occupied: u64,
    // The most entries one segment may hold. See `SEGMENT_LIMIT_FACTOR`.
    segment_limit: usize,
    // The capacity at or below which a segment does not shrink. A field, and
    // not the constant, so that tests can make small segments shrink.
    shrink_floor: usize,
    // The most entries that one grow or shrink of a segment moved.
    largest_relocation: usize,
    // The highest broker time that a call gave: the reference "now" for
    // expiry. Every stored timestamp is at or below it, because each one was
    // clamped to the `now_ms` of its call.
    watermark_ms: i64,
    // A position in the segments' dense arrays: a segment, and an index in
    // it. Replicas that make the same calls with the same times agree on it,
    // because they insert the same identities in the same order and so lay
    // out the arrays the same way. Two things break that. `revert` removes
    // entries a follower never inserted, which permutes the leader's arrays.
    // And each broker ticks on its own clock, so the cursors of two replicas
    // stand at different places. The two then sweep equal sets in different
    // orders and can drop different live entries for capacity. That needs the
    // index above 90% of its limit, and which live entry goes is already
    // best-effort, but it is inherent to bounded work: a cursor is a position
    // in a layout. Expiry itself never makes replicas decide differently,
    // because a lookup treats an entry that is not live as absent.
    sweep_segment: usize,
    sweep_pos: usize,
    // The histogram of the entries the current pass visited, measured against
    // a frame frozen at the start of the pass. The watermark moves while the
    // cursor walks, and buckets that move under it would describe no single
    // moment.
    pass_counts: Box<[usize; CAPACITY_EVICT_BUCKETS]>,
    pass_visited: usize,
    pass_lo: i64,
    pass_span: i64,
    pass_width: i64,
    // The timestamp below which the oldest quarter of the index lay when the
    // last pass completed. `i64::MIN` means that pass found no bucket prefix
    // small enough to cut, so nothing is dropped for capacity: more than a
    // quarter shares one narrow range, and dropping it would lose coverage
    // that nobody asked to lose.
    capacity_cutoff: i64,
    sweep_visits: u64,
    counters: DedupCounters,
}

impl DedupWindowFilter {
    /// The default per-partition entry limit.
    pub const DEFAULT_MAX_ENTRIES: usize = 1_000_000;
    /// The highest per-partition entry limit that configuration accepts.
    pub const MAX_MAX_ENTRIES: usize = 10_000_000;
    /// The number of hash tables the index is split into.
    pub const SEGMENTS: usize = 1 << SEGMENT_BITS;
    /// The most entries that one [`tick`](Self::tick) visits.
    ///
    /// The cursor drops an expired entry when it visits it, so a filter with no traffic releases at most this many entries for each tick. The interval of the caller's ticks therefore sets how soon an idle partition gives its expired entries back: an index of `len` entries needs `ceil(len / TICK_VISITS)` ticks after the last one expires.
    // 1024 is the sweep work of 128 new identities. A visit reads one entry
    // and a removal is a swap in one segment. On an idle index of two million
    // entries, a release build took about 0.1 ms for one tick, and about 1 ms
    // for a tick that also shrank a segment (`report_slowest_tick_while_idle`).
    // A caller can run that between two appends under the log lock without a
    // pause that a producer sees. A shrink moves at most one segment, as on
    // every other path. At one tick per second, a full default index of 10^6
    // entries is released in about 16 minutes after it expires, and at one
    // tick per 100 ms in under 2.
    pub const TICK_VISITS: usize = 1024;

    /// Makes an empty filter.
    ///
    /// `max_entries` is the per-partition entry limit. A filter with a `budget` also keeps the sum over every filter on that budget under the budget's limit. A negative `window_ms` makes no record a duplicate: every record is then not live on arrival, and none is indexed.
    ///
    /// The filter allocates no entry space until it indexes an identity, so an idle partition costs a few KiB.
    #[must_use]
    pub fn new(window_ms: i64, max_entries: usize, budget: Option<Arc<DedupBudget>>) -> Self {
        let mut filter = Self {
            window_ms,
            max_entries,
            budget,
            budget_accounted: 0,
            segments: empty_segments(),
            len: 0,
            occupied: 0,
            segment_limit: segment_limit_of(max_entries),
            shrink_floor: SHRINK_FLOOR,
            largest_relocation: 0,
            watermark_ms: i64::MIN,
            sweep_segment: 0,
            sweep_pos: 0,
            pass_counts: Box::new([0; CAPACITY_EVICT_BUCKETS]),
            pass_visited: 0,
            pass_lo: 0,
            pass_span: 0,
            pass_width: 1,
            capacity_cutoff: i64::MIN,
            sweep_visits: 0,
            counters: DedupCounters::default(),
        };
        filter.begin_pass();
        filter
    }

    /// Classifies the records of one produce request in order, and indexes the kept ones.
    ///
    /// Classification is first-wins in record order, so two records with one identity in the same request resolve as they would across requests. `now_ms` is the broker's time: it moves the watermark, and each timestamp is first clamped to it. A duplicate does not change the index, so a request that is all duplicates changes only the watermark. A record that is not live on arrival is kept and not indexed.
    pub fn classify(&mut self, records: &[DedupRecord], now_ms: i64) -> Classification {
        self.advance_watermark(now_ms);
        let mut classification = Classification {
            dropped: Vec::with_capacity(records.len()),
            kept: 0,
            undo: Undo::default(),
        };
        for record in records {
            let timestamp = self.clamp_to_now(record.timestamp_ms, now_ms);
            let dropped = record.identity.is_some_and(|identity| {
                self.is_duplicate(identity, timestamp, &mut classification.undo)
            });
            classification.dropped.push(dropped);
            if !dropped {
                classification.kept += 1;
            }
        }
        if classification.kept > 0 && classification.kept < records.len() {
            self.counters.rebuilt_batches += 1;
        }
        classification
    }

    /// Undoes the index changes of one request from [`classify`](Self::classify).
    ///
    /// This is compare-and-revert: an entry is restored or removed only while the index still holds the timestamp this request wrote. An identity that a later request overwrote keeps the later state. A restored entry can be one that is no longer live, which a lookup treats as absent. Sweeps and the watermark are not reverted: an expired entry cannot change a decision, and the watermark is the broker's time, which the failed request did not change.
    pub fn revert(&mut self, undo: &Undo) {
        // Only this request's own entries come back. A capacity eviction that
        // classifying it forced stays done, so a failed request can cost
        // coverage that was not its own. That is bounded by one slice.
        //
        // Reverse order, so that several changes to one identity unwind
        // correctly: the last entry restores what the one before it wrote,
        // and the first restores the state before the request. One request
        // has one broker time, so `classify` makes at most one change per
        // identity today, but the order keeps `revert` correct for any undo.
        for entry in undo.entries.iter().rev() {
            let segment = segment_of(entry.identity);
            let Some(timestamp) = self.segments[segment].table.get_mut(&entry.identity) else {
                continue;
            };
            if *timestamp != entry.applied_timestamp {
                continue;
            }
            if let Some(previous) = entry.previous_timestamp {
                *timestamp = previous;
            } else {
                self.segments[segment].table.swap_remove(&entry.identity);
                self.after_removal(segment);
            }
        }
    }

    /// Records an identity that the log admitted.
    ///
    /// The index keeps the highest timestamp per identity, so applying one log prefix any number of times at one broker time gives the same index. `now_ms` is the broker's time: it moves the watermark, and the timestamp is clamped to it, which changes nothing for a record behind the broker's time. A record that is not live on arrival is not indexed, as in [`classify`](Self::classify).
    pub fn populate(&mut self, identity: IdentityDigest, timestamp_ms: i64, now_ms: i64) {
        self.advance_watermark(now_ms);
        let timestamp = self.clamp_to_now(timestamp_ms, now_ms);
        if self.expired_on_arrival(timestamp) {
            return;
        }
        let segment = segment_of(identity);
        // Followers apply the leader's capacity rule at the same point as
        // `is_duplicate` does: look up, sweep, then insert. Expiry timing never
        // changes a decision, because a lookup treats an expired entry as
        // absent, and max-wins replaces it: a live timestamp is above every
        // expired one. Capacity
        // trimming drops entries that could still match, so the position of
        // the sweep relative to the insert decides what the index holds.
        if let Some(stored) = self.segments[segment].table.get_mut(&identity) {
            if timestamp > *stored {
                *stored = timestamp;
            }
            return;
        }
        self.sweep_step(SWEEP_BUDGET, SweepMode::ExpiredAndCapacity);
        if self.at_capacity(segment) && !self.make_room_for_one(segment) {
            self.counters.unindexed_records += 1;
            return;
        }
        self.insert_new(segment, identity, timestamp);
    }

    /// Moves the watermark to `now_ms`, and drops the entries that are no longer live from one bounded slice of the index.
    ///
    /// The cursor visits at most [`TICK_VISITS`](Self::TICK_VISITS) entries, and a tick on an empty filter does nothing else. A tick drops only entries that are not live, so it costs no coverage. A segment that a tick empties to a quarter shrinks, so the heap and the [`DedupBudget`] follow the live entries down with no traffic. Call it at a regular interval for every filter, with the broker's time.
    pub fn tick(&mut self, now_ms: i64) {
        self.advance_watermark(now_ms);
        self.sweep_step(Self::TICK_VISITS, SweepMode::Expired);
    }

    /// Changes the window, for example on a topic configuration change.
    ///
    /// The entries stay. The new window applies to every later comparison.
    pub fn set_window(&mut self, window_ms: i64) { self.window_ms = window_ms; }

    /// The window in milliseconds.
    #[must_use]
    pub fn window_ms(&self) -> i64 { self.window_ms }

    /// Removes every entry and gives the memory back.
    ///
    /// The cursor, its histogram, and the watermark reset as well, so the filter then indexes a log as a new filter does. The counters, [`sweep_visits`](Self::sweep_visits) and [`largest_relocation`](Self::largest_relocation) stay.
    pub fn clear(&mut self) {
        // New segments, not `IndexMap::clear`, which keeps the capacity each
        // grew to: tens of MiB at the default limit. Every caller wants it
        // back.
        self.segments = empty_segments();
        self.len = 0;
        self.occupied = 0;
        self.sync_budget();
        self.watermark_ms = i64::MIN;
        self.sweep_segment = 0;
        self.sweep_pos = 0;
        self.capacity_cutoff = i64::MIN;
        self.begin_pass();
    }

    /// The number of indexed identities.
    #[must_use]
    pub fn len(&self) -> usize { self.len }

    /// Whether no identity is indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// The per-partition entry limit.
    #[must_use]
    pub fn max_entries(&self) -> usize { self.max_entries }

    /// The highest broker time that a call gave since the filter was made or cleared, or `i64::MIN` when no call did.
    #[must_use]
    pub fn watermark_ms(&self) -> i64 { self.watermark_ms }

    /// The stored timestamp of `identity`, or `None` when it is not indexed.
    #[must_use]
    pub fn timestamp_of(&self, identity: IdentityDigest) -> Option<i64> {
        self.segments[segment_of(identity)]
            .table
            .get(&identity)
            .copied()
    }

    /// The indexed identities and their timestamps, in the order the sweep cursor walks: segment by segment, and in each segment in its dense order.
    pub fn iter(&self) -> impl Iterator<Item = (IdentityDigest, i64)> + '_ {
        self.segments
            .iter()
            .flat_map(|segment| segment.table.iter())
            .map(|(identity, timestamp)| (*identity, *timestamp))
    }

    /// The counters.
    #[must_use]
    pub fn counters(&self) -> DedupCounters { self.counters }

    /// The entries the sweep cursor visited over the filter's lifetime.
    ///
    /// Every operation visits a bounded number, so the change across one call shows that the call did no work proportional to the index.
    #[must_use]
    pub fn sweep_visits(&self) -> u64 { self.sweep_visits }

    /// The most entries of one segment that a single grow, shrink, or rehash moved, over the filter's lifetime.
    ///
    /// Such a step copies the entries of one segment or rebuilds its hash table, so this measures the longest pause that a change of capacity caused. It is at most `max(2 * ceil(max_entries / SEGMENTS), 4096)`.
    #[must_use]
    pub fn largest_relocation(&self) -> usize { self.largest_relocation }

    /// An estimate of the heap bytes that the index holds.
    ///
    /// It is computed from the capacity each segment was last sized to, with the entry and hash-table layouts of `indexmap` and `hashbrown`, plus the fixed segment table and cursor histogram. It is an estimate: it does not see allocator overhead, and it assumes that a segment's hash table is no larger than its entry array needs.
    #[must_use]
    pub fn heap_bytes(&self) -> usize {
        let tables: usize = self
            .segments
            .iter()
            .map(|segment| segment_heap_bytes(segment.capacity))
            .sum();
        tables
            + size_of::<Segment>() * Self::SEGMENTS
            + size_of::<[usize; CAPACITY_EVICT_BUCKETS]>()
    }

    /// The broker-wide budget this filter reports into, if it has one.
    #[must_use]
    pub fn budget(&self) -> Option<&DedupBudget> { self.budget.as_deref() }

    // Client `CreateTime` is untrusted, and the broker admits records ahead of
    // its own clock. An entry stored ahead of the broker's time would stay
    // live until the watermark passed it by a whole window, and would drop
    // correctly timed records for up to that long. The clamp also keeps every
    // stored timestamp at or below the watermark.
    //
    // The clamp changes nothing for a record at or behind the broker's time.
    // A record still ahead of it during log replay was clamped to its arrival,
    // which the log does not record, so the broker's rebuild skips it rather
    // than clamping it to the later replay time.
    fn clamp_to_now(&mut self, timestamp_ms: i64, now_ms: i64) -> i64 {
        if timestamp_ms > now_ms {
            self.counters.skew_clamped_records += 1;
            return now_ms;
        }
        timestamp_ms
    }

    fn is_duplicate(&mut self, identity: IdentityDigest, timestamp: i64, undo: &mut Undo) -> bool {
        if self.expired_on_arrival(timestamp) {
            return false;
        }
        let segment = segment_of(identity);
        let (watermark, window) = (self.watermark_ms, self.window_ms);
        if let Some(stored) = self.segments[segment].table.get_mut(&identity) {
            let previous = *stored;
            // An entry that is not live counts as absent, whether or not a
            // sweep has removed it yet, so sweep timing changes no decision.
            if is_live(watermark, window, previous) && within_window(window, timestamp, previous) {
                self.counters.dropped_records += 1;
                return true;
            }
            // Max-wins, as in `populate`. Only an entry that is not live gets
            // here, and a live timestamp is above it, so the record replaces it.
            if timestamp > previous {
                *stored = timestamp;
            }
            undo.entries.push(UndoEntry {
                identity,
                applied_timestamp: *stored,
                previous_timestamp: Some(previous),
            });
            return false;
        }
        // Before the insert, not after: `populate` sweeps at the same point,
        // and capacity trimming changes decisions, so the two orders must
        // match or replicas drift apart.
        self.sweep_step(SWEEP_BUDGET, SweepMode::ExpiredAndCapacity);
        if self.at_capacity(segment) && !self.make_room_for_one(segment) {
            // The bounded step freed nothing. The record is admitted, and its
            // identity is not remembered.
            self.counters.unindexed_records += 1;
            return false;
        }
        self.insert_new(segment, identity, timestamp);
        undo.entries.push(UndoEntry {
            identity,
            applied_timestamp: timestamp,
            previous_timestamp: None,
        });
        false
    }

    // Moves the watermark to the broker's time. It never moves back, so a
    // clock that steps back cannot make an expired entry live again.
    fn advance_watermark(&mut self, now_ms: i64) {
        self.watermark_ms = self.watermark_ms.max(now_ms);
    }

    // Whether an entry with `timestamp` can still match a record.
    fn is_live(&self, timestamp: i64) -> bool {
        is_live(self.watermark_ms, self.window_ms, timestamp)
    }

    // Counts a record whose clamped timestamp is already outside the window
    // of the watermark. It can match no entry, and no later record can match
    // it, so both paths admit it without indexing it and without a sweep. An
    // index rebuilt from the log at this time would not hold it either: the
    // rebuild starts one window before the broker's time.
    fn expired_on_arrival(&mut self, timestamp: i64) -> bool {
        if self.is_live(timestamp) {
            return false;
        }
        self.counters.expired_on_arrival_records += 1;
        true
    }

    // Inserts an identity that no segment holds.
    fn insert_new(&mut self, segment: usize, identity: IdentityDigest, timestamp: i64) {
        let relocated = self.segments[segment].insert_new(identity, timestamp);
        self.len += 1;
        self.occupied |= 1 << segment;
        self.largest_relocation = self.largest_relocation.max(relocated);
        self.sync_budget();
    }

    // Moves the cursor over at most `budget` entries. It drops the ones that
    // are not live and, in `ExpiredAndCapacity` mode near a limit, the ones
    // below the capacity cutoff. The cursor stays between calls and wraps
    // after the last segment, so successive calls walk the whole index in
    // bounded slices. This is the only eviction that `classify`, `populate`
    // and `tick` do.
    //
    // One call visits at most as many entries as the index holds. An index
    // smaller than the budget would otherwise be lapped several times in one
    // call, and every lap closes a pass and rebuilds its histogram: a new
    // identity on a near-empty partition at its limit cost 57 times one on an
    // idle index. A second visit to the same entry in one call learns nothing.
    fn sweep_step(&mut self, budget: usize, mode: SweepMode) {
        for _ in 0..budget.min(self.len) {
            if self.len == 0 {
                self.sweep_segment = 0;
                self.sweep_pos = 0;
                return;
            }
            if self.sweep_pos >= self.segments[self.sweep_segment].table.len() {
                // A removal elsewhere can also leave the cursor past the end
                // of its segment. The cursor then moves on early, which a
                // soft target can accept, and it stays a function of the call
                // sequence.
                self.advance_segment();
            }
            let segment = self.sweep_segment;
            let Some((_, &stamp)) = self.segments[segment].table.get_index(self.sweep_pos) else {
                return;
            };
            self.sweep_visits += 1;
            if !self.is_live(stamp) {
                // Expired: it cannot match again, so dropping it costs no
                // coverage. The cursor stays, because the removal moved the
                // last entry of the segment, not yet visited, into this slot.
                self.remove_at(segment, self.sweep_pos);
                continue;
            }
            if mode == SweepMode::ExpiredAndCapacity
                && stamp < self.capacity_cutoff
                && self.over_soft_limit()
            {
                // Inside the window, so this is coverage lost. Doing it a few
                // entries at a time from the soft limit up keeps the index from
                // dropping a quarter of itself in one step at the limit.
                self.remove_at(segment, self.sweep_pos);
                self.counters.capacity_evicted_entries += 1;
                continue;
            }
            // The frame is one pass old, and timestamps arrive out of order,
            // so an offset outside the frame is possible.
            let offset = stamp.saturating_sub(self.pass_lo).clamp(0, self.pass_span);
            let bucket = usize::try_from(offset / self.pass_width)
                .unwrap_or(CAPACITY_EVICT_BUCKETS - 1)
                .min(CAPACITY_EVICT_BUCKETS - 1);
            self.pass_counts[bucket] += 1;
            self.pass_visited += 1;
            self.sweep_pos += 1;
        }
    }

    // Moves the cursor to the start of the next segment that holds an entry.
    // Past the last one, the cursor wraps, and its histogram now covers the
    // index: it becomes the next cutoff. The occupied mask makes this constant
    // work however many segments are empty, so a sparse index costs no more
    // per call than a dense one. The caller makes sure an entry exists.
    fn advance_segment(&mut self) {
        let after = u32::try_from(self.sweep_segment + 1).unwrap_or(u32::MAX);
        let later = self.occupied & u64::MAX.checked_shl(after).unwrap_or(0);
        let next = if later == 0 {
            self.finish_pass();
            self.occupied.trailing_zeros()
        } else {
            later.trailing_zeros()
        };
        self.sweep_segment = usize::try_from(next).expect("a bit index of a u64 fits in usize");
        self.sweep_pos = 0;
    }

    // One bounded extra sweep, because a new identity arrived with the index,
    // or its segment, at a limit. Never a scan: if it frees nothing, the
    // caller admits the record unindexed, at a cost that does not depend on
    // the index size. The cursor advances either way, so later attempts walk
    // the index and complete a pass, which computes a new cutoff.
    fn make_room_for_one(&mut self, segment: usize) -> bool {
        self.counters.capacity_sweeps += 1;
        self.sweep_step(CAPACITY_EMERGENCY_BUDGET, SweepMode::ExpiredAndCapacity);
        !self.at_capacity(segment)
    }

    // Freezes the frame that a pass measures entries against, and clears the
    // histogram. Rounding the width up keeps `offset / width` below
    // CAPACITY_EVICT_BUCKETS for every offset in [0, span]. Dividing, not
    // multiplying, avoids an overflow for a window near `i64::MAX`.
    fn begin_pass(&mut self) {
        self.pass_counts.fill(0);
        self.pass_visited = 0;
        self.pass_span = self.window_ms.max(0);
        self.pass_lo = self.watermark_ms.saturating_sub(self.pass_span);
        self.pass_width = self.pass_span / LAST_BUCKET + 1;
    }

    // Turns the histogram of a completed pass into the next capacity cutoff,
    // and starts the next pass. Whole buckets are taken only while they stay
    // under the target, so the cutoff never marks more than a quarter.
    fn finish_pass(&mut self) {
        let target = self.pass_visited / CAPACITY_EVICT_DIVISOR;
        let mut accumulated = 0;
        let mut bucket = 0;
        while bucket < CAPACITY_EVICT_BUCKETS && accumulated + self.pass_counts[bucket] <= target {
            accumulated += self.pass_counts[bucket];
            bucket += 1;
        }
        self.capacity_cutoff = if accumulated == 0 {
            i64::MIN
        } else {
            let buckets = i64::try_from(bucket).unwrap_or(i64::MAX);
            self.pass_lo
                .saturating_add(buckets.saturating_mul(self.pass_width))
        };
        self.begin_pass();
    }

    // Removes the entry at `index` of `segment`. The segment moves its last
    // entry into the slot, so a cursor at `index` must look at the same index
    // again.
    fn remove_at(&mut self, segment: usize, index: usize) {
        self.segments[segment].table.swap_remove_index(index);
        self.after_removal(segment);
    }

    // Accounts for one entry removed from `segment`, and shrinks the segment
    // when it is a quarter full.
    fn after_removal(&mut self, segment: usize) {
        self.len -= 1;
        let relocated = self.segments[segment].shrink_if_sparse(self.shrink_floor);
        self.largest_relocation = self.largest_relocation.max(relocated);
        if self.segments[segment].table.is_empty() {
            self.occupied &= !(1 << segment);
        }
        self.sync_budget();
    }

    // Reports the size change since the last call into the budget. Called
    // after every insert or removal, so this filter's share is exact when a
    // capacity check reads it.
    fn sync_budget(&mut self) {
        let Some(budget) = &self.budget else {
            return;
        };
        let size = self.len;
        if size > self.budget_accounted {
            budget.add(size - self.budget_accounted);
        } else {
            budget.sub(self.budget_accounted - size);
        }
        self.budget_accounted = size;
    }

    // Whether a new identity for `segment` would exceed this partition's
    // limit, the segment's, or the broker's.
    fn at_capacity(&self, segment: usize) -> bool {
        self.len >= self.max_entries
            || self.segments[segment].table.len() >= self.segment_limit
            || self
                .budget
                .as_deref()
                .is_some_and(|budget| budget.entries() >= budget.max_entries())
    }

    // Whether the cursor drops entries below the capacity cutoff: this index
    // is within a tenth of its own limit, or the broker is within a tenth of
    // its limit.
    fn over_soft_limit(&self) -> bool {
        self.len >= soft_limit_of(self.max_entries)
            || self
                .budget
                .as_deref()
                .is_some_and(|budget| budget.entries() >= soft_limit_of(budget.max_entries()))
    }
}

impl Drop for DedupWindowFilter {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            budget.sub(self.budget_accounted);
        }
    }
}

impl fmt::Debug for DedupWindowFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DedupWindowFilter")
            .field("window_ms", &self.window_ms)
            .field("max_entries", &self.max_entries)
            .field("len", &self.len)
            .field("watermark_ms", &self.watermark_ms)
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

fn empty_segments() -> Box<[Segment]> {
    (0..DedupWindowFilter::SEGMENTS)
        .map(|_| Segment::default())
        .collect()
}

// The segment of an identity. The in-segment hasher takes `lo`, so routing by
// the top bits of `hi` keeps the two independent: every segment gets an even
// share, and each segment's table still sees well-spread hashes.
fn segment_of(identity: IdentityDigest) -> usize {
    let index = identity
        .hi
        .checked_shr(u64::BITS - SEGMENT_BITS)
        .unwrap_or(0);
    usize::try_from(index).expect("a segment index is below SEGMENTS")
}

// Whether an entry with `timestamp` is live at `watermark_ms`: at most
// `window_ms` behind it. Exact in i128, so that extreme client timestamps and
// windows cannot wrap. A negative window holds nothing live.
fn is_live(watermark_ms: i64, window_ms: i64, timestamp: i64) -> bool {
    i128::from(watermark_ms) - i128::from(timestamp) <= i128::from(window_ms)
}

// Whether two record timestamps are at most `window_ms` apart, in either
// order. Exact in i128, as `is_live` is.
fn within_window(window_ms: i64, first: i64, second: i64) -> bool {
    (i128::from(first) - i128::from(second)).abs() <= i128::from(window_ms)
}

// Twice the even share of the entry limit, and at least the floor.
fn segment_limit_of(max_entries: usize) -> usize {
    max_entries
        .div_ceil(DedupWindowFilter::SEGMENTS)
        .saturating_mul(SEGMENT_LIMIT_FACTOR)
        .max(SEGMENT_LIMIT_FLOOR)
}

// The heap of one segment with room for `capacity` entries: its dense array,
// and its hash table. hashbrown fills at most 7/8 of its buckets and rounds
// the bucket count up to a power of two, at least 4.
fn segment_heap_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = (capacity.saturating_mul(8) / 7).next_power_of_two().max(4);
    capacity * ENTRY_BYTES + buckets * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES
}

// A tenth below the limit, and at least one entry below it, so a very small
// limit still leaves the cursor a margin to trim in.
fn soft_limit_of(limit: usize) -> usize {
    limit - limit.min((limit / CAPACITY_SOFT_MARGIN_DIVISOR).max(1))
}
