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
mod tests;

// Entries the cursor visits for each new identity. Eight per new identity
// sweeps the whole index once every `len / 8` new identities, spread evenly.
// One whole-index pass at a time would stall the thread that runs it, and
// sweeps happen at the same log position on every replica, so the leader and
// its followers would stall together and leave nobody to complete an acks=all
// quorum.
const SWEEP_BUDGET: usize = 8;

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
/// The filter can stop deduplicating without failing a request in two ways: through producer clock skew and through a full index. Both are counted, with the drop count that makes them readable.
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
    /// Requests where some, but not all, records were dropped. Only these make the caller rebuild and recompress a batch, so this rate drives the cost of dedup on the produce path.
    pub rebuilt_batches: u64,
}

/// The first-wins dedup index of one partition.
///
/// A record is dropped when an earlier record with the same identity was admitted, and the two timestamps are at most `window_ms` apart. The leader classifies produce requests with [`classify`](Self::classify) and undoes one with [`revert`](Self::revert) when its replication fails. Every replica, the leader included after a restart, rebuilds the index from the log with [`populate`](Self::populate).
///
/// Call it only for plain produce batches. Idempotent and transactional batches bypass dedup, so that their producer sequence numbers stay intact.
///
/// Memory is bounded in three ways. Each entry is a fixed-size [`IdentityDigest`]. Entries older than the window, relative to the highest timestamp seen, cannot cause a drop again, and a cursor that visits a few entries per new identity drops them. A hard entry limit, and an optional broker-wide [`DedupBudget`], stop a large window, many identities, or timestamps that do not advance from using all memory. From a soft limit a tenth below either limit, the same cursor also drops the entries that a completed pass found in the oldest quarter. The effective window then narrows to about the limit divided by the rate of new identities, and dedup does not stop for new identities.
///
/// The broker budget makes the effective capacity of a partition depend on the other partitions of its broker, and replicas of one partition seldom share a broker with the same set. Under broker pressure, replicas can therefore trim different amounts. This is the same best-effort divergence that capacity trimming already has.
///
/// No operation does work proportional to the index. This is a requirement: sweeps happen at the same log position on every replica, so a whole-index pass stalls the leader and then the followers together.
///
/// For the same reason, the index is [`SEGMENTS`](Self::SEGMENTS) hash tables and not one. The top bits of an identity's digest choose its segment. A segment that is full doubles its capacity, and a segment that falls to a quarter of its capacity, above a floor of 256 entries, shrinks to twice its length. Either step moves the entries of that one segment, and each segment holds at most `max(2 * ceil(max_entries / SEGMENTS), 4096)` entries, so no call moves more than that many. One table for the partition would move every entry at once: tens to hundreds of milliseconds at millions of entries. [`largest_relocation`](Self::largest_relocation) reports the largest move so far. Shrinking keeps the heap within a small factor of the live entries, plus the floor of each segment, after a burst expires. [`heap_bytes`](Self::heap_bytes) estimates it.
///
/// The per-segment limit binds only when identities crowd one segment. Well-spread digests stay far below it, but xxh3 does not resist chosen input, so a producer can choose identities that share a segment. A new identity for a full segment is then admitted unindexed and counted, as at the entry limit.
///
/// The highest timestamp seen is a client-supplied `CreateTime` with no order guarantee, so each record timestamp is clamped to the broker's time before it reaches the index. Without the clamp, one future-dated record would move the expiry cutoff past the whole index and disable dedup for the partition. The clamp does not bound reordering within the window, so eviction at the window edge stays approximate.
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
    // The highest record timestamp seen: the reference "now" for expiry.
    max_ts: i64,
    // A position in the segments' dense arrays: a segment, and an index in
    // it. Replicas agree on it because they insert the same identities in
    // the same order and so lay out the arrays the same way. `revert` is the
    // exception: it removes entries a follower never inserted, which permutes
    // the leader's arrays. The two then sweep equal sets in different orders
    // and can drop different live entries for capacity. That needs the index
    // above 90% of its limit, and which live entry goes is already
    // best-effort, but it is inherent to bounded work: a cursor is a position
    // in a layout.
    sweep_segment: usize,
    sweep_pos: usize,
    // The histogram of the entries the current pass visited, measured against
    // a frame frozen at the start of the pass. `max_ts` moves while the cursor
    // walks, and buckets that move under it would describe no single moment.
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

    /// Makes an empty filter.
    ///
    /// `max_entries` is the per-partition entry limit. A filter with a `budget` also keeps the sum over every filter on that budget under the budget's limit. A negative `window_ms` makes no record a duplicate.
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
            max_ts: i64::MIN,
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
    /// Classification is first-wins in record order, so two records with one identity in the same request resolve as they would across requests. Each timestamp is first clamped to `now_ms`, the broker's time. A duplicate does not change the index, so a request that is all duplicates changes nothing.
    pub fn classify(&mut self, records: &[DedupRecord], now_ms: i64) -> Classification {
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
    /// This is compare-and-revert: an entry is restored or removed only while the index still holds the timestamp this request wrote. An identity that a later request overwrote keeps the later state. Sweeps and the highest timestamp seen are not reverted: an expired entry cannot change a decision, and a higher watermark only makes expiry sooner.
    pub fn revert(&mut self, undo: &Undo) {
        // Only this request's own entries come back. A capacity eviction that
        // classifying it forced stays done, so a failed request can cost
        // coverage that was not its own. That is bounded by one slice.
        //
        // Reverse order, so that several changes to one identity in a request
        // unwind correctly: the last entry restores what the one before it
        // wrote, and the first restores the state before the request.
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
    /// The index keeps the highest timestamp per identity, so applying one log prefix any number of times gives the same index. The timestamp is clamped to `now_ms` first, which changes nothing for a record behind the broker's time, so log replay stays deterministic.
    pub fn populate(&mut self, identity: IdentityDigest, timestamp_ms: i64, now_ms: i64) {
        let timestamp = self.clamp_to_now(timestamp_ms, now_ms);
        self.max_ts = self.max_ts.max(timestamp);
        let segment = segment_of(identity);
        // Followers apply the leader's capacity rule at the same point as
        // `is_duplicate` does: look up, sweep, then insert. Expiry timing never
        // changes a decision, because an expired entry cannot match. Capacity
        // trimming drops entries that could still match, so the position of
        // the sweep relative to the insert decides what the index holds.
        if let Some(stored) = self.segments[segment].table.get_mut(&identity) {
            if timestamp > *stored {
                *stored = timestamp;
            }
            return;
        }
        self.sweep_step(SWEEP_BUDGET);
        if self.at_capacity(segment) && !self.make_room_for_one(segment) {
            self.counters.unindexed_records += 1;
            return;
        }
        self.insert_new(segment, identity, timestamp);
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
    /// The cursor, its histogram, and the highest timestamp seen reset as well. The counters, [`sweep_visits`](Self::sweep_visits) and [`largest_relocation`](Self::largest_relocation) stay.
    pub fn clear(&mut self) {
        // New segments, not `IndexMap::clear`, which keeps the capacity each
        // grew to: tens of MiB at the default limit. Every caller wants it
        // back.
        self.segments = empty_segments();
        self.len = 0;
        self.occupied = 0;
        self.sync_budget();
        self.max_ts = i64::MIN;
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

    /// The highest clamped record timestamp seen, or `i64::MIN` when none was.
    #[must_use]
    pub fn max_timestamp_ms(&self) -> i64 { self.max_ts }

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

    // Client `CreateTime` is untrusted. The broker admits records ahead of its
    // own clock, and `max_ts` is a running maximum, so one future-dated record
    // would push the expiry cutoff past every entry. The next sweeps would
    // empty the index, and each later entry would already be below the cutoff:
    // dedup would stay off for the partition.
    //
    // The clamp changes nothing for a record at or behind the broker's time,
    // which is every record seen during log replay, so the index stays a
    // function of the log and not of the replay timing. It applies to the
    // stored entry as well as the watermark: an entry stored ahead of the
    // broker's time would stay above the cutoff and never expire.
    fn clamp_to_now(&mut self, timestamp_ms: i64, now_ms: i64) -> i64 {
        if timestamp_ms > now_ms {
            self.counters.skew_clamped_records += 1;
            return now_ms;
        }
        timestamp_ms
    }

    fn is_duplicate(&mut self, identity: IdentityDigest, timestamp: i64, undo: &mut Undo) -> bool {
        let segment = segment_of(identity);
        if let Some(stored) = self.segments[segment].table.get_mut(&identity) {
            let previous = *stored;
            // Exact in i128, so that extreme client timestamps cannot wrap.
            let distance = (i128::from(timestamp) - i128::from(previous)).abs();
            if distance <= i128::from(self.window_ms) {
                self.counters.dropped_records += 1;
                return true;
            }
            // Max-wins, as in `populate`: a classification that arrives out of
            // order must not lower the stored timestamp below what the
            // follower's apply of the same log computes.
            if timestamp > previous {
                *stored = timestamp;
            }
            let applied = *stored;
            self.max_ts = self.max_ts.max(timestamp);
            undo.entries.push(UndoEntry {
                identity,
                applied_timestamp: applied,
                previous_timestamp: Some(previous),
            });
            return false;
        }
        self.max_ts = self.max_ts.max(timestamp);
        // Before the insert, not after: `populate` sweeps at the same point,
        // and capacity trimming changes decisions, so the two orders must
        // match or replicas drift apart.
        self.sweep_step(SWEEP_BUDGET);
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

    // Inserts an identity that no segment holds.
    fn insert_new(&mut self, segment: usize, identity: IdentityDigest, timestamp: i64) {
        let relocated = self.segments[segment].insert_new(identity, timestamp);
        self.len += 1;
        self.occupied |= 1 << segment;
        self.largest_relocation = self.largest_relocation.max(relocated);
        self.sync_budget();
    }

    // Moves the cursor over at most `budget` entries. It drops the expired
    // ones and, near a limit, the ones below the capacity cutoff. The cursor
    // stays between calls and wraps after the last segment, so successive
    // calls walk the whole index in bounded slices. This is the only eviction
    // that `classify` and `populate` do.
    fn sweep_step(&mut self, budget: usize) {
        let expiry_cutoff = self.max_ts.saturating_sub(self.window_ms);
        for _ in 0..budget {
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
            if stamp < expiry_cutoff {
                // Expired: it cannot match again, so dropping it costs no
                // coverage. The cursor stays, because the removal moved the
                // last entry of the segment, not yet visited, into this slot.
                self.remove_at(segment, self.sweep_pos);
                continue;
            }
            if stamp < self.capacity_cutoff && self.over_soft_limit() {
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
        self.sweep_step(CAPACITY_EMERGENCY_BUDGET);
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
        self.pass_lo = self.max_ts.saturating_sub(self.pass_span);
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
            .field("max_timestamp_ms", &self.max_ts)
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
