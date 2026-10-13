use {
    crate::{
        budget::DedupBudget,
        digest::{DigestBuildHasher, IdentityDigest},
    },
    indexmap::IndexMap,
    std::{fmt, sync::Arc},
};

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
/// The highest timestamp seen is a client-supplied `CreateTime` with no order guarantee, so each record timestamp is clamped to the broker's time before it reaches the index. Without the clamp, one future-dated record would move the expiry cutoff past the whole index and disable dedup for the partition. The clamp does not bound reordering within the window, so eviction at the window edge stays approximate.
pub struct DedupWindowFilter {
    window_ms: i64,
    max_entries: usize,
    budget: Option<Arc<DedupBudget>>,
    // This filter's contribution to the budget's entry count.
    budget_accounted: usize,
    // A dense array of entries in insertion order with a hash index over it.
    // A removal moves the last entry into the freed slot, which the cursor
    // relies on.
    map: IndexMap<IdentityDigest, i64, DigestBuildHasher>,
    // The highest record timestamp seen: the reference "now" for expiry.
    max_ts: i64,
    // A position in the map's dense array. Replicas agree on it because they
    // insert the same identities in the same order and so lay out the array
    // the same way. `revert` is the exception: it removes entries a follower
    // never inserted, which permutes the leader's array. The two then sweep
    // equal sets in different orders and can drop different live entries for
    // capacity. That needs the index above 90% of its limit, and which live
    // entry goes is already best-effort, but it is inherent to bounded work:
    // a cursor is a position in a layout.
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

    /// Makes an empty filter.
    ///
    /// `max_entries` is the per-partition entry limit. A filter with a `budget` also keeps the sum over every filter on that budget under the budget's limit. A negative `window_ms` makes no record a duplicate.
    #[must_use]
    pub fn new(window_ms: i64, max_entries: usize, budget: Option<Arc<DedupBudget>>) -> Self {
        let mut filter = Self {
            window_ms,
            max_entries,
            budget,
            budget_accounted: 0,
            map: IndexMap::default(),
            max_ts: i64::MIN,
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
            let Some(timestamp) = self.map.get_mut(&entry.identity) else {
                continue;
            };
            if *timestamp != entry.applied_timestamp {
                continue;
            }
            match entry.previous_timestamp {
                Some(previous) => *timestamp = previous,
                None => {
                    self.map.swap_remove(&entry.identity);
                }
            }
        }
        self.sync_budget();
    }

    /// Records an identity that the log admitted.
    ///
    /// The index keeps the highest timestamp per identity, so applying one log prefix any number of times gives the same index. The timestamp is clamped to `now_ms` first, which changes nothing for a record behind the broker's time, so log replay stays deterministic.
    pub fn populate(&mut self, identity: IdentityDigest, timestamp_ms: i64, now_ms: i64) {
        let timestamp = self.clamp_to_now(timestamp_ms, now_ms);
        self.max_ts = self.max_ts.max(timestamp);
        // Followers apply the leader's capacity rule at the same point as
        // `is_duplicate` does: look up, sweep, then insert. Expiry timing never
        // changes a decision, because an expired entry cannot match. Capacity
        // trimming drops entries that could still match, so the position of
        // the sweep relative to the insert decides what the index holds.
        if let Some(stored) = self.map.get_mut(&identity) {
            if timestamp > *stored {
                *stored = timestamp;
            }
            return;
        }
        self.sweep_step(SWEEP_BUDGET);
        if self.at_capacity() && !self.make_room_for_one() {
            self.counters.unindexed_records += 1;
            return;
        }
        self.map.insert(identity, timestamp);
        self.sync_budget();
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
    /// The cursor, its histogram, and the highest timestamp seen reset as well. The counters stay.
    pub fn clear(&mut self) {
        // A new map, not `IndexMap::clear`, which keeps the capacity it grew
        // to: tens of MiB at the default limit. Every caller wants it back.
        self.map = IndexMap::default();
        self.sync_budget();
        self.max_ts = i64::MIN;
        self.sweep_pos = 0;
        self.capacity_cutoff = i64::MIN;
        self.begin_pass();
    }

    /// The number of indexed identities.
    #[must_use]
    pub fn len(&self) -> usize { self.map.len() }

    /// Whether no identity is indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.map.is_empty() }

    /// The per-partition entry limit.
    #[must_use]
    pub fn max_entries(&self) -> usize { self.max_entries }

    /// The highest clamped record timestamp seen, or `i64::MIN` when none was.
    #[must_use]
    pub fn max_timestamp_ms(&self) -> i64 { self.max_ts }

    /// The stored timestamp of `identity`, or `None` when it is not indexed.
    #[must_use]
    pub fn timestamp_of(&self, identity: IdentityDigest) -> Option<i64> {
        self.map.get(&identity).copied()
    }

    /// The indexed identities and their timestamps, in the dense order the sweep cursor walks.
    pub fn iter(&self) -> impl Iterator<Item = (IdentityDigest, i64)> + '_ {
        self.map
            .iter()
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

    /// The broker-wide budget this filter reports into, if it has one.
    #[must_use]
    pub fn budget(&self) -> Option<&DedupBudget> { self.budget.as_deref() }

    #[cfg(test)]
    fn map_capacity(&self) -> usize { self.map.capacity() }

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
        if let Some(stored) = self.map.get_mut(&identity) {
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
        if self.at_capacity() && !self.make_room_for_one() {
            // The bounded step freed nothing. The record is admitted, and its
            // identity is not remembered.
            self.counters.unindexed_records += 1;
            return false;
        }
        self.map.insert(identity, timestamp);
        self.sync_budget();
        undo.entries.push(UndoEntry {
            identity,
            applied_timestamp: timestamp,
            previous_timestamp: None,
        });
        false
    }

    // Moves the cursor over at most `budget` entries. It drops the expired
    // ones and, near a limit, the ones below the capacity cutoff. The cursor
    // stays between calls and wraps at the end of the array, so successive
    // calls walk the whole index in bounded slices. This is the only eviction
    // that `classify` and `populate` do.
    fn sweep_step(&mut self, budget: usize) {
        let expiry_cutoff = self.max_ts.saturating_sub(self.window_ms);
        for _ in 0..budget {
            if self.map.is_empty() {
                self.sweep_pos = 0;
                return;
            }
            if self.sweep_pos >= self.map.len() {
                // The cursor reached the end, so its histogram now covers the
                // index: make it the next cutoff, and wrap. A removal elsewhere
                // can also leave the cursor past the end. That ends the pass
                // early on a partial histogram, which a soft target can
                // accept, and it stays a function of the call sequence.
                self.sweep_pos = 0;
                self.finish_pass();
            }
            let Some((_, &stamp)) = self.map.get_index(self.sweep_pos) else {
                return;
            };
            self.sweep_visits += 1;
            if stamp < expiry_cutoff {
                // Expired: it cannot match again, so dropping it costs no
                // coverage. The cursor stays, because the removal moved the
                // last entry, not yet visited, into this slot.
                self.remove_at(self.sweep_pos);
                continue;
            }
            if stamp < self.capacity_cutoff && self.over_soft_limit() {
                // Inside the window, so this is coverage lost. Doing it a few
                // entries at a time from the soft limit up keeps the index from
                // dropping a quarter of itself in one step at the limit.
                self.remove_at(self.sweep_pos);
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

    // One bounded extra sweep, because a new identity arrived with the index
    // at a limit. Never a scan: if it frees nothing, the caller admits the
    // record unindexed, at a cost that does not depend on the index size. The
    // cursor advances either way, so later attempts walk the index and
    // complete a pass, which computes a new cutoff.
    fn make_room_for_one(&mut self) -> bool {
        self.counters.capacity_sweeps += 1;
        self.sweep_step(CAPACITY_EMERGENCY_BUDGET);
        !self.at_capacity()
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

    // Removes the entry at `index`. The map moves its last entry into the
    // slot, so a cursor at `index` must look at the same index again.
    fn remove_at(&mut self, index: usize) {
        self.map.swap_remove_index(index);
        self.sync_budget();
    }

    // Reports the size change since the last call into the budget. Called
    // after every insert or removal, so this filter's share is exact when a
    // capacity check reads it.
    fn sync_budget(&mut self) {
        let Some(budget) = &self.budget else {
            return;
        };
        let size = self.map.len();
        if size > self.budget_accounted {
            budget.add(size - self.budget_accounted);
        } else {
            budget.sub(self.budget_accounted - size);
        }
        self.budget_accounted = size;
    }

    // Whether a new identity would exceed this partition's limit or the
    // broker's.
    fn at_capacity(&self) -> bool {
        self.map.len() >= self.max_entries
            || self
                .budget
                .as_deref()
                .is_some_and(|budget| budget.entries() >= budget.max_entries())
    }

    // Whether the cursor drops entries below the capacity cutoff: this index
    // is within a tenth of its own limit, or the broker is within a tenth of
    // its limit.
    fn over_soft_limit(&self) -> bool {
        self.map.len() >= soft_limit_of(self.max_entries)
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
            .field("len", &self.map.len())
            .field("max_timestamp_ms", &self.max_ts)
            .field("counters", &self.counters)
            .finish_non_exhaustive()
    }
}

// A tenth below the limit, and at least one entry below it, so a very small
// limit still leaves the cursor a margin to trim in.
fn soft_limit_of(limit: usize) -> usize {
    limit - limit.min((limit / CAPACITY_SOFT_MARGIN_DIVISOR).max(1))
}
