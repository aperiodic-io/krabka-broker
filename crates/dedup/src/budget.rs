use std::sync::atomic::{AtomicUsize, Ordering};

/// The entry count shared by every dedup index on one broker.
///
/// The per-partition entry limit does not bound a broker: partitions are placed by count, not by index size, so several full indices can share one broker's memory. Every filter holds the same budget through an `Arc` and reports its own size changes into it. A filter that inserts while the broker total is near the limit trims its own oldest entries, as it does near its own limit.
///
/// Each filter keeps its own contribution exact. A read from another thread can see a total that is briefly out of date, which only moves the point where trimming starts.
#[derive(Debug)]
pub struct DedupBudget {
    entries: AtomicUsize,
    max_entries: usize,
}

impl DedupBudget {
    /// Makes an empty budget that allows `max_entries` entries in total.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: AtomicUsize::new(0),
            max_entries,
        }
    }

    /// The entries that every filter on this budget holds now.
    #[must_use]
    pub fn entries(&self) -> usize { self.entries.load(Ordering::Relaxed) }

    /// The total entry limit.
    #[must_use]
    pub fn max_entries(&self) -> usize { self.max_entries }

    pub(crate) fn add(&self, entries: usize) { self.entries.fetch_add(entries, Ordering::Relaxed); }

    pub(crate) fn sub(&self, entries: usize) { self.entries.fetch_sub(entries, Ordering::Relaxed); }
}
