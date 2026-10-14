// What the filter's two test modules share: the state the segments must agree
// on after every call, and the random operation model that both property tests
// drive several filters on one budget with. One copy keeps the two property
// tests checking the same bounds.

use {
    super::*,
    assert2::assert,
    proptest::{prelude::*, strategy::Union},
    std::ops::Range,
};

// The most entries the cursor visits in one call for one record.
pub(super) const VISITS_PER_CALL: u64 = (SWEEP_BUDGET + CAPACITY_EMERGENCY_BUDGET) as u64;

// The most entries the cursor visits in one tick.
pub(super) const VISITS_PER_TICK: u64 = DedupWindowFilter::TICK_VISITS as u64;

// How many filters share the budget in a run of random operations.
pub(super) const FILTERS: usize = 3;

pub(super) fn nth(index: u64) -> IdentityDigest { IdentityDigest::of(&index.to_le_bytes()) }

// The state the segments must agree on after every call.
pub(super) fn check_invariants(filter: &DedupWindowFilter) {
    let mut total = 0;
    let mut occupied = 0_u64;
    for (index, segment) in filter.segments.iter().enumerate() {
        total += segment.table.len();
        if !segment.table.is_empty() {
            occupied |= 1 << index;
        }
        assert!(
            segment.table.len() <= filter.segment_limit,
            "segment {index}"
        );
        // The usable slots never exceed the allocation the segment records.
        assert!(
            segment.table.capacity() <= segment.capacity,
            "segment {index}"
        );
    }
    assert!(total == filter.len());
    assert!(occupied == filter.occupied);
    assert!(filter.len() <= filter.max_entries());
    assert!(filter.largest_relocation() <= filter.segment_limit);
    assert!(filter.sweep_segment < DedupWindowFilter::SEGMENTS);
    // Every timestamp was clamped to the time of its call, and the watermark
    // never moves back, so no entry is ahead of it.
    for (identity, timestamp) in filter.iter() {
        assert!(timestamp <= filter.watermark_ms(), "{identity:?}");
    }
}

#[derive(Clone, Debug)]
pub(super) enum Op {
    Classify {
        filter: usize,
        key: u16,
        advance: i64,
        keep_undo: bool,
    },
    Populate {
        filter: usize,
        key: u16,
        advance: i64,
    },
    // A jump of the clock with no record: the next record then expires a
    // wave of entries, or arrives behind the stored ones.
    Jump {
        advance: i64,
    },
    // A tick of one filter with no record, after a step of the clock.
    Tick {
        filter: usize,
        advance: i64,
    },
    Revert,
    Clear {
        filter: usize,
    },
    Replace {
        filter: usize,
    },
}

// The shape of the random operations one property test draws.
pub(super) struct OpSpace {
    // Keys are drawn below this, so a smaller space repeats identities more.
    pub(super) keys: u16,
    // The clock step a record or a tick brings with it.
    pub(super) advance: Range<i64>,
    // The clock step of a jump, or `None` for a run without jumps.
    pub(super) jump: Option<Range<i64>>,
    pub(super) revert_weight: u32,
}

impl OpSpace {
    pub(super) fn strategy(self) -> impl Strategy<Value = Op> {
        let OpSpace {
            keys,
            advance,
            jump,
            revert_weight,
        } = self;
        let mut options = vec![
            (
                8,
                (0..FILTERS, 0..keys, advance.clone(), any::<bool>())
                    .prop_map(|(filter, key, advance, keep_undo)| Op::Classify {
                        filter,
                        key,
                        advance,
                        keep_undo,
                    })
                    .boxed(),
            ),
            (
                8,
                (0..FILTERS, 0..keys, advance.clone())
                    .prop_map(|(filter, key, advance)| Op::Populate {
                        filter,
                        key,
                        advance,
                    })
                    .boxed(),
            ),
            (
                2,
                (0..FILTERS, advance)
                    .prop_map(|(filter, advance)| Op::Tick { filter, advance })
                    .boxed(),
            ),
        ];
        // A union refuses an option of weight zero, so a run without jumps
        // leaves the option out instead.
        if let Some(jump) = jump {
            options.push((1, jump.prop_map(|advance| Op::Jump { advance }).boxed()));
        }
        options.extend([
            (revert_weight, Just(Op::Revert).boxed()),
            (
                1,
                (0..FILTERS).prop_map(|filter| Op::Clear { filter }).boxed(),
            ),
            (
                1,
                (0..FILTERS)
                    .prop_map(|filter| Op::Replace { filter })
                    .boxed(),
            ),
        ]);
        Union::new_weighted(options)
    }
}

// A run of random operations against `FILTERS` filters that share one budget.
// The run's clock is the broker's time, and each record carries it as its
// timestamp, so a step back of the clock brings records from behind the
// watermark.
pub(super) struct OpRun {
    pub(super) budget: usize,
    pub(super) start_ms: i64,
    // Builds each filter, at the start and on `Op::Replace`.
    pub(super) filter: fn(&Arc<DedupBudget>) -> DedupWindowFilter,
    pub(super) identity: fn(u16) -> IdentityDigest,
}

impl OpRun {
    // Applies `ops` in order. After each one the budget equals the sum of the
    // filters' sizes and stays within its limit, every filter keeps its
    // invariants, and no call or tick visits more than its share of entries. When the
    // filters go, the budget drops to zero.
    pub(super) fn run(&self, ops: Vec<Op>) {
        let shared = Arc::new(DedupBudget::new(self.budget));
        let mut filters: Vec<DedupWindowFilter> =
            (0..FILTERS).map(|_| (self.filter)(&shared)).collect();
        let mut clock = self.start_ms;
        let mut pending: Vec<(usize, Undo)> = Vec::new();
        for op in ops {
            match op {
                Op::Classify {
                    filter,
                    key,
                    advance,
                    keep_undo,
                } => {
                    clock += advance;
                    let record = DedupRecord {
                        identity: Some((self.identity)(key)),
                        timestamp_ms: clock,
                    };
                    let before = filters[filter].sweep_visits();
                    let result = filters[filter].classify(&[record], clock);
                    assert!(filters[filter].sweep_visits() - before <= VISITS_PER_CALL);
                    if keep_undo {
                        pending.push((filter, result.undo));
                    }
                }
                Op::Populate {
                    filter,
                    key,
                    advance,
                } => {
                    clock += advance;
                    let before = filters[filter].sweep_visits();
                    filters[filter].populate((self.identity)(key), clock, clock);
                    assert!(filters[filter].sweep_visits() - before <= VISITS_PER_CALL);
                }
                Op::Jump { advance } => clock += advance,
                Op::Tick { filter, advance } => {
                    clock += advance;
                    let before = filters[filter].sweep_visits();
                    filters[filter].tick(clock);
                    assert!(filters[filter].sweep_visits() - before <= VISITS_PER_TICK);
                }
                Op::Revert => {
                    if let Some((filter, undo)) = pending.pop() {
                        filters[filter].revert(&undo);
                    }
                }
                Op::Clear { filter } => filters[filter].clear(),
                Op::Replace { filter } => filters[filter] = (self.filter)(&shared),
            }
            let total: usize = filters.iter().map(DedupWindowFilter::len).sum();
            assert!(shared.entries() == total);
            assert!(shared.entries() <= shared.max_entries());
            for filter in &filters {
                check_invariants(filter);
            }
        }
        drop(filters);
        assert!(shared.entries() == 0);
    }
}
