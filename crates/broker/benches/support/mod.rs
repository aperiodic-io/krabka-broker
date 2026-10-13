//! Batch shapes and measurement controls shared by the leader and follower benchmarks.

pub(crate) struct BoundedResource<T> {
    value: T,
    written: usize,
    budget: usize,
    factory: fn() -> T,
}

#[bon::builder]
pub(crate) fn bounded_resource<T>(budget: usize, factory: fn() -> T) -> BoundedResource<T> {
    BoundedResource {
        value: factory(),
        written: 0,
        budget,
        factory,
    }
}

impl<T> BoundedResource<T> {
    /// Replace the resource when the next write would exceed its byte budget.
    pub(crate) fn rotate_if_full(&mut self, incoming: usize) -> &mut T {
        if self.written + incoming > self.budget {
            self.value = (self.factory)();
            self.written = 0;
        }
        self.written += incoming;
        &mut self.value
    }

    #[allow(dead_code)]
    pub(crate) fn get_mut(&mut self) -> &mut T { &mut self.value }
}

/// One batch shape a benchmark sweeps: its report label, record count and
/// per-record payload size.
pub struct BatchShape {
    pub label: &'static str,
    pub records: i32,
    pub payload: usize,
}

pub const SHAPES: [BatchShape; 3] = [
    BatchShape {
        label: "1rec_100KiB",
        records: 1,
        payload: 100 * 1024,
    },
    BatchShape {
        label: "100rec_1KiB",
        records: 100,
        payload: 1024,
    },
    BatchShape {
        label: "1000rec_100B",
        records: 1000,
        payload: 100,
    },
];

/// Reset the log outside the timed region after this many bytes.
pub const LOG_BUDGET: usize = 256 * 1024 * 1024;
/// Epoch carried by the benchmark's replicated batches.
pub const LEADER_EPOCH: i32 = 3;
/// Untimed iterations that fault in the fixture before measuring it.
pub const WARMUP: u64 = 8;
