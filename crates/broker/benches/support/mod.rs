//! Batch shapes and measurement controls shared by the leader and follower benchmarks.

pub const SHAPES: [(&str, i32, usize); 3] = [
    ("1rec_100KiB", 1, 100 * 1024),
    ("100rec_1KiB", 100, 1024),
    ("1000rec_100B", 1000, 100),
];

/// Reset the log outside the timed region after this many bytes.
pub const LOG_BUDGET: usize = 256 * 1024 * 1024;
/// Epoch carried by the benchmark's replicated batches.
pub const LEADER_EPOCH: i32 = 3;
/// Untimed iterations that fault in the fixture before measuring it.
pub const WARMUP: u64 = 8;
