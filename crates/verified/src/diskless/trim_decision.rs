use {
    super::{DisklessBatchStep, DisklessTrimDecision},
    creusot_std::prelude::*,
};

/// Extend an object byte span only across a contiguous whole indexed range.
///
/// The span extends exactly when the next range sits in the same object,
/// starts where the span ends, and the grown span stays within `max_bytes`.
#[ensures(match result {
    Some(total) => same_object
        && current_start@ + current_len@ == next_start@
        && total@ == current_len@ + next_len@
        && total@ <= max_bytes@,
    None => !same_object
        || current_start@ + current_len@ != next_start@
        || current_len@ + next_len@ > max_bytes@,
})]
#[must_use]
pub fn diskless_span_extension(
    current_start: u64,
    current_len: u64,
    next_start: u64,
    next_len: u64,
    same_object: bool,
    max_bytes: u64,
) -> Option<u64> {
    if !same_object || current_start.checked_add(current_len) != Some(next_start) {
        return None;
    }
    let total = current_len.checked_add(next_len)?;
    (total <= max_bytes).then_some(total)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn batch_step_valid(
    selected_start: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base_offset: i64,
    last_offset_delta: i32,
) -> bool {
    pearlite! {
        encoded_len@ > 0
            && last_offset_delta@ >= 0
            && batch_start@ + encoded_len@ <= usize::MAX@
            && base_offset@ + last_offset_delta@ <= i64::MAX@
            && match selected_start {
                Some(start) => start@ <= batch_start@,
                None => true,
            }
    }
}

/// Classify one decoded batch without splitting it or overflowing coordinates.
///
/// A batch is `Invalid` exactly when it is empty, has a negative last offset
/// delta, ends past `usize` or `i64`, or starts before the selected run. Every
/// valid batch lands in exactly one of the other four steps, so each valid
/// input advances the read by the batch's encoded length or stops it.
#[ensures((result == DisklessBatchStep::Invalid) == !batch_step_valid(
    selected_start,
    batch_start,
    encoded_len,
    base_offset,
    last_offset_delta,
))]
#[ensures(match result {
    DisklessBatchStep::Skip(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && base_offset@ + last_offset_delta@ < floor@,
    DisklessBatchStep::Start(next) => selected_start == None
        && next@ == batch_start@ + encoded_len@
        && floor@ <= base_offset@ + last_offset_delta@,
    DisklessBatchStep::Continue(next) => match selected_start {
        Some(start) => start@ <= batch_start@
            && next@ == batch_start@ + encoded_len@
            && next@ - start@ <= max_bytes@,
        None => false,
    },
    DisklessBatchStep::Stop => match selected_start {
        Some(start) => start@ <= batch_start@
            && batch_start@ + encoded_len@ - start@ > max_bytes@,
        None => false,
    },
    // Pinned by the `Invalid` iff above.
    DisklessBatchStep::Invalid => true,
})]
#[must_use]
pub fn diskless_batch_step(
    selected_start: Option<usize>,
    batch_start: usize,
    encoded_len: usize,
    base_offset: i64,
    last_offset_delta: i32,
    floor: i64,
    max_bytes: usize,
) -> DisklessBatchStep {
    if encoded_len == 0 || last_offset_delta < 0 {
        return DisklessBatchStep::Invalid;
    }
    let Some(next) = batch_start.checked_add(encoded_len) else {
        return DisklessBatchStep::Invalid;
    };
    let Some(last_offset) = base_offset.checked_add(i64::from(last_offset_delta)) else {
        return DisklessBatchStep::Invalid;
    };
    if let Some(start) = selected_start {
        if start > batch_start {
            return DisklessBatchStep::Invalid;
        }
        if next - start > max_bytes {
            DisklessBatchStep::Stop
        } else {
            DisklessBatchStep::Continue(next)
        }
    } else if last_offset < floor {
        DisklessBatchStep::Skip(next)
    } else {
        DisklessBatchStep::Start(next)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn effective_trim_lag(safety_lag: i64) -> Int {
    pearlite! { if safety_lag@ < 0 { 0 } else { safety_lag@ } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn trim_target(frontier: i64, high_watermark: i64, safety_lag: i64) -> Int {
    pearlite! {
        let high_watermark_floor = high_watermark@ - effective_trim_lag(safety_lag);
        if frontier@ < high_watermark_floor { frontier@ } else { high_watermark_floor }
    }
}

/// Plan a local trim behind both the committed object-store frontier and the
/// high watermark's configured safety lag.
///
/// Negative offsets and a lag larger than the high watermark fail closed. A
/// negative lag retains the caller's previous behavior and is treated as zero.
#[ensures(result.should_trim == (
    frontier@ >= 0
        && high_watermark@ >= 0
        && current_start@ >= 0
        && effective_trim_lag(safety_lag) <= high_watermark@
        && current_start@ < trim_target(frontier, high_watermark, safety_lag)
))]
#[ensures(result.target@ == if result.should_trim {
    trim_target(frontier, high_watermark, safety_lag)
} else {
    current_start@
})]
#[ensures(result.target@ >= current_start@)]
#[ensures(result.should_trim ==> result.target@ <= frontier@)]
#[ensures(result.should_trim ==>
    result.target@ + effective_trim_lag(safety_lag) <= high_watermark@)]
#[must_use]
pub fn diskless_trim_decision(
    frontier: i64,
    high_watermark: i64,
    safety_lag: i64,
    current_start: i64,
) -> DisklessTrimDecision {
    if frontier < 0 || high_watermark < 0 || current_start < 0 {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let safety_lag = safety_lag.max(0);
    if safety_lag > high_watermark {
        return DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        };
    }

    let target = frontier.min(high_watermark - safety_lag);
    if target <= current_start {
        DisklessTrimDecision {
            should_trim: false,
            target: current_start,
        }
    } else {
        DisklessTrimDecision {
            should_trim: true,
            target,
        }
    }
}
