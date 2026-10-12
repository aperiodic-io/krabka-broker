//! Deliver-at-time visibility: what a scheduled topic lets a reader see.

use std::time::{Duration, SystemTime};

use assert2::check;
use krabka_ids::Offset;
use krabka_log::{DeliveryAdvance, DeliveryPolicy, Log, LogConfig};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _, bytes, gibibytes, millis};
use tempfile::tempdir;

/// The default clock-confidence bound, in milliseconds.
const BOUND_MS: i64 = 250;

/// A fixed clock, so a schedule in a test is exact rather than nearly right.
const NOW_MS: i64 = 1_700_000_000_000;

/// Comfortably inside every retention window used here.
const PAST_MS: i64 = NOW_MS - 60_000;

/// Not due for another hour.
const FUTURE_MS: i64 = NOW_MS + 3_600_000;

fn scheduled_config(segment_size: ByteSize) -> LogConfig {
    LogConfig {
        segment_size,
        delivery_policy: DeliveryPolicy::Scheduled,
        ..LogConfig::default()
    }
}

#[path = "support/scheduled.rs"]
mod scheduled;
use scheduled::batch_at;

fn log_of(config: LogConfig, activations: &[i64]) -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), config).unwrap();
    for ts in activations {
        log.append(&mut batch_at(*ts)).unwrap();
    }
    (dir, log)
}

/// `segment_size = 1` byte rolls before every append after the first, so each
/// batch lands in a segment of its own.
const ONE_BATCH_PER_SEGMENT: ByteSize = bytes(1);

/// One segment big enough for every batch a test writes.
const ONE_SEGMENT: ByteSize = gibibytes(1);

#[test]
fn an_unscheduled_topic_makes_every_durable_record_visible() {
    let (_dir, mut log) = log_of(LogConfig::default(), &[PAST_MS, FUTURE_MS]);

    let advance = log.advance_delivery_watermark(NOW_MS);

    check!(
        advance
            == DeliveryAdvance {
                watermark: Offset(4),
                next_deadline_ms: None,
            }
    );
    check!(advance.watermark == log.log_end_offset());
    check!(log.delivery_watermark() == log.log_end_offset());
    check!(
        log.pending_activation_ranges(Offset(0), Offset(3), NOW_MS)
            .is_empty()
    );
}

/// The watermark stops at the first batch that is not due, and reports when
/// it comes due. A later batch that *is* due stays behind it: the watermark
/// is a fetch limit, and a limit is a prefix.
#[test]
fn the_watermark_stops_at_the_first_batch_that_is_not_due_yet() {
    let (_dir, mut log) = log_of(
        scheduled_config(ONE_SEGMENT),
        &[PAST_MS, PAST_MS, FUTURE_MS, PAST_MS],
    );

    let advance = log.advance_delivery_watermark(NOW_MS);

    check!(
        advance
            == DeliveryAdvance {
                watermark: Offset(4),
                next_deadline_ms: Some(FUTURE_MS + BOUND_MS),
            }
    );
    check!(log.delivery_watermark() == Offset(4));
    check!(log.log_end_offset() == Offset(8));
}

/// The clock bound is added to the activation time, never subtracted, so a
/// batch is delivered late rather than early. The deadline the advance
/// reports is exactly the instant the same call would let it through.
#[test]
fn a_batch_is_held_until_the_clock_bound_has_also_elapsed() {
    let (_dir, mut log) = log_of(scheduled_config(ONE_SEGMENT), &[PAST_MS, NOW_MS]);

    let held = log.advance_delivery_watermark(NOW_MS);
    check!(
        held == DeliveryAdvance {
            watermark: Offset(2),
            next_deadline_ms: Some(NOW_MS + BOUND_MS),
        }
    );

    // One millisecond before the deadline it is still held.
    let almost = log.advance_delivery_watermark(NOW_MS + BOUND_MS - 1);
    check!(almost == held);

    // On the deadline it becomes visible.
    let due = log.advance_delivery_watermark(NOW_MS + BOUND_MS);
    check!(
        due == DeliveryAdvance {
            watermark: Offset(4),
            next_deadline_ms: None,
        }
    );
}

/// A shorter bound is a narrower claim about the clock, so it releases the
/// batch sooner. The stored value is the raw activation time, so a config
/// change takes effect on the next call.
#[test]
fn the_configured_bound_decides_the_deadline() {
    let config = LogConfig {
        delivery_clock_uncertainty: millis(1_000),
        ..scheduled_config(ONE_SEGMENT)
    };
    let (_dir, mut log) = log_of(config, &[NOW_MS]);

    let advance = log.advance_delivery_watermark(NOW_MS);

    check!(
        advance
            == DeliveryAdvance {
                watermark: Offset(0),
                next_deadline_ms: Some(NOW_MS + 1_000),
            }
    );
    check!(log.advance_delivery_watermark(NOW_MS + 999).watermark == Offset(0));
    check!(log.advance_delivery_watermark(NOW_MS + 1_000).watermark == Offset(2));
}

/// The watermark never moves backwards. A clock that steps back, or a
/// straggler fetch carrying an older reading, must not hide a record that
/// has already been served.
#[test]
fn the_watermark_never_moves_backwards() {
    let (_dir, mut log) = log_of(
        scheduled_config(ONE_BATCH_PER_SEGMENT),
        &[PAST_MS, FUTURE_MS],
    );

    let released = log.advance_delivery_watermark(FUTURE_MS + BOUND_MS);
    check!(released.watermark == Offset(4));

    // Both cache paths: nothing is waiting, and something is waiting.
    check!(log.advance_delivery_watermark(NOW_MS).watermark == Offset(4));
    check!(log.advance_delivery_watermark(0).watermark == Offset(4));
    check!(log.delivery_watermark() == Offset(4));
}

/// The walk crosses segment boundaries and skips whole segments that are
/// wholly due, so a schedule spread over many segments resolves to one
/// watermark.
#[test]
fn the_walk_crosses_segments() {
    let (_dir, mut log) = log_of(
        scheduled_config(ONE_BATCH_PER_SEGMENT),
        &[PAST_MS, PAST_MS, FUTURE_MS, PAST_MS],
    );

    check!(log.advance_delivery_watermark(NOW_MS).watermark == Offset(4));
    check!(
        log.advance_delivery_watermark(FUTURE_MS + BOUND_MS)
            .watermark
            == Offset(8)
    );
}

/// Nothing is written down, so a reopened log rebuilds the same answer from
/// the record timestamps alone.
#[test]
fn a_reopened_log_rebuilds_the_same_watermark() {
    let dir = tempdir().unwrap();
    let config = scheduled_config(ONE_BATCH_PER_SEGMENT);
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for ts in [PAST_MS, PAST_MS, FUTURE_MS, PAST_MS] {
            log.append(&mut batch_at(ts)).unwrap();
        }
        check!(log.advance_delivery_watermark(NOW_MS).watermark == Offset(4));
        log.sync().unwrap();
    }

    let mut reopened = Log::open(dir.path(), config).unwrap();
    check!(
        reopened.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(4),
                next_deadline_ms: Some(FUTURE_MS + BOUND_MS),
            }
    );
}

/// A log opened without tail validation must not serve an undue batch.
///
/// `validate_on_open: false` skips `recover_active_tail`, so the active
/// segment keeps the unknown-timestamp sentinel while holding real batches.
/// This is the end-to-end promise over that recovery gap; the segment-level
/// shortcut that must not read the sentinel as an old timestamp is pinned
/// next to it, in `Segment`'s own tests.
#[test]
fn a_log_opened_without_validation_still_holds_an_undue_batch() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        validate_on_open: false,
        ..scheduled_config(ONE_BATCH_PER_SEGMENT)
    };
    {
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        log.append(&mut batch_at(PAST_MS)).unwrap();
        log.append(&mut batch_at(FUTURE_MS)).unwrap();
        log.sync().unwrap();
    }

    let mut reopened = Log::open(dir.path(), config).unwrap();
    let advance = reopened.advance_delivery_watermark(NOW_MS);

    // Offset 2 is where the scheduled batch begins: nothing may go past it.
    check!(advance.watermark <= Offset(2));
    check!(
        reopened
            .read_raw(Offset(0), advance.watermark, gibibytes(1))
            .unwrap()
            .last_offset
            < Some(Offset(2))
    );
}

/// A share consumer needs every gap in a window, not just the leading run.
/// Ranges are whole batches, adjacent ones merge, and a batch that is due
/// splits the run.
#[test]
fn pending_ranges_cover_whole_batches_and_merge_when_adjacent() {
    // offsets 0-1 due, 2-3 and 4-5 waiting (adjacent), 6-7 due, 8-9 waiting.
    let (_dir, log) = log_of(
        scheduled_config(ONE_SEGMENT),
        &[PAST_MS, FUTURE_MS, FUTURE_MS, PAST_MS, FUTURE_MS],
    );

    let ranges = log.pending_activation_ranges(Offset(0), Offset(9), NOW_MS);

    check!(ranges == vec![(Offset(2), Offset(5)), (Offset(8), Offset(9))]);
}

/// A window that cuts through a batch still reports that whole batch: the
/// share reader fetches with `read_raw`, which is batch-granular.
#[test]
fn a_window_that_splits_a_batch_reports_the_whole_batch() {
    let (_dir, log) = log_of(scheduled_config(ONE_SEGMENT), &[PAST_MS, FUTURE_MS]);

    // Offset 3 is the second half of the batch that spans 2-3.
    check!(
        log.pending_activation_ranges(Offset(3), Offset(3), NOW_MS) == vec![(Offset(2), Offset(3))]
    );
}

#[test]
fn pending_ranges_span_segments_and_stay_inside_the_log() {
    let (_dir, log) = log_of(
        scheduled_config(ONE_BATCH_PER_SEGMENT),
        &[FUTURE_MS, FUTURE_MS, PAST_MS],
    );

    // A window wider than the log clamps to what the log holds.
    check!(
        log.pending_activation_ranges(Offset(0), Offset(999), NOW_MS)
            == vec![(Offset(0), Offset(3))]
    );
    // A window entirely above the log holds nothing.
    check!(
        log.pending_activation_ranges(Offset(6), Offset(9), NOW_MS)
            .is_empty()
    );
    // So does an inverted one.
    check!(
        log.pending_activation_ranges(Offset(4), Offset(1), NOW_MS)
            .is_empty()
    );
    // Once everything is due, so does a window over the whole log.
    check!(
        log.pending_activation_ranges(Offset(0), Offset(5), FUTURE_MS + BOUND_MS)
            .is_empty()
    );
}

/// Size retention has no timestamp in it, so without a guard it deletes a
/// segment holding records nobody has been allowed to read yet. Retention
/// may take the segments below the watermark and no others.
///
/// Nothing fetches from this partition, so `tick` has to refresh the
/// watermark itself; a guard that waited for a reader would protect nothing.
#[test]
fn size_retention_never_evicts_a_segment_holding_an_undelivered_record() {
    let config = LogConfig {
        retention: None,
        // A budget of nothing: every sealed segment is over it.
        retention_size: Some(ByteSize::ZERO),
        ..scheduled_config(ONE_BATCH_PER_SEGMENT)
    };
    let (_dir, mut log) = log_of(config, &[PAST_MS, PAST_MS, FUTURE_MS, FUTURE_MS, PAST_MS]);

    let now = SystemTime::UNIX_EPOCH + Duration::from_millis(u64::try_from(NOW_MS).unwrap());
    log.tick(now, Offset(i64::MAX)).unwrap();

    check!(log.delivery_watermark() == Offset(4));

    // The two segments below the watermark went; the two holding scheduled
    // records stayed, and so did the active segment.
    check!(log.log_start_offset() == Offset(4));
    check!(log.log_end_offset() == Offset(10));
    let read = log.read(Offset(4), gibibytes(1)).unwrap();
    check!(read.batches.len() == 3);
}

/// The same budget on an ordinary topic evicts as it always did: the guard
/// reads the log end there, and no sealed segment reaches it.
#[test]
fn size_retention_on_an_unscheduled_topic_is_untouched_by_the_guard() {
    let config = LogConfig {
        retention: None,
        retention_size: Some(ByteSize::ZERO),
        segment_size: ONE_BATCH_PER_SEGMENT,
        ..LogConfig::default()
    };
    let (_dir, mut log) = log_of(config, &[PAST_MS, PAST_MS, PAST_MS]);

    let epoch_offset = Duration::from_millis(NOW_MS as u64);
    log.tick(SystemTime::UNIX_EPOCH + epoch_offset, Offset(i64::MAX))
        .unwrap();

    // Kafka's `deleteSegments`: with every byte over budget and the whole log
    // replicated, every segment goes, the active one after a roll, and the
    // log starts at its end.
    check!(log.log_start_offset() == log.log_end_offset());
}

/// A truncation can remove the batch the last walk stopped on. The cached
/// deadline must not outlive it, or the watermark stalls until an instant
/// that no record asks for any more.
#[test]
fn a_truncation_drops_the_deadline_it_invalidates() {
    let (_dir, mut log) = log_of(
        scheduled_config(ONE_BATCH_PER_SEGMENT),
        &[PAST_MS, FUTURE_MS, PAST_MS],
    );

    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(2),
                next_deadline_ms: Some(FUTURE_MS + BOUND_MS),
            }
    );

    // Cut the scheduled batch and everything after it away.
    log.truncate_to(Offset(2)).unwrap();

    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(2),
                next_deadline_ms: None,
            }
    );
    check!(log.log_end_offset() == Offset(2));
}

/// A truncation carries the watermark back down with the records it cut away.
///
/// Without that, an append that pushes the log end back above the stale
/// watermark unmasks it, and the walk resumes past the batch that took the
/// truncated one's place -- delivering it without ever reading its activation
/// time.
#[test]
fn a_batch_appended_over_a_truncated_tail_is_still_held_until_it_is_due() {
    let (_dir, mut log) = log_of(
        scheduled_config(ONE_BATCH_PER_SEGMENT),
        &[PAST_MS, PAST_MS, PAST_MS],
    );

    // Everything is due, so the watermark reaches the log end.
    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(6),
                next_deadline_ms: None,
            }
    );

    // Cut the last two batches away and put one scheduled batch in their place.
    log.truncate_to(Offset(2)).unwrap();
    log.append(&mut batch_at(FUTURE_MS)).unwrap();

    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(2),
                next_deadline_ms: Some(FUTURE_MS + BOUND_MS),
            }
    );
    check!(log.delivery_watermark() == Offset(2));

    // And it is released once its time arrives.
    check!(
        log.advance_delivery_watermark(FUTURE_MS + BOUND_MS)
            .watermark
            == Offset(4)
    );
}

/// An empty log has nothing to schedule, and a record appended after a walk
/// that found nothing waiting is still picked up.
#[test]
fn an_empty_log_and_a_late_append_both_resolve() {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), scheduled_config(ONE_SEGMENT)).unwrap();

    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(0),
                next_deadline_ms: None,
            }
    );

    log.append(&mut batch_at(FUTURE_MS)).unwrap();
    check!(
        log.advance_delivery_watermark(NOW_MS)
            == DeliveryAdvance {
                watermark: Offset(0),
                next_deadline_ms: Some(FUTURE_MS + BOUND_MS),
            }
    );

    log.append(&mut batch_at(PAST_MS)).unwrap();
    // The waiting batch still blocks the prefix behind it.
    check!(log.advance_delivery_watermark(NOW_MS).watermark == Offset(0));
    check!(
        log.pending_activation_ranges(Offset(0), Offset(3), NOW_MS) == vec![(Offset(0), Offset(1))]
    );
}
