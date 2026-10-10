//! Unit tests for the three read paths, covering the refused and empty
//! boundaries, the byte-exactness of the verbatim read, and the joins a
//! fetch makes across a segment seam.

use assert2::check;
use krabka_units::prelude::{gibibytes, kibibytes, mebibytes};
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{NO_LIMIT, sample_batch, test_batch_at, test_log, tiny_segments},
};

/// Read a complete raw response and check every byte against the written wire image.
fn check_raw_bytes(log: &Log, wire: &[u8]) -> crate::log::RawRead {
    let log_end = log.log_end_offset();
    let read = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    assert2::assert!(read.start_offset == Offset(0));
    assert2::assert!(read.total == wire.len());
    assert2::assert!(&read.bytes[..] == wire);
    read
}

#[test]
fn batch_header_floor_is_the_protocol_header_length() {
    // `read_raw`'s anti-stall floor: each segment must be asked for at
    // least one whole v2 header, or the boundary walk has nothing to read.
    assert2::assert!(batch_header().bytes_usize() == HEADER_LEN);
}

/// A raw fetch below the log start is refused, and one at or past the
/// limit reads nothing.
#[test]
fn a_raw_fetch_outside_the_readable_range_is_refused_or_empty() {
    let (_dir, mut log) = test_log();
    crate::log::test_support::append_samples(&mut log, 4, 2);
    log.set_log_start_offset(Offset(2)).expect("set log start");

    let below = log.read_raw(Offset(1), Offset(99), mebibytes(1));
    check!(
        matches!(below, Err(LogError::OffsetTooLow { .. })),
        "below the log start is refused, got {below:?}"
    );
    // Exactly at the log start is inside it.
    check!(log.read_raw(Offset(2), Offset(99), mebibytes(1)).is_ok());

    let at_limit = log
        .read_raw(Offset(2), Offset(2), mebibytes(1))
        .expect("a fetch at the limit is not an error");
    check!(
        at_limit.bytes.is_empty(),
        "a fetch at the limit reads nothing"
    );
}

/// A fetch spanning sealed segments and the active one returns every batch
/// once, in order, however many chunks it was assembled from.
///
/// One chunk is returned as it stands and several are concatenated; a fetch
/// that crosses the seal takes the second path, and getting the join wrong
/// duplicates or drops a batch at the boundary.
#[test]
fn a_fetch_across_the_seal_joins_its_chunks_in_order() {
    let dir = tempdir().unwrap();
    // A small segment cap so the appends roll and the fetch has to cross.
    let mut log = crate::test_support::segmented_log(dir.path(), kibibytes(1));
    crate::log::test_support::append_samples(&mut log, 40, 2);
    check!(!log.segments.is_empty(), "the appends should have rolled");

    let end = log.log_end_offset();
    let whole = log
        .read_raw(Offset(0), end, gibibytes(1))
        .expect("read the whole log");
    check!(!whole.bytes.is_empty());
    check!(
        whole.start_offset == Offset(0),
        "starts where asked, got {:?}",
        whole.start_offset
    );
    check!(
        whole.last_offset == Some(end - 1),
        "reaches the end, got {:?}",
        whole.last_offset
    );

    // A single sealed segment's worth: one chunk, returned unjoined.
    let first_only = log
        .read_raw(Offset(0), Offset(2), gibibytes(1))
        .expect("read one batch");
    check!(first_only.start_offset == Offset(0));
    check!(
        first_only.bytes.len() < whole.bytes.len(),
        "one batch is less than the whole log"
    );
}

#[test]
fn log_read_raw_spans_and_is_byte_exact() {
    let (dir, mut log) = test_log();
    let mut wire = bytes::BytesMut::new();
    for off in 0..4i64 {
        let mut b = test_batch_at(off);
        log.append(&mut b).unwrap();
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();
    check_raw_bytes(&log, &wire);
    drop(dir);
}

#[test]
fn log_read_raw_spans_multiple_segments() {
    // A tiny `segment_size` forces a roll partway through, so the
    // read must walk at least one sealed segment AND the active
    // segment — exercising the multi-chunk `BytesMut` concat path
    // that `log_read_raw_spans_and_is_byte_exact` (default ~1 GiB
    // segments) never reaches.
    let dir = tempdir().unwrap();
    // tiny: roll after roughly each batch
    let mut log = crate::test_support::segmented_log(dir.path(), bytes(100));

    let n: i64 = 6;
    let mut wire = bytes::BytesMut::new();
    let mut expected_bases = Vec::new();
    for off in 0..n {
        let mut b = test_batch_at(off);
        let (base, _) = log.append(&mut b).unwrap();
        expected_bases.push(base);
        b.encode(&mut wire).unwrap();
    }
    let wire = wire.freeze();

    // The roll must actually have happened: at least one sealed
    // segment plus the active segment.
    assert2::assert!(!log.segments.is_empty());
    assert2::assert!(log.active.is_some());

    let r = check_raw_bytes(&log, &wire);

    // Decode back to N batches with the expected base offsets.
    let mut cur: &[u8] = &r.bytes;
    let mut bases = Vec::new();
    while !cur.is_empty() {
        let b = krabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        bases.push(Offset(b.base_offset));
    }
    assert2::assert!(bases == expected_bases);
    drop(dir);
}

crate::sendfile_cfg! {
/// Increment D/E: `Log::read_raw_desc` across a segment seam must yield
/// regions whose **concatenation** is byte-identical to the coalesced
/// bytes of `read_raw`. The result must be several `FileRegion`s, one for
/// each contributing segment. That proves the cross-segment copy is gone.
#[test]
fn log_read_raw_desc_multi_segment_regions_equal_read_raw() {
    use std::os::unix::fs::FileExt;
    let dir = tempdir().unwrap();
    // tiny: roll roughly each batch
    let mut log = crate::test_support::segmented_log(dir.path(), bytes(100));

    let n: i64 = 6;
    for off in 0..n {
        let mut b = test_batch_at(off);
        log.append(&mut b).unwrap();
    }
    assert2::assert!(!log.segments.is_empty());

    let log_end = log.log_end_offset();
    let raw = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
    let desc = log.read_raw_desc(Offset(0), log_end, mebibytes(10)).unwrap();

    assert2::assert!(desc.start_offset == raw.start_offset);
    assert2::assert!(desc.total == raw.total);
    // Multi-segment ⇒ more than one region (no coalescing copy).
    check!(
        desc.regions.len() >= 2,
        "expected >=2 regions across the seam, got {}",
        desc.regions.len()
    );

    // Concatenate the pread'd regions and compare to read_raw's bytes.
    let mut assembled = Vec::with_capacity(desc.total);
    for region in &desc.regions {
        let mut buf = vec![0u8; region.len];
        let mut filled = 0;
        let mut off = region.offset;
        while filled < buf.len() {
            let r = region.file.read_at(&mut buf[filled..], off).unwrap();
            assert2::assert!(r > 0);
            filled += r;
            off += r as u64;
        }
        assembled.extend_from_slice(&buf);
    }
    assert2::assert!(assembled == raw.bytes[..]);
    drop(dir);
}

/// The descriptor twin of `a_read_never_skips_past_a_segment_its_budget_clipped`:
/// the zero-copy path a plaintext follower fetch takes.
#[test]
fn a_descriptor_read_never_skips_past_a_segment_its_budget_clipped() {
    use std::os::unix::fs::FileExt;
    check_seam_reads(
        |log, offset, end, budget| {
            let mut wire = Vec::new();
            for region in log.read_raw_desc(offset, end, budget).unwrap().regions {
                let mut bytes = vec![0u8; region.len];
                region.file.read_exact_at(&mut bytes, region.offset).unwrap();
                wire.extend_from_slice(&bytes);
            }
            batch_bases(&wire)
        },
        |bases| bases,
    );
}
} // sendfile_cfg!

/// A log of three segments whose batches are not all one size, so a budget
/// can stop a read inside one segment while the first batch of the next would
/// still fit what is left of it.
///
/// | segment | batches, as (base offset, records) |
/// |---------|------------------------------------|
/// | sealed  | (0, 1) (1, 8)                      |
/// | sealed  | (9, 1) (10, 1)                     |
/// | active  | (11, 8)                            |
///
/// Returns the log with the encoded size of a one-record batch.
fn seam_log() -> (tempfile::TempDir, Log, usize) {
    let small = sample_batch(1).encoded_len();
    let big = sample_batch(8).encoded_len();
    let dir = tempdir().unwrap();
    let config = LogConfig {
        // A segment rolls when the next batch would take it past this, so
        // the first holds one small and one big batch.
        segment_size: size_from_len(small + big),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    for records in [1, 8, 1, 1, 8] {
        log.append(&mut sample_batch(records)).unwrap();
    }
    let bases: Vec<Offset> = log
        .segments
        .iter()
        .chain(log.active.as_ref())
        .map(crate::segment::Segment::base_offset)
        .collect();
    assert2::assert!(bases == vec![Offset(0), Offset(9), Offset(11)]);
    (dir, log, small)
}

/// The rows of [`seam_log`] each read path is asked for: a label, the fetch
/// offset, the byte budget in one-record batches, and the base offsets of the
/// batches the read must serve.
///
/// A read leaves a segment only after it reads that segment to the end, as
/// Kafka's `LocalLog.read` never leaves the segment that it found data in. Only
/// the first batch of a read can be larger than the budget, which is KIP-74. A
/// read that went on into the next segment from a run that its budget clipped
/// served the first batch of that segment and skipped every offset between
/// them. A follower that caught up through a throttled reassignment lost most
/// of each 64 MiB segment that way.
fn seam_cases(small: usize) -> Vec<(&'static str, Offset, ByteSize, Vec<i64>)> {
    let budget = |halves: usize| size_from_len(small * halves / 2);
    vec![
        (
            "a read its budget clips inside a sealed segment ends there",
            Offset(0),
            budget(4),
            vec![0],
        ),
        (
            "a read its budget clips inside the last sealed segment never reaches the active one",
            Offset(9),
            budget(3),
            vec![9],
        ),
        (
            "past the first batch, one larger than what is left waits for the next read",
            Offset(9),
            budget(6),
            vec![9, 10],
        ),
        (
            "a read that empties a segment goes on into the next",
            Offset(9),
            NO_LIMIT,
            vec![9, 10, 11],
        ),
        (
            "a read from inside a batch starts at that batch and goes on to the end",
            Offset(8),
            NO_LIMIT,
            vec![1, 9, 10, 11],
        ),
        (
            "a whole-log read serves every batch",
            Offset(0),
            NO_LIMIT,
            vec![0, 1, 9, 10, 11],
        ),
    ]
}

/// The base offset of every batch in `wire`, which holds whole v2 batches.
fn batch_bases(mut wire: &[u8]) -> Vec<i64> {
    let mut bases = Vec::new();
    while !wire.is_empty() {
        bases.push(RecordBatch::decode(&mut wire).unwrap().base_offset);
    }
    bases
}

fn check_seam_reads<T: std::fmt::Debug + PartialEq>(
    read: impl Fn(&Log, Offset, Offset, ByteSize) -> T,
    expect: impl Fn(Vec<i64>) -> T,
) {
    let (_dir, log, small) = seam_log();
    let end = log.log_end_offset();
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (label, offset, budget, bases) in seam_cases(small) {
        actual.push((label, read(&log, offset, end, budget)));
        expected.push((label, expect(bases)));
    }
    check!(actual == expected);
}

#[test]
fn a_read_never_skips_past_a_segment_its_budget_clipped() {
    check_seam_reads(
        |log, offset, end, budget| {
            let decoded = log.read(offset, budget).unwrap().batches;
            let raw = log.read_raw(offset, end, budget).unwrap().bytes;
            (
                decoded.iter().map(|batch| batch.base_offset).collect(),
                batch_bases(&raw),
            )
        },
        |bases| (bases.clone(), bases),
    );
}

#[test]
fn append_then_read_back_in_order() {
    let (_dir, mut log) = test_log();
    let mut expected = Vec::new();
    for _ in 0..3 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        expected.push(b);
    }
    let out = log.read(Offset(0), NO_LIMIT).unwrap();
    assert2::assert!(out.batches == expected);
    assert2::assert!(out.start_offset == Offset(0));
}

#[test]
fn read_offset_too_low_errors() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    assert2::assert!(matches!(
        log.read(Offset(-1), kibibytes(1)),
        Err(LogError::OffsetTooLow { .. })
    ));
}

#[test]
fn read_at_log_end_returns_empty() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    let log_end = log.log_end_offset();
    let out = log.read(log_end, kibibytes(1)).unwrap();
    assert2::assert!(out.batches == Vec::new());
    assert2::assert!(out.start_offset == log_end);
}

#[test]
fn read_raw_after_reopen_does_not_skip_first_sealed_segment() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let cfg = tiny_segments();
    {
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        log.append(&mut sample_batch(1)).unwrap(); // offset 0 → sealed seg base 0
        log.append(&mut sample_batch(1)).unwrap(); // offset 1 → sealed seg base 1
        log.append(&mut sample_batch(1)).unwrap(); // offset 2 → active seg base 2
        assert2::assert!(log.log_end_offset() == 3);
        assert2::assert!(log.segments.len() >= 2);
    }
    // Reopen simulates a broker restart: sealed segments are loaded via the
    // no-scan Segment::open, which leaves last_offset = base - 1. Without
    // fixing last_offset from the next segment's base, read_raw skips the
    // first sealed segment (its stale last_offset < fetch_offset) and serves
    // a later segment's base — the on-cluster phantom-follower gap that
    // pinned the high-watermark and stalled acks=all.
    let reopened = Log::open(dir.path(), cfg).unwrap();
    let r = reopened
        .read_raw(Offset(0), reopened.log_end_offset(), mebibytes(1))
        .unwrap();
    assert2::assert!(r.start_offset == 0);
}

#[test]
fn read_raw_multi_segment_budget_and_limit() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    let cfg = tiny_segments();
    let mut log = Log::open(dir.path(), cfg).unwrap();
    crate::log::test_support::append_samples(&mut log, 3, 1);

    let full = log.read_raw(Offset(0), Offset(3), mebibytes(1)).unwrap();
    assert2::assert!(full.start_offset == Offset(0));
    assert2::assert!(full.last_offset == Some(Offset(2)));

    let first_len = u32::try_from(full.total / 3).unwrap();
    let clipped = log
        .read_raw(Offset(0), Offset(3), bytes(first_len))
        .unwrap();
    assert2::assert!(clipped.start_offset == Offset(0));
    assert2::assert!(clipped.last_offset == Some(Offset(0)));
    assert2::assert!(clipped.total < full.total);

    let limited = log.read_raw(Offset(0), Offset(2), mebibytes(1)).unwrap();
    assert2::assert!(limited.start_offset == Offset(0));
    assert2::assert!(limited.last_offset == Some(Offset(1)));
}

/// Sparse indexes can leave several earlier batches before a fetch target.
/// Their bytes must not consume the budget for the first eligible batch.
#[test]
fn a_small_budget_preserves_unindexed_records_before_a_segment_seam() {
    let dir = tempdir().unwrap();
    let small = sample_batch(1).encoded_len();
    let config = LogConfig {
        segment_size: size_from_len(4 * small),
        index_interval: kibibytes(4),
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    crate::log::test_support::append_samples(&mut log, 8, 1);
    check!(!log.segments.is_empty());
    let end = log.log_end_offset();
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for offset in 0..8 {
        for budget in [1, HEADER_LEN, small, 2 * small] {
            let read = log
                .read_raw(Offset(offset), end, size_from_len(budget))
                .unwrap();
            actual.push((offset, budget, batch_bases(&read.bytes).first().copied()));
            expected.push((offset, budget, Some(offset)));
        }
    }
    check!(actual == expected);
}

crate::sendfile_cfg! {
/// The file-region path must preserve the same first eligible batch, including
/// the anti-stall batch whose payload is larger than the requested budget.
#[test]
fn a_small_budget_preserves_unindexed_descriptor_records_before_a_seam() {
    use std::os::unix::fs::FileExt;
    let (_dir, log, small) = seam_log();
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for budget in [1, HEADER_LEN, small] {
        let desc = log
            .read_raw_desc(Offset(1), log.log_end_offset(), size_from_len(budget))
            .unwrap();
        let mut wire = Vec::new();
        for region in desc.regions {
            let mut data = vec![0; region.len];
            region.file.read_exact_at(&mut data, region.offset).unwrap();
            wire.extend_from_slice(&data);
        }
        actual.push((budget, batch_bases(&wire)));
        expected.push((budget, vec![1]));
    }
    check!(actual == expected);
}
}

crate::sendfile_cfg! {
/// The configured `read_ahead_max` reaches the readahead hint of both
/// verbatim reads: each asks for its window and at most that much more, not
/// the default 4 MiB past it.
#[test]
fn configured_read_ahead_max_reaches_both_verbatim_reads() {
    let budget = ByteSize::from_bytes(1_000);
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (read_ahead_max, hinted) in [(0, 1_000), (100, 1_100)] {
        let (_dir, mut log) = crate::log::test_support::configured_test_log(LogConfig {
            read_ahead_max: ByteSize::from_bytes(read_ahead_max),
            ..LogConfig::default()
        });
        crate::log::test_support::append_samples(&mut log, 40, 1);
        check!(log.active.as_ref().unwrap().size().bytes_u64() > 2 * budget.bytes_u64());
        let advice = std::sync::Arc::new(crate::test_support::RecordedAdvice::default());
        log.test_set_io(advice.clone());
        let end = log.log_end_offset();

        log.read_raw(Offset(0), end, budget).unwrap();
        actual.push((read_ahead_max, "read_raw", advice.take()));
        log.read_raw_desc(Offset(0), end, budget).unwrap();
        actual.push((read_ahead_max, "read_raw_desc", advice.take()));
        for path in ["read_raw", "read_raw_desc"] {
            expected.push((read_ahead_max, path, vec![(0, hinted)]));
        }
    }
    check!(actual == expected);
}
}
