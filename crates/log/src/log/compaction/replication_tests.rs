//! A follower replicating a compacted leader. Compaction leaves holes in the
//! offsets, whole batches gone and records missing from inside a batch, and
//! Kafka's `UnifiedLog.appendAsFollower` takes a batch whose first offset is
//! at or past the follower's log end offset, so the follower still catches up.

use {
    super::*,
    crate::log::test_support::{
        compacting_segments, compaction_ctx, keyed_batch, set_segment_size, test_log, verbatim_from,
    },
    krabka_ids::{LeaderEpoch, Offset},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::mebibytes,
    tempfile::tempdir,
};

/// A leader that one compaction pass has cut holes into. Each batch sits in a
/// segment of its own, and the last one stays in the active segment:
///
/// | offsets | batch                | what the pass does                       |
/// | ------- | -------------------- | ---------------------------------------- |
/// | 0       | `k0`                 | dropped, `k0` is rewritten at 7          |
/// | 1..=3   | `k1`, `k2`, `k3`     | keeps 1 and 3, `k2` is rewritten at 5    |
/// | 4       | `k4`                 | dropped, `k4` is rewritten at 6          |
/// | 5, 6, 7 | one record each      | kept                                     |
/// | 8       | `k9`                 | active, not cleaned                      |
fn compacted_leader(dir: &std::path::Path) -> Log {
    let mut log = Log::open(dir, compacting_segments()).unwrap();
    for mut batch in [
        keyed_batch(0, &[(0, b"k0", b"v0")]),
        keyed_batch(0, &[
            (0, b"k1", b"v1"),
            (1, b"k2", b"v2"),
            (2, b"k3", b"v3"),
        ]),
        keyed_batch(0, &[(0, b"k4", b"v4")]),
        keyed_batch(0, &[(0, b"k2", b"v5")]),
        keyed_batch(0, &[(0, b"k4", b"v6")]),
        keyed_batch(0, &[(0, b"k0", b"v7")]),
        keyed_batch(0, &[(0, b"k9", b"v8")]),
    ] {
        log.append(&mut batch).unwrap();
    }
    // The grouping cap shares `segment.bytes` with the roll that put each
    // batch in a segment of its own, so widen it for the pass.
    set_segment_size(&mut log, mebibytes(1));
    log.compact(&compaction_ctx()).unwrap();
    log
}

/// Every batch of `log` from `from` on, decoded.
fn batches_from(log: &Log, from: Offset) -> Vec<RecordBatch> {
    let read = log
        .read_raw(from, log.log_end_offset(), mebibytes(1))
        .unwrap();
    let mut cursor = &read.bytes[..];
    let mut batches = Vec::new();
    while !cursor.is_empty() {
        batches.push(RecordBatch::decode(&mut cursor).unwrap());
    }
    batches
}

/// What a follower's fetch loop does: fetch from its log end offset, append
/// every batch of the response at the offset the batch carries, and repeat
/// until it holds what the leader holds.
fn catch_up(leader: &Log, follower: &mut Log, verbatim: bool) {
    while follower.log_end_offset() < leader.log_end_offset() {
        let fetch_offset = follower.log_end_offset();
        let response = batches_from(leader, fetch_offset);
        assert2::assert!(
            !response.is_empty(),
            "the leader answers a fetch at {fetch_offset} with nothing"
        );
        for mut batch in response {
            let base_offset = Offset(batch.base_offset);
            if verbatim {
                let (_, wire) = verbatim_from(&batch, LeaderEpoch(batch.partition_leader_epoch));
                follower.append_verbatim_at(&wire, base_offset).unwrap();
            } else {
                follower.append_at(&mut batch, base_offset).unwrap();
            }
        }
    }
}

/// A follower that starts empty, or has read part of the log, takes the
/// leader's batches across a hole at the start (offset 0), between batches
/// (offset 4) and inside a batch (offset 2), by either append path, and ends
/// with the log the leader has.
#[test]
fn a_follower_catches_up_from_a_compacted_leader() {
    let leader_dir = tempdir().unwrap();
    let leader = compacted_leader(leader_dir.path());
    let leader_batches = batches_from(&leader, Offset(0));
    assert2::assert!(
        leader_batches
            .iter()
            .map(|batch| (batch.base_offset, batch.last_offset_delta))
            .collect::<Vec<_>>()
            == [(1, 2), (5, 0), (6, 0), (7, 0), (8, 0)],
        "the leader's log is not shaped as the test expects"
    );

    for (label, verbatim) in [("verbatim", true), ("owned", false)] {
        let (_follower_dir, mut follower) = test_log();

        catch_up(&leader, &mut follower, verbatim);

        assert2::assert!(
            follower.log_end_offset() == leader.log_end_offset(),
            "{label}"
        );
        assert2::assert!(
            batches_from(&follower, Offset(0)) == leader_batches,
            "{label}"
        );
    }
}

/// The follower's log end offset is still the floor: a batch that starts
/// below it is a duplicate or a divergence, and is refused whichever append
/// path carries it, leaving the log as it was.
#[test]
fn a_follower_still_refuses_a_batch_below_its_log_end_offset() {
    let leader_dir = tempdir().unwrap();
    let leader = compacted_leader(leader_dir.path());
    let (_follower_dir, mut follower) = test_log();
    catch_up(&leader, &mut follower, true);
    let leo = follower.log_end_offset();

    let mut stale = keyed_batch(leo.0 - 1, &[(0, b"k9", b"v8")]);
    let (_, wire) = verbatim_from(&stale, LeaderEpoch(0));
    let below = Offset(leo.0 - 1);

    assert2::assert!(let Err(LogError::OffsetMismatch { expected, actual }) = follower.append_at(&mut stale, below));
    assert2::assert!((expected, actual) == (leo, below));
    assert2::assert!(let Err(LogError::OffsetMismatch { .. }) = follower.append_verbatim_at(&wire, below));
    assert2::assert!(follower.log_end_offset() == leo);
}
