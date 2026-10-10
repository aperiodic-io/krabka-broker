//! Tests for KIP-630 snapshotting: the interval-driven checkpoint and prune,
//! the checkpoint directory's id ordering and two-snapshot retention, the
//! header timestamp each checkpoint is stamped with, and the fact that an
//! ordinary snapshot never reloads the live image.

use assert2::assert;

use super::*;
use crate::kraft::controller::{
    checkpoint::{
        checkpoint_id_is_newer, latest_checkpoint_id, load_checkpoint_by_id,
        load_latest_checkpoint, parse_checkpoint_name, retain_latest_checkpoint,
        retain_recent_checkpoints, write_checkpoint,
    },
    records::decode_control_record,
    test_support::{
        ControllerSetup, EngineSetup, TEST_ELECTION_TIMEOUT, await_leader, build,
        build_engine_only, elect_single_voter_engine, one_offset_batch, submit_change_with_timeout,
        test_metadata_log, topic_record, voter_set,
    },
};

async fn assert_checkpoint_pruned(ctrl: &KraftController, dir: &std::path::Path) {
    let checkpoint = load_latest_checkpoint(dir)
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    assert2::assert!(!checkpoint.is_empty());
    let quorum = ctrl.quorum_state().await.unwrap();
    assert2::assert!(quorum.log_start_offset > 0);
}

/// The `last_contained_log_timestamp` a checkpoint's KIP-630
/// `SnapshotHeaderRecord` is stamped with. The header is the first control
/// batch of the artifact, so decoding it is how a reader — the JVM
/// `kafka-dump-log --cluster-metadata-decoder` included — sees the value.
fn latest_header_timestamp(checkpoint_dir: &std::path::Path) -> i64 {
    let bytes = load_latest_checkpoint(checkpoint_dir)
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    snapshot_header_timestamp(&bytes)
}

fn snapshot_header_timestamp(bytes: &[u8]) -> i64 {
    let mut cursor = bytes;
    let header = RecordBatch::decode(&mut cursor).expect("decode the header batch");
    let record = header.records.first().expect("a header record");
    match decode_control_record(record).expect("decode the header control record") {
        ControlRecord::SnapshotHeader(header) => header.last_contained_log_timestamp,
        other => panic!("expected a snapshot header control record, got {other:?}"),
    }
}

#[test]
fn snapshot_pruning_stays_at_or_below_the_committed_frontier() {
    assert2::assert!(krabka_verified::snapshot_prune_admission(10, 10));
    assert2::assert!(krabka_verified::snapshot_prune_admission(9, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(11, 10));
    assert2::assert!(!krabka_verified::snapshot_prune_admission(-1, 10));
}

#[test]
fn checkpoint_id_ordering_prefers_higher_offset_then_epoch_without_equal_replacement() {
    for (_case, candidate, current, want) in [
        ("higher offset", (11, 1), (10, 9), true),
        ("same offset higher epoch", (10, 9), (10, 2), true),
        ("same offset lower epoch", (10, 2), (10, 9), false),
        ("equal checkpoint", (10, 9), (10, 9), false),
    ] {
        assert2::assert!(checkpoint_id_is_newer(candidate, current) == want);
    }
}

#[test]
fn checkpoint_names_must_use_the_canonical_fixed_width_encoding() {
    assert2::assert!(
        parse_checkpoint_name("00000000000000000010-0000000002.checkpoint") == Some((10, 2))
    );
    assert2::assert!(
        parse_checkpoint_name("00000000000000000000-0000000000.checkpoint") == Some((0, 0))
    );
    for malformed in [
        "10-2.checkpoint",
        "00000000000000000010-0000000002",
        "00000000000000000010-0000000002.checkpoint.tmp",
        "-0000000000000000001-0000000002.checkpoint",
        "00000000000000000010--000000001.checkpoint",
        "not-a-checkpoint",
    ] {
        assert2::assert!(parse_checkpoint_name(malformed).is_none(), "{malformed}");
    }
}

#[test]
fn ordinary_snapshot_does_not_reload_the_live_image() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let mut rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record("ordinary-snapshot"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    // Replication factor is derived from PartitionRecord on the wire. A
    // normal KIP-630 checkpoint must not replace unrelated in-memory state
    // with that canonicalized representation; only a downgrade reloads.
    let mut in_memory_only = engine.image.topic("ordinary-snapshot").unwrap().clone();
    in_memory_only.replication_factor = 99;
    engine.image.apply(&MetadataRecord::V1Topic(in_memory_only));

    engine
        .write_snapshot_and_prune()
        .expect("ordinary snapshot");

    assert2::assert!(
        engine
            .image
            .topic("ordinary-snapshot")
            .unwrap()
            .replication_factor
            == 99
    );
}

/// Elect the single-voter fixture before snapshot submissions.
async fn single_leader_snapshot_fixture() -> (KraftController, tempfile::TempDir) {
    let (ctrl, dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        snapshot_interval_records: crate::kraft::controller::test_support::SnapshotRecordCount(3),
        ..Default::default()
    });
    super::test_support::elect_single_voter_controller(&ctrl).await;
    (ctrl, dir)
}

/// A single-voter leader with `snapshot_interval_records = 3` snapshots each
/// time the committed offset advances past the threshold. The test engine
/// keeps no log beyond its newest snapshot, so the cleaning after the second
/// snapshot moves the log start up. After four distinct topics, a checkpoint
/// exists on disk and the log start has risen above 0.
#[tokio::test]
async fn leader_snapshots_and_prunes_at_threshold() {
    let (ctrl, dir) = single_leader_snapshot_fixture().await;

    // Four distinct topics, each committed immediately (single voter). Each
    // commit advances the HWM well past the 3-record interval, so a snapshot
    // fires, and the second one lets the cleaning move the log start.
    for name in ["a", "b", "c", "d"] {
        submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
            .await
            .unwrap();
    }

    // A checkpoint was written.
    // The log was cleaned: log-start advanced past 0.
    assert_checkpoint_pruned(&ctrl, dir.path()).await;
    ctrl.shutdown().await;
}

/// The byte-size cap (`max_bytes_between_snapshots`) fires once enough
/// records have committed, even though no single commit's batch reaches the
/// threshold on its own and the threshold falls strictly between batch
/// boundaries. `bytes_since_snapshot` is tracked incrementally from applied
/// batch lengths rather than re-derived from a bounded log read, which would
/// under-count whenever a batch that would cross the cap doesn't fit the
/// remaining read budget and gets silently excluded (never re-included on a
/// later, identical read of the same unchanged range).
#[tokio::test]
async fn leader_snapshots_and_prunes_at_byte_threshold_across_many_small_commits() {
    let (ctrl, dir) = build(ControllerSetup {
        ids: &[NodeId(1)],
        max_bytes_between_snapshots: krabka_units::prelude::bytes(200),
        ..Default::default()
    });
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    await_leader(&ctrl, Some(NodeId(1))).await;

    // Commit distinct single-topic batches, each comfortably under the
    // 200-byte cap on its own, until the cumulative bytes cross it.
    for i in 0..50 {
        submit_change_with_timeout(
            &ctrl,
            topic_record(&format!("bytes-cap-{i}")),
            "byte threshold submit",
        )
        .await
        .unwrap();
        if ctrl.quorum_state().await.unwrap().log_start_offset > 0 {
            break;
        }
    }

    assert_checkpoint_pruned(&ctrl, dir.path()).await;
    ctrl.shutdown().await;
}

#[test]
fn latest_checkpoint_id_picks_highest_offset_then_epoch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 10, 2, b"ten-two").expect("write checkpoint 10/2");
    write_checkpoint(&cp_dir, 10, 9, b"ten-nine").expect("write checkpoint 10/9");
    write_checkpoint(&cp_dir, 11, 1, b"eleven-one").expect("write checkpoint 11/1");

    assert2::assert!(latest_checkpoint_id(&cp_dir) == Some((11, 1)));
    let latest = load_latest_checkpoint(&cp_dir)
        .expect("load latest")
        .expect("latest exists");
    assert2::assert!(latest == b"eleven-one");
}

fn checkpoint_entry_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read checkpoint dir")
        .collect::<Result<Vec<_>, _>>()
        .expect("read entries")
        .len()
}

#[test]
fn retain_recent_checkpoints_keeps_the_two_newest_ids_and_deletes_the_rest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 5, 1, b"oldest").expect("write oldest");
    write_checkpoint(&cp_dir, 6, 0, b"older-same-offset").expect("write older same offset");
    write_checkpoint(&cp_dir, 6, 1, b"newest").expect("write newest");

    retain_recent_checkpoints(&cp_dir);

    for (_case, end_offset, epoch, want_present) in [
        ("the latest checkpoint", 6, 1, true),
        ("the one before it", 6, 0, true),
        ("older than both", 5, 1, false),
    ] {
        assert2::assert!(
            load_checkpoint_by_id(&cp_dir, end_offset, epoch).is_some() == want_present
        );
    }
    assert2::assert!(checkpoint_entry_count(&cp_dir) == 2);
}

#[test]
fn retain_recent_checkpoints_keeps_a_lone_checkpoint() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 7, 2, b"only").expect("write only");

    retain_recent_checkpoints(&cp_dir);

    assert2::assert!(load_checkpoint_by_id(&cp_dir, 7, 2) == Some(b"only".to_vec()));
}

#[test]
fn retain_latest_checkpoint_keeps_only_the_single_newest_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cp_dir = dir.path().to_path_buf();
    write_checkpoint(&cp_dir, 5, 1, b"oldest").expect("write oldest");
    write_checkpoint(&cp_dir, 6, 0, b"older").expect("write older");
    write_checkpoint(&cp_dir, 6, 1, b"newest").expect("write newest");

    retain_latest_checkpoint(&cp_dir);

    assert2::assert!(load_checkpoint_by_id(&cp_dir, 6, 1) == Some(b"newest".to_vec()));
    assert2::assert!(load_checkpoint_by_id(&cp_dir, 6, 0).is_none());
    assert2::assert!(load_checkpoint_by_id(&cp_dir, 5, 1).is_none());
    assert2::assert!(checkpoint_entry_count(&cp_dir) == 1);

    // Empty dir no-ops
    let empty_dir = tempfile::tempdir().expect("tempdir");
    retain_latest_checkpoint(empty_dir.path());
}

/// A snapshot roll leaves the checkpoint it replaces on disk, and only the
/// roll after that removes it. That one-snapshot grace is what a follower
/// chunking a `FetchSnapshot` through the previous id needs: Kafka keeps the
/// previous snapshot servable the same way, through retention rather than by
/// tracking in-flight readers.
#[test]
fn a_snapshot_roll_keeps_the_checkpoint_it_replaces_until_the_next_one() {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut engine);
    let cp_dir = dir.path().to_path_buf();

    let mut ids = Vec::new();
    for name in ["first", "second", "third"] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record(name));
        assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        engine
            .write_snapshot_and_prune()
            .expect("snapshot and prune");
        ids.push(latest_checkpoint_id(&cp_dir).expect("a checkpoint exists"));

        // Every roll must land on a genuinely new id, or retention would have
        // nothing to keep and this test would pass vacuously.
        assert2::assert!(ids.iter().filter(|id| **id == ids[ids.len() - 1]).count() == 1);

        if ids.len() == 2 {
            assert2::assert!(load_checkpoint_by_id(&cp_dir, ids[0].0, ids[0].1).is_some());
        }
    }

    assert2::assert!(
        (
            load_checkpoint_by_id(&cp_dir, ids[0].0, ids[0].1).is_some(),
            load_checkpoint_by_id(&cp_dir, ids[1].0, ids[1].1).is_some(),
            load_checkpoint_by_id(&cp_dir, ids[2].0, ids[2].1).is_some(),
        ) == (false, true, true)
    );
}

/// The KIP-630 header carries the create-time of the last batch the snapshot
/// contains, the way Kafka's `MetadataLoader` hands `createSnapshot` the
/// append time of the last batch it folded in. Before this the engine passed a
/// literal `0` and every checkpoint claimed 1970.
#[test]
fn the_checkpoint_header_carries_the_last_contained_batch_create_time() {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut engine);

    // Append below the engine's own submit path so the create-times are
    // chosen, not read off the wall clock. The snapshot covers both, so it
    // must name the newer one.
    let older = 1_700_000_000_000;
    let newer = 1_700_000_111_222;
    for (value, stamp) in [(b"a", older), (b"b", newer)] {
        let mut batch = one_offset_batch(0, 1, value);
        engine.log.append(&mut batch, stamp).expect("append");
    }
    engine.log.advance_hwm(engine.log.log_end_offset());

    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");

    let bytes = load_latest_checkpoint(dir.path())
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    assert2::assert!(snapshot_header_timestamp(&bytes) == newer);
}

/// A checkpoint rewritten at a boundary the previous one already pruned to
/// keeps that boundary's create-time. The prune moves the log start up to the
/// boundary, so `hwm - 1` is no longer readable; the last record the snapshot
/// contains has not changed, and reverting its header to the epoch would undo
/// the stamp on disk. A time-cap fire on an idle log and an explicit trigger
/// both land here.
#[test]
fn a_snapshot_at_an_already_pruned_boundary_keeps_the_header_timestamp() {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    elect_single_voter_engine(&mut engine);
    let stamp = 1_700_000_222_333;
    let mut batch = one_offset_batch(0, 1, b"only");
    engine.log.append(&mut batch, stamp).expect("append");
    engine.log.advance_hwm(engine.log.log_end_offset());

    let cp_dir = dir.path().to_path_buf();
    let mut stamps = Vec::new();
    engine.write_snapshot_and_prune().expect("first snapshot");
    stamps.push(latest_header_timestamp(&cp_dir));
    // Nothing committed in between, so both of these rewrite the same
    // checkpoint id over the pruned boundary.
    engine.write_snapshot_and_prune().expect("second snapshot");
    stamps.push(latest_header_timestamp(&cp_dir));
    engine.do_trigger_snapshot().expect("explicit trigger");
    stamps.push(latest_header_timestamp(&cp_dir));

    assert2::assert!(stamps == vec![stamp; 3]);
}

/// A follower that installed a snapshot has no log below the boundary either,
/// so the create-time it carries forward comes from the installed artifact's
/// own header — the one place the record's stamp still exists on this node.
#[test]
fn an_installed_snapshot_hands_its_header_timestamp_to_the_next_checkpoint() {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        me: NodeId(2),
        ids: &[NodeId(1), NodeId(2)],
        ..Default::default()
    });
    let stamp = 1_700_000_444_555;
    let mut image = engine.image.clone();
    image.apply(&MetadataRecord::V1Voters(VotersRecord {
        voters: voter_set(&[NodeId(1), NodeId(2)]),
    }));
    let bytes = crate::snapshot::SnapshotWriter::serialize(&image, stamp).expect("serialize");

    engine
        .install_fetched_snapshot((7, 0), &bytes)
        .expect("install the fetched snapshot");
    engine
        .do_trigger_snapshot()
        .expect("checkpoint after install");

    assert2::assert!(latest_header_timestamp(dir.path()) == stamp);
}

/// The same across a restart: the recovered checkpoint's header is the only
/// surviving record of the create-time, and a checkpoint written before any
/// new records commit must not drop it.
#[tokio::test]
async fn a_restart_recovers_the_header_timestamp_from_the_checkpoint() {
    let stamp = 1_700_000_666_777;
    // Snapshot and prune without an election, so the reopened controller's
    // bootstrap epoch matches and its checkpoint lands on the same id.
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let mut batch = one_offset_batch(0, 0, b"restart");
    engine.log.append(&mut batch, stamp).expect("append");
    engine.log.advance_hwm(engine.log.log_end_offset());
    engine
        .write_snapshot_and_prune()
        .expect("snapshot and prune");
    let cp_dir = dir.path().to_path_buf();
    assert2::assert!(latest_header_timestamp(&cp_dir) == stamp);
    drop(engine);

    let reopened = KraftController::open(
        dir.path().to_path_buf(),
        NodeId(1),
        uuid::Uuid::nil(),
        uuid::Uuid::nil(),
        voter_set(&[NodeId(1)]),
        TEST_ELECTION_TIMEOUT,
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
        Arc::new(crate::kraft::NullPeerSender),
        0,
        krabka_units::prelude::bytes(0),
        krabka_units::prelude::millis(0),
        MetadataSnapshotFetchMax::default(),
        test_metadata_log(),
        crate::kraft::Activation::default(),
    )
    .expect("reopen over the same data dir");
    reopened.trigger_snapshot().await.expect("trigger snapshot");

    assert2::assert!(latest_header_timestamp(&cp_dir) == stamp);
    reopened.shutdown().await;
}

/// The same stamp end to end through the engine's own leader append path: a
/// submitted change is stamped with the wall clock, and the checkpoint written
/// over it names that instant rather than the epoch.
#[tokio::test]
async fn a_submitted_change_stamps_its_checkpoint_with_the_append_wall_clock() {
    let (ctrl, dir) = single_leader_snapshot_fixture().await;

    let before = KraftController::wall_clock_ms();
    for name in ["a", "b", "c", "d"] {
        submit_change_with_timeout(&ctrl, topic_record(name), "snapshot threshold submit")
            .await
            .unwrap();
    }
    let after = KraftController::wall_clock_ms();

    let bytes = load_latest_checkpoint(dir.path())
        .expect("scan checkpoints")
        .expect("a checkpoint exists");
    let stamped = snapshot_header_timestamp(&bytes);
    assert2::assert!(before <= stamped && stamped <= after, "{stamped}");
    ctrl.shutdown().await;
}
