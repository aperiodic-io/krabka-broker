//! Tests for the mandatory KIP-1155 `metadata.version` downgrade checkpoint:
//! it is retried until it succeeds, it pins the exact lower-version image and
//! boundary, and a restart finishes it before the image is exposed.

use assert2::assert;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};

use super::*;
use crate::kraft::controller::{
    checkpoint::{load_latest_checkpoint, write_checkpoint},
    recovery::replay_committed,
    test_support::{
        EngineSetup, build_engine_only, elect_single_voter_engine, open_test_controller,
        topic_record, voter_set,
    },
};

fn metadata_version_update(level: i16) -> Vec<MetadataRecord> {
    vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level,
    })]
}

fn reopen_single_voter(data_dir: std::path::PathBuf) -> Result<KraftController, RaftError> {
    open_test_controller(data_dir, uuid::Uuid::nil(), voter_set(&[NodeId(1)]))
}

fn uncheckpointed_downgrade() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = super::test_support::single_voter_leader_engine();

    let mut rx = super::test_support::submit_on_engine(&mut engine, &metadata_version_update(25));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    engine.downgrade_snapshot_failures_remaining = usize::MAX;
    let mut rx = super::test_support::submit_on_engine(&mut engine, &metadata_version_update(16));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    (engine, dir)
}

#[test]
fn metadata_version_downgrade_retries_mandatory_snapshot_and_prune() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let published_image = engine.image_tx.subscribe();
    elect_single_voter_engine(&mut engine);
    let mut rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record("snapshot-reload"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    // Plant an in-memory-only sentinel in a TopicRecord field that the
    // KIP-631 wire shape does not carry. Snapshot decode reconstructs RF
    // from the PartitionRecord; a clone of the pre-snapshot image would
    // incorrectly retain 99.
    let mut in_memory_only = engine.image.topic("snapshot-reload").unwrap().clone();
    in_memory_only.replication_factor = 99;
    engine.image.apply(&MetadataRecord::V1Topic(in_memory_only));
    assert2::assert!(
        engine
            .image
            .topic("snapshot-reload")
            .unwrap()
            .replication_factor
            == 99
    );

    let mut rx = super::test_support::submit_on_engine(&mut engine, &metadata_version_update(25));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert2::assert!(engine.latest_snapshot_id().is_none());

    engine.downgrade_snapshot_failures_remaining = 1;
    let mut rx = super::test_support::submit_on_engine(&mut engine, &metadata_version_update(16));

    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert2::assert!(engine.image.finalized_metadata_version() == Some(16));
    assert2::assert!(engine.downgrade_snapshot_pending.is_some());
    assert2::assert!(engine.latest_snapshot_id().is_none());
    assert2::assert!(published_image.borrow().finalized_metadata_version() == Some(25));
    assert2::assert!(
        engine
            .image
            .topic("snapshot-reload")
            .unwrap()
            .replication_factor
            == 99
    );

    // A later feature upgrade and an ordinary metadata record may commit
    // while the mandatory lower-version checkpoint is still pending. They
    // remain unpublished, and the retry must checkpoint level 16 at its
    // exact boundary before replaying either suffix record.
    engine.downgrade_snapshot_failures_remaining = 2;
    let mut rx = super::test_support::submit_on_engine(&mut engine, &metadata_version_update(20));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let mut rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record("after-downgrade"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert2::assert!(engine.image.finalized_metadata_version() == Some(20));
    assert2::assert!(engine.image.topic("after-downgrade").is_some());
    assert2::assert!(published_image.borrow().finalized_metadata_version() == Some(25));
    let pending = engine
        .downgrade_snapshot_pending
        .as_ref()
        .expect("downgrade remains pending");
    assert2::assert!(pending.image.finalized_metadata_version() == Some(16));
    assert2::assert!(pending.image.topic("after-downgrade").is_none());
    let downgrade_end = pending.end_offset;

    let mut restart_image = MetadataImage::new(engine.image.cluster_id());
    assert2::assert!(
        replay_committed(
            &engine.log,
            &mut restart_image,
            Offset(0),
            MetadataRaftFetchMax::default(),
        )
        .expect("replay")
        .is_some()
    );
    assert2::assert!(restart_image.finalized_metadata_version() == Some(20));

    engine.downgrade_snapshot_failures_remaining = 0;
    engine.retry_pending_downgrade_snapshot();

    assert2::assert!(engine.downgrade_snapshot_pending.is_none());
    assert2::assert!(published_image.borrow().finalized_metadata_version() == Some(20));
    assert2::assert!(published_image.borrow().topic("after-downgrade").is_some());
    assert2::assert!(
        engine
            .image
            .topic("snapshot-reload")
            .unwrap()
            .replication_factor
            == 1
    );
    assert2::assert!(engine.latest_snapshot_id().is_some());
    assert2::assert!(engine.log.log_start_offset() == downgrade_end);
    assert2::assert!(engine.last_snapshot_end_offset == downgrade_end);
    let checkpoint = load_latest_checkpoint(&engine.data_dir)
        .expect("read downgrade checkpoint")
        .expect("downgrade checkpoint exists");
    let contents =
        crate::snapshot::SnapshotReader::read(&checkpoint).expect("decode downgrade checkpoint");
    let checkpoint_image =
        MetadataImage::from_records(engine.image.cluster_id(), &contents.metadata_records);
    assert2::assert!(checkpoint_image.finalized_metadata_version() == Some(16));
    assert2::assert!(checkpoint_image.topic("after-downgrade").is_none());
}

#[tokio::test]
async fn restart_finishes_downgrade_checkpoint_before_exposing_the_image() {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let data_dir = dir.path().to_path_buf();
    elect_single_voter_engine(&mut engine);

    for records in [
        metadata_version_update(25),
        metadata_version_update(16),
        topic_record("committed-after-downgrade"),
    ] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &records);
        assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        if engine.image.finalized_metadata_version() == Some(25) {
            engine.downgrade_snapshot_failures_remaining = usize::MAX;
        }
    }
    let downgrade_end = engine
        .downgrade_snapshot_pending
        .as_ref()
        .expect("downgrade remains pending before restart")
        .end_offset;
    assert2::assert!(engine.latest_snapshot_id().is_none());
    drop(engine);

    let controller =
        reopen_single_voter(data_dir).expect("restart completes mandatory downgrade recovery");

    assert2::assert!(controller.current_image().finalized_metadata_version() == Some(16));
    assert2::assert!(
        controller
            .current_image()
            .topic("committed-after-downgrade")
            .is_some()
    );
    drop(controller);
    let recovered_log = KraftLog::open(dir.path(), &crate::MetadataLogConfig::default())
        .expect("inspect recovered log");
    assert2::assert!(recovered_log.log_start_offset() == downgrade_end);
    let checkpoint = load_latest_checkpoint(dir.path())
        .expect("read checkpoint")
        .expect("checkpoint exists");
    let contents = crate::snapshot::SnapshotReader::read(&checkpoint).expect("decode checkpoint");
    let checkpoint_image =
        MetadataImage::from_records(uuid::Uuid::nil(), &contents.metadata_records);
    assert2::assert!(checkpoint_image.finalized_metadata_version() == Some(16));
    assert2::assert!(
        checkpoint_image
            .topic("committed-after-downgrade")
            .is_none()
    );
}

#[tokio::test]
async fn restart_recovers_checkpoint_written_before_downgrade_prune() {
    let (engine, dir) = uncheckpointed_downgrade();
    let data_dir = dir.path().to_path_buf();
    let pending = engine
        .downgrade_snapshot_pending
        .clone()
        .expect("downgrade remains pending");
    let bytes = crate::snapshot::SnapshotWriter::serialize(&pending.image, 0)
        .expect("serialize pending image");
    write_checkpoint(dir.path(), pending.end_offset.0, pending.epoch, &bytes)
        .expect("simulate checkpoint-before-prune crash");
    assert2::assert!(engine.log.log_start_offset() < pending.end_offset);
    drop(engine);

    let controller =
        reopen_single_voter(data_dir).expect("restart finishes checkpoint-before-prune recovery");
    assert2::assert!(controller.current_image().finalized_metadata_version() == Some(16));
    drop(controller);
    let recovered_log = KraftLog::open(dir.path(), &crate::MetadataLogConfig::default())
        .expect("inspect recovered log");
    assert2::assert!(recovered_log.log_start_offset() == pending.end_offset);
}

#[tokio::test]
async fn restart_propagates_persistent_downgrade_recovery_error() {
    let (engine, dir) = uncheckpointed_downgrade();
    let data_dir = dir.path().to_path_buf();
    let pending = engine
        .downgrade_snapshot_pending
        .clone()
        .expect("the downgrade checkpoint is still pending");
    drop(engine);

    // The checkpoints share the metadata partition directory with the log,
    // so the directory itself must stay. A directory where the checkpoint's
    // temporary file goes makes every write of that one checkpoint fail.
    std::fs::create_dir(data_dir.join(format!(
        "{:020}-{:010}.tmp",
        pending.end_offset.0, pending.epoch
    )))
    .expect("block the downgrade checkpoint");
    let result = reopen_single_voter(data_dir);
    let Err(error) = result else {
        panic!("persistent mandatory-checkpoint failure must fail open");
    };
    assert2::assert!(matches!(error, RaftError::Storage(_)));
}
