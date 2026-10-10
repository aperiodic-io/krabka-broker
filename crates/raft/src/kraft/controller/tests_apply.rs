//! Tests for the apply of newly committed records into the published
//! [`MetadataImage`] and for the views that apply refreshes: the leader and
//! quorum watches, and the committed slice an observer fetches.

use assert2::{assert, check};

use super::*;
use crate::kraft::controller::{
    records::{decode_batches, metadata_record_batch},
    recovery::replay_committed_with_timestamp,
    test_support::{
        EngineSetup, build_engine_only, one_offset_batch, topic_record, topic_record_named,
    },
};

#[test]
fn tiny_fetch_budget_does_not_skip_apply_or_replay_records() {
    let tiny = MetadataRaftFetchMax::try_from(krabka_units::bytes(1))
        .expect("one byte still makes progress");
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        metadata_raft_fetch_max: tiny,
        ..Default::default()
    });

    let mut scratch = engine.image.clone();
    for (name, id) in [("first", 1), ("second", 2)] {
        let records = topic_record_named(name, id);
        let mut blobs = Vec::new();
        for record in &records {
            blobs.extend(to_kraft_values(record, &scratch).expect("encode metadata"));
            scratch.apply(record);
        }
        let mut batch = metadata_record_batch(1, &blobs).expect("metadata batch");
        engine
            .log
            .append(&mut batch, 1_700_000_000_123)
            .expect("append metadata");
    }
    check!(
        engine
            .log
            .read_decoded(Offset(0), tiny.size())
            .expect("read first bounded batch")[0]
            .base_offset
            == 0
    );
    check!(
        engine
            .log
            .read_decoded(Offset(2), tiny.size())
            .expect("read second bounded batch")[0]
            .base_offset
            == 2
    );
    engine.advance_and_apply(engine.log.log_end_offset());

    assert2::assert!(engine.image.topic("first").is_some());
    assert2::assert!(engine.image.topic("second").is_some());

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    let (_, recovered_timestamp) =
        replay_committed_with_timestamp(&engine.log, &mut recovered, Offset(0), tiny)
            .expect("replay");
    assert2::assert!(recovered.topic("first").is_some());
    assert2::assert!(recovered.topic("second").is_some());
    assert2::assert!(recovered_timestamp == Some(1_700_000_000_123));
}

#[test]
fn publish_leader_updates_leader_and_quorum_watchers() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let mut leader_rx = engine.leader_tx.subscribe();
    let quorum_rx = engine.quorum_tx.subscribe();

    engine.on_event(Event::ElectionTimeout);

    check!(
        (
            *leader_rx.borrow_and_update(),
            quorum_rx.borrow().leader_id,
            quorum_rx.borrow().log_end_offset,
        ) == (
            Some(NodeId(1)),
            Some(NodeId(1)),
            engine.log.log_end_offset().0,
        )
    );
}

#[test]
fn quorum_snapshot_timestamp_tracks_applied_not_newer_committed_records() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let noop = super::records::noop_record_value().expect("no-op record");
    let mut batch = metadata_record_batch(1, &[noop]).expect("metadata batch");
    engine
        .log
        .append(&mut batch, 1_700_000_000_123)
        .expect("append");
    engine.advance_and_apply(Offset(1));
    let noop = super::records::noop_record_value().expect("no-op record");
    let mut later_batch = metadata_record_batch(1, &[noop]).expect("metadata batch");
    engine
        .log
        .append(&mut later_batch, 1_700_000_111_222)
        .expect("append later batch");
    // The quorum commits the later batch, but the metadata image has not
    // applied it yet.
    engine.log.advance_hwm(Offset(2));
    engine.leader_reported_hwm = 4;

    let snapshot = engine.quorum_state_snapshot();
    assert2::assert!(snapshot.last_applied_record_timestamp_ms == 1_700_000_000_123);
    assert2::assert!(engine.pending_record_processing_time_samples == 1);
}

#[test]
fn metadata_fetch_slice_excludes_negative_hwm_and_uncommitted_batches() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let mut multi = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 1,
        last_offset_delta: 1,
        records: vec![
            krabka_protocol::records::Record {
                value: Some(bytes::Bytes::from_static(b"a")),
                offset_delta: 0,
                ..Default::default()
            },
            krabka_protocol::records::Record {
                value: Some(bytes::Bytes::from_static(b"b")),
                offset_delta: 1,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    engine.log.append(&mut multi, 0).expect("append multi");
    engine.log.advance_hwm(Offset(1));

    assert2::assert!(
        engine
            .metadata_fetch_slice(-1, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .records
            .is_empty()
    );
    assert2::assert!(
        engine
            .metadata_fetch_slice(1, DEFAULT_METADATA_RAFT_FETCH_MAX)
            .records
            .is_empty()
    );

    let slice = engine.metadata_fetch_slice(0, DEFAULT_METADATA_RAFT_FETCH_MAX);
    let decoded = decode_batches(&slice.records).expect("decode fetch slice");
    check!(
        (
            decoded
                .iter()
                .map(|batch| batch.base_offset)
                .collect::<Vec<_>>(),
            slice.high_watermark,
        ) == (vec![0], 1)
    );
}

#[tokio::test]
async fn committed_batch_applies_to_image() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    assert2::assert!(ctrl.current_image().topic("t").is_none());

    let off = ctrl
        .test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(off >= 0);

    let mut img_rx = ctrl.watch_image();
    assert2::assert!(img_rx.borrow_and_update().topic("t").is_some());
    assert2::assert!(ctrl.current_image().topic("t").is_some());
    ctrl.shutdown().await;
}

#[tokio::test]
async fn duplicate_committed_record_rejected_on_apply() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    ctrl.test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(ctrl.current_image().topic("t").is_some());

    ctrl.test_append_and_commit(topic_record("t"))
        .await
        .unwrap();
    assert2::assert!(ctrl.current_image().topic("t").is_some());
    ctrl.shutdown().await;
}
