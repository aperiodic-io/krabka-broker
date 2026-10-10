//! Tests for the epoch-cache-truncation axis of the remote-retention pass
//! (#1200).

use std::sync::Arc;

use assert2::assert;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_log::LogConfig;
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
};
use uuid::Uuid;

use super::*;
use crate::remote_log_manager::{
    now_ms,
    test_support::{local_backends, tier, tp},
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RetentionSegmentSetup<'a> {
    #[default(Uuid::from_u128(1))]
    id: Uuid,
    #[default(Offset(0))]
    start: Offset,
    #[default(Offset(9))]
    end: Offset,
    #[default(&[(LeaderEpoch(0), Offset(0))])]
    epochs: &'a [(LeaderEpoch, Offset)],
}

/// A finished segment, optionally installed in the metadata manager.
fn finished_segment(
    rlmm: Option<&Arc<dyn RemoteLogMetadataManager>>,
    setup: RetentionSegmentSetup<'_>,
) -> RemoteLogSegmentMetadata {
    let RetentionSegmentSetup {
        id,
        start,
        end,
        epochs,
    } = setup;
    let segment_id = RemoteLogSegmentId::new(tp(), id);
    let started = RemoteLogSegmentMetadata::new(
        segment_id.clone(),
        start.0,
        end.0,
        100,
        1,
        100,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            100,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs
                .iter()
                .map(|&(epoch, first)| (epoch, first.0))
                .collect(),
        ),
    )
    .unwrap();
    let update = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: segment_id,
        event_timestamp_ms: 100,
        custom_metadata: None,
        state: RemoteLogSegmentState::CopySegmentFinished,
        broker_id: 1,
    };
    if let Some(rlmm) = rlmm {
        rlmm.add_remote_log_segment_metadata(started.clone())
            .unwrap();
        rlmm.update_remote_log_segment_metadata(update.clone())
            .unwrap();
    }
    started.with_update(&update).unwrap()
}

/// Kafka's `deleteLogSegmentsDueToLeaderEpochCacheTruncation` deletes the
/// segments whose epochs all lie below the leader's earliest epoch, and none
/// that reaches it. The rule is off without an earliest epoch.
#[test]
fn the_epoch_cache_eviction_set_holds_the_segments_below_the_earliest_epoch() {
    let below = finished_segment(
        None,
        RetentionSegmentSetup {
            epochs: &[(LeaderEpoch(0), Offset(0)), (LeaderEpoch(1), Offset(5))],
            ..Default::default()
        },
    );
    let reaches = finished_segment(
        None,
        RetentionSegmentSetup {
            id: Uuid::from_u128(2),
            start: Offset(10),
            end: Offset(19),
            epochs: &[(LeaderEpoch(1), Offset(10)), (LeaderEpoch(2), Offset(15))],
        },
    );
    let above = finished_segment(
        None,
        RetentionSegmentSetup {
            id: Uuid::from_u128(3),
            start: Offset(20),
            end: Offset(29),
            epochs: &[(LeaderEpoch(3), Offset(20))],
        },
    );
    let segments = [below.clone(), reaches.clone(), above];

    let cases = [
        ("no earliest epoch", None, vec![]),
        ("an earliest epoch nothing is below", Some(0), vec![]),
        ("the epoch a segment starts in", Some(1), vec![]),
        ("past the epochs of the first", Some(2), vec![below.clone()]),
        ("past the epochs of two", Some(3), vec![below, reaches]),
    ];
    for (name, earliest, expected) in cases {
        let evicted = epoch_cache_eviction_set(&segments, earliest.map(LeaderEpoch));
        assert!(evicted == expected, "{name}");
    }
}

/// The pass deletes a segment below the earliest epoch with the retention
/// lifecycle, leaves the rest, and leaves the log start where it is: those
/// offsets are in no lineage the log serves, so no reader is told anything
/// new. It runs with no retention setting and no `DeleteRecords` floor.
#[tokio::test]
async fn remote_retention_pass_deletes_segments_below_the_earliest_epoch() {
    let remote_dir = tempfile::tempdir().unwrap();
    let (rsm, rlmm) = local_backends(remote_dir.path());
    // An unclean election left epoch 1's segment over offsets that the current
    // lineage assigns to epochs 2 and 3.
    finished_segment(
        Some(&rlmm),
        RetentionSegmentSetup {
            start: Offset(10),
            end: Offset(19),
            epochs: &[(LeaderEpoch(1), Offset(10))],
            ..Default::default()
        },
    );
    let current = finished_segment(
        Some(&rlmm),
        RetentionSegmentSetup {
            id: Uuid::from_u128(2),
            start: Offset(20),
            end: Offset(29),
            epochs: &[(LeaderEpoch(2), Offset(20)), (LeaderEpoch(3), Offset(25))],
        },
    );
    // No retention setting: only the epoch axis is in play.
    let cfg = LogConfig {
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    };
    let bounds = |earliest_epoch| RemoteRetentionBounds {
        log_config: &cfg,
        log_start_offset: Offset(0),
        deleted_below: None,
        earliest_epoch,
        now_ms: now_ms(),
        local: LocalLogFootprint::EMPTY,
    };
    let tier = tier(ArchiveMode::Mutable, &rsm, &rlmm);

    let untouched = remote_retention_pass(&tp(), 1, bounds(None), &tier).await;
    assert!(untouched == RemoteRetentionOutcome::default());
    assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == 2);

    let outcome = remote_retention_pass(&tp(), 1, bounds(Some(LeaderEpoch(2))), &tier).await;

    assert!(
        outcome
            == RemoteRetentionOutcome {
                deleted: 1,
                log_start: None,
            }
    );
    let left = rlmm.list_remote_log_segments(&tp()).unwrap();
    assert!(left == [current]);
}
