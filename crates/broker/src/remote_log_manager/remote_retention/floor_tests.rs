use {
    super::*,
    crate::remote_log_manager::test_support as fixtures,
    assert2::assert,
    fixtures::{local_backends, tier, tp},
    krabka_remote_storage::{
        RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadataUpdate,
    },
    krabka_units::millis,
    uuid::Uuid,
};

#[tokio::test]
async fn completed_remote_delete_uses_a_representable_floor() {
    for (end, expected) in [(i64::MAX - 1, Some(Offset(i64::MAX))), (i64::MAX, None)] {
        let dir = tempfile::tempdir().unwrap();
        let fixtures::Backends { rsm, rlmm } = local_backends(dir.path());
        let id = RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            0,
            end,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id,
            event_timestamp_ms: 0,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
        let config = LogConfig {
            retention: Some(millis(1)),
            ..LogConfig::default()
        };
        let outcome = remote_retention_pass(
            &tp(),
            1,
            RemoteRetentionBounds {
                log_config: &config,
                log_start_offset: Offset(0),
                deleted_below: None,
                earliest_epoch: None,
                now_ms: 10,
                local: LocalLogFootprint::EMPTY,
            },
            &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        )
        .await;
        assert!(outcome.deleted == 1);
        assert!(outcome.log_start == expected);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }
}
