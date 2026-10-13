//! Election admission must remain current across the durable audit await.

use {super::*, crate::test_support::paused_audit::PausedAudit, assert2::assert};

#[tokio::test]
async fn recovery_commit_rejects_changes_while_durable_audit_is_pending() {
    for changes_image in [true, false] {
        let mut original = image_with_partition(1, &[1, 2, 3]);
        publish_elr(&mut original, &[2]);
        let mut newer = original.clone();
        let expected = PartitionRecord {
            leader: NodeId(3),
            isr: vec![NodeId(3)],
            leader_epoch: krabka_metadata::LeaderEpoch(6),
            partition_epoch: 1,
            ..original
                .partition("t", 0)
                .expect("seeded partition")
                .clone()
        };
        newer.apply(&MetadataRecord::V1Partition(expected.clone()));
        publish_elr(&mut newer, &[1]);
        newer.apply(&MetadataRecord::V1PartitionRecovery(
            PartitionRecoveryRecord {
                topic: "t".into(),
                partition: 0,
                state: LeaderRecoveryState::Recovering,
            },
        ));
        let source = source_with(Some(NODE), original);
        let image = source.current_image();
        let (audit, pause, writer) = PausedAudit::start();
        let manager = manager_with(
            source.clone(),
            liveness_with_alive(&[2, 3]).await,
            &gated(BackgroundUncleanRecovery::AuditOnly),
            audit.clone(),
        );
        let commit = tokio::spawn(async move {
            let outcome = commit_fixture_election(&manager, &image, &job(), fallback_to(2)).await;
            (manager, outcome)
        });
        let established = pause
            .release_after(|| {
                assert!(source.submitted().is_empty());
                if changes_image {
                    source.set_image(newer);
                } else {
                    source.set_leader(Some(NodeId(NODE + 1)));
                }
                source.current_image()
            })
            .await;
        let (manager, outcome) = commit.await.expect("recovery attempt completes");

        assert!(outcome == RecoveryOutcome::Stale);
        assert!(source.submitted().is_empty());
        assert!(Arc::ptr_eq(&source.current_image(), &established));
        if changes_image {
            assert!(established.partition("t", 0) == Some(&expected));
            assert!(established.partition_elr("t", 0).0 == [NodeId(1)]);
        }
        assert!(manager.metrics.unclean_leader_elections_total.get() == 0);
        assert!(bypasses(&manager.metrics) == 0);
        audit.close();
        writer.await.expect("audit writer finishes");
        assert!(
            pause.records().len() == 1,
            "only the attempted election was audited"
        );
    }
}

#[tokio::test]
async fn recovery_commit_rejects_an_uncommitted_metadata_tail() {
    let source = Arc::new(
        FakeMetadataSource::builder()
            .image(image_with_partition(1, &[1, 2]))
            .leader(Some(NodeId(NODE)))
            .on_submit(|_| Err(RaftError::UncommittedTail))
            .build(),
    );
    let image = source.current_image();
    let manager = manager(source.clone(), liveness_with_alive(&[2]).await);

    let outcome = commit_fixture_election(&manager, &image, &job(), fallback_to(2)).await;

    assert!(outcome == RecoveryOutcome::Stale);
    assert!(Arc::ptr_eq(&source.current_image(), &image));
    assert!(manager.metrics.unclean_leader_elections_total.get() == 0);
    assert!(bypasses(&manager.metrics) == 0);
}
