//! Guarded election admission after a real durable audit acknowledgment.

use {
    super::{super::submit_election_after_audit, *},
    crate::{
        handlers::forward_to_controller::guarded_operation_on_source,
        leader_election::select_new_leader_for_partition,
        metadata_source::MetadataSource,
        test_support::{FakeMetadataSource, paused_audit::PausedAudit},
    },
    assert2::assert,
    krabka_audit::{AuditEvent, LifecycleKind},
    krabka_raft::RaftError,
    std::sync::Arc,
};

#[tokio::test]
async fn election_batch_rejects_changes_while_durable_audit_is_pending() {
    for changes_image in [true, false] {
        let old = image_with(&[]);
        let expected = PartitionRecord {
            leader: NodeId(1),
            isr: vec![NodeId(1), NodeId(2)],
            partition_epoch: 1,
            ..old.partition(TOPIC, 0).expect("seeded partition").clone()
        };
        let mut newer = old.clone();
        newer.apply(&MetadataRecord::V1Partition(expected.clone()));
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(old)
                .leader(Some(NodeId(7)))
                .build(),
        );
        let image = source.current_image();
        let (audit, pause, writer) = PausedAudit::start();
        let committing_source = source.clone();
        let admission = audit.clone();
        let commit = tokio::spawn(async move {
            submit_election_after_audit(
                committing_source.as_ref(),
                image,
                vec![MetadataRecord::V1Partition(elected())],
                admission.emit_required(AuditEvent::Lifecycle {
                    kind: LifecycleKind::ConfigApplied,
                    node_id: 7,
                    time_ms: 0,
                }),
            )
            .await
        });
        let established = pause
            .release_after(|| {
                if changes_image {
                    source.set_image(newer);
                } else {
                    source.set_leader(Some(NodeId(8)));
                }
                assert!(source.submitted().is_empty(), "audit has not acknowledged");
                source.current_image()
            })
            .await;
        let failure = commit.await.expect("election admission completes");

        assert!(
            matches!(
                failure,
                Err(RaftError::StaleMetadataImage) if changes_image
            ) || matches!(failure, Err(RaftError::NotLeader { .. }) if !changes_image)
        );
        assert!(source.submitted().is_empty());
        assert!(Arc::ptr_eq(&source.current_image(), &established));
        if changes_image {
            assert!(established.partition(TOPIC, 0) == Some(&expected));
        }
        audit.close();
        writer.await.expect("audit writer finishes");
        assert!(pause.records().len() == 1);
    }
}

#[tokio::test]
async fn election_batch_rejects_a_pending_tail_without_audit_admission() {
    let source = FakeMetadataSource::builder()
        .image(image_with(&[]))
        .leader(Some(NodeId(7)))
        .on_submit(|_| Err(RaftError::UncommittedTail))
        .build();
    let image = source.current_image();

    let failure = submit_election_after_audit(
        &source,
        image.clone(),
        vec![MetadataRecord::V1Partition(elected())],
        std::future::ready(Ok(())),
    )
    .await;

    assert!(matches!(failure, Err(RaftError::UncommittedTail)));
    assert!(Arc::ptr_eq(&source.current_image(), &image));
}

#[tokio::test]
async fn election_retry_replans_the_original_operation_against_fresh_metadata() {
    let old = image_with(&[]);
    let mut newer = old.clone();
    newer.apply(&MetadataRecord::V1Partition(PartitionRecord {
        replicas: vec![NodeId(1), NodeId(3)],
        leader_epoch: LeaderEpoch(9),
        partition_epoch: 7,
        ..old.partition(TOPIC, 0).expect("seeded partition").clone()
    }));
    let source = FakeMetadataSource::builder()
        .image(old)
        .leader(Some(NodeId(7)))
        .before_guarded_submit(move |source| source.set_image(newer))
        .build();
    let alive = HashSet::from([2, 3]);
    let witnesses = HashSet::new();

    let result = guarded_operation_on_source(
        &source,
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        |image| {
            let source = &source;
            let alive = &alive;
            let witnesses = &witnesses;
            async move {
                let elected = select_new_leader_for_partition(
                    &image,
                    alive,
                    witnesses,
                    TOPIC,
                    0,
                    ElectionType::Unclean,
                )
                .expect("each assignment has one alive candidate");
                submit_election_after_audit(
                    source,
                    image,
                    vec![MetadataRecord::V1Partition(elected.clone())],
                    std::future::ready(Ok(())),
                )
                .await?
                .expect("audit admission is disabled");
                Ok(elected)
            }
        },
    )
    .await
    .expect("bounded retry completes")
    .expect("fresh operation commits");

    let expected = PartitionRecord {
        isr: vec![NodeId(3)],
        leader_epoch: LeaderEpoch(10),
        partition_epoch: 8,
        ..replicated_partition(ReplicatedPartitionSetup {
            topic: TOPIC,
            leader: NodeId(3),
            replicas: &[NodeId(1), NodeId(3)],
            ..Default::default()
        })
    };
    assert!(result == expected);
    let submitted = source.submitted();
    assert!(
        submitted.len() == 1,
        "the obsolete batch never reaches submit_change"
    );
    assert!(*one_partition_change(&submitted[0]) == expected);
}
