//! A generation-preserving topic deletion planned from each guarded image.

use {
    super::{
        audit::{audit_deleted_topics, deleted_topic_resources},
        authz::{Admission, admit_row, topic_access},
        gate::{consumed_proposal_id, delete_topic_records},
        teardown::LocalDeletion,
        tiering::{spawn_remote_cascades, tiered_partitions},
        wire::{delete_topic_result, refused_topic_result},
    },
    crate::{
        break_glass::handlers::audit::{GatedTransition, audit_transition, require_transition},
        broker::Broker,
        codes,
        freeze::resolve::{FreezeMutationResolution, FreezeVerdict, resolve_freeze_mutation},
        handlers::{RequestContext, partition_transition::audit_refusal},
        remote_log_manager::ArchiveMode,
        time_util::now_ms,
    },
    krabka_audit::PrivilegedPhase,
    krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord},
    krabka_protocol::{
        owned::delete_topics_response::DeletableTopicResult, primitives::uuid::Uuid as WireUuid,
    },
    krabka_raft::RaftError,
    krabka_verified::FreezeMutationKind,
    std::{future::Future, sync::Arc},
    uuid::Uuid,
};

/// Reject disappearance or recreation without retargeting the deletion.
pub(super) fn generation_refusal(
    image: &MetadataImage,
    name: &str,
    topic_id: WireUuid,
    by_id: bool,
) -> Option<DeletableTopicResult> {
    image
        .topic(name)
        .is_none_or(|topic| topic.topic_id.as_bytes() != &topic_id.0)
        .then(|| delete_topic_result(Some(name.to_owned()), topic_id, vanished_code(by_id)))
}

pub(super) fn vanished_code(by_id: bool) -> i16 {
    if by_id {
        codes::UNKNOWN_TOPIC_ID
    } else {
        codes::UNKNOWN_TOPIC_OR_PARTITION
    }
}

/// Recheck the topic generation, permissions and safety gates before append.
///
/// # Errors
/// Returns submission conflicts for the caller to replan, or a controller
/// failure. Authorization and policy refusals are successful response rows.
pub(super) async fn delete_topic_once(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    name: &str,
    wire_topic_id: WireUuid,
    by_id: bool,
    image: Arc<MetadataImage>,
) -> Result<DeletableTopicResult, RaftError> {
    if let Some(row) = generation_refusal(&image, name, wire_topic_id, by_id) {
        return Ok(row);
    }
    let access = topic_access(broker, &image, ctx, [name]);
    if let Admission::Answer(row) = admit_row(
        (Some(name.to_owned()), by_id, wire_topic_id),
        &image,
        &access,
    ) {
        return Ok(row);
    }
    let records = match deletion_records(broker, ctx, name, wire_topic_id, &image).await {
        Ok(records) => records,
        Err(row) => return Ok(row),
    };
    complete_topic_deletion(broker, ctx, name, wire_topic_id, image, records).await?;
    Ok(delete_topic_result(
        Some(name.to_owned()),
        wire_topic_id,
        codes::NONE,
    ))
}

/// The task owns commit and all postcommit side effects. Dropping the join wait
/// (a client deadline or cancellation) detaches it without aborting cleanup.
async fn owned_commit<T: Send + 'static>(
    submit: impl Future<Output = Result<T, RaftError>> + Send + 'static,
    complete: impl FnOnce() + Send + 'static,
) -> Result<T, RaftError> {
    tokio::spawn(async move {
        let committed = submit.await?;
        complete();
        Ok(committed)
    })
    .await
    .map_err(|error| {
        RaftError::ChangeRejected(format!("topic deletion completion failed: {error}"))
    })?
}

async fn complete_topic_deletion(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    name: &str,
    wire_topic_id: WireUuid,
    image: Arc<MetadataImage>,
    records: Vec<MetadataRecord>,
) -> Result<(), RaftError> {
    let local = broker.partitions.partitions_of(name);
    let cleanup = LocalDeletion::snapshot(broker, name, Uuid::from_bytes(wire_topic_id.0), &local);
    let tiered = tiered_partitions(broker, &broker.partitions, &image, name, &local);
    let controller = broker.controller.clone();
    let reader = broker.remote_reader.clone();
    let broker_id = broker.config.broker_id;
    let archive = ArchiveMode::from_worm(broker.config.remote_storage_worm.as_ref());
    let applied_audit = completion_audit(
        broker,
        ctx,
        name,
        wire_topic_id,
        records.first().and_then(consumed_proposal_id),
    );
    owned_commit(
        async move { controller.submit_change_if_image(records, image).await },
        move || {
            cleanup.remove();
            spawn_remote_cascades(reader, broker_id, archive, tiered);
            applied_audit();
        },
    )
    .await?;
    Ok(())
}

fn completion_audit(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    name: &str,
    topic_id: WireUuid,
    proposal_id: Option<Uuid>,
) -> impl FnOnce() + Send + 'static + use<> {
    let audit = broker.audit_log.clone();
    let config = broker.config.break_glass.clone();
    let principal = ctx.principal.clone();
    let peer = *ctx.peer;
    let name = name.to_owned();
    move || {
        // Both audit helpers consume only principal and source endpoint.
        let ctx = RequestContext::new(&principal, &peer, None, "", false, "");
        audit_transition(&audit, &config, &ctx, &GatedTransition {
            action: BreakGlassAction::DeleteTopic,
            target: &name,
            phase: PrivilegedPhase::Applied,
            proposal_id,
            reason: "topic deleted",
        });
        let row = delete_topic_result(Some(name), topic_id, codes::NONE);
        audit_deleted_topics(
            &audit,
            &ctx,
            deleted_topic_resources(std::slice::from_ref(&row)),
        );
    }
}

async fn deletion_records(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    name: &str,
    wire_topic_id: WireUuid,
    image: &MetadataImage,
) -> Result<Vec<MetadataRecord>, DeletableTopicResult> {
    // KFC-9: a write freeze refuses every operation that removes data
    // from the topic it covers, and it answers ahead of the two-person
    // rule. That order is the rule: a break-glass approval to delete does
    // not defeat a freeze, and a deletion the freeze refuses must not
    // spend an approval on its way to being refused.
    //
    // Like the produce gate, this refusal emits no privileged-action audit
    // event. A freeze is not a break-glass act, and the registry entry that
    // caused the refusal is already in the metadata log and in the audit
    // record of the freeze that set it.
    if let FreezeMutationResolution::Frozen(record) =
        resolve_freeze_mutation(image, name, true, FreezeMutationKind::DeleteTopic)
    {
        let verdict = FreezeVerdict::from(record);
        let message = verdict.removal_message();
        tracing::warn!(topic = %name, refusal = %message, "DeleteTopics refused by a freeze");
        return Err(refused_topic_result(
            name.to_owned(),
            wire_topic_id,
            codes::POLICY_VIOLATION,
            message,
        ));
    }

    // KFC-9: the two-person rule, and the records this append carries. It
    // runs before the broker snapshots any partition state, because a
    // deletion the broker will refuse has no reason to walk the topic's
    // logs.
    let records = match delete_topic_records(image, &broker.config.break_glass, name, now_ms()) {
        Ok(records) => records,
        Err(denial) => {
            let message = denial.to_string();
            audit_refusal(
                broker,
                ctx,
                BreakGlassAction::DeleteTopic,
                || name,
                &denial,
                &message,
            );
            return Err(refused_topic_result(
                name.to_owned(),
                wire_topic_id,
                codes::POLICY_VIOLATION,
                message,
            ));
        }
    };
    let proposal_id = records.first().and_then(consumed_proposal_id);
    if let Err(error) = require_transition(
        &broker.audit_log,
        &broker.config.break_glass,
        ctx,
        &GatedTransition {
            action: BreakGlassAction::DeleteTopic,
            target: name,
            phase: PrivilegedPhase::Applied,
            proposal_id,
            reason: "topic deletion admitted",
        },
    )
    .await
    {
        let message = format!("privileged action refused: {error}");
        return Err(refused_topic_result(
            name.to_owned(),
            wire_topic_id,
            codes::POLICY_VIOLATION,
            message,
        ));
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::BreakGlassConfig, handlers::forward_to_controller::guarded_operation_on_source,
            metadata_source::MetadataSource, test_support::FakeMetadataSource,
        },
        assert2::assert,
        krabka_metadata::{BrokerConfigRecord, TopicRecord},
        krabka_raft::{NodeId, SubmitChangeResult},
        std::{
            sync::atomic::{AtomicBool, Ordering},
            time::Duration,
        },
        uuid::Uuid,
    };

    fn topic_image(id: u128) -> MetadataImage {
        MetadataImage::from_records(Uuid::nil(), &[MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: Uuid::from_u128(id),
            partitions: 1,
            replication_factor: 1,
        })])
    }

    #[tokio::test]
    async fn deletion_timeout_does_not_cancel_late_commit_cleanup() {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .image(topic_image(1))
                .leader(Some(NodeId(1)))
                .build(),
        );
        let image = source.current_image();
        let records = delete_topic_records(&image, &BreakGlassConfig::default(), "orders", 0)
            .expect("ungated deletion");
        let dir = tempfile::tempdir().unwrap();
        let canonical = dir.path().join("orders-0");
        std::fs::create_dir(&canonical).unwrap();
        std::fs::write(canonical.join("old-records"), b"deleted generation").unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();
        let (cleaned_tx, cleaned_rx) = tokio::sync::oneshot::channel();
        let submit_source = Arc::clone(&source);
        let cleanup_dir = canonical.clone();
        let waiter = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(1),
                owned_commit(
                    async move {
                        started_tx.send(()).unwrap();
                        commit_rx.await.unwrap();
                        submit_source.submit_change_if_image(records, image).await
                    },
                    move || {
                        std::fs::remove_dir_all(cleanup_dir).unwrap();
                        cleaned_tx.send(()).unwrap();
                    },
                ),
            )
            .await
        });
        started_rx.await.unwrap();
        assert!(waiter.await.unwrap().is_err());
        assert!(source.submitted().is_empty());
        assert!(canonical.exists());

        // A durable completion after the client deadline still owns cleanup.
        commit_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), cleaned_rx)
            .await
            .expect("detached deletion completion")
            .unwrap();
        assert!(source.submitted().len() == 1);
        assert!(!canonical.exists());
    }

    /// A concurrent unrelated commit and a temporarily uncommitted tail must
    /// be retried; a replacement topic must survive either request shape.
    #[tokio::test]
    async fn deletion_retries_conflicts_without_retargeting_a_topic_generation() {
        for (recreated, pending_tail, by_id, expected_code, submissions) in [
            (false, false, false, codes::NONE, 1),
            (false, true, true, codes::NONE, 2),
            (true, false, false, codes::UNKNOWN_TOPIC_OR_PARTITION, 0),
            (true, false, true, codes::UNKNOWN_TOPIC_ID, 0),
        ] {
            let initial = topic_image(1);
            let mut newer = topic_image(if recreated { 2 } else { 1 });
            newer.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: "unrelated".into(),
                config_value: Some("value".into()),
            }));
            let pending = AtomicBool::new(pending_tail);
            let source = FakeMetadataSource::builder()
                .image(initial)
                .leader(Some(NodeId(1)))
                .before_guarded_submit(move |source| source.set_image(newer))
                .on_submit(move |_| {
                    if pending.swap(false, Ordering::Relaxed) {
                        Err(RaftError::UncommittedTail)
                    } else {
                        Ok(SubmitChangeResult::default())
                    }
                })
                .build();
            let id = WireUuid(Uuid::from_u128(1).into_bytes());
            let result = guarded_operation_on_source(
                &source,
                tokio::time::Instant::now() + Duration::from_secs(1),
                |image| {
                    let source = &source;
                    async move {
                        if let Some(row) = generation_refusal(&image, "orders", id, by_id) {
                            return Ok(row);
                        }
                        let records =
                            delete_topic_records(&image, &BreakGlassConfig::default(), "orders", 0)
                                .expect("ungated deletion");
                        source.submit_change_if_image(records, image).await?;
                        Ok(delete_topic_result(Some("orders".into()), id, codes::NONE))
                    }
                },
            )
            .await
            .expect("bounded retry")
            .expect("submission");
            assert!(result == delete_topic_result(Some("orders".into()), id, expected_code));
            assert!(source.submitted().len() == submissions);
        }
    }
}
