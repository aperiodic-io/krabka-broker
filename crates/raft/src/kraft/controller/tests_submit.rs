//! Tests for `submit_change`: the offsets a submission is assigned, the
//! waiters it parks, and how those waiters resolve, fail on leadership loss,
//! or take a per-record rejection scoped to their own appended range.

use std::time::Duration as StdDuration;

use assert2::{assert, check};
use krabka_ids::Offset;
use tokio::sync::oneshot;

use super::{CommitWaiter, Engine};
use crate::{
    SubmitChangeResult,
    error::RaftError,
    kraft::{
        controller::test_support::{
            ControllerSetup, EngineSetup, await_leader, build, build_engine_only, commit_pending,
            one_offset_batch, submit_change_with_timeout, topic_record, topic_record_named,
        },
        event::Event,
        types::NodeId,
    },
};

async fn acknowledge_tip(ctrl: &crate::kraft::KraftController, follower: NodeId) {
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    commit_pending(ctrl, follower).await;
}

fn waiter_offsets(engine: &super::Engine) -> Vec<Offset> {
    engine
        .commit_waiters
        .iter()
        .map(|waiter| waiter.need_offset)
        .collect()
}

fn check_future_waiter(
    engine: &super::Engine,
    receiver: &mut oneshot::Receiver<Result<SubmitChangeResult, RaftError>>,
) {
    assert2::assert!(matches!(
        receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert2::assert!(waiter_offsets(engine) == vec![Offset(6)]);
}

fn park_waiter(
    engine: &mut super::Engine,
    base: i64,
    need: i64,
) -> oneshot::Receiver<Result<SubmitChangeResult, RaftError>> {
    let (reply, receiver) = oneshot::channel();
    engine.commit_waiters.push(CommitWaiter {
        base_offset: Offset(base),
        need_offset: Offset(need),
        rejection: None,
        creates: Vec::new(),
        result: SubmitChangeResult::default(),
        reply,
    });
    receiver
}

#[test]
fn direct_single_voter_submit_applies_image_and_resolves_waiter() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    assert2::assert!(engine.image.topic("direct").is_none());

    let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record("direct"));

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    check!(engine.image.topic("direct").is_some());
    check!(engine.log.hwm() == engine.log.log_end_offset());
    check!(engine.commit_waiters.is_empty());
}

#[test]
fn offset_advance_submit_returns_actor_ordered_base() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record("topic"));
    assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let mut rx = super::test_support::submit_on_engine(&mut engine, &offset_advance("topic", 3));
    let first = rx.try_recv().expect("first reply").expect("first ok");
    let mut rx = super::test_support::submit_on_engine(&mut engine, &offset_advance("topic", 5));
    let second = rx.try_recv().expect("second reply").expect("second ok");

    assert!(first.offset_reservations[0].base_offset == 0);
    assert!(first.offset_reservations[0].count == 3);
    assert!(second.offset_reservations[0].base_offset == 3);
    assert!(second.offset_reservations[0].count == 5);
    assert!(engine.image.partition_next_offset("topic", 0) == Some(8));
}

#[test]
fn offset_advance_submit_rejects_counts_outside_verified_domain() {
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();

    for records in [topic_record("topic"), offset_advance("topic", 1)] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &records);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    let log_end = engine.log.log_end_offset();

    for count in [-1, 0, i64::MAX] {
        let mut rx =
            super::test_support::submit_on_engine(&mut engine, &offset_advance("topic", count));

        assert!(matches!(
            rx.try_recv(),
            Ok(Err(RaftError::ChangeRejected(_)))
        ));
        assert!(engine.image.partition_next_offset("topic", 0) == Some(1));
        assert!(engine.log.log_end_offset() == log_end);
    }
}

#[tokio::test]
async fn pending_offset_reservations_are_contiguous_before_commit() {
    let (ctrl, _dir) = super::test_support::three_voter_leader().await;

    let create = super::test_support::spawn_submit(&ctrl, topic_record("topic"));
    acknowledge_tip(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    let create_other = super::test_support::spawn_submit(&ctrl, topic_record("other"));
    acknowledge_tip(&ctrl, NodeId(2)).await;
    create_other.await.unwrap().unwrap();

    let first_ctrl = ctrl.clone();
    let first =
        tokio::spawn(async move { first_ctrl.submit_change(offset_advance("topic", 3)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let second_ctrl = ctrl.clone();
    let second =
        tokio::spawn(async move { second_ctrl.submit_change(offset_advance("topic", 5)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let third_ctrl = ctrl.clone();
    let third =
        tokio::spawn(async move { third_ctrl.submit_change(offset_advance("other", 4)).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let qs = super::test_support::commit_pending(&ctrl, NodeId(2)).await;

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    let third = third.await.unwrap().unwrap();
    assert!(first.offset_reservations[0].base_offset == 0);
    assert!(first.offset_reservations[0].count == 3);
    assert!(first.offset_reservations[0].leader_epoch == u64::from(qs.leader_epoch));
    assert!(second.offset_reservations[0].base_offset == 3);
    assert!(second.offset_reservations[0].count == 5);
    assert!(second.offset_reservations[0].leader_epoch == u64::from(qs.leader_epoch));
    assert!(third.offset_reservations[0].base_offset == 0);
    assert!(third.offset_reservations[0].count == 4);
    assert!(third.offset_reservations[0].leader_epoch == u64::from(qs.leader_epoch));
    assert!(ctrl.current_image().partition_next_offset("topic", 0) == Some(8));
    assert!(ctrl.current_image().partition_next_offset("other", 0) == Some(4));
    ctrl.shutdown().await;
}

async fn check_rejected_without_append(
    ctrl: &crate::kraft::KraftController,
    record: krabka_metadata::MetadataRecord,
    log_end: i64,
) {
    let result = ctrl.submit_change(vec![record]).await;
    assert2::assert!(matches!(result, Err(RaftError::ChangeRejected(_))));
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);
}

fn break_glass_proposal(
    id: u128,
    action: krabka_metadata::BreakGlassAction,
    target: &str,
    expires_at_ms: i64,
) -> krabka_metadata::BreakGlassProposalRecord {
    krabka_metadata::BreakGlassProposalRecord {
        proposal_id: uuid::Uuid::from_u128(id),
        action,
        target: target.to_owned(),
        proposer: "User:alice".to_owned(),
        reason: "incident".to_owned(),
        created_at_ms: 1,
        expires_at_ms,
        approvals: Vec::new(),
        consumed_at_ms: 0,
        withdrawn: false,
    }
}

#[tokio::test]
async fn break_glass_consume_is_exact_and_single_flight_until_commit() {
    use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataRecord};

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;
    let proposal = break_glass_proposal(0x271, BreakGlassAction::DeleteRecords, "orders-3", 1_000);

    let create = super::test_support::spawn_submit(
        &ctrl,
        vec![MetadataRecord::V1BreakGlassProposal(proposal.clone())],
    );
    acknowledge_tip(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    for malformed in [
        BreakGlassProposalRecord {
            consumed_at_ms: -1,
            ..proposal.clone()
        },
        BreakGlassProposalRecord {
            target: "orders-4".to_owned(),
            consumed_at_ms: 10,
            ..proposal.clone()
        },
    ] {
        check_rejected_without_append(
            &ctrl,
            MetadataRecord::V1BreakGlassProposal(malformed),
            log_end,
        )
        .await;
    }

    let consumed = BreakGlassProposalRecord {
        consumed_at_ms: i64::MAX,
        ..proposal.clone()
    };
    let first = super::test_support::pending_submission(
        &ctrl,
        vec![MetadataRecord::V1BreakGlassProposal(consumed.clone())],
    )
    .await;

    let concurrent = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_change(vec![MetadataRecord::V1BreakGlassProposal(consumed.clone())]),
    )
    .await
    .expect("a concurrent consume must be rejected before append");
    // The first consume is appended but not committed. The refusal is the
    // transient one that a caller retries once the tail commits.
    assert2::assert!(matches!(concurrent, Err(RaftError::UncommittedTail)));

    commit_pending(&ctrl, NodeId(2)).await;
    first.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .break_glass_proposal(proposal.proposal_id)
            .is_some_and(|stored| stored.consumed_at_ms == i64::MAX)
    );

    let retry = ctrl
        .submit_change(vec![MetadataRecord::V1BreakGlassProposal(consumed)])
        .await;
    assert2::assert!(matches!(retry, Err(RaftError::ChangeRejected(_))));
    ctrl.shutdown().await;
}

/// A new leader refuses a break-glass consume with the retriable
/// `UncommittedTail` until it commits a record from its own epoch.
///
/// Raft and `KRaft` both require a new leader to commit in its own epoch
/// before it exposes the committed state of earlier epochs. The proposal
/// committed under the first leadership, but the new leader's epoch-start
/// record is its uncommitted tail, so the consume must wait. Kafka's
/// controller answers `NOT_CONTROLLER` in the same window.
#[tokio::test]
async fn a_new_leader_refuses_a_consume_until_its_own_epoch_commits() {
    use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataRecord};

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;
    commit_pending(&ctrl, NodeId(2)).await;
    let proposal = break_glass_proposal(0x591, BreakGlassAction::DeleteTopic, "doomed", i64::MAX);
    let create = super::test_support::pending_submission(
        &ctrl,
        vec![MetadataRecord::V1BreakGlassProposal(proposal.clone())],
    )
    .await;
    commit_pending(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    // Node 2 takes over at epoch 5. Node 1 then wins the election for epoch 6
    // and appends its epoch-start record, which nothing has committed yet.
    ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 5,
    })
    .await
    .unwrap();
    await_leader(&ctrl, Some(NodeId(2))).await;
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    for epoch in [5, 6] {
        ctrl.inject_event(Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch,
            vote_granted: true,
        })
        .await
        .unwrap();
    }
    await_leader(&ctrl, Some(NodeId(1))).await;
    let quorum = ctrl.quorum_state().await.unwrap();
    check!(quorum.leader_epoch == 6);
    check!(quorum.high_watermark < quorum.log_end_offset);

    let consumed = MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
        consumed_at_ms: 10,
        ..proposal
    });
    let refused = ctrl.submit_change(vec![consumed.clone()]).await;
    assert!(matches!(refused, Err(RaftError::UncommittedTail)));
    check!(ctrl.quorum_state().await.unwrap().log_end_offset == quorum.log_end_offset);

    // Once the epoch-start record commits, the same consume appends.
    commit_pending(&ctrl, NodeId(2)).await;
    let consume_ctrl = ctrl.clone();
    let consume = tokio::spawn(async move { consume_ctrl.submit_change(vec![consumed]).await });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    commit_pending(&ctrl, NodeId(2)).await;
    consume.await.unwrap().unwrap();
    ctrl.shutdown().await;
}

#[tokio::test]
async fn topic_freeze_replacement_is_newer_only_and_single_flight_until_commit() {
    use krabka_metadata::{MetadataRecord, PatternType, TopicFreezeRecord};

    fn freeze(scope: &str, set_at_ms: i64, frozen: bool) -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type: PatternType::Literal,
            frozen,
            reason: "incident".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms,
            proposal_id: uuid::Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }
    }

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;
    commit_pending(&ctrl, NodeId(2)).await;

    let create = super::test_support::spawn_submit(
        &ctrl,
        vec![MetadataRecord::V1TopicFreeze(freeze("orders", 10, true))],
    );
    acknowledge_tip(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    for rejected in [
        freeze("orders", 10, true),
        freeze("orders", 9, true),
        freeze("missing", 11, false),
    ] {
        check_rejected_without_append(&ctrl, MetadataRecord::V1TopicFreeze(rejected), log_end)
            .await;
    }
    let batch = ctrl
        .submit_change(vec![
            MetadataRecord::V1TopicFreeze(freeze("a", 11, true)),
            MetadataRecord::V1TopicFreeze(freeze("b", 11, true)),
        ])
        .await;
    assert2::assert!(matches!(batch, Err(RaftError::ChangeRejected(_))));
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let replacement = freeze("orders", i64::MAX, true);
    let replace = super::test_support::pending_submission(
        &ctrl,
        vec![MetadataRecord::V1TopicFreeze(replacement.clone())],
    )
    .await;

    let concurrent = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_change(vec![MetadataRecord::V1TopicFreeze(replacement.clone())]),
    )
    .await
    .expect("a concurrent replacement must be rejected before append");
    assert2::assert!(matches!(concurrent, Err(RaftError::UncommittedTail)));

    commit_pending(&ctrl, NodeId(2)).await;
    replace.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .topic_freeze("orders")
            .is_some_and(|stored| stored.set_at_ms == i64::MAX)
    );

    let retry = ctrl
        .submit_change(vec![MetadataRecord::V1TopicFreeze(replacement)])
        .await;
    assert2::assert!(matches!(retry, Err(RaftError::ChangeRejected(_))));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn delegation_token_mutation_is_generation_bound_and_retry_idempotent() {
    use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
    use krabka_security::KafkaPrincipal;

    fn principal(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: name.to_string(),
        }
    }

    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let original = DelegationTokenRecord {
        token_id: "token-273".to_string(),
        owner: principal("alice"),
        requester: principal("alice"),
        issue_timestamp_ms: now - 1_000,
        expiry_timestamp_ms: now + 60_000,
        max_timestamp_ms: now + 600_000,
        renewers: vec![principal("bob")],
    };
    let renewed = DelegationTokenRecord {
        expiry_timestamp_ms: now + 120_000,
        ..original.clone()
    };

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;
    commit_pending(&ctrl, NodeId(2)).await;

    let create = super::test_support::pending_submission(
        &ctrl,
        vec![MetadataRecord::V1DelegationToken(original.clone())],
    )
    .await;
    commit_pending(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    let renew_ctrl = ctrl.clone();
    let renew = {
        let expected = original.clone();
        let replacement = renewed.clone();
        tokio::spawn(async move {
            renew_ctrl
                .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
                    expected,
                    replacement,
                }])
                .await
        })
    };
    tokio::time::sleep(StdDuration::from_millis(20)).await;

    let concurrent_delete = tokio::time::timeout(
        StdDuration::from_secs(1),
        ctrl.submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: original.clone(),
        }]),
    )
    .await
    .expect("concurrent delete must return before the first mutation commits");
    assert2::assert!(matches!(
        concurrent_delete,
        Err(RaftError::ChangeRejected(_))
    ));

    commit_pending(&ctrl, NodeId(2)).await;
    renew.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .delegation_token_by_id(&original.token_id)
            .is_some_and(|token| token.expiry_timestamp_ms == renewed.expiry_timestamp_ms)
    );

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let retry = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
            expected: original.clone(),
            replacement: renewed.clone(),
        }])
        .await;
    assert2::assert!(retry.is_ok());
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let stale = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: original,
        }])
        .await;
    assert2::assert!(matches!(stale, Err(RaftError::ChangeRejected(_))));

    let malformed = DelegationTokenRecord {
        owner: principal("mallory"),
        expiry_timestamp_ms: i64::MAX,
        ..renewed.clone()
    };
    let rejected = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Renew {
            expected: renewed.clone(),
            replacement: malformed,
        }])
        .await;
    assert2::assert!(matches!(rejected, Err(RaftError::ChangeRejected(_))));

    let delete_ctrl = ctrl.clone();
    let delete_expected = renewed.clone();
    let delete = tokio::spawn(async move {
        delete_ctrl
            .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
                expected: delete_expected,
            }])
            .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    commit_pending(&ctrl, NodeId(2)).await;
    delete.await.unwrap().unwrap();
    assert2::check!(
        ctrl.current_image()
            .delegation_token_by_id(&renewed.token_id)
            .is_none()
    );

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let delete_retry = ctrl
        .submit_delegation_token_mutations(vec![crate::DelegationTokenMutation::Delete {
            expected: renewed,
        }])
        .await;
    assert2::assert!(delete_retry.is_ok());
    assert2::check!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn offset_reservation_waits_for_current_epoch_commit_then_retries() {
    use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;

    let log_end = ctrl.quorum_state().await.unwrap().log_end_offset;
    let result = ctrl
        .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count: 1,
            },
        )])
        .await;

    assert!(matches!(result, Err(RaftError::UncommittedTail)));
    assert!(ctrl.quorum_state().await.unwrap().log_end_offset == log_end);

    let create = super::test_support::spawn_submit(&ctrl, topic_record("topic"));
    acknowledge_tip(&ctrl, NodeId(2)).await;
    create.await.unwrap().unwrap();

    let retry_ctrl = ctrl.clone();
    let retry = tokio::spawn(async move {
        retry_ctrl
            .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: "topic".to_string(),
                    partition: 0,
                    count: 1,
                },
            )])
            .await
    });
    acknowledge_tip(&ctrl, NodeId(2)).await;
    let retry = retry.await.unwrap().unwrap();
    assert!(retry.offset_reservations[0].base_offset == 0);
    assert!(ctrl.current_image().partition_next_offset("topic", 0) == Some(1));
    ctrl.shutdown().await;
}

#[test]
fn try_resolve_waiters_resolves_at_exact_hwm_and_keeps_future_waiter() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    for offset in 0..5 {
        let mut batch = one_offset_batch(offset, 1, b"x");
        engine.log.append(&mut batch, 0).expect("append");
    }
    engine.log.advance_hwm(Offset(5));

    let mut ready_rx = park_waiter(&mut engine, 4, 5);
    let mut future_rx = park_waiter(&mut engine, 5, 6);

    engine.try_resolve_waiters();

    assert!(matches!(ready_rx.try_recv(), Ok(Ok(_))));
    check_future_waiter(&engine, &mut future_rx);
}

#[test]
fn fail_waiters_reached_by_fails_only_waiters_at_or_below_target_hwm() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    let mut ready_rx = park_waiter(&mut engine, 4, 5);
    let mut future_rx = park_waiter(&mut engine, 5, 6);

    engine.fail_waiters_reached_by(Offset(5), "test hwm stall");

    assert2::assert!(matches!(
        ready_rx.try_recv(),
        Ok(Err(RaftError::ChangeRejected(_)))
    ));
    check_future_waiter(&engine, &mut future_rx);
}

#[tokio::test]
async fn submit_change_commits_on_single_voter_leader() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    tokio::time::timeout(
        StdDuration::from_secs(5),
        ctrl.submit_change(topic_record("orders")),
    )
    .await
    .expect("submit did not hang")
    .expect("submit ok");
    assert2::assert!(ctrl.current_image().topic("orders").is_some());

    let qs = ctrl.quorum_state().await.unwrap();
    assert2::assert!(qs.leader_id == Some(NodeId(1)));
    assert2::assert!(qs.high_watermark > 0);
    ctrl.shutdown().await;
}

#[tokio::test]
async fn submit_change_duplicate_rejected() {
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    submit_change_with_timeout(&ctrl, topic_record("t"), "first duplicate-test submit")
        .await
        .unwrap();
    let dup = submit_change_with_timeout(&ctrl, topic_record("t"), "duplicate-test submit").await;
    assert2::assert!(matches!(dup, Err(RaftError::Metadata(_))));
    ctrl.shutdown().await;
}

/// FIX 1: a leader that parks a `submit_change` waiter and then steps down
/// (higher-epoch `BeginQuorumEpoch` forces Leader → Follower) must fail the
/// parked waiter promptly with `NotLeader` rather than leaving it hung until
/// engine shutdown. In a 3-voter cluster with a `NullPeerSender`, no follower
/// ever fetches, so the appended record never commits — the only way the
/// waiter resolves is the leadership-loss drain.
#[tokio::test]
async fn submit_waiter_fails_on_leadership_loss() {
    let (ctrl, _dir) = super::test_support::three_voter_leader().await;

    // Park a submit on a separate task: it appends but cannot commit (no
    // peer fetches under NullPeerSender), so it stays parked.
    let submit = super::test_support::spawn_submit(&ctrl, topic_record("orders"));

    // Give the submit a moment to reach the engine and park its waiter.
    tokio::time::sleep(StdDuration::from_millis(50)).await;

    // A strictly-higher-epoch BeginQuorumEpoch from node 2 forces node 1 to
    // step down from Leader to Follower.
    ctrl.inject_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 9,
    })
    .await
    .unwrap();

    // The parked submit must resolve promptly (bounded) with NotLeader.
    let result = tokio::time::timeout(StdDuration::from_secs(5), submit)
        .await
        .expect("submit did not hang on leadership loss")
        .expect("join");
    assert2::assert!(matches!(
        result,
        Err(RaftError::NotLeader {
            current_leader: Some(NodeId(2))
        })
    ));
    ctrl.shutdown().await;
}

#[tokio::test]
async fn submit_change_on_non_leader_rejects() {
    let (ctrl, _dir) = build(ControllerSetup::default());
    // Never elected; node 1 is Unattached → not leader.
    let r = ctrl.submit_change(topic_record("t")).await;
    assert2::assert!(matches!(r, Err(RaftError::NotLeader { .. })));
    ctrl.shutdown().await;
}

/// FIX 2: a committed record that fails apply-`validate` must only fail the
/// waiter whose appended range actually contains it, not every later waiter.
/// A committed topic "zero" is the seed. Then park three submits in a 3-voter
/// leader (no peer fetches, so nothing commits on its own): A deletes "zero"
/// (valid), B sets a config on "zero" (valid at submit, but "zero" is gone at
/// apply, so apply rejects it), C creates "third" (valid). Then drive a single
/// HWM advance past all three via a follower fetch. B must get `Err`; C must
/// get `Ok` (not bled the rejection from B's earlier offset).
#[tokio::test]
async fn rejection_scoped_to_owning_waiter_range() {
    use krabka_metadata::{DeleteTopicRecord, MetadataRecord, TopicConfigRecord};

    let (ctrl, _dir) = super::test_support::three_voter_leader().await;

    let zero = super::test_support::pending_submission(&ctrl, topic_record_named("zero", 9)).await;
    commit_pending(&ctrl, NodeId(2)).await;
    let rz = tokio::time::timeout(StdDuration::from_secs(5), zero)
        .await
        .expect("seed did not hang")
        .expect("join");
    assert!(rz.is_ok(), "seed topic should commit: {rz:?}");

    let ca = ctrl.clone();
    let cb = ctrl.clone();
    let cc = ctrl.clone();
    let a = tokio::spawn(async move {
        ca.submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "zero".to_string(),
        })])
        .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let b = tokio::spawn(async move {
        cb.submit_change(vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "zero".to_string(),
            overrides: [("retention.ms".to_string(), "1000".to_string())].into(),
        })])
        .await
    });
    tokio::time::sleep(StdDuration::from_millis(20)).await;
    let c = tokio::spawn(async move { cc.submit_change(topic_record_named("third", 3)).await });
    tokio::time::sleep(StdDuration::from_millis(40)).await;

    commit_pending(&ctrl, NodeId(2)).await;

    let ra = tokio::time::timeout(StdDuration::from_secs(5), a)
        .await
        .expect("A did not hang")
        .expect("join");
    let rb = tokio::time::timeout(StdDuration::from_secs(5), b)
        .await
        .expect("B did not hang")
        .expect("join");
    let rc = tokio::time::timeout(StdDuration::from_secs(5), c)
        .await
        .expect("C did not hang")
        .expect("join");

    check!(ra.is_ok(), "A (delete) should commit: {ra:?}");
    assert2::assert!(matches!(
        rb,
        Err(RaftError::Metadata(krabka_metadata::MetadataError::UnknownTopic(name)))
            if name == "zero"
    ));
    check!(
        rc.is_ok(),
        "C (distinct valid) must NOT bleed B's rejection: {rc:?}"
    );
    ctrl.shutdown().await;
}

/// Elect node 1 of a three-voter engine. Node 2 grants the pre-vote at
/// `epoch` and the real vote at `epoch + 1`.
fn check_first_topic_only(engine: &Engine) {
    check!(
        engine.image.topic("first").map(|topic| topic.topic_id) == Some(uuid::Uuid::from_u128(1))
    );
    check!(
        engine
            .image
            .topic_by_id(&uuid::Uuid::from_u128(2))
            .is_none()
    );
}

fn elect_three_voter_engine(engine: &mut super::Engine, epoch: u32) {
    engine.on_event(Event::ElectionTimeout);
    for vote_epoch in [epoch, epoch + 1] {
        engine.on_event(Event::ReceiveVoteResponse {
            from: NodeId(2),
            epoch: vote_epoch,
            vote_granted: true,
        });
    }
    assert!(engine.core.role().is_leader());
}

fn elected_three_voter_engine() -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(EngineSetup::default());
    elect_three_voter_engine(&mut engine, 0);
    (engine, dir)
}

/// Kafka's `QuorumController` replays a record before it commits, so
/// `ReplicationControlManager.createTopics` sees a pending topic name as an
/// existing topic. A second create of a name that has not committed gets
/// `TOPIC_ALREADY_EXISTS`, and no second `TopicRecord` goes into the log.
#[tokio::test]
async fn second_create_of_uncommitted_topic_is_refused_before_append() {
    let (mut engine, _dir) = elected_three_voter_engine();

    let mut first_rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record_named("first", 1));
    assert!(matches!(
        first_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let end_after_first = engine.log.log_end_offset();

    let mut second_rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record_named("first", 2));
    assert!(matches!(
        second_rx.try_recv(),
        Ok(Err(RaftError::Metadata(
            krabka_metadata::MetadataError::TopicExists(name)
        ))) if name == "first"
    ));
    check!(engine.log.log_end_offset() == end_after_first);
    check!(
        engine
            .commit_waiters
            .iter()
            .map(|waiter| waiter.creates.clone())
            .collect::<Vec<_>>()
            == vec![vec!["first".to_string()]]
    );

    // Node 2 fetches the whole log. With node 1, that is a majority of three.
    engine.on_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: engine.core.quorum_state().leader_epoch,
        fetch_offset: end_after_first.0,
    });

    assert!(matches!(first_rx.try_recv(), Ok(Ok(_))));
    check!(engine.commit_waiters.is_empty());
    check_first_topic_only(&engine);
}

/// A waiter that fails on leadership loss leaves `commit_waiters`, but its
/// record stays in the log. After node 1 is leader again, that record is
/// still pending, so a create of the same name gets `TopicExists` and appends
/// nothing, as Kafka's controller has replayed the pending record. Once the
/// record commits, the name exists, and only one `TopicRecord` is in the log.
#[tokio::test]
async fn create_of_a_name_left_uncommitted_by_an_earlier_epoch_is_refused() {
    let (mut engine, _dir) = elected_three_voter_engine();

    let mut first_rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record_named("first", 1));
    check!(engine.commit_waiters.len() == 1);

    // A higher-epoch BeginQuorumEpoch from node 2 makes node 1 a follower.
    engine.on_event(Event::ReceiveBeginQuorumEpoch {
        leader_id: NodeId(2),
        leader_epoch: 9,
    });
    assert!(matches!(
        first_rx.try_recv(),
        Ok(Err(RaftError::NotLeader {
            current_leader: Some(NodeId(2))
        }))
    ));
    check!(engine.commit_waiters.is_empty());

    elect_three_voter_engine(&mut engine, 9);
    let end_before_retry = engine.log.log_end_offset();
    let mut retry_rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record_named("first", 2));
    assert!(matches!(
        retry_rx.try_recv(),
        Ok(Err(RaftError::Metadata(
            krabka_metadata::MetadataError::TopicExists(name)
        ))) if name == "first"
    ));
    check!(engine.log.log_end_offset() == end_before_retry);

    // A create of another name is not held back by the pending tail.
    let mut other_rx =
        super::test_support::submit_on_engine(&mut engine, &topic_record_named("other", 3));
    assert!(matches!(
        other_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let end_after_other = engine.log.log_end_offset();
    check!(end_after_other > end_before_retry);

    // Node 2 fetches the whole log. With node 1, that is a majority of three.
    engine.on_event(Event::ReceiveFetch {
        from: NodeId(2),
        fetch_epoch: engine.core.quorum_state().leader_epoch,
        fetch_offset: end_after_other.0,
    });
    assert!(matches!(other_rx.try_recv(), Ok(Ok(_))));
    check_first_topic_only(&engine);
    check!(
        engine.image.topic("other").map(|topic| topic.topic_id) == Some(uuid::Uuid::from_u128(3))
    );
}

/// A two-replica partition whose second replica has reported its directory,
/// and the partition record as a caller read it before that report: the
/// directory list is still empty.
fn partition_with_reported_directory(
    engine: &mut super::Engine,
) -> (krabka_metadata::PartitionRecord, uuid::Uuid) {
    use krabka_metadata::{
        LeaderEpoch, MetadataRecord, PartitionDirAssignmentRecord, PartitionRecord, TopicRecord,
    };

    let stale = PartitionRecord {
        topic: "dirs".to_string(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1), NodeId(2)],
        leader_epoch: LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    };
    let directory = uuid::Uuid::from_u128(0xd1);
    for records in [
        vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "dirs".to_string(),
                topic_id: uuid::Uuid::from_u128(0x70),
                partitions: 1,
                replication_factor: 2,
            }),
            MetadataRecord::V1Partition(stale.clone()),
        ],
        vec![MetadataRecord::V1PartitionDirAssignment(
            PartitionDirAssignmentRecord {
                topic: "dirs".to_string(),
                partition: 0,
                replica: NodeId(2),
                directory,
            },
        )],
    ] {
        let mut rx = super::test_support::submit_on_engine(engine, &records);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    (stale, directory)
}

/// An election computed from an image that predates a directory assignment
/// still takes effect, and keeps the assignment. Before the rebase, the stale
/// empty directory list encoded a `PartitionChangeRecord` with zero directories
/// for two replicas: the leader appended it, reported success, and every
/// replica dropped it on replay (#579).
#[test]
fn a_stale_partition_update_keeps_the_committed_directories() {
    use krabka_metadata::{
        LeaderEpoch, LeaderRecoveryState, MetadataRecord, PartitionUpdateRecord,
    };

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let (stale, directory) = partition_with_reported_directory(&mut engine);

    let elected = krabka_metadata::PartitionRecord {
        leader: NodeId(2),
        isr: vec![NodeId(2)],
        leader_epoch: LeaderEpoch(1),
        partition_epoch: 1,
        ..stale
    };
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(
        &[MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
            partition: elected,
            eligible_leader_replicas: Some(vec![]),
            last_known_elr: Some(vec![]),
            recovery_state: Some(LeaderRecoveryState::Recovering),
        })],
        reply,
    );

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let partition = engine.image.partition("dirs", 0).expect("partition");
    check!(partition.leader == NodeId(2));
    check!(partition.isr == vec![NodeId(2)]);
    check!(partition.directories == vec![uuid::Uuid::nil(), directory]);
}

/// A record whose `KRaft` encoding no replica could decode is refused before it
/// reaches the log. A replica change that carries a directory list of the
/// wrong length is one: replay rejects a `PartitionChangeRecord` whose
/// directory count differs from its replica count.
#[test]
fn a_record_that_would_not_replay_is_refused_and_not_appended() {
    use krabka_metadata::MetadataRecord;

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let (stale, _directory) = partition_with_reported_directory(&mut engine);
    let log_end = engine.log.log_end_offset();
    let before = engine.image.partition("dirs", 0).cloned();

    let reassigned = krabka_metadata::PartitionRecord {
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        directories: vec![uuid::Uuid::nil()],
        ..stale
    };
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(
        &[MetadataRecord::V1PartitionUpdate(
            krabka_metadata::PartitionUpdateRecord {
                partition: reassigned,
                eligible_leader_replicas: None,
                last_known_elr: None,
                recovery_state: None,
            },
        )],
        reply,
    );

    let refused = rx.try_recv().expect("an immediate reply");
    assert!(let Err(RaftError::ChangeRejected(message)) = refused);
    check!(message.contains("would not replay"), "{message}");
    check!(engine.log.log_end_offset() == log_end);
    check!(engine.image.partition("dirs", 0).cloned() == before);
}

fn offset_advance(topic: &str, count: i64) -> Vec<krabka_metadata::MetadataRecord> {
    vec![krabka_metadata::MetadataRecord::V1PartitionOffsetAdvance(
        krabka_metadata::PartitionOffsetAdvanceRecord {
            topic: topic.to_owned(),
            partition: 0,
            count,
        },
    )]
}
