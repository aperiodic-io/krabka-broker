//! Offset-commit fencing.
//!
//! `OffsetCommit` and `TxnOffsetCommit` both dispatch on the group's LIVE
//! protocol inside the actor, so the decisions and the request/reply wrappers
//! that carry them live in one module. The two requests fence by different
//! rules, as Kafka's `validateOffsetCommit` does with its `isTransactional`
//! flag, and [`CommitFence`] says which rule a `ValidateCommit` runs.

use krabka_protocol::primitives::uuid::Uuid;

use super::{ErrorCode, GroupActorHandle, GroupActorMessage};
use crate::{
    codes,
    coordinator::unified::{classic_ops, group::CoordinatorGroup},
    task_util::ask,
};

/// The request whose rule a `ValidateCommit` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFence {
    /// `OffsetCommit` at this api version: Kafka's `validateOffsetCommit`
    /// with `isTransactional = false`.
    Offset { api_version: i16 },
    /// `TxnOffsetCommit`.
    Transactional,
}

/// One commit to validate: who commits, at which generation or member epoch,
/// by which request, and the `(topic id, partition)` of every partition the
/// commit writes.
///
/// A consumer group runs Kafka's per-partition validator (KIP-1251) over
/// `partitions`. A classic group does not read them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRequest {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    /// The request's `generation_id_or_member_epoch` field. The actor reads
    /// it as the consumer `member_epoch` or as the classic generation,
    /// depending on the live kind.
    pub generation_or_epoch: i32,
    pub fence: CommitFence,
    pub partitions: Vec<(Uuid, i32)>,
}

pub(super) fn validate_commit_message(
    group: &mut CoordinatorGroup,
    commit: &CommitRequest,
) -> Result<(), ErrorCode> {
    let member_id = commit.member_id.as_str();
    let group_instance_id = commit.group_instance_id.as_deref();
    let generation_or_epoch = commit.generation_or_epoch;
    if let Some(state) = group.as_consumer() {
        return state.validate_offset_commit(
            member_id,
            group_instance_id,
            generation_or_epoch,
            commit.fence,
            &commit.partitions,
        );
    }
    match commit.fence {
        CommitFence::Offset { .. } => {
            if let Some(state) = group.as_classic_mut() {
                classic_ops::validate_offset_commit(
                    state,
                    member_id,
                    group_instance_id,
                    generation_or_epoch,
                )?;
                classic_ops::refresh_committer_session(state, member_id);
            }
            Ok(())
        }
        CommitFence::Transactional => group.as_classic().map_or(Ok(()), |state| {
            classic_ops::validate_commit(state, member_id, group_instance_id, generation_or_epoch)
                .map_or(Ok(()), Err)
        }),
    }
}

/// Sends one `ValidateCommit` to the group's actor and waits for the answer.
///
/// Dispatch happens inside the actor on the LIVE `group.kind`. It does not use
/// the spawn-time `handle.kind` hint, because a KIP-848 migration may have
/// flipped the protocol in place after spawn.
///
/// It returns `Some(error_code)` when the commit must be rejected, and `None`
/// when the commit may proceed.
///
/// With [`CommitFence::Offset`] the rule is Kafka's
/// `ClassicGroup.validateOffsetCommit` or `ConsumerGroup.validateOffsetCommit`
/// with its per-partition validator. A commit that passes refreshes the
/// session of a classic member while the group is `Stable` or
/// `PreparingRebalance`.
///
/// With [`CommitFence::Transactional`] the rule is Kafka's
/// `validateTransactionalOffsetCommit` and the per-partition validator of
/// `commitTransactionalOffset`. For a simple consumer (empty `member_id`, no
/// `group_instance_id`, epoch -1) neither protocol fences, so the broker never
/// fences a producer that supplies no group metadata.
pub(crate) async fn validate_commit(
    handle: &GroupActorHandle,
    commit: CommitRequest,
) -> Option<ErrorCode> {
    match ask(&handle.tx, |reply| GroupActorMessage::ValidateCommit {
        commit,
        reply,
    })
    .await
    {
        Ok(Ok(())) => None,
        Ok(Err(code)) => Some(code),
        Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            GroupKindTag,
            test_support::{
                BidirectionalClassicSetup, bidirectional_coordinator, join_native_consumer,
                make_coordinator, rpc, seeded_bidirectional_coordinator, spawn_and_downgrade,
            },
        },
        classic_state::OffsetEntry,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_offset_validate_heartbeat_arms() {
        use krabka_protocol::owned::heartbeat_request::HeartbeatRequest;

        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");

        // UpdateCommitted then FetchOffsets round-trips on the kind-agnostic Group.
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::UpdateCommitted {
            entries: vec![(
                ("t".to_string(), 0),
                OffsetEntry {
                    offset: Offset(42),
                    leader_epoch: 1,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                    expire_timestamp_ms: None,
                    topic_id: None,
                },
            )],
            reply: tx,
        })
        .await;
        rx.await.unwrap();
        let committed = rpc::fetch_offsets(&handle).await.committed;
        assert!(committed.get(&("t".to_string(), 0)).unwrap().offset == 42);

        // Classic offset-commit validate: a simple consumer (no member/instance)
        // is allowed. `ValidateCommit` dispatches on the live (classic) kind.
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ValidateCommit {
            commit: CommitRequest {
                member_id: String::new(),
                group_instance_id: None,
                generation_or_epoch: -1,
                fence: CommitFence::Offset { api_version: 9 },
                partitions: Vec::new(),
            },
            reply: tx,
        })
        .await;
        assert!(rx.await.unwrap() == Ok(()));

        // Classic Heartbeat for an unknown member on an empty group.
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicHeartbeat {
            req: HeartbeatRequest {
                group_id: "g".into(),
                member_id: "ghost".into(),
                generation_id: 0,
                ..Default::default()
            },
            reply: tx,
        })
        .await;
        assert!(rx.await.unwrap() == codes::UNKNOWN_MEMBER_ID);

        // RemoveCommitted clears the entry.
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::RemoveCommitted {
            keys: vec![("t".to_string(), 0)],
            reply: tx,
        })
        .await;
        rx.await.unwrap();
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::FetchOffsets { reply: tx }).await;
        assert!(rx.await.unwrap().committed.is_empty());
    }

    /// Regression for the stale-`handle.kind` defect (KIP-848 live migration).
    /// The group is SPAWNED as a consumer group, because its first RPC was a
    /// native `ConsumerGroupHeartbeat`, so `handle.kind == Consumer`. It later
    /// hosts a classic member and then DOWNGRADES in place when the last
    /// native member leaves. The handle's spawn-time `kind` stays `Consumer`
    /// and is now stale.
    ///
    /// The defect was this: `offset_commit::validate` pre-dispatched on a
    /// per-handle kind mirror, so the broker could route a downgraded classic
    /// member's offset commit to the next-gen epoch path. `group.as_consumer()`
    /// is now `None`, so that path would reject with `UNKNOWN_MEMBER_ID`.
    /// With the single-source-of-truth fix, the one `ValidateCommit` message
    /// dispatches on the actor's LIVE `group.kind`, which is now classic.
    /// `classic_ops::validate_commit` then finds the re-expressed classic
    /// member and accepts the commit (`Ok(())`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawned_consumer_group_downgrade_allows_classic_offset_commit() {
        let (coord, _log) = bidirectional_coordinator();

        let (handle, view) = spawn_and_downgrade(&coord).await;
        // The handle's spawn-time `kind` is unchanged (and stale) — validation
        // must NOT consult it.
        assert!(
            handle.kind == GroupKindTag::Consumer,
            "spawn-time kind unchanged"
        );
        assert!(
            view.members.iter().any(|m| m.member_id == "m-classic"),
            "the hosted classic member must survive the downgrade"
        );
        let generation = view.generation_id;

        // Prove the fix at the routing boundary `offset_commit::validate` uses:
        // the single `ValidateCommit` message dispatches on the actor's LIVE
        // `group.kind` (now classic) and accepts the downgraded classic member's
        // commit (`Ok(())`). Pre-refactor, a handle-side mirror could route this
        // to the consumer epoch path and reject with `UNKNOWN_MEMBER_ID`.
        let result = rpc::validate_commit(
            &handle,
            "m-classic",
            generation,
            CommitFence::Offset { api_version: 9 },
            &[],
        )
        .await;
        assert!(
            result == Ok(()),
            "ValidateCommit must dispatch on the live (classic) kind and accept \
             the downgraded member (got {result:?})"
        );
    }

    /// Regression (user-requested): an UPGRADED group runs the consumer epoch
    /// fence on a native consumer member's commit. A classic group upgrades
    /// when a native consumer heartbeats in, so the handle's spawn-time `kind`
    /// is a stale `Classic`. `ValidateCommit` for that native member must
    /// dispatch on the LIVE consumer kind and apply the epoch fence. Before the
    /// refactor, a spawned-Classic upgraded group took the classic validate
    /// path and SKIPPED the epoch check.
    ///
    /// Both requests answer `STALE_MEMBER_EPOCH` for a newer epoch, and for an
    /// older epoch on a partition the member was assigned after it, as Kafka's
    /// `ConsumerGroup.validateOffsetCommit` does. The `TxnOffsetCommit`
    /// handler maps the code by version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgraded_group_fences_stale_native_consumer_commit() {
        let (_coord, _log, handle) =
            seeded_bidirectional_coordinator(BidirectionalClassicSetup::default());
        assert!(handle.kind == GroupKindTag::Classic);
        let joined = join_native_consumer(&handle).await;
        let native = joined.member_id;
        let current_epoch = joined.epoch.0;

        // The handle's spawn-time kind is the stale `Classic`; validation must
        // not consult it — it must run the consumer epoch fence.
        assert!(handle.kind == GroupKindTag::Classic);

        let offset = CommitFence::Offset { api_version: 9 };
        let cases = [
            (offset, current_epoch - 1, Err(codes::STALE_MEMBER_EPOCH)),
            (offset, current_epoch + 1, Err(codes::STALE_MEMBER_EPOCH)),
            (offset, current_epoch, Ok(())),
            (
                CommitFence::Offset { api_version: 8 },
                current_epoch,
                Err(codes::UNSUPPORTED_VERSION),
            ),
            (
                CommitFence::Transactional,
                current_epoch - 1,
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (
                CommitFence::Transactional,
                current_epoch + 1,
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (CommitFence::Transactional, current_epoch, Ok(())),
        ];
        // Every partition the member was assigned came at `current_epoch`, so
        // an older epoch is refused for them.
        let partitions = [(Uuid([7; 16]), 0)];
        let mut actual = Vec::new();
        for (fence, epoch, _) in cases {
            let got = rpc::validate_commit(&handle, &native, epoch, fence, &partitions).await;
            actual.push((fence, epoch, got));
        }
        assert!(actual == cases);
    }

    /// KIP-1251 end to end through the actor: a member that joins alone gets
    /// both partitions at its first epoch. A second member joins and takes one
    /// of them away. The first member's next epoch is higher than its first,
    /// and a commit at its first epoch is accepted for the partition it kept
    /// from then, and refused for the one it no longer holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_older_epoch_commits_a_partition_held_since_that_epoch() {
        const TOPIC: Uuid = Uuid([7; 16]);
        let (coord, _log) = bidirectional_coordinator();
        let handle = coord.get_or_create_consumer("g");

        let first = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        let a = first.member_id.clone().expect("member id");
        let first_epoch = first.member_epoch;
        let second = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(second.error_code == codes::NONE);
        // `a` still owns both partitions and is told to revoke one; then it
        // reports that it did.
        let revoke =
            rpc::consumer_heartbeat_owning(&handle, &a, first_epoch, &[(TOPIC, &[0, 1])]).await;
        let kept: Vec<i32> = revoke
            .assignment
            .iter()
            .flat_map(|assignment| &assignment.topic_partitions)
            .flat_map(|tp| tp.partitions.iter().copied())
            .collect();
        let owned =
            rpc::consumer_heartbeat_owning(&handle, &a, first_epoch, &[(TOPIC, &kept)]).await;
        assert!(owned.error_code == codes::NONE);
        let current_epoch = owned.member_epoch;
        assert!(current_epoch > first_epoch);
        let [kept] = kept[..] else {
            panic!("`a` keeps exactly one partition, got {kept:?}");
        };
        let lost = 1 - kept;

        let offset = CommitFence::Offset { api_version: 9 };
        let rows = [
            (first_epoch, vec![(TOPIC, kept)], Ok(())),
            (
                first_epoch,
                vec![(TOPIC, lost)],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (
                first_epoch,
                vec![(TOPIC, kept), (TOPIC, lost)],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (
                first_epoch - 1,
                vec![(TOPIC, kept)],
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (current_epoch, vec![(TOPIC, lost)], Ok(())),
        ];
        let mut actual = Vec::new();
        for (epoch, partitions, _) in &rows {
            let got = rpc::validate_commit(&handle, &a, *epoch, offset, partitions).await;
            actual.push((*epoch, partitions.clone(), got));
        }
        assert!(actual == rows);
    }
}
