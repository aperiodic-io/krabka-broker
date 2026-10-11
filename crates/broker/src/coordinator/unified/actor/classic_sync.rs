//! The classic `SyncGroup` path.
//!
//! The leader's request installs the round's assignments, persists the classic
//! k2 snapshot, and releases every follower parked behind it. A consumer-kind
//! group answers the same RPC for a classic member it hosts from the member's
//! assigned partitions, and writes nothing, as Kafka's
//! `classicGroupSyncToConsumerGroup` does.

use krabka_protocol::owned::sync_group_request::SyncGroupRequest;
use tokio::sync::oneshot;

use super::{
    ActorServices, ParkedWaiters, SyncResult, persistence::flush_classic_metadata,
    waiters::drain_parked_followers,
};
use crate::{
    codes,
    coordinator::unified::{classic_ops, group::CoordinatorGroup, migration},
};

pub(super) async fn handle_classic_sync_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    request: SyncGroupRequest,
    reply: oneshot::Sender<SyncResult>,
) {
    if group.as_classic_mut().is_none() {
        let result = match group.as_consumer_mut() {
            Some(state) => {
                let result =
                    migration::serve_classic_sync(state, &request, &services.metadata.snapshot());
                // Kafka's `scheduleConsumerGroupSessionTimeout` once the sync
                // is answered.
                if result.error_code == codes::NONE
                    && let Some(member) = state.members.get_mut(&request.member_id)
                {
                    member.last_seen = std::time::Instant::now();
                }
                result
            }
            None => SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            },
        };
        let _ = reply.send(result);
        return;
    }
    let state = group
        .as_classic_mut()
        .expect("group kind checked immediately above");
    let previous = state.clone();
    match classic_ops::handle_sync(state, &request) {
        classic_ops::SyncAction::Immediate(result) => {
            let _ = reply.send(result);
        }
        classic_ops::SyncAction::Park => {
            parked.followers.insert(request.member_id, reply);
        }
        classic_ops::SyncAction::LeaderInstalled(result) => {
            if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                *state = previous;
                tracing::warn!(group_id = %state.group_id, %error,
                    "classic SyncGroup log write failed");
                // Kafka's `propagateAssignment` leaves the protocol type and
                // name null when it propagates an error.
                let failure = || SyncResult {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    ..SyncResult::default()
                };
                let _ = reply.send(failure());
                for (_, follower) in parked.followers.drain() {
                    let _ = follower.send(failure());
                }
                return;
            }
            let _ = reply.send(result);
            drain_parked_followers(state, &mut parked.followers);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            DescribeMember, GroupActorMessage,
            test_support::{
                decode_assignment, last_classic_metadata, make_coordinator,
                make_coordinator_with_topic_policy, rpc, seed_and_upgrade, seed_completing_classic,
                upgrade_and_rejoin_classic, upgrade_coordinator,
            },
        },
        classic_state::GroupState as ClassicGroupState,
        config::ConsumerGroupMigrationPolicy,
    };

    /// Partitions by topic id.
    type Partitions = std::collections::HashMap<krabka_protocol::primitives::uuid::Uuid, Vec<i32>>;

    /// What a member holds and at which epoch, as `Describe` reports it.
    fn held(member: &DescribeMember) -> (i32, Partitions, Partitions) {
        (
            member.member_epoch,
            member.assigned_partitions.clone(),
            member.partitions_pending_revocation.clone(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_leader_sync_persists_complete_stable_snapshot() {
        let (coord, log) = make_coordinator();
        let (handle, generation) = seed_completing_classic(&coord, &["m1", "m2"]);
        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicSync {
            req: rpc::assignment_sync_request(generation, "m1", Bytes::from_static(b"assignment")),
            reply: tx,
        })
        .await;

        check!(rx.await.unwrap().error_code == codes::NONE);
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == generation);
        check!(persisted.protocol_name.as_deref() == Some("range"));
        check!(persisted.members.len() == 2);
        check!(
            persisted
                .members
                .iter()
                .find(|member| member.member_id == "m1")
                .unwrap()
                .assignment
                == Bytes::from_static(b"assignment")
        );
        check!(
            persisted
                .members
                .iter()
                .find(|member| member.member_id == "m2")
                .unwrap()
                .assignment
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_sync_append_failure_rolls_back_and_can_retry() {
        let (coord, log) = make_coordinator();
        let (handle, generation) = seed_completing_classic(&coord, &["m1"]);
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let request =
            rpc::assignment_sync_request(generation, "m1", Bytes::from_static(b"assignment"));

        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicSync {
            req: request.clone(),
            reply: tx,
        })
        .await;
        let failure = rx.await.unwrap();
        check!(
            failure
                == SyncResult {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    ..SyncResult::default()
                }
        );
        let view = rpc::classic_inspect(&handle).await;
        check!(view.state == ClassicGroupState::CompletingRebalance);
        check!(view.members[0].assignment.is_none());
        check!(log.batches().await.is_empty());

        let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicSync {
            req: request,
            reply: tx,
        })
        .await;
        check!(rx.await.unwrap().error_code == codes::NONE);
        check!(log.batches().await.len() == 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hosted_classic_member_syncs_translated_assignment() {
        // `Upgrade` policy: the native member's leave in `seed_and_upgrade`
        // must NOT downgrade the group back to classic — this test exercises
        // serving a hosted classic member from the consumer-kind reconciler.
        let (coord, _log) = upgrade_coordinator();
        let handle = seed_and_upgrade(&coord, "t").await;
        let upgraded = rpc::describe_member(&handle, "m-classic").await;

        // 1. Heartbeat: the native member's join and leave moved the target
        //    past the member's epoch, so it must rejoin.
        assert!(
            rpc::classic_heartbeat(&handle, "m-classic", upgraded.member_epoch).await
                == codes::REBALANCE_IN_PROGRESS,
            "post-upgrade heartbeat must ask for a rejoin"
        );

        // 2. JoinGroup (rejoin of the existing member, unchanged subscription):
        //    success as a follower, with no leader and no member list, at the
        //    member epoch.
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        let member = rpc::describe_member(&handle, "m-classic").await;
        check!(
            join == crate::coordinator::unified::actor::JoinResult {
                error_code: codes::NONE,
                generation_id: member.member_epoch,
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                member_id: "m-classic".into(),
                ..Default::default()
            }
        );

        // 3. SyncGroup: returns the translated assignment for "t".
        let sync = rpc::classic_sync(&handle, "m-classic", join.generation_id).await;
        assert!(sync.error_code == codes::NONE);
        let asn = decode_assignment(&sync.assignment);
        let t_assign = asn
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "t")
            .expect("assignment contains topic t");
        assert!(
            !t_assign.partitions.is_empty(),
            "m-classic must own partitions of t"
        );

        // 4. Heartbeat again: reconciled → NONE.
        assert!(
            rpc::classic_heartbeat(&handle, "m-classic", join.generation_id).await == codes::NONE,
            "a reconciled member's heartbeat is NONE"
        );
    }

    /// A native KIP-848 member of an upgraded group must not be served the
    /// classic `SyncGroup` path. It reconciles through
    /// `ConsumerGroupHeartbeat`, acknowledging each target itself. Kafka's
    /// `throwIfMemberDoesNotUseClassicProtocol` answers `UNKNOWN_MEMBER_ID`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_member_sync_group_is_rejected_and_changes_no_assignment() {
        let (coord, _log) = upgrade_coordinator();
        // The hosted classic member syncs, so it holds both partitions of "t".
        let (handle, join) = upgrade_and_rejoin_classic(&coord).await;
        assert!(
            rpc::classic_sync(&handle, "m-classic", join.generation_id)
                .await
                .error_code
                == codes::NONE
        );

        let native = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(native.error_code == codes::NONE);
        let native_id = native.member_id.expect("native member id");
        let before = rpc::describe_member(&handle, &native_id).await;
        let classic_before = rpc::describe_member(&handle, "m-classic").await;

        let sync = rpc::classic_sync(&handle, &native_id, before.member_epoch).await;

        check!(
            sync == SyncResult {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..SyncResult::default()
            }
        );
        check!(held(&rpc::describe_member(&handle, &native_id).await) == held(&before));
        check!(held(&rpc::describe_member(&handle, "m-classic").await) == held(&classic_before));
    }

    /// A coordinator failover keeps a reconciled hosted classic member
    /// reconciled: the member's k7/k8 records are everything its `Heartbeat`
    /// and `SyncGroup` read, so a fresh actor hydrated from them answers the
    /// same.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failover_keeps_a_synced_hosted_classic_member_in_sync() {
        let (coord, _log) = upgrade_coordinator();
        let (handle, join) = upgrade_and_rejoin_classic(&coord).await;
        let sync = rpc::classic_sync(&handle, "m-classic", join.generation_id).await;
        assert!(sync.error_code == codes::NONE);

        // Fail over: a fresh coordinator hydrates the group from the records
        // the join left behind.
        let seed = coord
            .cached_seed("g")
            .expect("records for the synced group");
        let (failover, _failover_log) = upgrade_coordinator();
        let restored = failover.get_or_create_consumer("g");
        restored
            .tx
            .send(GroupActorMessage::Seed(seed))
            .await
            .unwrap();

        check!(
            rpc::classic_heartbeat(&restored, "m-classic", join.generation_id).await == codes::NONE,
            "a member that reconciled before the failover owes no rejoin after it"
        );
        let resync = rpc::classic_sync(&restored, "m-classic", join.generation_id).await;
        check!(resync == sync);
    }

    /// Kafka's `classicGroupJoinToConsumerGroup` reconciles a hosted member's
    /// assignment in its join, and its `classicGroupSyncToConsumerGroup`
    /// writes no record: the sync after a join that reconciled the member
    /// returns the member's assignment and appends nothing, and the member is
    /// in sync.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sync_after_a_reconciling_join_writes_nothing() {
        let (coord, log) = upgrade_coordinator();
        let (handle, join) = upgrade_and_rejoin_classic(&coord).await;
        let before = rpc::describe_member(&handle, "m-classic").await;
        let batches_before = log.batches().await.len();

        let synced = rpc::classic_sync(&handle, "m-classic", join.generation_id).await;

        check!(synced.error_code == codes::NONE);
        check!(
            !decode_assignment(&synced.assignment)
                .assigned_partitions
                .is_empty()
        );
        check!(held(&rpc::describe_member(&handle, "m-classic").await) == held(&before));
        check!(log.batches().await.len() == batches_before);
        check!(
            rpc::classic_heartbeat(&handle, "m-classic", join.generation_id).await == codes::NONE
        );
    }

    /// Kafka's `classicGroupSyncToConsumerGroup` serves `assignedPartitions`,
    /// not the target: a member whose join could not claim a partition that
    /// another member still holds syncs only what it was granted, and the
    /// sync writes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hosted_member_syncs_its_assigned_partitions_not_its_target() {
        let (coord, log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Upgrade);
        let handle = seed_and_upgrade(&coord, "t").await;
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        assert!(
            rpc::classic_sync(&handle, "m-classic", join.generation_id)
                .await
                .error_code
                == codes::NONE
        );
        // A second classic member joins: its target takes a partition that
        // m-classic holds until it rejoins and revokes it.
        let joined = rpc::classic_join(&handle, "m2", "t").await;
        let m2 = rpc::describe_member(&handle, "m2").await;
        assert!(m2.assigned_partitions.is_empty());
        let batches_before = log.batches().await.len();

        let synced = rpc::classic_sync(&handle, "m2", joined.generation_id).await;

        check!(synced.error_code == codes::NONE);
        check!(
            decode_assignment(&synced.assignment)
                .assigned_partitions
                .is_empty()
        );
        check!(log.batches().await.len() == batches_before);
    }
}
