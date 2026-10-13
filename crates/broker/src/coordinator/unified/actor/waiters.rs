//! Resolution of the parked classic `JoinGroup` and `SyncGroup` waiters.
//!
//! The classic protocol blocks a client inside the RPC until the rebalance
//! boundary, so the actor parks the reply `oneshot::Sender` and hands it to one
//! of these functions when the boundary arrives: the rebalance deadline, an
//! early completion, a member removal, or the leader's `SyncGroup`.

use {
    super::{JoinResult, ParkedWaiters, SyncResult},
    crate::{
        codes,
        coordinator::unified::{
            classic_ops,
            classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
        },
    },
    std::{collections::HashMap, time::Instant},
    tokio::sync::oneshot,
};

/// Runs Kafka's `completeClassicGroupJoin` and resolves every parked joiner.
/// It returns `true` when the round emptied the group, which the caller must
/// persist as Kafka writes the empty generation.
///
/// It also drains any stale parked follower with `REBALANCE_IN_PROGRESS`. Such
/// a follower belongs to a previous `CompletingRebalance` whose leader was
/// dead and never sent `SyncGroup`. The notification lets the client rejoin at
/// once instead of waiting for the 30-second request timeout.
pub(super) fn complete_classic_rebalance(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) -> bool {
    drain_followers_with(followers, codes::REBALANCE_IN_PROGRESS);
    match classic_ops::try_complete(state, Instant::now()) {
        Ok(classic_ops::Completion::NotPreparing) => {
            answer_joiners(state, joiners);
            false
        }
        Ok(classic_ops::Completion::Generation { removed }) => {
            drain_removed_classic_waiters(&removed, joiners, followers);
            answer_joiners(state, joiners);
            false
        }
        Ok(classic_ops::Completion::Postponed { removed }) => {
            drain_removed_classic_waiters(&removed, joiners, followers);
            false
        }
        Ok(classic_ops::Completion::Emptied { removed }) => {
            drain_removed_classic_waiters(&removed, joiners, followers);
            true
        }
        Err(error) => {
            state.rebalance_deadline = None;
            state.joined_this_round.clear();
            let error_code = match error {
                classic_ops::CompleteError::InconsistentProtocol => {
                    codes::INCONSISTENT_GROUP_PROTOCOL
                }
                classic_ops::CompleteError::EpochExhausted => codes::INVALID_REQUEST,
            };
            for (member_id, sender) in joiners.drain() {
                let _ = sender.send(JoinResult {
                    error_code,
                    member_id,
                    protocol_type: state.protocol_type.clone(),
                    ..JoinResult::default()
                });
            }
            false
        }
    }
}

fn answer_joiners(
    state: &ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
) {
    for (member_id, sender) in joiners.drain() {
        let _ = sender.send(classic_ops::build_join_result(state, &member_id));
    }
}

/// Answers every parked `SyncGroup` follower with `error_code`, as Kafka's
/// `prepareRebalance` does from `CompletingRebalance`.
pub(super) fn drain_followers_with(
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
    error_code: i16,
) {
    for (_, sender) in followers.drain() {
        let _ = sender.send(SyncResult {
            error_code,
            ..SyncResult::default()
        });
    }
}

/// Kafka's `onUnloaded`: every parked `JoinGroup` and `SyncGroup` answers
/// `NOT_COORDINATOR`, and the joiner keeps its member id.
pub(super) fn drain_parked_for_unload(
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for (member_id, sender) in joiners.drain() {
        let _ = sender.send(JoinResult {
            error_code: codes::NOT_COORDINATOR,
            member_id,
            ..JoinResult::default()
        });
    }
    drain_followers_with(followers, codes::NOT_COORDINATOR);
}

/// Kafka's `replaceStaticMember` fence: the old member id's parked
/// `JoinGroup` and `SyncGroup` get `FENCED_INSTANCE_ID`.
pub(super) fn fence_replaced_classic_member(
    member_id: &str,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    reject_parked_member(member_id, codes::FENCED_INSTANCE_ID, joiners, followers);
}

/// Remove and answer one member's join and sync waiters, in that order.
fn reject_parked_member(
    member_id: &str,
    error_code: i16,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    if let Some(sender) = joiners.remove(member_id) {
        let _ = sender.send(JoinResult {
            error_code,
            member_id: member_id.to_string(),
            ..JoinResult::default()
        });
    }
    if let Some(sender) = followers.remove(member_id) {
        let _ = sender.send(SyncResult {
            error_code,
            ..SyncResult::default()
        });
    }
}

pub(super) fn drain_removed_classic_waiters(
    removed: &[String],
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    for member_id in removed {
        reject_parked_member(member_id, codes::UNKNOWN_MEMBER_ID, joiners, followers);
    }
}

/// Answer removed members first, then fence followers of an interrupted sync
/// phase, and finally complete any join phase the survivors have all joined.
pub(super) fn settle_removed_classic_waiters(
    state: &mut ClassicState,
    previous: ClassicGroupState,
    removed: &[String],
    parked: &mut ParkedWaiters,
) {
    drain_removed_classic_waiters(removed, &mut parked.joiners, &mut parked.followers);
    if previous == ClassicGroupState::CompletingRebalance
        && state.state == ClassicGroupState::PreparingRebalance
    {
        drain_followers_with(&mut parked.followers, codes::REBALANCE_IN_PROGRESS);
    }
    maybe_complete_classic(state, &mut parked.joiners, &mut parked.followers);
}

/// Completes the rebalance early if and only if every member has joined a
/// round that did not open from `Empty`, Kafka's `maybeCompleteJoinPhase`
/// after a member left or expired.
pub(super) fn maybe_complete_classic(
    state: &mut ClassicState,
    joiners: &mut HashMap<String, oneshot::Sender<JoinResult>>,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let should = !state.rebalance_from_empty
        && matches!(state.state, ClassicGroupState::PreparingRebalance)
        && !state.members.is_empty()
        && state.has_all_members_joined();
    if should {
        // Every member joined, so none is removed and the group stays
        // non-empty: there is nothing to persist.
        let _ = complete_classic_rebalance(state, joiners, followers);
    }
}

/// Delivers to each parked follower its installed assignment, after the leader
/// sync.
pub(super) fn drain_parked_followers(
    state: &ClassicState,
    followers: &mut HashMap<String, oneshot::Sender<SyncResult>>,
) {
    let protocol_type = state.protocol_type.clone();
    let protocol_name = state.protocol_name.clone();
    for (member_id, sender) in followers.drain() {
        let result = classic_ops::read_sync_result(
            state,
            &member_id,
            protocol_type.clone(),
            protocol_name.clone(),
        );
        let _ = sender.send(result);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::Bytes,
        std::time::{Duration, Instant},
    };

    #[test]
    fn inconsistent_classic_completion_clears_deadline() {
        use crate::coordinator::unified::classic_state::Member;

        let mut state = ClassicState::new("g");
        state.protocol_type = Some("consumer".into());
        state.add_member(Member::new(
            "m1",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::new())],
        ));
        state.add_member(Member::new(
            "m2",
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("cooperative-sticky".into(), Bytes::new())],
        ));
        state.rebalance_deadline = Some(
            Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        );
        assert!(state.state == ClassicGroupState::PreparingRebalance);

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        let mut joiners = maplit::hashmap! {"m1".to_string() => tx1, "m2".to_string() => tx2};
        let mut followers = HashMap::new();

        complete_classic_rebalance(&mut state, &mut joiners, &mut followers);

        assert!(
            state.rebalance_deadline.is_none(),
            "a failed protocol vote must not leave an already-fired deadline armed"
        );
    }

    #[tokio::test]
    async fn removed_classic_members_drain_parked_waiters() {
        let (join_tx, join_rx) = tokio::sync::oneshot::channel();
        let (sync_tx, sync_rx) = tokio::sync::oneshot::channel();
        let mut joiners = maplit::hashmap! {"m1".to_string() => join_tx};
        let mut followers = maplit::hashmap! {"m1".to_string() => sync_tx};

        drain_removed_classic_waiters(&["m1".to_string()], &mut joiners, &mut followers);

        check!(joiners.is_empty());
        check!(followers.is_empty());
        check!(join_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
        check!(sync_rx.await.unwrap().error_code == codes::UNKNOWN_MEMBER_ID);
    }
}
