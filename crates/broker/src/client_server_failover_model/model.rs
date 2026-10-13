//! The stateright model itself: the initial state, the enabled actions, the
//! transition function, and the search boundary.
//!
//! The transitions dispatch to the sibling modules. This file only decides
//! which action is enabled in a state and how each one moves it.

use {
    super::{
        bounds::{
            BASE_OFFSET, BASE_SEQUENCE, INITIAL_LEADER, MAX_HWM, MAX_LOG_LEN,
            MAX_METADATA_REFRESHES, MAX_SEND_ATTEMPTS, NB,
        },
        produce::SendKind,
        state::{Action, BatchState, FailoverState, ProduceResult, RequestOutcome},
        witness::{WITNESS_FAILOVER, Witnesses},
    },
    stateright::{Model, Property},
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ClientServerFailoverModel;

impl Model for ClientServerFailoverModel {
    type Action = Action;
    type State = FailoverState;

    krabka_macros::model_transition!(last, action, s; {
            match action {
                Action::ClientSend(outcome) => last.apply_client_send(SendKind::Send, outcome),
                Action::ClientRetry(outcome) => last.apply_client_send(SendKind::Retry, outcome),
                Action::Replicate(follower) => {
                    if follower == s.leader || !s.live(s.leader) || !s.live(follower) {
                        return None;
                    }
                    let leader_log = s.logs[s.leader];
                    if s.log_len(s.leader) == 0 || s.logs[follower] == leader_log {
                        return None;
                    }
                    s.logs[follower] = leader_log;
                    Some(s)
                }
                Action::AdvanceHwm => {
                    if !s.live(s.leader) || s.hwm != 0 || !s.hwm_prefix_replicated() {
                        return None;
                    }
                    s.hwm = 1;
                    Some(s)
                }
                Action::AckCommitted => {
                    if !s.can_ack_committed() {
                        return None;
                    }
                    s.acked_offset = Some(BASE_OFFSET);
                    s.batch = BatchState::Acked;
                    s.last_result = Some(ProduceResult::Acked);
                    Some(s)
                }
                Action::KillLeader => {
                    if !s.live(s.leader) || s.live_count() <= 1 {
                        return None;
                    }
                    s.live &= !(1 << s.leader);
                    s.refresh_needed = true;
                    s.mark_failover();
                    Some(s)
                }
                Action::ElectClean(follower) => {
                    if follower == s.leader || !s.live(follower) || !s.contains_hwm_prefix(follower) {
                        return None;
                    }
                    s.leader = follower;
                    s.refresh_needed = s.cached_leader != s.leader || !s.live(s.cached_leader);
                    s.refresh_leader_producer_entry();
                    s.mark_failover();
                    Some(s)
                }
                Action::RefreshMetadata => {
                    if !s.live(s.leader) {
                        return None;
                    }
                    if s.metadata_refreshes >= MAX_METADATA_REFRESHES {
                        return None;
                    }
                    s.metadata_refreshes = s.metadata_refreshes.saturating_add(1);
                    s.cached_leader = s.leader;
                    s.refresh_needed = false;
                    s.refresh_leader_producer_entry();
                    Some(s)
                }
            }

    });

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            logs: [[None; MAX_LOG_LEN]; NB],
            leader: INITIAL_LEADER,
            live: (1 << NB) - 1,
            hwm: 0,
            cached_leader: INITIAL_LEADER,
            refresh_needed: false,
            batch: BatchState::Empty,
            next_sequence: BASE_SEQUENCE,
            accepted: None,
            producer_entry: None,
            acked_offset: None,
            last_result: None,
            send_attempts: 0,
            metadata_refreshes: 0,
            witnesses: Witnesses(0),
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        let routed = s.cached_leader_current();
        if s.send_attempts < MAX_SEND_ATTEMPTS && s.batch == BatchState::Empty {
            if routed {
                actions.push(Action::ClientSend(RequestOutcome::AppendedUnacked));
                actions.push(Action::ClientSend(RequestOutcome::TimedOutUnknown));
            } else {
                actions.push(Action::ClientSend(RequestOutcome::NotLeader));
            }
        }
        if s.send_attempts < MAX_SEND_ATTEMPTS
            && matches!(
                s.batch,
                BatchState::Prepared | BatchState::Appended | BatchState::Acked
            )
        {
            if routed {
                if s.can_try_duplicate() {
                    actions.push(Action::ClientRetry(RequestOutcome::Duplicate));
                } else {
                    actions.push(Action::ClientRetry(RequestOutcome::AppendedUnacked));
                    actions.push(Action::ClientRetry(RequestOutcome::TimedOutUnknown));
                }
            } else {
                actions.push(Action::ClientRetry(RequestOutcome::NotLeader));
            }
        }
        if s.live(s.leader) {
            for broker in 0..NB {
                if broker != s.leader
                    && s.live(broker)
                    && s.log_len(s.leader) > 0
                    && s.logs[broker] != s.logs[s.leader]
                {
                    actions.push(Action::Replicate(broker));
                }
            }
        }
        if s.hwm == 0 && s.hwm_prefix_replicated() {
            actions.push(Action::AdvanceHwm);
        }
        if s.can_ack_committed() {
            actions.push(Action::AckCommitted);
        }
        if s.live(s.leader) && s.live_count() > 1 {
            actions.push(Action::KillLeader);
        }
        for broker in 0..NB {
            if broker != s.leader
                && s.live(broker)
                && s.contains_hwm_prefix(broker)
                && (!s.witnesses.seen(WITNESS_FAILOVER) || !s.live(s.leader))
            {
                actions.push(Action::ElectClean(broker));
            }
        }
        if (s.refresh_needed || s.cached_leader != s.leader || !s.live(s.cached_leader))
            && s.metadata_refreshes < MAX_METADATA_REFRESHES
        {
            actions.push(Action::RefreshMetadata);
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = Self::safety_properties();
        properties.extend(Self::witness_properties());
        properties
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.logs
            .iter()
            .all(|log| log.iter().flatten().count() <= MAX_LOG_LEN)
            && s.hwm <= MAX_HWM
            && s.next_sequence <= BASE_SEQUENCE + 1
            && s.send_attempts <= MAX_SEND_ATTEMPTS
            && s.metadata_refreshes <= MAX_METADATA_REFRESHES
            && usize::from(s.live) < (1 << NB)
    }
}
