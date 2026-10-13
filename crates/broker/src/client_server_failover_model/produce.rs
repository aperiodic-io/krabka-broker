//! One client produce attempt, from the routing decision the client makes to
//! the dedup verdict the leader returns.
//!
//! This is the composition the model exists to check: the client's own retry
//! and sequence bookkeeping runs against `check_pure`, the real broker-side
//! idempotent-producer check, rather than against a restatement of it.

use {
    super::{
        bounds::{
            BASE_OFFSET, BASE_SEQUENCE, MAX_LOG_LEN, MAX_SEND_ATTEMPTS, PRODUCER_EPOCH, PRODUCER_ID,
        },
        state::{
            AcceptedBatch, BatchState, FailoverState, LogBatch, ProduceResult,
            ProducerEntryProjection, RequestOutcome,
        },
        witness::{
            WITNESS_APPENDED_UNACKED, WITNESS_DUPLICATE_AFTER_UNKNOWN, WITNESS_DUPLICATE_RESPONSE,
            WITNESS_FAILOVER, WITNESS_NOT_LEADER, WITNESS_PREPARED_RETRY, WITNESS_RETRY,
            WITNESS_RETRY_AFTER_FAILOVER, WITNESS_TIMED_OUT_UNKNOWN,
            WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER,
        },
    },
    crate::producer_state::{Decision, check_pure},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendKind {
    Send,
    Retry,
}

impl FailoverState {
    pub fn request_base_sequence(&mut self, kind: SendKind) -> Option<i32> {
        let base_sequence = match self.batch {
            BatchState::Empty => {
                if kind == SendKind::Retry {
                    return None;
                }
                BASE_SEQUENCE
            }
            BatchState::Prepared => {
                if kind == SendKind::Retry {
                    self.witnesses.mark(WITNESS_RETRY);
                    self.witnesses.mark(WITNESS_PREPARED_RETRY);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                    }
                }
                BASE_SEQUENCE
            }
            BatchState::Appended | BatchState::Acked => {
                if kind == SendKind::Send {
                    return None;
                }
                self.witnesses.mark(WITNESS_RETRY);
                if self.witnesses.seen(WITNESS_FAILOVER) {
                    self.witnesses.mark(WITNESS_RETRY_AFTER_FAILOVER);
                }
                BASE_SEQUENCE
            }
            BatchState::Failed => return None,
        };
        Some(base_sequence)
    }

    pub fn apply_client_send(&self, kind: SendKind, outcome: RequestOutcome) -> Option<Self> {
        if self.send_attempts >= MAX_SEND_ATTEMPTS {
            return None;
        }

        let mut s = self.clone();
        let base_sequence = s.request_base_sequence(kind)?;
        s.send_attempts = s.send_attempts.saturating_add(1);
        if s.batch == BatchState::Empty {
            s.batch = BatchState::Prepared;
        }

        if !s.cached_leader_current() {
            if outcome != RequestOutcome::NotLeader {
                return None;
            }
            s.refresh_needed = true;
            s.last_result = Some(ProduceResult::NotLeader);
            s.witnesses.mark(WITNESS_NOT_LEADER);
            return Some(s);
        } else if outcome == RequestOutcome::NotLeader {
            return None;
        }

        s.refresh_leader_producer_entry();
        let entry = s.producer_entry();
        match check_pure(entry.as_ref(), PRODUCER_EPOCH, base_sequence, 0) {
            Decision::Append => {
                if !matches!(
                    outcome,
                    RequestOutcome::AppendedUnacked | RequestOutcome::TimedOutUnknown
                ) {
                    return None;
                }
                if s.log_len(s.leader) >= MAX_LOG_LEN || (s.hwm > 0 && s.accepted.is_some()) {
                    return None;
                }
                let batch = LogBatch::initial();
                s.logs[s.leader][0] = Some(batch);
                s.accepted = Some(AcceptedBatch {
                    producer_id: PRODUCER_ID,
                    producer_epoch: PRODUCER_EPOCH,
                    base_sequence,
                    offset: BASE_OFFSET,
                });
                s.producer_entry = Some(ProducerEntryProjection {
                    epoch: PRODUCER_EPOCH,
                    last_sequence: base_sequence,
                    base_offset: BASE_OFFSET,
                });
                s.batch = BatchState::Appended;
                s.next_sequence = base_sequence + 1;
                match outcome {
                    RequestOutcome::AppendedUnacked => {
                        s.last_result = Some(ProduceResult::AppendedUnacked);
                        s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                    }
                    RequestOutcome::TimedOutUnknown => {
                        s.last_result = Some(ProduceResult::TimedOutUnknown);
                        s.witnesses.mark(WITNESS_TIMED_OUT_UNKNOWN);
                    }
                    RequestOutcome::NotLeader | RequestOutcome::Duplicate => return None,
                }
                Some(s)
            }
            Decision::Duplicate { base_offset } => {
                if outcome != RequestOutcome::Duplicate {
                    return None;
                }
                let accepted = s.accepted?;
                if accepted.producer_id != PRODUCER_ID
                    || accepted.producer_epoch != PRODUCER_EPOCH
                    || accepted.base_sequence != base_sequence
                    || accepted.offset != base_offset
                {
                    return None;
                }
                if !s.leader_contains_accepted() {
                    return None;
                }
                s.witnesses.mark(WITNESS_DUPLICATE_RESPONSE);
                if self.last_result == Some(ProduceResult::TimedOutUnknown) {
                    s.witnesses.mark(WITNESS_DUPLICATE_AFTER_UNKNOWN);
                    if self.witnesses.seen(WITNESS_FAILOVER) {
                        s.witnesses.mark(WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER);
                    }
                }
                if s.hwm == 1 {
                    s.acked_offset = Some(base_offset);
                    s.batch = BatchState::Acked;
                    s.last_result = Some(ProduceResult::Acked);
                } else {
                    s.batch = BatchState::Appended;
                    s.last_result = Some(ProduceResult::AppendedUnacked);
                    s.witnesses.mark(WITNESS_APPENDED_UNACKED);
                }
                Some(s)
            }
            Decision::OutOfOrder | Decision::Fenced => {
                s.batch = BatchState::Failed;
                Some(s)
            }
        }
    }
}
