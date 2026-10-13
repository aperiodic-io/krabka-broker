//! The state that the search enumerates, the actions that move between two
//! states, and the pure queries over one state that every transition module
//! shares.
//!
//! The types live apart from the transitions so a reader can see the whole
//! search space, including the client's cached routing and the accepted
//! batch, on one screen.

use {
    super::{
        bounds::{BASE_OFFSET, BASE_SEQUENCE, MAX_LOG_LEN, NB, PRODUCER_EPOCH, PRODUCER_ID},
        witness::{WITNESS_ACKED_BEFORE_FAILOVER, WITNESS_FAILOVER, Witnesses},
    },
    crate::producer_state::{NO_EARLIER_BATCHES, ProducerEntry},
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct LogBatch {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AcceptedBatch {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ProducerEntryProjection {
    pub epoch: i16,
    pub last_sequence: i32,
    pub base_offset: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BatchState {
    Empty,
    Prepared,
    Appended,
    Acked,
    Failed,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ProduceResult {
    NotLeader,
    TimedOutUnknown,
    AppendedUnacked,
    Acked,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RequestOutcome {
    NotLeader,
    AppendedUnacked,
    TimedOutUnknown,
    Duplicate,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FailoverState {
    pub logs: [[Option<LogBatch>; MAX_LOG_LEN]; NB],
    pub leader: usize,
    pub live: u8,
    pub hwm: u8,
    pub cached_leader: usize,
    pub refresh_needed: bool,
    pub batch: BatchState,
    pub next_sequence: i32,
    pub accepted: Option<AcceptedBatch>,
    pub producer_entry: Option<ProducerEntryProjection>,
    pub acked_offset: Option<i64>,
    pub last_result: Option<ProduceResult>,
    pub send_attempts: u8,
    pub metadata_refreshes: u8,
    pub witnesses: Witnesses,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Action {
    ClientSend(RequestOutcome),
    ClientRetry(RequestOutcome),
    Replicate(usize),
    AdvanceHwm,
    AckCommitted,
    KillLeader,
    ElectClean(usize),
    RefreshMetadata,
}

impl LogBatch {
    pub fn initial() -> Self {
        Self {
            producer_id: PRODUCER_ID,
            producer_epoch: PRODUCER_EPOCH,
            base_sequence: BASE_SEQUENCE,
            offset: BASE_OFFSET,
        }
    }
}

impl ProducerEntryProjection {
    pub fn as_entry(self) -> ProducerEntry {
        ProducerEntry {
            epoch: self.epoch,
            last_sequence: self.last_sequence,
            last_offset: self.base_offset,
            base_offset: self.base_offset,
            last_timestamp: 0,
            entry_timestamp: 0,
            current_txn_first_offset: None,
            earlier: NO_EARLIER_BATCHES,
        }
    }
}

impl FailoverState {
    pub fn live(&self, broker: usize) -> bool { self.live & (1 << broker) != 0 }

    pub fn live_count(&self) -> u32 { self.live.count_ones() }

    pub fn log_len(&self, broker: usize) -> usize { self.logs[broker].iter().flatten().count() }

    pub fn log_contains_base(&self, broker: usize) -> bool {
        self.logs[broker]
            .iter()
            .flatten()
            .any(|batch| batch.producer_id == PRODUCER_ID && batch.base_sequence == BASE_SEQUENCE)
    }

    pub fn contains_hwm_prefix(&self, broker: usize) -> bool {
        self.hwm == 0 || self.log_len(broker) >= usize::from(self.hwm)
    }

    pub fn hwm_prefix_replicated(&self) -> bool {
        self.live(self.leader)
            && self.log_len(self.leader) > 0
            && (0..NB)
                .filter(|broker| self.live(*broker) && self.logs[*broker] == self.logs[self.leader])
                .count()
                >= 2
    }

    pub fn producer_entry(&self) -> Option<ProducerEntry> {
        self.producer_entry.map(ProducerEntryProjection::as_entry)
    }

    pub fn producer_entry_for_broker(&self, broker: usize) -> Option<ProducerEntryProjection> {
        let batch = self.logs[broker].iter().flatten().next()?;
        if batch.producer_id != PRODUCER_ID {
            return None;
        }
        Some(ProducerEntryProjection {
            epoch: batch.producer_epoch,
            last_sequence: batch.base_sequence,
            base_offset: batch.offset,
        })
    }

    pub fn refresh_leader_producer_entry(&mut self) {
        self.producer_entry = self.producer_entry_for_broker(self.leader);
    }

    pub fn leader_contains_accepted(&self) -> bool {
        let Some(accepted) = self.accepted else {
            return false;
        };
        self.logs[self.leader].iter().flatten().any(|batch| {
            batch.producer_id == accepted.producer_id
                && batch.producer_epoch == accepted.producer_epoch
                && batch.base_sequence == accepted.base_sequence
                && batch.offset == accepted.offset
        })
    }

    pub fn can_ack_committed(&self) -> bool {
        self.batch == BatchState::Appended
            && self.acked_offset.is_none()
            && self.hwm == 1
            && self.live(self.leader)
            && self.leader_contains_accepted()
    }

    pub fn cached_leader_current(&self) -> bool {
        self.cached_leader == self.leader && self.live(self.cached_leader)
    }

    pub fn can_try_duplicate(&self) -> bool {
        self.accepted.is_some() && self.cached_leader_current() && self.leader_contains_accepted()
    }

    pub fn mark_failover(&mut self) {
        if self.acked_offset.is_some() {
            self.witnesses.mark(WITNESS_ACKED_BEFORE_FAILOVER);
        }
        self.witnesses.mark(WITNESS_FAILOVER);
    }
}
