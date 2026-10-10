//! The eleven `always` properties that state what must never happen, and the
//! ten `sometimes` properties that prove the search reached the
//! interleavings those claims are about.
//!
//! They are stated apart from the transition function so the claims can be
//! read without the mechanics that make them reachable.

use stateright::Property;

use super::{
    bounds::{
        BASE_OFFSET, BASE_SEQUENCE, INITIAL_LEADER, MAX_METADATA_REFRESHES, MAX_SEND_ATTEMPTS,
        PRODUCER_EPOCH, PRODUCER_ID,
    },
    model::ClientServerFailoverModel,
    state::{AcceptedBatch, FailoverState, ProduceResult},
    witness::{
        WITNESS_ACKED_BEFORE_FAILOVER, WITNESS_APPENDED_UNACKED, WITNESS_DUPLICATE_AFTER_UNKNOWN,
        WITNESS_DUPLICATE_RESPONSE, WITNESS_NOT_LEADER, WITNESS_PREPARED_RETRY,
        WITNESS_RETRY_AFTER_FAILOVER, WITNESS_TIMED_OUT_UNKNOWN,
        WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER,
    },
};

impl ClientServerFailoverModel {
    pub fn safety_properties() -> Vec<Property<Self>> {
        vec![
            Property::always("acked_all_durable", |_, s: &FailoverState| {
                s.acked_offset.is_none_or(|offset| {
                    offset >= i64::from(s.hwm) || leader_has_batch_at(s, krabka_ids::Offset(offset))
                })
            }),
            Property::always("ack_requires_hwm", |_, s: &FailoverState| {
                s.acked_offset.is_none_or(|offset| {
                    s.hwm == 1 && leader_has_batch_at(s, krabka_ids::Offset(offset))
                })
            }),
            Property::always("no_duplicate_append", |_, s: &FailoverState| {
                s.logs.iter().all(|log| {
                    log.iter()
                        .flatten()
                        .filter(|batch| {
                            batch.producer_id == PRODUCER_ID && batch.base_sequence == BASE_SEQUENCE
                        })
                        .count()
                        <= 1
                })
            }),
            Property::always("no_sequence_skip_on_reroute", |_, s: &FailoverState| {
                matches!(s.next_sequence, BASE_SEQUENCE..=1)
            }),
            Property::always(
                "not_leader_before_append_preserves_sequence",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::NotLeader)
                        || s.accepted.is_some()
                        || s.next_sequence == BASE_SEQUENCE
                },
            ),
            Property::always(
                "appended_unacked_not_acknowledged",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::AppendedUnacked)
                        || s.acked_offset.is_none()
                },
            ),
            Property::always(
                "acked_result_requires_committed_leader",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::Acked)
                        || (s.hwm == 1
                            && s.acked_offset == Some(BASE_OFFSET)
                            && s.leader_contains_accepted())
                },
            ),
            Property::always(
                "unknown_timeout_records_acceptance",
                |_, s: &FailoverState| {
                    s.last_result != Some(ProduceResult::TimedOutUnknown)
                        || (s.accepted
                            == Some(AcceptedBatch {
                                producer_id: PRODUCER_ID,
                                producer_epoch: PRODUCER_EPOCH,
                                base_sequence: BASE_SEQUENCE,
                                offset: BASE_OFFSET,
                            })
                            && s.next_sequence == BASE_SEQUENCE + 1)
                },
            ),
            Property::always("send_attempts_capped", |_, s: &FailoverState| {
                s.send_attempts <= MAX_SEND_ATTEMPTS
            }),
            Property::always("metadata_refreshes_capped", |_, s: &FailoverState| {
                s.metadata_refreshes <= MAX_METADATA_REFRESHES
            }),
            Property::always(
                "clean_leader_contains_hwm_prefix",
                |_, s: &FailoverState| s.log_len(s.leader) >= usize::from(s.hwm),
            ),
        ]
    }

    pub fn witness_properties() -> Vec<Property<Self>> {
        vec![
            Property::sometimes("not_leader_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_NOT_LEADER)
            }),
            Property::sometimes("timed_out_unknown_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_TIMED_OUT_UNKNOWN)
            }),
            Property::sometimes("appended_unacked_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_APPENDED_UNACKED)
            }),
            Property::sometimes("duplicate_after_unknown", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_DUPLICATE_AFTER_UNKNOWN)
            }),
            Property::sometimes("unknown_retry_after_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER)
            }),
            Property::sometimes("ack_before_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_ACKED_BEFORE_FAILOVER)
            }),
            Property::sometimes("retry_after_failover", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_RETRY_AFTER_FAILOVER)
            }),
            Property::sometimes("prepared_retry", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_PREPARED_RETRY)
            }),
            Property::sometimes("duplicate_response", |_, s: &FailoverState| {
                s.witnesses.seen(WITNESS_DUPLICATE_RESPONSE)
            }),
            Property::sometimes("clean_failover_preserves_ack", |_, s: &FailoverState| {
                s.leader != INITIAL_LEADER
                    && s.live(s.leader)
                    && s.hwm == 1
                    && s.acked_offset == Some(BASE_OFFSET)
                    && s.log_contains_base(s.leader)
            }),
        ]
    }
}

fn leader_has_batch_at(state: &FailoverState, offset: krabka_ids::Offset) -> bool {
    state.logs[state.leader].iter().flatten().any(|batch| {
        batch.producer_id == PRODUCER_ID
            && batch.offset == offset.0
            && batch.base_sequence == BASE_SEQUENCE
    })
}
