//! The per-record walk: the running chain state, and the two checks that
//! advance it.
//!
//! [`check_chained`] recomputes a data record's hash link and [`check_checkpoint`]
//! validates a signed checkpoint against that running chain, both against the
//! same [`WalkState`] the segment walk in [`super::verify_partition_dir`] carries
//! from record to record. They are the primitives the writer's own chaining
//! mirrors, kept in one file because a break found by either one is reported the
//! same way.

use {
    super::{TrustedKeys, VerifyBreak, VerifyLoss, VerifyReport},
    crate::{
        chain::{GENESIS_HEAD, chain_hash, from_hex32},
        checkpoint::Checkpoint,
        event::AuditEventClass,
        ids::{CheckpointCount, RecordCount, Seq},
        sink::{HEADER_PREV_HASH, HEADER_SEQ},
    },
    krabka_protocol::records::Record,
    krabka_verified::{
        AuditCheckpointAdmission, AuditLossMarkerAdmission, ChainStep, audit_checkpoint_admission,
        audit_loss_marker_admission, chain_step,
    },
};

pub(super) fn header<'a>(record: &'a Record, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value.as_deref())
}

fn broke(state: &WalkState, offset: i64, seq: Option<Seq>, reason: &str) -> VerifyReport {
    VerifyReport {
        records: state.records,
        checkpoints: state.checkpoints,
        ok: false,
        first_break: Some(VerifyBreak {
            offset,
            seq,
            reason: reason.to_string(),
        }),
        losses: state.losses.clone(),
        // unanchored_records is 0 when ok=false; the count is not meaningful
        // after a break since the walk stopped early.
        unanchored_records: RecordCount(0),
    }
}

/// Mutable per-record walk state that the helper functions share.
pub(super) struct WalkState {
    head: [u8; 32],
    expected_seq: Seq,
    pub(super) records: RecordCount,
    pub(super) checkpoints: CheckpointCount,
    pub(super) losses: Vec<VerifyLoss>,
    /// The `seq_high` of the most-recently validated checkpoint, if any.
    pub(super) last_checkpoint_seq_high: Option<Seq>,
    last_loss_generation: u64,
}

impl WalkState {
    pub(super) fn new() -> Self {
        Self {
            head: GENESIS_HEAD,
            expected_seq: Seq(0),
            records: RecordCount(0),
            checkpoints: CheckpointCount(0),
            losses: Vec::new(),
            last_checkpoint_seq_high: None,
            last_loss_generation: 0,
        }
    }
}

/// Validate a checkpoint record against the running chain and trusted keys.
///
/// Returns `Err(VerifyReport)` on the first detected break.
pub(super) fn check_checkpoint(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
    trusted: &TrustedKeys,
) -> Result<(), VerifyReport> {
    state.checkpoints.0 += 1;
    let value = rec.value.as_deref().unwrap_or_default();
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) else {
        return Err(broke(state, offset, None, "checkpoint value is not JSON"));
    };
    let Some(cp) = Checkpoint::from_value(&json) else {
        return Err(broke(state, offset, None, "malformed checkpoint"));
    };
    let Some(pubkey) = trusted.get(&cp.key_id) else {
        return Err(broke(
            state,
            offset,
            None,
            &format!("no trusted key for key_id '{}'", cp.key_id),
        ));
    };
    match audit_checkpoint_admission(
        cp.verify(pubkey),
        cp.chain_head == state.head,
        state.expected_seq.0,
        cp.seq_high.0,
    ) {
        AuditCheckpointAdmission::Admit => {}
        AuditCheckpointAdmission::RejectSignature => {
            return Err(broke(state, offset, None, "checkpoint signature invalid"));
        }
        AuditCheckpointAdmission::RejectHead => {
            return Err(broke(
                state,
                offset,
                None,
                "checkpoint chain_head does not match recomputed chain",
            ));
        }
        AuditCheckpointAdmission::RejectSequence => {
            return Err(broke(
                state,
                offset,
                None,
                "checkpoint seq_high does not match record count",
            ));
        }
    }
    state.last_checkpoint_seq_high = Some(cp.seq_high);
    Ok(())
}

/// Validate a chained data record and advance the chain head.
///
/// Returns `Err(VerifyReport)` on the first detected break.
pub(super) fn check_chained(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
) -> Result<(), VerifyReport> {
    let value = rec.value.as_deref().unwrap_or_default();
    let seq = header(rec, HEADER_SEQ)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Seq);
    let prev = header(rec, HEADER_PREV_HASH)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(from_hex32);
    let (Some(seq), Some(prev)) = (seq, prev) else {
        return Err(broke(state, offset, seq, "missing/invalid chain headers"));
    };
    let next_seq = match chain_step(state.expected_seq.0, seq.0, prev == state.head) {
        ChainStep::SequenceMismatch => {
            return Err(broke(
                state,
                offset,
                Some(seq),
                &format!("seq gap: expected {}, found {seq}", state.expected_seq),
            ));
        }
        ChainStep::HeadMismatch => {
            return Err(broke(
                state,
                offset,
                Some(seq),
                "prev_hash does not match recomputed chain head",
            ));
        }
        ChainStep::Exhausted => {
            return Err(broke(state, offset, Some(seq), "chain sequence exhausted"));
        }
        ChainStep::Continue(next_seq) => next_seq,
    };
    state.head = chain_hash(&state.head, seq.0, value);
    state.expected_seq = Seq(next_seq);
    state.records.0 += 1;
    Ok(())
}

/// Validate and record a chained fail-open loss marker.
pub(super) fn check_records_lost(
    rec: &Record,
    offset: i64,
    state: &mut WalkState,
) -> Result<(), VerifyReport> {
    let header_matches =
        header(rec, "event_class") == Some(AuditEventClass::RecordsLost.as_header().as_bytes());
    let Some(fields) = records_lost_fields(rec) else {
        return Err(broke(
            state,
            offset,
            None,
            "records-lost body/event_class header mismatch",
        ));
    };
    match audit_loss_marker_admission(
        header_matches,
        fields.field_count,
        fields.count,
        fields.generation,
        state.last_loss_generation,
    ) {
        AuditLossMarkerAdmission::Admit { generation } => {
            state.last_loss_generation = generation;
        }
        AuditLossMarkerAdmission::Reject => {
            return Err(broke(
                state,
                offset,
                None,
                "records-lost body/event_class header mismatch",
            ));
        }
    }

    check_chained(rec, offset, state)?;
    state.losses.push(VerifyLoss {
        offset,
        seq: Seq(state.expected_seq.0 - 1),
        records: RecordCount(fields.count),
    });
    Ok(())
}

/// The fields of a reserved records-lost marker body.
///
/// The marker shape is exactly `records_lost` and `loss_generation`; the
/// kernel rejects any other field count or a missing generation.
struct LossMarkerFields {
    field_count: u64,
    count: u64,
    generation: Option<u64>,
}

fn records_lost_fields(rec: &Record) -> Option<LossMarkerFields> {
    let value = rec.value.as_deref()?;
    let json = serde_json::from_slice::<serde_json::Value>(value).ok()?;
    let object = json.as_object()?;
    let count = object
        .get("records_lost")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let generation = object
        .get("loss_generation")
        .and_then(serde_json::Value::as_u64);
    Some(LossMarkerFields {
        field_count: u64::try_from(object.len()).unwrap_or(u64::MAX),
        count,
        generation,
    })
}

pub(super) fn records_lost_body_has_field(rec: &Record) -> bool {
    rec.value
        .as_deref()
        .and_then(|value| serde_json::from_slice::<serde_json::Value>(value).ok())
        .and_then(|json| {
            json.as_object()
                .map(|object| object.contains_key("records_lost"))
        })
        .unwrap_or(false)
}
