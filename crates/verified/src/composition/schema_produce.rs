#[cfg(creusot)]
use super::schema_walk::{decoded_id, field_admitted};
use {
    super::schema_walk::{SchemaWalkField, framed_schema_walk_admission},
    crate::{
        produce::{ProduceBatchAdmission, produce_batch_admission, produce_durability_frontier},
        schema::{SchemaBatchAdmission, SchemaFieldAction, SchemaFieldRole},
    },
    creusot_std::prelude::*,
};

type PreparedSchemaAppend = Option<(Vec<(SchemaFieldAction, Option<u32>, bool)>, i64)>;

open_logic! {
/// A coherent decoded walk has one key/value pair per record, in that order.
fn record_pairs(fields: Seq<SchemaWalkField>) -> bool {
    pearlite! { fields.len() % 2 == 0 && fields.len() / 2 <= i32::MAX@
    && forall<i: Int> 0 <= i && i < fields.len() ==> fields[i].role
        == if i % 2 == 0 { SchemaFieldRole::Key } else { SchemaFieldRole::Value } }
}
}

open_logic! {
/// Header admission uses the actual pair count, rather than a supplied tally.
fn header_admits(count: Int, delta: i32, flags: (bool, bool), producer: (i64, i32)) -> bool {
    pearlite! { 0 <= delta@ && delta@ < i32::MAX@ && count == delta@ + 1
    && count > 0 && !flags.0 && flags.1 && (producer.0@ < 0 || producer.1@ >= 0) }
}
}

/// Connect per-position schema validation to real Produce header admission and
/// its exact exclusive acknowledgement frontier. Invalid field-pair shape,
/// incomplete walks, schema failures and unrepresentable frontiers reject.
/// Decoded fields/header facts and actual registry/body validation must be
/// faithful. This proves neither byte decoding nor durable append completion.
#[requires(fields@.len() <= u64::MAX@)]
#[ensures((result != None) == (record_pairs(fields@)
    && header_admits(fields@.len() / 2, delta, flags, producer) && policy.1
    && (forall<i: Int> 0 <= i && i < fields@.len()
        ==> field_admitted(fields@[i], enabled.0, enabled.1, policy.0))
    && base@ >= 0 && base@ + delta@ + 1 <= i64::MAX@))]
#[ensures(match result { None => true, Some((rows, frontier)) =>
    rows@.len() == fields@.len() && frontier@ == base@ + fields@.len() / 2
    && base@ < frontier@
    && forall<i: Int> 0 <= i && i < fields@.len() ==> rows@[i].2
        && decoded_id(fields@[i], enabled.0, enabled.1, rows@[i].1)
        && ((rows@[i].0 == SchemaFieldAction::Skip)
            == (!fields@[i].present || match fields@[i].role {
                SchemaFieldRole::Key => !enabled.0, SchemaFieldRole::Value => !enabled.1,
            }))
        && ((rows@[i].0 == SchemaFieldAction::CheckKey)
            == (fields@[i].present && enabled.0 && fields@[i].role == SchemaFieldRole::Key))
        && ((rows@[i].0 == SchemaFieldAction::CheckValue)
            == (fields@[i].present && enabled.1 && fields@[i].role == SchemaFieldRole::Value)),
})]
pub(super) fn schema_checked_produce_frontier(
    fields: &[SchemaWalkField],
    enabled: (bool, bool),
    policy: (bool, bool), // fail-open, complete walk
    base: i64,
    delta: i32,
    flags: (bool, bool), // control batch, create time
    producer: (i64, i32),
) -> PreparedSchemaAppend {
    let (fail_open, walk_complete) = policy;
    if !fields.len().is_multiple_of(2) {
        return None;
    }
    let mut i = 0usize;
    let mut count = 0i32;
    #[invariant(i@ <= fields@.len())]
    #[invariant(count@ == (i@ + 1) / 2)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> fields@[j].role
        == if j % 2 == 0 { SchemaFieldRole::Key } else { SchemaFieldRole::Value })]
    #[variant(fields@.len() - i@)]
    while i < fields.len() {
        let matches = if i.is_multiple_of(2) {
            matches!(fields[i].role, SchemaFieldRole::Key)
        } else {
            matches!(fields[i].role, SchemaFieldRole::Value)
        };
        if !matches {
            return None;
        }
        if i.is_multiple_of(2) {
            if count == i32::MAX {
                return None;
            }
            count += 1;
        }
        i += 1;
    }
    if !matches!(
        produce_batch_admission(delta, count, flags.0, producer.0, producer.1, flags.1),
        ProduceBatchAdmission::Admit
    ) {
        return None;
    }
    let (rows, admission) = framed_schema_walk_admission(fields, enabled, fail_open, walk_complete);
    if !matches!(admission, SchemaBatchAdmission::Admit) {
        return None;
    }
    let frontier = produce_durability_frontier(base, delta)?;
    Some((rows, frontier))
}
