//! The request builders that the `AlterClientQuotas` tests share.
//!
//! The entry-validation tests and the live-broker tests build the same entry
//! and request shapes, so the two builders live in one module rather than
//! being duplicated per test file.

use krabka_protocol::owned::alter_client_quotas_request::{
    AlterClientQuotasRequest, EntityData, EntryData, OpData,
};

/// One component of a quota entity: its type and optional name.
#[derive(Clone, Copy)]
pub(super) struct EntityPart<'a> {
    pub(super) entity_type: &'a str,
    pub(super) entity_name: Option<&'a str>,
}

/// One operation on a quota key: the key, its value, and whether it removes the key.
#[derive(Clone, Copy)]
pub(super) struct QuotaOp {
    pub(super) key: &'static str,
    pub(super) value: f64,
    pub(super) remove: bool,
}

pub(super) fn part<'a>(entity_type: &'a str, entity_name: Option<&'a str>) -> EntityPart<'a> {
    EntityPart {
        entity_type,
        entity_name,
    }
}

pub(super) fn op(key: &'static str, value: f64) -> QuotaOp {
    QuotaOp {
        key,
        value,
        remove: false,
    }
}

pub(super) fn op_remove(key: &'static str, value: f64) -> QuotaOp {
    QuotaOp {
        key,
        value,
        remove: true,
    }
}

pub(super) fn entry(entity: Vec<EntityPart<'_>>, ops: Vec<QuotaOp>) -> EntryData {
    EntryData {
        entity: entity
            .into_iter()
            .map(|part| EntityData {
                entity_type: part.entity_type.into(),
                entity_name: part.entity_name.map(Into::into),
                ..Default::default()
            })
            .collect(),
        ops: ops
            .into_iter()
            .map(|op| OpData {
                key: op.key.into(),
                value: op.value,
                remove: op.remove,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn request(entries: Vec<EntryData>, validate_only: bool) -> AlterClientQuotasRequest {
    AlterClientQuotasRequest {
        entries,
        validate_only,
        ..Default::default()
    }
}
