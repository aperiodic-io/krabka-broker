//! Schema-registry failure classification and fail-open admission.

use creusot_std::prelude::*;

/// Security-relevant class of a failed schema-registry lookup.
#[krabka_macros::model_type]
pub enum SchemaFailureKind {
    /// The registry answered that the schema ID does not exist.
    Unknown,
    /// No authoritative answer is available yet: transport, throttling, or 5xx.
    Transient,
    /// The registry definitively rejected the request, such as with another 4xx.
    Permanent,
    /// A successful response could not be decoded into the required shape.
    Malformed,
}

/// Whether a failed lookup rejects the record or admits it without validation.
#[krabka_macros::model_type]
pub enum SchemaFailureDecision {
    Reject,
    AllowUnvalidated,
}

/// Apply the configured fail-open policy to a classified registry failure.
///
/// Only a transient failure may be admitted, and only when the operator opted
/// into fail-open behavior. Definite and malformed answers always fail closed.
#[ensures((result == SchemaFailureDecision::AllowUnvalidated)
    == (fail_open && failure == SchemaFailureKind::Transient))]
#[must_use]
pub fn schema_failure_decision(
    fail_open: bool,
    failure: SchemaFailureKind,
) -> SchemaFailureDecision {
    match failure {
        SchemaFailureKind::Transient if fail_open => SchemaFailureDecision::AllowUnvalidated,
        _ => SchemaFailureDecision::Reject,
    }
}

/// A schema-validated record field's role in subject selection.
#[krabka_macros::model_type]
pub enum SchemaFieldRole {
    Key,
    Value,
}

/// Whether the schema gate skips a field or validates it under one role.
#[krabka_macros::model_type]
pub enum SchemaFieldAction {
    Skip,
    CheckKey,
    CheckValue,
}

/// Whether every applicable field in a record batch was admitted.
#[krabka_macros::model_type]
pub enum SchemaBatchAdmission {
    Admit,
    Reject,
}

/// Decode the exact big-endian schema ID from a complete Confluent prefix.
///
/// The adapter supplies the five prefix bytes only after establishing that
/// they exist. Magic zero is the sole admitted framing version.
#[ensures(match result {
    None => magic != 0u8,
    Some(id) => magic == 0u8
        && id@ == id_0@ * 16_777_216 + id_1@ * 65_536 + id_2@ * 256 + id_3@,
})]
#[must_use]
pub fn schema_frame_id(magic: u8, id_0: u8, id_1: u8, id_2: u8, id_3: u8) -> Option<u32> {
    if magic != 0 {
        return None;
    }
    Some(
        u32::from(id_0) * 16_777_216
            + u32::from(id_1) * 65_536
            + u32::from(id_2) * 256
            + u32::from(id_3),
    )
}

/// Select exactly the configured, non-null key or value field.
#[ensures((result == SchemaFieldAction::CheckKey)
    == (key_enabled && present && role == SchemaFieldRole::Key))]
#[ensures((result == SchemaFieldAction::CheckValue)
    == (value_enabled && present && role == SchemaFieldRole::Value))]
#[ensures((result == SchemaFieldAction::Skip)
    == (!present
        || (role == SchemaFieldRole::Key && !key_enabled)
        || (role == SchemaFieldRole::Value && !value_enabled)))]
#[must_use]
pub fn schema_field_action(
    key_enabled: bool,
    value_enabled: bool,
    role: SchemaFieldRole,
    present: bool,
) -> SchemaFieldAction {
    if present {
        match role {
            SchemaFieldRole::Key if key_enabled => SchemaFieldAction::CheckKey,
            SchemaFieldRole::Value if value_enabled => SchemaFieldAction::CheckValue,
            SchemaFieldRole::Key | SchemaFieldRole::Value => SchemaFieldAction::Skip,
        }
    } else {
        SchemaFieldAction::Skip
    }
}

/// Admit a batch only after a complete walk where every applicable field was admitted.
#[ensures((result == SchemaBatchAdmission::Admit)
    == (walk_complete && applicable == admitted))]
#[must_use]
pub fn schema_batch_admission(
    walk_complete: bool,
    applicable: u64,
    admitted: u64,
) -> SchemaBatchAdmission {
    if walk_complete && applicable == admitted {
        SchemaBatchAdmission::Admit
    } else {
        SchemaBatchAdmission::Reject
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[yare::parameterized(
        unknown = { SchemaFailureKind::Unknown, SchemaFailureDecision::Reject },
        transient = { SchemaFailureKind::Transient, SchemaFailureDecision::AllowUnvalidated },
        permanent = { SchemaFailureKind::Permanent, SchemaFailureDecision::Reject },
        malformed = { SchemaFailureKind::Malformed, SchemaFailureDecision::Reject },
    )]
    fn fail_open_admits_only_transient_failures(
        failure: SchemaFailureKind,
        open_decision: SchemaFailureDecision,
    ) {
        check!(schema_failure_decision(false, failure) == SchemaFailureDecision::Reject);
        check!(schema_failure_decision(true, failure) == open_decision);
    }

    #[yare::parameterized(
        ordinary = { 0, 0x01, 0x23, 0x45, 0x67, Some(0x0123_4567) },
        one = { 0, 0, 0, 0, 1, Some(1) },
        max_id = { 0, 0xff, 0xff, 0xff, 0xff, Some(u32::MAX) },
        wrong_magic = { 1, 0x01, 0x23, 0x45, 0x67, None },
        invalid_magic = { 0xff, 0, 0, 0, 1, None },
    )]
    fn confluent_framing_is_magic_zero_then_a_big_endian_id(
        magic: u8,
        id_0: u8,
        id_1: u8,
        id_2: u8,
        id_3: u8,
        expected: Option<u32>,
    ) {
        check!(schema_frame_id(magic, id_0, id_1, id_2, id_3) == expected);
    }

    #[yare::parameterized(
        key_enabled = {
            "key validation checks a key",
            true,
            false,
            SchemaFieldRole::Key,
            true,
            SchemaFieldAction::CheckKey,
        },
        value_enabled = {
            "value validation checks a value",
            false,
            true,
            SchemaFieldRole::Value,
            true,
            SchemaFieldAction::CheckValue,
        },
        both_key = {
            "both enabled checks a key",
            true,
            true,
            SchemaFieldRole::Key,
            true,
            SchemaFieldAction::CheckKey,
        },
        both_value = {
            "both enabled checks a value",
            true,
            true,
            SchemaFieldRole::Value,
            true,
            SchemaFieldAction::CheckValue,
        },
        null_key = {
            "a null key is exempt",
            true,
            true,
            SchemaFieldRole::Key,
            false,
            SchemaFieldAction::Skip,
        },
        null_value = {
            "a null value (tombstone) is exempt",
            true,
            true,
            SchemaFieldRole::Value,
            false,
            SchemaFieldAction::Skip,
        },
        value_only_skips_key = {
            "value-only validation skips the key",
            false,
            true,
            SchemaFieldRole::Key,
            true,
            SchemaFieldAction::Skip,
        },
        key_only_skips_value = {
            "key-only validation skips the value",
            true,
            false,
            SchemaFieldRole::Value,
            true,
            SchemaFieldAction::Skip,
        },
        disabled = {
            "validation disabled skips everything",
            false,
            false,
            SchemaFieldRole::Value,
            true,
            SchemaFieldAction::Skip,
        },
    )]
    fn only_configured_non_null_fields_are_validated(
        scenario: &str,
        key_enabled: bool,
        value_enabled: bool,
        role: SchemaFieldRole,
        present: bool,
        expected: SchemaFieldAction,
    ) {
        check!(
            schema_field_action(key_enabled, value_enabled, role, present) == expected,
            "{scenario}"
        );
    }

    #[yare::parameterized(
        empty = { true, 0, 0, SchemaBatchAdmission::Admit },
        all_admitted = { true, 4, 4, SchemaBatchAdmission::Admit },
        one_missing = { true, 4, 3, SchemaBatchAdmission::Reject },
        too_many_admitted = { true, 3, 4, SchemaBatchAdmission::Reject },
        incomplete = { false, 4, 4, SchemaBatchAdmission::Reject },
        incomplete_empty = { false, 0, 0, SchemaBatchAdmission::Reject },
    )]
    fn batch_admits_only_a_complete_walk_with_every_field_admitted(
        walk_complete: bool,
        applicable: u64,
        admitted: u64,
        expected: SchemaBatchAdmission,
    ) {
        check!(schema_batch_admission(walk_complete, applicable, admitted) == expected);
    }
}
