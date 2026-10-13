use {
    crate::schema::{
        SchemaBatchAdmission, SchemaFailureDecision, SchemaFailureKind, SchemaFieldAction,
        SchemaFieldRole, schema_batch_admission, schema_failure_decision, schema_field_action,
        schema_frame_id,
    },
    creusot_std::prelude::*,
};

model_types! {
    @derives (derive(DeepModel))
        (derive(Clone, Copy, Debug));
    /// One field position from the decoded walk. `checked` means subject/body
    /// validation succeeded; `failure` classifies a registry lookup failure.
    /// Empty non-null fields are admitted by the host without registry validation.
    pub struct SchemaWalkField {
        pub role: SchemaFieldRole,
        pub present: bool,
        pub empty: bool,
        pub prefix: Option<(u8, u8, u8, u8, u8)>,
        pub checked: bool,
        pub failure: Option<SchemaFailureKind>,
    }
}

open_logic! {
pub fn required(field: SchemaWalkField, key: bool, value: bool) -> bool {
    pearlite! { field.present && match field.role {
        SchemaFieldRole::Key => key, SchemaFieldRole::Value => value,
    } }
}
}

open_logic! {
/// Acceptance over the actual field facts, independent of supplied counters.
pub(super) fn field_admitted(
    field: SchemaWalkField,
    key: bool,
    value: bool,
    fail_open: bool,
) -> bool {
    pearlite! { !required(field, key, value) || field.empty
    || (match field.prefix { Some((magic, _, _, _, _)) => magic@ == 0, None => false }
        && match field.failure {
            None => field.checked,
            Some(failure) => fail_open && failure == SchemaFailureKind::Transient,
        }) }
}
}

open_logic! {
/// IDs retain all four raw prefix bytes and exist only on applicable frames.
pub(super) fn decoded_id(field: SchemaWalkField, key: bool, value: bool, id: Option<u32>) -> bool {
    pearlite! { match (field.prefix, id) {
        (Some((magic, a, b, c, d)), Some(id)) => required(field, key, value) && !field.empty
            && magic@ == 0 && id@ == a@ * 16_777_216 + b@ * 65_536 + c@ * 256 + d@,
        (prefix, None) => !required(field, key, value) || field.empty
            || match prefix { None => true, Some((magic, _, _, _, _)) => magic@ != 0 },
        (None, Some(_)) => false,
    } }
}
}

type SchemaWalk = (
    Vec<(SchemaFieldAction, Option<u32>, bool)>,
    SchemaBatchAdmission,
);

/// Derive both batch counters from one visit to each actual field position.
/// Admission means every applicable field was accepted: null/disabled fields
/// are skipped, empty fields pass, and other fields need a valid frame and
/// successful validation or an explicitly allowed transient registry failure.
/// Faithful enumeration, complete decoding, registry classification and actual
/// validation are host facts. This does not prove the serde/registry I/O.
#[requires(fields@.len() <= u64::MAX@)]
#[ensures(result.0@.len() == fields@.len())]
#[ensures(forall<i: Int> 0 <= i && i < fields@.len()
    ==> result.0@[i].2 == field_admitted(fields@[i], enabled.0, enabled.1, fail_open))]
#[ensures(forall<i: Int> 0 <= i && i < fields@.len()
    ==> ((result.0@[i].0 == SchemaFieldAction::Skip) == !required(fields@[i], enabled.0, enabled.1))
    && ((result.0@[i].0 == SchemaFieldAction::CheckKey)
        == (required(fields@[i], enabled.0, enabled.1) && fields@[i].role == SchemaFieldRole::Key))
    && ((result.0@[i].0 == SchemaFieldAction::CheckValue)
        == (required(fields@[i], enabled.0, enabled.1) && fields@[i].role == SchemaFieldRole::Value)))]
#[ensures(forall<i: Int> 0 <= i && i < fields@.len()
    ==> decoded_id(fields@[i], enabled.0, enabled.1, result.0@[i].1))]
#[ensures((result.1 == SchemaBatchAdmission::Admit) == (walk_complete
    && forall<i: Int> 0 <= i && i < fields@.len()
        ==> field_admitted(fields@[i], enabled.0, enabled.1, fail_open)))]
pub(super) fn framed_schema_walk_admission(
    fields: &[SchemaWalkField],
    enabled: (bool, bool), // key, value
    fail_open: bool,
    walk_complete: bool,
) -> SchemaWalk {
    let mut rows: Vec<(SchemaFieldAction, Option<u32>, bool)> = Vec::new();
    let mut applicable = 0u64;
    let mut admitted = 0u64;
    let mut i = 0usize;
    #[invariant(rows@.len() == i@ && i@ <= fields@.len())]
    #[invariant(admitted@ <= applicable@ && applicable@ <= i@)]
    #[invariant((applicable == admitted)
        == (forall<j: Int> 0 <= j && j < i@ ==> rows@[j].2))]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> rows@[j].2 == field_admitted(fields@[j], enabled.0, enabled.1, fail_open)
        && ((rows@[j].0 == SchemaFieldAction::Skip) == !required(fields@[j], enabled.0, enabled.1))
        && ((rows@[j].0 == SchemaFieldAction::CheckKey)
            == (required(fields@[j], enabled.0, enabled.1) && fields@[j].role == SchemaFieldRole::Key))
        && ((rows@[j].0 == SchemaFieldAction::CheckValue)
            == (required(fields@[j], enabled.0, enabled.1) && fields@[j].role == SchemaFieldRole::Value)))]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> decoded_id(fields@[j], enabled.0, enabled.1, rows@[j].1))]
    #[variant(fields@.len() - i@)]
    while i < fields.len() {
        let field = &fields[i];
        let action = schema_field_action(enabled.0, enabled.1, field.role, field.present);
        let mut id = None;
        let mut accepted = true;
        if !matches!(action, SchemaFieldAction::Skip) {
            applicable += 1;
            if !field.empty {
                if let Some((magic, a, b, c, d)) = field.prefix {
                    id = schema_frame_id(magic, a, b, c, d);
                }
                accepted = id.is_some()
                    && match field.failure {
                        None => field.checked,
                        Some(failure) => matches!(
                            schema_failure_decision(fail_open, failure),
                            SchemaFailureDecision::AllowUnvalidated
                        ),
                    };
            }
            if accepted {
                admitted += 1;
            }
        }
        rows.push((action, id, accepted));
        i += 1;
    }
    (
        rows,
        schema_batch_admission(walk_complete, applicable, admitted),
    )
}
