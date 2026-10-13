use {
    super::*,
    crate::schema::{SchemaBatchAdmission, SchemaFailureKind, SchemaFieldAction, SchemaFieldRole},
    assert2::assert,
};

pub(super) fn oracle(
    fields: &[SchemaWalkField],
    enabled: (bool, bool),
    open: bool,
    complete: bool,
) -> (
    Vec<(SchemaFieldAction, Option<u32>, bool)>,
    SchemaBatchAdmission,
) {
    let (key, value) = enabled;
    let rows: Vec<_> = fields
        .iter()
        .map(|field| {
            let enabled = match field.role {
                SchemaFieldRole::Key => key,
                SchemaFieldRole::Value => value,
            } && field.present;
            let action = if !enabled {
                SchemaFieldAction::Skip
            } else if field.role == SchemaFieldRole::Key {
                SchemaFieldAction::CheckKey
            } else {
                SchemaFieldAction::CheckValue
            };
            let id = field.prefix.and_then(|(magic, a, b, c, d)| {
                (enabled && !field.empty && magic == 0).then(|| u32::from_be_bytes([a, b, c, d]))
            });
            let passed = !enabled
                || field.empty
                || (id.is_some()
                    && match field.failure {
                        None => field.checked,
                        Some(kind) => open && kind == SchemaFailureKind::Transient,
                    });
            (action, id, passed)
        })
        .collect();
    // No scalar counters: admission is the conjunction of actual field outcomes.
    let decision = if complete && rows.iter().all(|row| row.2) {
        SchemaBatchAdmission::Admit
    } else {
        SchemaBatchAdmission::Reject
    };
    (rows, decision)
}

fn check_walk(fields: &[SchemaWalkField], enabled: (bool, bool), open: bool, complete: bool) {
    assert!(
        framed_schema_walk_admission(fields, enabled, open, complete)
            == oracle(fields, enabled, open, complete)
    );
}

fn failure(kind: u8) -> Option<SchemaFailureKind> {
    match kind {
        0 => None,
        1 => Some(SchemaFailureKind::Transient),
        2 => Some(SchemaFailureKind::Unknown),
        3 => Some(SchemaFailureKind::Permanent),
        _ => Some(SchemaFailureKind::Malformed),
    }
}

proptest! {
    #[test]
    fn schema_walk_matches_per_position_and_big_endian_oracles(
        key in any::<bool>(), value in any::<bool>(), open in any::<bool>(), complete in any::<bool>(),
        fields in prop::collection::vec((any::<bool>(), any::<bool>(), any::<bool>(),
            prop::option::of((any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>())),
            any::<bool>(), 0u8..5), 0..32),
    ) {
        let rows: Vec<_> = fields.into_iter().map(|(key_role, present, empty, prefix, checked, kind)| SchemaWalkField {
            role: if key_role { SchemaFieldRole::Key } else { SchemaFieldRole::Value },
            present, empty, prefix, checked, failure: failure(kind),
        }).collect();
        check_walk(&rows, (key, value), open, complete);
    }
}

#[test]
fn every_applicable_position_requires_its_own_admission() {
    let good = SchemaWalkField {
        role: SchemaFieldRole::Value,
        present: true,
        empty: false,
        prefix: Some((0, 1, 2, 3, 4)),
        checked: true,
        failure: None,
    };
    let prefixes = [
        None,
        Some((1, 0, 0, 0, 1)),
        Some((0, 0, 0, 0, 0)),
        Some((0, 255, 255, 255, 255)),
    ];
    for role in [SchemaFieldRole::Key, SchemaFieldRole::Value] {
        for prefix in prefixes {
            for kind in 0..5 {
                for present in [false, true] {
                    for empty in [false, true] {
                        for checked in [false, true] {
                            let changed = SchemaWalkField {
                                role,
                                present,
                                empty,
                                prefix,
                                checked,
                                failure: failure(kind),
                            };
                            for key in [false, true] {
                                for value in [false, true] {
                                    for open in [false, true] {
                                        for complete in [false, true] {
                                            check_walk(
                                                &[good, changed, good],
                                                (key, value),
                                                open,
                                                complete,
                                            );
                                            check_walk(
                                                &[changed, good, good],
                                                (key, value),
                                                open,
                                                complete,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for complete in [false, true] {
        check_walk(&[], (true, true), true, complete);
    }
}
