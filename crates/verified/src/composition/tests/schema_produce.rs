use {
    super::*,
    crate::schema::{SchemaBatchAdmission, SchemaFailureKind, SchemaFieldRole},
    assert2::assert,
};

fn check_frontier(
    fields: &[SchemaWalkField],
    enabled: (bool, bool),
    policy: (bool, bool),
    base_offset: i64,
    last_delta: i32,
    flags: (bool, bool),
    producer_id_epoch: (i64, i32),
) {
    let count = fields.len() / 2;
    let coherent = fields.len().is_multiple_of(2)
        && i32::try_from(count).is_ok()
        && fields.iter().enumerate().all(|(i, field)| {
            field.role
                == if i.is_multiple_of(2) {
                    SchemaFieldRole::Key
                } else {
                    SchemaFieldRole::Value
                }
        });
    let header = (0..i32::MAX).contains(&last_delta)
        && i128::try_from(count).unwrap() == i128::from(last_delta) + 1
        && count > 0
        && !flags.0
        && flags.1
        && (producer_id_epoch.0 < 0 || producer_id_epoch.1 >= 0);
    let (rows, decision) = schema_walk::oracle(fields, enabled, policy.0, policy.1);
    let target = i128::from(base_offset) + i128::try_from(count).unwrap();
    let expected = if coherent
        && header
        && decision == SchemaBatchAdmission::Admit
        && base_offset >= 0
        && target <= i128::from(i64::MAX)
    {
        Some((rows, i64::try_from(target).unwrap()))
    } else {
        None
    };
    assert!(
        schema_checked_produce_frontier(
            fields,
            enabled,
            policy,
            base_offset,
            last_delta,
            flags,
            producer_id_epoch
        ) == expected
    );
}

proptest! {
    #[test]
    fn schema_produce_matches_actual_count_and_wide_frontier_oracles(
        enabled in (any::<bool>(), any::<bool>()), policy in (any::<bool>(), any::<bool>()),
        base in any::<i64>(), delta in -2i32..35, flags in (any::<bool>(), any::<bool>()),
        producer in (any::<i64>(), any::<i32>()),
        records in prop::collection::vec((any::<bool>(), any::<bool>(), any::<bool>()), 0..16),
        coherent in any::<bool>(),
    ) {
        let fields: Vec<_> = records.iter().flat_map(|&(present, empty, checked)| [SchemaFieldRole::Key, SchemaFieldRole::Value].map(|role| SchemaWalkField {
            role: if coherent { role } else { SchemaFieldRole::Key }, present, empty,
            prefix: Some((0, 1, 2, 3, 4)), checked, failure: None,
        })).collect();
        check_frontier(&fields, enabled, policy, base, delta, flags, producer);
    }
}

#[test]
fn header_schema_and_successor_gates_all_bound_the_same_append() {
    let key = SchemaWalkField {
        role: SchemaFieldRole::Key,
        present: true,
        empty: false,
        prefix: Some((0, 255, 255, 255, 255)),
        checked: true,
        failure: None,
    };
    let value = SchemaWalkField {
        role: SchemaFieldRole::Value,
        ..key
    };
    let transient = SchemaWalkField {
        failure: Some(SchemaFailureKind::Transient),
        ..value
    };
    let unknown = SchemaWalkField {
        failure: Some(SchemaFailureKind::Unknown),
        ..value
    };
    let unframed = SchemaWalkField {
        prefix: None,
        ..transient
    };
    let layouts: &[&[SchemaWalkField]] = &[
        &[],
        &[key],
        &[key, key],
        &[key, value],
        &[key, transient],
        &[key, unknown],
        &[key, unframed],
        &[key, value, key, value],
    ];
    for fields in layouts {
        for base in [-1, 0, i64::MAX - 2, i64::MAX - 1, i64::MAX] {
            for delta in [-1, 0, 1, 2, i32::MAX - 1, i32::MAX] {
                for flags in [(false, true), (true, true), (false, false)] {
                    for producer in [(-1, -1), (0, -1), (0, 0)] {
                        for enabled in [(false, false), (true, false), (false, true), (true, true)]
                        {
                            for policy in [(false, false), (false, true), (true, true)] {
                                check_frontier(
                                    fields, enabled, policy, base, delta, flags, producer,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
