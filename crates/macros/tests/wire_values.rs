//! Wire construction preserves explicit fields, expressions and evaluation order.

extern crate self as krabka_protocol;

use {assert2::assert, std::cell::Cell};

pub type UnknownTaggedFields = Vec<(u32, Vec<u8>)>;

// No Default implementation: every protocol field must be supplied explicitly.
#[derive(Debug, PartialEq)]
struct Record {
    version: i16,
    error_code: i16,
    values: Vec<i32>,
    unknown_tagged_fields: UnknownTaggedFields,
}

#[derive(Debug, PartialEq)]
struct Response {
    throttle_time_ms: i32,
    records: Vec<Record>,
    unknown_tagged_fields: UnknownTaggedFields,
}

mod model {
    use crate::UnknownTaggedFields;

    pub(crate) struct Empty {
        pub(crate) unknown_tagged_fields: UnknownTaggedFields,
    }

    pub(crate) struct Generic<T> {
        pub(crate) value: T,
        pub(crate) unknown_tagged_fields: UnknownTaggedFields,
    }
}

struct Pair<A, B> {
    first: A,
    second: B,
}
fn pair<A, B>(first: A, second: B) -> Pair<A, B> { Pair { first, second } }

#[test]
fn nested_constructors_preserve_shorthand_paths_versions_and_error_codes() {
    let version = 7;
    let error_code = -3;
    let actual = krabka_macros::unthrottled_wire!(Response {
        records: vec![krabka_macros::tagged_wire!(Record {
            version,
            error_code,
            values: vec![1, 2],
        })],
    });
    assert!(actual.throttle_time_ms == 0);
    assert!(actual.unknown_tagged_fields.is_empty());
    assert!(
        actual.records
            == vec![Record {
                version: 7,
                error_code: -3,
                values: vec![1, 2],
                unknown_tagged_fields: Vec::new(),
            }]
    );

    let empty = krabka_macros::tagged_wire!(crate::model::Empty {});
    assert!(empty.unknown_tagged_fields.is_empty());
    let generic = krabka_macros::tagged_wire!(model::Generic::<i32> { value: 19 });
    assert!(generic.value == 19);
    assert!(generic.unknown_tagged_fields.is_empty());
}

#[test]
fn arbitrary_expressions_keep_qualified_calls_closures_and_macro_tokens() {
    let values = |first: i32, second: i32| vec![first, second];
    let actual = krabka_macros::tagged_wire!(Record {
        version: if 1 < 2 { 4 } else { 0 },
        error_code: <i16 as From<i8>>::from(-1),
        values: values(
            {
                let pair = pair::<i32, i64>(2, 3);
                assert!(pair.second == 3);
                pair.first
            },
            vec![4].into_iter().next().unwrap(),
        )
    });
    assert!(actual.version == 4);
    assert!(actual.error_code == -1);
    assert!(actual.values == vec![2, 4]);
}

#[yare::parameterized(disabled = { false }, enabled = { true })]
fn multiline_if_else_blocks_retain_their_original_delimiter_locations(enabled: bool) {
    let response = krabka_macros::unthrottled_wire!(Response {
        records: vec![krabka_macros::tagged_wire!(Record {
            version: if enabled {
                [1, 1, 1, 1].into_iter().sum()
            } else {
                [0_i16].into_iter().sum()
            },
            error_code: 0,
            values: pair(
                if enabled {
                    [1, 2].into_iter().map(|value| value + 1).collect()
                } else {
                    Vec::new()
                },
                (),
            )
            .first,
        })],
    });
    let record = &response.records[0];
    assert2::assert!(record.version == if enabled { 4 } else { 0 });
    assert2::assert!(record.values == if enabled { vec![2, 3] } else { Vec::new() });
}

#[test]
fn supplied_expressions_are_evaluated_once_in_field_order() {
    let calls = Cell::new(0_i16);
    let next = || {
        let value = calls.get() + 1;
        calls.set(value);
        value
    };
    let response = krabka_macros::unthrottled_wire!(Response {
        records: vec![krabka_macros::tagged_wire!(Record {
            version: next(),
            error_code: next(),
            values: vec![i32::from(next())],
        })],
    });
    let actual = &response.records[0];
    assert!(calls.get() == 3);
    assert!(actual.version == 1);
    assert!(actual.error_code == 2);
    assert!(actual.values == vec![3]);
}

macro_rules! from_expression_fragment {
    ($version:expr) => {
        krabka_macros::tagged_wire!(Record {
            version: $version,
            error_code: 0,
            values: Vec::new(),
        })
    };
}

#[test]
fn an_opaque_expression_from_a_declarative_macro_keeps_its_context() {
    let version = 2;
    let actual = from_expression_fragment!(version + 1);
    assert!(actual.version == 3);
}

fn fallible(version: Result<i16, &'static str>) -> Result<Record, &'static str> {
    Ok(krabka_macros::tagged_wire!(Record {
        version: version?,
        error_code: 0,
        values: Vec::new(),
    }))
}

#[test]
fn a_supplied_question_mark_returns_the_original_error() {
    assert!(fallible(Err("version rejected")) == Err("version rejected"));
    assert!(fallible(Ok(5)).unwrap().version == 5);
}

#[test]
fn field_expressions_accept_reference_and_tuple_closure_patterns() {
    let response = krabka_macros::unthrottled_wire!(Response {
        records: vec![
            krabka_macros::tagged_wire!(Record {
                version: 1,
                error_code: 0,
                values: [1, 2].iter().map(|&value| value + 1).collect(),
            }),
            krabka_macros::tagged_wire!(Record {
                version: 2,
                error_code: 0,
                values: [(1, 2), (3, 4)]
                    .into_iter()
                    .map(|(left, right)| left + right)
                    .collect(),
            }),
        ],
    });
    assert!(response.records[0].values == vec![2, 3]);
    assert!(response.records[1].values == vec![3, 7]);
}

#[test]
fn range_fields_are_distinct_from_struct_updates() {
    let through = krabka_macros::tagged_wire!(model::Generic { value: ..=7 });
    assert!(through.value.end == 7);
    let bounded = krabka_macros::tagged_wire!(model::Generic { value: 2..5 });
    assert!(bounded.value.start == 2);
    assert!(bounded.value.end == 5);
}
