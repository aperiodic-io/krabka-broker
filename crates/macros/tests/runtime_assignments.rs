//! Runtime tables preserve assignment order, absent fields and validation errors.

use {
    assert2::assert,
    krabka_macros::set_runtime,
    std::{ops::RangeInclusive, time::Duration},
};

#[derive(Clone, Copy)]
struct Millis(u64);

impl Millis {
    fn to_std(self) -> Duration { Duration::from_millis(self.0) }
}

#[derive(Default)]
struct Runtime {
    enabled: Option<bool>,
    timeout: Option<Millis>,
    count: Option<i32>,
    limit: Option<i32>,
    untouched: Option<i32>,
}

#[derive(Debug, Default, PartialEq)]
struct Target {
    enabled: bool,
    timeout: Duration,
    nested: Nested,
}

#[derive(Debug, Default, PartialEq)]
struct Nested {
    count: i32,
    limit: i32,
    untouched: i32,
}

fn positive_time(name: &str, value: Millis) -> Result<Millis, String> {
    if value.0 == 0 {
        Err(name.to_owned())
    } else {
        Ok(value)
    }
}

fn positive(name: &str, value: i32) -> Result<i32, String> {
    if value <= 0 {
        Err(name.to_owned())
    } else {
        Ok(value)
    }
}

fn bounded(name: &str, value: i32, range: RangeInclusive<i32>) -> Result<i32, String> {
    if range.contains(&value) {
        Ok(value)
    } else {
        Err(name.to_owned())
    }
}

fn apply(runtime: &Runtime, target: &mut Target) -> Result<(), String> {
    set_runtime! {
        runtime => target;
        plain: enabled;
        duration: timeout;
        positive: count => nested.count, untouched => nested.untouched;
        bounded: limit => nested.limit in 1..=10;
    }
    Ok(())
}

#[test]
fn applies_each_kind_and_preserves_absent_values() {
    let mut target = Target {
        nested: Nested {
            untouched: 17,
            ..Nested::default()
        },
        ..Target::default()
    };
    let runtime = Runtime {
        enabled: Some(true),
        timeout: Some(Millis(12)),
        count: Some(3),
        limit: Some(10),
        ..Runtime::default()
    };
    assert!(apply(&runtime, &mut target).is_ok());
    assert!(
        target
            == Target {
                enabled: true,
                timeout: Duration::from_millis(12),
                nested: Nested {
                    count: 3,
                    limit: 10,
                    untouched: 17
                }
            }
    );
    assert!(apply(&Runtime::default(), &mut target).is_ok());
    assert!(target.nested.untouched == 17 && target.nested.count == 3);
}

#[test]
fn stops_at_the_first_invalid_field_with_its_source_name() {
    let mut target = Target::default();
    let runtime = Runtime {
        enabled: Some(true),
        count: Some(-1),
        untouched: Some(4),
        limit: Some(11),
        ..Runtime::default()
    };
    assert!(apply(&runtime, &mut target) == Err("count".into()));
    assert!(target.enabled);
    assert!(target.nested == Nested::default());

    let runtime = Runtime {
        timeout: Some(Millis(0)),
        count: Some(-1),
        ..Runtime::default()
    };
    assert!(apply(&runtime, &mut target) == Err("timeout".into()));
    let runtime = Runtime {
        limit: Some(11),
        ..Runtime::default()
    };
    assert!(apply(&runtime, &mut target) == Err("limit".into()));
}

#[test]
fn generated_binding_does_not_shadow_caller_bindings() -> Result<(), String> {
    let runtime = Runtime {
        count: Some(3),
        ..Runtime::default()
    };
    let mut value = Nested::default();
    set_runtime! { runtime => value; positive: count; }
    assert!(value.count == 3);

    let mut target = Target::default();
    let value = 1..=10;
    set_runtime! { runtime => target; bounded: count => nested.count in value; }
    assert!(target.nested.count == 3);
    Ok(())
}
