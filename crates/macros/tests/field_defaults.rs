//! What `#[derive(FieldDefaults)]` fills each field with.

use {
    assert2::assert,
    krabka_macros::FieldDefaults,
    std::{
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    },
};

/// Literals, calls, a constant, and fields with no attribute.
#[derive(Debug, PartialEq, FieldDefaults)]
struct Settings {
    /// A doc comment sits beside `#[default]`.
    #[default(Duration::from_secs(30))]
    interval: Duration,
    #[default(true)]
    enabled: bool,
    #[default(Self::LIMIT * 2)]
    limit: u64,
    #[default("orders".to_owned())]
    name: String,
    #[default(Some(vec![1, 2]))]
    partitions: Option<Vec<i32>>,
    ratio: f64,
    owners: Vec<String>,
}

impl Settings {
    const LIMIT: u64 = 512;
}

#[test]
fn fills_attributed_fields_with_their_expression_and_the_rest_with_default() {
    assert!(
        Settings::default()
            == Settings {
                interval: Duration::from_secs(30),
                enabled: true,
                limit: 1024,
                name: "orders".to_owned(),
                partitions: Some(vec![1, 2]),
                ratio: 0.0,
                owners: Vec::new(),
            }
    );
}

/// A generic struct, whose bounds the struct itself carries.
#[derive(Debug, PartialEq, FieldDefaults)]
struct Slot<T: Default> {
    #[default(7)]
    epoch: i32,
    value: T,
}

#[test]
fn keeps_the_struct_generics() {
    assert!(
        Slot::<String>::default()
            == Slot {
                epoch: 7,
                value: String::new(),
            }
    );
}

static CALLS: AtomicU32 = AtomicU32::new(0);

fn next_id() -> u32 { CALLS.fetch_add(1, Ordering::SeqCst) + 1 }

/// An expression with a side effect.
#[derive(Debug, PartialEq, FieldDefaults)]
struct Counted {
    #[default(next_id())]
    id: u32,
}

#[test]
fn evaluates_the_expression_on_every_call() {
    let first = Counted::default();
    let second = Counted::default();
    assert!(second.id == first.id + 1);
}
