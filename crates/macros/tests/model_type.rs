//! Runtime contracts and original declarations survive conditional model derives.

use {
    assert2::assert,
    std::{collections::HashSet, fmt::Debug},
};

macro_rules! coordinate_type {
    () => {
        u64
    };
}

/// A declaration whose local type macro and layout attributes must survive.
#[krabka_macros::model_type]
#[repr(C)]
struct Coordinates {
    /// The field order is part of this declaration's layout.
    first: coordinate_type!(),
    second: u8,
}

#[krabka_macros::model_type(proof(std::clone::Clone, Copy))]
enum Choice {
    Named { value: Coordinates },
    Tuple(u64),
    Empty,
}

#[krabka_macros::model_type(
    proof(DeepModel),
    runtime(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        serde::Serialize,
        serde::Deserialize
    )
)]
struct Tokens(u64);

#[krabka_macros::model_type(runtime(Clone, Copy, Debug))]
struct NoEquality {
    coordinates: Coordinates,
    measurement: f64,
}

fn copied_pair<T: Clone + Copy + Debug + Eq>(value: T) -> (T, T) { (value, value) }

#[test]
fn default_and_copy_only_models_preserve_struct_enum_and_payload_contracts() {
    let pair = copied_pair(Coordinates {
        first: 42,
        second: 7,
    });
    assert!(pair.0 == pair.1);
    assert!(format!("{:?}", pair.0) == "Coordinates { first: 42, second: 7 }");
    assert!(std::mem::offset_of!(Coordinates, first) == 0);
    assert!(std::mem::offset_of!(Coordinates, second) == 8);
    for choice in [
        Choice::Named { value: pair.0 },
        Choice::Tuple(9),
        Choice::Empty,
    ] {
        let (original, copied) = copied_pair(choice);
        assert!(original == copied);
    }
    let no_equality = NoEquality {
        coordinates: pair.1,
        measurement: 1.5,
    };
    let copied = no_equality;
    assert!(copied.coordinates == pair.0);
    assert!(copied.measurement.to_bits() == 1.5_f64.to_bits());
    assert!(format!("{copied:?}").contains("NoEquality"));
}

#[test]
fn explicit_runtime_derives_keep_ordering_hashing_defaults_and_qualified_paths() {
    let mut tokens = [Tokens(3), Tokens::default(), Tokens(3), Tokens(1)];
    tokens.sort();
    assert!(tokens == [Tokens(0), Tokens(1), Tokens(3), Tokens(3)]);
    let distinct: HashSet<_> = tokens.into_iter().collect();
    assert!(distinct.len() == 3);
    let encoded = serde_json::to_string(&Tokens(11)).unwrap();
    assert!(encoded == "11");
    assert!(serde_json::from_str::<Tokens>(&encoded).unwrap() == Tokens(11));
}
