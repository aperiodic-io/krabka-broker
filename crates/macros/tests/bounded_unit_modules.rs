//! Serde validates quantities through both emitted magnitude policies.

use {
    assert2::assert,
    krabka_units::{ByteSize, bytes, convert::ByteSizeExt as _},
    serde::{Deserialize, Serialize},
};

krabka_macros::bounded_unit_modules! {
    mod positive: ByteSize [codec = krabka_units::serde_units::human::byte_size, bound = "positive", requirement = "a positive size"];
    mod nonnegative: ByteSize [codec = krabka_units::serde_units::human::byte_size, bound = "nonnegative", requirement = "a size of zero or more"];
}

#[derive(Deserialize, Serialize)]
struct Sizes {
    #[serde(with = "positive")]
    record: ByteSize,
    #[serde(with = "nonnegative")]
    key: ByteSize,
}

#[test]
fn positive_and_nonnegative_boundaries_preserve_human_values_and_diagnostics() {
    let valid: Sizes = serde_json::from_str(r#"{"record":"1B","key":"0B"}"#).unwrap();
    assert!(valid.record == bytes(1) && valid.key == ByteSize::ZERO);
    let encoded = serde_json::to_string(&valid).unwrap();
    let restored: Sizes = serde_json::from_str(&encoded).unwrap();
    assert!(restored.record == valid.record && restored.key == valid.key);
    let error = serde_json::from_str::<Sizes>(r#"{"record":"1B","key":"-1B"}"#)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("must be a size of zero or more"));
}

#[yare::parameterized(
    zero = { r#"{"record":"0B","key":"0B"}"# },
    negative = { r#"{"record":"-1B","key":"0B"}"# },
)]
fn positive_size_rejects_nonpositive_values(invalid: &str) {
    let error = serde_json::from_str::<Sizes>(invalid)
        .err()
        .unwrap()
        .to_string();
    assert2::assert!(error.contains("must be a positive size"));
}
