//! The generated schema is consumed by schemars itself.

use {
    assert2::assert,
    schemars::{JsonSchema, Schema, SchemaGenerator, json_schema},
    std::borrow::Cow,
};

/// A byte extent written with a suffix.
#[derive(krabka_macros::UnitSchema)]
#[unit_schema(format = "byte-size")]
struct Size;

#[test]
fn marker_schema_is_inline_and_retains_the_public_unit_format() {
    assert!(Size::schema_name() == "Size" && Size::inline_schema());
    let schema = Size::json_schema(&mut SchemaGenerator::default());
    assert!(schema == json_schema!({ "type": "string", "format": "byte-size" }));
}
