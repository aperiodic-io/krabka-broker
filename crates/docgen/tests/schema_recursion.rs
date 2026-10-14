use {
    krabka_docgen::schema_md::{render_field_table, render_sectioned_field_table},
    serde_json::json,
};

#[yare::parameterized(
    self_array_field = {
        "self-referential array field",
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        render_field_table,
    },
    self_array_sectioned = {
        "self-referential array sectioned",
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        render_sectioned_field_table,
    },
    mutual_array_field = {
        "mutually-referential array fields",
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/B"}}, "B": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        render_field_table,
    },
    mutual_array_sectioned = {
        "mutually-referential array sectioned",
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/B"}}, "B": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        render_sectioned_field_table,
    },
    self_anyof_field = {
        "self-referential anyOf field",
        json!({"A": {"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}}),
        render_field_table,
    },
    self_anyof_sectioned = {
        "self-referential anyOf sectioned",
        json!({"A": {"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}}),
        render_sectioned_field_table,
    },
    mutual_ref_field = {
        "mutually-referential fields",
        json!({"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}),
        render_field_table,
    },
    mutual_ref_sectioned = {
        "mutually-referential sectioned",
        json!({"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}),
        render_sectioned_field_table,
    },
)]
fn recursive_array_and_reference_schemas_render_with_bounded_labels(
    case: &str,
    definitions: serde_json::Value,
    render: fn(&serde_json::Value) -> String,
) {
    let schema = json!({"type": "object", "$defs": definitions, "properties": {"tree": {"$ref": "#/$defs/A"}, "leaf": {"type": "integer"}}});
    let table = render(&schema);
    assert2::assert!(table.contains("| `tree` |"), "{case}");
    assert2::assert!(table.contains("| `leaf` | integer |"), "{case}");
    assert2::assert!(table.len() < 1024, "{case}");
}
