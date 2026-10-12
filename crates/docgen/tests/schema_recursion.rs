use assert2::assert;
use krabka_docgen::schema_md::{render_field_table, render_sectioned_field_table};
use serde_json::json;

#[test]
fn recursive_array_and_reference_schemas_render_with_bounded_labels() {
    let cases = [
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        json!({"A": {"type": "array", "items": {"$ref": "#/$defs/B"}}, "B": {"type": "array", "items": {"$ref": "#/$defs/A"}}}),
        json!({"A": {"anyOf": [{"$ref": "#/$defs/A"}, {"type": "null"}]}}),
        json!({"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}}),
    ];
    for definitions in cases {
        let schema = json!({"type": "object", "$defs": definitions, "properties": {"tree": {"$ref": "#/$defs/A"}, "leaf": {"type": "integer"}}});
        for render in [render_field_table, render_sectioned_field_table] {
            let table = render(&schema);
            assert!(table.contains("| `tree` |"));
            assert!(table.contains("| `leaf` | integer |"));
            assert!(table.len() < 1024);
        }
    }
}
