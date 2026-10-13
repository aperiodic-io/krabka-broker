//! The example configs under `docs/examples/` parse and apply.
//!
//! Each file is read twice. As written, it goes through `toml` into
//! `FileConfig` and then through `FileConfig::apply_to` onto a default
//! `BrokerConfig`, which is the path the binary takes under `--config-file`.
//! Then every disabled setting, a line that starts with `#` and no space, is
//! turned on and the result is parsed again, so a key that is commented out
//! because it needs a key file or a service still has to be a key the broker
//! knows. Apply is skipped on that second pass because those settings read
//! files at apply time.
//!
//! `FileConfig` tolerates unknown keys, so a key an example spells wrong is
//! still a file that parses. The last test therefore walks each example as
//! plain TOML and checks every key, at every depth, against
//! `docs/config-schema.json`, which is the schema `FileConfig` generates.

use {
    assert2::{assert, check},
    krabka_broker::{config::BrokerConfig, file_config::FileConfig},
    serde_json::Value as Json,
    toml::Value as Toml,
};

const EXAMPLES: [&str; 2] = [
    "docs/examples/broker-single-node.toml",
    "docs/examples/broker-three-node-quorum.toml",
];

#[path = "support/repository.rs"]
mod repository;

use repository::read_text as read_example;

/// Turn every disabled setting on. Prose comments start with `# ` and stay.
fn enable_disabled_settings(text: &str) -> String {
    text.lines()
        .map(|line| match line.strip_prefix('#') {
            Some(rest) if !rest.is_empty() && !rest.starts_with([' ', '#']) => rest,
            _ => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse(relative: &str, text: &str) -> FileConfig {
    toml::from_str(text).unwrap_or_else(|error| panic!("{relative} does not parse: {error}"))
}

// `OpaAuthorizer::new` captures the current tokio runtime handle, so the apply
// step runs inside one, as it does in the binary.
#[tokio::test]
async fn examples_parse_and_apply_as_written() {
    for relative in EXAMPLES {
        let file = parse(relative, &read_example(relative));
        let mut config = BrokerConfig::default();
        file.apply_to(&mut config)
            .unwrap_or_else(|error| panic!("{relative} does not apply: {error}"));
        check!(
            !config.listeners.is_empty(),
            "{relative} should configure at least one listener"
        );
    }
}

#[test]
fn examples_parse_with_every_disabled_setting_enabled() {
    for relative in EXAMPLES {
        let text = read_example(relative);
        let enabled = enable_disabled_settings(&text);
        assert!(
            enabled != text,
            "{relative} should carry at least one disabled setting"
        );
        let file = parse(relative, &enabled);
        check!(
            !file.operator_keys.is_empty(),
            "{relative} should show an [[operator_keys]] entry"
        );
    }
}

#[test]
fn single_node_example_covers_the_local_surface() {
    let file = parse(EXAMPLES[0], &read_example(EXAMPLES[0]));
    check!(file.broker_id == Some(1));
    check!(file.controller_quorum_voters.is_empty());
    check!(file.listeners.len() == 2);
    check!(file.listeners[1].tls_config.is_some());
    check!(file.listeners[1].sasl_config.is_some());
    check!(
        file.remote_storage
            .as_ref()
            .and_then(|storage| storage.storage_dir.as_deref())
            == Some("/var/lib/krabka/tiered")
    );
    check!(file.audit.is_some());
    check!(file.freeze.is_some());
    check!(file.break_glass.is_some());
    check!(file.delegation_token.is_some());
}

#[test]
fn three_node_example_covers_the_cluster_surface() {
    let file = parse(EXAMPLES[1], &read_example(EXAMPLES[1]));
    check!(file.controller_quorum_voters.len() == 3);
    check!(file.bootstrap_servers.len() == 3);
    check!(file.tls_config.is_some());
    check!(file.stretch.is_some());
    check!(
        file.inter_broker_principal_node_ids
            .as_ref()
            .is_some_and(|bindings| bindings.len() == 3)
    );
    check!(
        file.remote_storage
            .as_ref()
            .is_some_and(|storage| storage.s3.is_some())
    );
    check!(
        file.authorization
            .as_ref()
            .is_some_and(|authorization| authorization.opa.is_some())
    );
    check!(file.schema_registry.is_some());
    let runtime = file.runtime.as_ref().expect("[runtime] present");
    check!(runtime.diskless_wal_flush_interval.is_some());
    check!(runtime.diskless_wal_local_replica_count.is_some());

    let enabled = parse(
        EXAMPLES[1],
        &enable_disabled_settings(&read_example(EXAMPLES[1])),
    );
    check!(enabled.oauthbearer.is_some());
    check!(enabled.gssapi.is_some());
    check!(enabled.inter_broker_credentials.is_some());
    check!(enabled.operator_keys.len() == 2);
    check!(
        enabled
            .remote_storage
            .as_ref()
            .is_some_and(|storage| storage.worm.is_some())
    );
}

use repository::checked_in_schema;

/// The object schemas a node can stand for, with `$ref`, `anyOf` and `oneOf`
/// resolved. `null` branches are dropped: an absent table is not a key.
fn object_branches<'a>(root: &'a Json, node: &'a Json, out: &mut Vec<&'a Json>) {
    if let Some(reference) = node.get("$ref").and_then(Json::as_str) {
        let name = reference
            .strip_prefix("#/$defs/")
            .expect("schemars emits local $defs refs");
        object_branches(root, &root["$defs"][name], out);
        return;
    }
    let branches = ["anyOf", "oneOf"]
        .into_iter()
        .filter_map(|key| node.get(key).and_then(Json::as_array))
        .flatten();
    let mut nested = false;
    for branch in branches {
        nested = true;
        object_branches(root, branch, out);
    }
    if !nested && node["type"] != "null" {
        out.push(node);
    }
}

/// A property name the schema knows, and the schema that describes it.
struct KnownProperty<'a> {
    name: &'a String,
    schema: &'a Json,
}

/// Check every key of `table` against the schema `node` describes, then
/// descend into nested tables and arrays of tables.
fn check_table_keys(root: &Json, node: &Json, table: &toml::Table, path: &str) {
    let mut branches = Vec::new();
    object_branches(root, node, &mut branches);
    // A map with free-form keys, such as `server_properties`, has an
    // `additionalProperties` schema and no fixed `properties`; its keys are
    // the operator's to choose.
    if branches.iter().any(|branch| {
        branch
            .get("additionalProperties")
            .is_some_and(Json::is_object)
    }) {
        return;
    }
    let known: Vec<KnownProperty<'_>> = branches
        .iter()
        .filter_map(|branch| branch.get("properties").and_then(Json::as_object))
        .flat_map(|properties| properties.iter())
        .map(|(name, schema)| KnownProperty { name, schema })
        .collect();
    assert!(
        !known.is_empty(),
        "{path} is a table in the example but not an object in the schema"
    );
    for (key, value) in table {
        let field = known
            .iter()
            .find(|property| property.name == key)
            .map(|property| property.schema);
        let Some(field) = field else {
            panic!("{path}.{key} is not a key docs/config-schema.json knows");
        };
        let child = format!("{path}.{key}");
        match value {
            Toml::Table(nested) => check_table_keys(root, field, nested, &child),
            Toml::Array(items) => {
                let mut item_schemas = Vec::new();
                object_branches(root, field, &mut item_schemas);
                for item_schema in item_schemas {
                    let Some(items_node) = item_schema.get("items") else {
                        continue;
                    };
                    for (position, item) in items.iter().enumerate() {
                        if let Toml::Table(nested) = item {
                            check_table_keys(
                                root,
                                items_node,
                                nested,
                                &format!("{child}[{position}]"),
                            );
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

#[test]
fn every_example_key_is_in_the_schema() {
    let schema = checked_in_schema();
    for relative in EXAMPLES {
        let enabled = enable_disabled_settings(&read_example(relative));
        let table: toml::Table = toml::from_str(&enabled)
            .unwrap_or_else(|error| panic!("{relative} does not parse as TOML: {error}"));
        assert!(!table.is_empty(), "{relative} sets no keys");
        check_table_keys(&schema, &schema, &table, relative);
    }
}
