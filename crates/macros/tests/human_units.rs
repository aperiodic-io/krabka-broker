//! What `#[human_units]` gives a config struct: human strings in and out
//! through serde, and a JSON Schema that names each field's unit.

use {
    assert2::assert,
    krabka_units::{
        ByteSize, Time,
        prelude::{TimeExt as _, mebibytes, secs},
    },
    schemars::JsonSchema,
    serde::{Deserialize, Serialize},
    serde_json::json,
};

/// The markers the expansion names. The broker's own copy is
/// `krabka_broker::file_config::schema_units`.
mod file_config {
    pub mod schema_units {
        use {
            schemars::{JsonSchema, Schema, SchemaGenerator, json_schema},
            std::borrow::Cow,
        };

        pub struct Duration;

        impl JsonSchema for Duration {
            fn schema_name() -> Cow<'static, str> { Cow::Borrowed("Duration") }

            fn inline_schema() -> bool { true }

            fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
                json_schema!({ "type": "string", "format": "duration" })
            }
        }

        pub struct ByteSize;

        impl JsonSchema for ByteSize {
            fn schema_name() -> Cow<'static, str> { Cow::Borrowed("ByteSize") }

            fn inline_schema() -> bool { true }

            fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
                json_schema!({ "type": "string", "format": "byte-size" })
            }
        }
    }
}

#[krabka_macros::human_units]
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Config {
    /// How long to wait.
    timeout: Option<krabka_units::Time>,
    segment: Option<ByteSize>,
    #[serde(default)]
    retries: Option<u32>,
    #[serde(
        default,
        with = "krabka_units::serde_units::numeric::option_millis_i64"
    )]
    #[schemars(with = "Option<i64>")]
    backoff: Option<Time>,
}

#[test]
fn human_strings_deserialize_and_serialize_back() {
    let config: Config = serde_json::from_value(json!({
        "timeout": "5s",
        "segment": "1MiB",
        "retries": 3,
        "backoff": 250,
    }))
    .expect("config deserializes");
    let expected = Config {
        timeout: Some(secs(5)),
        segment: Some(mebibytes(1)),
        retries: Some(3),
        backoff: Some(Time::from_millis(250)),
    };
    assert!(config == expected);
    let round_trip: Config =
        serde_json::from_value(serde_json::to_value(&config).expect("config serializes"))
            .expect("serialized config deserializes");
    assert!(round_trip == expected);
}

#[test]
fn absent_fields_default_to_none() {
    let config: Config = serde_json::from_value(json!({})).expect("config deserializes");
    assert!(
        config
            == Config {
                timeout: None,
                segment: None,
                retries: None,
                backoff: None,
            }
    );
}

#[test]
fn schema_names_the_unit_of_each_field() {
    let schema = serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes");
    assert!(
        schema["properties"]
            == json!({
                "timeout": {
                    "default": null,
                    "description": "How long to wait.",
                    "format": "duration",
                    "type": ["string", "null"],
                },
                "segment": {
                    "default": null,
                    "format": "byte-size",
                    "type": ["string", "null"],
                },
                "retries": {
                    "default": null,
                    "format": "uint32",
                    "minimum": 0,
                    "type": ["integer", "null"],
                },
                "backoff": {
                    "default": null,
                    "format": "int64",
                    "type": ["integer", "null"],
                },
            })
    );
}
