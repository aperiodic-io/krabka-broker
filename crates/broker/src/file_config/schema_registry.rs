//! The `[schema_registry]` TOML shape and its cache defaults.
//!
//! [`FileSchemaRegistryConfig`] mirrors the constructor arguments of
//! [`crate::schema_validation::SchemaValidator::new`], the single
//! Confluent-compatible registry client each broker holds for KFC-7
//! broker-side schema validation.

use {schemars::JsonSchema, serde::Deserialize, std::path::PathBuf};

/// TOML shape of `[schema_registry]`. Mirrors the constructor arguments of
/// [`crate::schema_validation::SchemaValidator::new`], the one registry client
/// each broker holds.
///
/// `deny_unknown_fields` so a misspelled key is rejected at parse time. A
/// silently ignored `fail_open` would leave the operator with the opposite of
/// the policy they wrote.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileSchemaRegistryConfig {
    /// Base URL of the Confluent-compatible schema registry, e.g.
    /// `http://schema-registry:8081`. The registry API path is appended to it.
    pub url: String,
    /// HTTP Basic username for an authenticated registry.
    ///
    /// Set this together with `basic_auth_password_path`.
    pub basic_auth_username: Option<String>,
    /// File containing the HTTP Basic password. One trailing newline is ignored.
    ///
    /// Set this together with `basic_auth_username`; keeping the password in a
    /// separate mounted secret avoids putting it in broker arguments or TOML.
    pub basic_auth_password_path: Option<PathBuf>,
    /// **Security-sensitive.** Admit a record that the broker could not
    /// validate because the registry was unreachable. When `true`, a validated
    /// topic accepts whatever it is sent for the length of a registry outage.
    /// That is fail-open. Default `false`, which fails the produce instead.
    ///
    /// An unknown schema id, or a body that does not match its schema, is a
    /// rejection under either setting. This field governs only the case where
    /// the broker could not get an answer at all.
    #[serde(default)]
    pub fail_open: bool,
    /// Schema-cache capacity, in entries. Default `50_000`.
    #[serde(default = "default_schema_registry_maximum_cache_size")]
    pub maximum_cache_size: usize,
    /// Schema-cache entry TTL, in milliseconds. Default `300_000`, which is
    /// 5 minutes.
    #[serde(default = "default_schema_registry_expire_after_ms")]
    pub expire_after_ms: i64,
}

/// Default schema-cache capacity, in entries. The same as the OPA decision
/// cache: both hold one small entry for each distinct key a producer sends.
const DEFAULT_SCHEMA_REGISTRY_MAXIMUM_CACHE_SIZE: usize = 50_000;

/// Default schema-cache TTL: 5 minutes, in milliseconds.
///
/// The OPA decision cache uses an hour. This TTL is much shorter because a
/// newly registered schema has to become usable without an operator restart of
/// a broker. A producer that registers a schema and then produces with it at
/// once is the ordinary case. A negative cache entry for that id holds until
/// the TTL expires.
const DEFAULT_SCHEMA_REGISTRY_EXPIRE_AFTER_MS: i64 = 5 * 60 * 1_000;

fn default_schema_registry_maximum_cache_size() -> usize {
    DEFAULT_SCHEMA_REGISTRY_MAXIMUM_CACHE_SIZE
}

fn default_schema_registry_expire_after_ms() -> i64 { DEFAULT_SCHEMA_REGISTRY_EXPIRE_AFTER_MS }

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::BrokerConfig,
            file_config::{
                FileConfig, FileConfigError,
                test_support::{configured, configured_unwrap_parse},
            },
        },
        assert2::assert,
        krabka_units::millis,
    };

    #[test]
    fn schema_registry_section_round_trips_every_key() {
        let toml = r#"
[schema_registry]
url = "http://schema-registry:8081"
fail_open = true
maximum_cache_size = 128
expire_after_ms = 60000
"#;
        let file: FileConfig = toml::from_str(toml).expect("parse schema_registry section");

        let expected = FileSchemaRegistryConfig {
            url: "http://schema-registry:8081".to_owned(),
            basic_auth_username: None,
            basic_auth_password_path: None,
            fail_open: true,
            maximum_cache_size: 128,
            expire_after_ms: 60_000,
        };
        assert!(file.schema_registry == Some(expected));
    }

    #[test]
    fn schema_registry_defaults_are_fail_closed_with_a_five_minute_ttl() {
        // `url` is the one required key. The other three carry the documented
        // defaults, and `fail_open` must default to fail-closed.
        let toml = r#"
[schema_registry]
url = "http://schema-registry:8081"
"#;
        let file: FileConfig = toml::from_str(toml).expect("parse schema_registry section");

        let expected = FileSchemaRegistryConfig {
            url: "http://schema-registry:8081".to_owned(),
            basic_auth_username: None,
            basic_auth_password_path: None,
            fail_open: false,
            maximum_cache_size: 50_000,
            expire_after_ms: 300_000,
        };
        assert!(file.schema_registry == Some(expected));
    }

    #[test]
    fn schema_registry_section_rejects_a_misspelled_key() {
        // `deny_unknown_fields`: a silently ignored `fail_open` typo would
        // leave the broker on the opposite policy to the one the operator
        // wrote, so the parse must fail instead.
        let toml = r#"
[schema_registry]
url = "http://schema-registry:8081"
failopen = true
"#;
        assert!(toml::from_str::<FileConfig>(toml).is_err());
    }

    #[test]
    fn schema_registry_section_builds_the_validator() {
        // `schema-registry.invalid` deliberately does not resolve. No HTTP
        // call is made here; the constructor only builds the client.
        let toml = r#"
[schema_registry]
url = "http://schema-registry.invalid:8081"
"#;
        let cfg = configured(toml, "parse schema_registry section")
            .expect("apply schema_registry section");

        assert!(cfg.schema_validator.is_some());
    }

    #[test]
    fn schema_registry_basic_auth_requires_both_fields() {
        let file: FileConfig = toml::from_str(
            r#"
[schema_registry]
url = "http://schema-registry.invalid:8081"
basic_auth_username = "broker"
"#,
        )
        .unwrap();
        let mut cfg = BrokerConfig::default();

        let error = file.apply_to(&mut cfg).unwrap_err();

        assert!(error.to_string().contains("basic_auth_password_path"));
    }

    #[test]
    fn schema_registry_basic_auth_reads_password_file() {
        let dir = tempfile::tempdir().unwrap();
        let password_path = dir.path().join("registry-password");
        std::fs::write(&password_path, "secret\n").unwrap();
        let cfg = configured_unwrap_parse(&format!(
            r#"
[schema_registry]
url = "http://schema-registry.invalid:8081"
basic_auth_username = "broker"
basic_auth_password_path = "{}"
"#,
            password_path.display()
        ))
        .unwrap();

        assert!(cfg.schema_validator.is_some());
    }

    #[test]
    fn schema_registry_section_absent_leaves_no_validator() {
        let cfg = configured("", "parse empty config").expect("apply empty config");

        assert!(cfg.schema_validator.is_none());
    }

    #[test]
    fn schema_registry_zero_cache_size_is_a_config_error() {
        // A zero-capacity LRU makes every record a cache miss, so
        // `SchemaValidator::new` rejects it. The rejection must arrive as a
        // `FileConfigError`, not as a panic out of `NonZeroUsize`.
        let toml = r#"
[schema_registry]
url = "http://schema-registry.invalid:8081"
maximum_cache_size = 0
"#;
        let file: FileConfig = toml::from_str(toml).expect("parse schema_registry section");
        let mut cfg = BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("zero maximum_cache_size must be rejected");

        assert!(matches!(error, FileConfigError::SchemaRegistryConfig(_)));
        assert!(cfg.schema_validator.is_none());
    }

    #[test]
    fn runtime_schema_registry_http_timeout_applies() {
        let cfg = configured(
            r#"
[runtime]
schema_registry_http_timeout = "2500ms"
"#,
            "parse runtime config",
        )
        .expect("apply runtime config");

        assert!(cfg.schema_registry_http_timeout == millis(2_500));
    }
}
