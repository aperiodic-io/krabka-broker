//! The listener TOML shapes: `[[listeners]]`, `[tls_config]`, and the
//! per-listener SASL table.
//!
//! [`FileListener`] is the one entry a broker binds, and
//! [`FileListener::into_spec`] converts it into the [`ListenerSpec`] the broker
//! configuration holds. The TLS and SASL tables live here too because both are
//! written either at the top level or inline on a listener.

use {
    crate::{SslPrincipalMapper, config::ListenerSpec, file_config::FileConfigError},
    krabka_security::ListenerProtocol,
    schemars::JsonSchema,
    serde::Deserialize,
    std::net::SocketAddr,
};

#[krabka_macros::config_table(open)]
pub struct FileTlsConfig {
    /// PEM file holding this listener's server certificate chain.
    pub cert_path: std::path::PathBuf,
    /// PEM file holding the private key for `cert_path`.
    pub key_path: std::path::PathBuf,
    /// PEM file of CA(s) this broker trusts when validating a PEER's server
    /// cert as an outbound inter-broker / controller-quorum dialer. The
    /// operator renders the cluster CA here so KIP-595 controller peers can
    /// mutually authenticate over the controller listener. Maps to
    /// [`krabka_security::TlsConfig::trust_roots_path`].
    pub trust_roots_path: Option<std::path::PathBuf>,
    /// PEM file of CA(s) this listener validates client certificates
    /// against. Absent leaves mTLS without a client trust store, so
    /// `client_auth` has to be `Disabled`.
    pub client_ca_path: Option<std::path::PathBuf>,
    /// Whether this listener asks for and requires a client certificate.
    /// See [`FileClientAuthMode`]. Defaults to `Disabled`.
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
    /// KIP-371 `ssl.principal.mapping.rules`: how the Subject DN of an mTLS
    /// peer certificate becomes the principal name ACLs and `super_users` are
    /// written against. Each entry is either `DEFAULT`, which uses the DN
    /// itself, or `RULE:pattern/replacement/[L|U]`, where `pattern` has to
    /// match the whole DN, `replacement` may reference capture groups as `$1`,
    /// and a trailing `L` or `U` lowercases or uppercases the result. The
    /// rules are tried in order and the first match wins. Defaults to
    /// `["DEFAULT"]`, so a listener that says nothing keeps Kafka's behaviour
    /// of using the full DN. A malformed entry is rejected at startup.
    /// Case-insensitive backreferences have a Unicode compatibility edge:
    /// under `(?i)`, non-ASCII case pairs may match here when Java rejects
    /// them (for example `é` and `É`); under `(?iu)`, pairs Java accepts may
    /// fail here (for example `i` and `İ`, or `s` and `ſ`). Ordinary ASCII
    /// DNs are unaffected. Keep both captured and referenced text ASCII, or
    /// avoid case-insensitive backreferences when exact Java behavior matters.
    #[serde(default = "default_principal_mapping_rules")]
    pub principal_mapping_rules: Vec<String>,
}

/// Kafka's `ssl.principal.mapping.rules` default: pass the Subject DN
/// through unchanged.
fn default_principal_mapping_rules() -> Vec<String> { vec!["DEFAULT".to_owned()] }

/// Whether a TLS listener asks for and requires a client certificate,
/// Kafka's `ssl.client.auth`. `Disabled` is the default and never asks;
/// `Optional` asks and accepts a connection that presents none; `Required`
/// rejects a connection that presents none. `Optional` and `Required` both
/// need a `client_ca_path`.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[krabka_macros::config_table(open)]
pub struct FileListenerSaslConfig {
    /// SASL mechanisms this listener accepts, Kafka's
    /// `sasl.enabled.mechanisms`. Each entry is a wire name such as `PLAIN`,
    /// `SCRAM-SHA-256`, `GSSAPI`, or `OAUTHBEARER`; an unknown name is
    /// rejected at parse time.
    #[serde(default, deserialize_with = "deserialize_sasl_mechanisms")]
    #[schemars(with = "Vec<String>")]
    pub enabled_mechanisms: Vec<krabka_security::SaslMechanism>,
}

fn deserialize_sasl_mechanisms<'de, D>(
    deserializer: D,
) -> Result<Vec<krabka_security::SaslMechanism>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let names: Vec<String> = Vec::deserialize(deserializer)?;
    names
        .into_iter()
        .map(|s| {
            krabka_security::SaslMechanism::from_wire(&s)
                .ok_or_else(|| D::Error::custom(format!("unknown SASL mechanism: {s}")))
        })
        .collect()
}

#[krabka_macros::human_units]
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
pub struct FileListener {
    /// Listener name, for example `"PLAINTEXT"` or `"SASL_SSL"`. It is the
    /// name `inter_broker_listener_name` and Kafka's
    /// `listener.security.protocol.map` refer to.
    pub name: String,
    /// Local `host:port` this listener binds.
    #[schemars(with = "String")]
    pub bind_addr: SocketAddr,
    /// `host:port` published to clients in `Metadata` responses, Kafka's
    /// `advertised.listeners` entry for this listener.
    pub advertised: String,
    /// Security protocol this listener speaks: `Plaintext`, `Ssl`,
    /// `SaslPlaintext`, or `SaslSsl`.
    #[schemars(with = "String")]
    pub protocol: ListenerProtocol,
    pub tls_config: Option<FileTlsConfig>,
    pub sasl_config: Option<FileListenerSaslConfig>,
    /// Per-listener `connections.max.idle.ms`. Absent leaves this listener on
    /// the broker-wide `connections_max_idle`. Maps to an entry in
    /// [`crate::BrokerConfig::connections_max_idle_overrides`].
    pub connections_max_idle: Option<krabka_units::Time>,
    /// Per-listener KIP-368 `connections.max.reauth.ms`. Absent leaves this
    /// listener on the broker-wide `connections_max_reauth`. Maps to an entry
    /// in [`crate::BrokerConfig::connections_max_reauth_overrides`].
    pub connections_max_reauth: Option<krabka_units::Time>,
}

impl FileListener {
    /// Converts this entry into the [`ListenerSpec`] the broker holds,
    /// parsing the listener's KIP-371 principal mapping rules on the way.
    ///
    /// # Errors
    ///
    /// [`FileConfigError::InvalidConfig`] when a `principal_mapping_rules`
    /// entry is not a rule.
    pub fn into_spec(self) -> Result<ListenerSpec, FileConfigError> {
        use krabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
        let principal_mapper = match self.tls_config.as_ref() {
            Some(tls) => {
                SslPrincipalMapper::parse(&tls.principal_mapping_rules).map_err(|error| {
                    FileConfigError::InvalidConfig(format!(
                        "invalid ssl principal mapping rule on listener {}: {error}",
                        self.name
                    ))
                })?
            }
            None => SslPrincipalMapper::default(),
        };
        Ok(ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol,
            tls_config: self.tls_config.map(|t| BrokerTlsConfig {
                cert_chain_path: t.cert_path,
                private_key_path: t.key_path,
                trust_roots_path: t.trust_roots_path,
                client_ca_path: t.client_ca_path,
                client_auth: match t.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            }),
            sasl_mechanisms: self.sasl_config.map(|s| s.enabled_mechanisms),
            principal_mapper,
        })
    }
}

#[cfg(test)]
mod listener_auth_tests {
    use {
        super::*,
        crate::file_config::{FileConfig, test_support::configured_unwrap_parse},
        assert2::assert,
    };

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.listeners.len() == 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        let expected = FileTlsConfig {
            cert_path: std::path::PathBuf::from("/tls/broker.crt"),
            key_path: std::path::PathBuf::from("/tls/broker.key"),
            trust_roots_path: None,
            client_ca_path: Some(std::path::PathBuf::from("/tls/clients-ca.crt")),
            client_auth: FileClientAuthMode::Required,
            principal_mapping_rules: vec!["DEFAULT".to_owned()],
        };
        assert!(*data_tls == expected);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Disabled" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert!(sasl.enabled_mechanisms == vec![krabka_security::SaslMechanism::ScramSha512]);
    }

    /// A listener that says nothing about mapping keeps Kafka's `DEFAULT`,
    /// so the Subject DN is still the principal name.
    #[test]
    fn principal_mapping_rules_default_to_the_subject_dn() {
        let toml = r#"
[[listeners]]
name = "mtls"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let tls = cfg.listeners[0].tls_config.as_ref().unwrap();
        assert!(tls.principal_mapping_rules == vec!["DEFAULT".to_owned()]);
        let spec = cfg.listeners[0].clone().into_spec().unwrap();
        assert!(
            spec.principal_mapper.apply("CN=alice,OU=x,O=y").as_deref()
                == Some("CN=alice,OU=x,O=y")
        );
    }

    /// The configured rules reach the listener the accept path reads them
    /// from, so an mTLS peer's DN becomes the short name ACLs are written
    /// against.
    #[test]
    fn apply_to_listener_parses_principal_mapping_rules() {
        let toml = r#"
broker_id = 0
inter_broker_listener_name = "mtls"

[[listeners]]
name = "mtls"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Required", principal_mapping_rules = ["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"] }
"#;
        let cfg = configured_unwrap_parse(toml).expect("apply listener");
        let listener = cfg
            .listeners
            .iter()
            .find(|listener| listener.name == "mtls")
            .expect("mtls listener present");
        assert!(
            listener
                .principal_mapper
                .apply("CN=alice,OU=integration,O=krabka")
                .as_deref()
                == Some("alice")
        );
        assert!(
            listener
                .principal_mapper
                .apply("OU=integration,O=krabka")
                .as_deref()
                == Some("OU=integration,O=krabka")
        );
    }

    /// A rule that is neither `DEFAULT` nor `RULE:pattern/replacement/[L|U]`
    /// fails the config apply rather than silently mapping nothing.
    #[test]
    fn apply_to_listener_rejects_malformed_principal_mapping_rule() {
        let toml = r#"
[[listeners]]
name = "mtls"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", principal_mapping_rules = ["NOT_A_RULE:::"] }
"#;
        let err = configured_unwrap_parse(toml).unwrap_err();
        assert!(matches!(
            err,
            crate::file_config::FileConfigError::InvalidConfig(_)
        ));
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::file_config::FileConfig,
        assert2::{assert, check},
    };

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "SaslSsl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert!(cfg.listeners[0].protocol == ListenerProtocol::SaslSsl);
    }
    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "Plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_config: None,
            connections_max_idle: None,
            connections_max_reauth: None,
        };
        let spec = fl.into_spec().expect("no rules to reject");
        check!(spec.name == "X");
        check!(spec.bind_addr == "0.0.0.0:9094".parse::<SocketAddr>().unwrap());
        check!(spec.advertised == "h:9094");
        check!(spec.protocol == ListenerProtocol::Plaintext);
        check!(spec.tls_config.is_none());
        check!(spec.sasl_mechanisms.is_none());
    }
    #[test]
    fn tls_keys_round_trip() {
        let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/krabka/broker-tls/0.crt"
key_path  = "/etc/krabka/broker-tls/0.key"
client_ca_path = "/etc/krabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
        assert!(cfg.controller_listener_protocol == Some(ListenerProtocol::Ssl));
        let tls = cfg.tls_config.expect("tls_config present");
        assert!(tls.cert_path == std::path::PathBuf::from("/etc/krabka/broker-tls/0.crt"));
        assert!(tls.client_auth == FileClientAuthMode::Required);
    }
    #[test]
    fn tls_keys_absent_round_trips() {
        let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
        assert!(cfg.controller_listener_protocol == None);
        assert!(cfg.tls_config.is_none());
    }
}
