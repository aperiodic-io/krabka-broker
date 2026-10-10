//! The Kerberos TOML shapes: `[gssapi]` and `[inter_broker_credentials]`.
//!
//! [`FileGssapiConfig`] is the broker-global accept path for SASL/GSSAPI, and
//! [`FileInterBrokerCredentials`] is the credential this broker presents when
//! it dials a peer. Both name a keytab and a Kerberos service name, so they
//! share the protocol default for `sasl.kerberos.service.name`.

use krabka_units::Time;
use schemars::JsonSchema;
use serde::Deserialize;

/// Kafka protocol default for `sasl.kerberos.service.name`.
pub(super) const DEFAULT_KERBEROS_SERVICE_NAME: &str = "kafka";

/// Kafka's default `sasl.kerberos.principal.to.local.rules`, which is
/// `KerberosShortNamer.fromUnparsedRules`'s answer to no rules at all.
fn default_principal_to_local_rules() -> Vec<String> {
    vec!["DEFAULT".to_owned()]
}

/// TOML shape of `[gssapi]`. Maps to
/// [`crate::network::auth::GssapiConfig`]. `principal_to_local_rules`
/// are parsed into `KerberosRule` at `apply_to` time.
#[krabka_macros::human_units]
#[krabka_macros::config_table(strict)]
pub struct FileGssapiConfig {
    /// Keytab that holds this broker's Kerberos service key. The SASL/GSSAPI
    /// accept path reads it to answer a client's ticket.
    pub keytab_path: std::path::PathBuf,
    /// `sasl.kerberos.service.name`. Defaults to `"kafka"` when omitted.
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order (first match wins). Kafka's
    /// default for `sasl.kerberos.principal.to.local.rules` is `["DEFAULT"]`,
    /// and an empty list maps no principal.
    #[serde(default = "default_principal_to_local_rules")]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm, the only realm the `DEFAULT` rule accepts.
    /// Kafka reads it from `krb5.conf`; when it is omitted `DEFAULT` matches
    /// no principal and the rules have to name the realm.
    pub realm: Option<String>,
    /// KDC endpoint (e.g. `tcp://kdc:88`) that bypasses krb5.conf discovery;
    /// falls back to krb5.conf when omitted.
    pub kdc: Option<String>,
    /// Maximum tolerated difference between client and broker clocks.
    pub max_time_skew: Option<Time>,
}

/// TOML shape of `[inter_broker_credentials]`. A `type` discriminator
/// selects the variant. PLAIN/SCRAM inter-broker over TOML remain
/// intentionally unexposed.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FileInterBrokerCredentials {
    Gssapi {
        /// Keytab that holds the key for `client_principal`.
        keytab_path: std::path::PathBuf,
        /// Kerberos principal this broker authenticates as when it dials a
        /// peer.
        client_principal: String,
        /// The peer's `sasl.kerberos.service.name`. Defaults to `"kafka"`
        /// when omitted.
        service_name: Option<String>,
        /// KDC endpoint (for example `tcp://kdc:88`) used to obtain the
        /// service ticket, bypassing krb5.conf discovery.
        kdc_url: String,
    },
    #[serde(rename = "oauth-bearer")]
    OAuthBearer {
        /// File containing the bearer token. A trailing newline is ignored.
        /// The token itself never appears in the parsed config's `Debug` form.
        token_path: std::path::PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::secs;

    use crate::file_config::{FileConfig, FileConfigError};

    #[test]
    fn apply_to_gssapi_maps_all_fields() {
        let src = r#"
broker_id = 1
[gssapi]
keytab_path = "/etc/krabka/gssapi-keytab/keytab"
service_name = "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"
kdc = "tcp://kdc:88"
max_time_skew = "17s"
"#;
        let cfg = crate::file_config::test_support::configured(src, "parse [gssapi]")
            .expect("apply [gssapi]");
        let g = cfg.gssapi.expect("gssapi config present");
        check!(g.keytab_path == std::path::PathBuf::from("/etc/krabka/gssapi-keytab/keytab"));
        check!(g.service_name.as_str() == "kafka");
        check!(g.principal_to_local_rules.len() == 2);
        // Second rule in the fixture is the bare DEFAULT rule.
        check!(matches!(
            g.principal_to_local_rules[1],
            crate::network::auth::KerberosRule::Default
        ));
        check!(g.realm.as_deref() == Some("EXAMPLE.COM"));
        check!(g.kdc.as_deref() == Some("tcp://kdc:88"));
        check!(g.max_time_skew == secs(17));
    }

    #[test]
    fn apply_to_gssapi_defaults_service_name_to_kafka() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["DEFAULT"]
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(src).unwrap();
        let gssapi = cfg.gssapi.unwrap();
        assert!(gssapi.service_name == "kafka");
        assert!(gssapi.max_time_skew == krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW);
    }

    /// Kafka's `sasl.kerberos.principal.to.local.rules` defaults to
    /// `["DEFAULT"]`, so a `[gssapi]` section with no rules maps a principal in
    /// the default realm to its first component. An explicit empty list is
    /// Kafka's empty rule list, which maps nothing.
    #[test]
    fn apply_to_gssapi_defaults_the_rules_to_default() {
        let realm = "realm = \"EXAMPLE.COM\"\n";
        // (rules line, kinds of the rules that result)
        let cases = [
            ("", vec!["DEFAULT"]),
            ("principal_to_local_rules = []\n", vec![]),
            (
                "principal_to_local_rules = [\"RULE:[1:$1](.*)s/x/y/\"]\n",
                vec!["RULE"],
            ),
        ];
        for (rules, expected) in cases {
            let src = format!("[gssapi]\nkeytab_path = \"/k/keytab\"\n{realm}{rules}");
            let cfg = crate::file_config::test_support::configured_unwrap_parse(&src).unwrap();
            let kinds: Vec<&str> = cfg
                .gssapi
                .unwrap()
                .principal_to_local_rules
                .iter()
                .map(|rule| match rule {
                    crate::network::auth::KerberosRule::Default => "DEFAULT",
                    crate::network::auth::KerberosRule::Translate(_) => "RULE",
                })
                .collect();
            check!(kinds == expected, "{rules:?}");
        }
    }

    #[test]
    fn apply_to_gssapi_accepts_zero_clock_skew() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
max_time_skew = "0s"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(src).unwrap();
        assert!(cfg.gssapi.unwrap().max_time_skew == secs(0));
    }

    #[test]
    fn apply_to_gssapi_rejects_negative_clock_skew() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
max_time_skew = "-1s"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        assert!(file.apply_to(&mut cfg).is_err());
    }

    #[test]
    fn apply_to_gssapi_rejects_malformed_rule() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["NOT_A_RULE:::"]
"#;
        let err = crate::file_config::test_support::configured_unwrap_parse(src).unwrap_err();
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }
    #[test]
    fn apply_to_inter_broker_credentials_gssapi() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/krabka/gssapi-keytab/keytab"
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(src).unwrap();
        let expected = crate::config::InterBrokerCredentials::Gssapi {
            keytab_path: std::path::PathBuf::from("/etc/krabka/gssapi-keytab/keytab"),
            client_principal: "kafka@EXAMPLE.COM".to_string(),
            service_name: "kafka".to_string(),
            kdc_url: "tcp://kdc:88".to_string(),
        };
        assert!(cfg.inter_broker_credentials == Some(expected));
    }

    #[test]
    fn apply_to_inter_broker_credentials_rejects_unknown_type() {
        // Unknown `type` variants are rejected at TOML parse time because
        // `FileInterBrokerCredentials` is a tagged enum with `deny_unknown_fields`.
        let src = r#"
[inter_broker_credentials]
type = "carrier-pigeon"
"#;
        assert!(toml::from_str::<FileConfig>(src).is_err());
    }

    #[test]
    fn apply_to_inter_broker_credentials_defaults_service_name_to_kafka() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/k/keytab"
client_principal = "kafka@EXAMPLE.COM"
kdc_url = "tcp://kdc:88"
"#;
        let cfg = crate::file_config::test_support::configured_unwrap_parse(src).unwrap();
        match cfg.inter_broker_credentials.unwrap() {
            crate::config::InterBrokerCredentials::Gssapi { service_name, .. } => {
                assert!(service_name == "kafka");
            }
            other => panic!("expected Gssapi, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_inter_broker_credentials_oauthbearer_reads_redacted_token_file() {
        let (_dir, token_path) = crate::test_support::oauth_token_file(
            crate::test_support::OAuthTokenContents::Nonempty,
        );
        let src = format!(
            r#"
[inter_broker_credentials]
type = "oauth-bearer"
token_path = {}
"#,
            toml::Value::String(token_path.to_string_lossy().into_owned())
        );
        let cfg = crate::file_config::test_support::configured_unwrap_parse(&src).unwrap();

        let Some(crate::config::InterBrokerCredentials::OAuthBearer {
            token_path: actual_path,
        }) = cfg.inter_broker_credentials
        else {
            panic!("expected OAuthBearer credentials");
        };
        assert!(actual_path == token_path);
        assert!(!format!("{actual_path:?}").contains("header.payload"));
    }

    #[test]
    fn apply_to_inter_broker_credentials_oauthbearer_rejects_empty_token_file() {
        let (_dir, token_path) =
            crate::test_support::oauth_token_file(crate::test_support::OAuthTokenContents::Empty);
        let src = format!(
            r#"
[inter_broker_credentials]
type = "oauth-bearer"
token_path = {}
"#,
            toml::Value::String(token_path.to_string_lossy().into_owned())
        );
        let file: FileConfig = toml::from_str(&src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("empty bearer token is rejected");
        assert!(error.to_string().contains("token must be non-empty"));
    }
}
