//! Building the SASL/OAUTHBEARER validator from `[oauthbearer]`.
//!
//! `apply_oauthbearer` picks one of the three validators — signed JWS against
//! a JWKS endpoint, RFC 7662 introspection, or the unsecured development
//! validator — from which endpoint URI the table sets, compiles the `JsonPath`
//! expressions once, and parks the JWKS refresher's shared state on the broker
//! configuration.

use krabka_units::{Time, convert::TimeExt as _};

#[cfg(not(target_family = "wasm"))]
use super::oauthbearer::{DEFAULT_ALLOWABLE_CLOCK_SKEW, DEFAULT_INTROSPECTION_HTTP_TIMEOUT};
use super::{FileConfigError, oauthbearer::FileOAuthBearerConfig};
use crate::config::{BrokerConfig, DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE};
#[cfg(not(target_family = "wasm"))]
use crate::oauth_introspection::ReqwestIntrospectionClient;

#[cfg(not(target_family = "wasm"))]
fn configure_introspection_validator(
    oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    cfg: &mut BrokerConfig,
) {
    let client_id = oauth.introspection_client_id.clone().unwrap_or_else(|| {
        panic!("[oauthbearer]: introspection endpoint requires introspection_client_id")
    });
    let secret_path = oauth
        .introspection_client_secret_path
        .clone()
        .unwrap_or_else(|| {
            panic!(
                "[oauthbearer]: introspection endpoint requires introspection_client_secret_path"
            )
        });
    let client_secret = std::fs::read_to_string(&secret_path)
        .unwrap_or_else(|error| {
            panic!(
                "[oauthbearer]: failed to read client secret {}: {error}",
                secret_path.display()
            )
        })
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let timeout = oauth
        .introspection_http_timeout_ms
        .map_or(DEFAULT_INTROSPECTION_HTTP_TIMEOUT, |ms| {
            Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
        });
    let client = ReqwestIntrospectionClient::build(
        endpoint.to_owned(),
        oauth.userinfo_endpoint_uri.clone(),
        client_id,
        client_secret,
        oauth.idp_tls_trust.as_deref(),
        timeout,
    )
    .unwrap_or_else(|error| panic!("failed to build OAuth introspection client: {error}"));
    cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Introspection(
        krabka_security::IntrospectionValidator {
            client,
            principal_claim_name: oauth
                .principal_claim_name
                .clone()
                .unwrap_or_else(|| "sub".into()),
            custom_claim_check,
            call_userinfo: oauth.userinfo_endpoint_uri.is_some(),
            allowable_clock_skew: oauth
                .allowable_clock_skew_ms
                .map_or(DEFAULT_ALLOWABLE_CLOCK_SKEW, Time::from_millis),
            expected_audience: oauth.expected_audience.clone(),
            fallback_user_name_claim: oauth.fallback_user_name_claim.clone(),
            fallback_user_name_prefix: oauth.fallback_user_name_prefix.clone(),
            groups_claim,
            groups_claim_delimiter: oauth.groups_claim_delimiter.clone(),
        },
    );
}

/// RFC 7662 introspection needs an HTTP client stack, which wasm32-wasip1 does
/// not have.
#[cfg(target_family = "wasm")]
fn configure_introspection_validator(
    _oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    _custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    _groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    _cfg: &mut BrokerConfig,
) {
    panic!(
        "[oauthbearer]: introspection_endpoint_uri {endpoint:?} is set, but token introspection is unavailable on this platform"
    );
}

/// Whether any resolved listener offers SASL/OAUTHBEARER. A listener's own
/// `sasl_mechanisms` wins over the broker-wide list, matching
/// [`crate::config::BrokerConfig::validate`].
fn oauthbearer_listener_enabled(cfg: &BrokerConfig) -> bool {
    cfg.listeners.iter().any(|listener| {
        listener
            .sasl_mechanisms
            .as_ref()
            .unwrap_or(&cfg.enabled_sasl_mechanisms)
            .contains(&krabka_security::SaslMechanism::OAuthBearer)
    })
}

/// Build the SASL/OAUTHBEARER validator named by `[oauthbearer]`.
///
/// # Errors
///
/// Returns [`FileConfigError::InvalidConfig`] when the table selects no
/// endpoint, or is absent entirely — either of which would silently fall back
/// to the unsecured-JWS (`alg:none`) validator — while a listener enables
/// OAUTHBEARER and `allow_unsecured` is not `true`.
pub(super) fn apply_oauthbearer(
    oauth: Option<FileOAuthBearerConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    // An absent `[oauthbearer]` table is an endpoint-less one: it must still
    // reach the `(None, None)` arm below, or a listener enabling OAUTHBEARER
    // would keep the unsecured (`alg:none`) validator without the opt-in.
    let oauth = oauth.unwrap_or_default();
    // Thread the IdP trust-store path
    // unconditionally. Inert when no HTTPS-bound endpoint is set,
    // and harmlessly carried for the unsecured validator.
    cfg.oauthbearer_idp_tls_trust
        .clone_from(&oauth.idp_tls_trust);
    // Compile the JsonPath expression once at load time;
    // a malformed expression panics with a descriptive error.
    let custom_claim_check_compiled = oauth.custom_claim_check.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid custom_claim_check JsonPath expression {expr:?}: {e}")
        })
    });

    // Compile groups_claim JsonPath at load time.
    let groups_claim_compiled = oauth.groups_claim.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}")
        })
    });

    match (
        oauth.jwks_endpoint_uri.as_ref(),
        oauth.introspection_endpoint_uri.as_ref(),
    ) {
        (Some(_), Some(_)) => {
            panic!(
                "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
            );
        }
        (Some(_), None) => {
            // Signed-JWT validation. The empty key handle is
            // populated by the refresher `Broker::start` spawns.
            let jwks_uri = oauth.jwks_endpoint_uri.clone().unwrap();

            // Create the signal channel + the shared
            // timestamps here so the validator's `JwksHandle` and
            // the refresher (constructed in `Broker::start`) point at
            // the same Arc-shared state. Channel capacity 1 +
            // `try_send` on the producer ⇒ signals coalesce.
            let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(1);
            let last_successful = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
            let cache_generation = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let last_on_demand = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

            let handle = krabka_security::JwksHandle::new_with_refresher_handles(
                krabka_security::Jwks::empty(),
                last_successful.clone(),
                signal_tx,
            );

            let mut v = krabka_security::SignedJwsValidator::new(handle);
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            v.valid_issuer = oauth.valid_issuer_uri;
            v.expected_audience = oauth.expected_audience;
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check
                .clone_from(&custom_claim_check_compiled);
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim
                .clone_from(&oauth.fallback_user_name_claim);
            v.fallback_user_name_prefix
                .clone_from(&oauth.fallback_user_name_prefix);
            v.groups_claim.clone_from(&groups_claim_compiled);
            v.groups_claim_delimiter
                .clone_from(&oauth.groups_claim_delimiter);
            // Hard cache-expiry threshold.
            v.cache_expiry = oauth
                .jwks_expiry_seconds
                .map(|s| Time::from_secs(i64::from(s)));
            cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Signed(v);
            cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
            if let Some(ms) = oauth.jwks_refresh_interval_ms {
                cfg.oauthbearer_jwks_refresh_interval =
                    Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX));
            }

            // Park signal_rx + shared state for Broker::start.
            *cfg.oauthbearer_jwks_signal_rx.lock().unwrap() = Some(signal_rx);
            cfg.oauthbearer_jwks_last_successful_fetch_ms = last_successful;
            cfg.oauthbearer_jwks_cache_generation = cache_generation;
            cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
            cfg.oauthbearer_jwks_min_on_demand_pause = oauth
                .jwks_min_refresh_pause_seconds
                .map_or(DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, |s| {
                    Time::from_secs(i64::from(s))
                });
            cfg.features.oauthbearer_jwks_ignore_key_use =
                oauth.jwks_ignore_key_use.unwrap_or(false);
        }
        (None, Some(introspect_uri)) => {
            configure_introspection_validator(
                &oauth,
                introspect_uri,
                custom_claim_check_compiled.clone(),
                groups_claim_compiled.clone(),
                cfg,
            );
        }
        (None, None) => {
            // Unsecured-JWS validation (development only), behind an
            // explicit opt-in so a misspelled endpoint key cannot quietly
            // downgrade a listener to trust-the-client.
            if oauth.allow_unsecured.unwrap_or(false) {
                tracing::warn!(concat!(
                    "[oauthbearer]: allow_unsecured = true - SASL/OAUTHBEARER tokens ",
                    "are accepted without signature verification (alg:none). ",
                    "Development only."
                ));
            } else if oauthbearer_listener_enabled(cfg) {
                return Err(FileConfigError::InvalidConfig(
                    concat!(
                        "[oauthbearer]: a listener enables OAUTHBEARER but neither ",
                        "jwks_endpoint_uri nor introspection_endpoint_uri is set; that ",
                        "falls back to the unsecured-JWS (alg:none) validator. Configure ",
                        "an endpoint, or set allow_unsecured = true for development."
                    )
                    .to_owned(),
                ));
            }
            let mut v = krabka_security::UnsecuredJwsValidator::default();
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check = custom_claim_check_compiled;
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim = oauth.fallback_user_name_claim;
            v.fallback_user_name_prefix = oauth.fallback_user_name_prefix;
            v.groups_claim = groups_claim_compiled;
            v.groups_claim_delimiter = oauth.groups_claim_delimiter;
            cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Unsecured(v);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{minutes, secs};

    use crate::{
        config::BrokerConfig,
        file_config::{
            FileConfig, FileConfigError,
            test_support::{configured, configured_unwrap_parse},
        },
    };

    fn check_unsecured(cfg: &BrokerConfig) {
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        assert!(matches!(
            cfg.oauthbearer_validator,
            krabka_security::OAuthBearerValidator::Unsecured(_)
        ));
    }

    fn client_secret(value: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client-secret");
        std::fs::write(&path, value).unwrap();
        (dir, path)
    }

    #[test]
    fn apply_to_oauthbearer_jwks_selects_signed_validator() {
        let src = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
valid_issuer_uri = "https://idp.example"
expected_audience = "kafka"
principal_claim_name = "client_id"
jwks_refresh_interval_ms = 60000
jwks_expiry_seconds = 360
"#;
        let cfg = configured(src, "parse").unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.as_deref() == Some("https://idp.example/jwks"));
        assert!(cfg.oauthbearer_jwks_refresh_interval == minutes(1));
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Signed(v) => {
                check!(v.valid_issuer.as_deref() == Some("https://idp.example"));
                check!(v.expected_audience.as_deref() == Some("kafka"));
                check!(v.principal_claim_name.as_str() == "client_id");
                check!(v.cache_expiry == Some(secs(360)));
            }
            other => panic!("jwks_endpoint_uri must select the Signed validator; got {other:?}"),
        }
    }

    /// A listener offering OAUTHBEARER plus an endpoint-less `[oauthbearer]`
    /// table — the shape a misspelled `jwks_endpoint_uri` key produces.
    fn oauthbearer_listener_toml(extra: &str) -> String {
        format!(
            r#"
[[listeners]]
name = "SASL"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "SaslPlaintext"
sasl_config = {{ enabled_mechanisms = ["OAUTHBEARER"] }}

[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
{extra}
"#
        )
    }

    #[test]
    fn apply_to_oauthbearer_without_endpoint_rejects_unsecured_fallback() {
        let file: FileConfig = toml::from_str(&oauthbearer_listener_toml("")).expect("parse");
        let mut cfg = BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("unsecured fallback must be rejected without allow_unsecured");
        let FileConfigError::InvalidConfig(message) = &error else {
            panic!("expected InvalidConfig; got {error:?}")
        };
        assert!(message.contains("allow_unsecured"));
    }

    #[test]
    fn apply_to_oauthbearer_allow_unsecured_opts_into_unsecured_validator() {
        let cfg = configured(
            &oauthbearer_listener_toml("allow_unsecured = true"),
            "parse",
        )
        .unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert!(v.allowable_clock_skew == secs(5));
            }
            other => {
                panic!("allow_unsecured must keep the unsecured validator; got {other:?}")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_endpoint_stays_unsecured_when_no_listener_offers_it() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let cfg = configured(src, "parse").unwrap();
        check_unsecured(&cfg);
    }

    #[test]
    fn apply_to_missing_oauthbearer_table_rejects_unsecured_fallback() {
        let src = r#"
[[listeners]]
name = "SASL"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "SaslPlaintext"
sasl_config = { enabled_mechanisms = ["OAUTHBEARER"] }
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("a missing [oauthbearer] table must not silently stay unsecured");
        let FileConfigError::InvalidConfig(message) = &error else {
            panic!("expected InvalidConfig; got {error:?}")
        };
        assert!(message.contains("allow_unsecured"));
    }

    #[test]
    fn apply_to_missing_oauthbearer_table_applies_when_no_listener_offers_it() {
        let src = r#"
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "Plaintext"
"#;
        let cfg = configured(src, "parse").unwrap();
        check_unsecured(&cfg);
    }

    #[test]
    fn apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
idp_tls_trust = "/etc/krabka/oauth/idp-ca.pem"
"#;
        let cfg = configured_unwrap_parse(toml).unwrap();
        assert!(
            cfg.oauthbearer_idp_tls_trust.as_deref()
                == Some(std::path::Path::new("/etc/krabka/oauth/idp-ca.pem"))
        );
    }

    #[test]
    fn apply_to_oauthbearer_without_idp_tls_trust_leaves_field_none() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
"#;
        let cfg = configured_unwrap_parse(toml).unwrap();
        assert!(cfg.oauthbearer_idp_tls_trust.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
        let (_dir, secret_path) = client_secret("the-secret");
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let cfg = configured_unwrap_parse(&toml).unwrap();
        assert!(matches!(
            cfg.oauthbearer_validator,
            krabka_security::OAuthBearerValidator::Introspection(_)
        ));
    }

    #[test]
    #[should_panic(expected = "mutually exclusive")]
    fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
        let (_dir, secret_path) = client_secret("x");
        let toml = format!(
            r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let _cfg = configured_unwrap_parse(&toml).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_id")]
    fn apply_to_oauthbearer_introspection_requires_client_id() {
        let (_dir, secret_path) = client_secret("x");
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let _cfg = configured_unwrap_parse(&toml).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_secret_path")]
    fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
        let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
        let _cfg = configured_unwrap_parse(toml).unwrap();
    }

    #[test]
    fn introspection_calls_userinfo_only_when_an_endpoint_is_configured() {
        for (userinfo, expected) in [
            (
                "userinfo_endpoint_uri = \"https://idp.example/userinfo\"\n",
                true,
            ),
            ("", false),
        ] {
            let (_dir, secret_path) = client_secret("x");
            let toml = format!(
                r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
{userinfo}introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
                secret_path.display()
            );
            let cfg = configured_unwrap_parse(&toml).unwrap();
            match cfg.oauthbearer_validator {
                krabka_security::OAuthBearerValidator::Introspection(v) => assert!(
                    v.call_userinfo == expected,
                    "userinfo endpoint: {userinfo:?}"
                ),
                other => panic!("expected Introspection, got {other:?}"),
            }
        }
    }
}
