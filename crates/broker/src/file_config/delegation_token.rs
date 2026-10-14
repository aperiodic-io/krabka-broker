//! The `[delegation_token]` TOML shape and how it reaches the broker config.
//!
//! [`FileDelegationTokenConfig`] carries the KIP-48 HMAC master key and the
//! three lifetime knobs. `apply_delegation_tokens` validates each millisecond
//! value and leaves an already-set key — the one the environment supplies —
//! in place.

use {
    super::{FileConfigError, validate::positive_i64},
    crate::config::BrokerConfig,
    krabka_units::{Time, convert::TimeExt as _},
};

/// TOML shape of `[delegation_token]`. Maps to the three `delegation_token_*`
/// fields on [`crate::BrokerConfig`].
#[krabka_macros::config_table(strict)]
pub struct FileDelegationTokenConfig {
    /// HMAC master key. Overridden by `KRABKA_DELEGATION_TOKEN_SECRET_KEY`
    /// when set. Bytes are wrapped in
    /// [`krabka_security::SecretBytes`] before reaching `BrokerConfig`.
    pub secret_key: Option<String>,
    /// Hard upper bound on token lifetime, ms. Default 7 days.
    pub max_lifetime_ms: Option<i64>,
    /// Background sweep cadence, ms. Default 1 hour.
    pub expiry_check_interval_ms: Option<i64>,
    /// Default renew period — the initial `expiry_timestamp_ms` offset
    /// at create time and the implicit renew period when
    /// `RenewDelegationToken.renew_period_ms == -1`. Distinct from
    /// `max_lifetime_ms` (the absolute ceiling). Default 24 hours.
    pub default_renew_period_ms: Option<i64>,
}

pub(super) fn apply_delegation_tokens(
    delegation: Option<&FileDelegationTokenConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(delegation) = delegation else {
        return Ok(());
    };
    if cfg.delegation_token_secret_key.is_none()
        && let Some(key) = delegation.secret_key.clone()
    {
        cfg.delegation_token_secret_key = Some(krabka_security::SecretBytes::new(key.into_bytes()));
    }
    if let Some(milliseconds) = delegation.max_lifetime_ms {
        cfg.delegation_token_max_lifetime = Time::from_millis(positive_i64(
            "delegation_token.max_lifetime_ms",
            milliseconds,
        )?);
    }
    if let Some(milliseconds) = delegation.expiry_check_interval_ms {
        cfg.delegation_token_expiry_check_interval = Time::from_millis(positive_i64(
            "delegation_token.expiry_check_interval_ms",
            milliseconds,
        )?);
    }
    if let Some(milliseconds) = delegation.default_renew_period_ms {
        cfg.delegation_token_default_renew_period = Time::from_millis(positive_i64(
            "delegation_token.default_renew_period_ms",
            milliseconds,
        )?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::BrokerConfig,
            file_config::{FileConfig, test_support::configured_unwrap_parse},
        },
        assert2::{assert, check},
        krabka_units::{days, hours},
        std::sync::{Mutex, OnceLock},
    };

    /// Serializes any test that mutates process-wide env vars. Tests in
    /// the same `cargo test` process run on multiple threads by default,
    /// and `set_var`/`remove_var` are global side-effects.
    static ENV_LOCK_CELL: OnceLock<Mutex<()>> = OnceLock::new();
    fn env_lock() -> &'static Mutex<()> { ENV_LOCK_CELL.get_or_init(|| Mutex::new(())) }

    fn without_runtime_secret_key<T>(f: impl FnOnce() -> T) -> T {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("KRABKA_DELEGATION_TOKEN_SECRET_KEY", f)
    }

    #[test]
    fn delegation_token_section_parses_secret_key_and_defaults() {
        without_runtime_secret_key(|| {
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let cfg = configured_unwrap_parse(toml).unwrap();

            // KIP-48 defaults: 7 days max lifetime, 1 hour sweep cadence,
            // 24 hour default renew period.
            check!(
                cfg.delegation_token_secret_key
                    .as_ref()
                    .map(|s| s.as_bytes().to_vec())
                    == Some(b"abcdef".to_vec())
            );
            check!(cfg.delegation_token_max_lifetime == days(7));
            check!(cfg.delegation_token_expiry_check_interval == hours(1));
            check!(cfg.delegation_token_default_renew_period == hours(24));
        });
    }

    #[test]
    fn delegation_token_default_renew_period_ms_default_and_override() {
        without_runtime_secret_key(|| {
            // (1) When the TOML omits `default_renew_period_ms`, the config
            //     stays at the 24h KIP-48 default.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            assert!(
                cfg.delegation_token_default_renew_period == hours(24),
                "absent default_renew_period_ms should leave the 24h default in place"
            );

            // (2) When the TOML sets it, the override wins.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
default_renew_period_ms = 7200000
"#;
            let cfg = configured_unwrap_parse(toml).unwrap();
            assert!(
                cfg.delegation_token_default_renew_period == hours(2),
                "TOML default_renew_period_ms must override the default"
            );
        });
    }

    #[test]
    fn delegation_token_runtime_key_overrides_toml() {
        let toml = r#"
[delegation_token]
secret_key = "toml-loses"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = BrokerConfig {
            delegation_token_secret_key: Some(krabka_security::SecretBytes::new(
                b"runtime-wins".to_vec(),
            )),
            ..BrokerConfig::default()
        };
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.delegation_token_secret_key
                .as_ref()
                .map(|s| s.as_bytes().to_vec())
                == Some(b"runtime-wins".to_vec())
        );
    }

    #[test]
    fn delegation_token_absent_when_unset_anywhere() {
        without_runtime_secret_key(|| {
            let cfg = configured_unwrap_parse("").unwrap();

            // No secret key anywhere; lifetime knobs stay at their defaults
            // when no section is present.
            check!(cfg.delegation_token_secret_key.is_none());
            check!(
                (
                    cfg.delegation_token_max_lifetime,
                    cfg.delegation_token_expiry_check_interval,
                    cfg.delegation_token_default_renew_period,
                ) == (days(7), hours(1), hours(24))
            );
        });
    }
}
