//! Shared parse-and-apply setup for file configuration tests.

use super::{FileConfig, FileConfigError};
use crate::config::BrokerConfig;

pub(super) fn configured(
    source: &str,
    parse_context: &str,
) -> Result<BrokerConfig, FileConfigError> {
    apply(toml::from_str(source).expect(parse_context))
}

/// The setup for tests that originally used `unwrap` on the TOML parse.
pub(super) fn configured_unwrap_parse(source: &str) -> Result<BrokerConfig, FileConfigError> {
    apply(toml::from_str(source).unwrap())
}

fn apply(file: FileConfig) -> Result<BrokerConfig, FileConfigError> {
    let mut config = BrokerConfig::default();
    file.apply_to(&mut config)?;
    Ok(config)
}

/// Expose one applied feature while preserving the original `InvalidConfig` message.
pub(super) fn configured_feature<T>(
    source: &str,
    feature: impl FnOnce(&BrokerConfig) -> T,
) -> Result<T, String> {
    configured_unwrap_parse(source)
        .map(|config| feature(&config))
        .map_err(|error| match error {
            FileConfigError::InvalidConfig(message) => message,
            other => other.to_string(),
        })
}

/// Invalid TOML and invalid applied values both leave the optional feature absent.
pub(super) fn optional_configured_feature<T>(
    source: &str,
    feature: impl FnOnce(&BrokerConfig) -> T,
) -> Option<T> {
    let file = toml::from_str(source).ok()?;
    apply(file).ok().map(|config| feature(&config))
}
