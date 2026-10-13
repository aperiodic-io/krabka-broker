//! Resolution of the OTLP settings from the environment.
//!
//! The module holds `OtlpProtocol`, `OtlpConfig`, and the environment parsing
//! that decides whether OTLP export is on at all. It is separate from the
//! exporter build, so the decision logic stays pure and testable without a
//! collector.

use {
    crate::error::TelemetryError,
    krabka_units::prelude::{Time, TimeExt, secs},
};

/// OTLP transport.
///
/// The variants mirror the `OTEL_EXPORTER_OTLP_PROTOCOL` spec values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    /// The OTLP/gRPC transport. Default collector port `4317`.
    Grpc,
    /// The OTLP/HTTP transport with protobuf payloads. Default collector port
    /// `4318`.
    HttpProtobuf,
}

impl OtlpProtocol {
    /// Parse an `OTEL_EXPORTER_OTLP_PROTOCOL`-style value.
    ///
    /// A value that this function does not recognize falls back to gRPC, the
    /// SDK's default transport.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "http/protobuf" | "http" | "httpbinary" | "http-protobuf" => Self::HttpProtobuf,
            _ => Self::Grpc,
        }
    }

    #[must_use]
    pub fn default_endpoint(self) -> &'static str {
        match self {
            Self::Grpc => "http://localhost:4317",
            Self::HttpProtobuf => "http://localhost:4318",
        }
    }
}

/// Resolved OTLP configuration.
///
/// [`OtlpConfig::from_env`] builds this configuration. `Ok(None)` from that
/// constructor means that OTLP is disabled and that this crate builds no
/// exporter.
#[derive(Debug, Clone)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    /// Head sampling ratio in `[0.0, 1.0]`. A parent-based sampler wraps this
    /// ratio, so child spans keep an upstream sampling decision.
    pub sample_ratio: f64,
    pub service_name: String,
    pub service_version: String,
    pub service_instance_id: String,
    pub timeout: Time,
    pub heartbeat_interval: Option<Time>,
}

/// Truthy parse of `*_ENABLED` and `*_DISABLED` style environment values.
fn env_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn parse_time(name: &'static str, value: &str) -> Result<Time, TelemetryError> {
    let time = krabka_units::parse::non_negative_time(value).map_err(|error| {
        TelemetryError::InvalidConfig {
            name,
            message: error.to_string(),
        }
    })?;
    std::time::Duration::try_from_secs_f64(time.secs_f64()).map_err(|error| {
        TelemetryError::InvalidConfig {
            name,
            message: error.to_string(),
        }
    })?;
    Ok(time)
}

impl OtlpConfig {
    /// Resolve OTLP config from the environment.
    ///
    /// `get` is the environment lookup. The caller injects it, so this
    /// function is pure and easy to test. `service_instance_id` is the service
    /// instance id, for example the broker node id. `service_version` is the
    /// crate version. `default_service_name` is the fallback name for when
    /// `OTEL_SERVICE_NAME` is not set.
    ///
    /// Returns `Ok(None)` when OTLP is disabled. OTLP is disabled when nothing
    /// turned it on, or when `OTEL_SDK_DISABLED` turned it off.
    ///
    /// # Errors
    ///
    /// Returns an error when a custom Krabka duration is malformed.
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        service_instance_id: &str,
        service_version: &str,
        default_service_name: &str,
    ) -> Result<Option<Self>, TelemetryError> {
        if get("OTEL_SDK_DISABLED").as_deref().is_some_and(env_truthy) {
            return Ok(None);
        }

        let endpoint_override = get("KRABKA_OTLP_ENDPOINT")
            .or_else(|| get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
            .or_else(|| get("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());

        let explicitly_enabled = get("KRABKA_OTLP_ENABLED")
            .as_deref()
            .is_some_and(env_truthy);

        // Off unless something opts in.
        if endpoint_override.is_none() && !explicitly_enabled {
            return Ok(None);
        }

        let protocol = get("KRABKA_OTLP_PROTOCOL")
            .or_else(|| get("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .map_or(OtlpProtocol::Grpc, |s| OtlpProtocol::parse(&s));

        let endpoint = endpoint_override.unwrap_or_else(|| protocol.default_endpoint().to_owned());

        let sample_ratio = get("KRABKA_OTLP_SAMPLE_RATIO")
            .or_else(|| get("OTEL_TRACES_SAMPLER_ARG"))
            .and_then(|s| s.trim().parse::<f64>().ok())
            .map_or(1.0, |r| r.clamp(0.0, 1.0));

        let service_name = get("OTEL_SERVICE_NAME")
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default_service_name.to_owned());

        let timeout = match get("KRABKA_OTLP_TIMEOUT") {
            Some(value) => parse_time("KRABKA_OTLP_TIMEOUT", &value)?,
            None => get("OTEL_EXPORTER_OTLP_TIMEOUT_SECS")
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map_or_else(
                    || secs(10),
                    |seconds| Time::from_std(std::time::Duration::from_secs(seconds)),
                ),
        };

        let heartbeat_interval = get("KRABKA_OTLP_HEARTBEAT_INTERVAL")
            .map(|value| parse_time("KRABKA_OTLP_HEARTBEAT_INTERVAL", &value))
            .transpose()?
            .filter(|interval| *interval > Time::ZERO);

        Ok(Some(Self {
            endpoint,
            protocol,
            sample_ratio,
            service_name,
            service_version: service_version.to_owned(),
            service_instance_id: service_instance_id.to_owned(),
            timeout,
            heartbeat_interval,
        }))
    }
}

#[cfg(test)]
mod tests;
