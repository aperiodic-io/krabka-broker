//! The `OpenTelemetry` settings, which a flag may override ahead of the
//! process environment the telemetry layer otherwise reads.

use {crate::cli::Args, krabka_units::fmt::Human as _};

impl Args {
    pub fn telemetry_value(&self, key: &str) -> Option<String> {
        match key {
            "OTEL_SDK_DISABLED" => self.otel_sdk_disabled.clone(),
            "KRABKA_OTLP_ENDPOINT" => self.krabka_otlp_endpoint.clone(),
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => self.otel_exporter_otlp_traces_endpoint.clone(),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => self.otel_exporter_otlp_endpoint.clone(),
            "KRABKA_OTLP_ENABLED" => self.krabka_otlp_enabled.clone(),
            "KRABKA_OTLP_PROTOCOL" => self.krabka_otlp_protocol.clone(),
            "OTEL_EXPORTER_OTLP_PROTOCOL" => self.otel_exporter_otlp_protocol.clone(),
            "KRABKA_OTLP_SAMPLE_RATIO" => self.krabka_otlp_sample_ratio.clone(),
            "OTEL_TRACES_SAMPLER_ARG" => self.otel_traces_sampler_arg.clone(),
            "OTEL_SERVICE_NAME" => self.otel_service_name.clone(),
            "KRABKA_OTLP_TIMEOUT" => self
                .krabka_otlp_timeout
                .map(|value| value.human().to_string()),
            "OTEL_EXPORTER_OTLP_TIMEOUT_SECS" => self.otel_exporter_otlp_timeout_secs.clone(),
            "KRABKA_OTLP_HEARTBEAT_INTERVAL" => self
                .krabka_otlp_heartbeat_interval
                .map(|value| value.human().to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::test_support::env_guard, assert2::assert, clap::Parser};

    #[test]
    fn otlp_time_cli_values_override_environment() {
        let _guard = env_guard();

        temp_env::with_vars(
            [
                ("KRABKA_OTLP_TIMEOUT", Some("17s")),
                ("KRABKA_OTLP_HEARTBEAT_INTERVAL", Some("19s")),
            ],
            || {
                let args = Args::try_parse_from([
                    "krabka-broker",
                    "--krabka-otlp-timeout=23s",
                    "--krabka-otlp-heartbeat-interval=29s",
                ])
                .expect("parse CLI OTLP overrides");
                assert!(
                    (
                        args.telemetry_value("KRABKA_OTLP_TIMEOUT"),
                        args.telemetry_value("KRABKA_OTLP_HEARTBEAT_INTERVAL"),
                    ) == (Some("23s".to_owned()), Some("29s".to_owned()))
                );
            },
        );
    }
}
