//! Install of the global `tracing` subscriber and the guard that flushes it.
//!
//! The module wires the stdout JSON layer, the optional OTLP span layer, and
//! the optional OTLP log bridge into one registry, and it owns the shutdown
//! path for the providers that those layers export through.

use {
    crate::{config::OtlpConfig, error::TelemetryError, log_levels::LogLevelController},
    opentelemetry_sdk::{logs::SdkLoggerProvider, trace::SdkTracerProvider},
    tracing_subscriber::{
        EnvFilter, Layer, Registry, layer::SubscriberExt as _, util::SubscriberInitExt as _,
    },
};

/// The stdout layer, boxed as `krabka_logfmt::layer` returns it.
type StdoutLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// Owns the OTLP `SdkTracerProvider`, so shutdown flushes the spans.
///
/// Dropping the guard stops the heartbeat and shuts down both exporters,
/// including when another provider clone remains alive. Call
/// [`TelemetryGuard::shutdown`] before exit to deliver the final batch.
#[must_use = "hold the guard for the process lifetime and call shutdown() before exit"]
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    log_levels: LogLevelController,
}

impl TelemetryGuard {
    /// The handle that reads and retargets the stdout layer's filter while
    /// the process runs.
    ///
    /// A Kafka broker exposes this through the `BROKER_LOGGER` config
    /// resource, so `kafka-configs --entity-type broker-loggers` can raise a
    /// level without a restart. Clone it out before the guard is parked for
    /// the process lifetime.
    #[must_use]
    pub fn log_levels(&self) -> LogLevelController { self.log_levels.clone() }

    /// Flush and shut down the OTLP exporters for traces and logs.
    ///
    /// This function does nothing when OTLP is disabled.
    pub fn shutdown(mut self) { self.shutdown_providers(); }

    fn shutdown_providers(&mut self) {
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!(error = %e, "OTLP tracer provider shutdown error");
        }
        if let Some(logger_provider) = self.logger_provider.take()
            && let Err(e) = logger_provider.shutdown()
        {
            tracing::warn!(error = %e, "OTLP logger provider shutdown error");
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) { self.shutdown_providers(); }
}

/// Install the global `tracing` subscriber.
///
/// The subscriber has a stdout JSON `fmt` layer. When `otlp` is `Some`, the
/// subscriber also has a batch OTLP export layer.
///
/// - `fmt_default_filter`: the `fmt` layer's filter when `RUST_LOG` is unset.
/// - `otel_default_filter`: the OTLP layer's filter when `KRABKA_OTLP_FILTER`
///   is unset.
/// - `tracer_name`: the name that this function passes to
///   `TracerProvider::tracer(...)`.
///
/// You must call this function exactly one time, and from inside the tokio
/// runtime. The gRPC exporter captures the current runtime handle.
///
/// # Errors
///
/// Returns `TelemetryError::Exporter` when an OTLP exporter cannot be built.
/// On `wasm32-wasip1`, returns `TelemetryError::Unsupported` for any OTLP
/// configuration and installs nothing.
pub fn init(
    otlp: Option<OtlpConfig>,
    fmt_default_filter: &str,
    otel_default_filter: &str,
    tracer_name: &str,
) -> Result<TelemetryGuard, TelemetryError> {
    // The stdout layer's filter is the one an operator retargets at runtime,
    // so it is a `LogLevelFilter` and not a bare `EnvFilter`. It starts from
    // `RUST_LOG` when that is set and from `fmt_default_filter` otherwise.
    let fmt_spec = std::env::var("RUST_LOG")
        .ok()
        .filter(|spec| EnvFilter::try_new(spec).is_ok())
        .unwrap_or_else(|| fmt_default_filter.to_owned());
    let (log_levels, fmt_filter) = LogLevelController::new(&fmt_spec);
    // Structured Cloud Logging-friendly JSON to stdout (see `krabka_logfmt`),
    // so GKE / Cloud Logging ingests fields rather than ANSI-coloured text.
    let fmt_layer = krabka_logfmt::layer(fmt_filter, std::io::stdout);

    let Some(cfg) = otlp else {
        tracing_subscriber::registry().with(fmt_layer).init();
        return Ok(TelemetryGuard {
            provider: None,
            logger_provider: None,
            heartbeat_task: None,
            log_levels,
        });
    };
    otlp::install(
        &cfg,
        fmt_layer,
        log_levels,
        otel_default_filter,
        tracer_name,
    )
}

/// The OTLP span and log layers, installed beside the stdout layer.
#[cfg(not(target_family = "wasm"))]
mod otlp {
    use {
        super::{StdoutLayer, TelemetryGuard},
        crate::{
            config::OtlpConfig, error::TelemetryError, heartbeat::spawn_heartbeat_task,
            log_levels::LogLevelController,
        },
        krabka_units::prelude::TimeExt,
        opentelemetry::trace::TracerProvider as _,
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge,
        opentelemetry_sdk::{
            logs::SdkLoggerProvider, propagation::TraceContextPropagator, trace::SdkTracerProvider,
        },
        tracing_subscriber::{
            EnvFilter, Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _,
        },
    };

    /// Per-layer filter for the OTLP layer.
    ///
    /// Operators who want more control can override the filter with
    /// `KRABKA_OTLP_FILTER`. Without that variable, the filter falls back to
    /// `default`.
    fn otel_filter(default: &str, get: impl Fn(&str) -> Option<String>) -> EnvFilter {
        get("KRABKA_OTLP_FILTER")
            .and_then(|s| EnvFilter::try_new(s).ok())
            .unwrap_or_else(|| EnvFilter::new(default))
    }

    pub(super) fn install(
        cfg: &OtlpConfig,
        fmt_layer: StdoutLayer,
        log_levels: LogLevelController,
        otel_default_filter: &str,
        tracer_name: &str,
    ) -> Result<TelemetryGuard, TelemetryError> {
        let exporter = cfg.build_exporter()?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(cfg.resource())
            .with_sampler(cfg.sampler())
            .build();
        let tracer = provider.tracer(tracer_name.to_owned());

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        opentelemetry::global::set_tracer_provider(provider.clone());

        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_location(false)
            .with_filter(otel_filter(otel_default_filter, |k| std::env::var(k).ok()));

        // OTLP LOGS: a `tracing` → OTLP-log-record bridge so services ship their
        // logs over OTLP (alloy:4317 → logs-distributor) — Docker-stdout tailing
        // doesn't stream on Docker Desktop. Quiet the OTLP/transport stack on THIS
        // layer so the exporter's own events can't feed back into more log records.
        let log_exporter = cfg.build_log_exporter()?;
        let logger_provider = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(cfg.resource())
            .build();
        let logs_default = format!(
            "{otel_default_filter},opentelemetry=warn,opentelemetry_sdk=warn,opentelemetry_otlp=warn,hyper_util=warn,tonic=warn,h2=warn,tower=warn"
        );
        let otel_logs_layer = OpenTelemetryTracingBridge::new(&logger_provider)
            .with_filter(otel_filter(&logs_default, |k| std::env::var(k).ok()));

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel_layer)
            .with(otel_logs_layer)
            .init();

        let heartbeat_task = cfg.heartbeat_interval.map(|interval| {
            spawn_heartbeat_task(
                provider.clone(),
                cfg.service_name.clone(),
                cfg.service_instance_id.clone(),
                interval,
            )
        });

        tracing::info!(
            endpoint = %cfg.endpoint,
            protocol = ?cfg.protocol,
            sample_ratio = cfg.sample_ratio,
            heartbeat_interval_secs = cfg.heartbeat_interval.map(TimeExt::secs_f64),
            "OTLP distributed tracing + logs enabled"
        );

        Ok(TelemetryGuard {
            provider: Some(provider),
            logger_provider: Some(logger_provider),
            heartbeat_task,
            log_levels,
        })
    }

    #[cfg(test)]
    mod tests {
        use {super::otel_filter, crate::test_support::env_from};

        #[test]
        fn otel_filter_uses_override_or_default() {
            let filter = otel_filter("warn", env_from(&[]));
            assert2::assert!(filter.to_string() == "warn");

            let filter = otel_filter(
                "warn",
                env_from(&[("KRABKA_OTLP_FILTER", "krabka_telemetry=debug")]),
            );
            assert2::assert!(filter.to_string() == "krabka_telemetry=debug");

            let filter = otel_filter("warn", env_from(&[("KRABKA_OTLP_FILTER", "[")]));
            assert2::assert!(filter.to_string() == "warn");
        }
    }
}

// OTLP export needs a gRPC or HTTP client stack, which `wasm32-wasip1` does
// not have.
#[cfg(target_family = "wasm")]
mod otlp {
    use {
        super::{StdoutLayer, TelemetryGuard},
        crate::{config::OtlpConfig, error::TelemetryError, log_levels::LogLevelController},
    };

    pub(super) fn install(
        _cfg: &OtlpConfig,
        _fmt_layer: StdoutLayer,
        _log_levels: LogLevelController,
        _otel_default_filter: &str,
        _tracer_name: &str,
    ) -> Result<TelemetryGuard, TelemetryError> {
        Err(TelemetryError::Unsupported {
            what: "OTLP export",
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        opentelemetry_sdk::error::OTelSdkResult,
        std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering::SeqCst},
        },
    };

    #[derive(Clone, Debug)]
    struct ShutdownCountingSpanExporter {
        calls: Arc<AtomicUsize>,
    }

    impl opentelemetry_sdk::trace::SpanExporter for ShutdownCountingSpanExporter {
        fn export(
            &self,
            _batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            std::future::ready(Ok(()))
        }

        fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> OTelSdkResult {
            self.calls.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    #[test]
    fn telemetry_guard_shutdown_flushes_held_provider_clone() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(ShutdownCountingSpanExporter {
                calls: Arc::clone(&calls),
            })
            .build();
        let held_clone = provider.clone();

        TelemetryGuard {
            provider: Some(provider),
            logger_provider: None,
            heartbeat_task: None,
            log_levels: LogLevelController::new("info").0,
        }
        .shutdown();

        assert2::assert!(calls.load(SeqCst) == 1);
        drop(held_clone);
    }

    #[tokio::test]
    async fn telemetry_guard_drop_shuts_down_providers_and_stops_the_heartbeat() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(ShutdownCountingSpanExporter {
                calls: calls.clone(),
            })
            .build();
        let held_clone = provider.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finished, stopped) = tokio::sync::oneshot::channel::<()>();
        let heartbeat_task = tokio::spawn(async move {
            let _finished = finished;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        drop(TelemetryGuard {
            provider: Some(provider),
            logger_provider: None,
            heartbeat_task: Some(heartbeat_task),
            log_levels: LogLevelController::new("info").0,
        });
        assert2::assert!(calls.load(SeqCst) == 1);
        assert2::assert!(stopped.await.is_err());
        drop(held_clone);
        assert2::assert!(calls.load(SeqCst) == 1);
    }
}
