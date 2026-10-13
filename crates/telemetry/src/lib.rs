//! Generic OTLP distributed-tracing pipeline for Krabka services.
//!
//! The consuming service always installs a structured-JSON `tracing_subscriber`
//! `fmt` layer. That layer writes to stdout and uses the usual `RUST_LOG`
//! `EnvFilter`. Container log collectors such as GKE / Cloud Logging and Loki
//! thus ingest each line as fields, not as ANSI-coloured human text.
//!
//! The environment can also configure OTLP export. This crate then attaches a
//! second `tracing-opentelemetry` layer, which converts `tracing` spans into
//! OpenTelemetry spans. That layer batch-exports the spans over OTLP to a
//! collector: gRPC `:4317` or HTTP/protobuf `:4318`.
//!
//! ## Enabling
//!
//! OTLP is **off by default**. A service with no OTLP environment keeps its
//! behavior byte-for-byte. OTLP turns on when one of these variables sets an
//! endpoint: `KRABKA_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, or
//! `OTEL_EXPORTER_OTLP_ENDPOINT`. `KRABKA_OTLP_ENABLED=true` also turns OTLP
//! on. `OTEL_SDK_DISABLED=true` turns OTLP off and overrides the other
//! variables.
//!
//! ## WebAssembly
//!
//! On `wasm32-wasip1` the crate has no OTLP exporter and no profiling admin
//! server, because both need a network client stack or a socket `bind`. There,
//! [`init`] returns `TelemetryError::Unsupported` for an OTLP configuration
//! and installs nothing, and [`profiling`] exports only its configuration
//! types.
//!
//! ## Resolve OTLP settings without touching the environment
//!
//! ```rust
//! use krabka_telemetry::{OtlpConfig, OtlpProtocol};
//!
//! let cfg = OtlpConfig::from_env(
//!     |key| (key == "KRABKA_OTLP_ENABLED").then(|| "true".to_string()),
//!     "broker-1",
//!     "0.1.0",
//!     "krabka-broker",
//! )
//! .expect("valid OTLP configuration")
//! .expect("OTLP enabled");
//!
//! assert2::assert!((cfg.protocol) == (OtlpProtocol::Grpc));
//! assert2::assert!((cfg.endpoint) == ("http://localhost:4317"));
//! ```

#![forbid(unsafe_code)]

pub mod profiling;

mod config;
mod error;
#[cfg(not(target_family = "wasm"))]
mod exporter;
#[cfg(not(target_family = "wasm"))]
mod heartbeat;
mod log_levels;
mod subscriber;

#[cfg(test)]
mod test_support;

pub use self::{
    config::{OtlpConfig, OtlpProtocol},
    error::TelemetryError,
    log_levels::{LogLevel, LogLevelController, LogLevelFilter, ROOT_LOGGER, VALID_LOG_LEVELS},
    subscriber::{TelemetryGuard, init},
};
/// W3C Trace Context propagation helpers.
///
/// These helpers live in the standalone, publishable `krabka-trace-context`
/// crate. The wire-protocol crates can thus propagate a trace and do not link
/// the OTLP exporter, the admin HTTP server, or the profiler that this crate
/// depends on.
pub use krabka_trace_context as propagation;
