//! `init` with an OTLP configuration. It installs the process's one global
//! subscriber, so it runs in a test binary of its own.

use {
    krabka_telemetry::{OtlpConfig, OtlpProtocol},
    krabka_units::prelude::{millis, secs},
    opentelemetry::trace::TraceContextExt as _,
    tracing_opentelemetry::OpenTelemetrySpanExt as _,
};

/// A loopback address nothing listens on: the exporters connect lazily, so
/// the batches they cannot send are dropped and nothing blocks.
fn closed_loopback_endpoint() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_with_otlp_installs_the_span_and_log_export_and_shuts_down() {
    let cfg = OtlpConfig {
        endpoint: closed_loopback_endpoint(),
        protocol: OtlpProtocol::Grpc,
        sample_ratio: 1.0,
        service_name: "krabka-telemetry-test".to_owned(),
        service_version: "0.0.0".to_owned(),
        service_instance_id: "otlp-test-1".to_owned(),
        timeout: millis(200),
        heartbeat_interval: Some(secs(60)),
    };

    let guard = krabka_telemetry::init(Some(cfg), "info", "info", "krabka-telemetry-test");
    assert2::assert!(let Ok(guard) = guard);
    assert2::assert!(tracing::dispatcher::has_been_set());

    // Trace context crosses process boundaries in W3C `traceparent` and
    // `tracestate` headers.
    let fields: Vec<String> = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.fields().map(str::to_owned).collect()
    });
    assert2::assert!(fields == ["traceparent", "tracestate"]);

    // A `tracing` span carries a sampled OpenTelemetry span: the span layer
    // is installed, and a sample ratio of 1 keeps every trace.
    let span = tracing::info_span!("otlp-probe");
    let context = span.context();
    let span_context = context.span().span_context().clone();
    assert2::assert!(span_context.is_valid());
    assert2::assert!(span_context.is_sampled());
    span.in_scope(|| tracing::info!("an event for the stdout layer and the OTLP log bridge"));
    drop(span);

    guard.shutdown();
}
