//! Prometheus `/metrics` HTTP server.
//!
//! It is the same shape as [`crate::health`]: a small axum app over one
//! router. It returns `OpenMetrics` text on success, and 500 on an encoder
//! failure.
//!
//! The two servers stay apart because they answer to different callers on
//! different schedules. A scrape is periodic and can be skipped; a probe runs
//! from the moment the process starts, before `Broker::start` has a registry
//! to serve, and decides whether the node keeps receiving traffic.
//!
//! [`crate::Broker::start`] spawns the server when
//! `BrokerConfig::metrics_listen_addr` is `Some`. The broker's supervisor
//! shutdown token cancels it.
//!
//! `wasm32-wasip1` has no `bind` and no HTTP server stack, so there is no
//! server on that target. `BrokerConfig::validate` refuses the setting there,
//! and [`run`] answers `Unsupported`.

use std::net::SocketAddr;

#[cfg(not(target_family = "wasm"))]
use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio_util::sync::CancellationToken;

use crate::metrics::SharedRegistry;

/// Builds the router. It serves `/metrics` for Prometheus, and
/// `/debug/pprof/{profile,heap}`. The CPU route is always present on Unix. The
/// heap route needs a build with `--features heap-profiling`.
#[cfg(not(target_family = "wasm"))]
pub fn router(
    registry: SharedRegistry,
    profiling: krabka_telemetry::profiling::ProfilingConfig,
) -> Result<Router, krabka_telemetry::profiling::ProfilingError> {
    Ok(Router::new()
        .route("/metrics", get(metrics))
        .with_state(registry)
        .merge(krabka_telemetry::profiling::pprof_router_with_config(
            profiling,
        )?))
}

/// Binds and serves until `shutdown` fires. It returns the bound address, so
/// an integration test can scrape a `127.0.0.1:0` config without a guess at
/// the port. On any axum error, which is normally a socket close, it logs the
/// error and returns.
#[cfg(not(target_family = "wasm"))]
pub(crate) async fn run(
    addr: SocketAddr,
    registry: SharedRegistry,
    profiling: krabka_telemetry::profiling::ProfilingConfig,
    shutdown: CancellationToken,
) -> Result<SocketAddr, krabka_telemetry::profiling::ProfilingError> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics server listening");
    let app = router(registry, profiling)?;
    crate::http_server::spawn(listener, app, shutdown, |error| {
        tracing::warn!(error = %error, "metrics server error");
    });
    Ok(bound)
}

/// `BrokerConfig::validate` refuses `metrics_listen_addr` on this platform,
/// which has no `bind` and no HTTP server stack.
#[cfg(target_family = "wasm")]
pub(crate) fn run(
    addr: SocketAddr,
    _registry: SharedRegistry,
    _profiling: krabka_telemetry::profiling::ProfilingConfig,
    _shutdown: CancellationToken,
) -> std::future::Ready<Result<SocketAddr, krabka_telemetry::profiling::ProfilingError>> {
    std::future::ready(Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "cannot serve metrics on {addr}: the metrics server is unavailable on this platform"
        ),
    )
    .into()))
}

#[cfg(not(target_family = "wasm"))]
async fn metrics(State(registry): State<SharedRegistry>) -> impl IntoResponse {
    let mut buf = String::new();
    let r = registry.lock().await;
    if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &r) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
    }
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;

    use super::*;
    use crate::metrics::BrokerMetrics;

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = BrokerMetrics::new();
        m.record_produce(&Arc::from("t"), 42);
        let app = router(
            m.registry,
            krabka_telemetry::profiling::ProfilingConfig::default(),
        )
        .unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/openmetrics-text"), "ct={ct}");
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        for needle in ["krabka_broker_topic_bytes_in_total", "42", "# EOF"] {
            assert!(s.contains(needle), "missing {needle:?} in {s}");
        }
    }

    #[test]
    fn router_rejects_invalid_profiling_policy() {
        let m = BrokerMetrics::new();
        let profiling = krabka_telemetry::profiling::ProfilingConfig {
            profiling_cpu_default_duration: krabka_units::secs(61),
            ..krabka_telemetry::profiling::ProfilingConfig::default()
        };
        assert!(router(m.registry, profiling).is_err());
    }
}
